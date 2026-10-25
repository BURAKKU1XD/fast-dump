mod prefetch;
use std::sync::atomic::Ordering;

#[cfg(test)]
use prefetch::PREFETCH_PAYLOAD_BYTES;
#[cfg(test)]
pub(crate) use prefetch::materialize_batch;
pub(crate) use prefetch::{
    EntryContents, PrefetchBudget, READ_BATCH_ENTRIES, ReadBatch,
    materialize_budgeted_batch_controlled,
};

use std::fs::{self, File};
use std::io::{self, Read};
use std::ops::Range;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::network::protocol::{BundleEntryDescriptor, BundleEntryKind, SkipReason, SkippedEntry};
use crate::runtime::SessionControl;

use super::scan::{ScannedEntry, ScannedEntryKind};
use super::source_policy::{self, ExcludedFilesystem};

pub(crate) const MAX_IMMEDIATE_ATTEMPTS: u32 = 3;
#[cfg(test)]
pub(crate) const PREFETCH_FILE_BYTES: u64 = 256 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct MaterializedEntry {
    pub(crate) descriptor: BundleEntryDescriptor,
    pub(crate) mode: u32,
    pub(crate) contents: EntryContents,
}

#[derive(Debug)]
pub(crate) struct StreamedRegularFile {
    pub(crate) descriptor: BundleEntryDescriptor,
    pub(crate) mode: u32,
    pub(crate) abs_path: PathBuf,
    pub(crate) file: File,
    pub(crate) forced_short_read_limit: Option<u64>,
    pub(crate) open_attempts: u32,
}

#[derive(Debug)]
pub(crate) enum ReopenDecision {
    Opened,
    Unsupported(ExcludedFilesystem),
}

enum OpenRegularFile {
    Opened(File, fs::Metadata, Option<u64>),
    Unsupported(ExcludedFilesystem),
}

impl StreamedRegularFile {
    pub(crate) fn remaining_attempts(&self) -> u32 {
        MAX_IMMEDIATE_ATTEMPTS.saturating_sub(self.open_attempts) + 1
    }

    pub(crate) fn take_forced_short_read_limit(&mut self) -> Option<u64> {
        self.forced_short_read_limit.take()
    }

    pub(crate) fn reopen_for_retry(&mut self) -> Result<ReopenDecision> {
        anyhow::ensure!(
            self.open_attempts < MAX_IMMEDIATE_ATTEMPTS,
            "file attempt limit exhausted"
        );
        self.open_attempts += 1;
        let (file, metadata, forced_short_read_limit) =
            match open_regular_file_attempt(&self.abs_path)? {
                OpenRegularFile::Opened(file, metadata, forced_limit) => {
                    (file, metadata, forced_limit)
                }
                OpenRegularFile::Unsupported(filesystem) => {
                    return Ok(ReopenDecision::Unsupported(filesystem));
                }
            };
        self.descriptor.size_at_open = metadata.len();
        self.descriptor.best_effort = true;
        self.mode = regular_mode(&metadata);
        self.file = file;
        self.forced_short_read_limit = forced_short_read_limit;
        Ok(ReopenDecision::Opened)
    }
}

#[derive(Debug)]
pub(crate) enum ReadDecision {
    Accepted(MaterializedEntry),
    Streamed(StreamedRegularFile),
    Skipped(SkippedEntry),
}

#[cfg(test)]
pub(crate) fn materialize_owned_entry(
    entry: ScannedEntry,
    stream_threshold_bytes: u64,
) -> Result<ReadDecision> {
    materialize_owned_entry_controlled(entry, stream_threshold_bytes, None)
}

pub(crate) fn materialize_owned_entry_controlled(
    entry: ScannedEntry,
    stream_threshold_bytes: u64,
    control: Option<&SessionControl>,
) -> Result<ReadDecision> {
    super::check_control(control)?;
    match entry.kind {
        ScannedEntryKind::Regular => {
            materialize_regular_file(entry, stream_threshold_bytes, control)
        }
        ScannedEntryKind::Symlink => materialize_symlink(entry),
    }
}

#[cfg(test)]
pub(crate) fn materialize_entry(
    entry: &ScannedEntry,
    stream_threshold_bytes: u64,
) -> Result<ReadDecision> {
    materialize_owned_entry(entry.clone(), stream_threshold_bytes)
}

fn materialize_regular_file(
    entry: ScannedEntry,
    stream_threshold_bytes: u64,
    control: Option<&SessionControl>,
) -> Result<ReadDecision> {
    for attempt in 1..=MAX_IMMEDIATE_ATTEMPTS {
        before_read_attempt(control, attempt > 1)?;
        let (mut file, metadata, forced_short_read_limit) =
            match open_regular_file_attempt(&entry.abs_path) {
                Ok(OpenRegularFile::Opened(file, metadata, forced_limit)) => {
                    (file, metadata, forced_limit)
                }
                Ok(OpenRegularFile::Unsupported(filesystem)) => {
                    super::check_control(control)?;
                    source_policy::log_exclusion(control, &entry.relative_path, filesystem);
                    return Ok(ReadDecision::Skipped(SkippedEntry {
                        work_ordinal: entry.work_ordinal,
                        relative_path: entry.relative_path,
                        reason: SkipReason::UnsupportedEntry,
                    }));
                }
                Err(_) if attempt == MAX_IMMEDIATE_ATTEMPTS => {
                    super::check_control(control)?;
                    return Ok(ReadDecision::Skipped(SkippedEntry {
                        work_ordinal: entry.work_ordinal,
                        relative_path: entry.relative_path,
                        reason: SkipReason::OpenFailed,
                    }));
                }
                Err(_) => continue,
            };

        let size_at_open = metadata.len();
        let mode = regular_mode(&metadata);
        let contents = if size_at_open <= stream_threshold_bytes {
            match read_contents(&mut file, size_at_open, forced_short_read_limit) {
                Ok(contents) => Some(contents),
                Err(_) if attempt == MAX_IMMEDIATE_ATTEMPTS => {
                    super::check_control(control)?;
                    return Ok(ReadDecision::Skipped(SkippedEntry {
                        work_ordinal: entry.work_ordinal,
                        relative_path: entry.relative_path,
                        reason: SkipReason::UnstableAfterRetries,
                    }));
                }
                Err(_) => continue,
            }
        } else {
            None
        };

        // До успешного чтения путь остаётся у entry для retry, затем переносится без копии.
        let descriptor = BundleEntryDescriptor {
            work_ordinal: entry.work_ordinal,
            relative_path: entry.relative_path,
            kind: BundleEntryKind::Regular,
            size_at_open,
            best_effort: attempt != 1,
        };

        return Ok(match contents {
            Some(contents) => ReadDecision::Accepted(MaterializedEntry {
                descriptor,
                mode,
                contents: contents.into(),
            }),
            None => ReadDecision::Streamed(StreamedRegularFile {
                descriptor,
                mode,
                abs_path: entry.abs_path,
                file,
                forced_short_read_limit,
                open_attempts: attempt,
            }),
        });
    }

    Ok(ReadDecision::Skipped(SkippedEntry {
        work_ordinal: entry.work_ordinal,
        relative_path: entry.relative_path,
        reason: SkipReason::UnstableAfterRetries,
    }))
}

// Когда read-ahead не получил byte budget, planner дочитывает уже открытый handle:
// размер и лимит попыток остаются общими, новый open здесь не нужен.
#[cfg(test)]
pub(crate) fn finish_materializing(
    entry: StreamedRegularFile,
    stream_threshold_bytes: u64,
) -> Result<ReadDecision> {
    finish_materializing_controlled(entry, stream_threshold_bytes, None)
}

pub(crate) fn finish_materializing_controlled(
    mut entry: StreamedRegularFile,
    stream_threshold_bytes: u64,
    control: Option<&SessionControl>,
) -> Result<ReadDecision> {
    loop {
        super::check_control(control)?;
        if entry.descriptor.size_at_open > stream_threshold_bytes {
            return Ok(ReadDecision::Streamed(entry));
        }
        let forced_limit = entry.take_forced_short_read_limit();
        if let Ok(contents) =
            read_contents(&mut entry.file, entry.descriptor.size_at_open, forced_limit)
        {
            return Ok(ReadDecision::Accepted(MaterializedEntry {
                descriptor: entry.descriptor,
                mode: entry.mode,
                contents: contents.into(),
            }));
        }
        if let Some(reason) = reopen_after_short_read(&mut entry, control)? {
            return Ok(skipped_regular(entry, reason));
        }
    }
}

fn reopen_after_short_read(
    entry: &mut StreamedRegularFile,
    control: Option<&SessionControl>,
) -> Result<Option<SkipReason>> {
    super::check_control(control)?;
    if entry.open_attempts == MAX_IMMEDIATE_ATTEMPTS {
        return Ok(Some(SkipReason::UnstableAfterRetries));
    }
    loop {
        before_read_attempt(control, true)?;
        match entry.reopen_for_retry() {
            Ok(ReopenDecision::Opened) => return Ok(None),
            Ok(ReopenDecision::Unsupported(filesystem)) => {
                super::check_control(control)?;
                source_policy::log_exclusion(control, &entry.descriptor.relative_path, filesystem);
                return Ok(Some(SkipReason::UnsupportedEntry));
            }
            Err(_) => {}
        }
        super::check_control(control)?;
        if entry.open_attempts == MAX_IMMEDIATE_ATTEMPTS {
            return Ok(Some(SkipReason::OpenFailed));
        }
    }
}

pub(crate) fn before_read_attempt(control: Option<&SessionControl>, retry: bool) -> Result<()> {
    super::check_control(control)?;
    if retry && let Some(progress) = control.and_then(SessionControl::progress) {
        progress.retries.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

fn skipped_regular(entry: StreamedRegularFile, reason: SkipReason) -> ReadDecision {
    ReadDecision::Skipped(SkippedEntry {
        work_ordinal: entry.descriptor.work_ordinal,
        relative_path: entry.descriptor.relative_path,
        reason,
    })
}

// read_to_end заполняет свободную ёмкость Vec без предварительного обнуления.
// Take ограничивает чтение размером при открытии и отсекает дописанный хвост.
fn read_contents(file: &mut File, size: u64, forced_limit: Option<u64>) -> io::Result<Vec<u8>> {
    let len = usize::try_from(size)
        .map_err(|_| io::Error::other("file size exceeds addressable memory"))?;
    let mut contents = Vec::with_capacity(len);
    append_contents(file, size, forced_limit, &mut contents)?;
    Ok(contents)
}

fn append_contents(
    file: &mut File,
    size: u64,
    forced_limit: Option<u64>,
    contents: &mut Vec<u8>,
) -> io::Result<Range<usize>> {
    let start = contents.len();
    let len = usize::try_from(size)
        .map_err(|_| io::Error::other("file size exceeds addressable memory"))?;
    let limit = forced_limit.map_or(size, |limit| limit.min(size));
    file.take(limit).read_to_end(contents)?;
    if contents.len() - start != len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "file shortened while reading",
        ));
    }
    Ok(start..contents.len())
}

fn open_regular_file_attempt(path: &Path) -> Result<OpenRegularFile> {
    test_hooks::before_open(path)?;
    let mut options = fs::OpenOptions::new();
    options.read(true);
    // Если после scan файл заменили ссылкой, не читаем содержимое её цели.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // FIFO вместо файла не должен зависнуть в open до проверки типа handle.
        // На чтение обычного файла O_NONBLOCK не влияет.
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let file = options
        .open(path)
        .with_context(|| format!("failed to open v4 regular file {}", path.display()))?;
    let metadata = regular_file_metadata(&file, path)?;
    // Проверяем каждый новый handle до size_at_open и любых чтений: отдельный
    // bind mount может вести в служебную ФС внутри обычного каталога.
    #[cfg(test)]
    let excluded = test_hooks::classify_file(path, &file);
    #[cfg(not(test))]
    let excluded = source_policy::excluded_file(&file);
    if let Some(filesystem) = excluded.with_context(|| {
        format!(
            "failed to classify source filesystem for {}",
            path.display()
        )
    })? {
        return Ok(OpenRegularFile::Unsupported(filesystem));
    }
    #[cfg(windows)]
    let (file, metadata) = {
        use std::os::windows::fs::MetadataExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            // Symlink уже отвергнут. Другим reparse points, например Dedup,
            // нужен обычный filter processing для чтения восстановленных bytes.
            let file = reopen_for_normal_read(&file).with_context(|| {
                format!(
                    "failed to reopen v4 regular reparse file {}",
                    path.display()
                )
            })?;
            // Размер берём после повторного открытия, с того же handle, который читаем.
            let metadata = regular_file_metadata(&file, path)?;
            (file, metadata)
        } else {
            (file, metadata)
        }
    };
    let forced_short_read_limit = test_hooks::forced_short_read_after_open(path);
    Ok(OpenRegularFile::Opened(
        file,
        metadata,
        forced_short_read_limit,
    ))
}

fn regular_file_metadata(file: &File, path: &Path) -> Result<fs::Metadata> {
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to read v4 metadata for {}", path.display()))?;
    anyhow::ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "source entry is no longer a regular file: {}",
        path.display()
    );
    Ok(metadata)
}

#[cfg(windows)]
fn reopen_for_normal_read(file: &File) -> io::Result<File> {
    use std::os::windows::io::{AsRawHandle, FromRawHandle};
    use windows_sys::Win32::Foundation::{GENERIC_READ, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, ReOpenFile,
    };

    // ReOpenFile привязан к живому handle, а не к заменяемому pathname.
    // Нулевые flags снова разрешают обработку reparse point файловым фильтром.
    let handle = unsafe {
        ReOpenFile(
            file.as_raw_handle(),
            GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            0,
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // Успешный вызов передаёт новый owned handle, который File закроет ровно один раз.
    Ok(unsafe { File::from_raw_handle(handle) })
}

fn materialize_symlink(entry: ScannedEntry) -> Result<ReadDecision> {
    match fs::read_link(&entry.abs_path) {
        Ok(target) => {
            let metadata = fs::symlink_metadata(&entry.abs_path).with_context(|| {
                format!(
                    "failed to read v4 symlink metadata for {}",
                    entry.abs_path.display()
                )
            })?;
            Ok(ReadDecision::Accepted(MaterializedEntry {
                descriptor: BundleEntryDescriptor {
                    work_ordinal: entry.work_ordinal,
                    relative_path: entry.relative_path,
                    kind: BundleEntryKind::Symlink {
                        target: target.to_string_lossy().into_owned(),
                    },
                    size_at_open: 0,
                    best_effort: false,
                },
                mode: symlink_mode(&metadata),
                contents: Vec::new().into(),
            }))
        }
        Err(_) => Ok(ReadDecision::Skipped(SkippedEntry {
            work_ordinal: entry.work_ordinal,
            relative_path: entry.relative_path,
            reason: SkipReason::ReadSymlinkFailed,
        })),
    }
}

#[cfg(unix)]
fn regular_mode(metadata: &fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;

    metadata.permissions().mode()
}

#[cfg(not(unix))]
fn regular_mode(metadata: &fs::Metadata) -> u32 {
    if metadata.permissions().readonly() {
        0o444
    } else {
        0o644
    }
}

#[cfg(unix)]
fn symlink_mode(metadata: &fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;

    metadata.permissions().mode()
}

#[cfg(not(unix))]
fn symlink_mode(_metadata: &fs::Metadata) -> u32 {
    0o777
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sender::scan::scan_source_tree;
    use crate::tests::TestDir;
    use std::io::Write;
    use std::sync::Arc;

    #[test]
    fn excluded_filesystem_is_skipped_before_payload_without_retry() {
        let _guard = test_hooks::lock();
        test_hooks::clear();
        let dir = TestDir::new();
        let path = dir.path().join("file");
        fs::write(&path, b"original").unwrap();
        let entry = scan_source_tree(dir.path()).unwrap().entries.remove(0);
        test_hooks::classify_next_open(
            &path,
            test_hooks::FilesystemResult::Unsupported(ExcludedFilesystem::procfs_for_test()),
        );
        test_hooks::classify_next_open(&path, test_hooks::FilesystemResult::Failed);
        test_hooks::force_short_read_after_next_open(&path, 2);
        let run =
            crate::runtime::RunControl::new(Some(Arc::new(crate::progress::ProgressHub::new())));
        let control = run.session(crate::role::Role::Sender, "dump", "peer");
        let ReadDecision::Skipped(skipped) =
            materialize_owned_entry_controlled(entry, u64::MAX, Some(&control)).unwrap()
        else {
            panic!("excluded filesystem reached payload preparation");
        };
        assert_eq!(skipped.reason, SkipReason::UnsupportedEntry);
        assert_eq!(skipped.work_ordinal, 1);
        assert_eq!(skipped.relative_path, "file");
        assert_eq!(test_hooks::remaining_filesystem_results(&path), 1);
        assert_eq!(test_hooks::forced_short_read_after_open(&path), Some(2));
        let progress = control.progress().unwrap();
        assert_eq!(progress.retries.load(Ordering::Relaxed), 0);
        assert_eq!(progress.skips.load(Ordering::Relaxed), 0);
        test_hooks::clear();
    }

    #[test]
    fn filesystem_classification_errors_share_open_attempt_budget() {
        let _guard = test_hooks::lock();
        for failures in [2, 3] {
            test_hooks::clear();
            let dir = TestDir::new();
            let path = dir.path().join("file");
            fs::write(&path, b"original").unwrap();
            let entry = scan_source_tree(dir.path()).unwrap().entries.remove(0);
            for _ in 0..failures {
                test_hooks::classify_next_open(&path, test_hooks::FilesystemResult::Failed);
            }
            test_hooks::classify_next_open(&path, test_hooks::FilesystemResult::Allowed);
            let run = crate::runtime::RunControl::new(Some(Arc::new(
                crate::progress::ProgressHub::new(),
            )));
            let control = run.session(crate::role::Role::Sender, "dump", "peer");
            let result = materialize_owned_entry_controlled(entry, 1024, Some(&control)).unwrap();
            match result {
                ReadDecision::Accepted(entry) if failures == 2 => {
                    assert!(entry.descriptor.best_effort);
                    assert_eq!(entry.contents, b"original");
                }
                ReadDecision::Skipped(entry) if failures == 3 => {
                    assert_eq!(entry.reason, SkipReason::OpenFailed);
                }
                _ => panic!("classification failure changed the immediate attempt budget"),
            }
            assert_eq!(
                test_hooks::remaining_filesystem_results(&path),
                failures - 2
            );
            assert_eq!(
                control.progress().unwrap().retries.load(Ordering::Relaxed),
                2
            );
        }
        test_hooks::clear();
    }

    #[test]
    fn deferred_and_prefetched_retries_reject_new_excluded_handles() {
        let _guard = test_hooks::lock();
        for prefetched in [false, true] {
            test_hooks::clear();
            let dir = TestDir::new();
            let path = dir.path().join("file");
            fs::write(&path, b"original").unwrap();
            test_hooks::classify_next_open(&path, test_hooks::FilesystemResult::Allowed);
            test_hooks::classify_next_open(
                &path,
                test_hooks::FilesystemResult::Unsupported(ExcludedFilesystem::procfs_for_test()),
            );
            test_hooks::classify_next_open(&path, test_hooks::FilesystemResult::Allowed);
            test_hooks::force_short_read_after_next_open(&path, 2);
            let run = crate::runtime::RunControl::new(Some(Arc::new(
                crate::progress::ProgressHub::new(),
            )));
            let control = run.session(crate::role::Role::Sender, "dump", "peer");
            let budget = PrefetchBudget::new();
            let result = if prefetched {
                let [result, _, _, _] = materialize_budgeted_batch_controlled(
                    scanned_batch(dir.path()),
                    1024,
                    &budget,
                    Some(&control),
                );
                result.unwrap().unwrap()
            } else {
                let entry = scan_source_tree(dir.path()).unwrap().entries.remove(0);
                let ReadDecision::Streamed(file) =
                    materialize_owned_entry_controlled(entry, 1, Some(&control)).unwrap()
                else {
                    panic!("file should defer payload reading");
                };
                finish_materializing_controlled(file, 1024, Some(&control)).unwrap()
            };
            let ReadDecision::Skipped(skipped) = result else {
                panic!("retry accepted an excluded handle");
            };
            assert_eq!(skipped.reason, SkipReason::UnsupportedEntry);
            assert_eq!(test_hooks::remaining_filesystem_results(&path), 1);
            assert_eq!(
                budget.available.load(Ordering::Relaxed),
                PREFETCH_PAYLOAD_BYTES
            );
            assert_eq!(
                control.progress().unwrap().retries.load(Ordering::Relaxed),
                1
            );
            assert_eq!(control.progress().unwrap().skips.load(Ordering::Relaxed), 0);
        }
        test_hooks::clear();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn real_procfs_regular_handle_is_rejected_without_reading() {
        let entry = ScannedEntry {
            work_ordinal: 1,
            relative_path: "version".into(),
            abs_path: PathBuf::from("/proc/version"),
            kind: ScannedEntryKind::Regular,
        };
        let ReadDecision::Skipped(skipped) = materialize_owned_entry(entry, u64::MAX).unwrap()
        else {
            panic!("procfs regular file was accepted");
        };
        assert_eq!(skipped.reason, SkipReason::UnsupportedEntry);
    }

    #[test]
    fn controlled_reads_count_retries_across_open_and_short_read() {
        let _guard = test_hooks::lock();
        test_hooks::clear();
        let dir = TestDir::new();
        let path = dir.path().join("file");
        fs::write(&path, b"original").unwrap();
        let entry = scan_source_tree(dir.path()).unwrap().entries.remove(0);
        test_hooks::fail_next_open(&path);
        test_hooks::force_short_read_after_next_open(&path, 2);
        let run =
            crate::runtime::RunControl::new(Some(Arc::new(crate::progress::ProgressHub::new())));
        let control = run.session(crate::role::Role::Sender, "dump", "peer");
        let ReadDecision::Accepted(entry) =
            materialize_owned_entry_controlled(entry, 1024, Some(&control)).unwrap()
        else {
            panic!("third file attempt should succeed");
        };
        assert_eq!(entry.contents, b"original");
        assert!(entry.descriptor.best_effort);
        assert_eq!(
            control.progress().unwrap().retries.load(Ordering::Relaxed),
            2
        );
        test_hooks::clear();
    }

    #[test]
    fn cancelled_reads_do_not_retry_or_turn_missing_files_into_skips() {
        let dir = TestDir::new();
        let path = dir.path().join("file");
        fs::write(&path, b"original").unwrap();
        let entry = scan_source_tree(dir.path()).unwrap().entries.remove(0);
        fs::remove_file(path).unwrap();
        let run =
            crate::runtime::RunControl::new(Some(Arc::new(crate::progress::ProgressHub::new())));
        let control = run.session(crate::role::Role::Sender, "dump", "peer");
        control.cancel();
        let error = materialize_owned_entry_controlled(entry, 1024, Some(&control)).unwrap_err();
        assert!(error.downcast_ref::<crate::runtime::Cancelled>().is_some());
        assert_eq!(
            control.progress().unwrap().retries.load(Ordering::Relaxed),
            0
        );
        assert_eq!(control.progress().unwrap().skips.load(Ordering::Relaxed), 0);
    }

    #[cfg(windows)]
    #[test]
    fn normal_reopen_preserves_file_after_rename_and_path_replacement() {
        let dir = TestDir::new();
        let path = dir.path().join("file");
        let renamed = dir.path().join("renamed");
        fs::write(&path, b"original").unwrap();
        let OpenRegularFile::Opened(mut original, _, _) = open_regular_file_attempt(&path).unwrap()
        else {
            panic!("ordinary file was excluded");
        };
        let mut prefix = [0; 2];
        original.read_exact(&mut prefix).unwrap();
        fs::rename(&path, &renamed).unwrap();
        fs::write(&path, b"different replacement").unwrap();
        fs::OpenOptions::new()
            .append(true)
            .open(&renamed)
            .unwrap()
            .write_all(b"-tail")
            .unwrap();

        let mut reopened = reopen_for_normal_read(&original).unwrap();
        let metadata = regular_file_metadata(&reopened, &path).unwrap();
        assert_eq!(metadata.len(), 13);
        let contents = read_contents(&mut reopened, metadata.len(), None).unwrap();
        assert_eq!(contents, b"original-tail");
        original.read_exact(&mut prefix).unwrap();
        assert_eq!(&prefix, b"ig");
        assert_eq!(fs::read(&path).unwrap(), b"different replacement");
    }

    #[cfg(windows)]
    #[test]
    fn normal_reopen_ignores_replacement_symlink_but_path_open_rejects_it() {
        let dir = TestDir::new();
        let path = dir.path().join("file");
        let renamed = dir.path().join("renamed");
        let target = dir.path().join("target");
        fs::write(&path, b"original").unwrap();
        fs::write(&target, b"symlink target").unwrap();
        let OpenRegularFile::Opened(original, _, _) = open_regular_file_attempt(&path).unwrap()
        else {
            panic!("ordinary file was excluded");
        };
        fs::rename(&path, &renamed).unwrap();
        if std::os::windows::fs::symlink_file(&target, &path).is_err() {
            return;
        }

        let mut reopened = reopen_for_normal_read(&original).unwrap();
        let metadata = regular_file_metadata(&reopened, &path).unwrap();
        assert_eq!(
            read_contents(&mut reopened, metadata.len(), None).unwrap(),
            b"original"
        );
        assert!(open_regular_file_attempt(&path).is_err());
    }

    #[test]
    fn deferred_read_ignores_appended_tail_and_keeps_open_size() {
        let dir = TestDir::new();
        let path = dir.path().join("file");
        fs::write(&path, b"original").unwrap();
        let scan = scan_source_tree(dir.path()).unwrap();
        let ReadDecision::Streamed(file) = materialize_entry(&scan.entries[0], 1).unwrap() else {
            panic!();
        };
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"tail")
            .unwrap();
        let ReadDecision::Accepted(entry) = finish_materializing(file, 1024).unwrap() else {
            panic!();
        };
        assert_eq!(entry.contents, b"original");
        assert_eq!(entry.descriptor.size_at_open, 8);
    }

    #[test]
    fn deferred_read_does_not_reset_three_attempt_budget() {
        let _guard = test_hooks::lock();
        test_hooks::clear();
        let dir = TestDir::new();
        let path = dir.path().join("file");
        fs::write(&path, b"original").unwrap();
        test_hooks::fail_next_open(&path);
        test_hooks::fail_next_open(&path);
        test_hooks::force_short_read_after_next_open(&path, 2);
        let scan = scan_source_tree(dir.path()).unwrap();
        let ReadDecision::Streamed(file) = materialize_entry(&scan.entries[0], 1).unwrap() else {
            panic!();
        };
        assert_eq!(file.remaining_attempts(), 1);
        let ReadDecision::Skipped(entry) = finish_materializing(file, 1024).unwrap() else {
            panic!("exhausted file was retried");
        };
        assert_eq!(entry.reason, SkipReason::UnstableAfterRetries);
        test_hooks::clear();
    }

    #[test]
    fn retry_that_grows_past_bundle_target_switches_to_streaming() {
        let _guard = test_hooks::lock();
        test_hooks::clear();
        let dir = TestDir::new();
        let path = dir.path().join("file");
        fs::write(&path, b"original").unwrap();
        test_hooks::force_short_read_after_next_open(&path, 2);
        let scan = scan_source_tree(dir.path()).unwrap();
        let ReadDecision::Streamed(file) = materialize_entry(&scan.entries[0], 1).unwrap() else {
            panic!();
        };
        fs::write(&path, vec![42; 2048]).unwrap();
        let ReadDecision::Streamed(file) = finish_materializing(file, 1024).unwrap() else {
            panic!("grown file must stream");
        };
        assert_eq!(file.descriptor.size_at_open, 2048);
        assert_eq!(file.remaining_attempts(), 2);
        assert!(file.descriptor.best_effort);
        test_hooks::clear();
    }

    #[test]
    fn regular_file_replaced_by_symlink_is_not_dereferenced() {
        let dir = TestDir::new();
        let path = dir.path().join("file");
        let target = dir.path().join("target");
        fs::write(&path, b"original").unwrap();
        fs::write(&target, b"other object").unwrap();
        let scan = scan_source_tree(dir.path()).unwrap();
        fs::remove_file(&path).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, &path).unwrap();
        #[cfg(windows)]
        if std::os::windows::fs::symlink_file(&target, &path).is_err() {
            return;
        }
        let ReadDecision::Skipped(entry) = materialize_entry(&scan.entries[0], 1024).unwrap()
        else {
            panic!("replacement symlink was dereferenced");
        };
        assert_eq!(entry.reason, SkipReason::OpenFailed);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn regular_file_replaced_by_fifo_is_skipped_without_waiting_for_writer() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::OpenOptionsExt;
        use std::sync::mpsc;
        use std::time::Duration;

        let dir = TestDir::new();
        let path = dir.path().join("file");
        fs::write(&path, b"original").unwrap();
        let entry = scan_source_tree(dir.path()).unwrap().entries.remove(0);
        fs::remove_file(&path).unwrap();
        let fifo_path = CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: CString хранит корректный NUL-terminated путь до конца вызова.
        assert_eq!(unsafe { libc::mkfifo(fifo_path.as_ptr(), 0o600) }, 0);

        let (result_tx, result_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            result_tx
                .send(materialize_owned_entry(entry, 1024))
                .unwrap();
        });
        let first_result = result_rx.recv_timeout(Duration::from_secs(3));
        let blocked = first_result.is_err();
        // При регрессии освобождаем все три open, чтобы тест завершился ошибкой,
        // а не оставил зависший поток. Linux разрешает O_RDWR FIFO без peer.
        let rescue = blocked.then(|| {
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(&path)
                .unwrap()
        });
        let result = first_result.unwrap_or_else(|_| {
            result_rx
                .recv_timeout(Duration::from_secs(3))
                .expect("FIFO reader did not exit after supplying a peer")
        });
        worker.join().unwrap();
        drop(rescue);
        assert!(!blocked, "opening a replacement FIFO waited for a writer");
        let ReadDecision::Skipped(entry) = result.unwrap() else {
            panic!("replacement FIFO was accepted as a regular file");
        };
        assert_eq!(entry.reason, SkipReason::OpenFailed);
    }
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Mutex, OnceLock};

    use super::*;

    static OPEN_FAILURES: OnceLock<Mutex<BTreeMap<String, usize>>> = OnceLock::new();
    static FORCED_SHORT_READS: OnceLock<Mutex<BTreeMap<String, VecDeque<u64>>>> = OnceLock::new();
    static FILESYSTEM_RESULTS: OnceLock<Mutex<BTreeMap<String, VecDeque<FilesystemResult>>>> =
        OnceLock::new();
    static HOOK_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    pub(crate) enum FilesystemResult {
        Allowed,
        Unsupported(ExcludedFilesystem),
        Failed,
    }

    fn filesystem_results() -> &'static Mutex<BTreeMap<String, VecDeque<FilesystemResult>>> {
        FILESYSTEM_RESULTS.get_or_init(|| Mutex::new(BTreeMap::new()))
    }

    fn open_failures() -> &'static Mutex<BTreeMap<String, usize>> {
        OPEN_FAILURES.get_or_init(|| Mutex::new(BTreeMap::new()))
    }

    fn forced_short_reads() -> &'static Mutex<BTreeMap<String, VecDeque<u64>>> {
        FORCED_SHORT_READS.get_or_init(|| Mutex::new(BTreeMap::new()))
    }

    fn key(path: &Path) -> String {
        path.to_string_lossy().into_owned()
    }

    pub(crate) fn lock() -> std::sync::MutexGuard<'static, ()> {
        HOOK_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    pub(crate) fn clear() {
        open_failures().lock().unwrap().clear();
        forced_short_reads().lock().unwrap().clear();
        filesystem_results().lock().unwrap().clear();
    }

    pub(crate) fn fail_next_open(path: &Path) {
        *open_failures()
            .lock()
            .unwrap()
            .entry(key(path))
            .or_default() += 1;
    }

    pub(crate) fn force_short_read_after_next_open(path: &Path, limit: u64) {
        forced_short_reads()
            .lock()
            .unwrap()
            .entry(key(path))
            .or_default()
            .push_back(limit);
    }

    pub(crate) fn classify_next_open(path: &Path, result: FilesystemResult) {
        filesystem_results()
            .lock()
            .unwrap()
            .entry(key(path))
            .or_default()
            .push_back(result);
    }

    pub(crate) fn remaining_filesystem_results(path: &Path) -> usize {
        filesystem_results()
            .lock()
            .unwrap()
            .get(&key(path))
            .map_or(0, VecDeque::len)
    }

    pub(crate) fn classify_file(
        path: &Path,
        file: &File,
    ) -> io::Result<Option<ExcludedFilesystem>> {
        let result = filesystem_results()
            .lock()
            .unwrap()
            .get_mut(&key(path))
            .and_then(VecDeque::pop_front);
        match result {
            Some(FilesystemResult::Allowed) => Ok(None),
            Some(FilesystemResult::Unsupported(filesystem)) => Ok(Some(filesystem)),
            Some(FilesystemResult::Failed) => Err(io::Error::other(
                "v4 test hook forced filesystem classification failure",
            )),
            None => source_policy::excluded_file(file),
        }
    }

    pub(crate) fn before_open(path: &Path) -> io::Result<()> {
        let mut failures = open_failures().lock().unwrap();
        let key = key(path);
        match failures.get_mut(&key) {
            Some(remaining) if *remaining > 0 => {
                *remaining -= 1;
                Err(io::Error::other("v4 test hook forced open failure"))
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn forced_short_read_after_open(path: &Path) -> Option<u64> {
        forced_short_reads()
            .lock()
            .unwrap()
            .get_mut(&key(path))
            .and_then(VecDeque::pop_front)
    }
}

#[cfg(not(test))]
pub(crate) mod test_hooks {
    use super::*;

    pub(crate) fn before_open(_path: &Path) -> io::Result<()> {
        Ok(())
    }

    pub(crate) fn forced_short_read_after_open(_path: &Path) -> Option<u64> {
        None
    }
}

#[cfg(test)]
fn scanned_batch(directory: &Path) -> [Option<ScannedEntry>; READ_BATCH_ENTRIES] {
    let mut entries = crate::sender::scan::scan_source_tree(directory)
        .unwrap()
        .entries
        .into_iter();
    assert!(entries.len() <= READ_BATCH_ENTRIES);
    std::array::from_fn(|_| entries.next())
}

use std::fs::{self, File};
use std::io::{self, Read};
use std::ops::{Deref, Range};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use anyhow::{Context, Result};

use crate::network::protocol::{BundleEntryDescriptor, BundleEntryKind, SkipReason, SkippedEntry};

use super::scan::{ScannedEntry, ScannedEntryKind};

pub(crate) const MAX_IMMEDIATE_ATTEMPTS: u32 = 3;
pub(crate) const READ_BATCH_ENTRIES: usize = 4;
#[cfg(test)]
pub(crate) const PREFETCH_FILE_BYTES: u64 = 256 * 1024;
pub(crate) const PREFETCH_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct PrefetchBudget {
    available: AtomicUsize,
}

impl PrefetchBudget {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            available: AtomicUsize::new(PREFETCH_PAYLOAD_BYTES),
        })
    }

    fn reserve_batch(
        self: &Arc<Self>,
        sizes: [usize; READ_BATCH_ENTRIES],
    ) -> ([bool; READ_BATCH_ENTRIES], Option<PrefetchReservation>) {
        let mut available = self.available.load(Ordering::Relaxed);
        loop {
            let mut reserved = 0;
            let selected = sizes.map(|size| {
                let fits = size != 0 && size <= available - reserved;
                if fits {
                    reserved += size;
                }
                fits
            });
            if reserved == 0 {
                return (selected, None);
            }
            match self.available.compare_exchange_weak(
                available,
                available - reserved,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return (
                        selected,
                        Some(PrefetchReservation {
                            budget: self.clone(),
                            bytes: reserved,
                            delivered: AtomicBool::new(false),
                        }),
                    );
                }
                Err(current) => available = current,
            }
        }
    }
}

#[derive(Debug)]
struct PrefetchReservation {
    budget: Arc<PrefetchBudget>,
    bytes: usize,
    delivered: AtomicBool,
}

impl PrefetchReservation {
    fn release(&mut self, bytes: usize) {
        self.bytes -= bytes;
        self.budget.available.fetch_add(bytes, Ordering::Relaxed);
    }

    fn deliver_slab(&self) {
        // Planner отдельно ограничивает собираемый bundle. После последней
        // выдачи slab больше не занимает read-ahead budget, даже если ещё живёт
        // в planner. Не ждём сжатия 64 MiB bundle при бюджете очереди 8 MiB.
        if !self.delivered.swap(true, Ordering::Relaxed) {
            self.budget
                .available
                .fetch_add(self.bytes, Ordering::Relaxed);
        }
    }
}

impl Drop for PrefetchReservation {
    fn drop(&mut self) {
        if !*self.delivered.get_mut() {
            self.budget
                .available
                .fetch_add(self.bytes, Ordering::Relaxed);
        }
    }
}

#[derive(Debug)]
pub(crate) struct SharedContents {
    bytes: Vec<u8>,
    // Пока последний элемент slab не передан planner, весь slab входит в budget,
    // включая уже выданный префикс. При отмене Drop освобождает reservation.
    _reservation: Option<PrefetchReservation>,
}

impl Deref for SharedContents {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

#[derive(Debug, Clone)]
pub(crate) enum EntryContents {
    Owned(Vec<u8>),
    Shared {
        bytes: Arc<SharedContents>,
        range: Range<usize>,
    },
}

impl EntryContents {
    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(bytes) => bytes,
            Self::Shared { bytes, range } => &bytes[range.clone()],
        }
    }

    pub(crate) fn delivered_from_prefetch(&self) {
        // Slab заполняется в порядке обхода и выдаётся в том же порядке.
        // Последний непустой slice освобождает budget одной атомарной операцией
        // на batch; предыдущим slices не нужен отдельный atomic counter.
        if let Self::Shared { bytes, range } = self
            && range.end == bytes.len()
            && let Some(reservation) = &bytes._reservation
        {
            reservation.deliver_slab();
        }
    }
}

impl From<Vec<u8>> for EntryContents {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Owned(bytes)
    }
}

impl Deref for EntryContents {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for EntryContents {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<T: AsRef<[u8]> + ?Sized> PartialEq<T> for EntryContents {
    fn eq(&self, other: &T) -> bool {
        self.as_slice() == other.as_ref()
    }
}

impl Eq for EntryContents {}

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

impl StreamedRegularFile {
    pub(crate) fn remaining_attempts(&self) -> u32 {
        MAX_IMMEDIATE_ATTEMPTS.saturating_sub(self.open_attempts) + 1
    }

    pub(crate) fn take_forced_short_read_limit(&mut self) -> Option<u64> {
        self.forced_short_read_limit.take()
    }

    pub(crate) fn reopen_for_retry(&mut self) -> Result<()> {
        anyhow::ensure!(
            self.open_attempts < MAX_IMMEDIATE_ATTEMPTS,
            "file attempt limit exhausted"
        );
        self.open_attempts += 1;
        let (file, metadata, forced_short_read_limit) = open_regular_file_attempt(&self.abs_path)?;
        self.descriptor.size_at_open = metadata.len();
        self.descriptor.best_effort = true;
        self.mode = regular_mode(&metadata);
        self.file = file;
        self.forced_short_read_limit = forced_short_read_limit;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) enum ReadDecision {
    Accepted(MaterializedEntry),
    Streamed(StreamedRegularFile),
    Skipped(SkippedEntry),
}

pub(crate) fn materialize_owned_entry(
    entry: ScannedEntry,
    stream_threshold_bytes: u64,
) -> Result<ReadDecision> {
    match entry.kind {
        ScannedEntryKind::Regular => materialize_regular_file(entry, stream_threshold_bytes),
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
) -> Result<ReadDecision> {
    for attempt in 1..=MAX_IMMEDIATE_ATTEMPTS {
        let (mut file, metadata, forced_short_read_limit) =
            match open_regular_file_attempt(&entry.abs_path) {
                Ok(result) => result,
                Err(_) if attempt == MAX_IMMEDIATE_ATTEMPTS => {
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
pub(crate) fn finish_materializing(
    mut entry: StreamedRegularFile,
    stream_threshold_bytes: u64,
) -> Result<ReadDecision> {
    loop {
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
        if let Some(reason) = reopen_after_short_read(&mut entry) {
            return Ok(skipped_regular(entry, reason));
        }
    }
}

fn reopen_after_short_read(entry: &mut StreamedRegularFile) -> Option<SkipReason> {
    if entry.open_attempts == MAX_IMMEDIATE_ATTEMPTS {
        return Some(SkipReason::UnstableAfterRetries);
    }
    loop {
        if entry.reopen_for_retry().is_ok() {
            return None;
        }
        if entry.open_attempts == MAX_IMMEDIATE_ATTEMPTS {
            return Some(SkipReason::OpenFailed);
        }
    }
}

pub(crate) type ReadBatch = [Option<Result<ReadDecision>>; READ_BATCH_ENTRIES];

#[cfg(test)]
pub(crate) fn materialize_batch(
    entries: [Option<ScannedEntry>; READ_BATCH_ENTRIES],
    stream_threshold_bytes: u64,
) -> ReadBatch {
    finish_read_batch(open_read_batch(entries), stream_threshold_bytes)
}

pub(crate) fn materialize_budgeted_batch(
    entries: [Option<ScannedEntry>; READ_BATCH_ENTRIES],
    stream_threshold_bytes: u64,
    budget: &Arc<PrefetchBudget>,
) -> ReadBatch {
    finish_read_batch_with_budget(
        open_read_batch(entries),
        stream_threshold_bytes,
        Some(budget),
    )
}

fn open_read_batch(entries: [Option<ScannedEntry>; READ_BATCH_ENTRIES]) -> ReadBatch {
    // Порог 0 сохраняет открытый handle каждого непустого файла. Суммируем
    // размеры именно этих handles; отдельный stat и повторный open не нужны.
    entries.map(|entry| entry.map(|entry| materialize_owned_entry(entry, 0)))
}

#[cfg(test)]
fn finish_read_batch(outcomes: ReadBatch, stream_threshold_bytes: u64) -> ReadBatch {
    finish_read_batch_with_budget(
        outcomes,
        stream_threshold_bytes.min(PREFETCH_FILE_BYTES),
        None,
    )
}

fn finish_read_batch_with_budget(
    mut outcomes: ReadBatch,
    stream_threshold_bytes: u64,
    budget: Option<&Arc<PrefetchBudget>>,
) -> ReadBatch {
    struct SharedEntry {
        descriptor: BundleEntryDescriptor,
        mode: u32,
        range: Range<usize>,
    }

    let sizes = std::array::from_fn(|index| match &outcomes[index] {
        Some(Ok(ReadDecision::Streamed(file)))
            if file.descriptor.size_at_open <= stream_threshold_bytes =>
        {
            usize::try_from(file.descriptor.size_at_open).unwrap_or(usize::MAX)
        }
        _ => 0,
    });
    // Одна reservation на batch. Никогда не ждём память: planner может держать
    // предыдущие slabs до сжатия bundle; ожидание здесь создало бы взаимную блокировку.
    let (selected, mut reservation) = match budget {
        Some(budget) => budget.reserve_batch(sizes),
        None => (sizes.map(|size| size != 0), None),
    };
    let mut reserved: usize = sizes
        .iter()
        .zip(selected)
        .filter_map(|(&size, selected)| selected.then_some(size))
        .sum();
    let mut bytes = Vec::with_capacity(reserved);
    let mut shared_entries: [Option<SharedEntry>; READ_BATCH_ENTRIES] =
        std::array::from_fn(|_| None);
    for (index, outcome) in outcomes.iter_mut().enumerate() {
        if !selected[index] {
            continue;
        }
        let Some(Ok(ReadDecision::Streamed(mut file))) = outcome.take() else {
            unreachable!();
        };
        let start = bytes.len();
        let forced_limit = file.take_forced_short_read_limit();
        match append_contents(
            &mut file.file,
            file.descriptor.size_at_open,
            forced_limit,
            &mut bytes,
        ) {
            Ok(range) => {
                shared_entries[index] = Some(SharedEntry {
                    descriptor: file.descriptor,
                    mode: file.mode,
                    range,
                });
            }
            Err(_) => {
                bytes.truncate(start);
                reserved -= file.descriptor.size_at_open as usize;
                // Освобождаем reservation до Owned retry, чтобы оба payload
                // одновременно не удваивали ограниченное окно prefetch.
                bytes.shrink_to(reserved);
                if let Some(reservation) = &mut reservation {
                    reservation.release(file.descriptor.size_at_open as usize);
                }
                // Retry не меняет уже заполненный slab. Его новый size_at_open
                // может вырасти; редкий повтор читает в отдельный Owned payload.
                *outcome = Some(match reopen_after_short_read(&mut file) {
                    Some(reason) => Ok(skipped_regular(file, reason)),
                    // После изменения размера planner продолжит уже открытую
                    // попытку; worker не выходит за общую reservation новым payload.
                    None if budget.is_some() => Ok(ReadDecision::Streamed(file)),
                    None => finish_materializing(file, stream_threshold_bytes),
                });
            }
        }
    }
    if bytes.len() != reserved {
        // После ошибок маленький успешный файл не должен удерживать reservation
        // больших неудачных соседей через границы bundles. Только этот путь
        // допускает перемещение данных при уменьшении allocation.
        bytes.shrink_to_fit();
    }
    if !bytes.is_empty() {
        // Arc<Vec> переносит allocation без копии payload; Arc<[u8]> скопировал бы её.
        let bytes = Arc::new(SharedContents {
            bytes,
            _reservation: reservation,
        });
        for (outcome, entry) in outcomes.iter_mut().zip(shared_entries) {
            if let Some(entry) = entry {
                *outcome = Some(Ok(ReadDecision::Accepted(MaterializedEntry {
                    descriptor: entry.descriptor,
                    mode: entry.mode,
                    contents: EntryContents::Shared {
                        bytes: bytes.clone(),
                        range: entry.range,
                    },
                })));
            }
        }
    }
    outcomes
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

fn open_regular_file_attempt(path: &Path) -> Result<(File, fs::Metadata, Option<u64>)> {
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
    Ok((file, metadata, forced_short_read_limit))
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

    #[cfg(windows)]
    #[test]
    fn normal_reopen_preserves_file_after_rename_and_path_replacement() {
        let dir = TestDir::new();
        let path = dir.path().join("file");
        let renamed = dir.path().join("renamed");
        fs::write(&path, b"original").unwrap();
        let (mut original, _, _) = open_regular_file_attempt(&path).unwrap();
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
        let (original, _, _) = open_regular_file_attempt(&path).unwrap();
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

    fn scanned_batch(directory: &Path) -> [Option<ScannedEntry>; READ_BATCH_ENTRIES] {
        let mut entries = scan_source_tree(directory).unwrap().entries.into_iter();
        assert!(entries.len() <= READ_BATCH_ENTRIES);
        std::array::from_fn(|_| entries.next())
    }

    fn accepted(outcome: Option<Result<ReadDecision>>) -> MaterializedEntry {
        let ReadDecision::Accepted(entry) = outcome.unwrap().unwrap() else {
            panic!("batch omitted a readable file");
        };
        entry
    }

    #[test]
    fn medium_prefetch_budget_transfers_to_planner_and_releases_on_cancel() {
        let dir = TestDir::new();
        const SIZE: usize = 300_000;
        for index in 0..4 {
            fs::write(dir.path().join(format!("{index}")), vec![index as u8; SIZE]).unwrap();
        }
        let budget = Arc::new(PrefetchBudget {
            available: AtomicUsize::new(2 * SIZE),
        });
        let [first, second, third, fourth] =
            materialize_budgeted_batch(scanned_batch(dir.path()), 1024 * 1024, &budget);
        let first = accepted(first);
        let second = accepted(second);
        assert_eq!(first.contents, vec![0; SIZE]);
        assert_eq!(second.contents, vec![1; SIZE]);
        assert_eq!(budget.available.load(Ordering::Relaxed), 0);
        let ReadDecision::Streamed(third) = third.unwrap().unwrap() else {
            panic!("prefetch exceeded its shared byte budget");
        };
        assert_eq!(third.open_attempts, 1);
        assert!(matches!(fourth, Some(Ok(ReadDecision::Streamed(_)))));
        fs::OpenOptions::new()
            .append(true)
            .open(dir.path().join("2"))
            .unwrap()
            .write_all(b"appended-tail")
            .unwrap();
        first.contents.delivered_from_prefetch();
        assert_eq!(budget.available.load(Ordering::Relaxed), 0);
        second.contents.delivered_from_prefetch();
        assert_eq!(budget.available.load(Ordering::Relaxed), 2 * SIZE);
        assert_eq!(first.contents, vec![0; SIZE]);
        assert_eq!(second.contents, vec![1; SIZE]);
        drop(first);
        drop(second);
        assert_eq!(budget.available.load(Ordering::Relaxed), 2 * SIZE);
        let ReadDecision::Accepted(third) = finish_materializing(third, 1024 * 1024).unwrap()
        else {
            panic!("fallback lost the previously opened file");
        };
        assert_eq!(third.contents, vec![2; SIZE]);

        let pending = materialize_budgeted_batch(scanned_batch(dir.path()), 1024 * 1024, &budget);
        assert_eq!(budget.available.load(Ordering::Relaxed), 0);
        drop(pending);
        assert_eq!(budget.available.load(Ordering::Relaxed), 2 * SIZE);
    }

    #[test]
    fn budgeted_short_read_releases_payload_without_resetting_attempts() {
        let _guard = test_hooks::lock();
        test_hooks::clear();
        let dir = TestDir::new();
        let path = dir.path().join("file");
        fs::write(&path, vec![42; 300_000]).unwrap();
        for _ in 0..3 {
            test_hooks::force_short_read_after_next_open(&path, 2);
        }
        let budget = PrefetchBudget::new();
        let [file, _, _, _] =
            materialize_budgeted_batch(scanned_batch(dir.path()), 1024 * 1024, &budget);
        let ReadDecision::Streamed(file) = file.unwrap().unwrap() else {
            panic!("failed prefetch must defer the reopened attempt");
        };
        assert_eq!(file.open_attempts, 2);
        assert_eq!(
            budget.available.load(Ordering::Relaxed),
            PREFETCH_PAYLOAD_BYTES
        );
        let ReadDecision::Skipped(skipped) = finish_materializing(file, 1024 * 1024).unwrap()
        else {
            panic!("prefetch reset the shared three-attempt limit");
        };
        assert_eq!(skipped.reason, SkipReason::UnstableAfterRetries);
        test_hooks::clear();
    }

    #[test]
    fn batch_shares_exact_payload_allocation_and_outlives_other_entries() {
        let dir = TestDir::new();
        for (name, contents) in [
            ("a", b"abc".as_slice()),
            ("b", b""),
            ("c", b"12345"),
            ("d", b"tail"),
        ] {
            fs::write(dir.path().join(name), contents).unwrap();
        }
        let mut entries = materialize_batch(scanned_batch(dir.path()), 1024).map(accepted);
        let EntryContents::Shared { bytes, range } = &entries[0].contents else {
            panic!("nonempty batch did not share payload");
        };
        assert_eq!(range, &(0..3));
        assert_eq!(bytes.len(), 12);
        assert_eq!(bytes.capacity(), 12);
        let weak = Arc::downgrade(bytes);
        for (index, expected) in [(2, b"12345".as_slice()), (3, b"tail")] {
            let EntryContents::Shared {
                bytes: other,
                range,
            } = &entries[index].contents
            else {
                panic!("batch allocated per-file payload");
            };
            assert!(Arc::ptr_eq(bytes, other));
            assert_eq!(&other[range.clone()], expected);
        }
        assert!(matches!(&entries[1].contents, EntryContents::Owned(bytes) if bytes.is_empty()));
        let retained = std::mem::replace(&mut entries[2].contents, Vec::new().into());
        drop(entries);
        assert_eq!(retained, b"12345");
        assert_eq!(weak.strong_count(), 1);
        drop(retained);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn batch_snapshot_ignores_append_and_retry_growth_uses_fresh_open_size() {
        let _guard = test_hooks::lock();
        test_hooks::clear();
        let dir = TestDir::new();
        for name in ["a", "b", "c", "d"] {
            fs::write(dir.path().join(name), b"original").unwrap();
        }
        test_hooks::force_short_read_after_next_open(&dir.path().join("b"), 2);
        let opened = open_read_batch(scanned_batch(dir.path()));
        fs::OpenOptions::new()
            .append(true)
            .open(dir.path().join("a"))
            .unwrap()
            .write_all(b"-ignored-tail")
            .unwrap();
        fs::write(dir.path().join("b"), vec![0x5a; 2048]).unwrap();
        fs::write(dir.path().join("c"), b"tiny").unwrap();
        let [first, grown, shrunk, last] = finish_read_batch(opened, 1024);
        let first = accepted(first);
        let last = accepted(last);
        assert_eq!(first.contents, b"original");
        assert_eq!(first.descriptor.size_at_open, 8);
        assert_eq!(last.contents, b"original");
        let EntryContents::Shared { bytes, .. } = &first.contents else {
            panic!();
        };
        assert_eq!(bytes.capacity(), 16);
        let ReadDecision::Streamed(mut grown) = grown.unwrap().unwrap() else {
            panic!("retry growth must defer large payload");
        };
        assert_eq!(grown.open_attempts, 2);
        assert_eq!(grown.remaining_attempts(), 2);
        assert_eq!(grown.descriptor.size_at_open, 2048);
        assert!(grown.descriptor.best_effort);
        assert_eq!(
            read_contents(&mut grown.file, 2048, None).unwrap(),
            vec![0x5a; 2048]
        );
        let shrunk = accepted(shrunk);
        assert_eq!(shrunk.contents, b"tiny");
        assert_eq!(shrunk.descriptor.size_at_open, 4);
        assert!(shrunk.descriptor.best_effort);
        assert!(matches!(shrunk.contents, EntryContents::Owned(_)));
        test_hooks::clear();
    }

    #[test]
    fn batch_errors_preserve_three_attempt_budget_and_release_unused_capacity() {
        let _guard = test_hooks::lock();
        test_hooks::clear();
        let dir = TestDir::new();
        for name in ["a", "b", "c"] {
            fs::write(
                dir.path().join(name),
                vec![0xa5; PREFETCH_FILE_BYTES as usize],
            )
            .unwrap();
        }
        fs::write(dir.path().join("d"), b"ok").unwrap();
        let first = dir.path().join("a");
        test_hooks::fail_next_open(&first);
        test_hooks::fail_next_open(&first);
        test_hooks::force_short_read_after_next_open(&first, 2);
        let second = dir.path().join("b");
        for limit in [2, 3, 4, 99] {
            test_hooks::force_short_read_after_next_open(&second, limit);
        }
        let third = dir.path().join("c");
        for _ in 0..3 {
            test_hooks::fail_next_open(&third);
        }
        let [first, second_result, third, last] =
            materialize_batch(scanned_batch(dir.path()), PREFETCH_FILE_BYTES);
        for (outcome, reason) in [
            (first, SkipReason::UnstableAfterRetries),
            (second_result, SkipReason::UnstableAfterRetries),
            (third, SkipReason::OpenFailed),
        ] {
            let ReadDecision::Skipped(skipped) = outcome.unwrap().unwrap() else {
                panic!("batch retried an exhausted file");
            };
            assert_eq!(skipped.reason, reason);
        }
        assert_eq!(test_hooks::forced_short_read_after_open(&second), Some(99));
        let last = accepted(last);
        assert_eq!(last.contents, b"ok");
        let EntryContents::Shared { bytes, .. } = last.contents else {
            panic!();
        };
        assert_eq!(bytes.capacity(), 2);
        test_hooks::clear();
    }

    #[test]
    fn batch_prefetch_cap_defers_large_handles_and_preserves_empty_entries() {
        let dir = TestDir::new();
        fs::write(dir.path().join("a"), []).unwrap();
        fs::write(
            dir.path().join("b"),
            vec![0x31; PREFETCH_FILE_BYTES as usize],
        )
        .unwrap();
        fs::write(
            dir.path().join("c"),
            vec![0x32; PREFETCH_FILE_BYTES as usize + 1],
        )
        .unwrap();
        let [empty, small, large, tail] = materialize_batch(scanned_batch(dir.path()), u64::MAX);
        assert!(accepted(empty).contents.is_empty());
        let small = accepted(small);
        assert_eq!(small.contents.len(), PREFETCH_FILE_BYTES as usize);
        let EntryContents::Shared { bytes, .. } = small.contents else {
            panic!();
        };
        assert_eq!(bytes.capacity(), PREFETCH_FILE_BYTES as usize);
        let ReadDecision::Streamed(file) = large.unwrap().unwrap() else {
            panic!();
        };
        assert_eq!(file.open_attempts, 1);
        assert_eq!(file.descriptor.size_at_open, PREFETCH_FILE_BYTES + 1);
        assert!(tail.is_none());
    }

    #[test]
    fn batch_keeps_symlink_target_without_reading_its_contents() {
        let dir = TestDir::new();
        fs::write(dir.path().join("a"), b"target bytes").unwrap();
        let link = dir.path().join("b-link");
        #[cfg(unix)]
        std::os::unix::fs::symlink("a", &link).unwrap();
        #[cfg(windows)]
        if std::os::windows::fs::symlink_file("a", &link).is_err() {
            return;
        }
        let [file, symlink, tail, last] = materialize_batch(scanned_batch(dir.path()), 1024);
        assert_eq!(accepted(file).contents, b"target bytes");
        let symlink = accepted(symlink);
        assert_eq!(
            symlink.descriptor.kind,
            BundleEntryKind::Symlink { target: "a".into() }
        );
        assert_eq!(symlink.descriptor.size_at_open, 0);
        assert!(matches!(&symlink.contents, EntryContents::Owned(bytes) if bytes.is_empty()));
        assert!(tail.is_none());
        assert!(last.is_none());
    }
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Mutex, OnceLock};

    use super::*;

    static OPEN_FAILURES: OnceLock<Mutex<BTreeMap<String, usize>>> = OnceLock::new();
    static FORCED_SHORT_READS: OnceLock<Mutex<BTreeMap<String, VecDeque<u64>>>> = OnceLock::new();
    static HOOK_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

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

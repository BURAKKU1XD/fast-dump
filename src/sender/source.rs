use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use log::debug;

use crate::network::protocol::ResumeState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InputFile {
    pub(crate) index: u64,
    pub(crate) abs_path: PathBuf,
    pub(crate) rel_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceScanReport {
    pub pending_files: usize,
    pub pending_bytes: u64,
    pub final_file_index: u64,
    pub final_file_path: String,
}

#[derive(Debug)]
struct DirectoryFrame {
    entries: Vec<DiscoveredEntry>,
}

#[derive(Debug)]
struct DiscoveredEntry {
    rel_path: String,
    abs_path: PathBuf,
    file_type: fs::FileType,
}

#[derive(Debug)]
pub(crate) struct PendingFileWalker {
    source: PathBuf,
    resume_state: ResumeState,
    stack: Vec<DirectoryFrame>,
    next_index: u64,
    last_discovered_file: Option<InputFile>,
    resume_validated: bool,
    exhausted: bool,
}

#[cfg(test)]
mod test_hooks {
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};

    static FS_FAILURES: OnceLock<Mutex<BTreeMap<String, usize>>> = OnceLock::new();

    fn fs_failures() -> &'static Mutex<BTreeMap<String, usize>> {
        FS_FAILURES.get_or_init(|| Mutex::new(BTreeMap::new()))
    }

    fn path_key(path: &Path) -> String {
        path.to_string_lossy().into_owned()
    }

    fn read_dir_key(path: &Path) -> String {
        format!("read_dir:{}", path_key(path))
    }

    fn metadata_key(path: &Path) -> String {
        format!("metadata:{}", path_key(path))
    }

    fn maybe_fail(key: String) -> std::io::Result<()> {
        let mut failures = fs_failures().lock().unwrap();
        let Some(remaining) = failures.get_mut(&key) else {
            return Ok(());
        };

        *remaining = remaining.saturating_sub(1);
        if *remaining == 0 {
            failures.remove(&key);
        }

        Err(std::io::Error::other(format!(
            "sender test hook injected failure for {key}"
        )))
    }

    pub(crate) fn fail_next_read_dir(path: &Path) {
        fs_failures().lock().unwrap().insert(read_dir_key(path), 1);
    }

    pub(crate) fn fail_next_metadata(path: &Path) {
        fs_failures().lock().unwrap().insert(metadata_key(path), 1);
    }

    pub(crate) fn before_read_dir(path: &Path) -> std::io::Result<()> {
        maybe_fail(read_dir_key(path))
    }

    pub(crate) fn before_metadata(path: &Path) -> std::io::Result<()> {
        maybe_fail(metadata_key(path))
    }

    pub(crate) fn clear() {
        fs_failures().lock().unwrap().clear();
    }
}

#[cfg(not(test))]
mod test_hooks {
    use std::path::Path;

    pub(crate) fn before_read_dir(_path: &Path) -> std::io::Result<()> {
        Ok(())
    }

    pub(crate) fn before_metadata(_path: &Path) -> std::io::Result<()> {
        Ok(())
    }
}

fn read_dir_path(path: &Path) -> std::io::Result<fs::ReadDir> {
    test_hooks::before_read_dir(path)?;
    fs::read_dir(path)
}

fn metadata_path(path: &Path) -> std::io::Result<fs::Metadata> {
    test_hooks::before_metadata(path)?;
    fs::metadata(path)
}

fn validate_source_directory(source: &Path) -> Result<()> {
    let metadata = metadata_path(source)
        .with_context(|| format!("failed to inspect sender source {}", source.display()))?;
    if !metadata.is_dir() {
        bail!("sender source must be a directory: {}", source.display());
    }
    Ok(())
}

// Этот helper оставлен для диагностического materialized scan:
// production sender path и buffered prepare_dump больше не зависят от полного pending-list.
pub(crate) fn collect_pending_files(
    source: impl Into<PathBuf>,
    resume_state: &ResumeState,
    threads: u32,
    frame_target_bytes: u64,
) -> Result<(Vec<InputFile>, u64, String)> {
    if threads == 0 {
        bail!("sender threads must be at least 1");
    }
    if frame_target_bytes == 0 {
        bail!("frame target bytes must be at least 1");
    }

    let source = source.into();
    let discovery_started = Instant::now();
    validate_source_directory(&source)?;

    let files = discover_files(&source)?;
    validate_resume_state_against_files(&files, resume_state)?;
    let discovered_files = files.len();
    let final_file_index = files
        .last()
        .map(|file| file.index)
        .unwrap_or(resume_state.last_committed_file_index);
    let final_file_path = files
        .last()
        .map(|file| file.rel_path.clone())
        .unwrap_or_else(|| resume_state.last_committed_file_path.clone());
    let pending = files
        .into_iter()
        .filter(|file| file.index > resume_state.last_committed_file_index)
        .collect();
    debug!(
        "sender source scanned: source={} discovered_files={} resume_file_index={} elapsed_ms={}",
        source.display(),
        discovered_files,
        resume_state.last_committed_file_index,
        discovery_started.elapsed().as_millis()
    );

    Ok((pending, final_file_index, final_file_path))
}

impl PendingFileWalker {
    pub(crate) fn new(source: impl Into<PathBuf>, resume_state: &ResumeState) -> Result<Self> {
        let source = source.into();
        validate_source_directory(&source)?;
        let root_entries = read_sorted_entries(&source, &source)?;
        Ok(Self {
            source,
            resume_state: resume_state.clone(),
            stack: vec![DirectoryFrame {
                entries: root_entries,
            }],
            next_index: 1,
            last_discovered_file: None,
            resume_validated: resume_state.last_committed_file_index == 0,
            exhausted: false,
        })
    }

    // Продакшен-путь получает файлы по одному и начинает frame assembly,
    // как только пройден committed prefix. Это убирает необходимость в полном pending-list.
    pub(crate) fn next_pending_file(&mut self) -> Result<Option<InputFile>> {
        if self.exhausted {
            return Ok(None);
        }

        loop {
            let Some(entry) = self.next_entry()? else {
                self.finish_walk()?;
                return Ok(None);
            };

            if entry.file_type.is_dir() {
                let entries = read_sorted_entries(&self.source, &entry.abs_path)?;
                self.stack.push(DirectoryFrame { entries });
                continue;
            }

            if !entry.file_type.is_file() {
                eprintln!(
                    "warning: skipping non-regular entry {}",
                    entry.abs_path.display()
                );
                continue;
            }

            let file = InputFile {
                index: self.next_index,
                abs_path: entry.abs_path,
                rel_path: entry.rel_path,
            };
            self.next_index += 1;
            self.last_discovered_file = Some(file.clone());

            if file.index < self.resume_state.last_committed_file_index {
                continue;
            }

            if file.index == self.resume_state.last_committed_file_index {
                if file.rel_path != self.resume_state.last_committed_file_path {
                    bail!(
                        "sender source no longer matches resume state at file index {}: expected {} got {}",
                        self.resume_state.last_committed_file_index,
                        self.resume_state.last_committed_file_path,
                        file.rel_path
                    );
                }
                self.resume_validated = true;
                continue;
            }

            return Ok(Some(file));
        }
    }

    pub(crate) fn final_boundary(&self) -> Result<(u64, String)> {
        if !self.exhausted {
            bail!("sender pending file walker has not finished discovery yet");
        }
        Ok(self.current_boundary())
    }

    pub(crate) fn last_discovered_boundary(&self) -> (u64, String) {
        self.current_boundary()
    }

    fn next_entry(&mut self) -> Result<Option<DiscoveredEntry>> {
        loop {
            let Some(frame) = self.stack.last_mut() else {
                return Ok(None);
            };
            if frame.entries.is_empty() {
                self.stack.pop();
                continue;
            }
            let Some(entry) = frame.entries.pop() else {
                bail!("sender source walker lost a directory entry while iterating");
            };
            return Ok(Some(entry));
        }
    }

    fn finish_walk(&mut self) -> Result<()> {
        self.exhausted = true;
        if !self.resume_validated {
            bail!(
                "sender source no longer matches resume state: committed file index {} is missing from current tree with {} files",
                self.resume_state.last_committed_file_index,
                self.next_index.saturating_sub(1)
            );
        }
        Ok(())
    }

    fn current_boundary(&self) -> (u64, String) {
        self.last_discovered_file
            .as_ref()
            .map(|file| (file.index, file.rel_path.clone()))
            .unwrap_or_else(|| {
                (
                    self.resume_state.last_committed_file_index,
                    self.resume_state.last_committed_file_path.clone(),
                )
            })
    }
}

// Этот публичный helper нужен benchmark-ам и диагностике:
// он изолирует deterministic scan/resume-валидацию от чтения файлов и компрессии.
pub fn scan_source(
    source: impl Into<PathBuf>,
    resume_state: &ResumeState,
) -> Result<SourceScanReport> {
    let (pending, final_file_index, final_file_path) =
        collect_pending_files(source, resume_state, 1, 1)?;
    // Для диагностического scan-отчёта размер можно собрать отдельно,
    // но sender не использует эту цифру как источник истины для tar.
    let pending_bytes = pending.iter().try_fold(0_u64, |total, file| {
        let size = metadata_path(&file.abs_path)
            .with_context(|| {
                format!(
                    "failed to read sender file metadata for scan report {}",
                    file.abs_path.display()
                )
            })?
            .len();
        Ok::<_, anyhow::Error>(total.saturating_add(size))
    })?;
    Ok(SourceScanReport {
        pending_files: pending.len(),
        pending_bytes,
        final_file_index,
        final_file_path,
    })
}

fn validate_resume_state_against_files(
    files: &[InputFile],
    resume_state: &ResumeState,
) -> Result<()> {
    if resume_state.last_committed_file_index == 0 {
        return Ok(());
    }

    let committed_offset = resume_state.last_committed_file_index as usize - 1;
    let Some(committed_file) = files.get(committed_offset) else {
        bail!(
            "sender source no longer matches resume state: committed file index {} is missing from current tree with {} files",
            resume_state.last_committed_file_index,
            files.len()
        );
    };

    if committed_file.rel_path != resume_state.last_committed_file_path {
        bail!(
            "sender source no longer matches resume state at file index {}: expected {} got {}",
            resume_state.last_committed_file_index,
            resume_state.last_committed_file_path,
            committed_file.rel_path
        );
    }

    Ok(())
}

fn read_sorted_entries(source: &Path, dir: &Path) -> Result<Vec<DiscoveredEntry>> {
    let mut entries = Vec::new();
    for entry in read_dir_path(dir)
        .with_context(|| format!("failed to read sender directory {}", dir.display()))?
    {
        let entry = entry
            .with_context(|| format!("failed to enumerate sender directory {}", dir.display()))?;
        let path = entry.path();
        let rel_path = normalize_relative_path(source, &path)?;
        let file_type = entry.file_type().with_context(|| {
            format!(
                "failed to inspect sender entry type {}",
                entry.path().display()
            )
        })?;
        entries.push(DiscoveredEntry {
            rel_path,
            abs_path: path,
            file_type,
        });
    }
    entries.sort_by(|left, right| left.rel_path.cmp(&right.rel_path));
    entries.reverse();
    Ok(entries)
}

fn discover_files(source: &Path) -> Result<Vec<InputFile>> {
    let mut files = Vec::new();
    let mut next_index = 1_u64;
    collect_files_recursive(source, source, &mut next_index, &mut files)?;
    Ok(files)
}

fn collect_files_recursive(
    source: &Path,
    dir: &Path,
    next_index: &mut u64,
    files: &mut Vec<InputFile>,
) -> Result<()> {
    let mut entries = Vec::new();
    for entry in read_dir_path(dir)
        .with_context(|| format!("failed to read sender directory {}", dir.display()))?
    {
        let entry = entry
            .with_context(|| format!("failed to enumerate sender directory {}", dir.display()))?;
        let path = entry.path();
        let rel_path = normalize_relative_path(source, &path)?;
        let file_type = entry.file_type().with_context(|| {
            format!(
                "failed to inspect sender entry type {}",
                entry.path().display()
            )
        })?;
        entries.push((rel_path, path, file_type));
    }
    entries.sort_by(|left, right| left.0.cmp(&right.0));

    for (rel_path, path, file_type) in entries {
        if file_type.is_dir() {
            collect_files_recursive(source, &path, next_index, files)?;
            continue;
        }
        if file_type.is_file() {
            files.push(InputFile {
                index: *next_index,
                abs_path: path,
                rel_path,
            });
            *next_index += 1;
            continue;
        }

        eprintln!("warning: skipping non-regular entry {}", path.display());
    }

    Ok(())
}

fn normalize_relative_path(source: &Path, path: &Path) -> Result<String> {
    let relative = path.strip_prefix(source).with_context(|| {
        format!(
            "sender path {} is not inside source {}",
            path.display(),
            source.display()
        )
    })?;
    let mut segments = Vec::new();
    for component in relative.components() {
        match component {
            std::path::Component::Normal(value) => {
                segments.push(value.to_string_lossy().into_owned());
            }
            _ => bail!(
                "sender path must stay within source root: {}",
                path.display()
            ),
        }
    }
    if segments.is_empty() {
        bail!("sender relative path must not be empty: {}", path.display());
    }
    Ok(segments.join("/"))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        InputFile, PendingFileWalker, collect_pending_files, discover_files,
        normalize_relative_path, scan_source,
    };
    use crate::network::protocol::ResumeState;

    struct TestDir {
        path: PathBuf,
    }

    struct FsFailureGuard {
        _lock: MutexGuard<'static, ()>,
    }

    impl TestDir {
        fn new() -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "fast-dump-sender-source-tests-{}-{}",
                std::process::id(),
                unique
            ));
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    impl FsFailureGuard {
        fn new() -> Self {
            static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
            let lock = LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
            super::test_hooks::clear();
            Self { _lock: lock }
        }
    }

    impl Drop for FsFailureGuard {
        fn drop(&mut self) {
            super::test_hooks::clear();
        }
    }

    fn write_input_file(temp: &TestDir, rel_path: &str, bytes: &[u8], index: u64) -> InputFile {
        let path = temp.path().join(rel_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&path, bytes).unwrap();
        InputFile {
            index,
            abs_path: path,
            rel_path: rel_path.replace('\\', "/"),
        }
    }

    fn empty_resume_state() -> ResumeState {
        ResumeState {
            last_committed_frame: 0,
            last_committed_file_index: 0,
            last_committed_file_path: String::new(),
            archive_len: 0,
        }
    }

    #[cfg(unix)]
    fn create_symlink(path: &Path, target: &Path) -> std::io::Result<()> {
        std::os::unix::fs::symlink(target, path)
    }

    #[cfg(windows)]
    fn create_symlink(path: &Path, target: &Path) -> std::io::Result<()> {
        std::os::windows::fs::symlink_file(target, path)
    }

    #[test]
    fn collect_pending_files_rejects_missing_source_directory() {
        let temp = TestDir::new();
        let missing = temp.path().join("missing");

        let error = collect_pending_files(missing, &empty_resume_state(), 1, 1).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to inspect sender source")
        );
    }

    #[test]
    fn collect_pending_files_rejects_file_source() {
        let temp = TestDir::new();
        let source_file = temp.path().join("single.bin");
        fs::write(&source_file, b"abc").unwrap();

        let error = collect_pending_files(source_file, &empty_resume_state(), 1, 1).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("sender source must be a directory")
        );
    }

    #[test]
    fn collect_pending_files_returns_depth_first_sorted_order() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("b").join("nested")).unwrap();
        fs::create_dir_all(temp.path().join("a")).unwrap();
        fs::write(temp.path().join("b").join("second.txt"), b"222").unwrap();
        fs::write(
            temp.path().join("b").join("nested").join("third.txt"),
            b"333",
        )
        .unwrap();
        fs::write(temp.path().join("a").join("first.txt"), b"111").unwrap();
        fs::write(temp.path().join("root.txt"), b"444").unwrap();

        let (pending, final_file_index, final_file_path) =
            collect_pending_files(temp.path().to_path_buf(), &empty_resume_state(), 1, 1).unwrap();

        let rel_paths: Vec<_> = pending.iter().map(|file| file.rel_path.as_str()).collect();
        assert_eq!(
            rel_paths,
            vec![
                "a/first.txt",
                "b/nested/third.txt",
                "b/second.txt",
                "root.txt"
            ]
        );
        assert_eq!(
            pending.iter().map(|file| file.index).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert_eq!(final_file_index, 4);
        assert_eq!(final_file_path, "root.txt");
    }

    #[test]
    fn collect_pending_files_keeps_complex_depth_first_order_for_similar_names() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("alpha").join("nested")).unwrap();
        fs::create_dir_all(temp.path().join("beta").join("nested")).unwrap();
        fs::write(temp.path().join("alpha.txt"), b"root-alpha").unwrap();
        fs::write(temp.path().join("zeta.txt"), b"root-zeta").unwrap();
        fs::write(temp.path().join("alpha").join("alpha-1.txt"), b"a1").unwrap();
        fs::write(
            temp.path().join("alpha").join("nested").join("child.txt"),
            b"child",
        )
        .unwrap();
        fs::write(temp.path().join("alpha").join("zeta.txt"), b"az").unwrap();
        fs::write(temp.path().join("beta").join("a.txt"), b"ba").unwrap();
        fs::write(
            temp.path().join("beta").join("nested").join("deep.txt"),
            b"deep",
        )
        .unwrap();
        fs::write(temp.path().join("beta").join("z.txt"), b"bz").unwrap();

        let (pending, final_file_index, final_file_path) =
            collect_pending_files(temp.path().to_path_buf(), &empty_resume_state(), 1, 1).unwrap();

        let rel_paths: Vec<_> = pending.iter().map(|file| file.rel_path.as_str()).collect();
        assert_eq!(
            rel_paths,
            vec![
                "alpha/alpha-1.txt",
                "alpha/nested/child.txt",
                "alpha/zeta.txt",
                "alpha.txt",
                "beta/a.txt",
                "beta/nested/deep.txt",
                "beta/z.txt",
                "zeta.txt",
            ]
        );
        assert_eq!(final_file_index, 8);
        assert_eq!(final_file_path, "zeta.txt");
    }

    #[test]
    fn collect_pending_files_skips_committed_prefix_on_resume() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("dir")).unwrap();
        fs::write(temp.path().join("dir").join("one.txt"), b"111").unwrap();
        fs::write(temp.path().join("dir").join("two.txt"), b"222").unwrap();

        let resume_state = ResumeState {
            last_committed_frame: 1,
            last_committed_file_index: 1,
            last_committed_file_path: "dir/one.txt".to_owned(),
            archive_len: 123,
        };

        let (pending, final_file_index, final_file_path) =
            collect_pending_files(temp.path().to_path_buf(), &resume_state, 1, 1024).unwrap();

        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].index, 2);
        assert_eq!(pending[0].rel_path, "dir/two.txt");
        assert_eq!(final_file_index, 2);
        assert_eq!(final_file_path, "dir/two.txt");
    }

    #[test]
    fn collect_pending_files_returns_no_pending_when_resume_already_covers_all_files() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("dir")).unwrap();
        fs::write(temp.path().join("dir").join("one.txt"), b"111").unwrap();
        fs::write(temp.path().join("dir").join("two.txt"), b"222").unwrap();
        let resume_state = ResumeState {
            last_committed_frame: 1,
            last_committed_file_index: 2,
            last_committed_file_path: "dir/two.txt".to_owned(),
            archive_len: 123,
        };

        let (pending, final_file_index, final_file_path) =
            collect_pending_files(temp.path().to_path_buf(), &resume_state, 1, 1024).unwrap();

        assert!(pending.is_empty());
        assert_eq!(final_file_index, 2);
        assert_eq!(final_file_path, "dir/two.txt");
    }

    #[test]
    fn collect_pending_files_rejects_resume_when_committed_file_is_missing_from_current_tree() {
        let temp = TestDir::new();

        let resume_state = ResumeState {
            last_committed_frame: 3,
            last_committed_file_index: 2,
            last_committed_file_path: "dir/two.txt".to_owned(),
            archive_len: 4096,
        };

        let error =
            collect_pending_files(temp.path().to_path_buf(), &resume_state, 1, 1024).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("sender source no longer matches resume state")
        );
    }

    #[test]
    fn collect_pending_files_rejects_resume_when_committed_file_path_changed() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("dir")).unwrap();
        fs::write(temp.path().join("dir").join("replacement.txt"), b"111").unwrap();
        fs::write(temp.path().join("dir").join("two.txt"), b"222").unwrap();

        let resume_state = ResumeState {
            last_committed_frame: 1,
            last_committed_file_index: 1,
            last_committed_file_path: "dir/one.txt".to_owned(),
            archive_len: 123,
        };

        let error =
            collect_pending_files(temp.path().to_path_buf(), &resume_state, 1, 1024).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("sender source no longer matches resume state")
        );
    }

    #[test]
    fn collect_pending_files_skips_symlink_entries_when_platform_allows_it() {
        let temp = TestDir::new();
        fs::write(temp.path().join("real.txt"), b"abc").unwrap();
        let target = temp.path().join("real.txt");
        let symlink = temp.path().join("skip-me.txt");
        match create_symlink(&symlink, &target) {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::Unsupported
                ) || error.raw_os_error() == Some(1314) =>
            {
                return;
            }
            Err(error) => panic!("failed to create symlink fixture: {error}"),
        }

        let (pending, final_file_index, final_file_path) =
            collect_pending_files(temp.path().to_path_buf(), &empty_resume_state(), 1, 1).unwrap();

        let rel_paths: Vec<_> = pending.iter().map(|file| file.rel_path.as_str()).collect();
        assert_eq!(rel_paths, vec!["real.txt"]);
        assert_eq!(final_file_index, 1);
        assert_eq!(final_file_path, "real.txt");
    }

    #[test]
    fn discover_files_reports_directory_read_error() {
        let _guard = FsFailureGuard::new();
        let temp = TestDir::new();
        super::test_hooks::fail_next_read_dir(temp.path());

        let error = discover_files(temp.path()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to read sender directory")
        );
    }

    #[test]
    fn scan_source_reports_file_metadata_error_for_pending_byte_estimate() {
        let _guard = FsFailureGuard::new();
        let temp = TestDir::new();
        let file = write_input_file(&temp, "alpha.bin", b"abc", 1);
        super::test_hooks::fail_next_metadata(&file.abs_path);

        let error = scan_source(temp.path().to_path_buf(), &empty_resume_state()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to read sender file metadata for scan report")
        );
    }

    #[test]
    fn normalize_relative_path_rejects_source_root() {
        let temp = TestDir::new();

        let error = normalize_relative_path(temp.path(), temp.path()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("sender relative path must not be empty")
        );
    }

    #[test]
    fn pending_file_walker_returns_no_pending_for_fully_resumed_dump() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("dir")).unwrap();
        fs::write(temp.path().join("dir").join("one.txt"), b"111").unwrap();
        fs::write(temp.path().join("dir").join("two.txt"), b"222").unwrap();
        let resume_state = ResumeState {
            last_committed_frame: 1,
            last_committed_file_index: 2,
            last_committed_file_path: "dir/two.txt".to_owned(),
            archive_len: 123,
        };

        let mut walker = PendingFileWalker::new(temp.path().to_path_buf(), &resume_state).unwrap();

        assert!(walker.next_pending_file().unwrap().is_none());
        assert_eq!(
            walker.final_boundary().unwrap(),
            (2, "dir/two.txt".to_owned())
        );
    }

    #[test]
    fn pending_file_walker_detects_resume_mismatch_before_full_tree_walk() {
        let _guard = FsFailureGuard::new();
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("zzz")).unwrap();
        fs::write(temp.path().join("aaa.txt"), b"111").unwrap();
        fs::write(temp.path().join("zzz").join("later.txt"), b"222").unwrap();
        super::test_hooks::fail_next_read_dir(&temp.path().join("zzz"));
        let resume_state = ResumeState {
            last_committed_frame: 1,
            last_committed_file_index: 1,
            last_committed_file_path: "wrong.txt".to_owned(),
            archive_len: 64,
        };

        let mut walker = PendingFileWalker::new(temp.path().to_path_buf(), &resume_state).unwrap();
        let error = walker.next_pending_file().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("sender source no longer matches resume state at file index 1")
        );
        assert!(
            !error
                .to_string()
                .contains("failed to read sender directory")
        );
    }
}

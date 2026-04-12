use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use crossbeam_channel::{Receiver, Sender, TrySendError, bounded, unbounded};
use log::debug;

use super::PipelineProfileCollector;

#[cfg(unix)]
use std::ffi::{CString, OsStr, OsString};
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::os::unix::ffi::{OsStrExt, OsStringExt};

#[cfg(unix)]
const FORCE_ABSOLUTE_PATH_EXECUTOR_ENV: &str = "FAST_DUMP_FORCE_ABS_PATH_EXECUTOR";
const TEST_SENDER_MANIFEST_SPOOL_MAX_BYTES_ENV: &str =
    "FAST_DUMP_TEST_SENDER_MANIFEST_SPOOL_MAX_BYTES";
#[cfg(unix)]
const MAX_CACHED_DIRFD_DEPTH: usize = 64;
const DEFAULT_SENDER_MANIFEST_SPOOL_MAX_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_SENDER_FRESH_MANIFEST_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileResumeState {
    pub last_committed_frame: u64,
    // В active v3 это alias для committed work_ordinal; старое имя оставлено,
    // чтобы не ломать существующий sender-side resume/scanner код за один шаг.
    pub last_committed_file_index: u64,
    // Для v3 это путь, соответствующий committed work_ordinal; поле сохраняем
    // как compatibility alias для текущей валидации resume boundary.
    pub last_committed_file_path: String,
    pub archive_len: u64,
}

impl FileResumeState {
    pub fn last_committed_work_ordinal(&self) -> u64 {
        self.last_committed_file_index
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InputFileKind {
    Regular,
    Symlink { target: PathBuf },
}

#[derive(Debug, Clone)]
struct InputPathPrefix {
    abs_dir: PathBuf,
    rel_dir: Box<str>,
    name_component: Option<Box<str>>,
    #[cfg(unix)]
    depth: usize,
    #[cfg(unix)]
    parent: Option<Arc<Self>>,
    #[cfg(unix)]
    dir_handle: OnceLock<Arc<File>>,
}

impl InputPathPrefix {
    fn root(abs_dir: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            abs_dir,
            rel_dir: String::new().into_boxed_str(),
            name_component: None,
            #[cfg(unix)]
            depth: 0,
            #[cfg(unix)]
            parent: None,
            #[cfg(unix)]
            dir_handle: OnceLock::new(),
        })
    }

    fn child(parent: &Arc<Self>, component: &str) -> Arc<Self> {
        let rel_dir = if parent.rel_dir.is_empty() {
            component.to_owned()
        } else {
            format!("{}/{component}", parent.rel_dir)
        };
        Arc::new(Self {
            abs_dir: parent.abs_dir.join(component),
            rel_dir: rel_dir.into_boxed_str(),
            name_component: Some(component.to_owned().into_boxed_str()),
            #[cfg(unix)]
            depth: parent.depth.saturating_add(1),
            #[cfg(unix)]
            parent: Some(parent.clone()),
            #[cfg(unix)]
            dir_handle: OnceLock::new(),
        })
    }

    fn for_materialized(abs_path: &Path, rel_path: &str) -> Arc<Self> {
        let rel_dir = rel_path
            .rsplit_once('/')
            .map(|(dir, _)| dir.to_owned())
            .unwrap_or_default();
        #[cfg(unix)]
        let depth = if rel_dir.is_empty() {
            0
        } else {
            rel_dir.split('/').count()
        };
        let name_component = rel_dir
            .rsplit('/')
            .next()
            .filter(|value| !value.is_empty())
            .map(|value| value.to_owned().into_boxed_str());
        Arc::new(Self {
            abs_dir: abs_path.parent().map(Path::to_path_buf).unwrap_or_default(),
            rel_dir: rel_dir.into_boxed_str(),
            name_component,
            #[cfg(unix)]
            depth,
            #[cfg(unix)]
            parent: None,
            #[cfg(unix)]
            dir_handle: OnceLock::new(),
        })
    }

    fn name_component(&self) -> Option<&str> {
        self.name_component.as_deref()
    }

    #[cfg(unix)]
    fn open_dir_handle(&self) -> io::Result<Arc<File>> {
        if let Some(handle) = self.dir_handle.get() {
            return Ok(handle.clone());
        }

        if self.depth > MAX_CACHED_DIRFD_DEPTH {
            return File::open(&self.abs_dir).map(Arc::new);
        }

        let file = match (&self.parent, self.name_component()) {
            (Some(parent), Some(component)) => {
                let parent_handle = parent.open_dir_handle()?;
                open_dir_at(parent_handle.as_ref(), component)?
            }
            _ => File::open(&self.abs_dir)?,
        };
        let handle = Arc::new(file);
        let _ = self.dir_handle.set(handle.clone());
        Ok(handle)
    }

    #[cfg(unix)]
    fn dirfd_depth(&self) -> usize {
        self.depth.min(MAX_CACHED_DIRFD_DEPTH)
    }

    #[cfg(unix)]
    fn open_leaf_file(&self, leaf_name: &str) -> io::Result<File> {
        let dir_handle = self.open_dir_handle()?;
        open_file_at(dir_handle.as_ref(), leaf_name)
    }
}

#[derive(Debug)]
struct InputFileLocation {
    prefix: Arc<InputPathPrefix>,
    leaf_name: Box<str>,
    rel_path_cache: OnceLock<Box<str>>,
    abs_path_cache: OnceLock<PathBuf>,
}

impl InputFileLocation {
    fn materialized(abs_path: PathBuf, rel_path: String) -> Self {
        let prefix = InputPathPrefix::for_materialized(&abs_path, &rel_path);
        let leaf_name = input_file_leaf_name(&rel_path);
        let rel_path_cache = OnceLock::new();
        let abs_path_cache = OnceLock::new();
        let _ = rel_path_cache.set(rel_path.into_boxed_str());
        let _ = abs_path_cache.set(abs_path);
        Self {
            prefix,
            leaf_name,
            rel_path_cache,
            abs_path_cache,
        }
    }

    fn from_prefix(prefix: Arc<InputPathPrefix>, leaf_name: Box<str>) -> Self {
        Self {
            prefix,
            leaf_name,
            rel_path_cache: OnceLock::new(),
            abs_path_cache: OnceLock::new(),
        }
    }

    fn rel_path(&self) -> &str {
        self.rel_path_cache
            .get_or_init(|| {
                if self.prefix.rel_dir.is_empty() {
                    self.leaf_name.clone()
                } else {
                    format!("{}/{}", self.prefix.rel_dir, self.leaf_name).into_boxed_str()
                }
            })
            .as_ref()
    }

    fn abs_path(&self) -> &Path {
        self.abs_path_cache
            .get_or_init(|| self.prefix.abs_dir.join(self.leaf_name.as_ref()))
            .as_path()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputFileExecutorKind {
    AbsolutePath,
    #[cfg(unix)]
    DirFd,
}

impl InputFileExecutorKind {
    fn current() -> Self {
        #[cfg(unix)]
        if std::env::var_os(FORCE_ABSOLUTE_PATH_EXECUTOR_ENV).is_none() {
            return Self::DirFd;
        }

        Self::AbsolutePath
    }

    fn dirfd_depth(self, _prefix: &InputPathPrefix) -> usize {
        match self {
            Self::AbsolutePath => 0,
            #[cfg(unix)]
            Self::DirFd => _prefix.dirfd_depth(),
        }
    }

    fn open_for_read(self, location: &InputFileLocation) -> io::Result<File> {
        match self {
            Self::AbsolutePath => File::open(location.abs_path()),
            #[cfg(unix)]
            Self::DirFd => location.prefix.open_leaf_file(location.leaf_name.as_ref()),
        }
    }
}

#[derive(Clone)]
pub(crate) struct InputFile {
    pub(crate) index: u64,
    location: Arc<InputFileLocation>,
    pub(crate) kind: InputFileKind,
}

impl InputFile {
    pub(crate) fn regular(index: u64, abs_path: PathBuf, rel_path: String) -> Self {
        Self {
            index,
            location: Arc::new(InputFileLocation::materialized(abs_path, rel_path)),
            kind: InputFileKind::Regular,
        }
    }

    fn regular_from_prefix(index: u64, prefix: Arc<InputPathPrefix>, leaf_name: Box<str>) -> Self {
        Self {
            index,
            location: Arc::new(InputFileLocation::from_prefix(prefix, leaf_name)),
            kind: InputFileKind::Regular,
        }
    }

    pub(crate) fn symlink(
        index: u64,
        abs_path: PathBuf,
        rel_path: String,
        target: PathBuf,
    ) -> Self {
        Self {
            index,
            location: Arc::new(InputFileLocation::materialized(abs_path, rel_path)),
            kind: InputFileKind::Symlink { target },
        }
    }

    fn symlink_from_prefix(
        index: u64,
        prefix: Arc<InputPathPrefix>,
        leaf_name: Box<str>,
        target: PathBuf,
    ) -> Self {
        Self {
            index,
            location: Arc::new(InputFileLocation::from_prefix(prefix, leaf_name)),
            kind: InputFileKind::Symlink { target },
        }
    }

    pub(crate) fn is_regular(&self) -> bool {
        matches!(self.kind, InputFileKind::Regular)
    }

    pub(crate) fn abs_path(&self) -> &Path {
        self.location.abs_path()
    }

    pub(crate) fn rel_path(&self) -> &str {
        self.location.rel_path()
    }

    pub(crate) fn rel_path_string(&self) -> String {
        self.rel_path().to_owned()
    }

    pub(crate) fn dirfd_depth(&self) -> usize {
        InputFileExecutorKind::current().dirfd_depth(self.location.prefix.as_ref())
    }

    pub(crate) fn open_for_read(&self) -> io::Result<File> {
        InputFileExecutorKind::current().open_for_read(self.location.as_ref())
    }

    pub(crate) fn link_target(&self) -> Option<&Path> {
        match &self.kind {
            InputFileKind::Regular => None,
            InputFileKind::Symlink { target } => Some(target.as_path()),
        }
    }
}

impl std::fmt::Debug for InputFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InputFile")
            .field("index", &self.index)
            .field("abs_path", &self.abs_path())
            .field("rel_path", &self.rel_path())
            .field("kind", &self.kind)
            .finish()
    }
}

impl PartialEq for InputFile {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
            && self.kind == other.kind
            && self.abs_path() == other.abs_path()
            && self.rel_path() == other.rel_path()
    }
}

impl Eq for InputFile {}

fn input_file_leaf_name(rel_path: &str) -> Box<str> {
    rel_path
        .rsplit('/')
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or(rel_path)
        .to_owned()
        .into_boxed_str()
}

#[cfg(unix)]
fn c_string_path_component(component: &str) -> io::Result<CString> {
    c_string_os_component(OsStr::new(component))
}

#[cfg(unix)]
fn c_string_os_component(component: &OsStr) -> io::Result<CString> {
    CString::new(component.as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "sender path component must not contain interior NUL",
        )
    })
}

#[cfg(unix)]
fn open_dir_at(parent: &File, component: &str) -> io::Result<File> {
    let component = c_string_path_component(component)?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            component.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(unix)]
fn open_file_at(parent: &File, component: &str) -> io::Result<File> {
    let component = c_string_path_component(component)?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            component.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(unix)]
fn read_link_at(parent: &File, component: &OsStr) -> io::Result<PathBuf> {
    let component = c_string_os_component(component)?;
    let mut capacity = 256_usize;
    loop {
        let mut buffer = vec![0_u8; capacity];
        let read_len = unsafe {
            libc::readlinkat(
                parent.as_raw_fd(),
                component.as_ptr(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
            )
        };
        if read_len < 0 {
            return Err(io::Error::last_os_error());
        }
        let read_len = read_len as usize;
        if read_len < buffer.len() {
            buffer.truncate(read_len);
            return Ok(PathBuf::from(OsString::from_vec(buffer)));
        }

        let next_capacity = capacity.saturating_mul(2);
        if next_capacity == capacity {
            return Err(io::Error::other(
                "sender symlink target is too large for readlinkat buffer growth",
            ));
        }
        capacity = next_capacity;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SenderSourceKind {
    OrdinaryPath,
    SnapshotPath,
    PinnedPath,
}

impl SenderSourceKind {
    fn manifest_source_kind(self) -> manifest::ManifestSourceKind {
        match self {
            Self::OrdinaryPath => manifest::ManifestSourceKind::LivePath,
            Self::SnapshotPath => manifest::ManifestSourceKind::Snapshot,
            Self::PinnedPath => manifest::ManifestSourceKind::Pinned,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SenderSource {
    path: PathBuf,
    kind: SenderSourceKind,
}

impl SenderSource {
    pub fn ordinary_path(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            kind: SenderSourceKind::OrdinaryPath,
        }
    }

    pub fn snapshot_path(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            kind: SenderSourceKind::SnapshotPath,
        }
    }

    pub fn pinned_path(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            kind: SenderSourceKind::PinnedPath,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn kind(&self) -> SenderSourceKind {
        self.kind
    }
}

impl From<PathBuf> for SenderSource {
    fn from(path: PathBuf) -> Self {
        Self::ordinary_path(path)
    }
}

impl From<&Path> for SenderSource {
    fn from(path: &Path) -> Self {
        Self::ordinary_path(path.to_path_buf())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceScanReport {
    pub pending_files: usize,
    pub pending_bytes: u64,
    pub final_file_index: u64,
    pub final_file_path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SenderWorkloadClass {
    TinyHeavy,
    Mixed,
    LargeHeavy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SenderSourceProbe {
    pub(crate) workload_class: SenderWorkloadClass,
    pub(crate) sampled_directories: usize,
    pub(crate) sampled_regular_files: usize,
    pub(crate) sampled_regular_bytes: u64,
    pub(crate) small_regular_files: usize,
    pub(crate) large_regular_files: usize,
    pub(crate) max_regular_file_bytes: u64,
}

impl SenderSourceProbe {
    pub(crate) fn average_regular_file_bytes(&self) -> u64 {
        if self.sampled_regular_files == 0 {
            return 0;
        }

        self.sampled_regular_bytes / self.sampled_regular_files as u64
    }
}

const MANIFEST_CHECKPOINT_STRIDE_RECORDS: u64 = 4096;
const MANIFEST_BUILDER_VERSION: &str = "manifest-builder-v3-live-v1";
const SOURCE_PROBE_MAX_DIRECTORIES: usize = 128;
const SOURCE_PROBE_MAX_REGULAR_FILES: usize = 64;
const SOURCE_PROBE_SMALL_FILE_BYTES: u64 = 64 * 1024;
const SOURCE_PROBE_LARGE_FILE_BYTES: u64 = 32 * 1024 * 1024;
const SOURCE_PROBE_LARGE_AVG_FILE_BYTES: u64 = 16 * 1024 * 1024;

// В v3 фиксируем grammar и spool format отдельно от текущего source walker,
// чтобы дальнейшие задачи по durability и replay опирались на явную модель.
pub(crate) mod manifest {
    #![allow(dead_code)]

    use std::collections::VecDeque;
    use std::fs::{self, File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, Instant, SystemTime};

    use anyhow::{Context, Result, bail};
    use xxhash_rust::xxh3::xxh3_64;

    use crate::sender::FileResumeState;

    pub(crate) const MANIFEST_LOG_FILE_NAME: &str = "manifest.log.part";
    pub(crate) const MANIFEST_CHECKPOINT_FILE_NAME: &str = "manifest.ckpt.part";
    pub(crate) const MANIFEST_META_FILE_NAME: &str = "manifest.meta";
    pub(crate) const MANIFEST_SPOOL_FORMAT_VERSION: u32 = 1;
    pub(crate) const MANIFEST_RECORD_FLAG_HAS_WORK_ORDINAL: u16 = 0x0001;
    pub(crate) const MANIFEST_RECORD_FLAG_HAS_PAYLOAD: u16 = 0x0002;
    const MANIFEST_RECORD_FIXED_HEADER_LEN: usize = 35;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub(crate) struct ManifestRecordSeq(pub(crate) u64);

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub(crate) struct ManifestWorkOrdinal(pub(crate) u64);

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum ManifestRecordKind {
        DirectoryEnter,
        DirectoryLeave,
        File,
        Symlink,
        DiscoverySkip,
        SubtreeSkip,
    }

    impl ManifestRecordKind {
        pub(crate) fn tag(self) -> u8 {
            match self {
                Self::DirectoryEnter => 1,
                Self::DirectoryLeave => 2,
                Self::File => 3,
                Self::Symlink => 4,
                Self::DiscoverySkip => 5,
                Self::SubtreeSkip => 6,
            }
        }

        pub(crate) fn requires_work_ordinal(self) -> bool {
            matches!(self, Self::File | Self::Symlink)
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum ManifestSpecialEntryKind {
        Socket,
        Fifo,
        BlockDevice,
        CharDevice,
        Other,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum ManifestIoOperation {
        ReadDir,
        ReadLink,
        ClassifyEntry,
        Replay,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum ManifestSenderStateEvent {
        Invalidated,
        Corrupted,
        Rebuilt,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) enum ManifestDiscoverySkipReason {
        UnsupportedEntry {
            kind: ManifestSpecialEntryKind,
        },
        UnreadableSymlinkTarget {
            error: Box<str>,
        },
        VirtualFilesystem {
            kind: Box<str>,
        },
        Io {
            operation: ManifestIoOperation,
            error: Box<str>,
        },
        SenderState {
            event: ManifestSenderStateEvent,
            detail: Box<str>,
        },
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) enum ManifestSubtreeSkipReason {
        ReadDirFailed {
            error: Box<str>,
        },
        SourceInvalidated {
            detail: Box<str>,
        },
        Io {
            operation: ManifestIoOperation,
            error: Box<str>,
        },
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) enum ManifestRecordPayload {
        None,
        SymlinkTarget { target: Box<str> },
        DiscoverySkip { reason: ManifestDiscoverySkipReason },
        SubtreeSkip { reason: ManifestSubtreeSkipReason },
    }

    impl ManifestRecordPayload {
        pub(crate) fn encoded_len(&self) -> usize {
            match self {
                Self::None => 0,
                Self::SymlinkTarget { target } => target.len(),
                Self::DiscoverySkip { reason } => 1 + reason.encoded_len(),
                Self::SubtreeSkip { reason } => 1 + reason.encoded_len(),
            }
        }

        fn matches_kind(&self, kind: ManifestRecordKind) -> bool {
            matches!(
                (kind, self),
                (ManifestRecordKind::DirectoryEnter, Self::None)
                    | (ManifestRecordKind::DirectoryLeave, Self::None)
                    | (ManifestRecordKind::File, Self::None)
                    | (ManifestRecordKind::Symlink, Self::SymlinkTarget { .. })
                    | (
                        ManifestRecordKind::DiscoverySkip,
                        Self::DiscoverySkip { .. }
                    )
                    | (ManifestRecordKind::SubtreeSkip, Self::SubtreeSkip { .. })
            )
        }
    }

    impl ManifestDiscoverySkipReason {
        fn encoded_len(&self) -> usize {
            match self {
                Self::UnsupportedEntry { .. } => 1,
                Self::UnreadableSymlinkTarget { error } => encoded_string_len(error),
                Self::VirtualFilesystem { kind } => encoded_string_len(kind),
                Self::Io { error, .. } => 1 + encoded_string_len(error),
                Self::SenderState { detail, .. } => 1 + encoded_string_len(detail),
            }
        }
    }

    impl ManifestSubtreeSkipReason {
        fn encoded_len(&self) -> usize {
            match self {
                Self::ReadDirFailed { error } => encoded_string_len(error),
                Self::SourceInvalidated { detail } => encoded_string_len(detail),
                Self::Io { error, .. } => 1 + encoded_string_len(error),
            }
        }
    }

    fn encoded_string_len(value: &str) -> usize {
        2 + value.len()
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct ManifestRecord {
        pub(crate) record_seq: ManifestRecordSeq,
        pub(crate) kind: ManifestRecordKind,
        pub(crate) depth: u16,
        pub(crate) name_component: Box<str>,
        pub(crate) work_ordinal: Option<ManifestWorkOrdinal>,
        pub(crate) payload: ManifestRecordPayload,
    }

    impl ManifestRecord {
        pub(crate) fn new(
            record_seq: ManifestRecordSeq,
            kind: ManifestRecordKind,
            depth: u16,
            name_component: impl Into<Box<str>>,
            work_ordinal: Option<ManifestWorkOrdinal>,
            payload: ManifestRecordPayload,
        ) -> Result<Self> {
            let name_component = name_component.into();
            if name_component.is_empty() {
                bail!("manifest record name component must not be empty");
            }
            if name_component.contains('/') || name_component.contains('\\') {
                bail!("manifest record name component must store only one path segment");
            }
            if kind.requires_work_ordinal() != work_ordinal.is_some() {
                bail!(
                    "manifest record {:?} must {} a work_ordinal",
                    kind,
                    if kind.requires_work_ordinal() {
                        "carry"
                    } else {
                        "not carry"
                    }
                );
            }
            if !payload.matches_kind(kind) {
                bail!(
                    "manifest payload {:?} does not match kind {:?}",
                    payload,
                    kind
                );
            }

            Ok(Self {
                record_seq,
                kind,
                depth,
                name_component,
                work_ordinal,
                payload,
            })
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct ManifestSpoolRecordHeader {
        pub(crate) record_body_len: u32,
        pub(crate) checksum_u32: u32,
        pub(crate) record_seq: ManifestRecordSeq,
        pub(crate) work_ordinal_or_zero: u64,
        pub(crate) kind: ManifestRecordKind,
        pub(crate) depth: u16,
        pub(crate) flags: u16,
        pub(crate) name_len: u16,
        pub(crate) payload_len: u32,
    }

    impl ManifestSpoolRecordHeader {
        pub(crate) fn from_record(record: &ManifestRecord) -> Result<Self> {
            let name_len = u16::try_from(record.name_component.len())
                .map_err(|_| anyhow::anyhow!("manifest name component is too long"))?;
            let payload_len = u32::try_from(record.payload.encoded_len())
                .map_err(|_| anyhow::anyhow!("manifest payload is too large"))?;
            let mut flags = 0_u16;
            if record.work_ordinal.is_some() {
                flags |= MANIFEST_RECORD_FLAG_HAS_WORK_ORDINAL;
            }
            if payload_len != 0 {
                flags |= MANIFEST_RECORD_FLAG_HAS_PAYLOAD;
            }

            Ok(Self {
                record_body_len: u32::from(name_len) + payload_len,
                checksum_u32: 0,
                record_seq: record.record_seq,
                work_ordinal_or_zero: record.work_ordinal.map(|value| value.0).unwrap_or(0),
                kind: record.kind,
                depth: record.depth,
                flags,
                name_len,
                payload_len,
            })
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum ManifestSourceKind {
        LivePath,
        Snapshot,
        Pinned,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct ManifestSourceIdentity {
        pub(crate) normalized_root_path: Box<str>,
        pub(crate) identity_root: Box<str>,
        pub(crate) builder_version: Box<str>,
        pub(crate) source_kind: ManifestSourceKind,
    }

    impl ManifestSourceIdentity {
        pub(crate) fn new(
            normalized_root_path: impl Into<Box<str>>,
            identity_root: impl Into<Box<str>>,
            builder_version: impl Into<Box<str>>,
            source_kind: ManifestSourceKind,
        ) -> Result<Self> {
            let normalized_root_path = normalized_root_path.into();
            let identity_root = identity_root.into();
            let builder_version = builder_version.into();
            if normalized_root_path.is_empty() {
                bail!("manifest source identity requires normalized_root_path");
            }
            if identity_root.is_empty() {
                bail!("manifest source identity requires identity_root");
            }
            if builder_version.is_empty() {
                bail!("manifest source identity requires builder_version");
            }

            Ok(Self {
                normalized_root_path,
                identity_root,
                builder_version,
                source_kind,
            })
        }

        fn reuse_key_matches(&self, other: &Self) -> bool {
            self.normalized_root_path == other.normalized_root_path
                && self.identity_root == other.identity_root
                && self.builder_version == other.builder_version
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum ManifestBuildState {
        Clean,
        Dirty,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum ManifestSealState {
        Unsealed,
        Sealed,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum ManifestInvalidationReason {
        SourceIdentityMismatch,
        BuilderVersionMismatch,
        DumpIdMismatch,
        UnsealedLivePathRestart,
        SpoolCorrupted,
        RebuildRequested,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum ManifestReuseDecision {
        ReuseSealed,
        ContinueUnsealedBuild,
        InvalidateAndRebuild { reason: ManifestInvalidationReason },
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct ManifestMeta {
        pub(crate) dump_id: Box<str>,
        pub(crate) spool_format_version: u32,
        pub(crate) source_identity: ManifestSourceIdentity,
        pub(crate) root_source_path: Box<str>,
        pub(crate) build_state: ManifestBuildState,
        pub(crate) seal_state: ManifestSealState,
        pub(crate) last_record_seq: Option<ManifestRecordSeq>,
        pub(crate) last_work_ordinal: Option<ManifestWorkOrdinal>,
        pub(crate) last_checkpoint_seq: Option<u64>,
        pub(crate) last_invalidation_reason: Option<ManifestInvalidationReason>,
    }

    impl ManifestMeta {
        pub(crate) fn new(
            dump_id: impl Into<Box<str>>,
            source_identity: ManifestSourceIdentity,
            root_source_path: impl Into<Box<str>>,
        ) -> Result<Self> {
            let dump_id = dump_id.into();
            let root_source_path = root_source_path.into();
            if dump_id.is_empty() {
                bail!("manifest meta requires dump_id");
            }
            if root_source_path.is_empty() {
                bail!("manifest meta requires root_source_path");
            }

            Ok(Self {
                dump_id,
                spool_format_version: MANIFEST_SPOOL_FORMAT_VERSION,
                source_identity,
                root_source_path,
                build_state: ManifestBuildState::Clean,
                seal_state: ManifestSealState::Unsealed,
                last_record_seq: None,
                last_work_ordinal: None,
                last_checkpoint_seq: None,
                last_invalidation_reason: None,
            })
        }

        pub(crate) fn record_uncommitted_append(&mut self) {
            self.build_state = ManifestBuildState::Dirty;
            self.seal_state = ManifestSealState::Unsealed;
        }

        pub(crate) fn commit_checkpoint(
            &mut self,
            last_record_seq: ManifestRecordSeq,
            last_work_ordinal: Option<ManifestWorkOrdinal>,
            last_checkpoint_seq: u64,
        ) -> Result<()> {
            if let Some(previous) = self.last_record_seq
                && last_record_seq.0 < previous.0
            {
                bail!("manifest checkpoint must not move last_record_seq backwards");
            }
            if let Some(previous) = self.last_work_ordinal
                && last_work_ordinal.map(|value| value.0).unwrap_or(0) < previous.0
            {
                bail!("manifest checkpoint must not move last_work_ordinal backwards");
            }
            if let Some(previous) = self.last_checkpoint_seq
                && last_checkpoint_seq < previous
            {
                bail!("manifest checkpoint sequence must not move backwards");
            }

            self.last_record_seq = Some(last_record_seq);
            self.last_work_ordinal = last_work_ordinal;
            self.last_checkpoint_seq = Some(last_checkpoint_seq);
            self.build_state = ManifestBuildState::Clean;
            self.last_invalidation_reason = None;
            Ok(())
        }

        pub(crate) fn mark_sealed(&mut self) {
            self.build_state = ManifestBuildState::Clean;
            self.seal_state = ManifestSealState::Sealed;
            self.last_invalidation_reason = None;
        }

        pub(crate) fn invalidate(&mut self, reason: ManifestInvalidationReason) {
            self.build_state = ManifestBuildState::Clean;
            self.seal_state = ManifestSealState::Unsealed;
            self.last_record_seq = None;
            self.last_work_ordinal = None;
            self.last_checkpoint_seq = None;
            self.last_invalidation_reason = Some(reason);
        }

        pub(crate) fn encode(&self) -> Result<Vec<u8>> {
            let mut body = Vec::new();
            write_len_prefixed_string(&mut body, &self.dump_id)?;
            write_u32(&mut body, self.spool_format_version);
            write_len_prefixed_string(&mut body, &self.source_identity.normalized_root_path)?;
            write_len_prefixed_string(&mut body, &self.source_identity.identity_root)?;
            write_len_prefixed_string(&mut body, &self.source_identity.builder_version)?;
            body.push(encode_source_kind(self.source_identity.source_kind));
            write_len_prefixed_string(&mut body, &self.root_source_path)?;
            body.push(encode_build_state(self.build_state));
            body.push(encode_seal_state(self.seal_state));
            write_u64(
                &mut body,
                self.last_record_seq.map(|value| value.0).unwrap_or(0),
            );
            write_u64(
                &mut body,
                self.last_work_ordinal.map(|value| value.0).unwrap_or(0),
            );
            write_u64(&mut body, self.last_checkpoint_seq.unwrap_or(0));
            body.push(encode_invalidation_reason(self.last_invalidation_reason));

            let mut encoded = Vec::new();
            write_u32(&mut encoded, MANIFEST_SPOOL_FORMAT_VERSION);
            write_u32(&mut encoded, checksum_u32(&body));
            encoded.extend_from_slice(&body);
            Ok(encoded)
        }

        pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
            if bytes.len() < 8 {
                bail!("manifest meta is too short");
            }
            let mut cursor = 0_usize;
            let version = read_u32(bytes, &mut cursor)?;
            if version != MANIFEST_SPOOL_FORMAT_VERSION {
                bail!(
                    "unsupported manifest meta version: expected {}, got {}",
                    MANIFEST_SPOOL_FORMAT_VERSION,
                    version
                );
            }
            let expected_checksum = read_u32(bytes, &mut cursor)?;
            let body = &bytes[cursor..];
            let actual_checksum = checksum_u32(body);
            if actual_checksum != expected_checksum {
                bail!(
                    "manifest meta checksum mismatch: expected {}, got {}",
                    expected_checksum,
                    actual_checksum
                );
            }

            let mut body_cursor = 0_usize;
            let dump_id = read_len_prefixed_string(body, &mut body_cursor)?;
            let spool_format_version = read_u32(body, &mut body_cursor)?;
            let normalized_root_path = read_len_prefixed_string(body, &mut body_cursor)?;
            let identity_root = read_len_prefixed_string(body, &mut body_cursor)?;
            let builder_version = read_len_prefixed_string(body, &mut body_cursor)?;
            let source_kind = decode_source_kind(read_u8(body, &mut body_cursor)?)?;
            let root_source_path = read_len_prefixed_string(body, &mut body_cursor)?;
            let build_state = decode_build_state(read_u8(body, &mut body_cursor)?)?;
            let seal_state = decode_seal_state(read_u8(body, &mut body_cursor)?)?;
            let last_record_seq_raw = read_u64(body, &mut body_cursor)?;
            let last_work_ordinal_raw = read_u64(body, &mut body_cursor)?;
            let last_checkpoint_seq_raw = read_u64(body, &mut body_cursor)?;
            let last_invalidation_reason =
                decode_invalidation_reason(read_u8(body, &mut body_cursor)?)?;
            if body_cursor != body.len() {
                bail!("manifest meta contains trailing bytes");
            }

            Ok(Self {
                dump_id,
                spool_format_version,
                source_identity: ManifestSourceIdentity::new(
                    normalized_root_path,
                    identity_root,
                    builder_version,
                    source_kind,
                )?,
                root_source_path,
                build_state,
                seal_state,
                last_record_seq: if last_record_seq_raw == 0 {
                    None
                } else {
                    Some(ManifestRecordSeq(last_record_seq_raw))
                },
                last_work_ordinal: if last_work_ordinal_raw == 0 {
                    None
                } else {
                    Some(ManifestWorkOrdinal(last_work_ordinal_raw))
                },
                last_checkpoint_seq: if last_checkpoint_seq_raw == 0 {
                    None
                } else {
                    Some(last_checkpoint_seq_raw)
                },
                last_invalidation_reason,
            })
        }

        pub(crate) fn write_atomic(&self, path: &Path) -> Result<()> {
            let parent = path
                .parent()
                .ok_or_else(|| anyhow::anyhow!("manifest meta path has no parent"))?;
            fs::create_dir_all(parent)?;
            let temp_path = meta_temp_path(path);
            fs::write(&temp_path, self.encode()?).with_context(|| {
                format!(
                    "failed to write temporary manifest meta {}",
                    temp_path.display()
                )
            })?;
            fs::rename(&temp_path, path).with_context(|| {
                format!(
                    "failed to atomically replace manifest meta {}",
                    path.display()
                )
            })?;
            Ok(())
        }

        pub(crate) fn read_from_path(path: &Path) -> Result<Self> {
            let bytes = fs::read(path)
                .with_context(|| format!("failed to read manifest meta {}", path.display()))?;
            Self::decode(&bytes)
        }

        pub(crate) fn validate_reuse_for_source(
            &self,
            dump_id: &str,
            source_identity: &ManifestSourceIdentity,
        ) -> Result<ManifestReuseDecision> {
            if self.dump_id.as_ref() != dump_id {
                return Ok(ManifestReuseDecision::InvalidateAndRebuild {
                    reason: ManifestInvalidationReason::DumpIdMismatch,
                });
            }

            if self.source_identity.builder_version != source_identity.builder_version {
                return Ok(ManifestReuseDecision::InvalidateAndRebuild {
                    reason: ManifestInvalidationReason::BuilderVersionMismatch,
                });
            }

            if self.source_identity.normalized_root_path != source_identity.normalized_root_path
                || self.source_identity.identity_root != source_identity.identity_root
            {
                return Ok(ManifestReuseDecision::InvalidateAndRebuild {
                    reason: ManifestInvalidationReason::SourceIdentityMismatch,
                });
            }

            if !self.source_identity.reuse_key_matches(source_identity) {
                return Ok(ManifestReuseDecision::InvalidateAndRebuild {
                    reason: ManifestInvalidationReason::SourceIdentityMismatch,
                });
            }

            match self.seal_state {
                ManifestSealState::Sealed => Ok(ManifestReuseDecision::ReuseSealed),
                ManifestSealState::Unsealed => match source_identity.source_kind {
                    ManifestSourceKind::LivePath => {
                        Ok(ManifestReuseDecision::InvalidateAndRebuild {
                            reason: ManifestInvalidationReason::UnsealedLivePathRestart,
                        })
                    }
                    ManifestSourceKind::Snapshot | ManifestSourceKind::Pinned => {
                        Ok(ManifestReuseDecision::ContinueUnsealedBuild)
                    }
                },
            }
        }
    }

    fn encode_source_kind(kind: ManifestSourceKind) -> u8 {
        match kind {
            ManifestSourceKind::LivePath => 1,
            ManifestSourceKind::Snapshot => 2,
            ManifestSourceKind::Pinned => 3,
        }
    }

    fn decode_source_kind(tag: u8) -> Result<ManifestSourceKind> {
        match tag {
            1 => Ok(ManifestSourceKind::LivePath),
            2 => Ok(ManifestSourceKind::Snapshot),
            3 => Ok(ManifestSourceKind::Pinned),
            _ => bail!("unknown manifest source kind tag {}", tag),
        }
    }

    fn encode_build_state(state: ManifestBuildState) -> u8 {
        match state {
            ManifestBuildState::Clean => 1,
            ManifestBuildState::Dirty => 2,
        }
    }

    fn decode_build_state(tag: u8) -> Result<ManifestBuildState> {
        match tag {
            1 => Ok(ManifestBuildState::Clean),
            2 => Ok(ManifestBuildState::Dirty),
            _ => bail!("unknown manifest build state tag {}", tag),
        }
    }

    fn encode_seal_state(state: ManifestSealState) -> u8 {
        match state {
            ManifestSealState::Unsealed => 1,
            ManifestSealState::Sealed => 2,
        }
    }

    fn decode_seal_state(tag: u8) -> Result<ManifestSealState> {
        match tag {
            1 => Ok(ManifestSealState::Unsealed),
            2 => Ok(ManifestSealState::Sealed),
            _ => bail!("unknown manifest seal state tag {}", tag),
        }
    }

    fn encode_invalidation_reason(reason: Option<ManifestInvalidationReason>) -> u8 {
        match reason {
            None => 0,
            Some(ManifestInvalidationReason::SourceIdentityMismatch) => 1,
            Some(ManifestInvalidationReason::BuilderVersionMismatch) => 2,
            Some(ManifestInvalidationReason::DumpIdMismatch) => 3,
            Some(ManifestInvalidationReason::UnsealedLivePathRestart) => 4,
            Some(ManifestInvalidationReason::SpoolCorrupted) => 5,
            Some(ManifestInvalidationReason::RebuildRequested) => 6,
        }
    }

    fn decode_invalidation_reason(tag: u8) -> Result<Option<ManifestInvalidationReason>> {
        match tag {
            0 => Ok(None),
            1 => Ok(Some(ManifestInvalidationReason::SourceIdentityMismatch)),
            2 => Ok(Some(ManifestInvalidationReason::BuilderVersionMismatch)),
            3 => Ok(Some(ManifestInvalidationReason::DumpIdMismatch)),
            4 => Ok(Some(ManifestInvalidationReason::UnsealedLivePathRestart)),
            5 => Ok(Some(ManifestInvalidationReason::SpoolCorrupted)),
            6 => Ok(Some(ManifestInvalidationReason::RebuildRequested)),
            _ => bail!("unknown manifest invalidation reason tag {}", tag),
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct ManifestCheckpointFrame {
        pub(crate) name_component: Box<str>,
        pub(crate) child_emitted: bool,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct ManifestCheckpoint {
        pub(crate) checkpoint_seq: u64,
        pub(crate) last_record_seq: ManifestRecordSeq,
        pub(crate) last_work_ordinal: Option<ManifestWorkOrdinal>,
        pub(crate) log_offset: u64,
        pub(crate) path_stack: Vec<ManifestCheckpointFrame>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct ManifestCheckpointBuildReport {
        pub(crate) checkpoints: Vec<ManifestCheckpoint>,
        pub(crate) truncate_to_len: u64,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct ManifestReplayMetrics {
        pub(crate) checkpoint_seq: Option<u64>,
        pub(crate) replayed_records: u64,
        pub(crate) replayed_bytes: u64,
        pub(crate) replay_duration: Duration,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct ManifestReplayResult {
        pub(crate) checkpoint: Option<ManifestCheckpoint>,
        pub(crate) replayed_records: Vec<ManifestRecord>,
        pub(crate) final_state: ManifestGrammarState,
        pub(crate) metrics: ManifestReplayMetrics,
    }

    impl ManifestCheckpoint {
        pub(crate) fn encode(&self) -> Result<Vec<u8>> {
            let mut body = Vec::new();
            write_u64(&mut body, self.checkpoint_seq);
            write_u64(&mut body, self.last_record_seq.0);
            write_u64(
                &mut body,
                self.last_work_ordinal.map(|value| value.0).unwrap_or(0),
            );
            write_u64(&mut body, self.log_offset);
            write_u32(
                &mut body,
                u32::try_from(self.path_stack.len())
                    .map_err(|_| anyhow::anyhow!("manifest checkpoint path stack is too deep"))?,
            );
            for frame in &self.path_stack {
                write_len_prefixed_string(&mut body, &frame.name_component)?;
                body.push(u8::from(frame.child_emitted));
            }

            let mut encoded = Vec::new();
            write_u32(&mut encoded, MANIFEST_SPOOL_FORMAT_VERSION);
            write_u32(&mut encoded, checksum_u32(&body));
            encoded.extend_from_slice(&body);
            Ok(encoded)
        }

        pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
            if bytes.len() < 8 {
                bail!("manifest checkpoint is too short");
            }
            let mut cursor = 0_usize;
            let version = read_u32(bytes, &mut cursor)?;
            if version != MANIFEST_SPOOL_FORMAT_VERSION {
                bail!(
                    "unsupported manifest checkpoint version: expected {}, got {}",
                    MANIFEST_SPOOL_FORMAT_VERSION,
                    version
                );
            }
            let expected_checksum = read_u32(bytes, &mut cursor)?;
            let body = &bytes[cursor..];
            let actual_checksum = checksum_u32(body);
            if actual_checksum != expected_checksum {
                bail!(
                    "manifest checkpoint checksum mismatch: expected {}, got {}",
                    expected_checksum,
                    actual_checksum
                );
            }

            let mut body_cursor = 0_usize;
            let checkpoint_seq = read_u64(body, &mut body_cursor)?;
            let last_record_seq = ManifestRecordSeq(read_u64(body, &mut body_cursor)?);
            let last_work_ordinal_raw = read_u64(body, &mut body_cursor)?;
            let log_offset = read_u64(body, &mut body_cursor)?;
            let path_count = read_u32(body, &mut body_cursor)? as usize;
            let mut path_stack = Vec::with_capacity(path_count);
            for _ in 0..path_count {
                path_stack.push(ManifestCheckpointFrame {
                    name_component: read_len_prefixed_string(body, &mut body_cursor)?,
                    child_emitted: read_u8(body, &mut body_cursor)? != 0,
                });
            }
            if body_cursor != body.len() {
                bail!("manifest checkpoint contains trailing bytes");
            }

            Ok(Self {
                checkpoint_seq,
                last_record_seq,
                last_work_ordinal: if last_work_ordinal_raw == 0 {
                    None
                } else {
                    Some(ManifestWorkOrdinal(last_work_ordinal_raw))
                },
                log_offset,
                path_stack,
            })
        }

        pub(crate) fn write_atomic(&self, path: &Path) -> Result<()> {
            let parent = path
                .parent()
                .ok_or_else(|| anyhow::anyhow!("manifest checkpoint path has no parent"))?;
            fs::create_dir_all(parent)?;
            let temp_path = checkpoint_temp_path(path);
            fs::write(&temp_path, self.encode()?).with_context(|| {
                format!(
                    "failed to write temporary manifest checkpoint {}",
                    temp_path.display()
                )
            })?;
            fs::rename(&temp_path, path).with_context(|| {
                format!(
                    "failed to atomically replace manifest checkpoint {}",
                    path.display()
                )
            })?;
            Ok(())
        }

        pub(crate) fn read_from_path(path: &Path) -> Result<Self> {
            let bytes = fs::read(path).with_context(|| {
                format!("failed to read manifest checkpoint {}", path.display())
            })?;
            Self::decode(&bytes)
        }
    }

    enum ManifestDecodedRecord {
        Complete {
            record: ManifestRecord,
            next_cursor: usize,
        },
        TornTail {
            truncate_to_len: u64,
        },
    }

    fn decode_record_at(bytes: &[u8], cursor: usize) -> Result<ManifestDecodedRecord> {
        let record_start = cursor;
        let remaining = bytes.len().saturating_sub(cursor);
        if remaining < MANIFEST_RECORD_FIXED_HEADER_LEN {
            return Ok(ManifestDecodedRecord::TornTail {
                truncate_to_len: record_start as u64,
            });
        }

        let mut header_cursor = cursor;
        let record_body_len = read_u32(bytes, &mut header_cursor)? as usize;
        let expected_checksum = read_u32(bytes, &mut header_cursor)?;
        let record_seq = ManifestRecordSeq(read_u64(bytes, &mut header_cursor)?);
        let work_ordinal_or_zero = read_u64(bytes, &mut header_cursor)?;
        let kind = decode_record_kind(read_u8(bytes, &mut header_cursor)?)?;
        let depth = read_u16(bytes, &mut header_cursor)?;
        let flags = read_u16(bytes, &mut header_cursor)?;
        let name_len = read_u16(bytes, &mut header_cursor)? as usize;
        let payload_len = read_u32(bytes, &mut header_cursor)? as usize;
        if record_body_len != name_len + payload_len {
            bail!("manifest spool record_body_len does not match name_len + payload_len");
        }

        let record_end = header_cursor.saturating_add(record_body_len);
        if record_end > bytes.len() {
            return Ok(ManifestDecodedRecord::TornTail {
                truncate_to_len: record_start as u64,
            });
        }

        let checksum_region = &bytes[record_start + 8..record_end];
        let actual_checksum = checksum_u32(checksum_region);
        if actual_checksum != expected_checksum {
            bail!(
                "manifest spool checksum mismatch at offset {}: expected {}, got {}",
                record_start,
                expected_checksum,
                actual_checksum
            );
        }

        let mut body_cursor = header_cursor;
        let name_component = read_sized_utf8(bytes, &mut body_cursor, name_len)?;
        let payload = decode_record_payload(kind, payload_len, bytes, &mut body_cursor)?;
        if body_cursor != record_end {
            bail!("manifest spool record parser left trailing bytes inside a record");
        }

        let work_ordinal = if flags & MANIFEST_RECORD_FLAG_HAS_WORK_ORDINAL != 0 {
            if work_ordinal_or_zero == 0 {
                bail!("manifest spool record flagged work_ordinal but stored zero");
            }
            Some(ManifestWorkOrdinal(work_ordinal_or_zero))
        } else if work_ordinal_or_zero == 0 {
            None
        } else {
            bail!("manifest spool record stored work_ordinal without flag");
        };

        if (payload_len == 0) != (flags & MANIFEST_RECORD_FLAG_HAS_PAYLOAD == 0) {
            bail!("manifest spool payload flag does not match payload length");
        }

        let record = ManifestRecord::new(
            record_seq,
            kind,
            depth,
            name_component,
            work_ordinal,
            payload,
        )?;
        Ok(ManifestDecodedRecord::Complete {
            record,
            next_cursor: record_end,
        })
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct ManifestRecoveryReport {
        pub(crate) records: Vec<ManifestRecord>,
        pub(crate) truncate_to_len: u64,
    }

    pub(crate) struct ManifestSpoolWriter;

    impl ManifestSpoolWriter {
        pub(crate) fn encode_record(record: &ManifestRecord) -> Result<Vec<u8>> {
            let header = ManifestSpoolRecordHeader::from_record(record)?;
            let body = encode_record_body(record)?;
            if usize::try_from(header.record_body_len).unwrap_or(usize::MAX) != body.len() {
                bail!("manifest record_body_len does not match encoded body length");
            }

            let mut checksum_bytes =
                Vec::with_capacity(MANIFEST_RECORD_FIXED_HEADER_LEN - 8 + body.len());
            write_u64(&mut checksum_bytes, record.record_seq.0);
            write_u64(
                &mut checksum_bytes,
                record.work_ordinal.map(|value| value.0).unwrap_or(0),
            );
            checksum_bytes.push(record.kind.tag());
            write_u16(&mut checksum_bytes, record.depth);
            write_u16(&mut checksum_bytes, header.flags);
            write_u16(&mut checksum_bytes, header.name_len);
            write_u32(&mut checksum_bytes, header.payload_len);
            checksum_bytes.extend_from_slice(&body);
            let checksum_u32 = checksum_u32(&checksum_bytes);

            let mut encoded = Vec::with_capacity(MANIFEST_RECORD_FIXED_HEADER_LEN + body.len());
            write_u32(&mut encoded, header.record_body_len);
            write_u32(&mut encoded, checksum_u32);
            encoded.extend_from_slice(&checksum_bytes);
            Ok(encoded)
        }

        pub(crate) fn append_record<W: Write>(
            writer: &mut W,
            record: &ManifestRecord,
        ) -> Result<u64> {
            let encoded = Self::encode_record(record)?;
            writer.write_all(&encoded)?;
            Ok(encoded.len() as u64)
        }
    }

    pub(crate) struct ManifestSpoolReader;

    impl ManifestSpoolReader {
        pub(crate) fn recover_prefix<R: Read>(mut reader: R) -> Result<ManifestRecoveryReport> {
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes)?;
            Self::recover_prefix_bytes(&bytes)
        }

        pub(crate) fn recover_prefix_bytes(bytes: &[u8]) -> Result<ManifestRecoveryReport> {
            let mut cursor = 0_usize;
            let mut grammar = ManifestGrammarState::default();
            let mut records = Vec::new();

            while cursor < bytes.len() {
                match decode_record_at(bytes, cursor)? {
                    ManifestDecodedRecord::Complete {
                        record,
                        next_cursor,
                    } => {
                        grammar.validate_and_apply(&record)?;
                        records.push(record);
                        cursor = next_cursor;
                    }
                    ManifestDecodedRecord::TornTail { truncate_to_len } => {
                        return Ok(ManifestRecoveryReport {
                            records,
                            truncate_to_len,
                        });
                    }
                }
            }

            Ok(ManifestRecoveryReport {
                records,
                truncate_to_len: cursor as u64,
            })
        }

        pub(crate) fn build_sparse_checkpoints(
            bytes: &[u8],
            stride: u64,
        ) -> Result<ManifestCheckpointBuildReport> {
            if stride == 0 {
                bail!("manifest checkpoint stride must be at least 1");
            }

            let mut cursor = 0_usize;
            let mut grammar = ManifestGrammarState::default();
            let mut checkpoints = Vec::new();
            let mut next_checkpoint_seq = 1_u64;

            while cursor < bytes.len() {
                match decode_record_at(bytes, cursor)? {
                    ManifestDecodedRecord::Complete {
                        record,
                        next_cursor,
                    } => {
                        grammar.validate_and_apply(&record)?;
                        cursor = next_cursor;

                        let should_checkpoint = record.record_seq.0 % stride == 0;
                        if should_checkpoint
                            && let Some(checkpoint) =
                                grammar.snapshot_checkpoint(next_checkpoint_seq, cursor as u64)
                        {
                            checkpoints.push(checkpoint);
                            next_checkpoint_seq = next_checkpoint_seq.saturating_add(1);
                        }
                    }
                    ManifestDecodedRecord::TornTail { truncate_to_len } => {
                        return Ok(ManifestCheckpointBuildReport {
                            checkpoints,
                            truncate_to_len,
                        });
                    }
                }
            }

            if let Some(last_record_seq) = grammar.last_record_seq() {
                let needs_final_checkpoint = checkpoints
                    .last()
                    .map(|checkpoint| checkpoint.last_record_seq != last_record_seq)
                    .unwrap_or(true);
                if needs_final_checkpoint
                    && let Some(checkpoint) =
                        grammar.snapshot_checkpoint(next_checkpoint_seq, cursor as u64)
                {
                    checkpoints.push(checkpoint);
                }
            }

            Ok(ManifestCheckpointBuildReport {
                checkpoints,
                truncate_to_len: cursor as u64,
            })
        }

        pub(crate) fn choose_checkpoint_for_record_seq(
            checkpoints: &[ManifestCheckpoint],
            target_record_seq: ManifestRecordSeq,
        ) -> Option<&ManifestCheckpoint> {
            checkpoints
                .iter()
                .rev()
                .find(|checkpoint| checkpoint.last_record_seq.0 <= target_record_seq.0)
        }

        pub(crate) fn replay_from_checkpoints(
            bytes: &[u8],
            checkpoints: &[ManifestCheckpoint],
            target_record_seq: ManifestRecordSeq,
        ) -> Result<ManifestReplayResult> {
            let checkpoint =
                Self::choose_checkpoint_for_record_seq(checkpoints, target_record_seq).cloned();
            let replay_started = Instant::now();
            let mut grammar = checkpoint
                .as_ref()
                .map(ManifestGrammarState::from_checkpoint)
                .unwrap_or_default();
            let mut cursor = checkpoint
                .as_ref()
                .map(|value| value.log_offset as usize)
                .unwrap_or(0);
            let start_offset = cursor as u64;
            let mut replayed_records = Vec::new();

            while grammar
                .last_record_seq()
                .map(|value| value.0 < target_record_seq.0)
                .unwrap_or(true)
            {
                match decode_record_at(bytes, cursor)? {
                    ManifestDecodedRecord::Complete {
                        record,
                        next_cursor,
                    } => {
                        grammar.validate_and_apply(&record)?;
                        cursor = next_cursor;
                        replayed_records.push(record);
                    }
                    ManifestDecodedRecord::TornTail { truncate_to_len } => {
                        bail!(
                            "manifest replay encountered torn tail at offset {} before reaching record_seq {}",
                            truncate_to_len,
                            target_record_seq.0
                        );
                    }
                }
            }

            if grammar.last_record_seq() != Some(target_record_seq) {
                bail!(
                    "manifest replay could not reach target record_seq {}",
                    target_record_seq.0
                );
            }

            Ok(ManifestReplayResult {
                checkpoint: checkpoint.clone(),
                replayed_records,
                final_state: grammar,
                metrics: ManifestReplayMetrics {
                    checkpoint_seq: checkpoint.as_ref().map(|value| value.checkpoint_seq),
                    replayed_records: checkpoint
                        .as_ref()
                        .map(|value| target_record_seq.0.saturating_sub(value.last_record_seq.0))
                        .unwrap_or(target_record_seq.0),
                    replayed_bytes: cursor as u64 - start_offset,
                    replay_duration: replay_started.elapsed(),
                },
            })
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct ManifestRecoveryEvent {
        pub(crate) event: ManifestSenderStateEvent,
        pub(crate) detail: Box<str>,
    }

    impl ManifestRecoveryEvent {
        pub(crate) fn to_discovery_skip_reason(&self) -> ManifestDiscoverySkipReason {
            ManifestDiscoverySkipReason::SenderState {
                event: self.event,
                detail: self.detail.clone(),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct ManifestSpoolPaths {
        pub(crate) state_dir: PathBuf,
        pub(crate) log_path: PathBuf,
        pub(crate) checkpoint_path: PathBuf,
        pub(crate) meta_path: PathBuf,
    }

    impl ManifestSpoolPaths {
        pub(crate) fn for_dump(state_root: &Path, dump_id: &str) -> Self {
            let state_dir = state_root.join(manifest_state_dir_name(dump_id));
            Self {
                log_path: state_dir.join(MANIFEST_LOG_FILE_NAME),
                checkpoint_path: state_dir.join(MANIFEST_CHECKPOINT_FILE_NAME),
                meta_path: state_dir.join(MANIFEST_META_FILE_NAME),
                state_dir,
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct ManifestSpoolState {
        pub(crate) paths: ManifestSpoolPaths,
        pub(crate) meta: ManifestMeta,
        pub(crate) last_checkpoint: Option<ManifestCheckpoint>,
        pub(crate) recovery_events: Vec<ManifestRecoveryEvent>,
        pub(crate) fresh_log_bytes: Option<Arc<[u8]>>,
    }

    impl ManifestSpoolState {
        pub(crate) fn open_or_recover(
            state_root: &Path,
            dump_id: &str,
            source_identity: ManifestSourceIdentity,
            root_source_path: &str,
        ) -> Result<Self> {
            let paths = ManifestSpoolPaths::for_dump(state_root, dump_id);
            fs::create_dir_all(&paths.state_dir)?;

            let mut recovery_events = Vec::new();
            if !paths.meta_path.exists() {
                let has_stale_state = paths.log_path.exists() || paths.checkpoint_path.exists();
                if has_stale_state {
                    remove_if_exists(&paths.log_path)?;
                    remove_if_exists(&paths.checkpoint_path)?;
                    recovery_events.push(ManifestRecoveryEvent {
                        event: ManifestSenderStateEvent::Rebuilt,
                        detail: "sender manifest state rebuilt because meta was missing".into(),
                    });
                }

                let meta = ManifestMeta::new(dump_id, source_identity, root_source_path)?;
                meta.write_atomic(&paths.meta_path)?;
                return Ok(Self {
                    paths,
                    meta,
                    last_checkpoint: None,
                    recovery_events,
                    fresh_log_bytes: None,
                });
            }

            let loaded_meta = match ManifestMeta::read_from_path(&paths.meta_path) {
                Ok(meta) => meta,
                Err(error) => {
                    return Self::rebuild_with_events(
                        paths,
                        dump_id,
                        source_identity,
                        root_source_path,
                        vec![
                            ManifestRecoveryEvent {
                                event: ManifestSenderStateEvent::Corrupted,
                                detail: format!("manifest meta is corrupted: {error}").into(),
                            },
                            ManifestRecoveryEvent {
                                event: ManifestSenderStateEvent::Rebuilt,
                                detail: "sender manifest state rebuilt after meta corruption"
                                    .into(),
                            },
                        ],
                        Some(ManifestInvalidationReason::SpoolCorrupted),
                    );
                }
            };

            match loaded_meta.validate_reuse_for_source(dump_id, &source_identity)? {
                ManifestReuseDecision::InvalidateAndRebuild { reason } => {
                    let detail = match reason {
                        ManifestInvalidationReason::DumpIdMismatch => {
                            "sender manifest state invalidated because dump_id changed"
                        }
                        ManifestInvalidationReason::BuilderVersionMismatch => {
                            "sender manifest state invalidated because manifest builder version changed"
                        }
                        ManifestInvalidationReason::SourceIdentityMismatch => {
                            "sender manifest state invalidated because source identity changed"
                        }
                        ManifestInvalidationReason::UnsealedLivePathRestart => {
                            "sender manifest state invalidated because unsealed live-path spool cannot continue after restart"
                        }
                        ManifestInvalidationReason::SpoolCorrupted => {
                            "sender manifest state invalidated because spool corruption was detected"
                        }
                        ManifestInvalidationReason::RebuildRequested => {
                            "sender manifest state invalidated because rebuild was requested"
                        }
                    };
                    return Self::rebuild_with_events(
                        paths,
                        dump_id,
                        source_identity,
                        root_source_path,
                        vec![
                            ManifestRecoveryEvent {
                                event: ManifestSenderStateEvent::Invalidated,
                                detail: detail.into(),
                            },
                            ManifestRecoveryEvent {
                                event: ManifestSenderStateEvent::Rebuilt,
                                detail: "sender manifest state rebuilt after invalidation".into(),
                            },
                        ],
                        Some(reason),
                    );
                }
                ManifestReuseDecision::ReuseSealed
                | ManifestReuseDecision::ContinueUnsealedBuild => {}
            }

            let last_checkpoint = if paths.checkpoint_path.exists() {
                match ManifestCheckpoint::read_from_path(&paths.checkpoint_path) {
                    Ok(checkpoint) => Some(checkpoint),
                    Err(error) => {
                        return Self::rebuild_with_events(
                            paths,
                            dump_id,
                            source_identity,
                            root_source_path,
                            vec![
                                ManifestRecoveryEvent {
                                    event: ManifestSenderStateEvent::Corrupted,
                                    detail: format!("manifest checkpoint is corrupted: {error}")
                                        .into(),
                                },
                                ManifestRecoveryEvent {
                                    event: ManifestSenderStateEvent::Rebuilt,
                                    detail:
                                        "sender manifest state rebuilt after checkpoint corruption"
                                            .into(),
                                },
                            ],
                            Some(ManifestInvalidationReason::SpoolCorrupted),
                        );
                    }
                }
            } else {
                None
            };

            if let Some(detail) = checkpoint_meta_consistency_detail(
                &loaded_meta,
                last_checkpoint.as_ref(),
                paths.log_path.exists(),
            ) {
                return Self::rebuild_with_events(
                    paths,
                    dump_id,
                    source_identity,
                    root_source_path,
                    vec![
                        ManifestRecoveryEvent {
                            event: ManifestSenderStateEvent::Corrupted,
                            detail: detail.into(),
                        },
                        ManifestRecoveryEvent {
                            event: ManifestSenderStateEvent::Rebuilt,
                            detail: "sender manifest state rebuilt after meta/checkpoint mismatch"
                                .into(),
                        },
                    ],
                    Some(ManifestInvalidationReason::SpoolCorrupted),
                );
            }

            if paths.log_path.exists() {
                let log_bytes = fs::read(&paths.log_path).with_context(|| {
                    format!("failed to read manifest log {}", paths.log_path.display())
                })?;
                let recovery = match ManifestSpoolReader::recover_prefix_bytes(&log_bytes) {
                    Ok(report) => report,
                    Err(error) => {
                        return Self::rebuild_with_events(
                            paths,
                            dump_id,
                            source_identity,
                            root_source_path,
                            vec![
                                ManifestRecoveryEvent {
                                    event: ManifestSenderStateEvent::Corrupted,
                                    detail: format!("manifest log is corrupted: {error}").into(),
                                },
                                ManifestRecoveryEvent {
                                    event: ManifestSenderStateEvent::Rebuilt,
                                    detail: "sender manifest state rebuilt after log corruption"
                                        .into(),
                                },
                            ],
                            Some(ManifestInvalidationReason::SpoolCorrupted),
                        );
                    }
                };

                if recovery.truncate_to_len < log_bytes.len() as u64 {
                    let file = OpenOptions::new()
                        .write(true)
                        .open(&paths.log_path)
                        .with_context(|| {
                            format!(
                                "failed to open manifest log for truncation {}",
                                paths.log_path.display()
                            )
                        })?;
                    file.set_len(recovery.truncate_to_len).with_context(|| {
                        format!(
                            "failed to truncate manifest log {}",
                            paths.log_path.display()
                        )
                    })?;
                }

                if let Some(checkpoint) = &last_checkpoint {
                    let recovered_last_record_seq =
                        recovery.records.last().map(|record| record.record_seq.0);
                    if checkpoint.log_offset > recovery.truncate_to_len
                        || recovered_last_record_seq
                            .map(|value| checkpoint.last_record_seq.0 > value)
                            .unwrap_or(checkpoint.last_record_seq.0 != 0)
                    {
                        return Self::rebuild_with_events(
                            paths,
                            dump_id,
                            source_identity,
                            root_source_path,
                            vec![
                                ManifestRecoveryEvent {
                                    event: ManifestSenderStateEvent::Corrupted,
                                    detail: "manifest checkpoint points beyond the recovered log prefix".into(),
                                },
                                ManifestRecoveryEvent {
                                    event: ManifestSenderStateEvent::Rebuilt,
                                    detail: "sender manifest state rebuilt after checkpoint/log mismatch".into(),
                                },
                            ],
                            Some(ManifestInvalidationReason::SpoolCorrupted),
                        );
                    }
                }
            }

            Ok(Self {
                paths,
                meta: loaded_meta,
                last_checkpoint,
                recovery_events,
                fresh_log_bytes: None,
            })
        }

        fn rebuild_with_events(
            paths: ManifestSpoolPaths,
            dump_id: &str,
            source_identity: ManifestSourceIdentity,
            root_source_path: &str,
            recovery_events: Vec<ManifestRecoveryEvent>,
            invalidation_reason: Option<ManifestInvalidationReason>,
        ) -> Result<Self> {
            remove_if_exists(&paths.log_path)?;
            remove_if_exists(&paths.checkpoint_path)?;

            let mut meta = ManifestMeta::new(dump_id, source_identity, root_source_path)?;
            if let Some(reason) = invalidation_reason {
                meta.invalidate(reason);
            }
            meta.write_atomic(&paths.meta_path)?;

            Ok(Self {
                paths,
                meta,
                last_checkpoint: None,
                recovery_events,
                fresh_log_bytes: None,
            })
        }

        pub(crate) fn persist_meta(&self) -> Result<()> {
            self.meta.write_atomic(&self.paths.meta_path)
        }

        pub(crate) fn recover_manifest_prefix(&self) -> Result<ManifestRecoveryReport> {
            if let Some(bytes) = &self.fresh_log_bytes {
                let recovery = ManifestSpoolReader::recover_prefix_bytes(bytes.as_ref())?;
                if recovery.truncate_to_len != bytes.len() as u64 {
                    bail!(
                        "sender cached manifest spool ended with a torn tail: {} < {} bytes",
                        recovery.truncate_to_len,
                        bytes.len()
                    );
                }
                return Ok(recovery);
            }

            let log_bytes = fs::read(&self.paths.log_path).with_context(|| {
                format!(
                    "failed to read sender manifest spool {}",
                    self.paths.log_path.display()
                )
            })?;
            let recovery = ManifestSpoolReader::recover_prefix_bytes(&log_bytes)?;
            if recovery.truncate_to_len != log_bytes.len() as u64 {
                bail!(
                    "sender sealed manifest spool ended with a torn tail: {} < {} bytes",
                    recovery.truncate_to_len,
                    log_bytes.len()
                );
            }
            Ok(recovery)
        }

        pub(crate) fn recovery_issue_reasons(&self) -> Vec<ManifestDiscoverySkipReason> {
            self.recovery_events
                .iter()
                .map(ManifestRecoveryEvent::to_discovery_skip_reason)
                .collect()
        }

        pub(crate) fn cleanup_after_finish(&self) -> Result<()> {
            if self.paths.state_dir.exists() {
                fs::remove_dir_all(&self.paths.state_dir).with_context(|| {
                    format!(
                        "failed to remove sender manifest state directory {}",
                        self.paths.state_dir.display()
                    )
                })?;
            }
            Ok(())
        }

        pub(crate) fn collect_gc_candidates(
            state_root: &Path,
            now: SystemTime,
            max_age: Duration,
        ) -> Result<Vec<PathBuf>> {
            if !state_root.exists() {
                return Ok(Vec::new());
            }

            let mut candidates = Vec::new();
            for entry in fs::read_dir(state_root).with_context(|| {
                format!(
                    "failed to read sender manifest state root {}",
                    state_root.display()
                )
            })? {
                let entry = entry?;
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }

                let modified = entry.metadata()?.modified().unwrap_or(now);
                let age = now.duration_since(modified).unwrap_or_default();
                if age >= max_age {
                    candidates.push(path);
                }
            }

            candidates.sort();
            Ok(candidates)
        }
    }

    fn checkpoint_meta_consistency_detail(
        meta: &ManifestMeta,
        checkpoint: Option<&ManifestCheckpoint>,
        log_exists: bool,
    ) -> Option<String> {
        let meta_tracks_checkpoint = meta.last_checkpoint_seq.is_some()
            || meta.last_record_seq.is_some()
            || meta.last_work_ordinal.is_some();

        match checkpoint {
            Some(checkpoint) => {
                if !log_exists {
                    return Some("manifest checkpoint exists but manifest log is missing".into());
                }
                if meta.last_checkpoint_seq != Some(checkpoint.checkpoint_seq) {
                    return Some(
                        "manifest meta checkpoint sequence does not match checkpoint file".into(),
                    );
                }
                if meta.last_record_seq != Some(checkpoint.last_record_seq) {
                    return Some(
                        "manifest meta last_record_seq does not match checkpoint file".into(),
                    );
                }
                if meta.last_work_ordinal != checkpoint.last_work_ordinal {
                    return Some(
                        "manifest meta last_work_ordinal does not match checkpoint file".into(),
                    );
                }
            }
            None => {
                if meta_tracks_checkpoint {
                    return Some(
                        "manifest meta advertises checkpoint progress but checkpoint file is missing"
                            .into(),
                    );
                }
            }
        }

        None
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub(crate) struct ManifestBuildReport {
        pub(crate) records_written: u64,
        pub(crate) work_items_written: u64,
        pub(crate) discovery_issues_written: u64,
        pub(crate) subtree_issues_written: u64,
        pub(crate) spool_bytes_written: u64,
        pub(crate) final_record_seq: Option<ManifestRecordSeq>,
        pub(crate) final_work_ordinal: Option<ManifestWorkOrdinal>,
    }

    type ManifestBuildWithCheckpoint = (ManifestBuildReport, Option<ManifestCheckpoint>);
    type ManifestBuildWithCheckpointAndCache = (
        ManifestBuildReport,
        Option<ManifestCheckpoint>,
        Option<Arc<[u8]>>,
    );

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    struct ManifestCheckpointProgress {
        stride: Option<u64>,
        next_checkpoint_seq: u64,
        last_checkpoint: Option<ManifestCheckpoint>,
    }

    impl ManifestCheckpointProgress {
        fn disabled() -> Self {
            Self {
                stride: None,
                next_checkpoint_seq: 1,
                last_checkpoint: None,
            }
        }

        fn enabled(stride: u64) -> Result<Self> {
            if stride == 0 {
                bail!("manifest checkpoint stride must be at least 1");
            }
            Ok(Self {
                stride: Some(stride),
                next_checkpoint_seq: 1,
                last_checkpoint: None,
            })
        }

        // Во время build нам нужен только самый свежий sparse checkpoint:
        // recovery-path хранит один checkpoint-файл, поэтому держать весь список не нужно.
        fn observe_record(&mut self, grammar: &ManifestGrammarState, log_offset: u64) {
            let Some(stride) = self.stride else {
                return;
            };
            let Some(last_record_seq) = grammar.last_record_seq() else {
                return;
            };
            if last_record_seq.0 % stride != 0 {
                return;
            }
            if let Some(checkpoint) =
                grammar.snapshot_checkpoint(self.next_checkpoint_seq, log_offset)
            {
                self.last_checkpoint = Some(checkpoint);
                self.next_checkpoint_seq = self.next_checkpoint_seq.saturating_add(1);
            }
        }

        fn finish(
            mut self,
            grammar: &ManifestGrammarState,
            log_offset: u64,
        ) -> Option<ManifestCheckpoint> {
            let _ = self.stride?;
            let last_record_seq = grammar.last_record_seq()?;
            let needs_final_checkpoint = self
                .last_checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.last_record_seq != last_record_seq)
                .unwrap_or(true);
            if needs_final_checkpoint
                && let Some(checkpoint) =
                    grammar.snapshot_checkpoint(self.next_checkpoint_seq, log_offset)
            {
                self.last_checkpoint = Some(checkpoint);
            }
            self.last_checkpoint
        }
    }

    #[derive(Debug, Default)]
    struct ManifestLogCacheProgress {
        max_bytes: usize,
        cached_bytes: Option<Vec<u8>>,
    }

    impl ManifestLogCacheProgress {
        fn disabled() -> Self {
            Self::default()
        }

        fn enabled(max_bytes: usize) -> Self {
            Self {
                max_bytes,
                cached_bytes: Some(Vec::new()),
            }
        }

        fn observe_record(&mut self, encoded: &[u8]) {
            let Some(cached_bytes) = self.cached_bytes.as_mut() else {
                return;
            };
            if cached_bytes.len().saturating_add(encoded.len()) > self.max_bytes {
                self.cached_bytes = None;
                return;
            }
            cached_bytes.extend_from_slice(encoded);
        }

        fn finish(self) -> Option<Arc<[u8]>> {
            self.cached_bytes.map(Arc::<[u8]>::from)
        }
    }

    #[derive(Debug, Clone)]
    struct ManifestBuildDirectory {
        name_component: Box<str>,
        depth: u16,
    }

    #[derive(Debug, Clone)]
    struct ManifestBuildFrame {
        directory: Option<ManifestBuildDirectory>,
        depth: u16,
        entries: Vec<super::DiscoveredEntry>,
    }

    #[derive(Debug, Clone)]
    pub(crate) struct ManifestSpoolBuilder {
        source: PathBuf,
        listing_pool: super::DirectoryListingPool,
        stack: Vec<ManifestBuildFrame>,
        pending_records: VecDeque<ManifestRecord>,
        grammar: ManifestGrammarState,
        next_record_seq: u64,
        next_work_ordinal: u64,
    }

    impl ManifestSpoolBuilder {
        pub(crate) fn new(source: impl Into<PathBuf>) -> Result<Self> {
            let source = source.into();
            super::validate_source_directory(&source)?;
            let classifier = super::EntryClassifier::new();
            let listing_pool = super::DirectoryListingPool::new(&source, classifier.clone(), None);
            let root_entries = super::read_sorted_entries(&source, &classifier, &source)?;
            listing_pool.prefetch_directories(&root_entries);
            Ok(Self {
                source,
                listing_pool,
                stack: vec![ManifestBuildFrame {
                    directory: None,
                    depth: 0,
                    entries: root_entries,
                }],
                pending_records: VecDeque::new(),
                grammar: ManifestGrammarState::default(),
                next_record_seq: 1,
                next_work_ordinal: 1,
            })
        }

        pub(crate) fn build_to_path(
            source: impl Into<PathBuf>,
            spool_path: &Path,
        ) -> Result<ManifestBuildReport> {
            Self::build_to_path_with_limit(source, spool_path, u64::MAX)
        }

        pub(crate) fn build_to_path_with_limit(
            source: impl Into<PathBuf>,
            spool_path: &Path,
            max_bytes: u64,
        ) -> Result<ManifestBuildReport> {
            let (report, _) =
                Self::build_to_path_with_limit_and_checkpoint(source, spool_path, max_bytes, None)?;
            Ok(report)
        }

        pub(crate) fn build_to_path_with_limit_and_checkpoint(
            source: impl Into<PathBuf>,
            spool_path: &Path,
            max_bytes: u64,
            checkpoint_stride: Option<u64>,
        ) -> Result<ManifestBuildWithCheckpoint> {
            let (report, last_checkpoint, _) =
                Self::build_to_path_with_limit_and_checkpoint_and_cache(
                    source,
                    spool_path,
                    max_bytes,
                    checkpoint_stride,
                    None,
                )?;
            Ok((report, last_checkpoint))
        }

        pub(crate) fn build_to_path_with_limit_and_checkpoint_and_cache(
            source: impl Into<PathBuf>,
            spool_path: &Path,
            max_bytes: u64,
            checkpoint_stride: Option<u64>,
            cache_max_bytes: Option<usize>,
        ) -> Result<ManifestBuildWithCheckpointAndCache> {
            if let Some(parent) = spool_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(spool_path)
                .with_context(|| {
                    format!("failed to open manifest spool {}", spool_path.display())
                })?;
            let mut builder = Self::new(source)?;
            builder.build_into_with_limit_and_checkpoint_and_cache(
                &mut file,
                max_bytes,
                checkpoint_stride,
                cache_max_bytes,
            )
        }

        pub(crate) fn build_into<W: Write>(
            &mut self,
            writer: &mut W,
        ) -> Result<ManifestBuildReport> {
            self.build_into_with_limit(writer, u64::MAX)
        }

        pub(crate) fn build_into_with_limit<W: Write>(
            &mut self,
            writer: &mut W,
            max_bytes: u64,
        ) -> Result<ManifestBuildReport> {
            let (report, _) = self.build_into_with_limit_and_checkpoint(writer, max_bytes, None)?;
            Ok(report)
        }

        fn build_into_with_limit_and_checkpoint<W: Write>(
            &mut self,
            writer: &mut W,
            max_bytes: u64,
            checkpoint_stride: Option<u64>,
        ) -> Result<ManifestBuildWithCheckpoint> {
            let (report, last_checkpoint, _) = self
                .build_into_with_limit_and_checkpoint_and_cache(
                    writer,
                    max_bytes,
                    checkpoint_stride,
                    None,
                )?;
            Ok((report, last_checkpoint))
        }

        fn build_into_with_limit_and_checkpoint_and_cache<W: Write>(
            &mut self,
            writer: &mut W,
            max_bytes: u64,
            checkpoint_stride: Option<u64>,
            cache_max_bytes: Option<usize>,
        ) -> Result<ManifestBuildWithCheckpointAndCache> {
            if max_bytes == 0 {
                bail!("sender manifest spool quota must be at least 1 byte");
            }

            let mut report = ManifestBuildReport::default();
            let mut checkpoint_progress = match checkpoint_stride {
                Some(stride) => ManifestCheckpointProgress::enabled(stride)?,
                None => ManifestCheckpointProgress::disabled(),
            };
            let mut log_cache_progress = cache_max_bytes
                .map(ManifestLogCacheProgress::enabled)
                .unwrap_or_else(ManifestLogCacheProgress::disabled);
            while let Some(record) = self.next_record()? {
                let encoded = ManifestSpoolWriter::encode_record(&record)?;
                let next_total = report
                    .spool_bytes_written
                    .saturating_add(encoded.len() as u64);
                if next_total > max_bytes {
                    bail!(
                        "sender manifest spool quota exceeded at record_seq {}: attempted={} limit={}",
                        record.record_seq.0,
                        next_total,
                        max_bytes
                    );
                }
                writer.write_all(&encoded)?;
                log_cache_progress.observe_record(&encoded);
                report.records_written = report.records_written.saturating_add(1);
                report.spool_bytes_written = next_total;
                checkpoint_progress.observe_record(&self.grammar, report.spool_bytes_written);
                match record.kind {
                    ManifestRecordKind::File | ManifestRecordKind::Symlink => {
                        report.work_items_written = report.work_items_written.saturating_add(1);
                    }
                    ManifestRecordKind::DiscoverySkip => {
                        report.discovery_issues_written =
                            report.discovery_issues_written.saturating_add(1);
                    }
                    ManifestRecordKind::SubtreeSkip => {
                        report.subtree_issues_written =
                            report.subtree_issues_written.saturating_add(1);
                    }
                    ManifestRecordKind::DirectoryEnter | ManifestRecordKind::DirectoryLeave => {}
                }
            }
            report.final_record_seq = self.grammar.last_record_seq();
            report.final_work_ordinal = self.grammar.last_work_ordinal();
            let last_checkpoint =
                checkpoint_progress.finish(&self.grammar, report.spool_bytes_written);
            let fresh_log_bytes = log_cache_progress.finish();
            Ok((report, last_checkpoint, fresh_log_bytes))
        }

        fn next_record(&mut self) -> Result<Option<ManifestRecord>> {
            if let Some(record) = self.pending_records.pop_front() {
                return Ok(Some(record));
            }

            loop {
                let Some(frame) = self.stack.last_mut() else {
                    return Ok(None);
                };

                if let Some(entry) = frame.entries.pop() {
                    let depth = frame.depth;
                    let name_component = manifest_name_component(&entry.rel_path)?;
                    return match entry.kind {
                        super::DiscoveredEntryKind::Regular => self
                            .build_record(
                                ManifestRecordKind::File,
                                depth,
                                name_component,
                                ManifestRecordPayload::None,
                            )
                            .map(Some),
                        super::DiscoveredEntryKind::Symlink { target } => self
                            .build_record(
                                ManifestRecordKind::Symlink,
                                depth,
                                name_component,
                                ManifestRecordPayload::SymlinkTarget {
                                    target: target.to_string_lossy().into_owned().into_boxed_str(),
                                },
                            )
                            .map(Some),
                        super::DiscoveredEntryKind::Skipped(reason) => self
                            .build_record(
                                ManifestRecordKind::DiscoverySkip,
                                depth,
                                name_component,
                                ManifestRecordPayload::DiscoverySkip {
                                    reason: manifest_discovery_skip_reason(&reason),
                                },
                            )
                            .map(Some),
                        super::DiscoveredEntryKind::Directory => {
                            let enter = self.build_record(
                                ManifestRecordKind::DirectoryEnter,
                                depth,
                                name_component.clone(),
                                ManifestRecordPayload::None,
                            )?;
                            match self.listing_pool.take_or_read(&entry.abs_path) {
                                Ok(entries) => {
                                    self.listing_pool.prefetch_directories(&entries);
                                    self.stack.push(ManifestBuildFrame {
                                        directory: Some(ManifestBuildDirectory {
                                            name_component,
                                            depth,
                                        }),
                                        depth: depth.saturating_add(1),
                                        entries,
                                    });
                                }
                                Err(error) => {
                                    let subtree_skip = self.build_record(
                                        ManifestRecordKind::SubtreeSkip,
                                        depth,
                                        name_component.clone(),
                                        ManifestRecordPayload::SubtreeSkip {
                                            reason: ManifestSubtreeSkipReason::ReadDirFailed {
                                                error: error.to_string().into(),
                                            },
                                        },
                                    )?;
                                    let leave = self.build_record(
                                        ManifestRecordKind::DirectoryLeave,
                                        depth,
                                        name_component,
                                        ManifestRecordPayload::None,
                                    )?;
                                    self.pending_records.push_back(subtree_skip);
                                    self.pending_records.push_back(leave);
                                }
                            }
                            Ok(Some(enter))
                        }
                    };
                }

                let Some(frame) = self.stack.pop() else {
                    return Ok(None);
                };
                let Some(directory) = frame.directory else {
                    continue;
                };
                return self
                    .build_record(
                        ManifestRecordKind::DirectoryLeave,
                        directory.depth,
                        directory.name_component,
                        ManifestRecordPayload::None,
                    )
                    .map(Some);
            }
        }

        fn build_record(
            &mut self,
            kind: ManifestRecordKind,
            depth: u16,
            name_component: impl Into<Box<str>>,
            payload: ManifestRecordPayload,
        ) -> Result<ManifestRecord> {
            let work_ordinal = kind
                .requires_work_ordinal()
                .then_some(ManifestWorkOrdinal(self.next_work_ordinal));
            let record = ManifestRecord::new(
                ManifestRecordSeq(self.next_record_seq),
                kind,
                depth,
                name_component,
                work_ordinal,
                payload,
            )?;
            self.grammar.validate_and_apply(&record)?;
            self.next_record_seq = self.next_record_seq.saturating_add(1);
            if kind.requires_work_ordinal() {
                self.next_work_ordinal = self.next_work_ordinal.saturating_add(1);
            }
            Ok(record)
        }
    }

    fn manifest_name_component(rel_path: &str) -> Result<Box<str>> {
        let component = rel_path
            .rsplit('/')
            .next()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow::anyhow!("manifest entry path must not be empty"))?;
        Ok(component.to_owned().into_boxed_str())
    }

    fn manifest_discovery_skip_reason(
        reason: &super::SkippedEntryReason,
    ) -> ManifestDiscoverySkipReason {
        match reason {
            super::SkippedEntryReason::Unsupported(kind) => {
                ManifestDiscoverySkipReason::UnsupportedEntry {
                    kind: manifest_special_entry_kind(*kind),
                }
            }
            super::SkippedEntryReason::UnreadableSymlinkTarget { error } => {
                ManifestDiscoverySkipReason::UnreadableSymlinkTarget {
                    error: error.clone().into_boxed_str(),
                }
            }
            super::SkippedEntryReason::VirtualFilesystem { kind } => {
                ManifestDiscoverySkipReason::VirtualFilesystem {
                    kind: (*kind).into(),
                }
            }
        }
    }

    fn manifest_special_entry_kind(kind: super::UnsupportedEntryKind) -> ManifestSpecialEntryKind {
        match kind {
            super::UnsupportedEntryKind::Socket => ManifestSpecialEntryKind::Socket,
            super::UnsupportedEntryKind::Fifo => ManifestSpecialEntryKind::Fifo,
            super::UnsupportedEntryKind::BlockDevice => ManifestSpecialEntryKind::BlockDevice,
            super::UnsupportedEntryKind::CharDevice => ManifestSpecialEntryKind::CharDevice,
            super::UnsupportedEntryKind::Other => ManifestSpecialEntryKind::Other,
        }
    }

    #[derive(Debug)]
    pub(crate) struct ManifestPendingLeafWalker {
        pub(crate) state: ManifestSpoolState,
        resume_state: FileResumeState,
        root_prefix: Arc<super::InputPathPrefix>,
        current_offset: u64,
        prefix_stack: Vec<Arc<super::InputPathPrefix>>,
        current_file: Option<File>,
        profile: Option<super::PipelineProfileCollector>,
        discovery_issues: Vec<super::SourceDiscoveryIssue>,
        discovery_issue_summary: super::SourceDiscoveryIssueSummary,
        resume_validated: bool,
        exhausted: bool,
        last_boundary_file: Option<super::InputFile>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct ManifestChunkPlan {
        pub(crate) bytes: Vec<u8>,
        pub(crate) last_record_seq: Option<u64>,
        pub(crate) last_work_ordinal: Option<u64>,
        pub(crate) record_count: u64,
    }

    #[derive(Debug)]
    pub(crate) struct ProgressiveManifestPendingLeafWalker {
        pub(crate) state: ManifestSpoolState,
        builder: ManifestSpoolBuilder,
        resume_state: FileResumeState,
        root_prefix: Arc<super::InputPathPrefix>,
        prefix_stack: Vec<Arc<super::InputPathPrefix>>,
        spool_file: Option<File>,
        spool_len: u64,
        spool_max_bytes: u64,
        manifest_chunk_bytes: usize,
        manifest_chunk_records: u64,
        ready_chunks: VecDeque<ManifestChunkPlan>,
        current_chunk_bytes: Vec<u8>,
        current_chunk_last_record_seq: Option<u64>,
        current_chunk_last_work_ordinal: Option<u64>,
        current_chunk_record_count: u64,
        carried_last_work_ordinal: Option<u64>,
        produced_last_record_seq: Option<u64>,
        produced_last_work_ordinal: Option<u64>,
        profile: Option<super::PipelineProfileCollector>,
        discovery_issues: Vec<super::SourceDiscoveryIssue>,
        discovery_issue_summary: super::SourceDiscoveryIssueSummary,
        resume_validated: bool,
        exhausted: bool,
        last_boundary_file: Option<super::InputFile>,
    }

    impl Clone for ManifestPendingLeafWalker {
        fn clone(&self) -> Self {
            Self {
                state: self.state.clone(),
                resume_state: self.resume_state.clone(),
                root_prefix: self.root_prefix.clone(),
                current_offset: self.current_offset,
                prefix_stack: self.prefix_stack.clone(),
                current_file: None,
                profile: self.profile.clone(),
                discovery_issues: self.discovery_issues.clone(),
                discovery_issue_summary: self.discovery_issue_summary,
                resume_validated: self.resume_validated,
                exhausted: self.exhausted,
                last_boundary_file: self.last_boundary_file.clone(),
            }
        }
    }

    impl Clone for ProgressiveManifestPendingLeafWalker {
        fn clone(&self) -> Self {
            Self {
                state: self.state.clone(),
                builder: self.builder.clone(),
                resume_state: self.resume_state.clone(),
                root_prefix: self.root_prefix.clone(),
                prefix_stack: self.prefix_stack.clone(),
                spool_file: None,
                spool_len: self.spool_len,
                spool_max_bytes: self.spool_max_bytes,
                manifest_chunk_bytes: self.manifest_chunk_bytes,
                manifest_chunk_records: self.manifest_chunk_records,
                ready_chunks: self.ready_chunks.clone(),
                current_chunk_bytes: self.current_chunk_bytes.clone(),
                current_chunk_last_record_seq: self.current_chunk_last_record_seq,
                current_chunk_last_work_ordinal: self.current_chunk_last_work_ordinal,
                current_chunk_record_count: self.current_chunk_record_count,
                carried_last_work_ordinal: self.carried_last_work_ordinal,
                produced_last_record_seq: self.produced_last_record_seq,
                produced_last_work_ordinal: self.produced_last_work_ordinal,
                profile: self.profile.clone(),
                discovery_issues: self.discovery_issues.clone(),
                discovery_issue_summary: self.discovery_issue_summary,
                resume_validated: self.resume_validated,
                exhausted: self.exhausted,
                last_boundary_file: self.last_boundary_file.clone(),
            }
        }
    }

    impl ProgressiveManifestPendingLeafWalker {
        #[allow(clippy::too_many_arguments)]
        pub(crate) fn new(
            source_root: impl Into<PathBuf>,
            resume_state: &FileResumeState,
            state: ManifestSpoolState,
            base_manifest: &crate::network::protocol::ManifestBoundary,
            spool_max_bytes: u64,
            manifest_chunk_bytes: usize,
            manifest_chunk_records: u64,
            profile: Option<&super::PipelineProfileCollector>,
        ) -> Result<Self> {
            if manifest_chunk_bytes == 0 {
                bail!("manifest chunk bytes must be at least 1");
            }
            if manifest_chunk_records == 0 {
                bail!("manifest chunk records must be at least 1");
            }
            if spool_max_bytes == 0 {
                bail!("progressive manifest spool quota must be at least 1");
            }

            let source_root = source_root.into();
            let root_prefix = super::InputPathPrefix::root(source_root.clone());
            let discovery_issues = super::collect_source_discovery_issues(&state)?;
            let discovery_issue_summary =
                super::summarize_source_discovery_issues(&discovery_issues);
            Ok(Self {
                state,
                builder: ManifestSpoolBuilder::new(source_root)?,
                resume_state: resume_state.clone(),
                root_prefix,
                prefix_stack: Vec::new(),
                spool_file: None,
                spool_len: 0,
                spool_max_bytes,
                manifest_chunk_bytes,
                manifest_chunk_records,
                ready_chunks: VecDeque::new(),
                current_chunk_bytes: Vec::new(),
                current_chunk_last_record_seq: None,
                current_chunk_last_work_ordinal: None,
                current_chunk_record_count: 0,
                carried_last_work_ordinal: base_manifest.last_work_ordinal,
                produced_last_record_seq: None,
                produced_last_work_ordinal: None,
                profile: profile.cloned(),
                discovery_issues,
                discovery_issue_summary,
                resume_validated: resume_state.last_committed_file_index == 0,
                exhausted: false,
                last_boundary_file: None,
            })
        }

        pub(crate) fn next_pending_file(&mut self) -> Result<Option<super::InputFile>> {
            if self.exhausted {
                return Ok(None);
            }

            loop {
                let Some(record) = self.next_manifest_record()? else {
                    self.finish_walk()?;
                    return Ok(None);
                };

                match record.kind {
                    ManifestRecordKind::DirectoryEnter => {
                        let prefix = super::InputPathPrefix::child(
                            &self.current_prefix(),
                            record.name_component.as_ref(),
                        );
                        self.prefix_stack.push(prefix);
                    }
                    ManifestRecordKind::DirectoryLeave => {
                        let Some(current) = self.prefix_stack.pop() else {
                            bail!(
                                "progressive manifest replay encountered DirectoryLeave without open directory"
                            );
                        };
                        if current.name_component().unwrap_or("") != record.name_component.as_ref()
                        {
                            bail!(
                                "progressive manifest replay encountered mismatched DirectoryLeave: expected {} got {}",
                                current.name_component().unwrap_or(""),
                                record.name_component
                            );
                        }
                    }
                    ManifestRecordKind::DiscoverySkip => {
                        let ManifestRecordPayload::DiscoverySkip { reason } = record.payload else {
                            bail!("manifest discovery skip record is missing discovery payload");
                        };
                        self.discovery_issues.push(super::SourceDiscoveryIssue {
                            record_seq: Some(record.record_seq.0),
                            path: super::source_discovery_issue_path(
                                &self.path_stack_components(),
                                record.depth,
                                record.name_component.as_ref(),
                            )?,
                            scope: super::SourceDiscoveryIssueScope::Entry,
                            reason: super::source_discovery_reason_from_manifest_skip(&reason),
                        });
                        self.discovery_issue_summary =
                            super::summarize_source_discovery_issues(&self.discovery_issues);
                    }
                    ManifestRecordKind::SubtreeSkip => {
                        let ManifestRecordPayload::SubtreeSkip { reason } = record.payload else {
                            bail!("manifest subtree skip record is missing subtree payload");
                        };
                        self.discovery_issues.push(super::SourceDiscoveryIssue {
                            record_seq: Some(record.record_seq.0),
                            path: super::source_discovery_issue_path(
                                &self.path_stack_components(),
                                record.depth,
                                record.name_component.as_ref(),
                            )?,
                            scope: super::SourceDiscoveryIssueScope::Subtree,
                            reason: super::source_discovery_reason_from_subtree_skip(&reason),
                        });
                        self.discovery_issue_summary =
                            super::summarize_source_discovery_issues(&self.discovery_issues);
                    }
                    ManifestRecordKind::File | ManifestRecordKind::Symlink => {
                        let work_ordinal = record
                            .work_ordinal
                            .ok_or_else(|| {
                                anyhow::anyhow!("manifest leaf record is missing work_ordinal")
                            })?
                            .0;
                        let file = match record.payload {
                            ManifestRecordPayload::None => super::InputFile::regular_from_prefix(
                                work_ordinal,
                                self.current_prefix(),
                                record.name_component,
                            ),
                            ManifestRecordPayload::SymlinkTarget { target } => {
                                super::InputFile::symlink_from_prefix(
                                    work_ordinal,
                                    self.current_prefix(),
                                    record.name_component,
                                    PathBuf::from(target.as_ref()),
                                )
                            }
                            payload => {
                                bail!(
                                    "manifest leaf record carried unexpected payload: {payload:?}"
                                )
                            }
                        };
                        self.last_boundary_file = Some(file.clone());

                        if !self.resume_validated {
                            if work_ordinal < self.resume_state.last_committed_file_index {
                                continue;
                            }
                            if work_ordinal == self.resume_state.last_committed_file_index {
                                if file.rel_path() != self.resume_state.last_committed_file_path {
                                    bail!(
                                        "sender source no longer matches resume state at file index {}: expected {} got {}",
                                        self.resume_state.last_committed_file_index,
                                        self.resume_state.last_committed_file_path,
                                        file.rel_path()
                                    );
                                }
                                self.resume_validated = true;
                                continue;
                            }
                            bail!(
                                "sender source no longer matches resume state: committed file index {} is missing before {} in progressive manifest",
                                self.resume_state.last_committed_file_index,
                                file.rel_path()
                            );
                        }

                        return Ok(Some(file));
                    }
                }
            }
        }

        pub(crate) fn pop_manifest_chunk(
            &mut self,
            force_flush: bool,
        ) -> Result<Option<ManifestChunkPlan>> {
            if force_flush {
                self.flush_current_chunk();
            }
            Ok(self.ready_chunks.pop_front())
        }

        pub(crate) fn produced_manifest_work_ordinal(&self) -> u64 {
            self.produced_last_work_ordinal.unwrap_or(0)
        }

        pub(crate) fn manifest_exhausted(&self) -> bool {
            self.exhausted
        }

        pub(crate) fn produced_manifest_boundary(&self) -> (Option<u64>, Option<u64>) {
            (
                self.produced_last_record_seq,
                self.produced_last_work_ordinal,
            )
        }

        pub(crate) fn final_manifest_boundary(&self) -> Result<(Option<u64>, Option<u64>)> {
            if !self.exhausted {
                bail!("progressive manifest build has not finished yet");
            }
            Ok((
                self.produced_last_record_seq,
                self.produced_last_work_ordinal,
            ))
        }

        pub(crate) fn final_boundary(&self) -> Result<(u64, String)> {
            if !self.exhausted {
                bail!("progressive sender pending file walker has not finished discovery yet");
            }
            Ok(self.current_boundary())
        }

        pub(crate) fn last_discovered_boundary(&self) -> (u64, String) {
            self.current_boundary()
        }

        pub(crate) fn discovery_issues(&self) -> &[super::SourceDiscoveryIssue] {
            &self.discovery_issues
        }

        pub(crate) fn discovery_issue_summary(&self) -> super::SourceDiscoveryIssueSummary {
            self.discovery_issue_summary
        }

        fn next_manifest_record(&mut self) -> Result<Option<ManifestRecord>> {
            let Some(record) = self.builder.next_record()? else {
                return Ok(None);
            };
            self.append_record(&record)?;
            Ok(Some(record))
        }

        fn append_record(&mut self, record: &ManifestRecord) -> Result<()> {
            let append_started = Instant::now();
            let encoded = ManifestSpoolWriter::encode_record(record)?;
            if encoded.len() > self.manifest_chunk_bytes {
                bail!(
                    "manifest record {} exceeds configured manifest chunk window: {} > {} bytes",
                    record.record_seq.0,
                    encoded.len(),
                    self.manifest_chunk_bytes
                );
            }
            let next_spool_len = self.spool_len.saturating_add(encoded.len() as u64);
            if next_spool_len > self.spool_max_bytes {
                bail!(
                    "sender manifest spool build failed within quota {} bytes",
                    self.spool_max_bytes
                );
            }

            let log_path = self.state.paths.log_path.clone();
            {
                let file = self.current_spool_file()?;
                file.write_all(&encoded).with_context(|| {
                    format!(
                        "failed to append progressive manifest record {} into {}",
                        record.record_seq.0,
                        log_path.display()
                    )
                })?;
            }
            self.spool_len = next_spool_len;
            if let Some(profile) = &self.profile {
                profile.record_manifest_build(encoded.len() as u64, append_started.elapsed());
            }
            self.produced_last_record_seq = Some(record.record_seq.0);
            if let Some(work_ordinal) = record.work_ordinal {
                self.produced_last_work_ordinal = Some(work_ordinal.0);
            }

            if !self.current_chunk_bytes.is_empty()
                && (self.current_chunk_bytes.len().saturating_add(encoded.len())
                    > self.manifest_chunk_bytes
                    || self.current_chunk_record_count >= self.manifest_chunk_records)
            {
                self.flush_current_chunk();
            }
            if self.current_chunk_record_count == 0 {
                self.current_chunk_last_work_ordinal = self.carried_last_work_ordinal;
            }
            self.current_chunk_bytes.extend_from_slice(&encoded);
            self.current_chunk_last_record_seq = Some(record.record_seq.0);
            if let Some(work_ordinal) = record.work_ordinal {
                self.current_chunk_last_work_ordinal = Some(work_ordinal.0);
            }
            self.current_chunk_record_count = self.current_chunk_record_count.saturating_add(1);
            Ok(())
        }

        fn flush_current_chunk(&mut self) {
            if self.current_chunk_bytes.is_empty() {
                return;
            }

            let chunk = ManifestChunkPlan {
                bytes: std::mem::take(&mut self.current_chunk_bytes),
                last_record_seq: self.current_chunk_last_record_seq.take(),
                last_work_ordinal: self.current_chunk_last_work_ordinal.take(),
                record_count: std::mem::take(&mut self.current_chunk_record_count),
            };
            self.carried_last_work_ordinal = chunk.last_work_ordinal;
            self.ready_chunks.push_back(chunk);
        }

        fn current_spool_file(&mut self) -> Result<&mut File> {
            if self.spool_file.is_none() {
                if let Some(parent) = self.state.paths.log_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                let mut file = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .truncate(false)
                    .write(true)
                    .open(&self.state.paths.log_path)
                    .with_context(|| {
                        format!(
                            "failed to open progressive manifest spool {}",
                            self.state.paths.log_path.display()
                        )
                    })?;
                let current_len = file.metadata()?.len();
                if current_len != self.spool_len {
                    file.set_len(self.spool_len).with_context(|| {
                        format!(
                            "failed to truncate progressive manifest spool {} to {} bytes",
                            self.state.paths.log_path.display(),
                            self.spool_len
                        )
                    })?;
                }
                file.seek(SeekFrom::Start(self.spool_len))
                    .with_context(|| {
                        format!(
                            "failed to seek progressive manifest spool {}",
                            self.state.paths.log_path.display()
                        )
                    })?;
                self.spool_file = Some(file);
            }
            self.spool_file.as_mut().ok_or_else(|| {
                anyhow::anyhow!("progressive manifest spool file was not initialized")
            })
        }

        fn current_prefix(&self) -> Arc<super::InputPathPrefix> {
            self.prefix_stack
                .last()
                .cloned()
                .unwrap_or_else(|| self.root_prefix.clone())
        }

        fn path_stack_components(&self) -> Vec<Box<str>> {
            self.prefix_stack
                .iter()
                .filter_map(|prefix| {
                    prefix
                        .name_component()
                        .map(|component| component.to_owned().into_boxed_str())
                })
                .collect()
        }

        fn finish_walk(&mut self) -> Result<()> {
            self.exhausted = true;
            if !self.resume_validated {
                bail!(
                    "sender source no longer matches resume state: committed file index {} is missing from progressive manifest",
                    self.resume_state.last_committed_file_index
                );
            }
            Ok(())
        }

        fn current_boundary(&self) -> (u64, String) {
            self.last_boundary_file
                .as_ref()
                .map(|file| (file.index, file.rel_path_string()))
                .unwrap_or_else(|| {
                    (
                        self.resume_state.last_committed_file_index,
                        self.resume_state.last_committed_file_path.clone(),
                    )
                })
        }
    }

    impl ManifestPendingLeafWalker {
        pub(crate) fn new(
            source_root: impl Into<PathBuf>,
            resume_state: &FileResumeState,
            state: ManifestSpoolState,
            profile: Option<&super::PipelineProfileCollector>,
        ) -> Result<Self> {
            let source_root = source_root.into();
            let root_prefix = super::InputPathPrefix::root(source_root);
            let checkpoint = choose_resume_checkpoint(
                state.last_checkpoint.as_ref(),
                resume_state.last_committed_file_index,
            );
            let mut prefix_stack = Vec::new();
            let mut current_prefix = root_prefix.clone();
            if let Some(checkpoint) = checkpoint {
                for frame in &checkpoint.path_stack {
                    current_prefix = super::InputPathPrefix::child(
                        &current_prefix,
                        frame.name_component.as_ref(),
                    );
                    prefix_stack.push(current_prefix.clone());
                }
            }
            let current_offset = checkpoint.map(|value| value.log_offset).unwrap_or(0);
            let discovery_issues = super::collect_source_discovery_issues(&state)?;
            let discovery_issue_summary =
                super::summarize_source_discovery_issues(&discovery_issues);
            Ok(Self {
                state,
                resume_state: resume_state.clone(),
                root_prefix,
                current_offset,
                prefix_stack,
                current_file: None,
                profile: profile.cloned(),
                discovery_issues,
                discovery_issue_summary,
                resume_validated: resume_state.last_committed_file_index == 0,
                exhausted: false,
                last_boundary_file: None,
            })
        }

        pub(crate) fn next_pending_file(&mut self) -> Result<Option<super::InputFile>> {
            if self.exhausted {
                return Ok(None);
            }

            loop {
                let Some((record, read_bytes, read_duration)) = self.read_next_record()? else {
                    self.exhausted = true;
                    if !self.resume_validated {
                        bail!(
                            "sender source no longer matches resume state: committed file index {} is missing from manifest spool",
                            self.resume_state.last_committed_file_index
                        );
                    }
                    return Ok(None);
                };
                if !self.resume_validated
                    && let Some(profile) = &self.profile
                {
                    profile.record_manifest_replay(read_bytes, 1, read_duration);
                }

                let ManifestRecord {
                    kind,
                    name_component,
                    work_ordinal,
                    payload,
                    ..
                } = record;
                match kind {
                    ManifestRecordKind::DirectoryEnter => {
                        let prefix = super::InputPathPrefix::child(
                            &self.current_prefix(),
                            name_component.as_ref(),
                        );
                        self.prefix_stack.push(prefix);
                    }
                    ManifestRecordKind::DirectoryLeave => {
                        let Some(current) = self.prefix_stack.pop() else {
                            bail!(
                                "manifest replay encountered DirectoryLeave without open directory"
                            );
                        };
                        if current.name_component().unwrap_or("") != name_component.as_ref() {
                            bail!(
                                "manifest replay encountered mismatched DirectoryLeave: expected {} got {}",
                                current.name_component().unwrap_or(""),
                                name_component
                            );
                        }
                    }
                    ManifestRecordKind::DiscoverySkip | ManifestRecordKind::SubtreeSkip => {}
                    ManifestRecordKind::File | ManifestRecordKind::Symlink => {
                        let work_ordinal = work_ordinal
                            .ok_or_else(|| {
                                anyhow::anyhow!("manifest leaf record is missing work_ordinal")
                            })?
                            .0;
                        let file = match payload {
                            ManifestRecordPayload::None => super::InputFile::regular_from_prefix(
                                work_ordinal,
                                self.current_prefix(),
                                name_component,
                            ),
                            ManifestRecordPayload::SymlinkTarget { target } => {
                                super::InputFile::symlink_from_prefix(
                                    work_ordinal,
                                    self.current_prefix(),
                                    name_component,
                                    PathBuf::from(target.as_ref()),
                                )
                            }
                            payload => {
                                bail!(
                                    "manifest leaf record carried unexpected payload: {payload:?}"
                                )
                            }
                        };
                        self.last_boundary_file = Some(file.clone());

                        if !self.resume_validated {
                            if work_ordinal < self.resume_state.last_committed_file_index {
                                continue;
                            }
                            if work_ordinal == self.resume_state.last_committed_file_index {
                                if file.rel_path() != self.resume_state.last_committed_file_path {
                                    bail!(
                                        "sender source no longer matches resume state at file index {}: expected {} got {}",
                                        self.resume_state.last_committed_file_index,
                                        self.resume_state.last_committed_file_path,
                                        file.rel_path()
                                    );
                                }
                                self.resume_validated = true;
                                continue;
                            }
                            bail!(
                                "sender source no longer matches resume state: committed file index {} is missing before {} in manifest spool",
                                self.resume_state.last_committed_file_index,
                                file.rel_path()
                            );
                        }

                        return Ok(Some(file));
                    }
                }
            }
        }

        pub(crate) fn final_boundary(&self) -> Result<(u64, String)> {
            if !self.exhausted {
                bail!("sender manifest replay walker has not finished replay yet");
            }
            Ok(self.current_boundary())
        }

        pub(crate) fn last_discovered_boundary(&self) -> (u64, String) {
            self.current_boundary()
        }

        pub(crate) fn discovery_issues(&self) -> &[super::SourceDiscoveryIssue] {
            &self.discovery_issues
        }

        pub(crate) fn discovery_issue_summary(&self) -> super::SourceDiscoveryIssueSummary {
            self.discovery_issue_summary
        }

        fn read_next_record(
            &mut self,
        ) -> Result<Option<(ManifestRecord, u64, std::time::Duration)>> {
            let offset = self.current_offset;
            let read_started = Instant::now();
            if let Some(bytes) = self.state.fresh_log_bytes.as_deref() {
                let offset_usize = usize::try_from(offset).with_context(|| {
                    format!("manifest replay offset does not fit usize: {}", offset)
                })?;
                let Some((record, next_offset)) = (match decode_record_at(bytes, offset_usize)? {
                    ManifestDecodedRecord::Complete {
                        record,
                        next_cursor,
                    } => Some((record, next_cursor as u64)),
                    ManifestDecodedRecord::TornTail { .. } => None,
                }) else {
                    return Ok(None);
                };
                self.current_offset = next_offset;
                let read_duration = read_started.elapsed();
                return Ok(Some((
                    record,
                    next_offset.saturating_sub(offset),
                    read_duration,
                )));
            }
            let file = self.current_file()?;
            let Some((record, next_offset)) = read_record_from_file(file, offset)? else {
                return Ok(None);
            };
            self.current_offset = next_offset;
            let read_bytes = next_offset.saturating_sub(offset);
            let read_duration = read_started.elapsed();
            if let Some(profile) = &self.profile {
                profile.record_manifest_spool_read(read_bytes, read_duration);
            }
            Ok(Some((record, read_bytes, read_duration)))
        }

        fn current_file(&mut self) -> Result<&mut File> {
            if self.current_file.is_none() {
                let mut file = File::open(&self.state.paths.log_path).with_context(|| {
                    format!(
                        "failed to open manifest log for replay {}",
                        self.state.paths.log_path.display()
                    )
                })?;
                file.seek(SeekFrom::Start(self.current_offset))
                    .with_context(|| {
                        format!(
                            "failed to seek manifest log for replay {}",
                            self.state.paths.log_path.display()
                        )
                    })?;
                self.current_file = Some(file);
            }
            self.current_file
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("manifest replay file handle was not initialized"))
        }

        fn current_prefix(&self) -> Arc<super::InputPathPrefix> {
            self.prefix_stack
                .last()
                .cloned()
                .unwrap_or_else(|| self.root_prefix.clone())
        }

        fn current_boundary(&self) -> (u64, String) {
            self.last_boundary_file
                .as_ref()
                .map(|file| (file.index, file.rel_path_string()))
                .unwrap_or_else(|| {
                    (
                        self.resume_state.last_committed_file_index,
                        self.resume_state.last_committed_file_path.clone(),
                    )
                })
        }
    }

    fn choose_resume_checkpoint(
        checkpoint: Option<&ManifestCheckpoint>,
        committed_work_ordinal: u64,
    ) -> Option<&ManifestCheckpoint> {
        let checkpoint = checkpoint?;
        if checkpoint
            .last_work_ordinal
            .map(|value| value.0 < committed_work_ordinal)
            .unwrap_or(committed_work_ordinal > 0)
        {
            Some(checkpoint)
        } else {
            None
        }
    }

    fn manifest_relative_path(path_stack: &[Box<str>], leaf_name: &str) -> String {
        if path_stack.is_empty() {
            return leaf_name.to_owned();
        }
        let mut result = String::new();
        for component in path_stack {
            if !result.is_empty() {
                result.push('/');
            }
            result.push_str(component);
        }
        result.push('/');
        result.push_str(leaf_name);
        result
    }

    fn read_record_from_file(
        file: &mut File,
        offset: u64,
    ) -> Result<Option<(ManifestRecord, u64)>> {
        let mut header = [0_u8; MANIFEST_RECORD_FIXED_HEADER_LEN];
        let read = file.read(&mut header)?;
        if read == 0 {
            return Ok(None);
        }
        if read < MANIFEST_RECORD_FIXED_HEADER_LEN {
            bail!(
                "manifest replay encountered truncated record header at offset {}",
                offset
            );
        }

        let mut cursor = 0_usize;
        let body_len = read_u32(&header, &mut cursor)? as usize;
        let mut bytes = Vec::with_capacity(MANIFEST_RECORD_FIXED_HEADER_LEN + body_len);
        bytes.extend_from_slice(&header);
        if body_len > 0 {
            let mut body = vec![0_u8; body_len];
            file.read_exact(&mut body)?;
            bytes.extend_from_slice(&body);
        }

        match decode_record_at(&bytes, 0)? {
            ManifestDecodedRecord::Complete {
                record,
                next_cursor,
            } => Ok(Some((record, offset.saturating_add(next_cursor as u64)))),
            ManifestDecodedRecord::TornTail { truncate_to_len } => bail!(
                "manifest replay encountered torn tail at offset {}",
                offset.saturating_add(truncate_to_len)
            ),
        }
    }

    fn manifest_state_dir_name(dump_id: &str) -> String {
        let mut result = String::with_capacity(dump_id.len().max(8));
        for ch in dump_id.chars() {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                result.push(ch);
            } else {
                result.push('_');
            }
        }
        if result.is_empty() {
            result.push_str("default");
        }
        result
    }

    fn remove_if_exists(path: &Path) -> Result<()> {
        if path.exists() {
            fs::remove_file(path).with_context(|| {
                format!(
                    "failed to remove stale sender state file {}",
                    path.display()
                )
            })?;
        }
        Ok(())
    }

    fn checkpoint_temp_path(path: &Path) -> PathBuf {
        path.with_extension("tmp")
    }

    fn meta_temp_path(path: &Path) -> PathBuf {
        path.with_extension("tmp")
    }

    fn encode_record_body(record: &ManifestRecord) -> Result<Vec<u8>> {
        let mut body = Vec::new();
        body.extend_from_slice(record.name_component.as_bytes());
        match &record.payload {
            ManifestRecordPayload::None => {}
            ManifestRecordPayload::SymlinkTarget { target } => {
                body.extend_from_slice(target.as_bytes());
            }
            ManifestRecordPayload::DiscoverySkip { reason } => {
                encode_discovery_skip_reason(&mut body, reason)?
            }
            ManifestRecordPayload::SubtreeSkip { reason } => {
                encode_subtree_skip_reason(&mut body, reason)?
            }
        }
        Ok(body)
    }

    fn encode_discovery_skip_reason(
        output: &mut Vec<u8>,
        reason: &ManifestDiscoverySkipReason,
    ) -> Result<()> {
        match reason {
            ManifestDiscoverySkipReason::UnsupportedEntry { kind } => {
                output.push(1);
                output.push(encode_special_entry_kind(*kind));
            }
            ManifestDiscoverySkipReason::UnreadableSymlinkTarget { error } => {
                output.push(2);
                write_len_prefixed_string(output, error)?;
            }
            ManifestDiscoverySkipReason::VirtualFilesystem { kind } => {
                output.push(3);
                write_len_prefixed_string(output, kind)?;
            }
            ManifestDiscoverySkipReason::Io { operation, error } => {
                output.push(4);
                output.push(encode_io_operation(*operation));
                write_len_prefixed_string(output, error)?;
            }
            ManifestDiscoverySkipReason::SenderState { event, detail } => {
                output.push(5);
                output.push(encode_sender_state_event(*event));
                write_len_prefixed_string(output, detail)?;
            }
        }
        Ok(())
    }

    fn encode_subtree_skip_reason(
        output: &mut Vec<u8>,
        reason: &ManifestSubtreeSkipReason,
    ) -> Result<()> {
        match reason {
            ManifestSubtreeSkipReason::ReadDirFailed { error } => {
                output.push(1);
                write_len_prefixed_string(output, error)?;
            }
            ManifestSubtreeSkipReason::SourceInvalidated { detail } => {
                output.push(2);
                write_len_prefixed_string(output, detail)?;
            }
            ManifestSubtreeSkipReason::Io { operation, error } => {
                output.push(3);
                output.push(encode_io_operation(*operation));
                write_len_prefixed_string(output, error)?;
            }
        }
        Ok(())
    }

    fn decode_record_payload(
        kind: ManifestRecordKind,
        payload_len: usize,
        bytes: &[u8],
        cursor: &mut usize,
    ) -> Result<ManifestRecordPayload> {
        let payload_end = cursor.saturating_add(payload_len);
        if payload_end > bytes.len() {
            bail!("manifest spool payload exceeds available bytes");
        }

        let payload = match kind {
            ManifestRecordKind::DirectoryEnter
            | ManifestRecordKind::DirectoryLeave
            | ManifestRecordKind::File => {
                if payload_len != 0 {
                    bail!(
                        "manifest record kind {:?} must not carry payload bytes",
                        kind
                    );
                }
                ManifestRecordPayload::None
            }
            ManifestRecordKind::Symlink => {
                let target = read_sized_utf8(bytes, cursor, payload_len)?;
                ManifestRecordPayload::SymlinkTarget { target }
            }
            ManifestRecordKind::DiscoverySkip => ManifestRecordPayload::DiscoverySkip {
                reason: decode_discovery_skip_reason(bytes, cursor, payload_end)?,
            },
            ManifestRecordKind::SubtreeSkip => ManifestRecordPayload::SubtreeSkip {
                reason: decode_subtree_skip_reason(bytes, cursor, payload_end)?,
            },
        };

        if *cursor != payload_end {
            bail!("manifest payload decoder did not consume the full payload");
        }
        Ok(payload)
    }

    fn decode_discovery_skip_reason(
        bytes: &[u8],
        cursor: &mut usize,
        payload_end: usize,
    ) -> Result<ManifestDiscoverySkipReason> {
        let tag = read_u8(bytes, cursor)?;
        let reason = match tag {
            1 => ManifestDiscoverySkipReason::UnsupportedEntry {
                kind: decode_special_entry_kind(read_u8(bytes, cursor)?)?,
            },
            2 => ManifestDiscoverySkipReason::UnreadableSymlinkTarget {
                error: read_len_prefixed_string_with_limit(bytes, cursor, payload_end)?,
            },
            3 => ManifestDiscoverySkipReason::VirtualFilesystem {
                kind: read_len_prefixed_string_with_limit(bytes, cursor, payload_end)?,
            },
            4 => ManifestDiscoverySkipReason::Io {
                operation: decode_io_operation(read_u8(bytes, cursor)?)?,
                error: read_len_prefixed_string_with_limit(bytes, cursor, payload_end)?,
            },
            5 => ManifestDiscoverySkipReason::SenderState {
                event: decode_sender_state_event(read_u8(bytes, cursor)?)?,
                detail: read_len_prefixed_string_with_limit(bytes, cursor, payload_end)?,
            },
            _ => bail!("unknown manifest discovery skip reason tag {}", tag),
        };
        Ok(reason)
    }

    fn decode_subtree_skip_reason(
        bytes: &[u8],
        cursor: &mut usize,
        payload_end: usize,
    ) -> Result<ManifestSubtreeSkipReason> {
        let tag = read_u8(bytes, cursor)?;
        let reason = match tag {
            1 => ManifestSubtreeSkipReason::ReadDirFailed {
                error: read_len_prefixed_string_with_limit(bytes, cursor, payload_end)?,
            },
            2 => ManifestSubtreeSkipReason::SourceInvalidated {
                detail: read_len_prefixed_string_with_limit(bytes, cursor, payload_end)?,
            },
            3 => ManifestSubtreeSkipReason::Io {
                operation: decode_io_operation(read_u8(bytes, cursor)?)?,
                error: read_len_prefixed_string_with_limit(bytes, cursor, payload_end)?,
            },
            _ => bail!("unknown manifest subtree skip reason tag {}", tag),
        };
        Ok(reason)
    }

    fn encode_special_entry_kind(kind: ManifestSpecialEntryKind) -> u8 {
        match kind {
            ManifestSpecialEntryKind::Socket => 1,
            ManifestSpecialEntryKind::Fifo => 2,
            ManifestSpecialEntryKind::BlockDevice => 3,
            ManifestSpecialEntryKind::CharDevice => 4,
            ManifestSpecialEntryKind::Other => 5,
        }
    }

    fn decode_special_entry_kind(tag: u8) -> Result<ManifestSpecialEntryKind> {
        match tag {
            1 => Ok(ManifestSpecialEntryKind::Socket),
            2 => Ok(ManifestSpecialEntryKind::Fifo),
            3 => Ok(ManifestSpecialEntryKind::BlockDevice),
            4 => Ok(ManifestSpecialEntryKind::CharDevice),
            5 => Ok(ManifestSpecialEntryKind::Other),
            _ => bail!("unknown manifest special entry kind tag {}", tag),
        }
    }

    fn encode_io_operation(operation: ManifestIoOperation) -> u8 {
        match operation {
            ManifestIoOperation::ReadDir => 1,
            ManifestIoOperation::ReadLink => 2,
            ManifestIoOperation::ClassifyEntry => 3,
            ManifestIoOperation::Replay => 4,
        }
    }

    fn decode_io_operation(tag: u8) -> Result<ManifestIoOperation> {
        match tag {
            1 => Ok(ManifestIoOperation::ReadDir),
            2 => Ok(ManifestIoOperation::ReadLink),
            3 => Ok(ManifestIoOperation::ClassifyEntry),
            4 => Ok(ManifestIoOperation::Replay),
            _ => bail!("unknown manifest io operation tag {}", tag),
        }
    }

    fn encode_sender_state_event(event: ManifestSenderStateEvent) -> u8 {
        match event {
            ManifestSenderStateEvent::Invalidated => 1,
            ManifestSenderStateEvent::Corrupted => 2,
            ManifestSenderStateEvent::Rebuilt => 3,
        }
    }

    fn decode_sender_state_event(tag: u8) -> Result<ManifestSenderStateEvent> {
        match tag {
            1 => Ok(ManifestSenderStateEvent::Invalidated),
            2 => Ok(ManifestSenderStateEvent::Corrupted),
            3 => Ok(ManifestSenderStateEvent::Rebuilt),
            _ => bail!("unknown manifest sender-state event tag {}", tag),
        }
    }

    fn decode_record_kind(tag: u8) -> Result<ManifestRecordKind> {
        match tag {
            1 => Ok(ManifestRecordKind::DirectoryEnter),
            2 => Ok(ManifestRecordKind::DirectoryLeave),
            3 => Ok(ManifestRecordKind::File),
            4 => Ok(ManifestRecordKind::Symlink),
            5 => Ok(ManifestRecordKind::DiscoverySkip),
            6 => Ok(ManifestRecordKind::SubtreeSkip),
            _ => bail!("unknown manifest record kind tag {}", tag),
        }
    }

    fn checksum_u32(bytes: &[u8]) -> u32 {
        xxh3_64(bytes) as u32
    }

    fn write_u16(output: &mut Vec<u8>, value: u16) {
        output.extend_from_slice(&value.to_le_bytes());
    }

    fn write_u32(output: &mut Vec<u8>, value: u32) {
        output.extend_from_slice(&value.to_le_bytes());
    }

    fn write_u64(output: &mut Vec<u8>, value: u64) {
        output.extend_from_slice(&value.to_le_bytes());
    }

    fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8> {
        if *cursor >= bytes.len() {
            bail!("unexpected end of input while reading u8");
        }
        let value = bytes[*cursor];
        *cursor += 1;
        Ok(value)
    }

    fn read_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16> {
        let end = cursor.saturating_add(2);
        if end > bytes.len() {
            bail!("unexpected end of input while reading u16");
        }
        let value = u16::from_le_bytes(bytes[*cursor..end].try_into().unwrap());
        *cursor = end;
        Ok(value)
    }

    fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
        let end = cursor.saturating_add(4);
        if end > bytes.len() {
            bail!("unexpected end of input while reading u32");
        }
        let value = u32::from_le_bytes(bytes[*cursor..end].try_into().unwrap());
        *cursor = end;
        Ok(value)
    }

    fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
        let end = cursor.saturating_add(8);
        if end > bytes.len() {
            bail!("unexpected end of input while reading u64");
        }
        let value = u64::from_le_bytes(bytes[*cursor..end].try_into().unwrap());
        *cursor = end;
        Ok(value)
    }

    fn write_len_prefixed_string(output: &mut Vec<u8>, value: &str) -> Result<()> {
        let len = u16::try_from(value.len())
            .map_err(|_| anyhow::anyhow!("manifest string is too long to encode"))?;
        write_u16(output, len);
        output.extend_from_slice(value.as_bytes());
        Ok(())
    }

    fn read_len_prefixed_string(bytes: &[u8], cursor: &mut usize) -> Result<Box<str>> {
        let len = read_u16(bytes, cursor)? as usize;
        read_sized_utf8(bytes, cursor, len)
    }

    fn read_len_prefixed_string_with_limit(
        bytes: &[u8],
        cursor: &mut usize,
        limit: usize,
    ) -> Result<Box<str>> {
        let len = read_u16(bytes, cursor)? as usize;
        if cursor.saturating_add(len) > limit {
            bail!("manifest length-prefixed string exceeds payload bounds");
        }
        read_sized_utf8(bytes, cursor, len)
    }

    fn read_sized_utf8(bytes: &[u8], cursor: &mut usize, len: usize) -> Result<Box<str>> {
        let end = cursor.saturating_add(len);
        if end > bytes.len() {
            bail!("unexpected end of input while reading manifest utf-8 string");
        }
        let value = std::str::from_utf8(&bytes[*cursor..end])
            .context("manifest string payload is not valid utf-8")?;
        *cursor = end;
        Ok(value.into())
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub(crate) struct ManifestGrammarState {
        next_record_seq: u64,
        next_work_ordinal: u64,
        open_directories: Vec<OpenDirectory>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct OpenDirectory {
        name_component: Box<str>,
        child_emitted: bool,
    }

    impl ManifestGrammarState {
        pub(crate) fn from_checkpoint(checkpoint: &ManifestCheckpoint) -> Self {
            Self {
                next_record_seq: checkpoint.last_record_seq.0,
                next_work_ordinal: checkpoint
                    .last_work_ordinal
                    .map(|value| value.0)
                    .unwrap_or(0),
                open_directories: checkpoint
                    .path_stack
                    .iter()
                    .map(|frame| OpenDirectory {
                        name_component: frame.name_component.clone(),
                        child_emitted: frame.child_emitted,
                    })
                    .collect(),
            }
        }

        pub(crate) fn snapshot_checkpoint(
            &self,
            checkpoint_seq: u64,
            log_offset: u64,
        ) -> Option<ManifestCheckpoint> {
            if self.next_record_seq == 0 {
                return None;
            }

            Some(ManifestCheckpoint {
                checkpoint_seq,
                last_record_seq: ManifestRecordSeq(self.next_record_seq),
                last_work_ordinal: if self.next_work_ordinal == 0 {
                    None
                } else {
                    Some(ManifestWorkOrdinal(self.next_work_ordinal))
                },
                log_offset,
                path_stack: self
                    .open_directories
                    .iter()
                    .map(|frame| ManifestCheckpointFrame {
                        name_component: frame.name_component.clone(),
                        child_emitted: frame.child_emitted,
                    })
                    .collect(),
            })
        }

        pub(crate) fn last_record_seq(&self) -> Option<ManifestRecordSeq> {
            if self.next_record_seq == 0 {
                None
            } else {
                Some(ManifestRecordSeq(self.next_record_seq))
            }
        }

        pub(crate) fn last_work_ordinal(&self) -> Option<ManifestWorkOrdinal> {
            if self.next_work_ordinal == 0 {
                None
            } else {
                Some(ManifestWorkOrdinal(self.next_work_ordinal))
            }
        }

        pub(crate) fn path_stack(&self) -> Vec<Box<str>> {
            self.open_directories
                .iter()
                .map(|frame| frame.name_component.clone())
                .collect()
        }

        pub(crate) fn validate_and_apply(&mut self, record: &ManifestRecord) -> Result<()> {
            let expected_record_seq = self.next_record_seq.saturating_add(1);
            if record.record_seq.0 != expected_record_seq {
                bail!(
                    "manifest record_seq must be consecutive: expected {}, got {}",
                    expected_record_seq,
                    record.record_seq.0
                );
            }

            match record.kind {
                ManifestRecordKind::DirectoryEnter => {
                    if usize::from(record.depth) != self.open_directories.len() {
                        bail!("DirectoryEnter depth must match current path stack depth");
                    }
                    self.mark_child_emitted_for_parent();
                    self.open_directories.push(OpenDirectory {
                        name_component: record.name_component.clone(),
                        child_emitted: false,
                    });
                }
                ManifestRecordKind::DirectoryLeave => {
                    let expected_depth =
                        self.open_directories.len().checked_sub(1).ok_or_else(|| {
                            anyhow::anyhow!("DirectoryLeave without open directory")
                        })?;
                    if usize::from(record.depth) != expected_depth {
                        bail!("DirectoryLeave depth must point at the directory being closed");
                    }
                    let open = self
                        .open_directories
                        .pop()
                        .ok_or_else(|| anyhow::anyhow!("DirectoryLeave without open directory"))?;
                    if open.name_component != record.name_component {
                        bail!("DirectoryLeave must close the most recently opened directory");
                    }
                }
                ManifestRecordKind::File | ManifestRecordKind::Symlink => {
                    if usize::from(record.depth) != self.open_directories.len() {
                        bail!("leaf work item depth must match current path stack depth");
                    }
                    let expected_work_ordinal = self.next_work_ordinal.saturating_add(1);
                    if record.work_ordinal != Some(ManifestWorkOrdinal(expected_work_ordinal)) {
                        bail!(
                            "leaf work items must carry consecutive work_ordinal values: expected {}, got {:?}",
                            expected_work_ordinal,
                            record.work_ordinal
                        );
                    }
                    self.next_work_ordinal = expected_work_ordinal;
                    self.mark_child_emitted_for_parent();
                }
                ManifestRecordKind::DiscoverySkip => {
                    if usize::from(record.depth) != self.open_directories.len() {
                        bail!("DiscoverySkip depth must match the parent directory depth");
                    }
                    self.mark_child_emitted_for_parent();
                }
                ManifestRecordKind::SubtreeSkip => {
                    let expected_depth = self
                        .open_directories
                        .len()
                        .checked_sub(1)
                        .ok_or_else(|| anyhow::anyhow!("SubtreeSkip without open directory"))?;
                    if usize::from(record.depth) != expected_depth {
                        bail!("SubtreeSkip depth must point at the open directory subtree");
                    }
                    let open = self
                        .open_directories
                        .last()
                        .ok_or_else(|| anyhow::anyhow!("SubtreeSkip without open directory"))?;
                    if open.name_component != record.name_component {
                        bail!("SubtreeSkip must target the currently open directory");
                    }
                    if open.child_emitted {
                        bail!(
                            "SubtreeSkip is only legal before any child records are emitted for that directory"
                        );
                    }
                }
            }

            self.next_record_seq = record.record_seq.0;
            Ok(())
        }

        fn mark_child_emitted_for_parent(&mut self) {
            if let Some(parent) = self.open_directories.last_mut() {
                parent.child_emitted = true;
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn temp_state_root(label: &str) -> PathBuf {
            let path = std::env::temp_dir().join(format!(
                "fast-dump-{label}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            fs::create_dir_all(&path).unwrap();
            path
        }

        fn manifest_source_identity(
            root: &str,
            identity_root: &str,
            kind: ManifestSourceKind,
        ) -> ManifestSourceIdentity {
            ManifestSourceIdentity::new(root, identity_root, "builder-v1", kind).unwrap()
        }

        fn write_checkpointed_spool(
            paths: &ManifestSpoolPaths,
            meta: &mut ManifestMeta,
        ) -> ManifestCheckpoint {
            fs::create_dir_all(&paths.state_dir).unwrap();
            let record = ManifestRecord::new(
                ManifestRecordSeq(1),
                ManifestRecordKind::File,
                0,
                "alpha.txt",
                Some(ManifestWorkOrdinal(1)),
                ManifestRecordPayload::None,
            )
            .unwrap();
            let encoded = ManifestSpoolWriter::encode_record(&record).unwrap();
            fs::write(&paths.log_path, &encoded).unwrap();

            let checkpoint = ManifestCheckpoint {
                checkpoint_seq: 1,
                last_record_seq: record.record_seq,
                last_work_ordinal: record.work_ordinal,
                log_offset: encoded.len() as u64,
                path_stack: Vec::new(),
            };
            checkpoint.write_atomic(&paths.checkpoint_path).unwrap();
            meta.commit_checkpoint(
                checkpoint.last_record_seq,
                checkpoint.last_work_ordinal,
                checkpoint.checkpoint_seq,
            )
            .unwrap();
            meta.write_atomic(&paths.meta_path).unwrap();
            checkpoint
        }

        #[cfg(unix)]
        fn create_symlink(path: &Path, target: &Path) -> std::io::Result<()> {
            std::os::unix::fs::symlink(target, path)
        }

        #[cfg(windows)]
        fn create_symlink(path: &Path, target: &Path) -> std::io::Result<()> {
            std::os::windows::fs::symlink_file(target, path)
        }

        struct FsFailureGuard(std::sync::MutexGuard<'static, ()>);

        impl FsFailureGuard {
            fn new() -> Self {
                static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
                let guard = LOCK
                    .get_or_init(|| std::sync::Mutex::new(()))
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                super::super::test_hooks::clear();
                Self(guard)
            }
        }

        impl Drop for FsFailureGuard {
            fn drop(&mut self) {
                super::super::test_hooks::clear();
            }
        }

        #[test]
        fn grammar_accepts_directory_enter_file_and_leave() {
            let mut grammar = ManifestGrammarState::default();
            let records = [
                ManifestRecord::new(
                    ManifestRecordSeq(1),
                    ManifestRecordKind::DirectoryEnter,
                    0,
                    "dir",
                    None,
                    ManifestRecordPayload::None,
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(2),
                    ManifestRecordKind::File,
                    1,
                    "file.txt",
                    Some(ManifestWorkOrdinal(1)),
                    ManifestRecordPayload::None,
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(3),
                    ManifestRecordKind::DirectoryLeave,
                    0,
                    "dir",
                    None,
                    ManifestRecordPayload::None,
                )
                .unwrap(),
            ];

            for record in &records {
                grammar.validate_and_apply(record).unwrap();
            }
        }

        #[test]
        fn grammar_accepts_canonical_subtree_skip_sequence() {
            let mut grammar = ManifestGrammarState::default();
            let records = [
                ManifestRecord::new(
                    ManifestRecordSeq(1),
                    ManifestRecordKind::DirectoryEnter,
                    0,
                    "broken",
                    None,
                    ManifestRecordPayload::None,
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(2),
                    ManifestRecordKind::SubtreeSkip,
                    0,
                    "broken",
                    None,
                    ManifestRecordPayload::SubtreeSkip {
                        reason: ManifestSubtreeSkipReason::ReadDirFailed {
                            error: "permission denied".into(),
                        },
                    },
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(3),
                    ManifestRecordKind::DirectoryLeave,
                    0,
                    "broken",
                    None,
                    ManifestRecordPayload::None,
                )
                .unwrap(),
            ];

            for record in &records {
                grammar.validate_and_apply(record).unwrap();
            }
        }

        #[test]
        fn grammar_rejects_subtree_skip_after_child_record() {
            let mut grammar = ManifestGrammarState::default();
            let records = [
                ManifestRecord::new(
                    ManifestRecordSeq(1),
                    ManifestRecordKind::DirectoryEnter,
                    0,
                    "dir",
                    None,
                    ManifestRecordPayload::None,
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(2),
                    ManifestRecordKind::DiscoverySkip,
                    1,
                    "entry.sock",
                    None,
                    ManifestRecordPayload::DiscoverySkip {
                        reason: ManifestDiscoverySkipReason::UnsupportedEntry {
                            kind: ManifestSpecialEntryKind::Socket,
                        },
                    },
                )
                .unwrap(),
            ];
            for record in &records {
                grammar.validate_and_apply(record).unwrap();
            }

            let subtree_skip = ManifestRecord::new(
                ManifestRecordSeq(3),
                ManifestRecordKind::SubtreeSkip,
                0,
                "dir",
                None,
                ManifestRecordPayload::SubtreeSkip {
                    reason: ManifestSubtreeSkipReason::ReadDirFailed {
                        error: "late failure".into(),
                    },
                },
            )
            .unwrap();

            let error = grammar.validate_and_apply(&subtree_skip).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("SubtreeSkip is only legal before any child records"),
                "unexpected error: {error:#}"
            );
        }

        #[test]
        fn record_constructor_rejects_slash_in_name_component() {
            let error = ManifestRecord::new(
                ManifestRecordSeq(1),
                ManifestRecordKind::File,
                0,
                "nested/file.txt",
                Some(ManifestWorkOrdinal(1)),
                ManifestRecordPayload::None,
            )
            .unwrap_err();

            assert!(
                error
                    .to_string()
                    .contains("must store only one path segment"),
                "unexpected error: {error:#}"
            );
        }

        #[test]
        fn spool_header_marks_work_ordinal_and_payload_flags() {
            let record = ManifestRecord::new(
                ManifestRecordSeq(7),
                ManifestRecordKind::Symlink,
                2,
                "link",
                Some(ManifestWorkOrdinal(3)),
                ManifestRecordPayload::SymlinkTarget {
                    target: "dest/target.txt".into(),
                },
            )
            .unwrap();

            let header = ManifestSpoolRecordHeader::from_record(&record).unwrap();
            assert_eq!(header.record_seq, ManifestRecordSeq(7));
            assert_eq!(header.work_ordinal_or_zero, 3);
            assert_eq!(header.kind, ManifestRecordKind::Symlink);
            assert_eq!(header.depth, 2);
            assert_eq!(
                header.flags,
                MANIFEST_RECORD_FLAG_HAS_WORK_ORDINAL | MANIFEST_RECORD_FLAG_HAS_PAYLOAD
            );
            assert_eq!(header.name_len, 4);
            assert!(header.payload_len > 0);
        }

        #[test]
        fn meta_rejects_reuse_when_builder_version_differs() {
            let stored_identity = ManifestSourceIdentity::new(
                "/src",
                "dev:42",
                "builder-v1",
                ManifestSourceKind::Snapshot,
            )
            .unwrap();
            let mut meta = ManifestMeta::new("dump-a", stored_identity, "/src").unwrap();
            meta.commit_checkpoint(ManifestRecordSeq(10), Some(ManifestWorkOrdinal(4)), 1)
                .unwrap();

            let candidate = ManifestSourceIdentity::new(
                "/src",
                "dev:42",
                "builder-legacy-snapshot-v1",
                ManifestSourceKind::Snapshot,
            )
            .unwrap();
            let decision = meta
                .validate_reuse_for_source("dump-a", &candidate)
                .unwrap();
            assert_eq!(
                decision,
                ManifestReuseDecision::InvalidateAndRebuild {
                    reason: ManifestInvalidationReason::BuilderVersionMismatch
                }
            );
        }

        #[test]
        fn meta_invalidates_unsealed_live_path_spool_after_restart() {
            let source_identity = ManifestSourceIdentity::new(
                "/src",
                "dev:42",
                "builder-v1",
                ManifestSourceKind::LivePath,
            )
            .unwrap();
            let mut meta = ManifestMeta::new("dump-a", source_identity.clone(), "/src").unwrap();
            meta.record_uncommitted_append();
            meta.commit_checkpoint(ManifestRecordSeq(12), Some(ManifestWorkOrdinal(5)), 2)
                .unwrap();
            meta.record_uncommitted_append();

            let decision = meta
                .validate_reuse_for_source("dump-a", &source_identity)
                .unwrap();
            assert_eq!(
                decision,
                ManifestReuseDecision::InvalidateAndRebuild {
                    reason: ManifestInvalidationReason::UnsealedLivePathRestart
                }
            );
        }

        #[test]
        fn meta_allows_continuing_unsealed_snapshot_spool() {
            let source_identity = ManifestSourceIdentity::new(
                "/snap",
                "snapshot:001",
                "builder-v1",
                ManifestSourceKind::Snapshot,
            )
            .unwrap();
            let mut meta = ManifestMeta::new("dump-a", source_identity.clone(), "/snap").unwrap();
            meta.record_uncommitted_append();

            let decision = meta
                .validate_reuse_for_source("dump-a", &source_identity)
                .unwrap();
            assert_eq!(decision, ManifestReuseDecision::ContinueUnsealedBuild);
        }

        #[test]
        fn meta_transitions_track_dirty_clean_and_sealed_states() {
            let source_identity = ManifestSourceIdentity::new(
                "/snap",
                "snapshot:001",
                "builder-v1",
                ManifestSourceKind::Pinned,
            )
            .unwrap();
            let mut meta = ManifestMeta::new("dump-a", source_identity, "/snap").unwrap();
            assert_eq!(meta.build_state, ManifestBuildState::Clean);
            assert_eq!(meta.seal_state, ManifestSealState::Unsealed);

            meta.record_uncommitted_append();
            assert_eq!(meta.build_state, ManifestBuildState::Dirty);
            assert_eq!(meta.seal_state, ManifestSealState::Unsealed);

            meta.commit_checkpoint(ManifestRecordSeq(20), Some(ManifestWorkOrdinal(8)), 3)
                .unwrap();
            assert_eq!(meta.build_state, ManifestBuildState::Clean);
            assert_eq!(meta.last_record_seq, Some(ManifestRecordSeq(20)));
            assert_eq!(meta.last_work_ordinal, Some(ManifestWorkOrdinal(8)));
            assert_eq!(meta.last_checkpoint_seq, Some(3));

            meta.mark_sealed();
            assert_eq!(meta.build_state, ManifestBuildState::Clean);
            assert_eq!(meta.seal_state, ManifestSealState::Sealed);

            meta.invalidate(ManifestInvalidationReason::RebuildRequested);
            assert_eq!(meta.build_state, ManifestBuildState::Clean);
            assert_eq!(meta.seal_state, ManifestSealState::Unsealed);
            assert_eq!(meta.last_record_seq, None);
            assert_eq!(meta.last_work_ordinal, None);
            assert_eq!(meta.last_checkpoint_seq, None);
            assert_eq!(
                meta.last_invalidation_reason,
                Some(ManifestInvalidationReason::RebuildRequested)
            );
        }

        #[test]
        fn meta_roundtrip_preserves_sender_state_fields() {
            let source_identity =
                manifest_source_identity("/snap", "snapshot:001", ManifestSourceKind::Pinned);
            let mut meta = ManifestMeta::new("dump-a", source_identity, "/snap").unwrap();
            meta.commit_checkpoint(ManifestRecordSeq(20), Some(ManifestWorkOrdinal(8)), 3)
                .unwrap();
            meta.record_uncommitted_append();

            let decoded = ManifestMeta::decode(&meta.encode().unwrap()).unwrap();
            assert_eq!(decoded, meta);
        }

        #[test]
        fn recovery_event_maps_into_discovery_issue_reason() {
            let event = ManifestRecoveryEvent {
                event: ManifestSenderStateEvent::Corrupted,
                detail: "manifest checkpoint checksum mismatch".into(),
            };
            assert_eq!(
                event.to_discovery_skip_reason(),
                ManifestDiscoverySkipReason::SenderState {
                    event: ManifestSenderStateEvent::Corrupted,
                    detail: "manifest checkpoint checksum mismatch".into(),
                }
            );
        }

        #[test]
        fn spool_state_invalidates_unsealed_live_path_on_restart() {
            let state_root = temp_state_root("manifest-state-live-restart");
            let dump_id = "dump-a";
            let source_identity =
                manifest_source_identity("/src", "live:001", ManifestSourceKind::LivePath);
            let paths = ManifestSpoolPaths::for_dump(&state_root, dump_id);
            let mut meta = ManifestMeta::new(dump_id, source_identity.clone(), "/src").unwrap();
            write_checkpointed_spool(&paths, &mut meta);
            meta.record_uncommitted_append();
            meta.write_atomic(&paths.meta_path).unwrap();

            let state =
                ManifestSpoolState::open_or_recover(&state_root, dump_id, source_identity, "/src")
                    .unwrap();

            assert_eq!(
                state
                    .recovery_events
                    .iter()
                    .map(|event| event.event)
                    .collect::<Vec<_>>(),
                vec![
                    ManifestSenderStateEvent::Invalidated,
                    ManifestSenderStateEvent::Rebuilt
                ]
            );
            assert_eq!(
                state.meta.last_invalidation_reason,
                Some(ManifestInvalidationReason::UnsealedLivePathRestart)
            );
            assert!(state.last_checkpoint.is_none());
            assert!(!state.paths.log_path.exists());
            assert!(!state.paths.checkpoint_path.exists());
            assert_eq!(
                state.recovery_issue_reasons(),
                vec![
                    ManifestDiscoverySkipReason::SenderState {
                        event: ManifestSenderStateEvent::Invalidated,
                        detail: "sender manifest state invalidated because unsealed live-path spool cannot continue after restart".into(),
                    },
                    ManifestDiscoverySkipReason::SenderState {
                        event: ManifestSenderStateEvent::Rebuilt,
                        detail: "sender manifest state rebuilt after invalidation".into(),
                    }
                ]
            );

            let _ = fs::remove_dir_all(&state_root);
        }

        #[test]
        fn spool_state_allows_continuing_unsealed_snapshot_spool() {
            let state_root = temp_state_root("manifest-state-snapshot-restart");
            let dump_id = "dump-a";
            let source_identity =
                manifest_source_identity("/snap", "snapshot:001", ManifestSourceKind::Snapshot);
            let paths = ManifestSpoolPaths::for_dump(&state_root, dump_id);
            let mut meta = ManifestMeta::new(dump_id, source_identity.clone(), "/snap").unwrap();
            let checkpoint = write_checkpointed_spool(&paths, &mut meta);
            meta.record_uncommitted_append();
            meta.write_atomic(&paths.meta_path).unwrap();

            let state =
                ManifestSpoolState::open_or_recover(&state_root, dump_id, source_identity, "/snap")
                    .unwrap();

            assert!(state.recovery_events.is_empty());
            assert_eq!(state.last_checkpoint, Some(checkpoint));
            assert_eq!(state.meta.build_state, ManifestBuildState::Dirty);
            assert!(state.paths.log_path.exists());

            let _ = fs::remove_dir_all(&state_root);
        }

        #[test]
        fn spool_state_rebuilds_after_meta_corruption() {
            let state_root = temp_state_root("manifest-state-meta-corruption");
            let dump_id = "dump-a";
            let source_identity =
                manifest_source_identity("/snap", "snapshot:001", ManifestSourceKind::Snapshot);
            let paths = ManifestSpoolPaths::for_dump(&state_root, dump_id);
            let mut meta = ManifestMeta::new(dump_id, source_identity.clone(), "/snap").unwrap();
            write_checkpointed_spool(&paths, &mut meta);
            fs::write(&paths.meta_path, [0xAA, 0xBB, 0xCC]).unwrap();

            let state =
                ManifestSpoolState::open_or_recover(&state_root, dump_id, source_identity, "/snap")
                    .unwrap();

            assert_eq!(
                state
                    .recovery_events
                    .iter()
                    .map(|event| event.event)
                    .collect::<Vec<_>>(),
                vec![
                    ManifestSenderStateEvent::Corrupted,
                    ManifestSenderStateEvent::Rebuilt
                ]
            );
            assert_eq!(
                state.meta.last_invalidation_reason,
                Some(ManifestInvalidationReason::SpoolCorrupted)
            );
            assert!(!state.paths.log_path.exists());
            assert!(!state.paths.checkpoint_path.exists());

            let _ = fs::remove_dir_all(&state_root);
        }

        #[test]
        fn spool_state_rebuilds_when_checkpoint_is_missing_for_advertised_progress() {
            let state_root = temp_state_root("manifest-state-missing-checkpoint");
            let dump_id = "dump-a";
            let source_identity =
                manifest_source_identity("/snap", "snapshot:001", ManifestSourceKind::Snapshot);
            let paths = ManifestSpoolPaths::for_dump(&state_root, dump_id);
            let mut meta = ManifestMeta::new(dump_id, source_identity.clone(), "/snap").unwrap();
            write_checkpointed_spool(&paths, &mut meta);
            fs::remove_file(&paths.checkpoint_path).unwrap();

            let state =
                ManifestSpoolState::open_or_recover(&state_root, dump_id, source_identity, "/snap")
                    .unwrap();

            assert_eq!(
                state
                    .recovery_events
                    .iter()
                    .map(|event| event.event)
                    .collect::<Vec<_>>(),
                vec![
                    ManifestSenderStateEvent::Corrupted,
                    ManifestSenderStateEvent::Rebuilt
                ]
            );
            assert_eq!(
                state.meta.last_invalidation_reason,
                Some(ManifestInvalidationReason::SpoolCorrupted)
            );
            assert!(state.last_checkpoint.is_none());

            let _ = fs::remove_dir_all(&state_root);
        }

        #[test]
        fn spool_state_rebuilds_when_checkpoint_points_beyond_log_prefix() {
            let state_root = temp_state_root("manifest-state-checkpoint-mismatch");
            let dump_id = "dump-a";
            let source_identity =
                manifest_source_identity("/snap", "snapshot:001", ManifestSourceKind::Snapshot);
            let paths = ManifestSpoolPaths::for_dump(&state_root, dump_id);
            let mut meta = ManifestMeta::new(dump_id, source_identity.clone(), "/snap").unwrap();
            let checkpoint = write_checkpointed_spool(&paths, &mut meta);
            let corrupted_checkpoint = ManifestCheckpoint {
                log_offset: checkpoint.log_offset + 1,
                ..checkpoint
            };
            corrupted_checkpoint
                .write_atomic(&paths.checkpoint_path)
                .unwrap();

            let state =
                ManifestSpoolState::open_or_recover(&state_root, dump_id, source_identity, "/snap")
                    .unwrap();

            assert_eq!(
                state
                    .recovery_events
                    .iter()
                    .map(|event| event.event)
                    .collect::<Vec<_>>(),
                vec![
                    ManifestSenderStateEvent::Corrupted,
                    ManifestSenderStateEvent::Rebuilt
                ]
            );
            assert_eq!(
                state.meta.last_invalidation_reason,
                Some(ManifestInvalidationReason::SpoolCorrupted)
            );
            assert!(state.last_checkpoint.is_none());

            let _ = fs::remove_dir_all(&state_root);
        }

        #[test]
        fn spool_state_cleanup_removes_state_directory() {
            let state_root = temp_state_root("manifest-state-cleanup");
            let dump_id = "dump-a";
            let source_identity =
                manifest_source_identity("/snap", "snapshot:001", ManifestSourceKind::Pinned);
            let state =
                ManifestSpoolState::open_or_recover(&state_root, dump_id, source_identity, "/snap")
                    .unwrap();
            fs::write(&state.paths.log_path, b"stale").unwrap();

            state.cleanup_after_finish().unwrap();

            assert!(!state.paths.state_dir.exists());
            let _ = fs::remove_dir_all(&state_root);
        }

        #[test]
        fn spool_state_gc_candidates_return_sorted_directories_only() {
            let state_root = temp_state_root("manifest-state-gc");
            let stale_b = state_root.join("b");
            let stale_a = state_root.join("a");
            let file = state_root.join("c.txt");
            fs::create_dir_all(&stale_b).unwrap();
            fs::create_dir_all(&stale_a).unwrap();
            fs::write(&file, b"not-a-dir").unwrap();

            let candidates = ManifestSpoolState::collect_gc_candidates(
                &state_root,
                SystemTime::now(),
                Duration::ZERO,
            )
            .unwrap();

            assert_eq!(candidates, vec![stale_a, stale_b]);
            let _ = fs::remove_dir_all(&state_root);
        }

        #[test]
        fn spool_builder_maps_skipped_entry_reasons_into_manifest_skip_reasons() {
            assert_eq!(
                manifest_discovery_skip_reason(&super::super::SkippedEntryReason::Unsupported(
                    super::super::UnsupportedEntryKind::Socket,
                )),
                ManifestDiscoverySkipReason::UnsupportedEntry {
                    kind: ManifestSpecialEntryKind::Socket,
                }
            );
            assert_eq!(
                manifest_discovery_skip_reason(
                    &super::super::SkippedEntryReason::VirtualFilesystem { kind: "procfs" },
                ),
                ManifestDiscoverySkipReason::VirtualFilesystem {
                    kind: "procfs".into(),
                }
            );
        }

        #[test]
        fn spool_builder_streams_canonical_dfs_records_to_disk() {
            let temp_root = temp_state_root("manifest-builder-order");
            let source = temp_root.join("source");
            fs::create_dir_all(source.join("alpha").join("beta")).unwrap();
            fs::write(source.join("alpha").join("beta").join("gamma.txt"), b"1").unwrap();
            fs::write(source.join("root.txt"), b"2").unwrap();
            let spool_path = temp_root.join(MANIFEST_LOG_FILE_NAME);

            let report = ManifestSpoolBuilder::build_to_path(&source, &spool_path).unwrap();
            let bytes = fs::read(&spool_path).unwrap();
            let recovered = ManifestSpoolReader::recover_prefix_bytes(&bytes).unwrap();

            assert_eq!(
                recovered.records,
                vec![
                    ManifestRecord::new(
                        ManifestRecordSeq(1),
                        ManifestRecordKind::DirectoryEnter,
                        0,
                        "alpha",
                        None,
                        ManifestRecordPayload::None,
                    )
                    .unwrap(),
                    ManifestRecord::new(
                        ManifestRecordSeq(2),
                        ManifestRecordKind::DirectoryEnter,
                        1,
                        "beta",
                        None,
                        ManifestRecordPayload::None,
                    )
                    .unwrap(),
                    ManifestRecord::new(
                        ManifestRecordSeq(3),
                        ManifestRecordKind::File,
                        2,
                        "gamma.txt",
                        Some(ManifestWorkOrdinal(1)),
                        ManifestRecordPayload::None,
                    )
                    .unwrap(),
                    ManifestRecord::new(
                        ManifestRecordSeq(4),
                        ManifestRecordKind::DirectoryLeave,
                        1,
                        "beta",
                        None,
                        ManifestRecordPayload::None,
                    )
                    .unwrap(),
                    ManifestRecord::new(
                        ManifestRecordSeq(5),
                        ManifestRecordKind::DirectoryLeave,
                        0,
                        "alpha",
                        None,
                        ManifestRecordPayload::None,
                    )
                    .unwrap(),
                    ManifestRecord::new(
                        ManifestRecordSeq(6),
                        ManifestRecordKind::File,
                        0,
                        "root.txt",
                        Some(ManifestWorkOrdinal(2)),
                        ManifestRecordPayload::None,
                    )
                    .unwrap(),
                ]
            );
            assert_eq!(
                report,
                ManifestBuildReport {
                    records_written: 6,
                    work_items_written: 2,
                    discovery_issues_written: 0,
                    subtree_issues_written: 0,
                    spool_bytes_written: bytes.len() as u64,
                    final_record_seq: Some(ManifestRecordSeq(6)),
                    final_work_ordinal: Some(ManifestWorkOrdinal(2)),
                }
            );

            let _ = fs::remove_dir_all(&temp_root);
        }

        #[test]
        fn spool_builder_single_pass_checkpoint_matches_reader_checkpoint_pass() {
            let temp_root = temp_state_root("manifest-builder-checkpoint");
            let source = temp_root.join("source");
            fs::create_dir_all(source.join("alpha").join("beta")).unwrap();
            fs::write(source.join("alpha").join("beta").join("gamma.txt"), b"1").unwrap();
            fs::write(source.join("alpha").join("delta.txt"), b"2").unwrap();
            fs::write(source.join("root.txt"), b"3").unwrap();
            let spool_path = temp_root.join(MANIFEST_LOG_FILE_NAME);

            let (report, single_pass_checkpoint) =
                ManifestSpoolBuilder::build_to_path_with_limit_and_checkpoint(
                    &source,
                    &spool_path,
                    u64::MAX,
                    Some(2),
                )
                .unwrap();
            let bytes = fs::read(&spool_path).unwrap();
            let second_pass = ManifestSpoolReader::build_sparse_checkpoints(&bytes, 2).unwrap();

            assert_eq!(report.spool_bytes_written, bytes.len() as u64);
            assert_eq!(
                single_pass_checkpoint,
                second_pass.checkpoints.last().cloned()
            );

            let _ = fs::remove_dir_all(&temp_root);
        }

        #[test]
        fn spool_builder_rejects_quota_without_writing_partial_record() {
            let temp_root = temp_state_root("manifest-builder-quota");
            let source = temp_root.join("source");
            fs::create_dir_all(source.join("alpha")).unwrap();
            fs::write(source.join("root.txt"), b"root").unwrap();
            fs::write(source.join("alpha").join("child.txt"), b"child").unwrap();
            let spool_path = temp_root.join(MANIFEST_LOG_FILE_NAME);

            let full_report = ManifestSpoolBuilder::build_to_path(&source, &spool_path).unwrap();
            let full_bytes = fs::read(&spool_path).unwrap();
            assert_eq!(full_report.spool_bytes_written, full_bytes.len() as u64);

            let quota = full_report.spool_bytes_written.saturating_sub(1);
            let error = ManifestSpoolBuilder::build_to_path_with_limit(&source, &spool_path, quota)
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("sender manifest spool quota exceeded")
            );

            let limited_bytes = fs::read(&spool_path).unwrap();
            let recovered = ManifestSpoolReader::recover_prefix_bytes(&limited_bytes).unwrap();
            assert_eq!(recovered.truncate_to_len, limited_bytes.len() as u64);
            assert!(limited_bytes.len() as u64 <= quota);
            assert!(recovered.records.len() < 4);

            let _ = fs::remove_dir_all(&temp_root);
        }

        #[test]
        fn spool_builder_emits_subtree_skip_and_closes_directory_after_read_error() {
            let _guard = FsFailureGuard::new();
            let temp_root = temp_state_root("manifest-builder-subtree-skip");
            let source = temp_root.join("source");
            let broken = source.join("broken");
            let healthy = source.join("healthy");
            fs::create_dir_all(&broken).unwrap();
            fs::create_dir_all(&healthy).unwrap();
            fs::write(healthy.join("ok.txt"), b"ok").unwrap();
            super::super::test_hooks::fail_next_read_dir(&broken);

            let mut builder = ManifestSpoolBuilder::new(&source).unwrap();
            let mut bytes = Vec::new();
            let report = builder.build_into(&mut bytes).unwrap();
            let recovered = ManifestSpoolReader::recover_prefix_bytes(&bytes).unwrap();

            assert_eq!(report.subtree_issues_written, 1);
            assert_eq!(
                recovered.records[0].kind,
                ManifestRecordKind::DirectoryEnter
            );
            assert_eq!(recovered.records[0].name_component.as_ref(), "broken");
            assert_eq!(recovered.records[1].kind, ManifestRecordKind::SubtreeSkip);
            match &recovered.records[1].payload {
                ManifestRecordPayload::SubtreeSkip {
                    reason: ManifestSubtreeSkipReason::ReadDirFailed { error },
                } => assert!(
                    error.contains("failed to read sender directory") && error.contains("broken"),
                    "unexpected error: {error}"
                ),
                payload => panic!("unexpected payload: {payload:?}"),
            }
            assert_eq!(
                recovered.records[2].kind,
                ManifestRecordKind::DirectoryLeave
            );
            assert_eq!(recovered.records[2].name_component.as_ref(), "broken");
            assert_eq!(
                recovered.records[4],
                ManifestRecord::new(
                    ManifestRecordSeq(5),
                    ManifestRecordKind::File,
                    1,
                    "ok.txt",
                    Some(ManifestWorkOrdinal(1)),
                    ManifestRecordPayload::None,
                )
                .unwrap()
            );

            let _ = fs::remove_dir_all(&temp_root);
        }

        #[test]
        fn spool_builder_records_unreadable_symlink_as_discovery_skip_without_work_ordinal() {
            let _guard = FsFailureGuard::new();
            let temp_root = temp_state_root("manifest-builder-symlink-skip");
            let source = temp_root.join("source");
            fs::create_dir_all(&source).unwrap();
            let target = source.join("target.txt");
            fs::write(&target, b"payload").unwrap();
            let symlink = source.join("link.txt");
            match create_symlink(&symlink, &target) {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::Unsupported
                    ) || error.raw_os_error() == Some(1314) =>
                {
                    let _ = fs::remove_dir_all(&temp_root);
                    return;
                }
                Err(error) => panic!("failed to create symlink fixture: {error}"),
            }
            super::super::test_hooks::fail_next_read_link(&symlink);

            let mut builder = ManifestSpoolBuilder::new(&source).unwrap();
            let mut bytes = Vec::new();
            let report = builder.build_into(&mut bytes).unwrap();
            let recovered = ManifestSpoolReader::recover_prefix_bytes(&bytes).unwrap();

            assert_eq!(report.work_items_written, 1);
            assert_eq!(report.discovery_issues_written, 1);
            assert_eq!(recovered.records[0].kind, ManifestRecordKind::DiscoverySkip);
            assert_eq!(recovered.records[0].name_component.as_ref(), "link.txt");
            assert_eq!(recovered.records[0].work_ordinal, None);
            match &recovered.records[0].payload {
                ManifestRecordPayload::DiscoverySkip {
                    reason: ManifestDiscoverySkipReason::UnreadableSymlinkTarget { error },
                } => assert!(
                    error.contains("sender test hook injected failure for read_link:"),
                    "unexpected error: {error}"
                ),
                payload => panic!("unexpected payload: {payload:?}"),
            }
            assert_eq!(
                recovered.records[1],
                ManifestRecord::new(
                    ManifestRecordSeq(2),
                    ManifestRecordKind::File,
                    0,
                    "target.txt",
                    Some(ManifestWorkOrdinal(1)),
                    ManifestRecordPayload::None,
                )
                .unwrap()
            );

            let _ = fs::remove_dir_all(&temp_root);
        }

        #[test]
        fn spool_writer_and_reader_roundtrip_valid_records() {
            let records = vec![
                ManifestRecord::new(
                    ManifestRecordSeq(1),
                    ManifestRecordKind::DirectoryEnter,
                    0,
                    "root",
                    None,
                    ManifestRecordPayload::None,
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(2),
                    ManifestRecordKind::Symlink,
                    1,
                    "link",
                    Some(ManifestWorkOrdinal(1)),
                    ManifestRecordPayload::SymlinkTarget {
                        target: "nested/target".into(),
                    },
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(3),
                    ManifestRecordKind::DirectoryLeave,
                    0,
                    "root",
                    None,
                    ManifestRecordPayload::None,
                )
                .unwrap(),
            ];
            let mut bytes = Vec::new();
            for record in &records {
                ManifestSpoolWriter::append_record(&mut bytes, record).unwrap();
            }

            let recovered = ManifestSpoolReader::recover_prefix_bytes(&bytes).unwrap();
            assert_eq!(recovered.records, records);
            assert_eq!(recovered.truncate_to_len, bytes.len() as u64);
        }

        #[test]
        fn spool_recovery_trims_torn_tail_to_last_complete_record() {
            let first = ManifestRecord::new(
                ManifestRecordSeq(1),
                ManifestRecordKind::File,
                0,
                "alpha.txt",
                Some(ManifestWorkOrdinal(1)),
                ManifestRecordPayload::None,
            )
            .unwrap();
            let second = ManifestRecord::new(
                ManifestRecordSeq(2),
                ManifestRecordKind::File,
                0,
                "beta.txt",
                Some(ManifestWorkOrdinal(2)),
                ManifestRecordPayload::None,
            )
            .unwrap();

            let first_bytes = ManifestSpoolWriter::encode_record(&first).unwrap();
            let second_bytes = ManifestSpoolWriter::encode_record(&second).unwrap();
            let mut bytes = first_bytes.clone();
            bytes.extend_from_slice(&second_bytes[..second_bytes.len() / 2]);

            let recovered = ManifestSpoolReader::recover_prefix_bytes(&bytes).unwrap();
            assert_eq!(recovered.records, vec![first]);
            assert_eq!(recovered.truncate_to_len, first_bytes.len() as u64);
        }

        #[test]
        fn spool_recovery_rejects_mid_file_corruption() {
            let record = ManifestRecord::new(
                ManifestRecordSeq(1),
                ManifestRecordKind::DiscoverySkip,
                0,
                "socket",
                None,
                ManifestRecordPayload::DiscoverySkip {
                    reason: ManifestDiscoverySkipReason::UnsupportedEntry {
                        kind: ManifestSpecialEntryKind::Socket,
                    },
                },
            )
            .unwrap();
            let mut bytes = ManifestSpoolWriter::encode_record(&record).unwrap();
            *bytes.last_mut().unwrap() ^= 0x5A;

            let error = ManifestSpoolReader::recover_prefix_bytes(&bytes).unwrap_err();
            assert!(
                error.to_string().contains("checksum mismatch"),
                "unexpected error: {error:#}"
            );
        }

        #[test]
        fn checkpoint_roundtrip_uses_atomic_replace() {
            let checkpoint = ManifestCheckpoint {
                checkpoint_seq: 4,
                last_record_seq: ManifestRecordSeq(11),
                last_work_ordinal: Some(ManifestWorkOrdinal(7)),
                log_offset: 4096,
                path_stack: vec![
                    ManifestCheckpointFrame {
                        name_component: "alpha".into(),
                        child_emitted: true,
                    },
                    ManifestCheckpointFrame {
                        name_component: "beta".into(),
                        child_emitted: false,
                    },
                ],
            };
            let temp_root = std::env::temp_dir().join(format!(
                "fast-dump-checkpoint-test-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            fs::create_dir_all(&temp_root).unwrap();
            let checkpoint_path = temp_root.join(MANIFEST_CHECKPOINT_FILE_NAME);

            checkpoint.write_atomic(&checkpoint_path).unwrap();
            let decoded = ManifestCheckpoint::read_from_path(&checkpoint_path).unwrap();

            assert_eq!(decoded, checkpoint);
            assert!(!checkpoint_temp_path(&checkpoint_path).exists());
            let _ = fs::remove_dir_all(&temp_root);
        }

        #[test]
        fn sparse_checkpoints_capture_open_directory_state() {
            let records = vec![
                ManifestRecord::new(
                    ManifestRecordSeq(1),
                    ManifestRecordKind::DirectoryEnter,
                    0,
                    "root",
                    None,
                    ManifestRecordPayload::None,
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(2),
                    ManifestRecordKind::File,
                    1,
                    "child.txt",
                    Some(ManifestWorkOrdinal(1)),
                    ManifestRecordPayload::None,
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(3),
                    ManifestRecordKind::DirectoryLeave,
                    0,
                    "root",
                    None,
                    ManifestRecordPayload::None,
                )
                .unwrap(),
            ];
            let mut bytes = Vec::new();
            for record in &records {
                ManifestSpoolWriter::append_record(&mut bytes, record).unwrap();
            }

            let report = ManifestSpoolReader::build_sparse_checkpoints(&bytes, 1).unwrap();
            assert_eq!(report.truncate_to_len, bytes.len() as u64);
            assert_eq!(report.checkpoints.len(), 3);
            assert_eq!(
                report.checkpoints[0].path_stack,
                vec![ManifestCheckpointFrame {
                    name_component: "root".into(),
                    child_emitted: false
                }]
            );
            assert_eq!(
                report.checkpoints[1].path_stack,
                vec![ManifestCheckpointFrame {
                    name_component: "root".into(),
                    child_emitted: true
                }]
            );
            assert!(report.checkpoints[2].path_stack.is_empty());
        }

        #[test]
        fn replay_uses_nearest_checkpoint_and_replays_only_tail() {
            let records = vec![
                ManifestRecord::new(
                    ManifestRecordSeq(1),
                    ManifestRecordKind::DirectoryEnter,
                    0,
                    "root",
                    None,
                    ManifestRecordPayload::None,
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(2),
                    ManifestRecordKind::File,
                    1,
                    "a.txt",
                    Some(ManifestWorkOrdinal(1)),
                    ManifestRecordPayload::None,
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(3),
                    ManifestRecordKind::File,
                    1,
                    "b.txt",
                    Some(ManifestWorkOrdinal(2)),
                    ManifestRecordPayload::None,
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(4),
                    ManifestRecordKind::File,
                    1,
                    "c.txt",
                    Some(ManifestWorkOrdinal(3)),
                    ManifestRecordPayload::None,
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(5),
                    ManifestRecordKind::DirectoryLeave,
                    0,
                    "root",
                    None,
                    ManifestRecordPayload::None,
                )
                .unwrap(),
            ];
            let mut bytes = Vec::new();
            for record in &records {
                ManifestSpoolWriter::append_record(&mut bytes, record).unwrap();
            }

            let checkpoints = ManifestSpoolReader::build_sparse_checkpoints(&bytes, 2)
                .unwrap()
                .checkpoints;
            let replay = ManifestSpoolReader::replay_from_checkpoints(
                &bytes,
                &checkpoints[..2],
                ManifestRecordSeq(5),
            )
            .unwrap();

            assert_eq!(
                replay
                    .checkpoint
                    .as_ref()
                    .map(|value| value.last_record_seq.0),
                Some(4)
            );
            assert_eq!(replay.replayed_records.len(), 1);
            assert_eq!(replay.replayed_records[0], records[4]);
            assert_eq!(
                replay.final_state.last_record_seq(),
                Some(ManifestRecordSeq(5))
            );
            assert_eq!(replay.metrics.checkpoint_seq, Some(2));
            assert_eq!(replay.metrics.replayed_records, 1);
            assert!(replay.metrics.replayed_bytes > 0);
        }

        #[test]
        fn replay_restores_subtree_skip_eligibility_from_checkpoint_state() {
            let records = vec![
                ManifestRecord::new(
                    ManifestRecordSeq(1),
                    ManifestRecordKind::DirectoryEnter,
                    0,
                    "broken",
                    None,
                    ManifestRecordPayload::None,
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(2),
                    ManifestRecordKind::SubtreeSkip,
                    0,
                    "broken",
                    None,
                    ManifestRecordPayload::SubtreeSkip {
                        reason: ManifestSubtreeSkipReason::ReadDirFailed {
                            error: "permission denied".into(),
                        },
                    },
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(3),
                    ManifestRecordKind::DirectoryLeave,
                    0,
                    "broken",
                    None,
                    ManifestRecordPayload::None,
                )
                .unwrap(),
            ];
            let mut bytes = Vec::new();
            for record in &records {
                ManifestSpoolWriter::append_record(&mut bytes, record).unwrap();
            }

            let checkpoints = ManifestSpoolReader::build_sparse_checkpoints(&bytes, 1)
                .unwrap()
                .checkpoints;
            let replay_from_first = ManifestSpoolReader::replay_from_checkpoints(
                &bytes,
                &checkpoints[..1],
                ManifestRecordSeq(3),
            )
            .unwrap();
            assert_eq!(
                replay_from_first.replayed_records,
                vec![records[1].clone(), records[2].clone()]
            );
            assert_eq!(
                replay_from_first.final_state.last_record_seq(),
                Some(ManifestRecordSeq(3))
            );
        }
    }
}

#[derive(Debug, Clone)]
struct DirectoryFrame {
    entries: Vec<DiscoveredEntry>,
}

#[derive(Debug, Clone)]
struct DiscoveredEntry {
    rel_path: String,
    abs_path: PathBuf,
    kind: DiscoveredEntryKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DiscoveredEntryKind {
    Directory,
    Regular,
    Symlink { target: PathBuf },
    Skipped(SkippedEntryReason),
}

#[cfg_attr(not(unix), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnsupportedEntryKind {
    Socket,
    Fifo,
    BlockDevice,
    CharDevice,
    Other,
}

impl std::fmt::Display for UnsupportedEntryKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let label = match self {
            Self::Socket => "socket",
            Self::Fifo => "fifo",
            Self::BlockDevice => "block-device",
            Self::CharDevice => "char-device",
            Self::Other => "special",
        };
        f.write_str(label)
    }
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq)]
enum SkippedEntryReason {
    Unsupported(UnsupportedEntryKind),
    UnreadableSymlinkTarget { error: String },
    VirtualFilesystem { kind: &'static str },
}

#[derive(Debug, Clone, Default)]
struct EntryClassifier {
    #[cfg(target_os = "linux")]
    linux_mounts: LinuxMountTable,
}

impl EntryClassifier {
    fn new() -> Self {
        #[cfg(target_os = "linux")]
        {
            let linux_mounts = match LinuxMountTable::load() {
                Ok(table) => table,
                Err(error) => {
                    debug!(
                        "sender mountinfo cache disabled: failed to load /proc/self/mountinfo: {}",
                        error
                    );
                    LinuxMountTable::default()
                }
            };
            Self { linux_mounts }
        }

        #[cfg(not(target_os = "linux"))]
        {
            Self::default()
        }
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Default)]
struct LinuxMountTable {
    mounts: Vec<LinuxMount>,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone)]
struct LinuxMount {
    mount_point: PathBuf,
    virtual_kind: Option<&'static str>,
}

enum DirectoryListingCommand {
    List(PathBuf),
    Stop,
}

struct DirectoryListingResult {
    key: String,
    entries: std::result::Result<Vec<DiscoveredEntry>, String>,
}

enum DirectoryListingState {
    Pending,
    Ready(std::result::Result<Vec<DiscoveredEntry>, String>),
}

#[derive(Clone)]
struct DirectoryListingPool {
    inner: Arc<DirectoryListingPoolInner>,
}

struct DirectoryListingPoolInner {
    source: PathBuf,
    classifier: EntryClassifier,
    task_tx: Sender<DirectoryListingCommand>,
    result_rx: Receiver<DirectoryListingResult>,
    pending: Mutex<BTreeMap<String, DirectoryListingState>>,
    profile: Option<PipelineProfileCollector>,
    worker_count: usize,
    inflight_limit: usize,
    workers: Mutex<Vec<thread::JoinHandle<()>>>,
}

impl std::fmt::Debug for DirectoryListingPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectoryListingPool")
            .field("worker_count", &self.inner.worker_count)
            .field("inflight_limit", &self.inner.inflight_limit)
            .finish()
    }
}

impl DirectoryListingPool {
    fn new(
        source: &Path,
        classifier: EntryClassifier,
        profile: Option<&PipelineProfileCollector>,
    ) -> Self {
        let worker_count = thread::available_parallelism()
            .map(|parallelism| parallelism.get().clamp(1, 4))
            .unwrap_or(2);
        let inflight_limit = worker_count.saturating_mul(4).max(1);
        let (task_tx, task_rx) = bounded::<DirectoryListingCommand>(inflight_limit);
        let (result_tx, result_rx) = unbounded::<DirectoryListingResult>();
        let mut workers = Vec::with_capacity(worker_count);

        for _ in 0..worker_count {
            let task_rx = task_rx.clone();
            let result_tx = result_tx.clone();
            let source = source.to_path_buf();
            let classifier = classifier.clone();
            workers.push(thread::spawn(move || {
                while let Ok(command) = task_rx.recv() {
                    match command {
                        DirectoryListingCommand::List(abs_path) => {
                            let key = directory_listing_key(&abs_path);
                            let entries = read_sorted_entries(&source, &classifier, &abs_path)
                                .map_err(|error| error.to_string());
                            if result_tx
                                .send(DirectoryListingResult { key, entries })
                                .is_err()
                            {
                                break;
                            }
                        }
                        DirectoryListingCommand::Stop => break,
                    }
                }
            }));
        }

        Self {
            inner: Arc::new(DirectoryListingPoolInner {
                source: source.to_path_buf(),
                classifier,
                task_tx,
                result_rx,
                pending: Mutex::new(BTreeMap::new()),
                profile: profile.cloned(),
                worker_count,
                inflight_limit,
                workers: Mutex::new(workers),
            }),
        }
    }

    fn prefetch_directories(&self, entries: &[DiscoveredEntry]) {
        for entry in entries {
            if matches!(entry.kind, DiscoveredEntryKind::Directory) {
                self.submit_if_room(&entry.abs_path);
            }
        }
    }

    fn take_or_read(&self, dir: &Path) -> Result<Vec<DiscoveredEntry>> {
        let key = directory_listing_key(dir);
        loop {
            let ready = {
                let mut pending = self.inner.pending.lock().unwrap();
                match pending.remove(&key) {
                    Some(DirectoryListingState::Ready(result)) => Some(result),
                    Some(DirectoryListingState::Pending) => {
                        pending.insert(key.clone(), DirectoryListingState::Pending);
                        None
                    }
                    None => {
                        return read_sorted_entries(
                            &self.inner.source,
                            &self.inner.classifier,
                            dir,
                        );
                    }
                }
            };
            if let Some(result) = ready {
                return result.map_err(anyhow::Error::msg);
            }

            let waited_at = Instant::now();
            let result = self
                .inner
                .result_rx
                .recv()
                .context("sender directory listing pool disconnected unexpectedly")?;
            if let Some(profile) = &self.inner.profile {
                profile.record_traversal_wait(waited_at.elapsed());
            }
            self.inner
                .pending
                .lock()
                .unwrap()
                .insert(result.key, DirectoryListingState::Ready(result.entries));
        }
    }

    fn submit_if_room(&self, dir: &Path) {
        let key = directory_listing_key(dir);
        let should_submit = {
            let mut pending = self.inner.pending.lock().unwrap();
            if pending.contains_key(&key) || pending.len() >= self.inner.inflight_limit {
                false
            } else {
                pending.insert(key.clone(), DirectoryListingState::Pending);
                if let Some(profile) = &self.inner.profile {
                    let inflight = pending
                        .values()
                        .filter(|state| matches!(state, DirectoryListingState::Pending))
                        .count();
                    profile.record_directory_inflight(inflight);
                }
                true
            }
        };
        if !should_submit {
            return;
        }

        match self
            .inner
            .task_tx
            .try_send(DirectoryListingCommand::List(dir.to_path_buf()))
        {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.inner.pending.lock().unwrap().remove(&key);
            }
        }
    }
}

impl Drop for DirectoryListingPoolInner {
    fn drop(&mut self) {
        for _ in 0..self.worker_count {
            let _ = self.task_tx.send(DirectoryListingCommand::Stop);
        }
        for worker in self.workers.get_mut().unwrap().drain(..) {
            let _ = worker.join();
        }
    }
}

fn directory_listing_key(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(target_os = "linux")]
impl LinuxMountTable {
    fn load() -> Result<Self> {
        let contents = fs::read_to_string("/proc/self/mountinfo")
            .context("failed to read /proc/self/mountinfo")?;
        Self::parse(&contents)
    }

    fn parse(contents: &str) -> Result<Self> {
        let mut mounts = Vec::new();
        for (line_no, line) in contents.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }

            let (left, right) = line.split_once(" - ").with_context(|| {
                format!(
                    "failed to parse mountinfo line {}: missing separator",
                    line_no + 1
                )
            })?;
            let left_fields = left.split_whitespace().collect::<Vec<_>>();
            if left_fields.len() < 5 {
                bail!(
                    "failed to parse mountinfo line {}: expected at least 5 fields before separator",
                    line_no + 1
                );
            }

            let right_fields = right.split_whitespace().collect::<Vec<_>>();
            if right_fields.is_empty() {
                bail!(
                    "failed to parse mountinfo line {}: expected filesystem type after separator",
                    line_no + 1
                );
            }

            mounts.push(LinuxMount {
                mount_point: parse_linux_mountinfo_path(left_fields[4])?,
                virtual_kind: linux_virtual_filesystem_kind_from_mountinfo_fs_type(right_fields[0]),
            });
        }

        mounts.sort_by(|left, right| {
            right
                .mount_point
                .as_os_str()
                .len()
                .cmp(&left.mount_point.as_os_str().len())
        });
        Ok(Self { mounts })
    }

    fn virtual_kind_for_path(&self, path: &Path) -> Option<&'static str> {
        for mount in &self.mounts {
            if path.strip_prefix(&mount.mount_point).is_ok() {
                return mount.virtual_kind;
            }
        }

        None
    }
}

#[cfg(target_os = "linux")]
fn parse_linux_mountinfo_path(raw: &str) -> Result<PathBuf> {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let mut bytes = Vec::with_capacity(raw.len());
    let raw_bytes = raw.as_bytes();
    let mut index = 0;
    while index < raw_bytes.len() {
        if raw_bytes[index] == b'\\' && index + 3 < raw_bytes.len() {
            let octal = &raw_bytes[index + 1..index + 4];
            if octal.iter().all(|byte| (b'0'..=b'7').contains(byte)) {
                let value = (octal[0] - b'0') * 64 + (octal[1] - b'0') * 8 + (octal[2] - b'0');
                bytes.push(value);
                index += 4;
                continue;
            }
        }

        bytes.push(raw_bytes[index]);
        index += 1;
    }

    Ok(PathBuf::from(OsStr::from_bytes(&bytes)))
}

#[cfg(target_os = "linux")]
fn linux_virtual_filesystem_kind_from_mountinfo_fs_type(fs_type: &str) -> Option<&'static str> {
    match fs_type {
        "proc" => Some("procfs"),
        "sysfs" => Some("sysfs"),
        "cgroup" => Some("cgroup"),
        "cgroup2" => Some("cgroup2"),
        "debugfs" => Some("debugfs"),
        "tracefs" => Some("tracefs"),
        "securityfs" => Some("securityfs"),
        "configfs" => Some("configfs"),
        "bpf" => Some("bpf"),
        "mqueue" => Some("mqueue"),
        "devpts" => Some("devpts"),
        _ => None,
    }
}

#[derive(Debug, Clone)]
enum PendingFileWalkerMode {
    #[cfg(test)]
    Live(LivePendingFileWalker),
    Manifest(Box<manifest::ManifestPendingLeafWalker>),
    Progressive(Box<manifest::ProgressiveManifestPendingLeafWalker>),
}

#[derive(Debug, Clone)]
pub(crate) struct PendingFileWalker {
    mode: PendingFileWalkerMode,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct LivePendingFileWalker {
    resume_state: FileResumeState,
    listing_pool: DirectoryListingPool,
    stack: Vec<DirectoryFrame>,
    next_index: u64,
    last_discovered_file: Option<InputFile>,
    resume_validated: bool,
    exhausted: bool,
}

#[cfg(test)]
mod test_hooks {
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};

    static FS_FAILURES: OnceLock<Mutex<BTreeMap<String, usize>>> = OnceLock::new();
    thread_local! {
        static MANIFEST_SPOOL_MAX_BYTES_OVERRIDE: Cell<Option<u64>> = const { Cell::new(None) };
    }

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

    fn read_link_key(path: &Path) -> String {
        format!("read_link:{}", path_key(path))
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

    pub(crate) fn fail_next_read_link(path: &Path) {
        fs_failures().lock().unwrap().insert(read_link_key(path), 1);
    }

    pub(crate) fn before_read_dir(path: &Path) -> std::io::Result<()> {
        maybe_fail(read_dir_key(path))
    }

    pub(crate) fn before_metadata(path: &Path) -> std::io::Result<()> {
        maybe_fail(metadata_key(path))
    }

    pub(crate) fn before_read_link(path: &Path) -> std::io::Result<()> {
        maybe_fail(read_link_key(path))
    }

    pub(crate) fn manifest_spool_max_bytes_override_value() -> Option<u64> {
        MANIFEST_SPOOL_MAX_BYTES_OVERRIDE.with(Cell::get)
    }

    pub(crate) fn set_manifest_spool_max_bytes_override(value: Option<u64>) {
        MANIFEST_SPOOL_MAX_BYTES_OVERRIDE.with(|current| current.set(value));
    }

    pub(crate) fn clear() {
        fs_failures().lock().unwrap().clear();
        MANIFEST_SPOOL_MAX_BYTES_OVERRIDE.with(|current| current.set(None));
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

    pub(crate) fn before_read_link(_path: &Path) -> std::io::Result<()> {
        Ok(())
    }

    pub(crate) fn manifest_spool_max_bytes_override_value() -> Option<u64> {
        None
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

fn read_link_path(path: &Path) -> std::io::Result<PathBuf> {
    test_hooks::before_read_link(path)?;

    #[cfg(unix)]
    if let (Some(parent), Some(component)) = (path.parent(), path.file_name()) {
        return read_link_at(&File::open(parent)?, component);
    }

    fs::read_link(path)
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
// production sender path no longer depends on the full pending-list.
pub(crate) fn collect_pending_files(
    source: impl Into<PathBuf>,
    resume_state: &FileResumeState,
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
    let classifier = EntryClassifier::new();

    let files = discover_files(&source, &classifier)?;
    validate_resume_state_against_files(&files, resume_state)?;
    let discovered_files = files.len();
    let final_file_index = files
        .last()
        .map(|file| file.index)
        .unwrap_or(resume_state.last_committed_file_index);
    let final_file_path = files
        .last()
        .map(|file| file.rel_path_string())
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

fn manifest_root_identity_path(source: &Path) -> Result<String> {
    let canonical = fs::canonicalize(source)
        .with_context(|| format!("failed to canonicalize sender source {}", source.display()))?;
    Ok(canonical.to_string_lossy().replace('\\', "/"))
}

fn sender_manifest_source_identity(
    source: &SenderSource,
) -> Result<manifest::ManifestSourceIdentity> {
    let root = manifest_root_identity_path(source.path())?;
    manifest::ManifestSourceIdentity::new(
        root.clone(),
        root,
        MANIFEST_BUILDER_VERSION,
        source.kind().manifest_source_kind(),
    )
}

fn sender_manifest_spool_max_bytes() -> u64 {
    test_hooks::manifest_spool_max_bytes_override_value()
        .or_else(env_override_manifest_spool_max_bytes)
        .unwrap_or(DEFAULT_SENDER_MANIFEST_SPOOL_MAX_BYTES)
}

fn env_override_manifest_spool_max_bytes() -> Option<u64> {
    let raw = std::env::var(TEST_SENDER_MANIFEST_SPOOL_MAX_BYTES_ENV).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    match trimmed.parse::<u64>() {
        Ok(value) => Some(value),
        Err(error) => {
            debug!(
                "ignoring invalid {}={:?}: {}",
                TEST_SENDER_MANIFEST_SPOOL_MAX_BYTES_ENV, raw, error
            );
            None
        }
    }
}

fn sender_manifest_state_root() -> PathBuf {
    std::env::temp_dir().join("fast-dump").join("sender-state")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourceDiscoveryIssueScope {
    Entry,
    Subtree,
    SenderState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SourceDiscoveryIssueReason {
    UnsupportedEntry {
        kind: manifest::ManifestSpecialEntryKind,
    },
    UnreadableSymlinkTarget {
        error: String,
    },
    VirtualFilesystem {
        kind: String,
    },
    Io {
        operation: manifest::ManifestIoOperation,
        error: String,
    },
    SenderState {
        event: manifest::ManifestSenderStateEvent,
        detail: String,
    },
    ReadDirFailed {
        error: String,
    },
    SourceInvalidated {
        detail: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceDiscoveryIssue {
    pub(crate) record_seq: Option<u64>,
    pub(crate) path: String,
    pub(crate) scope: SourceDiscoveryIssueScope,
    pub(crate) reason: SourceDiscoveryIssueReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct SourceDiscoveryIssueSummary {
    pub(crate) discovery_issues_total: u64,
    pub(crate) subtrees_skipped: u64,
    pub(crate) manifest_resets_total: u64,
}

fn source_discovery_issue_path(
    path_stack: &[Box<str>],
    depth: u16,
    name_component: &str,
) -> Result<String> {
    let depth = usize::from(depth);
    if depth > path_stack.len() {
        bail!(
            "manifest discovery issue depth {} exceeds path stack length {}",
            depth,
            path_stack.len()
        );
    }

    let mut components = path_stack
        .iter()
        .take(depth)
        .map(|component| component.as_ref())
        .collect::<Vec<_>>();
    components.push(name_component);
    Ok(components.join("/"))
}

fn source_discovery_reason_from_manifest_skip(
    reason: &manifest::ManifestDiscoverySkipReason,
) -> SourceDiscoveryIssueReason {
    match reason {
        manifest::ManifestDiscoverySkipReason::UnsupportedEntry { kind } => {
            SourceDiscoveryIssueReason::UnsupportedEntry { kind: *kind }
        }
        manifest::ManifestDiscoverySkipReason::UnreadableSymlinkTarget { error } => {
            SourceDiscoveryIssueReason::UnreadableSymlinkTarget {
                error: error.to_string(),
            }
        }
        manifest::ManifestDiscoverySkipReason::VirtualFilesystem { kind } => {
            SourceDiscoveryIssueReason::VirtualFilesystem {
                kind: kind.to_string(),
            }
        }
        manifest::ManifestDiscoverySkipReason::Io { operation, error } => {
            SourceDiscoveryIssueReason::Io {
                operation: *operation,
                error: error.to_string(),
            }
        }
        manifest::ManifestDiscoverySkipReason::SenderState { event, detail } => {
            SourceDiscoveryIssueReason::SenderState {
                event: *event,
                detail: detail.to_string(),
            }
        }
    }
}

fn source_discovery_reason_from_subtree_skip(
    reason: &manifest::ManifestSubtreeSkipReason,
) -> SourceDiscoveryIssueReason {
    match reason {
        manifest::ManifestSubtreeSkipReason::ReadDirFailed { error } => {
            SourceDiscoveryIssueReason::ReadDirFailed {
                error: error.to_string(),
            }
        }
        manifest::ManifestSubtreeSkipReason::SourceInvalidated { detail } => {
            SourceDiscoveryIssueReason::SourceInvalidated {
                detail: detail.to_string(),
            }
        }
        manifest::ManifestSubtreeSkipReason::Io { operation, error } => {
            SourceDiscoveryIssueReason::Io {
                operation: *operation,
                error: error.to_string(),
            }
        }
    }
}

fn summarize_source_discovery_issues(
    issues: &[SourceDiscoveryIssue],
) -> SourceDiscoveryIssueSummary {
    SourceDiscoveryIssueSummary {
        discovery_issues_total: issues.len() as u64,
        subtrees_skipped: issues
            .iter()
            .filter(|issue| issue.scope == SourceDiscoveryIssueScope::Subtree)
            .count() as u64,
        manifest_resets_total: issues
            .iter()
            .filter(|issue| {
                matches!(
                    issue.reason,
                    SourceDiscoveryIssueReason::SenderState {
                        event: manifest::ManifestSenderStateEvent::Rebuilt,
                        ..
                    }
                )
            })
            .count() as u64,
    }
}

fn collect_source_discovery_issues(
    state: &manifest::ManifestSpoolState,
) -> Result<Vec<SourceDiscoveryIssue>> {
    let mut issues = state
        .recovery_events
        .iter()
        .map(|event| SourceDiscoveryIssue {
            record_seq: None,
            path: state.meta.root_source_path.to_string(),
            scope: SourceDiscoveryIssueScope::SenderState,
            reason: SourceDiscoveryIssueReason::SenderState {
                event: event.event,
                detail: event.detail.to_string(),
            },
        })
        .collect::<Vec<_>>();

    if !state.paths.log_path.exists() && state.fresh_log_bytes.is_none() {
        return Ok(issues);
    }

    let recovery = state.recover_manifest_prefix()?;
    let mut path_stack = Vec::<Box<str>>::new();
    for record in recovery.records {
        match record.kind {
            manifest::ManifestRecordKind::DirectoryEnter => {
                if path_stack.len() != usize::from(record.depth) {
                    bail!(
                        "manifest directory enter depth {} does not match path stack length {}",
                        record.depth,
                        path_stack.len()
                    );
                }
                path_stack.push(record.name_component);
            }
            manifest::ManifestRecordKind::DirectoryLeave => {
                if path_stack.len() != usize::from(record.depth).saturating_add(1) {
                    bail!(
                        "manifest directory leave depth {} does not match path stack length {}",
                        record.depth,
                        path_stack.len()
                    );
                }
                let Some(last_component) = path_stack.pop() else {
                    bail!("manifest directory leave encountered empty path stack");
                };
                if last_component.as_ref() != record.name_component.as_ref() {
                    bail!(
                        "manifest directory leave component mismatch: expected `{}`, got `{}`",
                        last_component,
                        record.name_component
                    );
                }
            }
            manifest::ManifestRecordKind::DiscoverySkip => {
                let manifest::ManifestRecordPayload::DiscoverySkip { reason } = record.payload
                else {
                    bail!("manifest discovery skip record is missing discovery payload");
                };
                issues.push(SourceDiscoveryIssue {
                    record_seq: Some(record.record_seq.0),
                    path: source_discovery_issue_path(
                        &path_stack,
                        record.depth,
                        record.name_component.as_ref(),
                    )?,
                    scope: SourceDiscoveryIssueScope::Entry,
                    reason: source_discovery_reason_from_manifest_skip(&reason),
                });
            }
            manifest::ManifestRecordKind::SubtreeSkip => {
                let manifest::ManifestRecordPayload::SubtreeSkip { reason } = record.payload else {
                    bail!("manifest subtree skip record is missing subtree payload");
                };
                issues.push(SourceDiscoveryIssue {
                    record_seq: Some(record.record_seq.0),
                    path: source_discovery_issue_path(
                        &path_stack,
                        record.depth,
                        record.name_component.as_ref(),
                    )?,
                    scope: SourceDiscoveryIssueScope::Subtree,
                    reason: source_discovery_reason_from_subtree_skip(&reason),
                });
            }
            manifest::ManifestRecordKind::File | manifest::ManifestRecordKind::Symlink => {}
        }
    }

    Ok(issues)
}

pub(crate) fn ensure_sealed_manifest_state(
    source: &SenderSource,
    dump_id: &str,
    profile: Option<&PipelineProfileCollector>,
) -> Result<manifest::ManifestSpoolState> {
    let root_source_path = manifest_root_identity_path(source.path())?;
    let source_identity = sender_manifest_source_identity(source)?;
    let mut state = manifest::ManifestSpoolState::open_or_recover(
        &sender_manifest_state_root(),
        dump_id,
        source_identity,
        &root_source_path,
    )?;

    for event in &state.recovery_events {
        debug!(
            "sender manifest recovery event: dump_id={} event={:?} detail={}",
            dump_id, event.event, event.detail
        );
    }

    if state.meta.seal_state == manifest::ManifestSealState::Sealed {
        return Ok(state);
    }

    let build_started = Instant::now();
    let spool_max_bytes = sender_manifest_spool_max_bytes();
    let (report, last_checkpoint, fresh_log_bytes) =
        match manifest::ManifestSpoolBuilder::build_to_path_with_limit_and_checkpoint_and_cache(
            source.path().to_path_buf(),
            &state.paths.log_path,
            spool_max_bytes,
            Some(MANIFEST_CHECKPOINT_STRIDE_RECORDS),
            Some(DEFAULT_SENDER_FRESH_MANIFEST_CACHE_MAX_BYTES),
        ) {
            Ok(result) => result,
            Err(error) => {
                if state.paths.log_path.exists() {
                    fs::remove_file(&state.paths.log_path).with_context(|| {
                        format!(
                            "failed to remove partial manifest spool {}",
                            state.paths.log_path.display()
                        )
                    })?;
                }
                if state.paths.checkpoint_path.exists() {
                    fs::remove_file(&state.paths.checkpoint_path).with_context(|| {
                        format!(
                            "failed to remove stale manifest checkpoint {}",
                            state.paths.checkpoint_path.display()
                        )
                    })?;
                }
                return Err(error.context(format!(
                    "sender manifest spool build failed within quota {} bytes",
                    spool_max_bytes
                )));
            }
        };
    state.fresh_log_bytes = fresh_log_bytes;
    if let Some(profile) = profile {
        profile.record_manifest_build(report.spool_bytes_written, build_started.elapsed());
    }
    if let Some(checkpoint) = last_checkpoint {
        checkpoint.write_atomic(&state.paths.checkpoint_path)?;
        state.meta.commit_checkpoint(
            checkpoint.last_record_seq,
            checkpoint.last_work_ordinal,
            checkpoint.checkpoint_seq,
        )?;
        state.last_checkpoint = Some(checkpoint);
    } else if state.paths.checkpoint_path.exists() {
        fs::remove_file(&state.paths.checkpoint_path).with_context(|| {
            format!(
                "failed to remove stale manifest checkpoint {}",
                state.paths.checkpoint_path.display()
            )
        })?;
        state.last_checkpoint = None;
    }
    state.meta.mark_sealed();
    state.persist_meta()?;
    debug!(
        "sender manifest spool sealed: dump_id={} records={} work_items={} discovery_issues={} subtree_issues={} spool_bytes={} spool_quota_bytes={} elapsed_ms={}",
        dump_id,
        report.records_written,
        report.work_items_written,
        report.discovery_issues_written,
        report.subtree_issues_written,
        report.spool_bytes_written,
        spool_max_bytes,
        build_started.elapsed().as_millis()
    );
    Ok(state)
}

fn prepare_progressive_manifest_state(
    source: &SenderSource,
    dump_id: &str,
) -> Result<manifest::ManifestSpoolState> {
    let root_source_path = manifest_root_identity_path(source.path())?;
    let source_identity = sender_manifest_source_identity(source)?;
    let mut state = manifest::ManifestSpoolState::open_or_recover(
        &sender_manifest_state_root(),
        dump_id,
        source_identity,
        &root_source_path,
    )?;

    if state.paths.log_path.exists() {
        fs::remove_file(&state.paths.log_path).with_context(|| {
            format!(
                "failed to remove stale progressive manifest spool {}",
                state.paths.log_path.display()
            )
        })?;
    }
    if state.paths.checkpoint_path.exists() {
        fs::remove_file(&state.paths.checkpoint_path).with_context(|| {
            format!(
                "failed to remove stale progressive manifest checkpoint {}",
                state.paths.checkpoint_path.display()
            )
        })?;
    }
    state
        .meta
        .invalidate(manifest::ManifestInvalidationReason::RebuildRequested);
    state.persist_meta()?;
    state.last_checkpoint = None;
    state.recovery_events.clear();
    state.fresh_log_bytes = None;
    Ok(state)
}

impl PendingFileWalker {
    #[cfg(test)]
    pub(crate) fn new(source: impl Into<PathBuf>, resume_state: &FileResumeState) -> Result<Self> {
        Self::new_live_with_profile(source, resume_state, None)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new_with_manifest(
        source: impl Into<SenderSource>,
        dump_id: &str,
        resume_state: &FileResumeState,
        profile: Option<&PipelineProfileCollector>,
    ) -> Result<Self> {
        let source = source.into();
        let state = ensure_sealed_manifest_state(&source, dump_id, profile)?;
        Self::new_with_manifest_state(source, resume_state, state, profile)
    }

    pub(crate) fn new_with_manifest_state(
        source: impl Into<SenderSource>,
        resume_state: &FileResumeState,
        state: manifest::ManifestSpoolState,
        profile: Option<&PipelineProfileCollector>,
    ) -> Result<Self> {
        let source = source.into();
        validate_source_directory(source.path())?;
        Ok(Self {
            mode: PendingFileWalkerMode::Manifest(Box::new(
                manifest::ManifestPendingLeafWalker::new(
                    source.path().to_path_buf(),
                    resume_state,
                    state,
                    profile,
                )?,
            )),
        })
    }

    pub(crate) fn new_progressive(
        source: impl Into<SenderSource>,
        dump_id: &str,
        resume_state: &FileResumeState,
        base_manifest: &crate::network::protocol::ManifestBoundary,
        manifest_chunk_bytes: usize,
        manifest_chunk_records: u64,
        profile: Option<&PipelineProfileCollector>,
    ) -> Result<Self> {
        let source = source.into();
        validate_source_directory(source.path())?;
        let state = prepare_progressive_manifest_state(&source, dump_id)?;
        let spool_max_bytes = sender_manifest_spool_max_bytes();
        Ok(Self {
            mode: PendingFileWalkerMode::Progressive(Box::new(
                manifest::ProgressiveManifestPendingLeafWalker::new(
                    source.path().to_path_buf(),
                    resume_state,
                    state,
                    base_manifest,
                    spool_max_bytes,
                    manifest_chunk_bytes,
                    manifest_chunk_records,
                    profile,
                )?,
            )),
        })
    }

    #[cfg(test)]
    fn new_live_with_profile(
        source: impl Into<PathBuf>,
        resume_state: &FileResumeState,
        profile: Option<&PipelineProfileCollector>,
    ) -> Result<Self> {
        let source = source.into();
        validate_source_directory(&source)?;
        let classifier = EntryClassifier::new();
        let root_entries = read_sorted_entries(&source, &classifier, &source)?;
        let listing_pool = DirectoryListingPool::new(&source, classifier, profile);
        listing_pool.prefetch_directories(&root_entries);
        Ok(Self {
            mode: PendingFileWalkerMode::Live(LivePendingFileWalker {
                resume_state: resume_state.clone(),
                listing_pool,
                stack: vec![DirectoryFrame {
                    entries: root_entries,
                }],
                next_index: 1,
                last_discovered_file: None,
                resume_validated: resume_state.last_committed_file_index == 0,
                exhausted: false,
            }),
        })
    }

    // Продакшен-путь получает файлы по одному и начинает frame assembly,
    // как только пройден committed prefix. Это убирает необходимость в полном pending-list.
    pub(crate) fn next_pending_file(&mut self) -> Result<Option<InputFile>> {
        match &mut self.mode {
            #[cfg(test)]
            PendingFileWalkerMode::Live(walker) => walker.next_pending_file(),
            PendingFileWalkerMode::Manifest(walker) => walker.next_pending_file(),
            PendingFileWalkerMode::Progressive(walker) => walker.next_pending_file(),
        }
    }

    pub(crate) fn final_boundary(&self) -> Result<(u64, String)> {
        match &self.mode {
            #[cfg(test)]
            PendingFileWalkerMode::Live(walker) => walker.final_boundary(),
            PendingFileWalkerMode::Manifest(walker) => walker.final_boundary(),
            PendingFileWalkerMode::Progressive(walker) => walker.final_boundary(),
        }
    }

    pub(crate) fn last_discovered_boundary(&self) -> (u64, String) {
        match &self.mode {
            #[cfg(test)]
            PendingFileWalkerMode::Live(walker) => walker.last_discovered_boundary(),
            PendingFileWalkerMode::Manifest(walker) => walker.last_discovered_boundary(),
            PendingFileWalkerMode::Progressive(walker) => walker.last_discovered_boundary(),
        }
    }

    pub(crate) fn discovery_issues(&self) -> Vec<SourceDiscoveryIssue> {
        match &self.mode {
            #[cfg(test)]
            PendingFileWalkerMode::Live(_) => Vec::new(),
            PendingFileWalkerMode::Manifest(walker) => walker.discovery_issues().to_vec(),
            PendingFileWalkerMode::Progressive(walker) => walker.discovery_issues().to_vec(),
        }
    }

    pub(crate) fn discovery_issue_summary(&self) -> SourceDiscoveryIssueSummary {
        match &self.mode {
            #[cfg(test)]
            PendingFileWalkerMode::Live(_) => SourceDiscoveryIssueSummary::default(),
            PendingFileWalkerMode::Manifest(walker) => walker.discovery_issue_summary(),
            PendingFileWalkerMode::Progressive(walker) => walker.discovery_issue_summary(),
        }
    }

    pub(crate) fn cleanup_after_finish(&self) -> Result<()> {
        match &self.mode {
            #[cfg(test)]
            PendingFileWalkerMode::Live(_) => Ok(()),
            PendingFileWalkerMode::Manifest(walker) => walker.state.cleanup_after_finish(),
            PendingFileWalkerMode::Progressive(walker) => walker.state.cleanup_after_finish(),
        }
    }

    pub(crate) fn pop_manifest_chunk(
        &mut self,
        force_flush: bool,
    ) -> Result<Option<manifest::ManifestChunkPlan>> {
        match &mut self.mode {
            PendingFileWalkerMode::Progressive(walker) => walker.pop_manifest_chunk(force_flush),
            _ => Ok(None),
        }
    }

    pub(crate) fn produced_manifest_work_ordinal(&self) -> u64 {
        match &self.mode {
            PendingFileWalkerMode::Progressive(walker) => walker.produced_manifest_work_ordinal(),
            _ => 0,
        }
    }

    pub(crate) fn manifest_exhausted(&self) -> bool {
        match &self.mode {
            PendingFileWalkerMode::Progressive(walker) => walker.manifest_exhausted(),
            PendingFileWalkerMode::Manifest(_) => false,
            #[cfg(test)]
            PendingFileWalkerMode::Live(_) => false,
        }
    }

    pub(crate) fn produced_manifest_boundary(&self) -> (Option<u64>, Option<u64>) {
        match &self.mode {
            PendingFileWalkerMode::Progressive(walker) => walker.produced_manifest_boundary(),
            PendingFileWalkerMode::Manifest(walker) => {
                let recovery = walker.state.recover_manifest_prefix();
                match recovery {
                    Ok(recovery) => (
                        recovery.records.last().map(|record| record.record_seq.0),
                        recovery
                            .records
                            .iter()
                            .filter_map(|record| record.work_ordinal.map(|value| value.0))
                            .next_back(),
                    ),
                    Err(_) => (None, None),
                }
            }
            #[cfg(test)]
            PendingFileWalkerMode::Live(_) => (None, None),
        }
    }

    pub(crate) fn final_manifest_boundary(&self) -> Result<(Option<u64>, Option<u64>)> {
        match &self.mode {
            PendingFileWalkerMode::Progressive(walker) => walker.final_manifest_boundary(),
            PendingFileWalkerMode::Manifest(walker) => {
                let recovery = walker.state.recover_manifest_prefix()?;
                Ok((
                    recovery.records.last().map(|record| record.record_seq.0),
                    recovery
                        .records
                        .iter()
                        .filter_map(|record| record.work_ordinal.map(|value| value.0))
                        .next_back(),
                ))
            }
            #[cfg(test)]
            PendingFileWalkerMode::Live(_) => Ok((None, None)),
        }
    }
}

#[cfg(test)]
impl LivePendingFileWalker {
    fn next_pending_file(&mut self) -> Result<Option<InputFile>> {
        if self.exhausted {
            return Ok(None);
        }

        loop {
            let Some(entry) = self.next_entry()? else {
                self.finish_walk()?;
                return Ok(None);
            };

            let file = match entry.kind {
                DiscoveredEntryKind::Directory => {
                    let entries = self.listing_pool.take_or_read(&entry.abs_path)?;
                    self.listing_pool.prefetch_directories(&entries);
                    self.stack.push(DirectoryFrame { entries });
                    continue;
                }
                DiscoveredEntryKind::Regular => {
                    InputFile::regular(self.next_index, entry.abs_path, entry.rel_path)
                }
                DiscoveredEntryKind::Symlink { target } => {
                    InputFile::symlink(self.next_index, entry.abs_path, entry.rel_path, target)
                }
                DiscoveredEntryKind::Skipped(reason) => {
                    warn_skipped_entry(&entry.abs_path, &reason);
                    continue;
                }
            };
            self.next_index += 1;
            self.last_discovered_file = Some(file.clone());

            if file.index < self.resume_state.last_committed_file_index {
                continue;
            }

            if file.index == self.resume_state.last_committed_file_index {
                if file.rel_path() != self.resume_state.last_committed_file_path {
                    bail!(
                        "sender source no longer matches resume state at file index {}: expected {} got {}",
                        self.resume_state.last_committed_file_index,
                        self.resume_state.last_committed_file_path,
                        file.rel_path()
                    );
                }
                self.resume_validated = true;
                continue;
            }

            return Ok(Some(file));
        }
    }

    fn final_boundary(&self) -> Result<(u64, String)> {
        if !self.exhausted {
            bail!("sender pending file walker has not finished discovery yet");
        }
        Ok(self.current_boundary())
    }

    fn last_discovered_boundary(&self) -> (u64, String) {
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
            .map(|file| (file.index, file.rel_path_string()))
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
    resume_state: &FileResumeState,
) -> Result<SourceScanReport> {
    let (pending, final_file_index, final_file_path) =
        collect_pending_files(source, resume_state, 1, 1)?;
    // Для диагностического scan-отчёта размер можно собрать отдельно,
    // но sender не использует эту цифру как источник истины для tar.
    let pending_bytes = pending.iter().try_fold(0_u64, |total, file| {
        if !file.is_regular() {
            return Ok::<_, anyhow::Error>(total);
        }
        let size = metadata_path(file.abs_path())
            .with_context(|| {
                format!(
                    "failed to read sender file metadata for scan report {}",
                    file.abs_path().display()
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

pub(crate) fn probe_source_workload(source: &SenderSource) -> Result<SenderSourceProbe> {
    validate_source_directory(source.path())?;

    let classifier = EntryClassifier::new();
    let mut stack = vec![DirectoryFrame {
        entries: read_sorted_entries(source.path(), &classifier, source.path())?,
    }];
    let mut probe = SenderSourceProbe {
        workload_class: SenderWorkloadClass::Mixed,
        sampled_directories: 1,
        sampled_regular_files: 0,
        sampled_regular_bytes: 0,
        small_regular_files: 0,
        large_regular_files: 0,
        max_regular_file_bytes: 0,
    };

    while probe.sampled_regular_files < SOURCE_PROBE_MAX_REGULAR_FILES {
        let Some(frame) = stack.last_mut() else {
            break;
        };
        let Some(entry) = frame.entries.pop() else {
            stack.pop();
            continue;
        };

        match entry.kind {
            DiscoveredEntryKind::Directory => {
                if probe.sampled_directories >= SOURCE_PROBE_MAX_DIRECTORIES {
                    continue;
                }

                let entries = read_sorted_entries(source.path(), &classifier, &entry.abs_path)?;
                stack.push(DirectoryFrame { entries });
                probe.sampled_directories += 1;
            }
            DiscoveredEntryKind::Regular => {
                record_probe_regular_file(&mut probe, &entry.abs_path);
            }
            DiscoveredEntryKind::Symlink { .. } | DiscoveredEntryKind::Skipped(_) => {}
        }
    }

    probe.workload_class = classify_probe_workload(&probe);
    Ok(probe)
}

fn record_probe_regular_file(probe: &mut SenderSourceProbe, path: &Path) {
    let metadata = match metadata_path(path) {
        Ok(metadata) => metadata,
        Err(error) => {
            debug!(
                "sender adaptive probe skipped unreadable metadata {}: {}",
                path.display(),
                error
            );
            return;
        }
    };
    let file_bytes = metadata.len();

    probe.sampled_regular_files += 1;
    probe.sampled_regular_bytes = probe.sampled_regular_bytes.saturating_add(file_bytes);
    probe.max_regular_file_bytes = probe.max_regular_file_bytes.max(file_bytes);
    if file_bytes <= SOURCE_PROBE_SMALL_FILE_BYTES {
        probe.small_regular_files += 1;
    }
    if file_bytes >= SOURCE_PROBE_LARGE_FILE_BYTES {
        probe.large_regular_files += 1;
    }
}

fn classify_probe_workload(probe: &SenderSourceProbe) -> SenderWorkloadClass {
    if probe.sampled_regular_files == 0 {
        return SenderWorkloadClass::TinyHeavy;
    }

    let average_file_bytes = probe.average_regular_file_bytes();
    let sampled_regular_files = probe.sampled_regular_files;
    if average_file_bytes >= SOURCE_PROBE_LARGE_AVG_FILE_BYTES
        && (sampled_regular_files <= 16
            || probe.large_regular_files.saturating_mul(2) >= sampled_regular_files)
    {
        return SenderWorkloadClass::LargeHeavy;
    }
    if probe.max_regular_file_bytes >= SOURCE_PROBE_LARGE_FILE_BYTES.saturating_mul(4)
        && sampled_regular_files <= 8
    {
        return SenderWorkloadClass::LargeHeavy;
    }
    if average_file_bytes <= SOURCE_PROBE_SMALL_FILE_BYTES
        && probe.small_regular_files.saturating_mul(3) >= sampled_regular_files.saturating_mul(2)
    {
        return SenderWorkloadClass::TinyHeavy;
    }

    SenderWorkloadClass::Mixed
}

fn warn_skipped_entry(path: &Path, reason: &SkippedEntryReason) {
    match reason {
        SkippedEntryReason::Unsupported(kind) => {
            eprintln!(
                "warning: skipping unsupported {kind} entry {}",
                path.display()
            );
        }
        SkippedEntryReason::UnreadableSymlinkTarget { error } => {
            eprintln!(
                "warning: skipping symlink entry {} because failed to read link target: {}",
                path.display(),
                error
            );
        }
        SkippedEntryReason::VirtualFilesystem { kind } => {
            eprintln!(
                "warning: skipping virtual {kind} entry {} because pseudo-filesystem contents are not snapshot-safe",
                path.display()
            );
        }
    }
}

fn classify_discovered_entry(
    _classifier: &EntryClassifier,
    path: &Path,
    file_type: fs::FileType,
) -> Result<DiscoveredEntryKind> {
    #[cfg(target_os = "linux")]
    if let Some(kind) = _classifier.linux_mounts.virtual_kind_for_path(path) {
        return Ok(DiscoveredEntryKind::Skipped(
            SkippedEntryReason::VirtualFilesystem { kind },
        ));
    }

    if file_type.is_dir() {
        return Ok(DiscoveredEntryKind::Directory);
    }
    if file_type.is_file() {
        return Ok(DiscoveredEntryKind::Regular);
    }
    if file_type.is_symlink() {
        return Ok(match read_link_path(path) {
            Ok(target) => DiscoveredEntryKind::Symlink { target },
            Err(error) => {
                DiscoveredEntryKind::Skipped(SkippedEntryReason::UnreadableSymlinkTarget {
                    error: error.to_string(),
                })
            }
        });
    }

    Ok(DiscoveredEntryKind::Skipped(
        SkippedEntryReason::Unsupported(classify_unsupported_entry_kind(&file_type)),
    ))
}

#[cfg(unix)]
fn classify_unsupported_entry_kind(file_type: &fs::FileType) -> UnsupportedEntryKind {
    use std::os::unix::fs::FileTypeExt;

    if file_type.is_socket() {
        UnsupportedEntryKind::Socket
    } else if file_type.is_fifo() {
        UnsupportedEntryKind::Fifo
    } else if file_type.is_block_device() {
        UnsupportedEntryKind::BlockDevice
    } else if file_type.is_char_device() {
        UnsupportedEntryKind::CharDevice
    } else {
        UnsupportedEntryKind::Other
    }
}

#[cfg(not(unix))]
fn classify_unsupported_entry_kind(_file_type: &fs::FileType) -> UnsupportedEntryKind {
    UnsupportedEntryKind::Other
}

fn validate_resume_state_against_files(
    files: &[InputFile],
    resume_state: &FileResumeState,
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

    if committed_file.rel_path() != resume_state.last_committed_file_path {
        bail!(
            "sender source no longer matches resume state at file index {}: expected {} got {}",
            resume_state.last_committed_file_index,
            resume_state.last_committed_file_path,
            committed_file.rel_path()
        );
    }

    Ok(())
}

fn read_sorted_entries(
    source: &Path,
    classifier: &EntryClassifier,
    dir: &Path,
) -> Result<Vec<DiscoveredEntry>> {
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
        let kind = classify_discovered_entry(classifier, &path, file_type)?;
        entries.push(DiscoveredEntry {
            rel_path,
            abs_path: path,
            kind,
        });
    }
    entries.sort_by(|left, right| left.rel_path.cmp(&right.rel_path));
    entries.reverse();
    Ok(entries)
}

fn discover_files(source: &Path, classifier: &EntryClassifier) -> Result<Vec<InputFile>> {
    let mut files = Vec::new();
    let mut next_index = 1_u64;
    collect_files_recursive(source, classifier, source, &mut next_index, &mut files)?;
    Ok(files)
}

fn collect_files_recursive(
    source: &Path,
    classifier: &EntryClassifier,
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
        let kind = classify_discovered_entry(classifier, &path, file_type)?;
        entries.push((rel_path, path, kind));
    }
    entries.sort_by(|left, right| left.0.cmp(&right.0));

    for (rel_path, path, kind) in entries {
        match kind {
            DiscoveredEntryKind::Directory => {
                collect_files_recursive(source, classifier, &path, next_index, files)?;
            }
            DiscoveredEntryKind::Regular => {
                files.push(InputFile::regular(*next_index, path, rel_path));
                *next_index += 1;
            }
            DiscoveredEntryKind::Symlink { target } => {
                files.push(InputFile::symlink(*next_index, path, rel_path, target));
                *next_index += 1;
            }
            DiscoveredEntryKind::Skipped(reason) => {
                warn_skipped_entry(&path, &reason);
            }
        }
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
    #[cfg(unix)]
    use std::ffi::OsString;
    use std::fs;
    #[cfg(unix)]
    use std::io::Read;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        InputFile, PendingFileWalker, PipelineProfileCollector, SenderWorkloadClass,
        collect_pending_files, discover_files, normalize_relative_path, probe_source_workload,
        scan_source,
    };
    #[cfg(target_os = "linux")]
    use super::{LinuxMountTable, parse_linux_mountinfo_path};
    use crate::sender::FileResumeState;

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
        InputFile::regular(index, path, rel_path.replace('\\', "/"))
    }

    fn empty_resume_state() -> FileResumeState {
        FileResumeState {
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

    #[cfg(unix)]
    struct AbsolutePathExecutorEnvGuard {
        _lock: MutexGuard<'static, ()>,
        previous: Option<OsString>,
    }

    #[cfg(unix)]
    impl AbsolutePathExecutorEnvGuard {
        fn set(value: &str) -> Self {
            static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
            let lock = LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
            let previous = std::env::var_os(super::FORCE_ABSOLUTE_PATH_EXECUTOR_ENV);
            unsafe {
                std::env::set_var(super::FORCE_ABSOLUTE_PATH_EXECUTOR_ENV, value);
            }
            Self {
                _lock: lock,
                previous,
            }
        }
    }

    #[cfg(unix)]
    impl Drop for AbsolutePathExecutorEnvGuard {
        fn drop(&mut self) {
            unsafe {
                if let Some(previous) = &self.previous {
                    std::env::set_var(super::FORCE_ABSOLUTE_PATH_EXECUTOR_ENV, previous);
                } else {
                    std::env::remove_var(super::FORCE_ABSOLUTE_PATH_EXECUTOR_ENV);
                }
            }
        }
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

        let rel_paths: Vec<_> = pending.iter().map(|file| file.rel_path()).collect();
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

        let rel_paths: Vec<_> = pending.iter().map(|file| file.rel_path()).collect();
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

        let resume_state = FileResumeState {
            last_committed_frame: 1,
            last_committed_file_index: 1,
            last_committed_file_path: "dir/one.txt".to_owned(),
            archive_len: 123,
        };

        let (pending, final_file_index, final_file_path) =
            collect_pending_files(temp.path().to_path_buf(), &resume_state, 1, 1024).unwrap();

        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].index, 2);
        assert_eq!(pending[0].rel_path(), "dir/two.txt");
        assert_eq!(final_file_index, 2);
        assert_eq!(final_file_path, "dir/two.txt");
    }

    #[test]
    fn collect_pending_files_returns_no_pending_when_resume_already_covers_all_files() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("dir")).unwrap();
        fs::write(temp.path().join("dir").join("one.txt"), b"111").unwrap();
        fs::write(temp.path().join("dir").join("two.txt"), b"222").unwrap();
        let resume_state = FileResumeState {
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

        let resume_state = FileResumeState {
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

        let resume_state = FileResumeState {
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
    fn collect_pending_files_preserves_symlink_entries_when_platform_allows_it() {
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

        let rel_paths: Vec<_> = pending.iter().map(|file| file.rel_path()).collect();
        assert_eq!(rel_paths, vec!["real.txt", "skip-me.txt"]);
        assert!(pending[0].is_regular());
        assert_eq!(pending[1].link_target(), Some(target.as_path()));
        assert_eq!(final_file_index, 2);
        assert_eq!(final_file_path, "skip-me.txt");
    }

    #[test]
    fn collect_pending_files_skips_symlink_when_link_target_cannot_be_read() {
        let _guard = FsFailureGuard::new();
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
        super::test_hooks::fail_next_read_link(&symlink);

        let (pending, final_file_index, final_file_path) =
            collect_pending_files(temp.path().to_path_buf(), &empty_resume_state(), 1, 1).unwrap();

        let rel_paths: Vec<_> = pending.iter().map(|file| file.rel_path()).collect();
        assert_eq!(rel_paths, vec!["real.txt"]);
        assert_eq!(final_file_index, 1);
        assert_eq!(final_file_path, "real.txt");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_mount_table_parses_mountinfo_and_uses_most_specific_mountpoint() {
        let mountinfo = "\
24 23 0:22 / / rw,relatime - ext4 /dev/root rw\n\
25 24 0:23 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw\n\
26 24 0:24 / /sys ro,nosuid,nodev,noexec,relatime - sysfs sysfs ro\n\
27 24 0:25 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw\n\
28 24 0:26 / /tmp/with\\040space rw,relatime - tmpfs tmpfs rw\n";
        let table = LinuxMountTable::parse(mountinfo).unwrap();

        assert_eq!(
            table.virtual_kind_for_path(Path::new("/proc/kcore")),
            Some("procfs")
        );
        assert_eq!(
            table.virtual_kind_for_path(Path::new("/sys/kernel/debug")),
            Some("sysfs")
        );
        assert_eq!(
            table.virtual_kind_for_path(Path::new("/sys/fs/cgroup/memory.current")),
            Some("cgroup2")
        );
        assert_eq!(
            table.virtual_kind_for_path(Path::new("/tmp/with space/file.txt")),
            None
        );
        assert_eq!(
            table.virtual_kind_for_path(Path::new("/var/lib/data.bin")),
            None
        );
        assert_eq!(
            parse_linux_mountinfo_path("/tmp/with\\040space").unwrap(),
            PathBuf::from("/tmp/with space")
        );
    }

    #[test]
    fn discover_files_reports_directory_read_error() {
        let _guard = FsFailureGuard::new();
        let temp = TestDir::new();
        super::test_hooks::fail_next_read_dir(temp.path());

        let error = discover_files(temp.path(), &super::EntryClassifier::new()).unwrap_err();

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
        super::test_hooks::fail_next_metadata(file.abs_path());

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
    fn input_file_from_prefix_materializes_rel_and_abs_paths_on_demand() {
        let temp = TestDir::new();
        let root_prefix = super::InputPathPrefix::root(temp.path().to_path_buf());
        let nested_prefix = super::InputPathPrefix::child(&root_prefix, "nested");
        let file = InputFile::regular_from_prefix(
            7,
            nested_prefix,
            "leaf.txt".to_owned().into_boxed_str(),
        );

        assert_eq!(file.index, 7);
        assert_eq!(file.rel_path(), "nested/leaf.txt");
        assert_eq!(file.abs_path(), temp.path().join("nested").join("leaf.txt"));
        assert_eq!(file.clone().rel_path(), "nested/leaf.txt");
    }

    #[test]
    fn collect_source_discovery_issues_counts_rebuild_events_as_manifest_resets() {
        let temp = TestDir::new();
        let root = fs::canonicalize(temp.path())
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let state = super::manifest::ManifestSpoolState {
            paths: super::manifest::ManifestSpoolPaths::for_dump(
                &temp.path().join("state-root"),
                "dump-reset",
            ),
            meta: super::manifest::ManifestMeta::new(
                "dump-reset",
                super::manifest::ManifestSourceIdentity::new(
                    root.clone(),
                    root.clone(),
                    "manifest-builder-v3-live-v1",
                    super::manifest::ManifestSourceKind::LivePath,
                )
                .unwrap(),
                root.clone(),
            )
            .unwrap(),
            last_checkpoint: None,
            recovery_events: vec![super::manifest::ManifestRecoveryEvent {
                event: super::manifest::ManifestSenderStateEvent::Rebuilt,
                detail: "recovered from corrupted spool".into(),
            }],
            fresh_log_bytes: None,
        };

        let issues = super::collect_source_discovery_issues(&state).unwrap();
        let summary = super::summarize_source_discovery_issues(&issues);

        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].record_seq, None);
        assert_eq!(issues[0].path, root);
        assert_eq!(
            issues[0].scope,
            super::SourceDiscoveryIssueScope::SenderState
        );
        match &issues[0].reason {
            super::SourceDiscoveryIssueReason::SenderState { event, detail } => {
                assert_eq!(*event, super::manifest::ManifestSenderStateEvent::Rebuilt);
                assert!(detail.contains("corrupted spool"));
            }
            other => panic!("unexpected discovery issue reason: {other:?}"),
        }
        assert_eq!(summary.discovery_issues_total, 1);
        assert_eq!(summary.subtrees_skipped, 0);
        assert_eq!(summary.manifest_resets_total, 1);
    }

    #[test]
    fn sender_source_backend_kind_maps_into_manifest_source_identity() {
        let temp = TestDir::new();
        let ordinary = super::SenderSource::ordinary_path(temp.path().to_path_buf());
        let snapshot = super::SenderSource::snapshot_path(temp.path().to_path_buf());
        let pinned = super::SenderSource::pinned_path(temp.path().to_path_buf());

        assert_eq!(
            super::sender_manifest_source_identity(&ordinary)
                .unwrap()
                .source_kind,
            super::manifest::ManifestSourceKind::LivePath
        );
        assert_eq!(
            super::sender_manifest_source_identity(&snapshot)
                .unwrap()
                .source_kind,
            super::manifest::ManifestSourceKind::Snapshot
        );
        assert_eq!(
            super::sender_manifest_source_identity(&pinned)
                .unwrap()
                .source_kind,
            super::manifest::ManifestSourceKind::Pinned
        );
    }

    #[cfg(unix)]
    #[test]
    fn input_file_from_prefix_opens_nested_file_via_dirfd_executor() {
        let temp = TestDir::new();
        let nested = temp.path().join("nested");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join("leaf.txt"), b"payload").unwrap();
        let root_prefix = super::InputPathPrefix::root(temp.path().to_path_buf());
        let nested_prefix = super::InputPathPrefix::child(&root_prefix, "nested");
        let file = InputFile::regular_from_prefix(
            7,
            nested_prefix,
            "leaf.txt".to_owned().into_boxed_str(),
        );

        let mut handle = file.open_for_read().unwrap();
        let mut bytes = Vec::new();
        handle.read_to_end(&mut bytes).unwrap();

        assert_eq!(bytes, b"payload");
        assert_eq!(file.dirfd_depth(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn input_file_from_prefix_can_force_absolute_path_executor() {
        let _guard = AbsolutePathExecutorEnvGuard::set("1");
        let temp = TestDir::new();
        let nested = temp.path().join("nested");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join("leaf.txt"), b"payload").unwrap();
        let root_prefix = super::InputPathPrefix::root(temp.path().to_path_buf());
        let nested_prefix = super::InputPathPrefix::child(&root_prefix, "nested");
        let file = InputFile::regular_from_prefix(
            9,
            nested_prefix,
            "leaf.txt".to_owned().into_boxed_str(),
        );

        let mut handle = file.open_for_read().unwrap();
        let mut bytes = Vec::new();
        handle.read_to_end(&mut bytes).unwrap();

        assert_eq!(bytes, b"payload");
        assert_eq!(file.dirfd_depth(), 0);
    }

    #[test]
    fn pending_file_walker_returns_no_pending_for_fully_resumed_dump() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("dir")).unwrap();
        fs::write(temp.path().join("dir").join("one.txt"), b"111").unwrap();
        fs::write(temp.path().join("dir").join("two.txt"), b"222").unwrap();
        let resume_state = FileResumeState {
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
        let resume_state = FileResumeState {
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

    #[test]
    fn pending_file_walker_matches_discover_files_order_on_branched_tree() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("a").join("deep")).unwrap();
        fs::create_dir_all(temp.path().join("b").join("nested")).unwrap();
        fs::create_dir_all(temp.path().join("c")).unwrap();
        fs::write(temp.path().join("root.txt"), b"root").unwrap();
        fs::write(temp.path().join("a").join("alpha.txt"), b"alpha").unwrap();
        fs::write(temp.path().join("a").join("deep").join("beta.txt"), b"beta").unwrap();
        fs::write(
            temp.path().join("b").join("nested").join("gamma.txt"),
            b"gamma",
        )
        .unwrap();
        fs::write(temp.path().join("c").join("delta.txt"), b"delta").unwrap();

        let expected = discover_files(temp.path(), &super::EntryClassifier::new()).unwrap();
        let mut walker =
            PendingFileWalker::new(temp.path().to_path_buf(), &empty_resume_state()).unwrap();
        let mut actual = Vec::new();
        while let Some(file) = walker.next_pending_file().unwrap() {
            actual.push(file);
        }

        assert_eq!(actual, expected);
    }

    #[test]
    fn pending_file_walker_replays_manifest_without_live_read_dir_after_seal() {
        let _guard = FsFailureGuard::new();
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("zzz")).unwrap();
        fs::write(temp.path().join("aaa.txt"), b"111").unwrap();
        fs::write(temp.path().join("zzz").join("later.txt"), b"222").unwrap();
        let dump_id = format!(
            "manifest-runtime-{}",
            temp.path().file_name().unwrap().to_string_lossy()
        );
        let state_paths = super::manifest::ManifestSpoolPaths::for_dump(
            &super::sender_manifest_state_root(),
            &dump_id,
        );

        let walker = PendingFileWalker::new_with_manifest(
            temp.path().to_path_buf(),
            &dump_id,
            &empty_resume_state(),
            None,
        )
        .unwrap();
        drop(walker);
        assert!(state_paths.state_dir.exists());

        super::test_hooks::fail_next_read_dir(&temp.path().join("zzz"));
        let resume_state = FileResumeState {
            last_committed_frame: 1,
            last_committed_file_index: 1,
            last_committed_file_path: "aaa.txt".to_owned(),
            archive_len: 64,
        };
        let mut walker = PendingFileWalker::new_with_manifest(
            temp.path().to_path_buf(),
            &dump_id,
            &resume_state,
            None,
        )
        .unwrap();

        let file = walker.next_pending_file().unwrap().unwrap();
        assert_eq!(file.index, 2);
        assert_eq!(file.rel_path(), "zzz/later.txt");
        assert!(walker.next_pending_file().unwrap().is_none());
        assert_eq!(
            walker.final_boundary().unwrap(),
            (2, "zzz/later.txt".to_owned())
        );

        walker.cleanup_after_finish().unwrap();
        assert!(!state_paths.state_dir.exists());
    }

    #[test]
    fn ensure_sealed_manifest_state_keeps_small_fresh_spool_in_memory() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("nested")).unwrap();
        fs::write(temp.path().join("alpha.txt"), b"111").unwrap();
        fs::write(temp.path().join("nested").join("beta.txt"), b"222").unwrap();
        let dump_id = format!(
            "manifest-fresh-cache-{}",
            temp.path().file_name().unwrap().to_string_lossy()
        );

        let state = super::ensure_sealed_manifest_state(
            &super::SenderSource::ordinary_path(temp.path().to_path_buf()),
            &dump_id,
            None,
        )
        .unwrap();
        let cached_len = state
            .fresh_log_bytes
            .as_ref()
            .map(|bytes| bytes.len())
            .unwrap();
        fs::remove_file(&state.paths.log_path).unwrap();

        let recovery = state.recover_manifest_prefix().unwrap();

        assert_eq!(recovery.truncate_to_len, cached_len as u64);
        assert_eq!(recovery.records.len(), 4);

        state.cleanup_after_finish().unwrap();
    }

    #[test]
    fn pending_file_walker_fails_loudly_when_manifest_spool_quota_is_exhausted() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("nested")).unwrap();
        fs::write(temp.path().join("alpha.txt"), b"111").unwrap();
        fs::write(temp.path().join("nested").join("beta.txt"), b"222").unwrap();
        let dump_id = format!(
            "manifest-quota-{}",
            temp.path().file_name().unwrap().to_string_lossy()
        );
        let state_paths = super::manifest::ManifestSpoolPaths::for_dump(
            &super::sender_manifest_state_root(),
            &dump_id,
        );
        let _ = fs::remove_dir_all(&state_paths.state_dir);

        super::test_hooks::set_manifest_spool_max_bytes_override(Some(1));
        assert_eq!(super::sender_manifest_spool_max_bytes(), 1);
        let error = PendingFileWalker::new_with_manifest(
            temp.path().to_path_buf(),
            &dump_id,
            &empty_resume_state(),
            None,
        )
        .unwrap_err();
        super::test_hooks::clear();

        let message = error.to_string();
        assert!(message.contains("sender manifest spool build failed within quota 1 bytes"));
        assert!(!state_paths.log_path.exists());
        assert!(!state_paths.checkpoint_path.exists());
    }

    #[test]
    fn pending_file_walker_records_manifest_build_and_replay_profile_metrics() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("nested")).unwrap();
        fs::write(temp.path().join("alpha.txt"), b"111").unwrap();
        fs::write(temp.path().join("nested").join("beta.txt"), b"222").unwrap();
        fs::write(temp.path().join("nested").join("gamma.txt"), b"333").unwrap();
        let dump_id = format!(
            "manifest-profile-{}",
            temp.path().file_name().unwrap().to_string_lossy()
        );

        let build_profile = PipelineProfileCollector::default();
        let mut build_walker = PendingFileWalker::new_with_manifest(
            temp.path().to_path_buf(),
            &dump_id,
            &empty_resume_state(),
            Some(&build_profile),
        )
        .unwrap();
        while build_walker.next_pending_file().unwrap().is_some() {}
        let build_metrics = build_profile.finish();
        assert!(build_metrics.manifest_build.bytes > 0);
        assert_eq!(build_metrics.manifest_spool_read.bytes, 0);
        assert_eq!(build_metrics.manifest_replay.bytes, 0);
        drop(build_walker);

        let replay_profile = PipelineProfileCollector::default();
        let resume_state = FileResumeState {
            last_committed_frame: 1,
            last_committed_file_index: 1,
            last_committed_file_path: "alpha.txt".to_owned(),
            archive_len: 64,
        };
        let mut replay_walker = PendingFileWalker::new_with_manifest(
            temp.path().to_path_buf(),
            &dump_id,
            &resume_state,
            Some(&replay_profile),
        )
        .unwrap();
        let replayed = replay_walker.next_pending_file().unwrap().unwrap();
        assert_eq!(replayed.index, 2);
        assert_eq!(replayed.rel_path(), "nested/beta.txt");
        let replay_metrics = replay_profile.finish();
        assert_eq!(replay_metrics.manifest_build.bytes, 0);
        assert!(replay_metrics.manifest_spool_read.bytes > 0);
        assert!(replay_metrics.manifest_replay.bytes > 0);
        assert!(replay_metrics.manifest_replay_records > 0);

        replay_walker.cleanup_after_finish().unwrap();
    }

    #[test]
    fn probe_source_workload_classifies_tiny_heavy_tree() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("nested")).unwrap();
        for index in 0..24 {
            fs::write(
                temp.path()
                    .join("nested")
                    .join(format!("file-{index:02}.txt")),
                b"tiny payload",
            )
            .unwrap();
        }

        let probe =
            probe_source_workload(&super::SenderSource::ordinary_path(temp.path())).unwrap();

        assert_eq!(probe.workload_class, SenderWorkloadClass::TinyHeavy);
        assert!(probe.sampled_regular_files >= 24);
        assert_eq!(probe.large_regular_files, 0);
    }

    #[test]
    fn probe_source_workload_classifies_large_heavy_tree() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("nested")).unwrap();
        for index in 0..4 {
            let path = temp
                .path()
                .join("nested")
                .join(format!("blob-{index:02}.bin"));
            let file = fs::File::create(path).unwrap();
            file.set_len(64 * 1024 * 1024).unwrap();
        }

        let probe =
            probe_source_workload(&super::SenderSource::ordinary_path(temp.path())).unwrap();

        assert_eq!(probe.workload_class, SenderWorkloadClass::LargeHeavy);
        assert_eq!(probe.sampled_regular_files, 4);
        assert_eq!(probe.large_regular_files, 4);
        assert_eq!(probe.max_regular_file_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn probe_source_workload_classifies_mixed_tree() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("deep")).unwrap();
        for (name, size) in [
            ("alpha.bin", 256 * 1024_u64),
            ("beta.bin", 1024 * 1024_u64),
            ("deep/gamma.bin", 4 * 1024 * 1024_u64),
        ] {
            let path = temp.path().join(name);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            let file = fs::File::create(path).unwrap();
            file.set_len(size).unwrap();
        }

        let probe =
            probe_source_workload(&super::SenderSource::ordinary_path(temp.path())).unwrap();

        assert_eq!(probe.workload_class, SenderWorkloadClass::Mixed);
        assert_eq!(probe.large_regular_files, 0);
        assert_eq!(probe.small_regular_files, 0);
    }
}

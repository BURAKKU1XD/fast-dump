use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use log::debug;

use crate::network::protocol::{self, Message};

pub(crate) const DEFAULT_RECEIVER_MANIFEST_MAX_BYTES: u64 = 512 * 1024 * 1024;
pub(crate) const DEFAULT_RECEIVER_ISSUE_TEMP_MAX_BYTES: u64 = 64 * 1024 * 1024;
const TEST_RECEIVER_MANIFEST_MAX_BYTES_ENV: &str = "FAST_DUMP_TEST_RECEIVER_MANIFEST_MAX_BYTES";
const TEST_RECEIVER_ISSUE_TEMP_MAX_BYTES_ENV: &str = "FAST_DUMP_TEST_RECEIVER_ISSUE_TEMP_MAX_BYTES";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ReceiverTempFileKind {
    Manifest,
    IssueJournalTemp,
}

impl ReceiverTempFileKind {
    fn display_name(self) -> &'static str {
        match self {
            Self::Manifest => "receiver manifest store",
            Self::IssueJournalTemp => "receiver typed issue journal temp file",
        }
    }

    fn protocol_name(self) -> &'static str {
        match self {
            Self::Manifest => "receiver manifest storage",
            Self::IssueJournalTemp => "receiver issue-tail temp storage",
        }
    }

    fn default_quota_bytes(self) -> u64 {
        match self {
            Self::Manifest => DEFAULT_RECEIVER_MANIFEST_MAX_BYTES,
            Self::IssueJournalTemp => DEFAULT_RECEIVER_ISSUE_TEMP_MAX_BYTES,
        }
    }
}

#[derive(Debug)]
pub(crate) enum ReceiverTempStorageError {
    QuotaExceeded {
        kind: ReceiverTempFileKind,
        path: PathBuf,
        limit_bytes: u64,
        attempted_bytes: u64,
    },
    StorageExhausted {
        kind: ReceiverTempFileKind,
        path: PathBuf,
        operation: &'static str,
        source: std::io::Error,
    },
}

impl ReceiverTempStorageError {
    fn protocol_message(&self) -> String {
        match self {
            Self::QuotaExceeded {
                kind,
                limit_bytes,
                attempted_bytes,
                ..
            } => format!(
                "{} quota exhausted: attempted {} bytes with limit {} bytes; committed boundaries preserved",
                kind.protocol_name(),
                attempted_bytes,
                limit_bytes
            ),
            Self::StorageExhausted {
                kind, operation, ..
            } => format!(
                "{} exhausted during {}; committed boundaries preserved",
                kind.protocol_name(),
                operation
            ),
        }
    }
}

impl fmt::Display for ReceiverTempStorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QuotaExceeded {
                kind,
                path,
                limit_bytes,
                attempted_bytes,
            } => write!(
                f,
                "{} quota exceeded for {}: attempted {} bytes with limit {} bytes",
                kind.display_name(),
                path.display(),
                attempted_bytes,
                limit_bytes
            ),
            Self::StorageExhausted {
                kind,
                path,
                operation,
                source,
            } => write!(
                f,
                "{} exhausted during {} {}: {}",
                kind.display_name(),
                operation,
                path.display(),
                source
            ),
        }
    }
}

impl std::error::Error for ReceiverTempStorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::QuotaExceeded { .. } => None,
            Self::StorageExhausted { source, .. } => Some(source),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ReceiverTempFileAppender {
    path: PathBuf,
    kind: ReceiverTempFileKind,
    file: File,
    current_len: u64,
}

impl ReceiverTempFileAppender {
    pub(crate) fn open(path: &Path, kind: ReceiverTempFileKind) -> Result<Self> {
        let current_len = receiver_temp_file_len(path)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| receiver_temp_storage_io_error(kind, path, "open", error))?;
        Ok(Self {
            path: path.to_path_buf(),
            kind,
            file,
            current_len,
        })
    }

    pub(crate) fn current_len(&self) -> u64 {
        self.current_len
    }

    pub(crate) fn append(&mut self, payload: &[u8]) -> Result<u64> {
        if payload.is_empty() {
            return Ok(self.current_len);
        }

        let next_len = self
            .current_len
            .checked_add(payload.len() as u64)
            .context("receiver temp file length overflow")?;
        let quota_bytes = receiver_temp_file_quota_bytes(self.kind);
        if next_len > quota_bytes {
            return Err(anyhow!(ReceiverTempStorageError::QuotaExceeded {
                kind: self.kind,
                path: self.path.clone(),
                limit_bytes: quota_bytes,
                attempted_bytes: next_len,
            }));
        }

        test_hooks::before_append(&self.path, self.kind, self.current_len, payload.len())?;
        if let Err(error) = self
            .file
            .write_all(payload)
            .map_err(|error| receiver_temp_storage_io_error(self.kind, &self.path, "append", error))
        {
            return Err(rollback_after_append_error(
                &self.path,
                self.kind,
                self.current_len,
                error,
            ));
        }
        self.current_len = next_len;
        Ok(self.current_len)
    }

    pub(crate) fn sync_after_append(&mut self, previous_len: u64) -> Result<()> {
        if previous_len > self.current_len {
            bail!(
                "receiver temp sync base {} exceeds current length {}",
                previous_len,
                self.current_len
            );
        }
        let appended_len = usize::try_from(self.current_len.saturating_sub(previous_len))
            .context("receiver temp sync delta does not fit into usize")?;
        if let Err(error) =
            test_hooks::after_append_before_sync(&self.path, self.kind, previous_len, appended_len)
        {
            return Err(receiver_temp_storage_io_error(
                self.kind, &self.path, "sync", error,
            ));
        }
        self.file
            .sync_all()
            .map_err(|error| receiver_temp_storage_io_error(self.kind, &self.path, "sync", error))
    }

    pub(crate) fn rollback_to(&mut self, target_len: u64) -> Result<()> {
        if target_len > self.current_len {
            bail!(
                "receiver temp rollback target {} exceeds current length {}",
                target_len,
                self.current_len
            );
        }
        if self.current_len == target_len {
            return Ok(());
        }

        let rollback_file = OpenOptions::new()
            .write(true)
            .open(&self.path)
            .with_context(|| {
                format!(
                    "failed to reopen {} {} for rollback to {} bytes",
                    self.kind.display_name(),
                    self.path.display(),
                    target_len
                )
            })?;
        rollback_file.set_len(target_len).with_context(|| {
            format!(
                "failed to roll back {} {} to {} bytes",
                self.kind.display_name(),
                self.path.display(),
                target_len
            )
        })?;
        rollback_file.sync_all().with_context(|| {
            format!(
                "failed to sync rolled back {} {}",
                self.kind.display_name(),
                self.path.display()
            )
        })?;
        self.current_len = target_len;
        Ok(())
    }
}

fn receiver_temp_file_len(path: &Path) -> Result<u64> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(anyhow!(error))
            .with_context(|| format!("failed to inspect receiver temp file {}", path.display())),
    }
}

pub(crate) fn rollback_appender_to(
    appender: &mut ReceiverTempFileAppender,
    target_len: u64,
    error: anyhow::Error,
) -> anyhow::Error {
    match appender.rollback_to(target_len) {
        Ok(()) => error,
        Err(rollback_error) => error.context(format!(
            "failed to roll back {} {} to {} bytes after error: {}",
            appender.kind.display_name(),
            appender.path.display(),
            target_len,
            rollback_error
        )),
    }
}

pub(crate) fn rename_path(from: &Path, to: &Path) -> std::io::Result<()> {
    test_hooks::before_rename(from, to)?;
    fs::rename(from, to)
}

pub(crate) fn remove_file_path(path: &Path) -> std::io::Result<()> {
    test_hooks::before_remove_file(path)?;
    fs::remove_file(path)
}

pub(crate) fn remove_file_if_exists(path: &Path) -> Result<()> {
    if path.exists() {
        remove_file_path(path).with_context(|| format!("failed to remove {}", path.display()))?;
    }
    Ok(())
}

pub(crate) fn bytes_per_sec(bytes: u64, elapsed: Duration) -> u64 {
    let nanos = elapsed.as_nanos();
    if nanos == 0 {
        return bytes;
    }

    ((bytes as u128) * 1_000_000_000 / nanos) as u64
}

pub(crate) fn send_protocol_error<T>(stream: &mut T, message: &str) -> Result<()>
where
    T: Write,
{
    protocol::write_message(
        stream,
        &Message::Error(protocol::ErrorMessage {
            message: message.to_owned(),
        }),
    )?;
    stream
        .flush()
        .context("failed to flush protocol error to peer")
}

pub(crate) fn receiver_storage_exhausted_protocol_message(error: &anyhow::Error) -> Option<String> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<ReceiverTempStorageError>()
            .map(ReceiverTempStorageError::protocol_message)
    })
}

pub(crate) fn append_temp_file_payload(
    path: &Path,
    payload: &[u8],
    kind: ReceiverTempFileKind,
) -> Result<u64> {
    let mut appender = ReceiverTempFileAppender::open(path, kind)?;
    let previous_len = appender.current_len();
    let next_len = appender.append(payload)?;
    if let Err(error) = appender.sync_after_append(previous_len) {
        return Err(rollback_appender_to(&mut appender, previous_len, error));
    }
    Ok(next_len)
}

fn receiver_temp_file_quota_bytes(kind: ReceiverTempFileKind) -> u64 {
    #[cfg(test)]
    if let Some(quota) = test_hooks::quota_override(kind) {
        return quota;
    }

    env_override_receiver_temp_file_quota_bytes(kind).unwrap_or_else(|| kind.default_quota_bytes())
}

fn env_override_receiver_temp_file_quota_bytes(kind: ReceiverTempFileKind) -> Option<u64> {
    let env_name = match kind {
        ReceiverTempFileKind::Manifest => TEST_RECEIVER_MANIFEST_MAX_BYTES_ENV,
        ReceiverTempFileKind::IssueJournalTemp => TEST_RECEIVER_ISSUE_TEMP_MAX_BYTES_ENV,
    };
    let raw = std::env::var(env_name).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    match trimmed.parse::<u64>() {
        Ok(value) => Some(value),
        Err(error) => {
            debug!("ignoring invalid {}={:?}: {}", env_name, raw, error);
            None
        }
    }
}

fn rollback_after_append_error(
    path: &Path,
    kind: ReceiverTempFileKind,
    original_len: u64,
    error: anyhow::Error,
) -> anyhow::Error {
    // Откатываем хвост сразу, чтобы ENOSPC/квота не оставляли за собой полузаписанный temp-файл.
    let rollback_file = match OpenOptions::new().write(true).open(path) {
        Ok(file) => file,
        Err(rollback_error) => {
            return error.context(format!(
                "failed to reopen {} {} for rollback: {}",
                kind.display_name(),
                path.display(),
                rollback_error
            ));
        }
    };
    if let Err(rollback_error) = rollback_file.set_len(original_len) {
        return error.context(format!(
            "failed to roll back {} {} to {} bytes: {}",
            kind.display_name(),
            path.display(),
            original_len,
            rollback_error
        ));
    }
    if let Err(sync_error) = rollback_file.sync_all() {
        return error.context(format!(
            "failed to sync rolled back {} {}: {}",
            kind.display_name(),
            path.display(),
            sync_error
        ));
    }
    error
}

fn receiver_temp_storage_io_error(
    kind: ReceiverTempFileKind,
    path: &Path,
    operation: &'static str,
    error: std::io::Error,
) -> anyhow::Error {
    if is_storage_exhausted_io_error(&error) {
        anyhow!(ReceiverTempStorageError::StorageExhausted {
            kind,
            path: path.to_path_buf(),
            operation,
            source: error,
        })
    } else {
        anyhow!(error).context(format!(
            "failed to {} {} {}",
            operation,
            kind.display_name(),
            path.display()
        ))
    }
}

fn is_storage_exhausted_io_error(error: &std::io::Error) -> bool {
    matches!(error.raw_os_error(), Some(28 | 112 | 122 | 1816))
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    use super::ReceiverTempFileKind;

    fn failures() -> &'static Mutex<BTreeMap<String, usize>> {
        static FAILURES: OnceLock<Mutex<BTreeMap<String, usize>>> = OnceLock::new();
        FAILURES.get_or_init(|| Mutex::new(BTreeMap::new()))
    }

    fn quotas() -> &'static Mutex<BTreeMap<ReceiverTempFileKind, u64>> {
        static QUOTAS: OnceLock<Mutex<BTreeMap<ReceiverTempFileKind, u64>>> = OnceLock::new();
        QUOTAS.get_or_init(|| Mutex::new(BTreeMap::new()))
    }

    fn lock_unpoisoned<T>(mutex: &'static Mutex<T>) -> MutexGuard<'static, T> {
        mutex.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn path_key(path: &Path) -> String {
        path.to_string_lossy().into_owned()
    }

    fn rename_key(from: &Path, to: &Path) -> String {
        format!("rename:{}=>{}", path_key(from), path_key(to))
    }

    fn remove_key(path: &Path) -> String {
        format!("remove:{}", path_key(path))
    }

    fn append_key(prefix: &str, path: &Path) -> String {
        format!("{prefix}:{}", path_key(path))
    }

    fn storage_full_error() -> std::io::Error {
        #[cfg(windows)]
        return std::io::Error::from_raw_os_error(112);

        #[cfg(not(windows))]
        return std::io::Error::from_raw_os_error(28);
    }

    fn maybe_fail(key: String) -> std::io::Result<()> {
        let mut failures = lock_unpoisoned(failures());
        let Some(remaining) = failures.get_mut(&key) else {
            return Ok(());
        };

        *remaining = remaining.saturating_sub(1);
        if *remaining == 0 {
            failures.remove(&key);
        }

        Err(std::io::Error::other(format!(
            "receiver test hook injected failure for {key}"
        )))
    }

    pub(crate) fn fail_next_rename(from: &Path, to: &Path) {
        lock_unpoisoned(failures()).insert(rename_key(from, to), 1);
    }

    #[allow(dead_code)]
    pub(crate) fn fail_next_remove_file(path: &Path) {
        lock_unpoisoned(failures()).insert(remove_key(path), 1);
    }

    pub(crate) fn fail_next_append_after_write(path: &Path) {
        lock_unpoisoned(failures()).insert(append_key("append-after-write", path), 1);
    }

    pub(crate) fn set_quota_override(kind: ReceiverTempFileKind, bytes: u64) {
        lock_unpoisoned(quotas()).insert(kind, bytes);
    }

    pub(crate) fn quota_override(kind: ReceiverTempFileKind) -> Option<u64> {
        lock_unpoisoned(quotas()).get(&kind).copied()
    }

    pub(crate) fn before_rename(from: &Path, to: &Path) -> std::io::Result<()> {
        maybe_fail(rename_key(from, to))
    }

    pub(crate) fn before_remove_file(path: &Path) -> std::io::Result<()> {
        maybe_fail(remove_key(path))
    }

    pub(crate) fn before_append(
        path: &Path,
        _kind: ReceiverTempFileKind,
        _current_len: u64,
        _payload_len: usize,
    ) -> std::io::Result<()> {
        maybe_fail(append_key("append-before-sync", path))
    }

    pub(crate) fn after_append_before_sync(
        path: &Path,
        _kind: ReceiverTempFileKind,
        _current_len: u64,
        _payload_len: usize,
    ) -> std::io::Result<()> {
        let key = append_key("append-after-write", path);
        let mut failures = lock_unpoisoned(failures());
        let Some(remaining) = failures.get_mut(&key) else {
            return Ok(());
        };

        *remaining = remaining.saturating_sub(1);
        if *remaining == 0 {
            failures.remove(&key);
        }

        Err(storage_full_error())
    }

    pub(crate) fn clear() {
        lock_unpoisoned(failures()).clear();
        lock_unpoisoned(quotas()).clear();
    }
}

#[cfg(not(test))]
pub(crate) mod test_hooks {
    use std::path::Path;

    use super::ReceiverTempFileKind;

    pub(crate) fn before_rename(_from: &Path, _to: &Path) -> std::io::Result<()> {
        Ok(())
    }

    pub(crate) fn before_remove_file(_path: &Path) -> std::io::Result<()> {
        Ok(())
    }

    pub(crate) fn before_append(
        _path: &Path,
        _kind: ReceiverTempFileKind,
        _current_len: u64,
        _payload_len: usize,
    ) -> std::io::Result<()> {
        Ok(())
    }

    pub(crate) fn after_append_before_sync(
        _path: &Path,
        _kind: ReceiverTempFileKind,
        _current_len: u64,
        _payload_len: usize,
    ) -> std::io::Result<()> {
        Ok(())
    }
}

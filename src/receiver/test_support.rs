use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::sender::SendOptions;

use super::shared::test_hooks;

pub(crate) struct TestDir {
    path: PathBuf,
}

pub(crate) struct FsFailureGuard {
    _lock: MutexGuard<'static, ()>,
}

impl TestDir {
    pub(crate) fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "fast-dump-receiver-tests-{}-{}",
            std::process::id(),
            unique
        ));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

impl FsFailureGuard {
    pub(crate) fn new() -> Self {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let lock = LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        test_hooks::clear();
        Self { _lock: lock }
    }
}

impl Drop for FsFailureGuard {
    fn drop(&mut self) {
        test_hooks::clear();
    }
}

pub(crate) fn archive_entries(path: &Path) -> Vec<(String, Vec<u8>)> {
    let file = fs::File::open(path).unwrap();
    let decoder = zstd::stream::read::Decoder::new(file).unwrap();
    let mut archive = tar::Archive::new(decoder);
    let mut entries = Vec::new();
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        let path = entry.path().unwrap().to_string_lossy().into_owned();
        let mut bytes = Vec::new();
        std::io::copy(&mut entry, &mut bytes).unwrap();
        entries.push((path, bytes));
    }
    entries
}

pub(crate) fn sample_send_options() -> SendOptions {
    SendOptions {
        level: 3,
        threads: 1,
        frame_target_bytes: 1,
        min_readers: 2,
        max_readers: 4,
        read_buffer_bytes: 1024,
        manifest_chunk_bytes: 512 * 1024,
        manifest_ack_records: 4_096,
        manifest_ack_bytes: 8 * 1024 * 1024,
    }
}

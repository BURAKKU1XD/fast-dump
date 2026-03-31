use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::network::protocol::{self, Message};

pub(crate) fn rename_path(from: &Path, to: &Path) -> std::io::Result<()> {
    test_hooks::before_rename(from, to)?;
    fs::rename(from, to)
}

pub(crate) fn remove_file_path(path: &Path) -> std::io::Result<()> {
    test_hooks::before_remove_file(path)?;
    fs::remove_file(path)
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

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};

    fn failures() -> &'static Mutex<BTreeMap<String, usize>> {
        static FAILURES: OnceLock<Mutex<BTreeMap<String, usize>>> = OnceLock::new();
        FAILURES.get_or_init(|| Mutex::new(BTreeMap::new()))
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

    fn maybe_fail(key: String) -> std::io::Result<()> {
        let mut failures = failures().lock().unwrap();
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
        failures().lock().unwrap().insert(rename_key(from, to), 1);
    }

    pub(crate) fn fail_next_remove_file(path: &Path) {
        failures().lock().unwrap().insert(remove_key(path), 1);
    }

    pub(crate) fn before_rename(from: &Path, to: &Path) -> std::io::Result<()> {
        maybe_fail(rename_key(from, to))
    }

    pub(crate) fn before_remove_file(path: &Path) -> std::io::Result<()> {
        maybe_fail(remove_key(path))
    }

    pub(crate) fn clear() {
        failures().lock().unwrap().clear();
    }
}

#[cfg(not(test))]
pub(crate) mod test_hooks {
    use std::path::Path;

    pub(crate) fn before_rename(_from: &Path, _to: &Path) -> std::io::Result<()> {
        Ok(())
    }

    pub(crate) fn before_remove_file(_path: &Path) -> std::io::Result<()> {
        Ok(())
    }
}

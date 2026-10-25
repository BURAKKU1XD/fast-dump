use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

pub(crate) fn create_run_dir(suite: &str) -> Result<PathBuf> {
    let root = std::env::var_os("FAST_DUMP_RESULTS_RUN_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("results/local/benchmarks")
        });
    std::fs::create_dir_all(&root)?;
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    // create_dir не позволяет двум процессам перезаписать один эксперимент.
    for sequence in 0..100 {
        let path = root.join(format!("{stamp}-{suite}-{}-{sequence}", std::process::id()));
        match std::fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).context("create benchmark result directory"),
        }
    }
    anyhow::bail!("could not reserve a unique benchmark result directory")
}

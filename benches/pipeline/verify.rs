use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};

enum SourceEntry {
    Regular { path: PathBuf, size: u64 },
    Symlink { target: PathBuf },
}

pub(crate) fn verify_if_requested(source_dir: &Path, archive_path: &Path) -> Result<()> {
    if !verification_enabled(std::env::var_os("FAST_DUMP_BENCH_VERIFY_ARCHIVE").as_deref())? {
        eprintln!("[bench-verify] disabled by FAST_DUMP_BENCH_VERIFY_ARCHIVE=0");
        return Ok(());
    }
    let (entries, bytes) = verify_archive(source_dir, archive_path)?;
    eprintln!(
        "[bench-verify] verified {entries} entries, {bytes} regular-file bytes against {}",
        source_dir.display()
    );
    Ok(())
}

fn verification_enabled(value: Option<&OsStr>) -> Result<bool> {
    match value {
        None => Ok(true),
        Some(value) if value == OsStr::new("1") => Ok(true),
        Some(value) if value == OsStr::new("0") => Ok(false),
        Some(value) => {
            anyhow::bail!("invalid FAST_DUMP_BENCH_VERIFY_ARCHIVE value {value:?}: expected 0 or 1")
        }
    }
}

fn collect_source_entries(source_dir: &Path) -> Result<BTreeMap<String, SourceEntry>> {
    let mut entries = BTreeMap::new();
    let mut directories = vec![source_dir.to_path_buf()];
    while let Some(directory) = directories.pop() {
        for child in fs::read_dir(&directory)
            .with_context(|| format!("failed to read fixture directory {}", directory.display()))?
        {
            let child = child?;
            let file_type = child.file_type()?;
            let path = child.path();
            if file_type.is_dir() {
                directories.push(path);
                continue;
            }
            let entry = if file_type.is_file() {
                SourceEntry::Regular {
                    size: child.metadata()?.len(),
                    path: path.clone(),
                }
            } else if file_type.is_symlink() {
                SourceEntry::Symlink {
                    target: fs::read_link(&path)?,
                }
            } else {
                continue;
            };
            let relative = path
                .strip_prefix(source_dir)?
                .to_string_lossy()
                .replace('\\', "/");
            ensure!(
                entries.insert(relative.clone(), entry).is_none(),
                "duplicate normalized fixture path: {relative}"
            );
        }
    }
    Ok(entries)
}

fn verify_archive(source_dir: &Path, archive_path: &Path) -> Result<(usize, u64)> {
    let mut expected = collect_source_entries(source_dir)?;
    let entry_count = expected.len();
    let archive_file = File::open(archive_path)
        .with_context(|| format!("failed to open archive {}", archive_path.display()))?;
    // Decoder читает все сцепленные zstd frames; tar останавливается только
    // на штатных нулевых блоках, поэтому преждевременный конец не скрывает потери.
    let decoder = zstd::stream::read::Decoder::new(archive_file)?;
    let mut archive = tar::Archive::new(decoder);
    let mut source_buffer = vec![0_u8; 128 * 1024];
    let mut archive_buffer = vec![0_u8; source_buffer.len()];
    let mut regular_bytes = 0_u64;
    for entry in archive.entries()? {
        let mut entry = entry.context("failed to decode archive entry")?;
        let relative = entry.path()?.to_string_lossy().replace('\\', "/");
        let source_entry = expected
            .remove(&relative)
            .with_context(|| format!("unexpected or duplicate archive path: {relative}"))?;
        match source_entry {
            SourceEntry::Regular { path, size } => {
                ensure!(
                    entry.header().entry_type().is_file(),
                    "expected regular file: {relative}"
                );
                ensure!(entry.size() == size, "archive size mismatch: {relative}");
                let mut source = File::open(&path)?;
                let mut offset = 0_u64;
                while offset < size {
                    let count = (size - offset).min(source_buffer.len() as u64) as usize;
                    source
                        .read_exact(&mut source_buffer[..count])
                        .with_context(|| format!("fixture changed while reading {relative}"))?;
                    entry
                        .read_exact(&mut archive_buffer[..count])
                        .with_context(|| format!("truncated archive entry: {relative}"))?;
                    ensure!(
                        archive_buffer[..count] == source_buffer[..count],
                        "archive contents differ at byte block {offset}: {relative}"
                    );
                    offset += count as u64;
                }
                ensure!(
                    source.read(&mut source_buffer[..1])? == 0,
                    "fixture grew: {relative}"
                );
                regular_bytes += size;
            }
            SourceEntry::Symlink { target } => {
                ensure!(
                    entry.header().entry_type().is_symlink(),
                    "expected symlink: {relative}"
                );
                ensure!(
                    entry.size() == 0,
                    "symlink has an unexpected payload: {relative}"
                );
                let archived_target = entry
                    .link_name()?
                    .with_context(|| format!("symlink target missing: {relative}"))?;
                ensure!(
                    archived_target.as_ref() == target.as_path(),
                    "symlink target mismatch: {relative}"
                );
            }
        }
    }
    ensure!(
        expected.is_empty(),
        "archive omitted {} source entries; first missing: {:?}",
        expected.len(),
        expected.keys().next()
    );

    // Итератор tar уже прочитал первый нулевой блок. Проверяем второй и EOF
    // декодера: так не пропускаем испорченный zstd footer или записи после конца tar.
    let mut decoder = archive.into_inner();
    let mut tail_bytes = 0_u64;
    loop {
        let count = decoder.read(&mut archive_buffer)?;
        if count == 0 {
            break;
        }
        ensure!(
            archive_buffer[..count].iter().all(|byte| *byte == 0),
            "nonzero data after tar termination"
        );
        tail_bytes += count as u64;
    }
    ensure!(
        tail_bytes >= 512,
        "archive is missing its final tar zero block"
    );
    Ok((entry_count, regular_bytes))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn verification_defaults_to_enabled_and_rejects_invalid_configuration() {
        assert!(verification_enabled(None).unwrap());
        assert!(verification_enabled(Some(OsStr::new("1"))).unwrap());
        assert!(!verification_enabled(Some(OsStr::new("0"))).unwrap());
        for value in ["", "true", "false", "2", " 1", "0 "] {
            assert!(verification_enabled(Some(OsStr::new(value))).is_err());
        }
    }

    fn write_archive(path: &Path, entries: &[(&str, &[u8])]) {
        let mut builder = tar::Builder::new(Vec::new());
        for (name, contents) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(contents.len() as u64);
            header.set_mode(0o600);
            builder.append_data(&mut header, name, *contents).unwrap();
        }
        let bytes = builder.into_inner().unwrap();
        let mut file = File::create(path).unwrap();
        for chunk in bytes.chunks(1024) {
            file.write_all(&zstd::bulk::compress(chunk, 1).unwrap())
                .unwrap();
        }
    }

    #[test]
    fn verification_checks_all_files_and_contents_across_frames() {
        let dir = crate::pipeline::shared::BenchDir::new("archive-verification");
        let source = dir.path().join("source");
        fs::create_dir_all(source.join("empty-directory")).unwrap();
        fs::write(source.join("one.bin"), b"one").unwrap();
        fs::write(source.join("two.bin"), b"two").unwrap();
        let archive = dir.path().join("dump.tar.zst");

        write_archive(&archive, &[("one.bin", b"one"), ("two.bin", b"two")]);
        assert_eq!(verify_archive(&source, &archive).unwrap(), (2, 6));
        write_archive(&archive, &[("one.bin", b"one")]);
        assert!(verify_archive(&source, &archive).is_err());
        write_archive(
            &archive,
            &[
                ("one.bin", b"one"),
                ("one.bin", b"one"),
                ("two.bin", b"two"),
            ],
        );
        assert!(verify_archive(&source, &archive).is_err());
        write_archive(&archive, &[("one.bin", b"one"), ("two.bin", b"bad")]);
        assert!(verify_archive(&source, &archive).is_err());
    }

    #[test]
    fn verification_rejects_truncated_final_frame() {
        let dir = crate::pipeline::shared::BenchDir::new("archive-verification-truncated");
        let source = dir.path().join("source");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("file.bin"), b"content").unwrap();
        let archive = dir.path().join("dump.tar.zst");
        write_archive(&archive, &[("file.bin", b"content")]);
        let bytes = fs::read(&archive).unwrap();
        fs::write(&archive, &bytes[..bytes.len() - 1]).unwrap();
        assert!(verify_archive(&source, &archive).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn verification_compares_symlink_target_without_dereferencing() {
        let dir = crate::pipeline::shared::BenchDir::new("archive-verification-symlink");
        let source = dir.path().join("source");
        fs::create_dir_all(&source).unwrap();
        let target = "../missing-target";
        std::os::unix::fs::symlink(target, source.join("link")).unwrap();
        let archive = dir.path().join("dump.tar.zst");
        for (archived_target, should_pass) in [(target, true), ("../other-target", false)] {
            let mut builder = tar::Builder::new(Vec::new());
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Symlink);
            header.set_size(0);
            header.set_mode(0o777);
            builder
                .append_link(&mut header, "link", archived_target)
                .unwrap();
            let bytes = builder.into_inner().unwrap();
            fs::write(&archive, zstd::bulk::compress(&bytes, 1).unwrap()).unwrap();
            assert_eq!(verify_archive(&source, &archive).is_ok(), should_pass);
        }
    }
}

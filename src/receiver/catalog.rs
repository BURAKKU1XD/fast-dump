use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};

use crate::network::protocol::{SkipReason, SkippedEntry};

use super::ReceiverPaths;

const CATALOG_HEADER: &str = "fast-dump-v4-catalog-1";

pub(crate) fn load_terminal_skips(path: &Path) -> Result<Vec<SkippedEntry>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read v4 catalog {}", path.display()))?;
    // Каталог публикуется целиком через synced temp+rename. Пустой файл или
    // хвост без newline не являются незавершённой транзакцией: это потеря skips.
    ensure!(
        content.ends_with('\n'),
        "incomplete v4 catalog {}",
        path.display()
    );
    let mut lines = content.lines();
    match lines.next() {
        Some(CATALOG_HEADER) => {}
        Some(other) => bail!(
            "unexpected v4 catalog header in {}: {}",
            path.display(),
            other
        ),
        None => bail!("empty v4 catalog {}", path.display()),
    }

    let mut skips = Vec::new();
    for line in lines {
        skips.push(parse_skip(line)?);
    }
    Ok(skips)
}

pub(crate) fn merge_terminal_skips(
    path: &Path,
    new_skips: &[SkippedEntry],
) -> Result<Vec<SkippedEntry>> {
    let mut merged = load_terminal_skips(path)?
        .into_iter()
        .map(|skip| (skip.relative_path.clone(), skip))
        .collect::<BTreeMap<_, _>>();

    for skip in new_skips {
        merged
            .entry(skip.relative_path.clone())
            .or_insert_with(|| skip.clone());
    }

    let mut values = merged.into_values().collect::<Vec<_>>();
    values.sort_by(|left, right| {
        left.work_ordinal
            .cmp(&right.work_ordinal)
            .then_with(|| left.relative_path.cmp(&right.relative_path))
    });
    write_catalog(path, &values)?;
    Ok(values)
}

pub(crate) fn finalize_issue_report(
    paths: &ReceiverPaths,
    terminal_skips: &[SkippedEntry],
) -> Result<Option<PathBuf>> {
    if terminal_skips.is_empty() {
        let mut removed = false;
        for path in [&paths.issues_part_path, &paths.issues_final_path] {
            if path.exists() {
                fs::remove_file(path).with_context(|| {
                    format!("failed to remove stale v4 issue report {}", path.display())
                })?;
                removed = true;
            }
        }
        if removed {
            sync_parent_directory(&paths.issues_final_path)?;
        }
        return Ok(None);
    }

    let mut output = String::from("work_ordinal\treason\trelative_path\n");
    for skip in terminal_skips {
        output.push_str(&format!(
            "{}\t{}\t{}\n",
            skip.work_ordinal,
            skip.reason.as_str(),
            skip.relative_path
        ));
    }

    write_synced_file(&paths.issues_part_path, output.as_bytes())?;
    replace_synced_file(&paths.issues_part_path, &paths.issues_final_path)?;

    Ok(Some(paths.issues_final_path.clone()))
}

fn write_catalog(path: &Path, skips: &[SkippedEntry]) -> Result<()> {
    let tmp_path = temporary_catalog_path(path);
    let mut output = String::new();
    output.push_str(CATALOG_HEADER);
    output.push('\n');
    for skip in skips {
        output.push_str(&format_skip(skip));
        output.push('\n');
    }

    write_synced_file(&tmp_path, output.as_bytes())?;
    replace_synced_file(&tmp_path, path)
}

fn write_synced_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = File::create(path)
        .with_context(|| format!("failed to create v4 state file {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("failed to write v4 state file {}", path.display()))?;
    // SkipAck разрешён только после сохранения нового содержимого каталога.
    file.sync_all()
        .with_context(|| format!("failed to sync v4 state file {}", path.display()))
}

fn replace_synced_file(source: &Path, destination: &Path) -> Result<()> {
    // Rename заменяет файл целиком: старый каталог остаётся доступен до успеха.
    fs::rename(source, destination).with_context(|| {
        format!(
            "failed to replace v4 state file {} -> {}",
            source.display(),
            destination.display()
        )
    })?;
    sync_parent_directory(destination)
}

pub(super) fn sync_parent_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .with_context(|| format!("failed to sync v4 state directory {}", parent.display()))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn temporary_catalog_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

fn format_skip(skip: &SkippedEntry) -> String {
    format!(
        "skip\t{}\t{}\t{}",
        skip.work_ordinal,
        skip.reason.as_str(),
        escape_field(&skip.relative_path)
    )
}

fn parse_skip(line: &str) -> Result<SkippedEntry> {
    let columns = line.split('\t').collect::<Vec<_>>();
    if columns.len() != 4 || columns[0] != "skip" {
        bail!("unexpected v4 catalog record format");
    }
    Ok(SkippedEntry {
        work_ordinal: columns[1]
            .parse::<u64>()
            .with_context(|| "failed to parse v4 catalog work_ordinal")?,
        reason: parse_reason(columns[2])?,
        relative_path: unescape_field(columns[3])?,
    })
}

fn parse_reason(raw: &str) -> Result<SkipReason> {
    match raw {
        "open_failed" => Ok(SkipReason::OpenFailed),
        "unstable_after_retries" => Ok(SkipReason::UnstableAfterRetries),
        "read_symlink_failed" => Ok(SkipReason::ReadSymlinkFailed),
        "unsupported_entry" => Ok(SkipReason::UnsupportedEntry),
        other => bail!("unexpected v4 catalog skip reason: {}", other),
    }
}

fn escape_field(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn unescape_field(value: &str) -> Result<String> {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            output.push(ch);
            continue;
        }
        let Some(escaped) = chars.next() else {
            bail!("dangling escape in v4 catalog field");
        };
        match escaped {
            '\\' => output.push('\\'),
            't' => output.push('\t'),
            'n' => output.push('\n'),
            'r' => output.push('\r'),
            other => bail!("unsupported v4 catalog escape sequence: \\{}", other),
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::TestDir;

    fn skip(work_ordinal: u64, path: &str, reason: SkipReason) -> SkippedEntry {
        SkippedEntry {
            work_ordinal,
            relative_path: path.into(),
            reason,
        }
    }

    #[test]
    fn replacing_catalog_preserves_union_and_original_terminal_reason() {
        let directory = TestDir::new();
        let path = directory.path().join("dump.catalog.part");
        let first = skip(7, "nested/original.bin", SkipReason::OpenFailed);
        let second = skip(
            2,
            "tab\tline\nslash\\.bin",
            SkipReason::UnstableAfterRetries,
        );
        merge_terminal_skips(&path, std::slice::from_ref(&first)).unwrap();
        let duplicate = skip(99, &first.relative_path, SkipReason::UnsupportedEntry);
        let expected = vec![second.clone(), first];
        assert_eq!(
            merge_terminal_skips(&path, &[duplicate, second]).unwrap(),
            expected
        );
        assert_eq!(load_terminal_skips(&path).unwrap(), expected);
        assert!(!temporary_catalog_path(&path).exists());
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            "fast-dump-v4-catalog-1\nskip\t2\tunstable_after_retries\ttab\\tline\\nslash\\\\.bin\nskip\t7\topen_failed\tnested/original.bin\n"
        );
    }

    #[test]
    fn failed_catalog_temp_write_preserves_previous_catalog() {
        let directory = TestDir::new();
        let path = directory.path().join("dump.catalog.part");
        let original = skip(1, "original.bin", SkipReason::OpenFailed);
        merge_terminal_skips(&path, std::slice::from_ref(&original)).unwrap();
        let bytes = fs::read(&path).unwrap();
        fs::create_dir(temporary_catalog_path(&path)).unwrap();
        assert!(
            merge_terminal_skips(&path, &[skip(2, "new.bin", SkipReason::OpenFailed)]).is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(load_terminal_skips(&path).unwrap(), vec![original]);
    }

    #[test]
    fn malformed_catalog_is_not_reinterpreted_as_no_terminal_skips() {
        let directory = TestDir::new();
        let path = directory.path().join("dump.catalog.part");
        for corrupt in [
            "",
            "fast-dump-v4-catalog-1",
            "fast-dump-v4-catalog-1\nskip\t1\topen_failed\tfile.bin",
            "fast-dump-v4-catalog-1\n\n",
        ] {
            fs::write(&path, corrupt).unwrap();
            assert!(load_terminal_skips(&path).is_err());
            assert!(
                merge_terminal_skips(&path, &[skip(2, "next.bin", SkipReason::OpenFailed)])
                    .is_err()
            );
            assert_eq!(fs::read_to_string(&path).unwrap(), corrupt);
        }
        fs::write(&path, "fast-dump-v4-catalog-1\n").unwrap();
        assert!(load_terminal_skips(&path).unwrap().is_empty());
    }

    #[test]
    fn failed_rename_does_not_remove_existing_destination() {
        let directory = TestDir::new();
        let destination = directory.path().join("existing.catalog.part");
        fs::write(&destination, b"durable original state").unwrap();
        assert!(replace_synced_file(&directory.path().join("missing.tmp"), &destination).is_err());
        assert_eq!(fs::read(destination).unwrap(), b"durable original state");
    }

    #[test]
    fn issue_report_replaces_stale_content_and_preserves_exact_tail_format() {
        let directory = TestDir::new();
        let paths = ReceiverPaths::new(directory.path().to_path_buf(), "dump");
        fs::write(&paths.issues_final_path, b"stale report").unwrap();
        let skips = [
            skip(1, "first.bin", SkipReason::OpenFailed),
            skip(3, "nested/second.bin", SkipReason::UnstableAfterRetries),
        ];
        assert_eq!(
            finalize_issue_report(&paths, &skips).unwrap(),
            Some(paths.issues_final_path.clone())
        );
        assert_eq!(
            fs::read_to_string(&paths.issues_final_path).unwrap(),
            "work_ordinal\treason\trelative_path\n1\topen_failed\tfirst.bin\n3\tunstable_after_retries\tnested/second.bin\n"
        );
        assert!(!paths.issues_part_path.exists());
        assert_eq!(finalize_issue_report(&paths, &[]).unwrap(), None);
        assert!(!paths.issues_final_path.exists());
    }

    #[test]
    fn failed_issue_part_write_preserves_previous_report() {
        let directory = TestDir::new();
        let paths = ReceiverPaths::new(directory.path().to_path_buf(), "dump");
        fs::write(&paths.issues_final_path, b"previous report").unwrap();
        fs::create_dir(&paths.issues_part_path).unwrap();
        assert!(
            finalize_issue_report(&paths, &[skip(1, "file.bin", SkipReason::OpenFailed)]).is_err()
        );
        assert_eq!(
            fs::read(&paths.issues_final_path).unwrap(),
            b"previous report"
        );
    }
}

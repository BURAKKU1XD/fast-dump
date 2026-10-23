use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{Context, Result, bail, ensure};

use crate::network::protocol::BundleDescriptor;

const WAL_HEADER: &str = "fast-dump-v4-wal-1";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct WalState {
    pub(crate) last_committed_bundle_no: u64,
    pub(crate) last_committed_work_ordinal: u64,
    pub(crate) last_committed_relative_path: String,
    pub(crate) archive_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalRecord {
    pub(crate) bundle_no: u64,
    pub(crate) first_work_ordinal: u64,
    pub(crate) last_work_ordinal: u64,
    pub(crate) archive_len: u64,
    pub(crate) compressed_payload_bytes: u64,
    pub(crate) compressed_xxh3_64: u64,
    pub(crate) last_relative_path: String,
}

pub(crate) struct WalWriter {
    file: File,
}

impl WalWriter {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let write_header = !path.exists() || fs::metadata(path)?.len() == 0;
        let valid_len = if write_header {
            None
        } else {
            Some(read_valid_state(path)?.1)
        };
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(false)
            .open(path)
            .with_context(|| format!("failed to open v4 wal {}", path.display()))?;
        if let Some(valid_len) = valid_len {
            // Оборванная строка не должна скрывать последующие commit после resume.
            if file.metadata()?.len() != valid_len {
                file.set_len(valid_len)?;
            }
        }
        // Windows не разрешает set_len для append-only handle; writer здесь один.
        file.seek(SeekFrom::End(0))?;
        if write_header {
            writeln!(file, "{WAL_HEADER}")
                .with_context(|| format!("failed to initialize v4 wal {}", path.display()))?;
            file.sync_all()
                .with_context(|| format!("failed to sync v4 wal header {}", path.display()))?;
        }
        Ok(Self { file })
    }

    pub(crate) fn append_commit_record(&mut self, record: &WalRecord) -> Result<()> {
        #[cfg(test)]
        super::storage_faults::write_failure(
            super::storage_faults::Point::WalAppend,
            &mut self.file,
            format!("{}\n", format_record(record)).as_bytes(),
        )?;
        writeln!(self.file, "{}", format_record(record))
            .with_context(|| "failed to append v4 wal commit record")
    }

    pub(crate) fn sync(&mut self) -> Result<()> {
        #[cfg(test)]
        super::storage_faults::check(super::storage_faults::Point::WalSync)?;
        self.file
            .sync_all()
            .with_context(|| "failed to sync v4 wal commit record")
    }
}

pub(crate) fn wal_record_from_descriptor(
    descriptor: &BundleDescriptor,
    archive_len: u64,
) -> WalRecord {
    WalRecord {
        bundle_no: descriptor.bundle_no,
        first_work_ordinal: descriptor.first_work_ordinal,
        last_work_ordinal: descriptor.last_work_ordinal,
        archive_len,
        compressed_payload_bytes: descriptor.compressed_payload_bytes,
        compressed_xxh3_64: descriptor.compressed_xxh3_64,
        last_relative_path: descriptor.last_relative_path.clone(),
    }
}

pub(crate) fn load_state(path: &Path) -> Result<WalState> {
    if !path.exists() {
        return Ok(WalState::default());
    }

    Ok(read_valid_state(path)?.0)
}

fn read_valid_state(path: &Path) -> Result<(WalState, u64)> {
    let content =
        fs::read(path).with_context(|| format!("failed to read v4 wal {}", path.display()))?;
    if content.is_empty() {
        return Ok((WalState::default(), 0));
    }
    let mut lines = content.split_inclusive(|byte| *byte == b'\n');
    match lines.next() {
        Some(header) if header.strip_suffix(b"\n") == Some(WAL_HEADER.as_bytes()) => {}
        Some(_) => bail!("unexpected v4 wal header in {}", path.display()),
        None => unreachable!(),
    }

    let mut state = WalState::default();
    let mut valid_len = (WAL_HEADER.len() + 1) as u64;
    for bytes in lines {
        let Some(line) = bytes.strip_suffix(b"\n") else {
            break;
        };
        // Оборван только EOF без newline. Повреждённая полная строка могла
        // принадлежать подтверждённому commit: молча откатывать её нельзя.
        let line = std::str::from_utf8(line).with_context(|| {
            format!("invalid UTF-8 in complete v4 wal record at byte {valid_len}")
        })?;
        let record = parse_record(line)
            .with_context(|| format!("invalid complete v4 wal record at byte {valid_len}"))?;
        validate_record(&state, &record)
            .with_context(|| format!("inconsistent complete v4 wal record at byte {valid_len}"))?;
        state.last_committed_bundle_no = record.bundle_no;
        state.last_committed_work_ordinal = record.last_work_ordinal;
        state.last_committed_relative_path = record.last_relative_path;
        state.archive_len = record.archive_len;
        valid_len += bytes.len() as u64;
    }
    Ok((state, valid_len))
}

fn validate_record(previous: &WalState, record: &WalRecord) -> Result<()> {
    // Номера могут иметь пропуски: streamed bundle резервируется ещё до retries,
    // а окончательный skip не пишет commit. Возврат назад недопустим.
    ensure!(
        record.bundle_no > previous.last_committed_bundle_no,
        "v4 wal bundle number is not increasing"
    );
    ensure!(
        record.first_work_ordinal > previous.last_committed_work_ordinal
            && record.last_work_ordinal >= record.first_work_ordinal,
        "v4 wal work ordinal range is not increasing"
    );
    ensure!(
        !record.last_relative_path.is_empty(),
        "v4 wal last relative path is empty"
    );
    ensure!(
        record.compressed_payload_bytes > 0,
        "v4 wal committed payload is empty"
    );
    let expected_end = previous
        .archive_len
        .checked_add(record.compressed_payload_bytes)
        .context("v4 wal archive boundary overflow")?;
    ensure!(
        record.archive_len == expected_end,
        "v4 wal archive boundary does not match committed payload length"
    );
    Ok(())
}

fn format_record(record: &WalRecord) -> String {
    format!(
        "commit\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        record.bundle_no,
        record.first_work_ordinal,
        record.last_work_ordinal,
        record.archive_len,
        record.compressed_payload_bytes,
        record.compressed_xxh3_64,
        escape_field(&record.last_relative_path)
    )
}

fn parse_record(line: &str) -> Result<WalRecord> {
    let columns = line.split('\t').collect::<Vec<_>>();
    if columns.len() != 8 || columns[0] != "commit" {
        bail!("unexpected v4 wal record format");
    }
    Ok(WalRecord {
        bundle_no: parse_u64(columns[1], "bundle_no")?,
        first_work_ordinal: parse_u64(columns[2], "first_work_ordinal")?,
        last_work_ordinal: parse_u64(columns[3], "last_work_ordinal")?,
        archive_len: parse_u64(columns[4], "archive_len")?,
        compressed_payload_bytes: parse_u64(columns[5], "compressed_payload_bytes")?,
        compressed_xxh3_64: parse_u64(columns[6], "compressed_xxh3_64")?,
        last_relative_path: unescape_field(columns[7])?,
    })
}

fn parse_u64(raw: &str, field: &str) -> Result<u64> {
    raw.parse::<u64>()
        .with_context(|| format!("failed to parse v4 wal field {field}"))
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
            bail!("dangling escape in v4 wal field");
        };
        match escaped {
            '\\' => output.push('\\'),
            't' => output.push('\t'),
            'n' => output.push('\n'),
            'r' => output.push('\r'),
            other => bail!("unsupported v4 wal escape sequence: \\{}", other),
        }
    }
    Ok(output)
}

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use xxhash_rust::xxh3::xxh3_64;

use crate::network::protocol::{self, ArchiveBoundaryV3, Message, ResumeState, ResumeStateV3};

use super::shared::{
    ReceiverTempFileKind, append_temp_file_payload, remove_file_if_exists, remove_file_path,
    rename_path,
};

// Отдельный magic помогает не перепутать journal с частичным архивом
// или с произвольным мусором на диске.
const JOURNAL_MAGIC: u32 = 0x4A4D4446;
const MANIFEST_FRAME_FLAG_HAS_RECORD_SEQ: u16 = 0x0001;
const MANIFEST_FRAME_FLAG_HAS_WORK_ORDINAL: u16 = 0x0002;
const MANIFEST_FRAME_FLAG_SEALED: u16 = 0x0004;
const MANIFEST_FRAME_FIXED_HEADER_LEN: usize = 4 + 4 + 8 + 8 + 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverPaths {
    data_dir: PathBuf,
    dump_id: String,
    partial_archive_path: PathBuf,
    final_archive_path: PathBuf,
    manifest_path: PathBuf,
    journal_path: PathBuf,
    issue_journal_path: PathBuf,
}

impl ReceiverPaths {
    pub fn new(data_dir: impl Into<PathBuf>, dump_id: impl Into<String>) -> Result<Self> {
        let data_dir = data_dir.into();
        let dump_id = dump_id.into();
        validate_dump_id(&dump_id)?;

        Ok(Self {
            partial_archive_path: data_dir.join(format!("{dump_id}.tar.zst.part")),
            final_archive_path: data_dir.join(format!("{dump_id}.tar.zst")),
            manifest_path: data_dir.join(format!("{dump_id}.manifest.part")),
            journal_path: data_dir.join(format!("{dump_id}.tar.zst.part.journal")),
            issue_journal_path: data_dir.join(format!("{dump_id}.tar.zst.issues.tsv")),
            data_dir,
            dump_id,
        })
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn dump_id(&self) -> &str {
        &self.dump_id
    }

    pub fn partial_archive_path(&self) -> &Path {
        &self.partial_archive_path
    }

    pub fn final_archive_path(&self) -> &Path {
        &self.final_archive_path
    }

    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    pub fn journal_path(&self) -> &Path {
        &self.journal_path
    }

    pub fn issue_journal_path(&self) -> &Path {
        &self.issue_journal_path
    }

    pub fn issue_journal_temp_path(&self) -> PathBuf {
        self.data_dir
            .join(format!("{}.tar.zst.issues.tsv.part", self.dump_id))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverJournal {
    pub protocol_version: u16,
    pub dump_id: String,
    pub last_committed_frame: u64,
    pub last_committed_file_index: u64,
    pub last_committed_file_path: String,
    pub last_committed_work_ordinal: Option<u64>,
    pub archive_len: u64,
    pub manifest_bytes_persisted: u64,
    pub last_manifest_record_seq: Option<u64>,
    pub last_manifest_work_ordinal: Option<u64>,
    pub manifest_sealed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReceiverManifestBoundary {
    pub manifest_bytes_persisted: u64,
    pub last_manifest_record_seq: Option<u64>,
    pub last_manifest_work_ordinal: Option<u64>,
    pub manifest_sealed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReceiverManifestRecoveryIssue {
    TruncatedTail { detail: String },
    CorruptedTail { detail: String },
}

impl ReceiverJournal {
    pub fn initial(dump_id: impl Into<String>) -> Result<Self> {
        let journal = Self {
            protocol_version: protocol::VERSION,
            dump_id: dump_id.into(),
            last_committed_frame: 0,
            last_committed_file_index: 0,
            last_committed_file_path: String::new(),
            last_committed_work_ordinal: None,
            archive_len: 0,
            manifest_bytes_persisted: 0,
            last_manifest_record_seq: None,
            last_manifest_work_ordinal: None,
            manifest_sealed: false,
        };
        validate_dump_id(&journal.dump_id)?;
        validate_against_protocol(&journal)?;
        validate_manifest_boundary(journal.manifest_boundary())?;
        Ok(journal)
    }

    pub fn load(path: &Path) -> Result<Self> {
        let bytes = fs::read(path)
            .with_context(|| format!("failed to read receiver journal {}", path.display()))?;
        let mut reader = Cursor::new(bytes.as_slice());

        let magic = read_u32(&mut reader)?;
        if magic != JOURNAL_MAGIC {
            bail!("invalid receiver journal magic: {magic:#010x}");
        }

        let mut journal = Self {
            protocol_version: read_u16(&mut reader)?,
            dump_id: read_string(&mut reader, "dump_id")?,
            last_committed_frame: read_u64(&mut reader)?,
            last_committed_file_index: read_u64(&mut reader)?,
            last_committed_file_path: read_string(&mut reader, "last_committed_file_path")?,
            last_committed_work_ordinal: None,
            archive_len: read_u64(&mut reader)?,
            manifest_bytes_persisted: read_u64(&mut reader)?,
            last_manifest_record_seq: decode_optional_u64(read_u64(&mut reader)?),
            last_manifest_work_ordinal: decode_optional_u64(read_u64(&mut reader)?),
            manifest_sealed: read_bool(&mut reader, "manifest_sealed")?,
        };

        if bytes.len().saturating_sub(reader.position() as usize) >= 8 {
            journal.last_committed_work_ordinal = decode_optional_u64(read_u64(&mut reader)?);
        }

        if journal.protocol_version != protocol::VERSION {
            bail!(
                "unsupported receiver journal protocol version: {}",
                journal.protocol_version
            );
        }
        validate_dump_id(&journal.dump_id)?;
        validate_against_protocol(&journal)?;
        validate_manifest_boundary(journal.manifest_boundary())?;

        let consumed = reader.position() as usize;
        if consumed != bytes.len() {
            bail!(
                "receiver journal contains unexpected trailing bytes: consumed {consumed} of {}",
                bytes.len()
            );
        }

        Ok(journal)
    }

    pub fn persist(&self, path: &Path) -> Result<()> {
        validate_dump_id(&self.dump_id)?;
        if self.protocol_version != protocol::VERSION {
            bail!(
                "receiver journal protocol version must match current protocol version: {} != {}",
                self.protocol_version,
                protocol::VERSION
            );
        }
        validate_against_protocol(self)?;
        validate_manifest_boundary(self.manifest_boundary())?;

        let parent = path
            .parent()
            .context("receiver journal path must have a parent directory")?;
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create receiver data dir {}", parent.display()))?;

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&JOURNAL_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&self.protocol_version.to_le_bytes());
        write_string(&mut bytes, &self.dump_id)?;
        bytes.extend_from_slice(&self.last_committed_frame.to_le_bytes());
        bytes.extend_from_slice(&self.last_committed_file_index.to_le_bytes());
        write_string(&mut bytes, &self.last_committed_file_path)?;
        bytes.extend_from_slice(&self.archive_len.to_le_bytes());
        bytes.extend_from_slice(&self.manifest_bytes_persisted.to_le_bytes());
        bytes.extend_from_slice(&encode_optional_u64(self.last_manifest_record_seq).to_le_bytes());
        bytes
            .extend_from_slice(&encode_optional_u64(self.last_manifest_work_ordinal).to_le_bytes());
        bytes.push(u8::from(self.manifest_sealed));
        bytes.extend_from_slice(
            &encode_optional_u64(self.last_committed_work_ordinal).to_le_bytes(),
        );

        let temp_path = journal_temp_path(path)?;
        let backup_path = journal_backup_path(path)?;
        {
            let mut file = File::create(&temp_path).with_context(|| {
                format!(
                    "failed to create temporary receiver journal {}",
                    temp_path.display()
                )
            })?;
            file.write_all(&bytes).with_context(|| {
                format!(
                    "failed to write temporary receiver journal {}",
                    temp_path.display()
                )
            })?;
            file.sync_all().with_context(|| {
                format!(
                    "failed to sync temporary receiver journal {}",
                    temp_path.display()
                )
            })?;
        }

        let had_existing_journal = path.exists();
        if had_existing_journal {
            if backup_path.exists() {
                remove_file_path(&backup_path).with_context(|| {
                    format!(
                        "failed to remove stale receiver journal backup {}",
                        backup_path.display()
                    )
                })?;
            }
            rename_path(path, &backup_path).with_context(|| {
                format!(
                    "failed to move existing receiver journal {} to backup {}",
                    path.display(),
                    backup_path.display()
                )
            })?;
        }

        if let Err(error) = rename_path(&temp_path, path) {
            let error = anyhow::Error::from(error).context(format!(
                "failed to move receiver journal {} into place {}",
                temp_path.display(),
                path.display()
            ));
            let _ = remove_file_path(&temp_path);
            if had_existing_journal
                && backup_path.exists()
                && let Err(restore_error) = rename_path(&backup_path, path)
            {
                return Err(error.context(format!(
                    "failed to restore previous receiver journal {} from backup {}: {}",
                    path.display(),
                    backup_path.display(),
                    restore_error
                )));
            }
            return Err(error);
        }

        if backup_path.exists() {
            let _ = remove_file_path(&backup_path);
        }

        Ok(())
    }

    pub fn to_resume_state(&self) -> ResumeState {
        ResumeState {
            last_committed_frame: self.last_committed_frame,
            last_committed_file_index: self.last_committed_file_index,
            last_committed_file_path: self.last_committed_file_path.clone(),
            archive_len: self.archive_len,
        }
    }

    pub fn archive_boundary_v3(&self) -> ArchiveBoundaryV3 {
        ArchiveBoundaryV3 {
            last_committed_frame: self.last_committed_frame,
            last_committed_work_ordinal: self.last_committed_work_ordinal,
            archive_len: self.archive_len,
        }
    }

    pub fn to_resume_state_v3(&self) -> ResumeStateV3 {
        ResumeStateV3 {
            manifest: protocol::ManifestBoundaryV3 {
                manifest_bytes_persisted: self.manifest_bytes_persisted,
                last_record_seq: self.last_manifest_record_seq,
                last_work_ordinal: self.last_manifest_work_ordinal,
                sealed: self.manifest_sealed,
            },
            archive: self.archive_boundary_v3(),
        }
    }

    pub fn manifest_boundary(&self) -> ReceiverManifestBoundary {
        ReceiverManifestBoundary {
            manifest_bytes_persisted: self.manifest_bytes_persisted,
            last_manifest_record_seq: self.last_manifest_record_seq,
            last_manifest_work_ordinal: self.last_manifest_work_ordinal,
            manifest_sealed: self.manifest_sealed,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverState {
    pub paths: ReceiverPaths,
    pub journal: ReceiverJournal,
    pub manifest_recovery_issues: Vec<ReceiverManifestRecoveryIssue>,
}

impl ReceiverState {
    pub fn open(data_dir: impl Into<PathBuf>, dump_id: impl Into<String>) -> Result<Self> {
        let paths = ReceiverPaths::new(data_dir, dump_id)?;
        fs::create_dir_all(paths.data_dir()).with_context(|| {
            format!(
                "failed to create receiver data dir {}",
                paths.data_dir().display()
            )
        })?;
        recover_journal_backup(paths.journal_path())?;

        let journal = if paths.journal_path().exists() {
            let journal = ReceiverJournal::load(paths.journal_path())?;
            if journal.dump_id != paths.dump_id() {
                bail!(
                    "receiver journal dump_id mismatch: expected {} got {}",
                    paths.dump_id(),
                    journal.dump_id
                );
            }
            journal
        } else {
            if paths.partial_archive_path().exists() {
                bail!(
                    "partial archive exists without journal: {}",
                    paths.partial_archive_path().display()
                );
            }
            if paths.final_archive_path().exists() {
                bail!(
                    "final archive already exists for dump_id {}: {}",
                    paths.dump_id(),
                    paths.final_archive_path().display()
                );
            }

            remove_file_if_exists(paths.manifest_path()).with_context(|| {
                format!(
                    "failed to discard abandoned receiver manifest store {}",
                    paths.manifest_path().display()
                )
            })?;
            let journal = ReceiverJournal::initial(paths.dump_id().to_owned())?;
            journal.persist(paths.journal_path())?;
            journal
        };

        recover_partial_archive(paths.partial_archive_path(), journal.archive_len)?;
        let manifest_recovery_issues =
            recover_manifest_file(paths.manifest_path(), journal.manifest_boundary())?;
        let issue_journal_temp_path = paths.issue_journal_temp_path();
        if issue_journal_temp_path.exists() {
            remove_file_path(&issue_journal_temp_path).with_context(|| {
                format!(
                    "failed to remove stale receiver issue journal temp file {}",
                    issue_journal_temp_path.display()
                )
            })?;
        }

        Ok(Self {
            paths,
            journal,
            manifest_recovery_issues,
        })
    }

    pub fn persist_journal(&self) -> Result<()> {
        self.journal.persist(self.paths.journal_path())
    }

    pub fn commit_manifest_chunk(
        &mut self,
        payload: &[u8],
        last_record_seq: Option<u64>,
        last_work_ordinal: Option<u64>,
        sealed: bool,
    ) -> Result<ReceiverManifestBoundary> {
        let current = self.journal.manifest_boundary();
        if current.manifest_sealed && !sealed {
            bail!("receiver manifest cannot move from sealed back to unsealed");
        }
        if current.last_manifest_record_seq.unwrap_or(0) > last_record_seq.unwrap_or(0) {
            bail!("receiver manifest record sequence must not move backwards");
        }
        if current.last_manifest_work_ordinal.unwrap_or(0) > last_work_ordinal.unwrap_or(0) {
            bail!("receiver manifest work ordinal must not move backwards");
        }

        let encoded = encode_manifest_frame(payload, last_record_seq, last_work_ordinal, sealed);
        let mut next_boundary = ReceiverManifestBoundary {
            manifest_bytes_persisted: current
                .manifest_bytes_persisted
                .saturating_add(encoded.len() as u64),
            last_manifest_record_seq: last_record_seq,
            last_manifest_work_ordinal: last_work_ordinal,
            manifest_sealed: sealed,
        };
        validate_manifest_boundary(next_boundary)?;
        next_boundary.manifest_bytes_persisted = append_temp_file_payload(
            self.paths.manifest_path(),
            &encoded,
            ReceiverTempFileKind::Manifest,
        )?;
        validate_manifest_boundary(next_boundary)?;

        self.journal.manifest_bytes_persisted = next_boundary.manifest_bytes_persisted;
        self.journal.last_manifest_record_seq = next_boundary.last_manifest_record_seq;
        self.journal.last_manifest_work_ordinal = next_boundary.last_manifest_work_ordinal;
        self.journal.manifest_sealed = next_boundary.manifest_sealed;
        self.persist_journal()?;
        Ok(next_boundary)
    }

    pub fn resume_state(&self) -> ResumeState {
        self.journal.to_resume_state()
    }

    pub fn resume_state_v3(&self) -> ResumeStateV3 {
        self.journal.to_resume_state_v3()
    }
}

fn validate_dump_id(dump_id: &str) -> Result<()> {
    if dump_id.is_empty() {
        bail!("dump_id must not be empty");
    }
    if matches!(dump_id, "." | "..") {
        bail!("dump_id must not be '.' or '..'");
    }
    if dump_id.chars().any(|ch| {
        ch.is_ascii_control() || matches!(ch, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*')
    }) {
        bail!("dump_id must be a single safe path component");
    }

    Ok(())
}

fn validate_against_protocol(journal: &ReceiverJournal) -> Result<()> {
    let mut sink = Vec::new();
    protocol::write_message(&mut sink, &Message::ResumeState(journal.to_resume_state()))
        .context("receiver journal contains invalid resume state")
}

fn validate_manifest_boundary(boundary: ReceiverManifestBoundary) -> Result<()> {
    if boundary.last_manifest_work_ordinal.is_some() && boundary.last_manifest_record_seq.is_none()
    {
        bail!("receiver journal manifest boundary cannot carry work ordinal without record seq");
    }
    if boundary.manifest_bytes_persisted == 0
        && (boundary.last_manifest_record_seq.is_some()
            || boundary.last_manifest_work_ordinal.is_some())
    {
        bail!(
            "receiver journal manifest boundary cannot carry progress when manifest_bytes_persisted is zero"
        );
    }
    Ok(())
}

fn encode_optional_u64(value: Option<u64>) -> u64 {
    value.unwrap_or(0)
}

fn decode_optional_u64(value: u64) -> Option<u64> {
    (value != 0).then_some(value)
}

fn recover_journal_backup(path: &Path) -> Result<()> {
    let backup_path = journal_backup_path(path)?;
    if path.exists() {
        if backup_path.exists() {
            remove_file_path(&backup_path).with_context(|| {
                format!(
                    "failed to remove stale receiver journal backup {}",
                    backup_path.display()
                )
            })?;
        }
        return Ok(());
    }
    if !backup_path.exists() {
        return Ok(());
    }

    rename_path(&backup_path, path).with_context(|| {
        format!(
            "failed to restore receiver journal {} from backup {}",
            path.display(),
            backup_path.display()
        )
    })
}

pub(crate) fn journal_temp_path(path: &Path) -> Result<PathBuf> {
    let file_name = path
        .file_name()
        .context("receiver journal path must contain a file name")?;
    let mut temp_name = OsString::from(file_name);
    temp_name.push(".tmp");
    Ok(path.with_file_name(temp_name))
}

pub(crate) fn journal_backup_path(path: &Path) -> Result<PathBuf> {
    let file_name = path
        .file_name()
        .context("receiver journal path must contain a file name")?;
    let mut backup_name = OsString::from(file_name);
    backup_name.push(".bak");
    Ok(path.with_file_name(backup_name))
}

fn write_string(bytes: &mut Vec<u8>, value: &str) -> Result<()> {
    let len = u32::try_from(value.len()).context("receiver journal string field is too large")?;
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_u16(cursor: &mut Cursor<&[u8]>) -> Result<u16> {
    let mut buf = [0_u8; 2];
    cursor
        .read_exact(&mut buf)
        .context("failed to read u16 from receiver journal")?;
    Ok(u16::from_le_bytes(buf))
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut buf = [0_u8; 4];
    cursor
        .read_exact(&mut buf)
        .context("failed to read u32 from receiver journal")?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut buf = [0_u8; 8];
    cursor
        .read_exact(&mut buf)
        .context("failed to read u64 from receiver journal")?;
    Ok(u64::from_le_bytes(buf))
}

fn read_bool(cursor: &mut Cursor<&[u8]>, field_name: &str) -> Result<bool> {
    let mut buf = [0_u8; 1];
    cursor
        .read_exact(&mut buf)
        .with_context(|| format!("failed to read {field_name} from receiver journal"))?;
    match buf[0] {
        0 => Ok(false),
        1 => Ok(true),
        other => bail!("{field_name} in receiver journal contains invalid bool tag {other}"),
    }
}

fn read_string(cursor: &mut Cursor<&[u8]>, field_name: &str) -> Result<String> {
    let len = read_u32(cursor)? as usize;
    let mut bytes = vec![0_u8; len];
    cursor
        .read_exact(&mut bytes)
        .with_context(|| format!("failed to read {field_name} from receiver journal"))?;
    String::from_utf8(bytes)
        .with_context(|| format!("{field_name} in receiver journal is not valid utf-8"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReceiverManifestFrameHeader {
    payload_len: u32,
    checksum_u32: u32,
    last_record_seq_or_zero: u64,
    last_work_ordinal_or_zero: u64,
    flags: u16,
}

impl ReceiverManifestFrameHeader {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(MANIFEST_FRAME_FIXED_HEADER_LEN);
        bytes.extend_from_slice(&self.payload_len.to_le_bytes());
        bytes.extend_from_slice(&self.checksum_u32.to_le_bytes());
        bytes.extend_from_slice(&self.last_record_seq_or_zero.to_le_bytes());
        bytes.extend_from_slice(&self.last_work_ordinal_or_zero.to_le_bytes());
        bytes.extend_from_slice(&self.flags.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Self {
        let mut payload_len = [0_u8; 4];
        payload_len.copy_from_slice(&bytes[0..4]);
        let mut checksum_u32 = [0_u8; 4];
        checksum_u32.copy_from_slice(&bytes[4..8]);
        let mut last_record_seq_or_zero = [0_u8; 8];
        last_record_seq_or_zero.copy_from_slice(&bytes[8..16]);
        let mut last_work_ordinal_or_zero = [0_u8; 8];
        last_work_ordinal_or_zero.copy_from_slice(&bytes[16..24]);
        let mut flags = [0_u8; 2];
        flags.copy_from_slice(&bytes[24..26]);
        Self {
            payload_len: u32::from_le_bytes(payload_len),
            checksum_u32: u32::from_le_bytes(checksum_u32),
            last_record_seq_or_zero: u64::from_le_bytes(last_record_seq_or_zero),
            last_work_ordinal_or_zero: u64::from_le_bytes(last_work_ordinal_or_zero),
            flags: u16::from_le_bytes(flags),
        }
    }

    fn boundary(self, bytes_persisted: u64) -> ReceiverManifestBoundary {
        ReceiverManifestBoundary {
            manifest_bytes_persisted: bytes_persisted,
            last_manifest_record_seq: (self.flags & MANIFEST_FRAME_FLAG_HAS_RECORD_SEQ != 0)
                .then_some(self.last_record_seq_or_zero),
            last_manifest_work_ordinal: (self.flags & MANIFEST_FRAME_FLAG_HAS_WORK_ORDINAL != 0)
                .then_some(self.last_work_ordinal_or_zero),
            manifest_sealed: self.flags & MANIFEST_FRAME_FLAG_SEALED != 0,
        }
    }
}

fn manifest_frame_checksum(header: &ReceiverManifestFrameHeader, payload: &[u8]) -> u32 {
    let mut bytes = Vec::with_capacity(8 + 8 + 2 + payload.len());
    bytes.extend_from_slice(&header.last_record_seq_or_zero.to_le_bytes());
    bytes.extend_from_slice(&header.last_work_ordinal_or_zero.to_le_bytes());
    bytes.extend_from_slice(&header.flags.to_le_bytes());
    bytes.extend_from_slice(payload);
    xxh3_64(&bytes) as u32
}

fn encode_manifest_frame(
    payload: &[u8],
    last_record_seq: Option<u64>,
    last_work_ordinal: Option<u64>,
    sealed: bool,
) -> Vec<u8> {
    let mut flags = 0_u16;
    if last_record_seq.is_some() {
        flags |= MANIFEST_FRAME_FLAG_HAS_RECORD_SEQ;
    }
    if last_work_ordinal.is_some() {
        flags |= MANIFEST_FRAME_FLAG_HAS_WORK_ORDINAL;
    }
    if sealed {
        flags |= MANIFEST_FRAME_FLAG_SEALED;
    }
    let mut header = ReceiverManifestFrameHeader {
        payload_len: payload.len() as u32,
        checksum_u32: 0,
        last_record_seq_or_zero: last_record_seq.unwrap_or(0),
        last_work_ordinal_or_zero: last_work_ordinal.unwrap_or(0),
        flags,
    };
    header.checksum_u32 = manifest_frame_checksum(&header, payload);
    let mut bytes = header.encode();
    bytes.extend_from_slice(payload);
    bytes
}

fn recover_manifest_file(
    path: &Path,
    committed_boundary: ReceiverManifestBoundary,
) -> Result<Vec<ReceiverManifestRecoveryIssue>> {
    validate_manifest_boundary(committed_boundary)?;
    match fs::metadata(path) {
        Ok(metadata) => {
            let actual_len = metadata.len();
            let bytes = fs::read(path).with_context(|| {
                format!("failed to read receiver manifest store {}", path.display())
            })?;
            let mut cursor = 0_usize;
            let mut boundaries = BTreeMap::<u64, ReceiverManifestBoundary>::new();
            let mut issues = Vec::new();

            while cursor < bytes.len() {
                if bytes.len() - cursor < MANIFEST_FRAME_FIXED_HEADER_LEN {
                    issues.push(ReceiverManifestRecoveryIssue::TruncatedTail {
                        detail: format!(
                            "receiver manifest tail truncated inside frame header at offset {}",
                            cursor
                        ),
                    });
                    break;
                }

                let header = ReceiverManifestFrameHeader::decode(
                    &bytes[cursor..cursor + MANIFEST_FRAME_FIXED_HEADER_LEN],
                );
                let payload_start = cursor + MANIFEST_FRAME_FIXED_HEADER_LEN;
                let payload_end = payload_start.saturating_add(header.payload_len as usize);
                if payload_end > bytes.len() {
                    issues.push(ReceiverManifestRecoveryIssue::TruncatedTail {
                        detail: format!(
                            "receiver manifest tail truncated inside frame payload at offset {}",
                            cursor
                        ),
                    });
                    break;
                }

                let payload = &bytes[payload_start..payload_end];
                let actual_checksum = manifest_frame_checksum(&header, payload);
                if actual_checksum != header.checksum_u32 {
                    issues.push(ReceiverManifestRecoveryIssue::CorruptedTail {
                        detail: format!(
                            "receiver manifest checksum mismatch at offset {}: expected {} got {}",
                            cursor, header.checksum_u32, actual_checksum
                        ),
                    });
                    break;
                }

                let boundary = header.boundary(payload_end as u64);
                validate_manifest_boundary(boundary)?;
                boundaries.insert(boundary.manifest_bytes_persisted, boundary);
                cursor = payload_end;
            }

            let valid_len = cursor as u64;
            if committed_boundary.manifest_bytes_persisted > valid_len {
                bail!(
                    "receiver manifest is shorter than committed manifest boundary: {} < {}",
                    valid_len,
                    committed_boundary.manifest_bytes_persisted
                );
            }
            if committed_boundary.manifest_bytes_persisted != 0 {
                let Some(boundary) = boundaries.get(&committed_boundary.manifest_bytes_persisted)
                else {
                    bail!(
                        "receiver journal manifest boundary {} does not align with a framed manifest commit",
                        committed_boundary.manifest_bytes_persisted
                    );
                };
                if boundary != &committed_boundary {
                    bail!(
                        "receiver journal manifest boundary does not match framed manifest state at {} bytes",
                        committed_boundary.manifest_bytes_persisted
                    );
                }
            }

            let target_len = committed_boundary.manifest_bytes_persisted;
            if actual_len > valid_len {
                let file = OpenOptions::new().write(true).open(path).with_context(|| {
                    format!(
                        "failed to open receiver manifest store for truncation {}",
                        path.display()
                    )
                })?;
                file.set_len(valid_len).with_context(|| {
                    format!(
                        "failed to truncate corrupted receiver manifest store {} to {} bytes",
                        path.display(),
                        valid_len
                    )
                })?;
                file.sync_all().with_context(|| {
                    format!(
                        "failed to sync truncated receiver manifest store {}",
                        path.display()
                    )
                })?;
            }
            if valid_len > target_len {
                let file = OpenOptions::new().write(true).open(path).with_context(|| {
                    format!(
                        "failed to open receiver manifest store for rollback {}",
                        path.display()
                    )
                })?;
                file.set_len(target_len).with_context(|| {
                    format!(
                        "failed to roll back receiver manifest store {} to {} bytes",
                        path.display(),
                        target_len
                    )
                })?;
                file.sync_all().with_context(|| {
                    format!(
                        "failed to sync rolled back receiver manifest store {}",
                        path.display()
                    )
                })?;
            }

            Ok(issues)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if committed_boundary.manifest_bytes_persisted != 0 {
                bail!(
                    "receiver manifest store is missing while journal expects {} bytes",
                    committed_boundary.manifest_bytes_persisted
                );
            }

            let file = File::create(path).with_context(|| {
                format!(
                    "failed to create fresh receiver manifest store {}",
                    path.display()
                )
            })?;
            file.sync_all().with_context(|| {
                format!(
                    "failed to sync fresh receiver manifest store {}",
                    path.display()
                )
            })?;
            Ok(Vec::new())
        }
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed to inspect receiver manifest store {}",
                path.display()
            )
        }),
    }
}

pub(crate) fn recover_partial_archive(path: &Path, archive_len: u64) -> Result<()> {
    match fs::metadata(path) {
        Ok(metadata) => {
            let actual_len = metadata.len();
            if actual_len < archive_len {
                bail!(
                    "partial archive is shorter than committed archive_len: {} < {}",
                    actual_len,
                    archive_len
                );
            }

            if actual_len > archive_len {
                let file = OpenOptions::new().write(true).open(path).with_context(|| {
                    format!(
                        "failed to open partial archive for recovery {}",
                        path.display()
                    )
                })?;
                file.set_len(archive_len).with_context(|| {
                    format!(
                        "failed to truncate partial archive {} to {} bytes",
                        path.display(),
                        archive_len
                    )
                })?;
                file.sync_all().with_context(|| {
                    format!(
                        "failed to sync recovered partial archive {}",
                        path.display()
                    )
                })?;
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if archive_len != 0 {
                bail!(
                    "partial archive is missing while journal expects {} bytes",
                    archive_len
                );
            }

            let file = File::create(path).with_context(|| {
                format!("failed to create fresh partial archive {}", path.display())
            })?;
            file.sync_all().with_context(|| {
                format!("failed to sync fresh partial archive {}", path.display())
            })?;
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to inspect partial archive {}", path.display()));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use super::*;
    use crate::receiver::shared::test_hooks;
    use crate::receiver::test_support::*;

    fn sample_committed_journal() -> ReceiverJournal {
        ReceiverJournal {
            protocol_version: protocol::VERSION,
            dump_id: "dump-01".to_owned(),
            last_committed_frame: 2,
            last_committed_file_index: 5,
            last_committed_file_path: "root/file.txt".to_owned(),
            last_committed_work_ordinal: None,
            archive_len: 4,
            manifest_bytes_persisted: 0,
            last_manifest_record_seq: None,
            last_manifest_work_ordinal: None,
            manifest_sealed: false,
        }
    }

    fn persist_and_read_bytes(
        temp: &TestDir,
        journal: &ReceiverJournal,
    ) -> (ReceiverPaths, Vec<u8>) {
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), &journal.dump_id).unwrap();
        journal.persist(paths.journal_path()).unwrap();
        let bytes = fs::read(paths.journal_path()).unwrap();
        (paths, bytes)
    }

    fn last_file_path_offset(journal: &ReceiverJournal) -> usize {
        4 + 2 + 4 + journal.dump_id.len() + 8 + 8 + 4
    }

    #[test]
    fn receiver_paths_place_journal_next_to_partial_archive() {
        let temp = TestDir::new();

        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();

        assert_eq!(paths.data_dir(), temp.path());
        assert_eq!(paths.dump_id(), "dump-01");
        assert_eq!(
            paths.partial_archive_path(),
            temp.path().join("dump-01.tar.zst.part")
        );
        assert_eq!(
            paths.final_archive_path(),
            temp.path().join("dump-01.tar.zst")
        );
        assert_eq!(
            paths.manifest_path(),
            temp.path().join("dump-01.manifest.part")
        );
        assert_eq!(
            paths.journal_path(),
            temp.path().join("dump-01.tar.zst.part.journal")
        );
    }

    #[test]
    fn receiver_paths_reject_dump_id_with_path_separators() {
        let temp = TestDir::new();

        let error = ReceiverPaths::new(temp.path().to_path_buf(), "nested/dump").unwrap_err();

        assert!(error.to_string().contains("dump_id"));
    }

    #[test]
    fn receiver_paths_reject_dump_id_edge_cases() {
        let temp = TestDir::new();

        for dump_id in ["", ".", "..", "bad*id", "bad\x01id"] {
            let error = ReceiverPaths::new(temp.path().to_path_buf(), dump_id).unwrap_err();
            assert!(
                error.to_string().contains("dump_id"),
                "{dump_id:?}: {error}"
            );
        }
    }

    #[test]
    fn open_fresh_state_persists_initial_journal_and_creates_empty_partial_archive() {
        let temp = TestDir::new();

        let state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();

        assert_eq!(state.journal.protocol_version, protocol::VERSION);
        assert_eq!(state.journal.dump_id, "dump-01");
        assert_eq!(state.journal.last_committed_frame, 0);
        assert_eq!(state.journal.last_committed_file_index, 0);
        assert_eq!(state.journal.last_committed_file_path, "");
        assert_eq!(state.journal.archive_len, 0);
        assert_eq!(state.journal.manifest_bytes_persisted, 0);
        assert_eq!(state.journal.last_manifest_record_seq, None);
        assert_eq!(state.journal.last_manifest_work_ordinal, None);
        assert!(!state.journal.manifest_sealed);
        assert!(state.paths.journal_path().exists());
        assert!(state.paths.partial_archive_path().exists());
        assert!(state.paths.manifest_path().exists());
        assert_eq!(
            fs::metadata(state.paths.partial_archive_path())
                .unwrap()
                .len(),
            0
        );
        assert_eq!(fs::metadata(state.paths.manifest_path()).unwrap().len(), 0);
    }

    #[test]
    fn open_removes_stale_issue_journal_temp_file() {
        let temp = TestDir::new();
        let state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        let temp_issue_path = state.paths.issue_journal_temp_path();
        drop(state);
        fs::write(&temp_issue_path, b"partial-tail").unwrap();

        let reopened = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();

        assert_eq!(reopened.journal.dump_id, "dump-01");
        assert!(!temp_issue_path.exists());
        assert!(!reopened.paths.issue_journal_path().exists());
    }

    #[test]
    fn open_reloads_existing_journal_and_resume_state() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        let journal = ReceiverJournal {
            protocol_version: protocol::VERSION,
            dump_id: "dump-01".to_owned(),
            last_committed_frame: 7,
            last_committed_file_index: 15,
            last_committed_file_path: "root/file.txt".to_owned(),
            last_committed_work_ordinal: None,
            archive_len: 4,
            manifest_bytes_persisted: 0,
            last_manifest_record_seq: None,
            last_manifest_work_ordinal: None,
            manifest_sealed: false,
        };

        journal.persist(paths.journal_path()).unwrap();
        fs::write(paths.partial_archive_path(), b"abcd").unwrap();

        let state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();

        assert_eq!(state.journal, journal);
        assert_eq!(state.resume_state(), journal.to_resume_state());
    }

    #[test]
    fn persist_journal_saves_updated_receiver_state() {
        let temp = TestDir::new();

        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        state.journal.last_committed_frame = 3;
        state.journal.last_committed_file_index = 9;
        state.journal.last_committed_file_path = "root/file.bin".to_owned();
        state.journal.archive_len = 512;
        state.persist_journal().unwrap();

        let reloaded = ReceiverJournal::load(state.paths.journal_path()).unwrap();

        assert_eq!(reloaded, state.journal);
    }

    #[test]
    fn persist_and_load_round_trips_v3_work_ordinal_resume_boundary() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        let journal = ReceiverJournal {
            protocol_version: protocol::VERSION,
            dump_id: "dump-01".to_owned(),
            last_committed_frame: 4,
            last_committed_file_index: 9,
            last_committed_file_path: "root/file.bin".to_owned(),
            last_committed_work_ordinal: Some(12),
            archive_len: 512,
            manifest_bytes_persisted: 128,
            last_manifest_record_seq: Some(7),
            last_manifest_work_ordinal: Some(12),
            manifest_sealed: true,
        };

        journal.persist(paths.journal_path()).unwrap();

        let reloaded = ReceiverJournal::load(paths.journal_path()).unwrap();

        assert_eq!(reloaded, journal);
        assert_eq!(
            reloaded.archive_boundary_v3(),
            ArchiveBoundaryV3 {
                last_committed_frame: 4,
                last_committed_work_ordinal: Some(12),
                archive_len: 512,
            }
        );
        assert_eq!(
            reloaded.to_resume_state_v3(),
            ResumeStateV3 {
                manifest: protocol::ManifestBoundaryV3 {
                    manifest_bytes_persisted: 128,
                    last_record_seq: Some(7),
                    last_work_ordinal: Some(12),
                    sealed: true,
                },
                archive: ArchiveBoundaryV3 {
                    last_committed_frame: 4,
                    last_committed_work_ordinal: Some(12),
                    archive_len: 512,
                },
            }
        );
    }

    #[test]
    fn load_accepts_legacy_journal_without_v3_work_ordinal_tail() {
        let temp = TestDir::new();
        let mut journal = sample_committed_journal();
        journal.manifest_bytes_persisted = 128;
        journal.last_manifest_record_seq = Some(7);
        journal.last_manifest_work_ordinal = Some(9);
        journal.manifest_sealed = true;
        journal.last_committed_work_ordinal = Some(9);
        let (paths, mut bytes) = persist_and_read_bytes(&temp, &journal);
        bytes.truncate(bytes.len() - 8);
        fs::write(paths.journal_path(), bytes).unwrap();

        let reloaded = ReceiverJournal::load(paths.journal_path()).unwrap();

        journal.last_committed_work_ordinal = None;
        assert_eq!(reloaded, journal);
    }

    #[test]
    fn commit_manifest_chunk_persists_manifest_boundary_independently_from_archive_len() {
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        state.journal.archive_len = 17;
        fs::write(state.paths.partial_archive_path(), vec![0_u8; 17]).unwrap();
        state.persist_journal().unwrap();

        let expected = encode_manifest_frame(b"alpha", Some(3), Some(1), false);
        let boundary = state
            .commit_manifest_chunk(b"alpha", Some(3), Some(1), false)
            .unwrap();

        assert_eq!(boundary.manifest_bytes_persisted, expected.len() as u64);
        assert_eq!(boundary.last_manifest_record_seq, Some(3));
        assert_eq!(boundary.last_manifest_work_ordinal, Some(1));
        assert!(!boundary.manifest_sealed);
        assert_eq!(state.journal.archive_len, 17);
        assert_eq!(state.journal.manifest_boundary(), boundary);
        assert_eq!(fs::read(state.paths.manifest_path()).unwrap(), expected);

        let reloaded = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();

        assert_eq!(reloaded.journal.archive_len, 17);
        assert_eq!(reloaded.journal.manifest_boundary(), boundary);
        assert!(reloaded.manifest_recovery_issues.is_empty());
    }

    #[test]
    fn commit_manifest_chunk_rejects_non_monotonic_manifest_progress() {
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        let committed = encode_manifest_frame(b"alpha", Some(5), Some(2), true);

        state
            .commit_manifest_chunk(b"alpha", Some(5), Some(2), true)
            .unwrap();

        let record_error = state
            .commit_manifest_chunk(b"beta", Some(4), Some(3), true)
            .unwrap_err();
        assert!(record_error.to_string().contains("record sequence"));

        let work_error = state
            .commit_manifest_chunk(b"beta", Some(5), Some(1), true)
            .unwrap_err();
        assert!(work_error.to_string().contains("work ordinal"));

        let seal_error = state
            .commit_manifest_chunk(b"beta", Some(6), Some(3), false)
            .unwrap_err();
        assert!(seal_error.to_string().contains("sealed back to unsealed"));

        assert_eq!(fs::read(state.paths.manifest_path()).unwrap(), committed);
    }

    #[test]
    fn commit_manifest_chunk_rejects_quota_exhaustion_without_advancing_boundary() {
        let _guard = FsFailureGuard::new();
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        let committed = state
            .commit_manifest_chunk(b"alpha", Some(3), Some(1), false)
            .unwrap();
        let committed_bytes = fs::read(state.paths.manifest_path()).unwrap();

        test_hooks::set_quota_override(
            ReceiverTempFileKind::Manifest,
            committed.manifest_bytes_persisted + 8,
        );

        let error = state
            .commit_manifest_chunk(b"beta-payload", Some(4), Some(2), false)
            .unwrap_err();

        assert!(error.to_string().contains("quota exceeded"));
        assert_eq!(state.journal.manifest_boundary(), committed);
        assert_eq!(
            fs::read(state.paths.manifest_path()).unwrap(),
            committed_bytes
        );
    }

    #[test]
    fn commit_manifest_chunk_rolls_back_partial_tail_after_storage_full() {
        let _guard = FsFailureGuard::new();
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();

        test_hooks::fail_next_append_after_write(state.paths.manifest_path());

        let error = state
            .commit_manifest_chunk(b"alpha", Some(1), Some(1), false)
            .unwrap_err();

        assert!(!error.to_string().is_empty());
        assert_eq!(state.journal.manifest_bytes_persisted, 0);
        assert_eq!(state.journal.last_manifest_record_seq, None);
        assert_eq!(state.journal.last_manifest_work_ordinal, None);
        assert!(!state.journal.manifest_sealed);
        assert_eq!(fs::metadata(state.paths.manifest_path()).unwrap().len(), 0);
    }

    #[test]
    fn open_restores_journal_from_backup_when_primary_file_is_missing() {
        let temp = TestDir::new();
        let state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        let backup_path = journal_backup_path(state.paths.journal_path()).unwrap();

        fs::rename(state.paths.journal_path(), &backup_path).unwrap();

        let reopened = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();

        assert!(reopened.paths.journal_path().exists());
        assert!(!backup_path.exists());
        assert_eq!(reopened.journal.dump_id, "dump-01");
    }

    #[test]
    fn open_removes_stale_backup_when_primary_journal_exists() {
        let temp = TestDir::new();
        let state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        let backup_path = journal_backup_path(state.paths.journal_path()).unwrap();
        fs::write(&backup_path, b"stale-backup").unwrap();

        let reopened = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();

        assert_eq!(reopened.journal.dump_id, "dump-01");
        assert!(reopened.paths.journal_path().exists());
        assert!(!backup_path.exists());
    }

    #[test]
    fn load_rejects_corrupted_journal_magic() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        fs::write(paths.journal_path(), b"bad!").unwrap();

        let error = ReceiverJournal::load(paths.journal_path()).unwrap_err();

        assert!(error.to_string().contains("invalid receiver journal magic"));
    }

    #[test]
    fn load_rejects_truncated_journal_magic_field() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        fs::write(paths.journal_path(), [0x46, 0x44, 0x4D]).unwrap();

        let error = ReceiverJournal::load(paths.journal_path()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to read u32 from receiver journal")
        );
    }

    #[test]
    fn load_rejects_truncated_protocol_version_field() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        let mut bytes = JOURNAL_MAGIC.to_le_bytes().to_vec();
        bytes.push(0x01);
        fs::write(paths.journal_path(), bytes).unwrap();

        let error = ReceiverJournal::load(paths.journal_path()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to read u16 from receiver journal")
        );
    }

    #[test]
    fn load_rejects_trailing_bytes_after_valid_journal() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        let journal = ReceiverJournal::initial("dump-01").unwrap();

        journal.persist(paths.journal_path()).unwrap();
        let mut bytes = fs::read(paths.journal_path()).unwrap();
        bytes.push(0xFF);
        fs::write(paths.journal_path(), bytes).unwrap();

        let error = ReceiverJournal::load(paths.journal_path()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("receiver journal contains unexpected trailing bytes")
        );
    }

    #[test]
    fn load_rejects_unsupported_protocol_version() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        let journal = ReceiverJournal::initial("dump-01").unwrap();

        journal.persist(paths.journal_path()).unwrap();
        let mut bytes = fs::read(paths.journal_path()).unwrap();
        bytes[4..6].copy_from_slice(&(protocol::VERSION + 1).to_le_bytes());
        fs::write(paths.journal_path(), bytes).unwrap();

        let error = ReceiverJournal::load(paths.journal_path()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unsupported receiver journal protocol version")
        );
    }

    #[test]
    fn load_rejects_older_protocol_version() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        let journal = ReceiverJournal::initial("dump-01").unwrap();

        journal.persist(paths.journal_path()).unwrap();
        let mut bytes = fs::read(paths.journal_path()).unwrap();
        bytes[4..6].copy_from_slice(&protocol::VERSION.saturating_sub(1).to_le_bytes());
        fs::write(paths.journal_path(), bytes).unwrap();

        let error = ReceiverJournal::load(paths.journal_path()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unsupported receiver journal protocol version")
        );
    }

    #[test]
    fn load_rejects_invalid_utf8_in_last_committed_file_path() {
        let temp = TestDir::new();
        let journal = sample_committed_journal();
        let (paths, mut bytes) = persist_and_read_bytes(&temp, &journal);
        let path_offset = last_file_path_offset(&journal);
        bytes[path_offset] = 0xFF;
        fs::write(paths.journal_path(), bytes).unwrap();

        let error = ReceiverJournal::load(paths.journal_path()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("last_committed_file_path in receiver journal is not valid utf-8")
        );
    }

    #[test]
    fn load_rejects_truncated_last_committed_file_path() {
        let temp = TestDir::new();
        let journal = sample_committed_journal();
        let (paths, mut bytes) = persist_and_read_bytes(&temp, &journal);
        bytes
            .truncate(last_file_path_offset(&journal) + journal.last_committed_file_path.len() - 1);
        fs::write(paths.journal_path(), bytes).unwrap();

        let error = ReceiverJournal::load(paths.journal_path()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to read last_committed_file_path from receiver journal")
        );
    }

    #[test]
    fn load_rejects_truncated_archive_len_field() {
        let temp = TestDir::new();
        let journal = sample_committed_journal();
        let (paths, mut bytes) = persist_and_read_bytes(&temp, &journal);
        let archive_len_end =
            last_file_path_offset(&journal) + journal.last_committed_file_path.len() + 7;
        bytes.truncate(archive_len_end);
        fs::write(paths.journal_path(), bytes).unwrap();

        let error = ReceiverJournal::load(paths.journal_path()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to read u64 from receiver journal")
        );
    }

    #[test]
    fn persist_rejects_protocol_version_mismatch() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        let mut journal = ReceiverJournal::initial("dump-01").unwrap();
        journal.protocol_version = protocol::VERSION + 1;

        let error = journal.persist(paths.journal_path()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("receiver journal protocol version must match current protocol version")
        );
    }

    #[test]
    fn persist_rejects_invalid_resume_state_path() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        let mut journal = sample_committed_journal();
        journal.last_committed_file_path = "../escape.txt".to_owned();

        let error = journal.persist(paths.journal_path()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("receiver journal contains invalid resume state")
        );
    }

    #[test]
    fn persist_removes_stale_backup_before_replacing_existing_journal() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        let backup_path = journal_backup_path(paths.journal_path()).unwrap();
        let original = ReceiverJournal::initial("dump-01").unwrap();
        let updated = sample_committed_journal();

        original.persist(paths.journal_path()).unwrap();
        fs::write(&backup_path, b"stale-backup").unwrap();

        updated.persist(paths.journal_path()).unwrap();

        assert_eq!(
            ReceiverJournal::load(paths.journal_path()).unwrap(),
            updated
        );
        assert!(!backup_path.exists());
    }

    #[test]
    fn persist_reports_restore_failure_when_replace_and_restore_both_fail() {
        let _guard = FsFailureGuard::new();
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        let original = ReceiverJournal::initial("dump-01").unwrap();
        let updated = sample_committed_journal();
        let temp_path = journal_temp_path(paths.journal_path()).unwrap();
        let backup_path = journal_backup_path(paths.journal_path()).unwrap();

        original.persist(paths.journal_path()).unwrap();
        test_hooks::fail_next_rename(&temp_path, paths.journal_path());
        test_hooks::fail_next_rename(&backup_path, paths.journal_path());

        let error = updated.persist(paths.journal_path()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to restore previous receiver journal")
        );
        assert!(!temp_path.exists());
        assert!(!paths.journal_path().exists());
        assert_eq!(ReceiverJournal::load(&backup_path).unwrap(), original);
    }

    #[test]
    fn open_rejects_journal_dump_id_mismatch() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        let journal = ReceiverJournal {
            protocol_version: protocol::VERSION,
            dump_id: "dump-02".to_owned(),
            last_committed_frame: 0,
            last_committed_file_index: 0,
            last_committed_file_path: String::new(),
            last_committed_work_ordinal: None,
            archive_len: 0,
            manifest_bytes_persisted: 0,
            last_manifest_record_seq: None,
            last_manifest_work_ordinal: None,
            manifest_sealed: false,
        };

        journal.persist(paths.journal_path()).unwrap();
        fs::write(paths.partial_archive_path(), b"").unwrap();

        let error = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap_err();

        assert!(
            error
                .to_string()
                .contains("receiver journal dump_id mismatch")
        );
    }

    #[test]
    fn open_truncates_partial_archive_back_to_committed_archive_len() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        let journal = ReceiverJournal {
            protocol_version: protocol::VERSION,
            dump_id: "dump-01".to_owned(),
            last_committed_frame: 2,
            last_committed_file_index: 5,
            last_committed_file_path: "root/file.txt".to_owned(),
            last_committed_work_ordinal: None,
            archive_len: 4,
            manifest_bytes_persisted: 0,
            last_manifest_record_seq: None,
            last_manifest_work_ordinal: None,
            manifest_sealed: false,
        };

        journal.persist(paths.journal_path()).unwrap();
        fs::write(paths.partial_archive_path(), b"abcdef").unwrap();

        let state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();

        assert_eq!(state.journal.archive_len, 4);
        assert_eq!(
            fs::read(state.paths.partial_archive_path()).unwrap(),
            b"abcd"
        );
    }

    #[test]
    fn open_truncates_manifest_store_back_to_committed_boundary() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        let committed = encode_manifest_frame(b"alpha", Some(3), Some(1), false);
        let uncommitted = encode_manifest_frame(b"beta", Some(5), Some(2), true);
        let journal = ReceiverJournal {
            protocol_version: protocol::VERSION,
            dump_id: "dump-01".to_owned(),
            last_committed_frame: 0,
            last_committed_file_index: 0,
            last_committed_file_path: String::new(),
            last_committed_work_ordinal: None,
            archive_len: 0,
            manifest_bytes_persisted: committed.len() as u64,
            last_manifest_record_seq: Some(3),
            last_manifest_work_ordinal: Some(1),
            manifest_sealed: false,
        };

        journal.persist(paths.journal_path()).unwrap();
        fs::write(
            paths.manifest_path(),
            [committed.clone(), uncommitted].concat(),
        )
        .unwrap();

        let state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();

        assert_eq!(state.manifest_recovery_issues.len(), 0);
        assert_eq!(fs::read(state.paths.manifest_path()).unwrap(), committed);
    }

    #[test]
    fn open_truncates_truncated_manifest_tail_and_reports_issue() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        let committed = encode_manifest_frame(b"alpha", Some(3), Some(1), false);
        let uncommitted = encode_manifest_frame(b"beta", Some(5), Some(2), true);
        let truncated = &uncommitted[..uncommitted.len() - 2];
        let journal = ReceiverJournal {
            protocol_version: protocol::VERSION,
            dump_id: "dump-01".to_owned(),
            last_committed_frame: 0,
            last_committed_file_index: 0,
            last_committed_file_path: String::new(),
            last_committed_work_ordinal: None,
            archive_len: 0,
            manifest_bytes_persisted: committed.len() as u64,
            last_manifest_record_seq: Some(3),
            last_manifest_work_ordinal: Some(1),
            manifest_sealed: false,
        };

        journal.persist(paths.journal_path()).unwrap();
        fs::write(
            paths.manifest_path(),
            [committed.clone(), truncated.to_vec()].concat(),
        )
        .unwrap();

        let state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();

        assert_eq!(fs::read(state.paths.manifest_path()).unwrap(), committed);
        assert_eq!(state.manifest_recovery_issues.len(), 1);
        assert!(matches!(
            &state.manifest_recovery_issues[0],
            ReceiverManifestRecoveryIssue::TruncatedTail { detail }
            if detail.contains("truncated")
        ));
    }

    #[test]
    fn open_truncates_corrupted_manifest_tail_and_reports_issue() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        let committed = encode_manifest_frame(b"alpha", Some(3), Some(1), false);
        let mut corrupted = encode_manifest_frame(b"beta", Some(5), Some(2), true);
        let last_byte = corrupted.len() - 1;
        corrupted[last_byte] ^= 0x55;
        let journal = ReceiverJournal {
            protocol_version: protocol::VERSION,
            dump_id: "dump-01".to_owned(),
            last_committed_frame: 0,
            last_committed_file_index: 0,
            last_committed_file_path: String::new(),
            last_committed_work_ordinal: None,
            archive_len: 0,
            manifest_bytes_persisted: committed.len() as u64,
            last_manifest_record_seq: Some(3),
            last_manifest_work_ordinal: Some(1),
            manifest_sealed: false,
        };

        journal.persist(paths.journal_path()).unwrap();
        fs::write(
            paths.manifest_path(),
            [committed.clone(), corrupted].concat(),
        )
        .unwrap();

        let state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();

        assert_eq!(fs::read(state.paths.manifest_path()).unwrap(), committed);
        assert_eq!(state.manifest_recovery_issues.len(), 1);
        assert!(matches!(
            &state.manifest_recovery_issues[0],
            ReceiverManifestRecoveryIssue::CorruptedTail { detail }
            if detail.contains("checksum mismatch")
        ));
    }

    #[test]
    fn open_rejects_manifest_store_shorter_than_committed_boundary() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        let committed = encode_manifest_frame(b"alpha", Some(3), Some(1), false);
        let journal = ReceiverJournal {
            protocol_version: protocol::VERSION,
            dump_id: "dump-01".to_owned(),
            last_committed_frame: 0,
            last_committed_file_index: 0,
            last_committed_file_path: String::new(),
            last_committed_work_ordinal: None,
            archive_len: 0,
            manifest_bytes_persisted: committed.len() as u64,
            last_manifest_record_seq: Some(3),
            last_manifest_work_ordinal: Some(1),
            manifest_sealed: false,
        };

        journal.persist(paths.journal_path()).unwrap();
        fs::write(paths.manifest_path(), &committed[..committed.len() - 1]).unwrap();

        let error = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap_err();

        assert!(
            error
                .to_string()
                .contains("receiver manifest is shorter than committed manifest boundary")
        );
    }

    #[test]
    fn open_rejects_partial_archive_shorter_than_committed_len() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        let journal = ReceiverJournal {
            protocol_version: protocol::VERSION,
            dump_id: "dump-01".to_owned(),
            last_committed_frame: 1,
            last_committed_file_index: 1,
            last_committed_file_path: "root/file.txt".to_owned(),
            last_committed_work_ordinal: None,
            archive_len: 5,
            manifest_bytes_persisted: 0,
            last_manifest_record_seq: None,
            last_manifest_work_ordinal: None,
            manifest_sealed: false,
        };

        journal.persist(paths.journal_path()).unwrap();
        fs::write(paths.partial_archive_path(), b"abcd").unwrap();

        let error = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap_err();

        assert!(
            error
                .to_string()
                .contains("partial archive is shorter than committed archive_len")
        );
    }

    #[test]
    fn open_rejects_missing_partial_archive_when_journal_expects_bytes() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        let journal = ReceiverJournal {
            protocol_version: protocol::VERSION,
            dump_id: "dump-01".to_owned(),
            last_committed_frame: 1,
            last_committed_file_index: 1,
            last_committed_file_path: "root/file.txt".to_owned(),
            last_committed_work_ordinal: None,
            archive_len: 5,
            manifest_bytes_persisted: 0,
            last_manifest_record_seq: None,
            last_manifest_work_ordinal: None,
            manifest_sealed: false,
        };

        journal.persist(paths.journal_path()).unwrap();

        let error = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap_err();

        assert!(
            error
                .to_string()
                .contains("partial archive is missing while journal expects")
        );
    }

    #[test]
    fn open_rejects_orphaned_partial_archive_without_journal() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        fs::write(paths.partial_archive_path(), b"orphan").unwrap();

        let error = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap_err();

        assert!(
            error
                .to_string()
                .contains("partial archive exists without journal")
        );
    }

    #[test]
    fn open_rejects_existing_final_archive_for_same_dump_id() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        fs::write(paths.final_archive_path(), b"final").unwrap();

        let error = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap_err();

        assert!(
            error
                .to_string()
                .contains("final archive already exists for dump_id")
        );
    }

    #[test]
    fn journal_temp_and_backup_paths_require_file_name() {
        let error = journal_temp_path(Path::new("")).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("receiver journal path must contain a file name")
        );

        let error = journal_backup_path(Path::new("")).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("receiver journal path must contain a file name")
        );
    }
}

use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::network::protocol::{self, Message, ResumeState};

use super::shared::{remove_file_path, rename_path};

// Отдельный magic помогает не перепутать journal с частичным архивом
// или с произвольным мусором на диске.
const JOURNAL_MAGIC: u32 = 0x4A4D4446;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverPaths {
    data_dir: PathBuf,
    dump_id: String,
    partial_archive_path: PathBuf,
    final_archive_path: PathBuf,
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

    pub fn journal_path(&self) -> &Path {
        &self.journal_path
    }

    pub fn issue_journal_path(&self) -> &Path {
        &self.issue_journal_path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverJournal {
    pub protocol_version: u16,
    pub dump_id: String,
    pub last_committed_frame: u64,
    pub last_committed_file_index: u64,
    pub last_committed_file_path: String,
    pub archive_len: u64,
}

impl ReceiverJournal {
    pub fn initial(dump_id: impl Into<String>) -> Result<Self> {
        let journal = Self {
            protocol_version: protocol::VERSION,
            dump_id: dump_id.into(),
            last_committed_frame: 0,
            last_committed_file_index: 0,
            last_committed_file_path: String::new(),
            archive_len: 0,
        };
        validate_dump_id(&journal.dump_id)?;
        validate_against_protocol(&journal)?;
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

        let journal = Self {
            protocol_version: read_u16(&mut reader)?,
            dump_id: read_string(&mut reader, "dump_id")?,
            last_committed_frame: read_u64(&mut reader)?,
            last_committed_file_index: read_u64(&mut reader)?,
            last_committed_file_path: read_string(&mut reader, "last_committed_file_path")?,
            archive_len: read_u64(&mut reader)?,
        };

        if journal.protocol_version != protocol::VERSION {
            bail!(
                "unsupported receiver journal protocol version: {}",
                journal.protocol_version
            );
        }
        validate_dump_id(&journal.dump_id)?;
        validate_against_protocol(&journal)?;

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverState {
    pub paths: ReceiverPaths,
    pub journal: ReceiverJournal,
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

            let journal = ReceiverJournal::initial(paths.dump_id().to_owned())?;
            journal.persist(paths.journal_path())?;
            journal
        };

        recover_partial_archive(paths.partial_archive_path(), journal.archive_len)?;

        Ok(Self { paths, journal })
    }

    pub fn persist_journal(&self) -> Result<()> {
        self.journal.persist(self.paths.journal_path())
    }

    pub fn resume_state(&self) -> ResumeState {
        self.journal.to_resume_state()
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

fn read_string(cursor: &mut Cursor<&[u8]>, field_name: &str) -> Result<String> {
    let len = read_u32(cursor)? as usize;
    let mut bytes = vec![0_u8; len];
    cursor
        .read_exact(&mut bytes)
        .with_context(|| format!("failed to read {field_name} from receiver journal"))?;
    String::from_utf8(bytes)
        .with_context(|| format!("{field_name} in receiver journal is not valid utf-8"))
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
            archive_len: 4,
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
        assert!(state.paths.journal_path().exists());
        assert!(state.paths.partial_archive_path().exists());
        assert_eq!(
            fs::metadata(state.paths.partial_archive_path())
                .unwrap()
                .len(),
            0
        );
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
            archive_len: 4,
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
        bytes.pop();
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
            archive_len: 0,
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
            archive_len: 4,
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
    fn open_rejects_partial_archive_shorter_than_committed_len() {
        let temp = TestDir::new();
        let paths = ReceiverPaths::new(temp.path().to_path_buf(), "dump-01").unwrap();
        let journal = ReceiverJournal {
            protocol_version: protocol::VERSION,
            dump_id: "dump-01".to_owned(),
            last_committed_frame: 1,
            last_committed_file_index: 1,
            last_committed_file_path: "root/file.txt".to_owned(),
            archive_len: 5,
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
            archive_len: 5,
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

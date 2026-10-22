use std::fs::{self, File};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::network::protocol::{BundleDescriptor, ResumeState, SkippedEntry, validate_dump_id};

#[cfg(test)]
use crate::network::protocol::PreparedBundle;

use super::archive::ArchiveAppender;
use super::wal::{WalRecord, WalWriter, wal_record_from_descriptor};
use super::{ReceiverPaths, catalog, recovery};

#[derive(Debug, Clone)]
pub(crate) struct OfflineReceiveReport {
    pub(crate) final_archive_path: PathBuf,
    pub(crate) issue_report_path: Option<PathBuf>,
    pub(crate) resume_state: ResumeState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct FinalizeProfile {
    pub(crate) total_duration: Duration,
    pub(crate) archive_finalize_duration: Duration,
    pub(crate) issue_finalize_duration: Duration,
}

struct FileLock(File);

impl Drop for FileLock {
    fn drop(&mut self) {
        // После fork/try_clone другой fd может держать ту же open file description.
        // Одного close недостаточно: освобождаем lock до закрытия нашего handle.
        while let Err(error) = self.0.unlock() {
            if error.kind() != std::io::ErrorKind::Interrupted {
                break;
            }
        }
    }
}

pub(crate) struct ReceiverSession {
    paths: ReceiverPaths,
    archive: ArchiveAppender,
    wal: WalWriter,
    resume_state: ResumeState,
    compression_level: i32,
    // Последнее поле закрывается после archive/WAL; lock-файл не удаляем,
    // иначе новый процесс может захватить другой inode под тем же именем.
    _dump_lock: FileLock,
}

impl ReceiverSession {
    pub(crate) fn open(data_dir: PathBuf, dump_id: &str, compression_level: i32) -> Result<Self> {
        validate_dump_id(dump_id)?;
        let paths = ReceiverPaths::new(data_dir, dump_id);
        fs::create_dir_all(&paths.data_dir).with_context(|| {
            format!(
                "failed to create v4 receiver directory {}",
                paths.data_dir.display()
            )
        })?;
        // Открытия сериализуются лишь до завершения migration/recovery: marker
        // соседнего dump_id с суффиксом .v4 не должен появиться между проверкой
        // старых имён и rename. Весь transfer держит только собственный dump lock.
        let open_lock_path = paths.data_dir.join(".fast-dump-open-lock");
        let open_lock = File::options()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&open_lock_path)
            .with_context(|| {
                format!(
                    "failed to open v4 receiver initialization lock {}",
                    open_lock_path.display()
                )
            })?;
        open_lock.lock().with_context(|| {
            format!(
                "failed to lock v4 receiver initialization {}",
                open_lock_path.display()
            )
        })?;
        let _open_lock = FileLock(open_lock);
        let lock_path = paths.data_dir.join(format!("{dump_id}.lock"));
        let dump_lock = File::options()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .with_context(|| format!("failed to open v4 dump lock {}", lock_path.display()))?;
        // Межпроцессная блокировка должна предшествовать migration и truncate.
        dump_lock.try_lock().with_context(|| {
            format!(
                "v4 dump is already active or cannot be locked: {}",
                lock_path.display()
            )
        })?;
        let dump_lock = FileLock(dump_lock);
        paths.migrate_legacy_state()?;

        if paths.final_archive_path.exists() {
            bail!(
                "completed v4 dump already exists for {} at {}",
                paths.dump_id,
                paths.final_archive_path.display()
            );
        }

        let resume_state = recovery::recover(&paths)?;
        let archive = ArchiveAppender::open(&paths, resume_state.archive_len)?;
        let wal = WalWriter::open(&paths.wal_path)?;
        // Первый durable commit требует сохранённых имён обоих partial-файлов.
        catalog::sync_parent_directory(&paths.wal_path)?;

        Ok(Self {
            paths,
            archive,
            wal,
            resume_state,
            compression_level,
            _dump_lock: dump_lock,
        })
    }

    pub(crate) fn resume_state(&self) -> &ResumeState {
        &self.resume_state
    }

    #[cfg(test)]
    pub(crate) fn record_terminal_skips(&mut self, skips: &[SkippedEntry]) -> Result<()> {
        self.record_terminal_skips_profiled(skips).map(|_| ())
    }

    pub(crate) fn record_terminal_skips_profiled(
        &mut self,
        skips: &[SkippedEntry],
    ) -> Result<Duration> {
        let started = Instant::now();
        self.resume_state.terminal_skips =
            catalog::merge_terminal_skips(&self.paths.catalog_path, skips)?;
        Ok(started.elapsed())
    }

    #[cfg(test)]
    pub(crate) fn stage_bundle(&mut self, bundle: &PreparedBundle) -> Result<()> {
        self.archive.append_chunk(&bundle.compressed_payload)?;
        self.archive.sync()?;
        Ok(())
    }

    pub(crate) const fn current_archive_len(&self) -> u64 {
        self.archive.current_len()
    }

    pub(crate) fn append_bundle_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        self.archive.append_chunk(bytes)
    }

    pub(crate) fn sync_archive(&mut self) -> Result<()> {
        self.archive.sync()
    }

    pub(crate) fn sync_wal(&mut self) -> Result<()> {
        self.wal.sync()
    }

    pub(crate) fn rollback_staged_bundle(&mut self, archive_len_before_append: u64) -> Result<()> {
        self.archive.truncate_to(archive_len_before_append)
    }

    pub(crate) fn buffered_commit_record(
        &mut self,
        descriptor: &BundleDescriptor,
        archive_len: u64,
    ) -> Result<WalRecord> {
        let record = wal_record_from_descriptor(descriptor, archive_len);
        self.wal.append_commit_record(&record)?;
        Ok(record)
    }

    pub(crate) fn apply_commit_record(&mut self, record: WalRecord) {
        self.resume_state.last_committed_bundle_no = record.bundle_no;
        self.resume_state.last_committed_work_ordinal = record.last_work_ordinal;
        self.resume_state.last_committed_relative_path = record.last_relative_path;
        self.resume_state.archive_len = record.archive_len;
    }

    #[cfg(test)]
    pub(crate) fn commit_bundle(&mut self, bundle: &PreparedBundle) -> Result<()> {
        self.archive.append_chunk(&bundle.compressed_payload)?;
        self.archive.sync()?;
        let record = self.buffered_commit_record(&bundle.descriptor, self.archive.current_len())?;
        self.sync_wal()?;
        self.apply_commit_record(record);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn finalize(self) -> Result<OfflineReceiveReport> {
        Ok(self.finalize_profiled()?.0)
    }

    pub(crate) fn finalize_profiled(mut self) -> Result<(OfflineReceiveReport, FinalizeProfile)> {
        let total_started = Instant::now();
        self.archive
            .append_termination_frame(self.compression_level)?;
        let issue_finalize_started = Instant::now();
        let issue_report_path =
            catalog::finalize_issue_report(&self.paths, &self.resume_state.terminal_skips)?;
        let issue_finalize_duration = issue_finalize_started.elapsed();
        let archive_finalize_started = Instant::now();
        let final_archive_path = self.archive.finalize()?;
        let archive_finalize_duration = archive_finalize_started.elapsed();
        // WAL сохраняет возможность resume до появления готового архива.
        // После rename оставшийся WAL означает лишь незавершённую уборку.
        Self::cleanup_partial_state(&self.paths)?;
        Ok((
            OfflineReceiveReport {
                final_archive_path,
                issue_report_path,
                resume_state: self.resume_state,
            },
            FinalizeProfile {
                total_duration: total_started.elapsed(),
                archive_finalize_duration,
                issue_finalize_duration,
            },
        ))
    }

    fn cleanup_partial_state(paths: &ReceiverPaths) -> Result<()> {
        for path in [
            &paths.wal_path,
            &paths.catalog_path,
            &paths.issues_part_path,
        ] {
            if path.exists() {
                fs::remove_file(path).with_context(|| {
                    format!(
                        "failed to remove stale v4 receiver state {}",
                        path.display()
                    )
                })?;
            }
        }
        catalog::sync_parent_directory(&paths.wal_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::TestDir;

    #[test]
    fn session_drop_unlocks_even_while_inherited_descriptor_remains_open() {
        let directory = TestDir::new();
        let receiver = ReceiverSession::open(directory.path().to_path_buf(), "guard", 1).unwrap();
        // try_clone моделирует fd, который другой thread унаследовал через fork.
        let inherited = receiver._dump_lock.0.try_clone().unwrap();
        assert!(ReceiverSession::open(directory.path().to_path_buf(), "guard", 1).is_err());
        drop(receiver);

        let reopened = ReceiverSession::open(directory.path().to_path_buf(), "guard", 1).unwrap();
        assert!(inherited.metadata().is_ok());
        assert_eq!(reopened.resume_state().last_committed_bundle_no, 0);
    }

    #[test]
    fn initialization_guard_drop_unlocks_even_while_inherited_descriptor_remains_open() {
        let directory = TestDir::new();
        let path = directory.path().join(".fast-dump-open-lock");
        let file = File::create(&path).unwrap();
        file.lock().unwrap();
        let guard = FileLock(file);
        let inherited = guard.0.try_clone().unwrap();
        let contender = File::options().read(true).write(true).open(&path).unwrap();
        assert!(matches!(
            contender.try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
        drop(guard);

        contender.try_lock().unwrap();
        let _replacement_guard = FileLock(contender);
        assert!(inherited.metadata().is_ok());
    }
}

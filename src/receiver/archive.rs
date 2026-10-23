use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};

use crate::sender::compress_tar_termination_frame;

use super::ReceiverPaths;

pub(crate) struct ArchiveAppender {
    part_path: PathBuf,
    final_path: PathBuf,
    file: File,
    current_len: u64,
    unsynced_bytes: u64,
    dirty: bool,
}

impl ArchiveAppender {
    pub(crate) fn open(paths: &ReceiverPaths, committed_len: u64) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(committed_len == 0)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&paths.archive_part_path)
            .with_context(|| {
                format!(
                    "failed to open v4 archive part {}",
                    paths.archive_part_path.display()
                )
            })?;
        let archive_len = file.metadata()?.len();
        ensure!(
            archive_len >= committed_len,
            "v4 archive is shorter than durable WAL boundary: {} < {}",
            archive_len,
            committed_len,
        );
        if archive_len != committed_len {
            file.set_len(committed_len).with_context(|| {
                format!(
                    "failed to truncate v4 archive part {}",
                    paths.archive_part_path.display()
                )
            })?;
        }
        file.seek(SeekFrom::Start(committed_len))?;
        Ok(Self {
            part_path: paths.archive_part_path.clone(),
            final_path: paths.final_archive_path.clone(),
            file,
            current_len: committed_len,
            unsynced_bytes: 0,
            // Нулевая граница допускает только что созданный файл; усечение
            // меняет metadata даже при отсутствии новых payload-байт.
            dirty: committed_len == 0 || archive_len != committed_len,
        })
    }

    pub(crate) const fn current_len(&self) -> u64 {
        self.current_len
    }

    pub(crate) fn append_chunk(&mut self, bytes: &[u8]) -> Result<Option<Duration>> {
        // write_all может записать часть данных до ошибки: dirty ставим заранее.
        self.dirty |= !bytes.is_empty();
        #[cfg(test)]
        super::storage_faults::write_failure(
            super::storage_faults::Point::ArchiveWrite,
            &mut self.file,
            bytes,
        )?;
        self.file.write_all(bytes)?;
        self.current_len = self.current_len.saturating_add(bytes.len() as u64);
        self.unsynced_bytes = self.unsynced_bytes.saturating_add(bytes.len() as u64);
        // Равномерно выталкиваем большой поток, пока sender ещё сжимает данные.
        // Это не commit: resumable граница меняется только после sync WAL.
        if self.unsynced_bytes >= 64 * 1024 * 1024 {
            let started = Instant::now();
            self.sync()?;
            return Ok(Some(started.elapsed()));
        }
        Ok(None)
    }

    pub(crate) fn sync(&mut self) -> Result<bool> {
        if !self.dirty {
            return Ok(false);
        }
        #[cfg(test)]
        super::storage_faults::check(super::storage_faults::Point::ArchiveSync)?;
        self.file.sync_all().with_context(|| {
            format!(
                "failed to sync v4 archive part {}",
                self.part_path.display()
            )
        })?;
        self.unsynced_bytes = 0;
        self.dirty = false;
        Ok(true)
    }

    pub(crate) fn truncate_to(&mut self, len: u64) -> Result<()> {
        self.dirty = true;
        self.file.set_len(len).with_context(|| {
            format!(
                "failed to truncate v4 archive part {}",
                self.part_path.display()
            )
        })?;
        self.file.seek(SeekFrom::Start(len))?;
        self.current_len = len;
        self.unsynced_bytes = 0;
        Ok(())
    }

    pub(crate) fn append_termination_frame(&mut self, compression_level: i32) -> Result<()> {
        let termination_frame = compress_tar_termination_frame(compression_level)?;
        self.append_chunk(&termination_frame).map(|_| ())
    }

    pub(crate) fn finalize(mut self) -> Result<PathBuf> {
        ensure!(
            !self.final_path.exists(),
            "refusing to overwrite completed v4 archive {}",
            self.final_path.display()
        );
        self.sync()?;
        drop(self.file);

        #[cfg(test)]
        super::storage_faults::check(super::storage_faults::Point::ArchiveRename)?;
        fs::rename(&self.part_path, &self.final_path).with_context(|| {
            format!(
                "failed to finalize v4 archive {} -> {}",
                self.part_path.display(),
                self.final_path.display()
            )
        })?;
        // WAL можно удалять только после сохранения имени готового архива.
        #[cfg(test)]
        super::storage_faults::check(super::storage_faults::Point::ArchiveParentSync)?;
        super::catalog::sync_parent_directory(&self.final_path)?;
        Ok(self.final_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::TestDir;

    #[test]
    fn sync_skips_only_clean_archive_and_preserves_truncation_durability() {
        let directory = TestDir::new();
        let paths = ReceiverPaths::new(directory.path().to_path_buf(), "dirty");
        let mut archive = ArchiveAppender::open(&paths, 0).unwrap();
        assert!(archive.sync().unwrap(), "creation must become durable");
        assert!(!archive.sync().unwrap());
        assert!(archive.append_chunk(&[]).unwrap().is_none());
        assert!(!archive.sync().unwrap());
        archive.append_chunk(b"abcdefgh").unwrap();
        assert!(archive.sync().unwrap());
        assert!(!archive.sync().unwrap());
        drop(archive);

        let mut archive = ArchiveAppender::open(&paths, 8).unwrap();
        assert!(
            !archive.sync().unwrap(),
            "unchanged durable boundary is clean"
        );
        archive.truncate_to(4).unwrap();
        assert!(
            archive.sync().unwrap(),
            "rollback changed metadata without new bytes"
        );
        assert!(!archive.sync().unwrap());
        drop(archive);

        let mut archive = ArchiveAppender::open(&paths, 2).unwrap();
        assert!(
            archive.sync().unwrap(),
            "recovery truncation must become durable"
        );
        assert!(!archive.sync().unwrap());
        assert_eq!(fs::read(&paths.archive_part_path).unwrap(), b"ab");
    }

    #[test]
    fn periodic_sync_can_satisfy_commit_sync_until_next_write() {
        let directory = TestDir::new();
        let paths = ReceiverPaths::new(directory.path().to_path_buf(), "periodic");
        let mut archive = ArchiveAppender::open(&paths, 0).unwrap();
        let chunk = vec![0xa5; 4 * 1024 * 1024];
        for index in 0..16 {
            let periodic = archive.append_chunk(&chunk).unwrap();
            assert_eq!(periodic.is_some(), index == 15);
        }
        assert!(
            !archive.sync().unwrap(),
            "the whole commit group is already durable"
        );
        archive.append_chunk(b"tail").unwrap();
        assert!(
            archive.sync().unwrap(),
            "a new tail still requires sync before WAL"
        );
        archive.truncate_to(0).unwrap();
        assert!(archive.sync().unwrap());
    }
}

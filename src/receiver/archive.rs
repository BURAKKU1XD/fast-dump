use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, ensure};

use crate::sender::compress_tar_termination_frame;

use super::ReceiverPaths;

pub(crate) struct ArchiveAppender {
    part_path: PathBuf,
    final_path: PathBuf,
    file: File,
    current_len: u64,
    unsynced_bytes: u64,
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
        })
    }

    pub(crate) const fn current_len(&self) -> u64 {
        self.current_len
    }

    pub(crate) fn append_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        self.file.write_all(bytes)?;
        self.current_len = self.current_len.saturating_add(bytes.len() as u64);
        self.unsynced_bytes = self.unsynced_bytes.saturating_add(bytes.len() as u64);
        // Равномерно выталкиваем большой поток, пока sender ещё сжимает данные.
        // Это не commit: resumable граница меняется только после sync WAL.
        if self.unsynced_bytes >= 64 * 1024 * 1024 {
            self.sync()?;
        }
        Ok(())
    }

    pub(crate) fn sync(&mut self) -> Result<()> {
        self.file.sync_all().with_context(|| {
            format!(
                "failed to sync v4 archive part {}",
                self.part_path.display()
            )
        })?;
        self.unsynced_bytes = 0;
        Ok(())
    }

    pub(crate) fn truncate_to(&mut self, len: u64) -> Result<()> {
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
        self.append_chunk(&termination_frame)
    }

    pub(crate) fn finalize(mut self) -> Result<PathBuf> {
        ensure!(
            !self.final_path.exists(),
            "refusing to overwrite completed v4 archive {}",
            self.final_path.display()
        );
        self.sync()?;
        drop(self.file);

        fs::rename(&self.part_path, &self.final_path).with_context(|| {
            format!(
                "failed to finalize v4 archive {} -> {}",
                self.part_path.display(),
                self.final_path.display()
            )
        })?;
        // WAL можно удалять только после сохранения имени готового архива.
        super::catalog::sync_parent_directory(&self.final_path)?;
        Ok(self.final_path)
    }
}

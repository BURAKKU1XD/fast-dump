use std::path::PathBuf;

use anyhow::{Result, bail};

#[derive(Debug, Clone)]
pub(crate) struct ReceiverPaths {
    pub(crate) data_dir: PathBuf,
    pub(crate) dump_id: String,
    pub(crate) archive_part_path: PathBuf,
    pub(crate) final_archive_path: PathBuf,
    pub(crate) wal_path: PathBuf,
    pub(crate) catalog_path: PathBuf,
    pub(crate) issues_part_path: PathBuf,
    pub(crate) issues_final_path: PathBuf,
}

#[derive(Debug, Clone)]
struct LegacyReceiverPaths {
    archive_part_path: PathBuf,
    final_archive_path: PathBuf,
    wal_path: PathBuf,
    catalog_path: PathBuf,
    issues_part_path: PathBuf,
    issues_final_path: PathBuf,
}

impl ReceiverPaths {
    pub(crate) fn new(data_dir: PathBuf, dump_id: &str) -> Self {
        Self {
            archive_part_path: data_dir.join(format!("{dump_id}.tar.zst.part")),
            final_archive_path: data_dir.join(format!("{dump_id}.tar.zst")),
            wal_path: data_dir.join(format!("{dump_id}.wal.part")),
            catalog_path: data_dir.join(format!("{dump_id}.catalog.part")),
            issues_part_path: data_dir.join(format!("{dump_id}.tar.zst.issues.tsv.part")),
            issues_final_path: data_dir.join(format!("{dump_id}.tar.zst.issues.tsv")),
            data_dir,
            dump_id: dump_id.to_owned(),
        }
    }

    pub(crate) fn migrate_legacy_state(&self) -> Result<()> {
        // Сохранённый marker отличает canonical dump "foo.v4" от legacy "foo".
        // Вызывающий держит общий open-lock, поэтому соседняя сессия не может
        // создать marker между этой проверкой и переименованием старых файлов.
        if self
            .data_dir
            .join(format!("{}.v4.lock", self.dump_id))
            .try_exists()?
        {
            return Ok(());
        }
        let legacy = self.legacy_paths();
        for (legacy_path, canonical_path, label) in [
            (
                &legacy.archive_part_path,
                &self.archive_part_path,
                "archive part",
            ),
            (
                &legacy.final_archive_path,
                &self.final_archive_path,
                "final archive",
            ),
            (&legacy.wal_path, &self.wal_path, "wal"),
            (&legacy.catalog_path, &self.catalog_path, "catalog"),
            (
                &legacy.issues_part_path,
                &self.issues_part_path,
                "issues part",
            ),
            (
                &legacy.issues_final_path,
                &self.issues_final_path,
                "issues final",
            ),
        ] {
            migrate_legacy_path(legacy_path, canonical_path, label)?;
        }
        Ok(())
    }

    fn legacy_paths(&self) -> LegacyReceiverPaths {
        LegacyReceiverPaths {
            archive_part_path: self
                .data_dir
                .join(format!("{}.v4.tar.zst.part", self.dump_id)),
            final_archive_path: self.data_dir.join(format!("{}.v4.tar.zst", self.dump_id)),
            wal_path: self.data_dir.join(format!("{}.v4.wal.part", self.dump_id)),
            catalog_path: self
                .data_dir
                .join(format!("{}.v4.catalog.part", self.dump_id)),
            issues_part_path: self
                .data_dir
                .join(format!("{}.v4.tar.zst.issues.tsv.part", self.dump_id)),
            issues_final_path: self
                .data_dir
                .join(format!("{}.v4.tar.zst.issues.tsv", self.dump_id)),
        }
    }
}

fn migrate_legacy_path(legacy_path: &PathBuf, canonical_path: &PathBuf, label: &str) -> Result<()> {
    if !legacy_path.exists() {
        return Ok(());
    }
    if canonical_path.exists() {
        bail!(
            "found both legacy and canonical {} paths during cutover migration: {} and {}",
            label,
            legacy_path.display(),
            canonical_path.display()
        );
    }
    std::fs::rename(legacy_path, canonical_path)?;
    Ok(())
}

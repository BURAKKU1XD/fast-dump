use std::borrow::Cow;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::network::protocol::{SkipReason, SkippedEntry};
use crate::sender::source_policy::{self, ExcludedFilesystem};
use anyhow::{Context, Result, bail};

#[derive(Debug, Clone)]
pub(super) struct DiscoveredEntry {
    pub(super) abs_path: PathBuf,
    pub(super) relative_path: String,
    pub(super) file_type: fs::FileType,
    pub(super) ambiguous_name: bool,
}

pub(super) enum SourceDirectory {
    Entries(Vec<DiscoveredEntry>),
    Excluded(ExcludedFilesystem),
}

pub(super) fn validate_source_root(root: &Path) -> Result<()> {
    if !root.exists() {
        bail!("v4 sender source does not exist: {}", root.display());
    }
    if !root.is_dir() {
        bail!("v4 sender source is not a directory: {}", root.display());
    }
    Ok(())
}

pub(super) fn read_source_root(
    root: &Path,
    cancelled: Option<&AtomicBool>,
) -> Result<Vec<DiscoveredEntry>> {
    match read_source_directory(root, "", cancelled)? {
        SourceDirectory::Entries(entries) => Ok(entries),
        SourceDirectory::Excluded(filesystem) => bail!(
            "v4 sender source {} is on excluded filesystem {}: {}",
            root.display(),
            filesystem.name(),
            filesystem.reason(),
        ),
    }
}

pub(super) fn read_source_directory(
    directory: &Path,
    relative_path: &str,
    cancelled: Option<&AtomicBool>,
) -> Result<SourceDirectory> {
    anyhow::ensure!(
        !cancelled.is_some_and(|flag| flag.load(Ordering::Relaxed)),
        "source scan cancelled"
    );
    // Проверяем каждый вход в каталог: путь может быть отдельным или bind mount.
    // Неизвестную из-за ошибки syscall FS нельзя молча считать разрешённой.
    if let Some(filesystem) = source_policy::excluded_directory(directory).with_context(|| {
        format!(
            "failed to classify filesystem of v4 source directory {}",
            directory.display()
        )
    })? {
        return Ok(SourceDirectory::Excluded(filesystem));
    }
    read_sorted_entries_cancellable(directory, relative_path, cancelled)
        .map(SourceDirectory::Entries)
}

pub(super) fn excluded_directory_entry(relative_path: String) -> SkippedEntry {
    SkippedEntry {
        work_ordinal: 0,
        relative_path,
        reason: SkipReason::UnsupportedEntry,
    }
}

#[cfg(test)]
pub(super) fn read_sorted_entries(dir: &Path, rel_dir: &str) -> Result<Vec<DiscoveredEntry>> {
    read_sorted_entries_cancellable(dir, rel_dir, None)
}

pub(super) fn read_sorted_entries_cancellable(
    dir: &Path,
    rel_dir: &str,
    cancelled: Option<&AtomicBool>,
) -> Result<Vec<DiscoveredEntry>> {
    let mut entries = fs::read_dir(dir)
        .with_context(|| format!("failed to read v4 source directory {}", dir.display()))?
        .map(|entry| -> Result<_> {
            anyhow::ensure!(
                !cancelled.is_some_and(|flag| flag.load(Ordering::Relaxed)),
                "source scan cancelled"
            );
            let entry = entry
                .with_context(|| format!("failed to read v4 source entry in {}", dir.display()))?;
            #[cfg(windows)]
            let native_name = entry.file_name();
            #[cfg(windows)]
            let abs_path = joined_scan_entry_path(dir, &native_name);
            #[cfg(windows)]
            let name = native_name.to_string_lossy();
            #[cfg(not(windows))]
            let abs_path = entry.path();
            // Unix DirEntry::path заимствует native basename внутри std; отдельный
            // file_name здесь создал бы дополнительный OsString.
            #[cfg(not(windows))]
            let name = abs_path
                .file_name()
                .context("v4 source entry path has no file name")?
                .to_string_lossy();
            let ambiguous_name = matches!(&name, Cow::Owned(_)) || name.contains('\\');
            let relative_path = relative_entry_path(rel_dir, name);
            Ok(DiscoveredEntry {
                abs_path,
                relative_path,
                file_type: entry
                    .file_type()
                    .with_context(|| "failed to resolve v4 source file type")?,
                ambiguous_name,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let prefix_len = rel_dir.len() + usize::from(!rel_dir.is_empty());
    sort_directory_entries(&mut entries, prefix_len);
    Ok(entries)
}

#[cfg(windows)]
pub(super) fn joined_scan_entry_path(dir: &Path, name: &std::ffi::OsStr) -> PathBuf {
    // Длины OsStr измерены в единицах capacity, без повторного UTF-16 преобразования.
    // Обычному пути хватает одной allocation; push сохраняет семантику verbatim roots.
    let mut path = PathBuf::with_capacity(dir.as_os_str().len() + 1 + name.len());
    path.push(dir);
    path.push(name);
    path
}

pub(super) fn sort_directory_entries(entries: &mut [DiscoveredEntry], prefix_len: usize) {
    // Префикс каталога у всех ключей одинаков. Stable sort сохраняет прежний
    // порядок равных нормализованных имён, включая lossy Unicode collisions.
    entries.sort_by(|left, right| {
        right.relative_path.as_bytes()[prefix_len..]
            .cmp(&left.relative_path.as_bytes()[prefix_len..])
    });
}

pub(super) fn relative_entry_path(rel_dir: &str, name: Cow<'_, str>) -> String {
    let needs_normalization = name.contains('\\');
    if rel_dir.is_empty() && !needs_normalization {
        return name.into_owned();
    }
    let mut path =
        String::with_capacity(rel_dir.len() + usize::from(!rel_dir.is_empty()) + name.len());
    if !rel_dir.is_empty() {
        path.push_str(rel_dir);
        path.push('/');
    }
    if needs_normalization {
        // Нормализация сразу пишет в итоговую строку, без временного replace-буфера.
        let mut components = name.split('\\');
        path.push_str(components.next().unwrap_or_default());
        for component in components {
            path.push('/');
            path.push_str(component);
        }
    } else {
        path.push_str(&name);
    }
    path
}

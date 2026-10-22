use std::borrow::Cow;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;

use anyhow::{Context, Result, bail};
use crossbeam_channel::bounded;

use crate::network::protocol::{SkipReason, SkippedEntry};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScannedEntryKind {
    Regular,
    Symlink,
}

#[derive(Debug, Clone)]
pub(crate) struct ScannedEntry {
    pub(crate) work_ordinal: u64,
    pub(crate) abs_path: PathBuf,
    pub(crate) relative_path: String,
    pub(crate) kind: ScannedEntryKind,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ScanReport {
    pub(crate) entries: Vec<ScannedEntry>,
    pub(crate) skipped: Vec<SkippedEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceScanReport {
    pub pending_files: u64,
    pub pending_bytes: u64,
    pub final_file_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceWalkReport {
    pub pending_files: u64,
    pub skipped_entries: u64,
    pub final_file_index: u64,
}

#[derive(Debug, Clone)]
struct DirectoryFrame {
    entries: Vec<DiscoveredEntry>,
}

#[derive(Debug, Clone)]
struct DiscoveredEntry {
    abs_path: PathBuf,
    relative_path: String,
    file_type: fs::FileType,
}

pub(crate) fn scan_source_tree(root: &Path) -> Result<ScanReport> {
    let workers = thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(8);
    scan_source_tree_with_workers(root, workers)
}

fn scan_source_tree_with_workers(root: &Path, workers: usize) -> Result<ScanReport> {
    if !root.exists() {
        bail!("v4 sender source does not exist: {}", root.display());
    }
    if !root.is_dir() {
        bail!("v4 sender source is not a directory: {}", root.display());
    }

    let mut report = scan_entries(read_sorted_entries(root, "")?, workers)?;
    for (index, entry) in report.entries.iter_mut().enumerate() {
        entry.work_ordinal = index as u64 + 1;
    }
    Ok(report)
}

fn scan_entries(entries: Vec<DiscoveredEntry>, workers: usize) -> Result<ScanReport> {
    let mut report = ScanReport::default();
    let mut stack = vec![DirectoryFrame { entries }];

    while let Some(mut frame) = stack.pop() {
        if workers > 1
            && frame
                .entries
                .iter()
                .filter(|entry| entry.file_type.is_dir())
                .take(2)
                .count()
                == 2
        {
            // Поддеревья независимы; порядок их завершения не меняет DFS и resume ordinal.
            let subtrees = scan_parallel_subtrees(&frame.entries, workers)?;
            report
                .entries
                .reserve(subtrees.iter().map(|subtree| subtree.entries.len()).sum());
            let mut subtrees = subtrees.into_iter();
            while let Some(entry) = frame.entries.pop() {
                if entry.file_type.is_dir() {
                    let mut subtree = subtrees.next().expect("directory scan result is missing");
                    report.entries.append(&mut subtree.entries);
                    report.skipped.append(&mut subtree.skipped);
                } else {
                    record_entry(&mut report, entry);
                }
            }
            continue;
        }

        while let Some(entry) = frame.entries.pop() {
            if entry.file_type.is_dir() {
                let entries = read_sorted_entries(&entry.abs_path, &entry.relative_path)?;
                stack.push(frame);
                stack.push(DirectoryFrame { entries });
                break;
            }
            record_entry(&mut report, entry);
        }
    }

    Ok(report)
}

struct SubtreeTask {
    index: usize,
    abs_path: PathBuf,
    relative_path: String,
}

fn scan_parallel_subtrees(entries: &[DiscoveredEntry], workers: usize) -> Result<Vec<ScanReport>> {
    let directory_count = entries
        .iter()
        .filter(|entry| entry.file_type.is_dir())
        .count();
    let workers = workers.min(directory_count);
    let mut reports = vec![None; directory_count];
    let mut directories = entries
        .iter()
        .rev()
        .filter(|entry| entry.file_type.is_dir())
        .enumerate();

    thread::scope(|scope| -> Result<()> {
        let (task_sender, task_receiver) = bounded::<SubtreeTask>(workers);
        let (result_sender, result_receiver) = bounded(workers);
        for _ in 0..workers {
            let tasks = task_receiver.clone();
            let results = result_sender.clone();
            scope.spawn(move || {
                while let Ok(task) = tasks.recv() {
                    // Внутри поддерева сохраняется последовательный DFS: нет вложенных пулов.
                    let report = read_sorted_entries(&task.abs_path, &task.relative_path)
                        .and_then(|entries| scan_entries(entries, 1));
                    if results.send((task.index, report)).is_err() {
                        break;
                    }
                }
            });
        }
        drop(task_receiver);
        drop(result_sender);

        let result = (|| -> Result<()> {
            let mut inflight = 0;
            loop {
                while inflight < workers {
                    let Some((index, entry)) = directories.next() else {
                        break;
                    };
                    task_sender
                        .send(SubtreeTask {
                            index,
                            abs_path: entry.abs_path.clone(),
                            relative_path: entry.relative_path.clone(),
                        })
                        .context("source scan workers stopped before receiving a subtree")?;
                    inflight += 1;
                }
                if inflight == 0 {
                    return Ok(());
                }
                let (index, report) = result_receiver
                    .recv()
                    .context("source scan workers stopped before completing a subtree")?;
                inflight -= 1;
                reports[index] = Some(report?);
            }
        })();
        // При ошибке отключаем оба канала до join, чтобы workers не ждали потребителя.
        drop(task_sender);
        drop(result_receiver);
        result
    })?;

    Ok(reports
        .into_iter()
        .map(|report| report.expect("directory scan result is missing"))
        .collect())
}

fn record_entry(report: &mut ScanReport, entry: DiscoveredEntry) {
    let kind = if entry.file_type.is_file() {
        ScannedEntryKind::Regular
    } else if entry.file_type.is_symlink() {
        ScannedEntryKind::Symlink
    } else {
        report.skipped.push(SkippedEntry {
            work_ordinal: 0,
            relative_path: entry.relative_path,
            reason: SkipReason::UnsupportedEntry,
        });
        return;
    };
    report.entries.push(ScannedEntry {
        work_ordinal: 0,
        abs_path: entry.abs_path,
        relative_path: entry.relative_path,
        kind,
    });
}

// Тот же обход, который запускает sender, без отдельного metadata для размеров.
pub fn walk_source(root: impl AsRef<Path>) -> Result<SourceWalkReport> {
    let report = scan_source_tree(root.as_ref())?;
    Ok(SourceWalkReport {
        pending_files: report.entries.len() as u64,
        skipped_entries: report.skipped.len() as u64,
        final_file_index: report
            .entries
            .last()
            .map(|entry| entry.work_ordinal)
            .unwrap_or(0),
    })
}

pub fn scan_source(root: impl AsRef<Path>) -> Result<SourceScanReport> {
    let report = scan_source_tree(root.as_ref())?;
    let mut pending_bytes = 0_u64;

    for entry in &report.entries {
        if matches!(entry.kind, ScannedEntryKind::Regular) {
            pending_bytes = pending_bytes.saturating_add(
                fs::metadata(&entry.abs_path)
                    .with_context(|| {
                        format!(
                            "failed to read v4 source metadata for {}",
                            entry.abs_path.display()
                        )
                    })?
                    .len(),
            );
        }
    }

    Ok(SourceScanReport {
        pending_files: report.entries.len() as u64,
        pending_bytes,
        final_file_index: report
            .entries
            .last()
            .map(|entry| entry.work_ordinal)
            .unwrap_or(0),
    })
}

fn read_sorted_entries(dir: &Path, rel_dir: &str) -> Result<Vec<DiscoveredEntry>> {
    let mut entries = fs::read_dir(dir)
        .with_context(|| format!("failed to read v4 source directory {}", dir.display()))?
        .map(|entry| -> Result<_> {
            let entry = entry
                .with_context(|| format!("failed to read v4 source entry in {}", dir.display()))?;
            let abs_path = entry.path();
            // path уже содержит имя: заимствуем его, не создавая второй OsString.
            let name = abs_path
                .file_name()
                .context("v4 source entry path has no file name")?
                .to_string_lossy();
            let name = normalize_component(&name);
            let relative_path = if rel_dir.is_empty() {
                name.into_owned()
            } else {
                let mut path = String::with_capacity(rel_dir.len() + 1 + name.len());
                path.push_str(rel_dir);
                path.push('/');
                path.push_str(&name);
                path
            };
            Ok(DiscoveredEntry {
                abs_path,
                relative_path,
                file_type: entry
                    .file_type()
                    .with_context(|| "failed to resolve v4 source file type")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let prefix_len = rel_dir.len() + usize::from(!rel_dir.is_empty());
    sort_directory_entries(&mut entries, prefix_len);
    Ok(entries)
}

fn sort_directory_entries(entries: &mut [DiscoveredEntry], prefix_len: usize) {
    // Префикс каталога у всех ключей одинаков. Stable sort сохраняет прежний
    // порядок равных нормализованных имён, включая lossy Unicode collisions.
    entries.sort_by(|left, right| {
        right.relative_path.as_bytes()[prefix_len..]
            .cmp(&left.relative_path.as_bytes()[prefix_len..])
    });
}

fn normalize_component(component: &str) -> Cow<'_, str> {
    if component.contains('\\') {
        Cow::Owned(component.replace('\\', "/"))
    } else {
        // Обычное имя сразу войдёт в полный относительный путь без промежуточной аллокации.
        Cow::Borrowed(component)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::TestDir;

    #[test]
    fn walk_counts_empty_directories_without_changing_size_scan() {
        let source = TestDir::new();
        fs::create_dir_all(source.path().join("пустой/ещё-пустой")).unwrap();
        assert_eq!(
            walk_source(source.path()).unwrap(),
            SourceWalkReport {
                pending_files: 0,
                skipped_entries: 0,
                final_file_index: 0,
            }
        );
        assert_eq!(
            scan_source(source.path()).unwrap(),
            SourceScanReport {
                pending_files: 0,
                pending_bytes: 0,
                final_file_index: 0,
            }
        );
    }

    #[test]
    fn unicode_prefix_preserves_component_order_and_depth_first_traversal() {
        let source = TestDir::new();
        let prefix = "общий-🙂";
        let files = ["🙂.txt", "я/z.bin", "é.txt", "é/я.bin", "a.txt"];
        for name in files {
            let path = source.path().join(prefix).join(name);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, [1, 2, 3]).unwrap();
        }
        fs::create_dir(source.path().join(prefix).join("empty")).unwrap();
        let expected = ["a.txt", "é/я.bin", "é.txt", "я/z.bin", "🙂.txt"]
            .map(|name| format!("{prefix}/{name}"));
        for workers in [1, 4] {
            let scan = scan_source_tree_with_workers(source.path(), workers).unwrap();
            assert!(scan.skipped.is_empty());
            assert_eq!(scan.entries.len(), expected.len());
            for (index, (entry, relative_path)) in scan.entries.iter().zip(&expected).enumerate() {
                assert_eq!(entry.work_ordinal, index as u64 + 1);
                assert_eq!(&entry.relative_path, relative_path);
                assert_eq!(entry.abs_path, source.path().join(relative_path));
                assert_eq!(entry.kind, ScannedEntryKind::Regular);
            }
        }
        assert_eq!(
            walk_source(source.path()).unwrap(),
            SourceWalkReport {
                pending_files: 5,
                skipped_entries: 0,
                final_file_index: 5,
            }
        );
        assert_eq!(
            scan_source(source.path()).unwrap(),
            SourceScanReport {
                pending_files: 5,
                pending_bytes: 15,
                final_file_index: 5,
            }
        );
    }

    #[test]
    fn suffix_sort_retains_full_path_order_and_equal_normalized_keys() {
        let source = TestDir::new();
        let file = source.path().join("kind.bin");
        fs::write(&file, []).unwrap();
        let file_type = fs::metadata(file).unwrap().file_type();
        for prefix in ["", "длинный-префикс/🙂/каталог/"] {
            // Разные исходные имена могут дать одинаковый lossy Unicode ключ.
            // Уникальный abs_path позволяет проверить сохранение порядка ties.
            let names = ["я.bin", "same�.bin", "é.bin", "a.bin", "same�.bin", "z.bin"];
            let mut actual = (0..128)
                .map(|index| DiscoveredEntry {
                    abs_path: source.path().join(format!("{index:03}")),
                    relative_path: format!("{prefix}{}", names[index % names.len()]),
                    file_type,
                })
                .collect::<Vec<_>>();
            let mut expected = actual.clone();
            expected.sort_by(|left, right| right.relative_path.cmp(&left.relative_path));
            sort_directory_entries(&mut actual, prefix.len());
            // pop в DFS читает этот массив с конца, включая одинаковые ключи.
            for (actual, expected) in actual.iter().rev().zip(expected.iter().rev()) {
                assert_eq!(actual.relative_path, expected.relative_path);
                assert_eq!(actual.abs_path, expected.abs_path);
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn backslash_normalization_preserves_colliding_paths_in_dfs_order() {
        let source = TestDir::new();
        fs::create_dir(source.path().join("a")).unwrap();
        fs::create_dir(source.path().join("empty")).unwrap();
        fs::write(source.path().join("a/b"), b"nested").unwrap();
        fs::write(source.path().join("a\\b"), b"literal backslash").unwrap();
        fs::write(source.path().join("é\\я"), b"unicode").unwrap();
        for workers in [1, 4] {
            let scan = scan_source_tree_with_workers(source.path(), workers).unwrap();
            let expected = [("a/b", "a/b"), ("a/b", "a\\b"), ("é/я", "é\\я")];
            assert_eq!(scan.entries.len(), expected.len());
            for (index, (entry, (relative, original))) in
                scan.entries.iter().zip(expected).enumerate()
            {
                assert_eq!(entry.relative_path, relative);
                assert_eq!(entry.abs_path, source.path().join(original));
                assert_eq!(entry.work_ordinal, index as u64 + 1);
            }
        }
    }

    #[test]
    fn parallel_scan_preserves_dfs_across_more_subtrees_than_workers() {
        let source = TestDir::new();
        fs::write(source.path().join("0-root.txt"), b"root").unwrap();
        fs::write(source.path().join("z-root.txt"), b"root").unwrap();
        let branch = source.path().join("chain/next");
        for directory in (0..24).rev() {
            let path = branch.join(format!("dir-{directory:02}"));
            fs::create_dir_all(path.join("a/empty")).unwrap();
            fs::write(path.join("a/z.txt"), b"nested").unwrap();
            fs::write(path.join("a.txt"), b"sibling").unwrap();
        }
        let sequential = scan_source_tree_with_workers(source.path(), 1).unwrap();
        let parallel = scan_source_tree_with_workers(source.path(), 4).unwrap();
        assert_eq!(parallel.entries.len(), 50);
        assert_eq!(parallel.skipped, sequential.skipped);
        for (index, (expected, actual)) in
            sequential.entries.iter().zip(&parallel.entries).enumerate()
        {
            assert_eq!(actual.work_ordinal, index as u64 + 1);
            assert_eq!(actual.relative_path, expected.relative_path);
            assert_eq!(actual.abs_path, expected.abs_path);
            assert_eq!(actual.kind, expected.kind);
        }
        assert_eq!(
            parallel.entries[1].relative_path,
            "chain/next/dir-00/a/z.txt"
        );
        assert_eq!(parallel.entries[2].relative_path, "chain/next/dir-00/a.txt");
        assert_eq!(parallel.entries[49].relative_path, "z-root.txt");
    }

    #[test]
    fn parallel_scan_propagates_missing_subtree_error() {
        let source = TestDir::new();
        for name in ["a", "b", "c"] {
            fs::create_dir(source.path().join(name)).unwrap();
        }
        let entries = read_sorted_entries(source.path(), "").unwrap();
        fs::remove_dir(source.path().join("b")).unwrap();
        let error = scan_parallel_subtrees(&entries, 2).unwrap_err();
        assert!(error.to_string().contains("source directory"));
        assert!(error.to_string().contains('b'));
    }

    #[cfg(unix)]
    #[test]
    fn parallel_scan_does_not_descend_into_directory_symlinks() {
        let source = TestDir::new();
        fs::create_dir(source.path().join("a")).unwrap();
        fs::create_dir(source.path().join("b")).unwrap();
        fs::write(source.path().join("b/file.txt"), b"payload").unwrap();
        std::os::unix::fs::symlink("../b", source.path().join("a/link")).unwrap();
        let report = scan_source_tree_with_workers(source.path(), 4).unwrap();
        assert_eq!(report.entries.len(), 2);
        assert_eq!(report.entries[0].relative_path, "a/link");
        assert_eq!(report.entries[0].kind, ScannedEntryKind::Symlink);
        assert_eq!(report.entries[1].relative_path, "b/file.txt");
    }
}

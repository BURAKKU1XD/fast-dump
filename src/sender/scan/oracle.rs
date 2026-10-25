use super::directory::{
    DiscoveredEntry, SourceDirectory, excluded_directory_entry, read_source_directory,
    read_source_root, validate_source_root,
};
use super::{DirectoryFrame, ScanReport, ScannedEntry, ScannedEntryKind};
use crate::network::protocol::{SkipReason, SkippedEntry};
use anyhow::{Context, Result};
use crossbeam_channel::bounded;
use std::path::Path;
use std::thread;

// Прежний собирающий алгоритм остаётся независимым oracle только для тестов DFS.
pub(super) fn scan_source_tree_with_workers(root: &Path, workers: usize) -> Result<ScanReport> {
    validate_source_root(root)?;
    let mut report = scan_entries(read_source_root(root, None)?, workers)?;
    for (index, entry) in report.entries.iter_mut().enumerate() {
        entry.work_ordinal = index as u64 + 1;
    }
    Ok(report)
}

pub(super) fn scan_entries(entries: Vec<DiscoveredEntry>, workers: usize) -> Result<ScanReport> {
    let mut report = ScanReport::default();
    let mut stack = vec![DirectoryFrame::new(entries)];

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
                match read_source_directory(&entry.abs_path, &entry.relative_path, None)? {
                    SourceDirectory::Entries(entries) => {
                        stack.push(frame);
                        stack.push(DirectoryFrame::new(entries));
                        break;
                    }
                    SourceDirectory::Excluded(_) => {
                        report
                            .skipped
                            .push(excluded_directory_entry(entry.relative_path));
                        continue;
                    }
                }
            }
            record_entry(&mut report, entry);
        }
    }

    Ok(report)
}

pub(super) struct SubtreeTask<'a> {
    pub(super) index: usize,
    pub(super) entry: &'a DiscoveredEntry,
}

pub(super) fn scan_parallel_subtrees(
    entries: &[DiscoveredEntry],
    workers: usize,
) -> Result<Vec<ScanReport>> {
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
        let (task_sender, task_receiver) = bounded::<SubtreeTask<'_>>(workers);
        let (result_sender, result_receiver) = bounded(workers);
        for _ in 0..workers {
            let tasks = task_receiver.clone();
            let results = result_sender.clone();
            scope.spawn(move || {
                while let Ok(task) = tasks.recv() {
                    // Внутри поддерева сохраняется последовательный DFS: нет вложенных пулов.
                    let report = read_source_directory(
                        &task.entry.abs_path,
                        &task.entry.relative_path,
                        None,
                    )
                    .and_then(|directory| match directory {
                        SourceDirectory::Entries(entries) => scan_entries(entries, 1),
                        SourceDirectory::Excluded(_) => Ok(ScanReport {
                            entries: Vec::new(),
                            skipped: vec![excluded_directory_entry(
                                task.entry.relative_path.clone(),
                            )],
                        }),
                    });
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
                        .send(SubtreeTask { index, entry })
                        .map_err(|_| {
                            anyhow::anyhow!(
                                "source scan workers stopped before receiving a subtree"
                            )
                        })?;
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

pub(super) fn record_entry(report: &mut ScanReport, entry: DiscoveredEntry) {
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

use std::collections::{BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use super::directory::{
    DiscoveredEntry, SourceDirectory, excluded_directory_entry, read_source_directory,
};
use super::{
    DirectoryFrame, SCAN_BATCH_ENTRIES, SCAN_QUEUE_BATCHES, ScanItem, ScanMessage, ScannedEntry,
    ScannedEntryKind,
};
use crate::network::protocol::{SkipReason, SkippedEntry};
use crate::progress::{self, PipelineStage, PipelineState};
use crate::runtime::SessionControl;
use anyhow::{Context, Result};
use crossbeam_channel::{Sender, bounded};

pub(super) struct ScanOutput {
    pub(super) sender: Sender<Result<ScanMessage>>,
    pub(super) batch: Vec<ScanItem>,
    pub(super) started: Instant,
    pub(super) blocked: Duration,
    pub(super) control: Option<SessionControl>,
}

impl ScanOutput {
    pub(super) fn new(sender: Sender<Result<ScanMessage>>) -> Self {
        Self {
            sender,
            batch: Vec::with_capacity(SCAN_BATCH_ENTRIES),
            started: Instant::now(),
            blocked: Duration::ZERO,
            control: None,
        }
    }

    pub(super) fn emit(&mut self, item: ScanItem) -> Result<()> {
        self.batch.push(item);
        if self.batch.len() == SCAN_BATCH_ENTRIES {
            self.flush()?;
        }
        Ok(())
    }

    pub(super) fn duration(&self) -> Duration {
        self.started.elapsed().saturating_sub(self.blocked)
    }

    pub(super) fn flush(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let batch = std::mem::replace(&mut self.batch, Vec::with_capacity(SCAN_BATCH_ENTRIES));
        self.send_batch(batch)
    }

    fn send_batch(&mut self, batch: Vec<ScanItem>) -> Result<()> {
        let message = ScanMessage::Batch(batch, self.duration());
        let started = Instant::now();
        let _waiting = progress::stage(
            self.control.as_ref(),
            PipelineStage::Scan,
            PipelineState::WaitingConsumer,
        );
        let result = self.sender.send(Ok(message));
        self.blocked += started.elapsed();
        result.context("source scan consumer stopped")
    }

    pub(super) fn complete(mut self, result: Result<()>) {
        // Даже при поздней ошибке сохраняем порядок: сначала найденный префикс.
        if !self.batch.is_empty() {
            // После последнего batch новый буфер уже не понадобится.
            let batch = std::mem::take(&mut self.batch);
            if self.send_batch(batch).is_err() {
                return;
            }
        }
        let message = result.map(|()| ScanMessage::Finished(self.duration()));
        let _waiting = progress::stage(
            self.control.as_ref(),
            PipelineStage::Scan,
            PipelineState::WaitingConsumer,
        );
        let _ = self.sender.send(message);
    }
}

pub(super) fn stream_scan_entries(
    entries: Vec<DiscoveredEntry>,
    workers: usize,
    output: &mut ScanOutput,
    cancelled: &AtomicBool,
) -> Result<()> {
    let mut stack = vec![DirectoryFrame::new(entries)];
    while let Some(mut frame) = stack.pop() {
        anyhow::ensure!(!cancelled.load(Ordering::Relaxed), "source scan cancelled");
        if frame.ambiguous_names {
            // Backslash и lossy Unicode могут свести разные native paths к одному
            // tar path. До первой выдачи собираем весь общий каталог, чтобы даже
            // поздний UnsupportedEntry подавил совпадающий regular, как прежде.
            for item in collect_ambiguous_subtree(frame.entries, cancelled)? {
                anyhow::ensure!(!cancelled.load(Ordering::Relaxed), "source scan cancelled");
                output.emit(item)?;
            }
            continue;
        }
        if workers > 1
            && frame
                .entries
                .iter()
                .filter(|entry| entry.file_type.is_dir())
                .take(2)
                .count()
                == 2
        {
            stream_parallel_subtrees(frame.entries, workers, output, cancelled)?;
            continue;
        }
        while let Some(entry) = frame.entries.pop() {
            anyhow::ensure!(!cancelled.load(Ordering::Relaxed), "source scan cancelled");
            if entry.file_type.is_dir() {
                match read_source_directory(&entry.abs_path, &entry.relative_path, Some(cancelled))?
                {
                    SourceDirectory::Entries(entries) => {
                        stack.push(frame);
                        stack.push(DirectoryFrame::new(entries));
                        break;
                    }
                    SourceDirectory::Excluded(filesystem) => {
                        output.emit(ScanItem::Excluded(
                            excluded_directory_entry(entry.relative_path),
                            filesystem,
                        ))?;
                        continue;
                    }
                }
            }
            stream_record_entry(output, entry)?;
        }
    }
    Ok(())
}

pub(super) struct StreamingSubtreeTask {
    pub(super) entry: DiscoveredEntry,
    pub(super) output: Sender<Result<ScanMessage>>,
}

pub(super) fn stream_parallel_subtrees(
    mut entries: Vec<DiscoveredEntry>,
    workers: usize,
    output: &mut ScanOutput,
    cancelled: &AtomicBool,
) -> Result<()> {
    let workers = workers.min(
        entries
            .iter()
            .filter(|entry| entry.file_type.is_dir())
            .count(),
    );
    thread::scope(|scope| -> Result<()> {
        let (jobs, tasks) = bounded::<StreamingSubtreeTask>(workers);
        for _ in 0..workers {
            let tasks = tasks.clone();
            thread::Builder::new()
                .name("dump-subtree".into())
                .spawn_scoped(scope, move || {
                    while let Ok(task) = tasks.recv() {
                        let mut output = ScanOutput::new(task.output);
                        let result = read_source_directory(
                            &task.entry.abs_path,
                            &task.entry.relative_path,
                            Some(cancelled),
                        )
                        .and_then(|directory| match directory {
                            SourceDirectory::Entries(entries) => {
                                stream_scan_entries(entries, 1, &mut output, cancelled)
                            }
                            SourceDirectory::Excluded(filesystem) => {
                                output.emit(ScanItem::Excluded(
                                    excluded_directory_entry(task.entry.relative_path),
                                    filesystem,
                                ))
                            }
                        });
                        output.complete(result);
                    }
                })
                .context("failed to start source subtree scanner")?;
        }
        drop(tasks);
        let mut pending = VecDeque::with_capacity(workers);
        let mut next_directory = entries.len();
        let result = (|| -> Result<()> {
            loop {
                while pending.len() < workers && next_directory != 0 {
                    next_directory -= 1;
                    let entry = &entries[next_directory];
                    if !entry.file_type.is_dir() {
                        continue;
                    }
                    let (sender, receiver) = bounded(SCAN_QUEUE_BATCHES);
                    jobs.send(StreamingSubtreeTask {
                        entry: entry.clone(),
                        output: sender,
                    })
                    .context("source subtree scanners stopped before receiving work")?;
                    pending.push_back(receiver);
                }
                let Some(entry) = entries.pop() else {
                    break;
                };
                anyhow::ensure!(!cancelled.load(Ordering::Relaxed), "source scan cancelled");
                if !entry.file_type.is_dir() {
                    stream_record_entry(output, entry)?;
                    continue;
                }
                let receiver = pending
                    .pop_front()
                    .expect("directory scan result is missing");
                while let ScanMessage::Batch(items, _) = receiver
                    .recv()
                    .context("source subtree scanner stopped early")??
                {
                    for item in items {
                        output.emit(item)?;
                    }
                }
            }
            Ok(())
        })();
        // Обе стороны backpressure должны исчезнуть до join, в том числе при ошибке.
        drop(jobs);
        drop(pending);
        result
    })
}

pub(super) fn stream_record_entry(output: &mut ScanOutput, entry: DiscoveredEntry) -> Result<()> {
    output.emit(discovered_item(entry))
}

pub(super) fn discovered_item(entry: DiscoveredEntry) -> ScanItem {
    let kind = if entry.file_type.is_file() {
        ScannedEntryKind::Regular
    } else if entry.file_type.is_symlink() {
        ScannedEntryKind::Symlink
    } else {
        return ScanItem::Skipped(SkippedEntry {
            work_ordinal: 0,
            relative_path: entry.relative_path,
            reason: SkipReason::UnsupportedEntry,
        });
    };
    ScanItem::Entry(
        ScannedEntry {
            work_ordinal: 0,
            abs_path: entry.abs_path,
            relative_path: entry.relative_path,
            kind,
        },
        false,
    )
}

pub(super) fn collect_ambiguous_subtree(
    entries: Vec<DiscoveredEntry>,
    cancelled: &AtomicBool,
) -> Result<Vec<ScanItem>> {
    let mut items = Vec::new();
    let mut stack = vec![DirectoryFrame::new(entries)];
    while let Some(mut frame) = stack.pop() {
        anyhow::ensure!(!cancelled.load(Ordering::Relaxed), "source scan cancelled");
        while let Some(entry) = frame.entries.pop() {
            anyhow::ensure!(!cancelled.load(Ordering::Relaxed), "source scan cancelled");
            if entry.file_type.is_dir() {
                match read_source_directory(&entry.abs_path, &entry.relative_path, Some(cancelled))?
                {
                    SourceDirectory::Entries(entries) => {
                        stack.push(frame);
                        stack.push(DirectoryFrame::new(entries));
                        break;
                    }
                    SourceDirectory::Excluded(filesystem) => {
                        items.push(ScanItem::Excluded(
                            excluded_directory_entry(entry.relative_path),
                            filesystem,
                        ));
                        continue;
                    }
                }
            }
            items.push(discovered_item(entry));
        }
    }
    let mut skipped_paths = BTreeSet::new();
    for item in &items {
        anyhow::ensure!(!cancelled.load(Ordering::Relaxed), "source scan cancelled");
        if let ScanItem::Skipped(skipped) | ScanItem::Excluded(skipped, _) = item {
            skipped_paths.insert(skipped.relative_path.clone());
        }
    }
    for item in &mut items {
        anyhow::ensure!(!cancelled.load(Ordering::Relaxed), "source scan cancelled");
        if let ScanItem::Entry(entry, suppressed) = item {
            *suppressed = skipped_paths.contains(&entry.relative_path);
        }
    }
    Ok(items)
}

use std::borrow::Cow;
use std::collections::{BTreeSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use crossbeam_channel::{Receiver, Sender, bounded};

use crate::network::protocol::{SkipReason, SkippedEntry};
use crate::progress::{self, PipelineStage, PipelineState};
use crate::runtime::SessionControl;

use super::source_policy::{self, ExcludedFilesystem};

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
    ambiguous_names: bool,
}

impl DirectoryFrame {
    fn new(entries: Vec<DiscoveredEntry>) -> Self {
        let ambiguous_names = entries.iter().any(|entry| entry.ambiguous_name);
        Self {
            entries,
            ambiguous_names,
        }
    }
}

#[derive(Debug, Clone)]
struct DiscoveredEntry {
    abs_path: PathBuf,
    relative_path: String,
    file_type: fs::FileType,
    ambiguous_name: bool,
}

// На Windows крупные порции уменьшают остановки subtree workers. Лимит
// потоков отдельно сдерживает конкуренцию read_dir с чтением содержимого.
#[cfg(windows)]
const SCAN_BATCH_ENTRIES: usize = 1024;
#[cfg(not(windows))]
const SCAN_BATCH_ENTRIES: usize = 64;
#[cfg(windows)]
const SCAN_MAX_WORKERS: usize = 2;
#[cfg(not(windows))]
const SCAN_MAX_WORKERS: usize = 8;
const SCAN_QUEUE_BATCHES: usize = 2;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ScanProgress {
    pub(crate) duration: Duration,
    pub(crate) discovered_entries: u64,
}

enum ScanItem {
    // Флаг исключает коллизию с terminal skip только из runtime pending;
    // публичный scan и проверка resume всё равно видят entry и его ordinal.
    Entry(ScannedEntry, bool),
    Skipped(SkippedEntry),
    Excluded(SkippedEntry, ExcludedFilesystem),
}

enum SourceDirectory {
    Entries(Vec<DiscoveredEntry>),
    Excluded(ExcludedFilesystem),
}

enum ScanMessage {
    Batch(Vec<ScanItem>, Duration),
    Finished(Duration),
}

// Держим только ограниченные очереди и отсортированные каталоги текущего DFS.
// Отдельный большой каталог всё ещё надо прочитать целиком: иначе нет stable sort.
pub(crate) struct StreamingScan {
    messages: Option<Receiver<Result<ScanMessage>>>,
    worker: Option<thread::JoinHandle<()>>,
    cancelled: Arc<AtomicBool>,
    control: Option<SessionControl>,
    batch: std::vec::IntoIter<ScanItem>,
    skipped: Vec<SkippedEntry>,
    progress: ScanProgress,
    finished: bool,
    failed: bool,
}

impl StreamingScan {
    pub(crate) fn new(root: &Path) -> Result<Self> {
        Self::with_workers(root, scan_worker_count())
    }

    pub(crate) fn controlled(root: &Path, control: Option<SessionControl>) -> Result<Self> {
        super::check_control(control.as_ref())?;
        Self::with_workers_controlled(root, scan_worker_count(), control)
    }

    pub(crate) fn control(&self) -> Option<SessionControl> {
        self.control.clone()
    }

    fn with_workers(root: &Path, workers: usize) -> Result<Self> {
        Self::with_workers_controlled(root, workers, None)
    }

    fn with_workers_controlled(
        root: &Path,
        workers: usize,
        control: Option<SessionControl>,
    ) -> Result<Self> {
        let root = root.to_owned();
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_cancelled = Arc::clone(&cancelled);
        let (sender, messages) = bounded(SCAN_QUEUE_BATCHES);
        let worker_control = control.clone();
        let worker = thread::Builder::new()
            .name("dump-scan".into())
            .spawn(move || {
                let scanning = progress::stage(
                    worker_control.as_ref(),
                    PipelineStage::Scan,
                    PipelineState::Scanning,
                );
                let mut output = ScanOutput::new(sender);
                output.control = worker_control.clone();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    validate_source_root(&root)?;
                    stream_scan_entries(
                        read_source_root(&root, Some(&worker_cancelled))?,
                        workers.clamp(1, 8),
                        &mut output,
                        &worker_cancelled,
                    )
                }))
                .unwrap_or_else(|_| Err(anyhow::anyhow!("source scanner panicked")));
                let state = if worker_cancelled.load(Ordering::Relaxed)
                    || worker_control
                        .as_ref()
                        .is_some_and(SessionControl::is_cancelled)
                {
                    PipelineState::Stopped
                } else if result.is_ok() {
                    PipelineState::Done
                } else {
                    PipelineState::Failed
                };
                output.complete(result);
                drop(scanning);
                progress::set_stage(worker_control.as_ref(), PipelineStage::Scan, state);
            })
            .context("failed to start source scanner")?;
        Ok(Self {
            messages: Some(messages),
            worker: Some(worker),
            cancelled,
            control,
            batch: Vec::new().into_iter(),
            skipped: Vec::new(),
            progress: ScanProgress::default(),
            finished: false,
            failed: false,
        })
    }

    pub(crate) fn next_entry(&mut self) -> Result<Option<ScannedEntry>> {
        self.next_entry_filtered(false)
    }

    pub(crate) fn next_pending_entry(&mut self) -> Result<Option<ScannedEntry>> {
        self.next_entry_filtered(true)
    }

    pub(crate) fn cancellation_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancelled)
    }

    fn next_entry_filtered(&mut self, pending_only: bool) -> Result<Option<ScannedEntry>> {
        anyhow::ensure!(!self.failed, "source scan has already failed");
        let result = self.next_entry_inner(pending_only);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn next_entry_inner(&mut self, pending_only: bool) -> Result<Option<ScannedEntry>> {
        loop {
            super::check_control(self.control.as_ref())?;
            // Planner может ждать следующий pending entry среди длинного хвоста
            // skips. Общий флаг прерывает и consumer, и сборку поддеревьев.
            anyhow::ensure!(
                !self.cancelled.load(Ordering::Relaxed),
                "source scan cancelled"
            );
            if let Some(item) = self.batch.next() {
                match item {
                    ScanItem::Entry(mut entry, suppressed) => {
                        self.progress.discovered_entries = self
                            .progress
                            .discovered_entries
                            .checked_add(1)
                            .context("source work ordinal overflow")?;
                        entry.work_ordinal = self.progress.discovered_entries;
                        if pending_only && suppressed {
                            continue;
                        }
                        return Ok(Some(entry));
                    }
                    ScanItem::Skipped(skipped) => self.skipped.push(skipped),
                    ScanItem::Excluded(skipped, filesystem) => {
                        source_policy::log_exclusion(
                            self.control.as_ref(),
                            &skipped.relative_path,
                            filesystem,
                        );
                        self.skipped.push(skipped);
                    }
                }
                continue;
            }
            if self.finished {
                return Ok(None);
            }
            let messages = self.messages.as_ref().unwrap();
            let _waiting = progress::stage(
                self.control.as_ref(),
                PipelineStage::Plan,
                PipelineState::WaitingScan,
            );
            let message = if self.control.is_some() {
                loop {
                    super::check_control(self.control.as_ref())?;
                    anyhow::ensure!(
                        !self.cancelled.load(Ordering::Relaxed),
                        "source scan cancelled"
                    );
                    match messages.recv_timeout(Duration::from_millis(50)) {
                        Ok(message) => break message?,
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                        Err(error) => {
                            return Err(error)
                                .context("source scanner stopped before completing the tree");
                        }
                    }
                }
            } else {
                messages
                    .recv()
                    .context("source scanner stopped before completing the tree")??
            };
            match message {
                ScanMessage::Batch(batch, duration) => {
                    if let Some(progress) = self.control.as_ref().and_then(SessionControl::progress)
                    {
                        let discovered = batch
                            .iter()
                            .filter(|item| matches!(item, ScanItem::Entry(..)))
                            .count() as u64;
                        progress.discovered.store(
                            self.progress.discovered_entries.saturating_add(discovered),
                            Ordering::Relaxed,
                        );
                    }
                    self.batch = batch.into_iter();
                    self.progress.duration = duration;
                }
                ScanMessage::Finished(duration) => {
                    super::check_control(self.control.as_ref())?;
                    self.progress.duration = duration;
                    self.finished = true;
                    if let Some(progress) = self.control.as_ref().and_then(SessionControl::progress)
                    {
                        progress.scan_finished.store(true, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    pub(crate) fn take_skips(&mut self) -> Vec<SkippedEntry> {
        std::mem::take(&mut self.skipped)
    }

    pub(crate) const fn progress(&self) -> ScanProgress {
        self.progress
    }
}

impl Drop for StreamingScan {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
        // Сначала разблокируем send координатора; тот отключит очереди поддеревьев.
        self.messages.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

struct ScanOutput {
    sender: Sender<Result<ScanMessage>>,
    batch: Vec<ScanItem>,
    started: Instant,
    blocked: Duration,
    control: Option<SessionControl>,
}

impl ScanOutput {
    fn new(sender: Sender<Result<ScanMessage>>) -> Self {
        Self {
            sender,
            batch: Vec::with_capacity(SCAN_BATCH_ENTRIES),
            started: Instant::now(),
            blocked: Duration::ZERO,
            control: None,
        }
    }

    fn emit(&mut self, item: ScanItem) -> Result<()> {
        self.batch.push(item);
        if self.batch.len() == SCAN_BATCH_ENTRIES {
            self.flush()?;
        }
        Ok(())
    }

    fn duration(&self) -> Duration {
        self.started.elapsed().saturating_sub(self.blocked)
    }

    fn flush(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let batch = std::mem::replace(&mut self.batch, Vec::with_capacity(SCAN_BATCH_ENTRIES));
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

    fn complete(mut self, result: Result<()>) {
        // Даже при поздней ошибке сохраняем порядок: сначала найденный префикс.
        if self.flush().is_ok() {
            let message = result.map(|()| ScanMessage::Finished(self.duration()));
            let _waiting = progress::stage(
                self.control.as_ref(),
                PipelineStage::Scan,
                PipelineState::WaitingConsumer,
            );
            let _ = self.sender.send(message);
        }
    }
}

fn scan_worker_count() -> usize {
    thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(SCAN_MAX_WORKERS)
}

fn validate_source_root(root: &Path) -> Result<()> {
    if !root.exists() {
        bail!("v4 sender source does not exist: {}", root.display());
    }
    if !root.is_dir() {
        bail!("v4 sender source is not a directory: {}", root.display());
    }
    Ok(())
}

fn read_source_root(root: &Path, cancelled: Option<&AtomicBool>) -> Result<Vec<DiscoveredEntry>> {
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

fn read_source_directory(
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

fn excluded_directory_entry(relative_path: String) -> SkippedEntry {
    SkippedEntry {
        work_ordinal: 0,
        relative_path,
        reason: SkipReason::UnsupportedEntry,
    }
}

fn stream_scan_entries(
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

struct StreamingSubtreeTask {
    entry: DiscoveredEntry,
    output: Sender<Result<ScanMessage>>,
}

fn stream_parallel_subtrees(
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

fn stream_record_entry(output: &mut ScanOutput, entry: DiscoveredEntry) -> Result<()> {
    output.emit(discovered_item(entry))
}

fn discovered_item(entry: DiscoveredEntry) -> ScanItem {
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

fn collect_ambiguous_subtree(
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

pub(crate) fn scan_source_tree(root: &Path) -> Result<ScanReport> {
    let mut scan = StreamingScan::new(root)?;
    let mut entries = Vec::new();
    while let Some(entry) = scan.next_entry()? {
        entries.push(entry);
    }
    Ok(ScanReport {
        entries,
        skipped: scan.take_skips(),
    })
}

// Прежний собирающий алгоритм остаётся независимым oracle только для тестов DFS.
#[cfg(test)]
fn scan_source_tree_with_workers(root: &Path, workers: usize) -> Result<ScanReport> {
    validate_source_root(root)?;
    let mut report = scan_entries(read_source_root(root, None)?, workers)?;
    for (index, entry) in report.entries.iter_mut().enumerate() {
        entry.work_ordinal = index as u64 + 1;
    }
    Ok(report)
}

#[cfg(test)]
fn scan_entries(entries: Vec<DiscoveredEntry>, workers: usize) -> Result<ScanReport> {
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

#[cfg(test)]
struct SubtreeTask<'a> {
    index: usize,
    entry: &'a DiscoveredEntry,
}

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
fn read_sorted_entries(dir: &Path, rel_dir: &str) -> Result<Vec<DiscoveredEntry>> {
    read_sorted_entries_cancellable(dir, rel_dir, None)
}

fn read_sorted_entries_cancellable(
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
fn joined_scan_entry_path(dir: &Path, name: &std::ffi::OsStr) -> PathBuf {
    // Длины OsStr измерены в единицах capacity, без повторного UTF-16 преобразования.
    // Обычному пути хватает одной allocation; push сохраняет семантику verbatim roots.
    let mut path = PathBuf::with_capacity(dir.as_os_str().len() + 1 + name.len());
    path.push(dir);
    path.push(name);
    path
}

fn sort_directory_entries(entries: &mut [DiscoveredEntry], prefix_len: usize) {
    // Префикс каталога у всех ключей одинаков. Stable sort сохраняет прежний
    // порядок равных нормализованных имён, включая lossy Unicode collisions.
    entries.sort_by(|left, right| {
        right.relative_path.as_bytes()[prefix_len..]
            .cmp(&left.relative_path.as_bytes()[prefix_len..])
    });
}

fn relative_entry_path(rel_dir: &str, name: Cow<'_, str>) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::TestDir;

    #[test]
    fn excluded_subtree_is_pruned_once_in_every_scan_path() {
        let source = TestDir::new();
        fs::create_dir_all(source.path().join("a-data")).unwrap();
        fs::create_dir_all(source.path().join("b-interface/child")).unwrap();
        fs::create_dir_all(source.path().join("z-data")).unwrap();
        fs::write(source.path().join("a-data/first"), b"one").unwrap();
        fs::write(
            source.path().join("b-interface/child/never"),
            b"not source data",
        )
        .unwrap();
        fs::write(source.path().join("z-data/last"), b"two").unwrap();
        let _excluded = source_policy::test_hooks::directory(
            &source.path().join("b-interface"),
            Ok(ExcludedFilesystem::procfs_for_test()),
        );
        // Если любой путь обхода всё же зайдёт в исключённое поддерево, тест упадёт.
        let _unreadable = source_policy::test_hooks::directory(
            &source.path().join("b-interface/child"),
            Err(std::io::ErrorKind::PermissionDenied),
        );
        let expected = vec![(1, "a-data/first"), (2, "z-data/last")];
        let expected_skips = vec![excluded_directory_entry("b-interface".into())];
        for workers in [1, 2, 8] {
            let oracle = scan_source_tree_with_workers(source.path(), workers).unwrap();
            assert_eq!(
                oracle
                    .entries
                    .iter()
                    .map(|entry| (entry.work_ordinal, entry.relative_path.as_str()))
                    .collect::<Vec<_>>(),
                expected,
            );
            assert_eq!(oracle.skipped, expected_skips);
            let mut scan = StreamingScan::with_workers(source.path(), workers).unwrap();
            let mut actual = Vec::new();
            while let Some(entry) = scan.next_pending_entry().unwrap() {
                actual.push((entry.work_ordinal, entry.relative_path));
            }
            assert_eq!(
                actual
                    .iter()
                    .map(|(ordinal, path)| (*ordinal, path.as_str()))
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(scan.take_skips(), expected_skips);
            assert_eq!(scan.progress.discovered_entries, 2);
        }
        assert_eq!(
            walk_source(source.path()).unwrap(),
            SourceWalkReport {
                pending_files: 2,
                skipped_entries: 1,
                final_file_index: 2,
            }
        );
        assert_eq!(
            scan_source(source.path()).unwrap(),
            SourceScanReport {
                pending_files: 2,
                pending_bytes: 6,
                final_file_index: 2,
            }
        );
    }

    #[test]
    fn excluded_source_root_is_rejected_with_filesystem_reason() {
        let source = TestDir::new();
        let _excluded = source_policy::test_hooks::directory(
            source.path(),
            Ok(ExcludedFilesystem::procfs_for_test()),
        );
        for error in [
            scan_source_tree(source.path()).unwrap_err(),
            scan_source_tree_with_workers(source.path(), 2).unwrap_err(),
            walk_source(source.path()).unwrap_err(),
            scan_source(source.path()).unwrap_err(),
        ] {
            let message = error.to_string();
            assert!(message.contains("excluded filesystem proc"), "{message}");
            assert!(message.contains("kernel memory"), "{message}");
            assert!(message.contains(&source.path().display().to_string()));
        }
    }

    #[test]
    fn directory_classification_failure_is_an_error_not_a_skip() {
        let source = TestDir::new();
        fs::create_dir(source.path().join("a-directory")).unwrap();
        fs::create_dir(source.path().join("b-directory")).unwrap();
        let _failure = source_policy::test_hooks::directory(
            &source.path().join("a-directory"),
            Err(std::io::ErrorKind::PermissionDenied),
        );
        for workers in [1, 2] {
            let mut scan = StreamingScan::with_workers(source.path(), workers).unwrap();
            let error = scan.next_entry().unwrap_err();
            assert!(error.to_string().contains("failed to classify"));
            assert!(error.to_string().contains("a-directory"));
            assert!(scan.take_skips().is_empty());
            assert!(scan_source_tree_with_workers(source.path(), workers).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn excluded_ambiguous_directory_suppresses_colliding_file_before_publication() {
        let source = TestDir::new();
        fs::create_dir(source.path().join("a")).unwrap();
        fs::create_dir(source.path().join("a\\b")).unwrap();
        fs::write(source.path().join("a/b"), b"colliding path").unwrap();
        fs::write(source.path().join("a\\b/never"), b"excluded subtree").unwrap();
        fs::write(source.path().join("z"), b"keep").unwrap();
        let _excluded = source_policy::test_hooks::directory(
            &source.path().join("a\\b"),
            Ok(ExcludedFilesystem::procfs_for_test()),
        );
        assert_colliding_skip_keeps_scan_and_resume_ordinals(source.path());
        for workers in [1, 8] {
            let mut scan = StreamingScan::with_workers(source.path(), workers).unwrap();
            let entry = scan.next_pending_entry().unwrap().unwrap();
            assert_eq!(entry.relative_path, "z");
            assert_eq!(entry.work_ordinal, 2);
            assert!(scan.next_pending_entry().unwrap().is_none());
            assert_eq!(
                scan.take_skips(),
                vec![excluded_directory_entry("a/b".into())]
            );
        }
    }

    #[test]
    fn excluded_directory_log_explains_type_without_claiming_durability() {
        let source = TestDir::new();
        fs::create_dir(source.path().join("interface")).unwrap();
        let _excluded = source_policy::test_hooks::directory(
            &source.path().join("interface"),
            Ok(ExcludedFilesystem::procfs_for_test()),
        );
        let hub = Arc::new(crate::progress::ProgressHub::new());
        let run = crate::runtime::RunControl::new(Some(hub.clone()));
        let control = run.session(crate::role::Role::Sender, "dump", "peer");
        let mut scan = StreamingScan::controlled(source.path(), Some(control)).unwrap();
        assert!(scan.next_entry().unwrap().is_none());
        let logs = hub.logs();
        assert_eq!(logs.len(), 1);
        assert!(logs[0].message.contains("Excluded interface: proc"));
        assert!(
            logs[0]
                .message
                .contains("pending durable skip acknowledgement")
        );
    }

    #[test]
    fn session_cancellation_wakes_scan_wait_with_producer_still_connected() {
        let run =
            crate::runtime::RunControl::new(Some(Arc::new(crate::progress::ProgressHub::new())));
        let control = run.session(crate::role::Role::Sender, "dump", "peer");
        let (sender, messages) = bounded(1);
        let mut scan = StreamingScan {
            messages: Some(messages),
            worker: None,
            cancelled: Arc::new(AtomicBool::new(false)),
            control: Some(control.clone()),
            batch: Vec::new().into_iter(),
            skipped: Vec::new(),
            progress: ScanProgress::default(),
            finished: false,
            failed: false,
        };
        let (done, completed) = std::sync::mpsc::channel();
        let worker = thread::spawn(move || {
            let error = scan.next_entry().unwrap_err();
            done.send(error.downcast_ref::<crate::runtime::Cancelled>().is_some())
                .unwrap();
        });
        assert!(matches!(
            completed.recv_timeout(Duration::from_millis(75)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        control.cancel();
        assert!(completed.recv_timeout(Duration::from_secs(2)).unwrap());
        worker.join().unwrap();
        assert!(
            !control
                .progress()
                .unwrap()
                .scan_finished
                .load(Ordering::Relaxed)
        );
        drop(sender);
    }

    #[test]
    fn streaming_scan_matches_collected_dfs_across_batches_and_subtrees() {
        let source = TestDir::new();
        for directory in ["a", "b/nested", "b/nested-more", "d", "é/🙂"] {
            fs::create_dir_all(source.path().join(directory)).unwrap();
            for index in 0..97 {
                fs::write(
                    source
                        .path()
                        .join(directory)
                        .join(format!("{index:03}.bin")),
                    [],
                )
                .unwrap();
            }
        }
        for file in ["a.txt", "c.txt", "é.txt"] {
            fs::write(source.path().join(file), []).unwrap();
        }
        let expected = scan_source_tree_with_workers(source.path(), 1).unwrap();
        for workers in [1, 2, 8] {
            let run = crate::runtime::RunControl::new(Some(Arc::new(
                crate::progress::ProgressHub::new(),
            )));
            let control = run.session(crate::role::Role::Sender, "dump", "peer");
            let mut scan = StreamingScan::with_workers(source.path(), workers).unwrap();
            scan.control = Some(control.clone());
            assert!(
                !control
                    .progress()
                    .unwrap()
                    .scan_finished
                    .load(Ordering::Relaxed)
            );
            for entry in &expected.entries {
                let actual = scan.next_entry().unwrap().unwrap();
                assert_eq!(actual.work_ordinal, entry.work_ordinal);
                assert_eq!(actual.abs_path, entry.abs_path);
                assert_eq!(actual.relative_path, entry.relative_path);
                assert_eq!(actual.kind, entry.kind);
            }
            assert!(scan.next_entry().unwrap().is_none());
            assert!(scan.next_entry().unwrap().is_none());
            assert!(
                control
                    .progress()
                    .unwrap()
                    .scan_finished
                    .load(Ordering::Relaxed)
            );
            assert_eq!(
                scan.progress().discovered_entries,
                expected.entries.len() as u64
            );
            assert_eq!(scan.take_skips(), expected.skipped);
        }
    }

    #[test]
    fn dropping_streaming_scan_disconnects_full_queues_and_joins_subtrees() {
        let source = TestDir::new();
        for directory in 0..2 {
            let directory = source.path().join(format!("{directory}"));
            fs::create_dir(&directory).unwrap();
            for index in 0..SCAN_BATCH_ENTRIES * (SCAN_QUEUE_BATCHES + 1) + 1 {
                fs::write(directory.join(format!("{index:03}")), []).unwrap();
            }
        }
        let scan = StreamingScan::with_workers(source.path(), 2).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while scan.messages.as_ref().unwrap().len() != SCAN_QUEUE_BATCHES {
            assert!(
                Instant::now() < deadline,
                "scanner did not fill its output queue"
            );
            thread::yield_now();
        }
        let (done, completed) = std::sync::mpsc::channel();
        let dropper = thread::spawn(move || {
            drop(scan);
            done.send(()).unwrap();
        });
        completed
            .recv_timeout(Duration::from_secs(10))
            .expect("scanner drop deadlocked");
        dropper.join().unwrap();
        fs::remove_dir_all(source.path()).unwrap();
    }

    #[test]
    fn late_directory_failure_follows_valid_prefix_and_is_sticky() {
        let source = TestDir::new();
        fs::create_dir(source.path().join("a-valid")).unwrap();
        for index in 0..97 {
            fs::write(
                source.path().join("a-valid").join(format!("{index:03}")),
                [],
            )
            .unwrap();
        }
        fs::create_dir(source.path().join("z-missing")).unwrap();
        let entries = read_sorted_entries(source.path(), "").unwrap();
        fs::remove_dir(source.path().join("z-missing")).unwrap();
        // Тип каталога уже известен; исчезновение перед read_dir не зависит от гонки.
        for workers in [1, 2] {
            let (sender, messages) = bounded(8);
            let mut output = ScanOutput::new(sender);
            let cancelled = AtomicBool::new(false);
            let result = stream_scan_entries(entries.clone(), workers, &mut output, &cancelled);
            assert!(result.is_err());
            output.complete(result);
            let mut scan = StreamingScan {
                messages: Some(messages),
                worker: None,
                cancelled: Arc::new(AtomicBool::new(false)),
                control: None,
                batch: Vec::new().into_iter(),
                skipped: Vec::new(),
                progress: ScanProgress::default(),
                finished: false,
                failed: false,
            };
            for ordinal in 1..=97 {
                assert_eq!(scan.next_entry().unwrap().unwrap().work_ordinal, ordinal);
            }
            assert!(
                scan.next_entry()
                    .unwrap_err()
                    .to_string()
                    .contains("z-missing")
            );
            assert!(
                scan.next_entry()
                    .unwrap_err()
                    .to_string()
                    .contains("already failed")
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn streaming_scan_retains_unsupported_tail_without_assigning_an_ordinal() {
        let source = TestDir::new();
        fs::write(source.path().join("a-file"), []).unwrap();
        let _socket =
            std::os::unix::net::UnixListener::bind(source.path().join("z-socket")).unwrap();
        let mut scan = StreamingScan::new(source.path()).unwrap();
        assert_eq!(scan.next_entry().unwrap().unwrap().work_ordinal, 1);
        assert!(scan.next_entry().unwrap().is_none());
        assert_eq!(scan.progress().discovered_entries, 1);
        assert_eq!(
            scan.take_skips(),
            vec![SkippedEntry {
                work_ordinal: 0,
                relative_path: "z-socket".into(),
                reason: SkipReason::UnsupportedEntry,
            }]
        );
    }

    #[cfg(unix)]
    fn assert_colliding_skip_keeps_scan_and_resume_ordinals(source: &Path) {
        let expected = scan_source_tree_with_workers(source, 1).unwrap();
        let skipped_paths = expected
            .skipped
            .iter()
            .map(|skip| skip.relative_path.as_str())
            .collect::<BTreeSet<_>>();
        assert!(!skipped_paths.is_empty());
        assert!(
            expected
                .entries
                .iter()
                .any(|entry| skipped_paths.contains(entry.relative_path.as_str()))
        );
        for workers in [1, 4] {
            for prefix in 0..=expected.entries.len() {
                let mut scan = StreamingScan::with_workers(source, workers).unwrap();
                // Возобновление считает также regular entries с terminal path.
                for expected in &expected.entries[..prefix] {
                    let actual = scan.next_entry().unwrap().unwrap();
                    assert_eq!(actual.work_ordinal, expected.work_ordinal);
                    assert_eq!(actual.abs_path, expected.abs_path);
                    assert_eq!(actual.relative_path, expected.relative_path);
                    assert_eq!(actual.kind, expected.kind);
                }
                for expected in expected.entries[prefix..]
                    .iter()
                    .filter(|entry| !skipped_paths.contains(entry.relative_path.as_str()))
                {
                    let actual = scan.next_pending_entry().unwrap().unwrap();
                    assert_eq!(actual.work_ordinal, expected.work_ordinal);
                    assert_eq!(actual.abs_path, expected.abs_path);
                    assert_eq!(actual.relative_path, expected.relative_path);
                }
                assert!(scan.next_pending_entry().unwrap().is_none());
                assert_eq!(scan.take_skips(), expected.skipped);
                assert_eq!(
                    scan.progress().discovered_entries,
                    expected.entries.len() as u64
                );
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn backslash_skip_suppresses_earlier_nested_file_without_changing_ordinals() {
        let source = TestDir::new();
        fs::create_dir(source.path().join("a")).unwrap();
        fs::create_dir(source.path().join("c")).unwrap();
        fs::write(source.path().join("a/b"), b"must be suppressed").unwrap();
        fs::write(source.path().join("c/file"), b"keep").unwrap();
        let _socket = std::os::unix::net::UnixListener::bind(source.path().join("a\\b")).unwrap();
        assert_colliding_skip_keeps_scan_and_resume_ordinals(source.path());
    }

    #[cfg(unix)]
    #[test]
    fn lossy_sibling_directories_share_skip_suppression_before_parallel_scan() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let source = TestDir::new();
        let regular_dir = source.path().join(OsString::from_vec(b"dir-\xfe".to_vec()));
        let skipped_dir = source.path().join(OsString::from_vec(b"dir-\xff".to_vec()));
        fs::create_dir(&regular_dir).unwrap();
        fs::create_dir(&skipped_dir).unwrap();
        fs::write(regular_dir.join("same"), b"must be suppressed").unwrap();
        fs::write(regular_dir.join("keep"), b"keep").unwrap();
        fs::write(skipped_dir.join("tail"), b"keep too").unwrap();
        let _socket = std::os::unix::net::UnixListener::bind(skipped_dir.join("same")).unwrap();
        assert_colliding_skip_keeps_scan_and_resume_ordinals(source.path());
    }

    #[test]
    fn ambiguous_collection_cancellation_precedes_subtree_enumeration() {
        let source = TestDir::new();
        let missing = source.path().join("directory");
        fs::create_dir(&missing).unwrap();
        let entries = read_sorted_entries(source.path(), "").unwrap();
        fs::remove_dir(missing).unwrap();
        let error = collect_ambiguous_subtree(entries, &AtomicBool::new(true))
            .err()
            .expect("cancelled fallback unexpectedly completed");
        assert!(error.to_string().contains("source scan cancelled"));
    }

    #[test]
    fn relative_paths_normalize_boundary_backslashes_and_preserve_unicode() {
        for directory in ["", "общий/🙂"] {
            for (name, normalized) in [
                ("plain", "plain"),
                ("\\leading", "/leading"),
                ("trailing\\", "trailing/"),
                ("two\\\\slashes", "two//slashes"),
                ("é\\🙂", "é/🙂"),
                ("lossy�", "lossy�"),
            ] {
                let expected = if directory.is_empty() {
                    normalized.to_owned()
                } else {
                    format!("{directory}/{normalized}")
                };
                assert_eq!(
                    relative_entry_path(directory, Cow::Borrowed(name)),
                    expected
                );
                assert_eq!(
                    relative_entry_path(directory, Cow::Owned(name.to_owned())),
                    expected
                );
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn scanning_preserves_raw_names_when_lossy_unicode_collides() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let source = TestDir::new();
        let directory = source.path().join(OsString::from_vec(b"dir-\xfe".to_vec()));
        fs::create_dir(&directory).unwrap();
        for name in [b"same-\xfe".as_slice(), b"same-\xff", b"slash-\\-\xff"] {
            fs::write(directory.join(OsString::from_vec(name.to_vec())), name).unwrap();
        }
        // Сравниваем с прежним Path::join и stable sort, включая обратное чтение ties.
        let mut expected = fs::read_dir(&directory)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                let relative = format!(
                    "dir-�/{}",
                    path.file_name()
                        .unwrap()
                        .to_string_lossy()
                        .replace('\\', "/")
                );
                (path, relative)
            })
            .collect::<Vec<_>>();
        expected.sort_by(|left, right| right.1.cmp(&left.1));
        expected.reverse();
        let report = scan_source_tree_with_workers(source.path(), 1).unwrap();
        assert_eq!(report.entries.len(), expected.len());
        for (entry, (path, relative)) in report.entries.iter().zip(expected) {
            assert_eq!(entry.abs_path.as_os_str(), path.as_os_str());
            assert_eq!(entry.relative_path, relative);
            assert_eq!(fs::read(&entry.abs_path).unwrap(), fs::read(path).unwrap());
        }
    }

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
                    ambiguous_name: false,
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

#[cfg(all(test, windows))]
mod windows_path_tests {
    use super::*;
    use std::ffi::OsString;
    use std::os::windows::ffi::OsStringExt;

    #[test]
    fn presized_native_paths_preserve_windows_join_and_raw_unicode() {
        let names = [
            OsString::from("plain.bin"),
            OsString::from("é-🙂.bin"),
            // Проверяем native code units, не представимые как UTF-8.
            OsString::from_wide(&[0x0066, 0xd800, 0x0078]),
        ];
        for root in [
            "",
            ".",
            "relative",
            r"C:",
            r"C:\",
            r"C:\каталог",
            r"C:\каталог\",
            r"\\server\share\каталог",
            r"\\?\C:\каталог",
            r"\\?\C:\first\..\каталог",
        ] {
            let root = Path::new(root);
            for name in &names {
                let expected = root.join(name);
                let actual = joined_scan_entry_path(root, name);
                assert_eq!(actual.as_os_str(), expected.as_os_str());
                assert_eq!(
                    actual.file_name().unwrap().to_string_lossy(),
                    name.to_string_lossy()
                );
            }
        }
    }

    #[test]
    fn presized_windows_scan_retains_native_and_normalized_unicode_paths() {
        let source = crate::tests::TestDir::new();
        let directory = source.path().join("каталог-🙂");
        fs::create_dir(&directory).unwrap();
        for name in ["plain.bin", "é-🙂.bin", "中.bin"] {
            fs::write(directory.join(name), []).unwrap();
        }
        let expected = fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        let actual = read_sorted_entries(&directory, "каталог-🙂").unwrap();
        assert_eq!(actual.len(), expected.len());
        for entry in actual {
            assert!(expected.contains(&entry.abs_path));
            let basename = entry.abs_path.file_name().unwrap().to_string_lossy();
            assert_eq!(entry.relative_path, format!("каталог-🙂/{basename}"));
            assert!(!entry.ambiguous_name);
        }
    }
}

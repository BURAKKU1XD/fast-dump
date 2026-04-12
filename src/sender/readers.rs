use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};
use log::debug;

use super::{InputFile, PipelineProfileCollector};

const LONG_READ_WARNING_AFTER: Duration = Duration::from_secs(30);
const LONG_READ_WARNING_REPEAT: Duration = Duration::from_secs(30);
const LONG_READ_WATCHDOG_POLL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy)]
pub(crate) struct LongReadWarningSchedule {
    first_warning_after: Duration,
    repeat_warning_after: Duration,
    poll_interval: Duration,
}

impl LongReadWarningSchedule {
    const fn default() -> Self {
        Self {
            first_warning_after: LONG_READ_WARNING_AFTER,
            repeat_warning_after: LONG_READ_WARNING_REPEAT,
            poll_interval: LONG_READ_WATCHDOG_POLL,
        }
    }
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::collections::{BTreeMap, VecDeque};
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::path::Path;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::Duration;

    use super::LongReadWarningSchedule;

    static SCHEDULED_READER_TASKS: OnceLock<Mutex<BTreeMap<String, usize>>> = OnceLock::new();
    static BEFORE_OPEN_HOOKS: OnceLock<Mutex<BTreeMap<String, VecDeque<BeforeOpenHook>>>> =
        OnceLock::new();
    static AFTER_OPEN_HOOKS: OnceLock<Mutex<BTreeMap<String, VecDeque<AfterOpenHook>>>> =
        OnceLock::new();
    static FORCED_SHORT_READS: OnceLock<Mutex<BTreeMap<String, VecDeque<u64>>>> = OnceLock::new();
    static LONG_READ_WARNING_SCHEDULE: OnceLock<Mutex<Option<LongReadWarningSchedule>>> =
        OnceLock::new();
    static LONG_READ_WARNINGS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
    static HOOK_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[derive(Debug)]
    enum BeforeOpenHook {
        Fail,
        Sleep(Duration),
    }

    #[derive(Debug)]
    enum AfterOpenHook {
        Append(Vec<u8>),
    }

    fn scheduled_reader_tasks() -> &'static Mutex<BTreeMap<String, usize>> {
        SCHEDULED_READER_TASKS.get_or_init(|| Mutex::new(BTreeMap::new()))
    }

    fn before_open_hooks() -> &'static Mutex<BTreeMap<String, VecDeque<BeforeOpenHook>>> {
        BEFORE_OPEN_HOOKS.get_or_init(|| Mutex::new(BTreeMap::new()))
    }

    fn after_open_hooks() -> &'static Mutex<BTreeMap<String, VecDeque<AfterOpenHook>>> {
        AFTER_OPEN_HOOKS.get_or_init(|| Mutex::new(BTreeMap::new()))
    }

    fn forced_short_reads() -> &'static Mutex<BTreeMap<String, VecDeque<u64>>> {
        FORCED_SHORT_READS.get_or_init(|| Mutex::new(BTreeMap::new()))
    }

    fn long_read_warning_schedule_cell() -> &'static Mutex<Option<LongReadWarningSchedule>> {
        LONG_READ_WARNING_SCHEDULE.get_or_init(|| Mutex::new(None))
    }

    fn long_read_warnings() -> &'static Mutex<Vec<String>> {
        LONG_READ_WARNINGS.get_or_init(|| Mutex::new(Vec::new()))
    }

    pub(crate) fn lock() -> MutexGuard<'static, ()> {
        HOOK_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    fn path_key(path: &Path) -> String {
        path.to_string_lossy().into_owned()
    }

    fn push_before_open(path: &Path, hook: BeforeOpenHook) {
        before_open_hooks()
            .lock()
            .unwrap()
            .entry(path_key(path))
            .or_default()
            .push_back(hook);
    }

    fn push_after_open(path: &Path, hook: AfterOpenHook) {
        after_open_hooks()
            .lock()
            .unwrap()
            .entry(path_key(path))
            .or_default()
            .push_back(hook);
    }

    fn push_forced_short_read(path: &Path, limit: u64) {
        forced_short_reads()
            .lock()
            .unwrap()
            .entry(path_key(path))
            .or_default()
            .push_back(limit);
    }

    pub(crate) fn record_scheduled_reader_task(path: &Path) {
        let mut scheduled = scheduled_reader_tasks().lock().unwrap();
        *scheduled.entry(path_key(path)).or_default() += 1;
    }

    #[allow(dead_code)]
    pub(crate) fn scheduled_reader_task_count(path: &Path) -> usize {
        scheduled_reader_tasks()
            .lock()
            .unwrap()
            .get(&path_key(path))
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn fail_next_open(path: &Path) {
        push_before_open(path, BeforeOpenHook::Fail);
    }

    pub(crate) fn sleep_before_next_open(path: &Path, duration: Duration) {
        push_before_open(path, BeforeOpenHook::Sleep(duration));
    }

    pub(crate) fn append_bytes_after_next_open(path: &Path, bytes: Vec<u8>) {
        push_after_open(path, AfterOpenHook::Append(bytes));
    }

    pub(crate) fn force_short_read_after_next_open(path: &Path, limit: u64) {
        push_forced_short_read(path, limit);
    }

    pub(crate) fn before_open(path: &Path) -> std::io::Result<()> {
        let hook = before_open_hooks()
            .lock()
            .unwrap()
            .get_mut(&path_key(path))
            .and_then(VecDeque::pop_front);
        match hook {
            Some(BeforeOpenHook::Fail) => Err(std::io::Error::other(
                "sender test hook injected open failure",
            )),
            Some(BeforeOpenHook::Sleep(duration)) => {
                std::thread::sleep(duration);
                Ok(())
            }
            None => Ok(()),
        }
    }

    pub(crate) fn after_open(path: &Path) -> std::io::Result<()> {
        let hook = after_open_hooks()
            .lock()
            .unwrap()
            .get_mut(&path_key(path))
            .and_then(VecDeque::pop_front);
        match hook {
            Some(AfterOpenHook::Append(bytes)) => {
                let mut file = OpenOptions::new().append(true).open(path)?;
                file.write_all(&bytes)?;
                Ok(())
            }
            None => Ok(()),
        }
    }

    pub(crate) fn forced_short_read_after_open(path: &Path) -> Option<u64> {
        forced_short_reads()
            .lock()
            .unwrap()
            .get_mut(&path_key(path))
            .and_then(VecDeque::pop_front)
    }

    pub(crate) fn set_long_read_warning_schedule(schedule: LongReadWarningSchedule) {
        *long_read_warning_schedule_cell().lock().unwrap() = Some(schedule);
    }

    pub(crate) fn long_read_warning_schedule() -> Option<LongReadWarningSchedule> {
        *long_read_warning_schedule_cell().lock().unwrap()
    }

    pub(crate) fn record_long_read_warning(message: String) {
        long_read_warnings().lock().unwrap().push(message);
    }

    pub(crate) fn recorded_long_read_warnings() -> Vec<String> {
        long_read_warnings().lock().unwrap().clone()
    }

    pub(crate) fn clear() {
        before_open_hooks().lock().unwrap().clear();
        after_open_hooks().lock().unwrap().clear();
        forced_short_reads().lock().unwrap().clear();
        *long_read_warning_schedule_cell().lock().unwrap() = None;
        long_read_warnings().lock().unwrap().clear();
    }
}

#[cfg(not(test))]
pub(crate) mod test_hooks {
    use std::path::Path;

    use super::LongReadWarningSchedule;

    pub(crate) fn record_scheduled_reader_task(_path: &Path) {}

    pub(crate) fn after_open(_path: &Path) -> std::io::Result<()> {
        Ok(())
    }

    pub(crate) fn before_open(_path: &Path) -> std::io::Result<()> {
        Ok(())
    }

    pub(crate) fn forced_short_read_after_open(_path: &Path) -> Option<u64> {
        None
    }

    #[allow(dead_code)]
    pub(crate) fn long_read_warning_schedule() -> Option<LongReadWarningSchedule> {
        None
    }
}

#[derive(Debug)]
struct ReaderTask {
    file: InputFile,
    read_buffer_bytes: usize,
    max_attempts: u32,
    profile: Option<PipelineProfileCollector>,
}

#[derive(Debug)]
enum ReaderCommand {
    Read(ReaderTask),
    Stop,
}

pub(crate) const MAX_SNAPSHOT_ATTEMPTS: u32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileDisposition {
    Clean,
    BestEffort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileSkipReason {
    OpenFailed,
    UnstableAfterRetries,
}

pub(crate) const IN_MEMORY_SNAPSHOT_THRESHOLD_BYTES: u64 = 64 * 1024;

#[derive(Debug)]
struct ActiveLongRead {
    path: String,
    stage: &'static str,
    attempt: u32,
    started_at: Instant,
    warnings_emitted: u32,
}

#[derive(Debug)]
struct PendingLongReadWarning {
    path: String,
    stage: &'static str,
    attempt: u32,
    elapsed: Duration,
}

#[derive(Default)]
struct LongReadMonitor {
    state: Mutex<LongReadMonitorState>,
    wake: Condvar,
}

#[derive(Default)]
struct LongReadMonitorState {
    started: bool,
    active: BTreeMap<u64, ActiveLongRead>,
}

pub(crate) struct LongReadWatchdog {
    monitor: &'static LongReadMonitor,
    id: u64,
}

static LONG_READ_MONITOR: OnceLock<LongReadMonitor> = OnceLock::new();
static NEXT_LONG_READ_ID: AtomicU64 = AtomicU64::new(1);

fn long_read_monitor() -> &'static LongReadMonitor {
    LONG_READ_MONITOR.get_or_init(LongReadMonitor::default)
}

fn long_read_warning_schedule() -> LongReadWarningSchedule {
    #[cfg(test)]
    if let Some(schedule) = test_hooks::long_read_warning_schedule() {
        return schedule;
    }

    LongReadWarningSchedule::default()
}

fn spawn_long_read_monitor_thread(monitor: &'static LongReadMonitor) {
    let _ = thread::Builder::new()
        .name("sender-read-watchdog".to_owned())
        .spawn(move || run_long_read_monitor(monitor));
}

fn next_long_read_warning_threshold(
    schedule: LongReadWarningSchedule,
    warnings_emitted: u32,
) -> Duration {
    schedule
        .repeat_warning_after
        .checked_mul(warnings_emitted)
        .and_then(|repeat| schedule.first_warning_after.checked_add(repeat))
        .unwrap_or(Duration::MAX)
}

fn format_long_read_warning(warning: &PendingLongReadWarning) -> String {
    format!(
        "warning: sender file read is still in progress: stage={} attempt={} elapsed_ms={} path={}",
        warning.stage,
        warning.attempt,
        warning.elapsed.as_millis(),
        warning.path
    )
}

fn emit_long_read_warning(warning: PendingLongReadWarning) {
    let message = format_long_read_warning(&warning);
    eprintln!("{message}");
    #[cfg(test)]
    test_hooks::record_long_read_warning(message);
}

fn run_long_read_monitor(monitor: &'static LongReadMonitor) {
    loop {
        let mut state = monitor.state.lock().unwrap();
        while state.active.is_empty() {
            state = monitor.wake.wait(state).unwrap();
        }

        let poll_interval = long_read_warning_schedule().poll_interval;
        let (mut state, _) = monitor.wake.wait_timeout(state, poll_interval).unwrap();
        if state.active.is_empty() {
            continue;
        }

        let schedule = long_read_warning_schedule();
        let now = Instant::now();
        let mut warnings = Vec::new();
        for active in state.active.values_mut() {
            let elapsed = now.saturating_duration_since(active.started_at);
            let threshold = next_long_read_warning_threshold(schedule, active.warnings_emitted);
            if elapsed < threshold {
                continue;
            }

            active.warnings_emitted = active.warnings_emitted.saturating_add(1);
            warnings.push(PendingLongReadWarning {
                path: active.path.clone(),
                stage: active.stage,
                attempt: active.attempt,
                elapsed,
            });
        }
        drop(state);

        for warning in warnings {
            emit_long_read_warning(warning);
        }
    }
}

pub(crate) fn start_long_read_watchdog(
    path: &Path,
    stage: &'static str,
    attempt: u32,
) -> LongReadWatchdog {
    let monitor = long_read_monitor();
    let id = NEXT_LONG_READ_ID.fetch_add(1, Ordering::Relaxed);
    let mut state = monitor.state.lock().unwrap();
    if !state.started {
        state.started = true;
        spawn_long_read_monitor_thread(monitor);
    }
    state.active.insert(
        id,
        ActiveLongRead {
            path: path.display().to_string(),
            stage,
            attempt,
            started_at: Instant::now(),
            warnings_emitted: 0,
        },
    );
    drop(state);
    monitor.wake.notify_one();

    LongReadWatchdog { monitor, id }
}

impl Drop for LongReadWatchdog {
    fn drop(&mut self) {
        let mut state = self.monitor.state.lock().unwrap();
        state.active.remove(&self.id);
        drop(state);
        self.monitor.wake.notify_one();
    }
}

#[derive(Debug)]
pub(crate) enum SnapshotStorage {
    Memory(Vec<u8>),
}

#[derive(Debug)]
pub(crate) struct FileSnapshot {
    pub(crate) file: InputFile,
    pub(crate) size_at_open: u64,
    pub(crate) storage: SnapshotStorage,
    pub(crate) disposition: FileDisposition,
    pub(crate) retries: u32,
}

#[cfg(test)]
impl FileSnapshot {
    fn is_in_memory(&self) -> bool {
        matches!(self.storage, SnapshotStorage::Memory(_))
    }

    fn bytes_for_test(&self) -> Vec<u8> {
        match &self.storage {
            SnapshotStorage::Memory(bytes) => bytes.clone(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct DirectFileStream {
    pub(crate) file: InputFile,
    pub(crate) input: File,
    pub(crate) size_at_open: u64,
    pub(crate) forced_short_read_limit: Option<u64>,
    pub(crate) disposition: FileDisposition,
    pub(crate) retries: u32,
    pub(crate) read_started: Instant,
    pub(crate) read_buffer_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct SkippedFile {
    pub(crate) file: InputFile,
    pub(crate) reason: FileSkipReason,
    pub(crate) retries: u32,
    pub(crate) last_error: String,
}

#[derive(Debug)]
pub(crate) enum FileReadOutcome {
    Snapshot(FileSnapshot),
    Direct(DirectFileStream),
    Skip(SkippedFile),
}

#[derive(Debug)]
pub(crate) enum ReadEvent {
    Ready(FileReadOutcome),
    Fatal { file_index: u64, message: String },
}

impl ReadEvent {
    fn file_index(&self) -> u64 {
        match self {
            Self::Ready(FileReadOutcome::Snapshot(snapshot)) => snapshot.file.index,
            Self::Ready(FileReadOutcome::Direct(direct)) => direct.file.index,
            Self::Ready(FileReadOutcome::Skip(skipped)) => skipped.file.index,
            Self::Fatal { file_index, .. } => *file_index,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AdaptiveReaderScaling {
    min_readers: usize,
    max_readers: usize,
    target_workers: usize,
    empty_observations: usize,
    full_observations: usize,
    scale_up_cooldown: usize,
    bytes_since_scale_check: u64,
    last_growth_window_bytes: Option<u64>,
}

impl AdaptiveReaderScaling {
    fn new(pending_len: usize, min_readers: usize, max_readers: usize) -> Self {
        let max_readers = pending_len.max(1).min(max_readers.max(1));
        let min_readers = pending_len.min(min_readers.max(1)).min(max_readers);

        Self {
            min_readers,
            max_readers,
            target_workers: min_readers,
            empty_observations: 0,
            full_observations: 0,
            scale_up_cooldown: 0,
            bytes_since_scale_check: 0,
            last_growth_window_bytes: None,
        }
    }

    fn target_workers(&self) -> usize {
        self.target_workers
    }

    fn note_bytes_consumed(&mut self, bytes: usize) {
        self.bytes_since_scale_check = self.bytes_since_scale_check.saturating_add(bytes as u64);
    }

    fn note_waiting_for_input(&mut self, pending_remaining: usize) -> bool {
        self.full_observations = 0;
        if self.scale_up_cooldown > 0 {
            self.scale_up_cooldown -= 1;
            self.empty_observations = 0;
            return false;
        }
        if pending_remaining <= self.target_workers || self.target_workers >= self.max_readers {
            self.empty_observations = 0;
            return false;
        }

        self.empty_observations += 1;
        if self.empty_observations < 2 {
            return false;
        }

        self.empty_observations = 0;
        // Новый рост reader-пула имеет смысл только если прошлое расширение
        // дало больший объём реально потреблённых байт между окнами starvation.
        let window_bytes = std::mem::take(&mut self.bytes_since_scale_check);
        if let Some(previous_window_bytes) = self.last_growth_window_bytes
            && window_bytes <= previous_window_bytes
        {
            return false;
        }
        self.last_growth_window_bytes = Some(window_bytes);
        self.target_workers += 1;
        // Небольшой cooldown не даёт пулу "взрываться" из-за кратковременного пустого окна.
        self.scale_up_cooldown = 1;
        true
    }

    fn note_queue_pressure(&mut self, queue_len: usize, queue_capacity: usize) -> bool {
        self.empty_observations = 0;
        if self.scale_up_cooldown > 0 {
            self.scale_up_cooldown -= 1;
        }
        if self.target_workers <= self.min_readers || queue_capacity == 0 {
            self.full_observations = 0;
            return false;
        }

        // Если очередь близка к насыщению несколько наблюдений подряд, текущий рост readers
        // уже не помогает компрессору и только держит лишние буферы в памяти.
        let near_full_threshold = queue_capacity.saturating_sub(self.target_workers).max(1);
        if queue_len < near_full_threshold {
            self.full_observations = 0;
            return false;
        }

        self.full_observations += 1;
        if self.full_observations < 2 {
            return false;
        }

        self.full_observations = 0;
        self.target_workers -= 1;
        self.bytes_since_scale_check = 0;
        self.last_growth_window_bytes = None;
        self.scale_up_cooldown = 3;
        true
    }
}

pub(crate) struct ReaderPool {
    task_tx: Sender<ReaderCommand>,
    task_rx: Receiver<ReaderCommand>,
    event_tx: Sender<ReadEvent>,
    event_rx: Receiver<ReadEvent>,
    handles: Vec<thread::JoinHandle<()>>,
    buffered: BTreeMap<u64, VecDeque<ReadEvent>>,
    worker_limit: usize,
    active_workers: usize,
    min_workers: usize,
    event_capacity: usize,
    scaling: AdaptiveReaderScaling,
    read_buffer_bytes: usize,
    profile: Option<PipelineProfileCollector>,
}

impl ReaderPool {
    pub(crate) fn new(
        pending_len: usize,
        min_readers: usize,
        max_readers: usize,
        read_buffer_bytes: usize,
        profile: Option<PipelineProfileCollector>,
    ) -> Self {
        let scaling = AdaptiveReaderScaling::new(pending_len, min_readers, max_readers);
        let worker_limit = scaling.max_readers.max(1);
        let initial_workers = scaling.target_workers();
        let event_capacity = (worker_limit * 2).max(1);
        let (task_tx, task_rx) = bounded(worker_limit.max(1));
        let (event_tx, event_rx) = bounded(event_capacity);
        let mut pool = Self {
            task_tx,
            task_rx,
            event_tx,
            event_rx,
            handles: Vec::new(),
            buffered: BTreeMap::new(),
            worker_limit,
            active_workers: 0,
            min_workers: scaling.min_readers,
            event_capacity,
            scaling,
            read_buffer_bytes,
            profile,
        };

        for _ in 0..initial_workers {
            pool.spawn_worker();
        }
        pool
    }

    pub(crate) fn prefetch_target(&self) -> usize {
        self.active_workers.max(1)
    }

    pub(crate) fn schedule_with_attempt_limit(
        &mut self,
        file: InputFile,
        max_attempts: u32,
    ) -> Result<()> {
        test_hooks::record_scheduled_reader_task(file.abs_path());
        self.task_tx
            .send(ReaderCommand::Read(ReaderTask {
                file,
                read_buffer_bytes: self.read_buffer_bytes,
                max_attempts: max_attempts.max(1),
                profile: self.profile.clone(),
            }))
            .context("failed to schedule sender reader task")
    }

    pub(crate) fn next_for_with_scheduler<F>(
        &mut self,
        file_index: u64,
        mut schedule_more: F,
    ) -> Result<ReadEvent>
    where
        F: FnMut(&mut Self) -> Result<(usize, bool)>,
    {
        loop {
            self.maybe_scale_down()?;

            if let Some(event) = self.take_buffered_event(file_index) {
                self.note_event_consumed(&event);
                return Ok(event);
            }

            let (scheduled_not_consumed, has_more_to_schedule) = schedule_more(self)?;

            // Когда compressor/network ждёт следующий file boundary, sender может
            // расширить reader pool, но только если за текущим окном ещё есть работа.
            if self.event_rx.is_empty() {
                let pending_hint = if has_more_to_schedule {
                    scheduled_not_consumed.max(self.scaling.target_workers().saturating_add(1))
                } else {
                    scheduled_not_consumed
                };
                if self.scaling.note_waiting_for_input(pending_hint) {
                    self.spawn_additional_reader();
                    let _ = schedule_more(self)?;
                }
            }

            let event = self
                .event_rx
                .recv()
                .context("sender reader pipeline disconnected unexpectedly")?;
            self.buffered
                .entry(event.file_index())
                .or_default()
                .push_back(event);
            let _ = schedule_more(self)?;
        }
    }

    fn take_buffered_event(&mut self, file_index: u64) -> Option<ReadEvent> {
        let events = self.buffered.get_mut(&file_index)?;
        let event = events.pop_front()?;
        if events.is_empty() {
            self.buffered.remove(&file_index);
        }
        Some(event)
    }

    fn note_event_consumed(&mut self, event: &ReadEvent) {
        match event {
            ReadEvent::Ready(FileReadOutcome::Snapshot(snapshot)) => self
                .scaling
                .note_bytes_consumed(snapshot.size_at_open.min(usize::MAX as u64) as usize),
            ReadEvent::Ready(FileReadOutcome::Direct(direct)) => self
                .scaling
                .note_bytes_consumed(direct.size_at_open.min(usize::MAX as u64) as usize),
            _ => {}
        }
    }

    fn maybe_scale_down(&mut self) -> Result<()> {
        if self.active_workers <= self.min_workers {
            return Ok(());
        }
        if !self
            .scaling
            .note_queue_pressure(self.event_rx.len(), self.event_capacity)
        {
            return Ok(());
        }

        self.active_workers -= 1;
        match self.task_tx.try_send(ReaderCommand::Stop) {
            Ok(()) => {
                debug!(
                    "sender reader pool scaled down: active_workers={} target_workers={} queue_len={} queue_capacity={}",
                    self.active_workers,
                    self.scaling.target_workers,
                    self.event_rx.len(),
                    self.event_capacity
                );
            }
            Err(TrySendError::Full(_)) => {
                self.active_workers += 1;
                self.scaling.target_workers += 1;
                self.scaling.scale_up_cooldown = 0;
                debug!(
                    "sender reader pool scale-down cancelled because stop queue is full: active_workers={} target_workers={}",
                    self.active_workers, self.scaling.target_workers
                );
            }
            Err(TrySendError::Disconnected(_)) => {
                return Err(anyhow::anyhow!("failed to schedule sender reader shutdown"));
            }
        }
        Ok(())
    }

    fn spawn_additional_reader(&mut self) {
        if self.active_workers >= self.worker_limit {
            return;
        }
        debug!(
            "sender reader pool scaling up: active_workers={} target_workers={} worker_limit={}",
            self.active_workers, self.scaling.target_workers, self.worker_limit
        );
        self.spawn_worker();
    }

    fn spawn_worker(&mut self) {
        self.active_workers += 1;
        let task_rx = self.task_rx.clone();
        let event_tx = self.event_tx.clone();
        self.handles.push(thread::spawn(move || {
            while let Ok(command) = task_rx.recv() {
                match command {
                    ReaderCommand::Read(task) => {
                        if read_file_into_events(task, &event_tx).is_err() {
                            break;
                        }
                    }
                    ReaderCommand::Stop => break,
                }
            }
        }));
    }

    pub(crate) fn finish(self) {
        drop(self);
    }
}

impl Drop for ReaderPool {
    fn drop(&mut self) {
        let (task_tx, task_rx) = bounded::<ReaderCommand>(0);
        let (event_tx, event_rx) = bounded::<ReadEvent>(0);
        let _ = std::mem::replace(&mut self.task_tx, task_tx);
        let _ = std::mem::replace(&mut self.task_rx, task_rx);
        let _ = std::mem::replace(&mut self.event_tx, event_tx);
        let _ = std::mem::replace(&mut self.event_rx, event_rx);

        for handle in std::mem::take(&mut self.handles) {
            let _ = handle.join();
        }
    }
}

fn read_file_into_events(
    task: ReaderTask,
    event_tx: &Sender<ReadEvent>,
) -> std::result::Result<(), ()> {
    let file_index = task.file.index;
    let event = match read_file_outcome(task) {
        Ok(outcome) => ReadEvent::Ready(outcome),
        Err(message) => ReadEvent::Fatal {
            file_index,
            message,
        },
    };
    event_tx.send(event).map_err(|_| ())
}

#[derive(Debug)]
enum AttemptFailure {
    Open(String),
    Retryable(String),
    Fatal(String),
}

fn read_file_outcome(task: ReaderTask) -> std::result::Result<FileReadOutcome, String> {
    let ReaderTask {
        file,
        read_buffer_bytes,
        max_attempts,
        profile,
    } = task;
    let mut saw_retryable_failure = false;

    for attempt in 1..=max_attempts.max(1) {
        match capture_snapshot_attempt(&file, read_buffer_bytes, profile.as_ref(), attempt) {
            Ok(outcome) => return Ok(outcome),
            Err(AttemptFailure::Open(message)) => {
                debug!(
                    "sender snapshot open attempt failed: file_index={} path={} attempt={} message={}",
                    file.index,
                    file.rel_path(),
                    attempt,
                    message
                );
                if attempt == max_attempts.max(1) {
                    return Ok(FileReadOutcome::Skip(SkippedFile {
                        file,
                        reason: if saw_retryable_failure {
                            FileSkipReason::UnstableAfterRetries
                        } else {
                            FileSkipReason::OpenFailed
                        },
                        retries: attempt.saturating_sub(1),
                        last_error: message,
                    }));
                }
            }
            Err(AttemptFailure::Retryable(message)) => {
                saw_retryable_failure = true;
                debug!(
                    "sender snapshot attempt will retry: file_index={} path={} attempt={} message={}",
                    file.index,
                    file.rel_path(),
                    attempt,
                    message
                );
                if attempt == max_attempts.max(1) {
                    return Ok(FileReadOutcome::Skip(SkippedFile {
                        file,
                        reason: FileSkipReason::UnstableAfterRetries,
                        retries: attempt.saturating_sub(1),
                        last_error: message,
                    }));
                }
            }
            Err(AttemptFailure::Fatal(message)) => return Err(message),
        }
    }

    unreachable!("snapshot loop must return on success or retry exhaustion")
}

fn capture_snapshot_attempt(
    file: &InputFile,
    read_buffer_bytes: usize,
    profile: Option<&PipelineProfileCollector>,
    attempt: u32,
) -> std::result::Result<FileReadOutcome, AttemptFailure> {
    let read_started = Instant::now();
    // Отдельный watchdog нужен, потому что blocking open/read может зависнуть
    // без ошибки и без возврата управления в reader worker.
    let _watchdog = start_long_read_watchdog(file.abs_path(), "snapshot-capture", attempt);
    test_hooks::before_open(file.abs_path()).map_err(|error| {
        AttemptFailure::Open(format!(
            "failed to prepare sender file {} before open: {error}",
            file.abs_path().display()
        ))
    })?;

    let mut input = file.open_for_read().map_err(|error| {
        AttemptFailure::Open(format!(
            "failed to open sender file {}: {error}",
            file.abs_path().display()
        ))
    })?;
    if let Some(profile) = profile {
        profile.record_dirfd_depth(file.dirfd_depth());
    }
    let size_at_open = input
        .metadata()
        .map_err(|error| {
            AttemptFailure::Retryable(format!(
                "failed to read sender file metadata from opened handle {}: {error}",
                file.abs_path().display()
            ))
        })?
        .len();
    test_hooks::after_open(file.abs_path()).map_err(|error| {
        AttemptFailure::Fatal(format!(
            "failed to prepare sender file {} after open: {error}",
            file.abs_path().display()
        ))
    })?;
    let forced_short_read_limit = test_hooks::forced_short_read_after_open(file.abs_path());
    if size_at_open > IN_MEMORY_SNAPSHOT_THRESHOLD_BYTES {
        return Ok(FileReadOutcome::Direct(DirectFileStream {
            file: file.clone(),
            input,
            size_at_open,
            forced_short_read_limit,
            disposition: if attempt == 1 {
                FileDisposition::Clean
            } else {
                FileDisposition::BestEffort
            },
            retries: attempt.saturating_sub(1),
            read_started,
            read_buffer_bytes,
        }));
    }

    let mut storage = SnapshotStorage::Memory(Vec::with_capacity(size_at_open as usize));
    let mut buffer = vec![0_u8; read_buffer_bytes];
    let mut remaining = size_at_open;
    let mut read_buffers = 0_u64;
    let mut exposed_bytes = 0_u64;

    while remaining > 0 {
        if let Some(limit) = forced_short_read_limit
            && exposed_bytes >= limit
        {
            return Err(AttemptFailure::Retryable(format!(
                "sender file {} reached EOF before size_at_open {} bytes",
                file.abs_path().display(),
                size_at_open
            )));
        }
        let mut read_limit = buffer.len().min(remaining as usize);
        if let Some(limit) = forced_short_read_limit {
            read_limit = read_limit.min(limit.saturating_sub(exposed_bytes) as usize);
        }
        let read = input.read(&mut buffer[..read_limit]).map_err(|error| {
            AttemptFailure::Retryable(format!(
                "failed to read sender file {}: {error}",
                file.abs_path().display()
            ))
        })?;
        if read == 0 {
            return Err(AttemptFailure::Retryable(format!(
                "sender file {} reached EOF before size_at_open {} bytes",
                file.abs_path().display(),
                size_at_open
            )));
        }
        match &mut storage {
            SnapshotStorage::Memory(bytes) => bytes.extend_from_slice(&buffer[..read]),
        }
        remaining -= read as u64;
        exposed_bytes += read as u64;
        read_buffers += 1;
    }

    let read_finished = Instant::now();
    debug!(
        "sender reader captured snapshot: file_index={} path={} size_at_open={} attempt={} elapsed_ms={}",
        file.index,
        file.rel_path(),
        size_at_open,
        attempt,
        read_finished
            .saturating_duration_since(read_started)
            .as_millis()
    );
    if let Some(profile) = profile {
        profile.record_file_read(
            file.index,
            file.rel_path(),
            size_at_open,
            read_buffers,
            read_started,
            read_finished,
        );
    }

    Ok(FileReadOutcome::Snapshot(FileSnapshot {
        file: file.clone(),
        size_at_open,
        storage,
        disposition: if attempt == 1 {
            FileDisposition::Clean
        } else {
            FileDisposition::BestEffort
        },
        retries: attempt.saturating_sub(1),
    }))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::MutexGuard;
    use std::thread;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use crossbeam_channel::bounded;

    use super::{
        AdaptiveReaderScaling, FileDisposition, FileReadOutcome, FileSkipReason, FileSnapshot,
        IN_MEMORY_SNAPSHOT_THRESHOLD_BYTES, LongReadWarningSchedule, MAX_SNAPSHOT_ATTEMPTS,
        ReadEvent, ReaderCommand, ReaderPool, ReaderTask, SkippedFile, SnapshotStorage,
        read_file_into_events,
    };
    use crate::sender::InputFile;

    struct TestDir {
        path: PathBuf,
    }

    struct ReadHookGuard {
        _lock: MutexGuard<'static, ()>,
    }

    impl TestDir {
        fn new() -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "fast-dump-sender-readers-tests-{}-{}",
                std::process::id(),
                unique
            ));
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    impl ReadHookGuard {
        fn new() -> Self {
            let lock = super::test_hooks::lock();
            super::test_hooks::clear();
            Self { _lock: lock }
        }
    }

    impl Drop for ReadHookGuard {
        fn drop(&mut self) {
            super::test_hooks::clear();
        }
    }

    fn write_input_file(temp: &TestDir, rel_path: &str, bytes: &[u8], index: u64) -> InputFile {
        let path = temp.path().join(rel_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&path, bytes).unwrap();
        InputFile::regular(index, path, rel_path.replace('\\', "/"))
    }

    #[test]
    fn adaptive_reader_scaling_starts_at_min_and_scales_up_on_repeated_starvation() {
        let mut scaling = AdaptiveReaderScaling::new(8, 2, 5);

        assert_eq!(scaling.target_workers(), 2);
        assert!(!scaling.note_waiting_for_input(8));
        assert!(scaling.note_waiting_for_input(8));
        assert_eq!(scaling.target_workers(), 3);
    }

    #[test]
    fn adaptive_reader_scaling_scales_down_when_queue_stays_near_full() {
        let mut scaling = AdaptiveReaderScaling::new(8, 2, 5);
        assert!(!scaling.note_waiting_for_input(8));
        assert!(scaling.note_waiting_for_input(8));
        assert_eq!(scaling.target_workers(), 3);

        assert!(!scaling.note_queue_pressure(6, 6));
        assert!(scaling.note_queue_pressure(6, 6));
        assert_eq!(scaling.target_workers(), 2);
    }

    #[test]
    fn adaptive_reader_scaling_cooldown_blocks_immediate_regrowth_after_scale_down() {
        let mut scaling = AdaptiveReaderScaling::new(8, 2, 5);
        assert!(!scaling.note_waiting_for_input(8));
        assert!(scaling.note_waiting_for_input(8));
        assert_eq!(scaling.target_workers(), 3);
        assert!(!scaling.note_queue_pressure(6, 6));
        assert!(scaling.note_queue_pressure(6, 6));
        assert_eq!(scaling.target_workers(), 2);

        assert!(!scaling.note_waiting_for_input(8));
        assert!(!scaling.note_waiting_for_input(8));
        assert!(!scaling.note_waiting_for_input(8));
        assert!(!scaling.note_waiting_for_input(8));
        assert!(scaling.note_waiting_for_input(8));
        assert_eq!(scaling.target_workers(), 3);
    }

    #[test]
    fn adaptive_reader_scaling_ignores_queue_pressure_when_scaling_down_is_not_needed() {
        let mut scaling = AdaptiveReaderScaling::new(8, 2, 5);

        assert!(!scaling.note_queue_pressure(1, 8));
        assert_eq!(scaling.target_workers(), 2);

        assert!(!scaling.note_waiting_for_input(8));
        assert!(scaling.note_waiting_for_input(8));
        assert_eq!(scaling.target_workers(), 3);

        assert!(!scaling.note_queue_pressure(1, 8));
        assert_eq!(scaling.target_workers(), 3);
    }

    #[test]
    fn adaptive_reader_scaling_stops_growing_when_throughput_does_not_improve() {
        let mut scaling = AdaptiveReaderScaling::new(8, 2, 5);
        scaling.note_bytes_consumed(100);

        assert!(!scaling.note_waiting_for_input(8));
        assert!(scaling.note_waiting_for_input(8));
        assert_eq!(scaling.target_workers(), 3);

        scaling.note_bytes_consumed(80);
        assert!(!scaling.note_waiting_for_input(8));
        assert!(!scaling.note_waiting_for_input(8));
        assert!(!scaling.note_waiting_for_input(8));
        assert_eq!(scaling.target_workers(), 3);
    }

    #[test]
    fn adaptive_reader_scaling_can_grow_again_after_later_throughput_gain() {
        let mut scaling = AdaptiveReaderScaling::new(8, 2, 5);
        scaling.note_bytes_consumed(100);

        assert!(!scaling.note_waiting_for_input(8));
        assert!(scaling.note_waiting_for_input(8));
        assert_eq!(scaling.target_workers(), 3);

        scaling.note_bytes_consumed(80);
        assert!(!scaling.note_waiting_for_input(8));
        assert!(!scaling.note_waiting_for_input(8));
        assert!(!scaling.note_waiting_for_input(8));
        assert_eq!(scaling.target_workers(), 3);

        scaling.note_bytes_consumed(160);
        assert!(!scaling.note_waiting_for_input(8));
        assert!(scaling.note_waiting_for_input(8));
        assert_eq!(scaling.target_workers(), 4);
    }

    #[test]
    fn adaptive_reader_scaling_resets_throughput_baseline_after_scale_down() {
        let mut scaling = AdaptiveReaderScaling::new(8, 2, 5);
        scaling.note_bytes_consumed(100);

        assert!(!scaling.note_waiting_for_input(8));
        assert!(scaling.note_waiting_for_input(8));
        assert_eq!(scaling.target_workers(), 3);

        assert!(!scaling.note_queue_pressure(6, 6));
        assert!(scaling.note_queue_pressure(6, 6));
        assert_eq!(scaling.target_workers(), 2);

        scaling.note_bytes_consumed(80);
        assert!(!scaling.note_waiting_for_input(8));
        assert!(!scaling.note_waiting_for_input(8));
        assert!(!scaling.note_waiting_for_input(8));
        assert!(!scaling.note_waiting_for_input(8));
        assert!(scaling.note_waiting_for_input(8));
        assert_eq!(scaling.target_workers(), 3);
    }

    fn skipped_event(file_index: u64) -> ReadEvent {
        ReadEvent::Ready(FileReadOutcome::Skip(SkippedFile {
            file: InputFile::regular(
                file_index,
                PathBuf::from(format!("file-{file_index}.bin")),
                format!("file-{file_index}.bin"),
            ),
            reason: FileSkipReason::OpenFailed,
            retries: 2,
            last_error: "failed to open sender file".to_owned(),
        }))
    }

    #[test]
    fn read_event_reports_file_index_for_all_variants() {
        let temp = TestDir::new();
        let file = write_input_file(&temp, "alpha.bin", b"abc", 7);

        assert_eq!(
            ReadEvent::Ready(FileReadOutcome::Snapshot(FileSnapshot {
                file: file.clone(),
                size_at_open: 3,
                storage: SnapshotStorage::Memory(b"abc".to_vec()),
                disposition: FileDisposition::Clean,
                retries: 0,
            }))
            .file_index(),
            7
        );
        assert_eq!(skipped_event(8).file_index(), 8);
        assert_eq!(
            ReadEvent::Fatal {
                file_index: 9,
                message: "boom".to_owned(),
            }
            .file_index(),
            9
        );
    }

    #[test]
    fn read_file_into_events_skips_file_after_open_retry_budget() {
        let _guard = ReadHookGuard::new();
        let temp = TestDir::new();
        let file = write_input_file(&temp, "missing.bin", b"payload", 4);
        for _ in 0..3 {
            super::test_hooks::fail_next_open(file.abs_path());
        }
        let (event_tx, event_rx) = bounded::<ReadEvent>(1);
        let task = ReaderTask {
            file,
            read_buffer_bytes: 8,
            max_attempts: MAX_SNAPSHOT_ATTEMPTS,
            profile: None,
        };

        assert!(read_file_into_events(task, &event_tx).is_ok());
        match event_rx.recv().unwrap() {
            ReadEvent::Ready(FileReadOutcome::Skip(skipped)) => {
                assert_eq!(skipped.file.index, 4);
                assert_eq!(skipped.reason, FileSkipReason::OpenFailed);
                assert_eq!(skipped.retries, 2);
            }
            other => panic!("unexpected read event: {other:?}"),
        }
    }

    #[test]
    fn read_file_into_events_accepts_file_growth_after_open() {
        let _guard = ReadHookGuard::new();
        let temp = TestDir::new();
        let file = write_input_file(&temp, "alpha.bin", b"abc", 1);
        super::test_hooks::append_bytes_after_next_open(file.abs_path(), b"def".to_vec());
        let (event_tx, event_rx) = bounded::<ReadEvent>(1);

        let result = read_file_into_events(
            ReaderTask {
                file,
                read_buffer_bytes: 8,
                max_attempts: MAX_SNAPSHOT_ATTEMPTS,
                profile: None,
            },
            &event_tx,
        );

        assert!(result.is_ok());
        match event_rx.recv().unwrap() {
            ReadEvent::Ready(FileReadOutcome::Snapshot(snapshot)) => {
                assert_eq!(snapshot.file.index, 1);
                assert_eq!(snapshot.size_at_open, 3);
                assert_eq!(snapshot.disposition, FileDisposition::Clean);
                assert_eq!(snapshot.retries, 0);
                assert!(snapshot.is_in_memory());
                assert_eq!(snapshot.bytes_for_test(), b"abc");
            }
            other => panic!("unexpected read event: {other:?}"),
        }
    }

    #[test]
    fn read_file_into_events_retries_short_read_and_marks_best_effort_on_success() {
        let _guard = ReadHookGuard::new();
        let temp = TestDir::new();
        let file = write_input_file(&temp, "alpha.bin", b"abcdef", 1);
        super::test_hooks::force_short_read_after_next_open(file.abs_path(), 3);
        let (event_tx, event_rx) = bounded::<ReadEvent>(1);

        let result = read_file_into_events(
            ReaderTask {
                file,
                read_buffer_bytes: 8,
                max_attempts: MAX_SNAPSHOT_ATTEMPTS,
                profile: None,
            },
            &event_tx,
        );

        assert!(result.is_ok());
        match event_rx.recv().unwrap() {
            ReadEvent::Ready(FileReadOutcome::Snapshot(snapshot)) => {
                assert_eq!(snapshot.file.index, 1);
                assert_eq!(snapshot.size_at_open, 6);
                assert_eq!(snapshot.disposition, FileDisposition::BestEffort);
                assert_eq!(snapshot.retries, 1);
                assert!(snapshot.is_in_memory());
                assert_eq!(snapshot.bytes_for_test(), b"abcdef");
            }
            other => panic!("unexpected read event: {other:?}"),
        }
    }

    #[test]
    fn read_file_into_events_warns_when_open_stays_blocked_too_long() {
        let _guard = ReadHookGuard::new();
        let temp = TestDir::new();
        let file = write_input_file(&temp, "slow.bin", b"abc", 1);
        super::test_hooks::set_long_read_warning_schedule(LongReadWarningSchedule {
            first_warning_after: Duration::from_millis(20),
            repeat_warning_after: Duration::from_millis(20),
            poll_interval: Duration::from_millis(5),
        });
        super::test_hooks::sleep_before_next_open(file.abs_path(), Duration::from_millis(60));
        let (event_tx, event_rx) = bounded::<ReadEvent>(1);

        let result = read_file_into_events(
            ReaderTask {
                file: file.clone(),
                read_buffer_bytes: 8,
                max_attempts: MAX_SNAPSHOT_ATTEMPTS,
                profile: None,
            },
            &event_tx,
        );

        assert!(result.is_ok());
        match event_rx.recv().unwrap() {
            ReadEvent::Ready(FileReadOutcome::Snapshot(snapshot)) => {
                assert_eq!(snapshot.file.index, 1);
                assert_eq!(snapshot.bytes_for_test(), b"abc");
            }
            other => panic!("unexpected read event: {other:?}"),
        }

        let deadline = Instant::now() + Duration::from_millis(250);
        loop {
            let warnings = super::test_hooks::recorded_long_read_warnings();
            if warnings.iter().any(|warning| {
                warning.contains("stage=snapshot-capture")
                    && warning.contains(&file.abs_path().display().to_string())
            }) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "long-read watchdog did not emit warning in time"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn read_file_into_events_returns_direct_stream_for_large_file() {
        let _guard = ReadHookGuard::new();
        let temp = TestDir::new();
        let payload = vec![7_u8; IN_MEMORY_SNAPSHOT_THRESHOLD_BYTES as usize + 1];
        let file = write_input_file(&temp, "large.bin", &payload, 1);
        let (event_tx, event_rx) = bounded::<ReadEvent>(1);

        let result = read_file_into_events(
            ReaderTask {
                file,
                read_buffer_bytes: 8 * 1024,
                max_attempts: MAX_SNAPSHOT_ATTEMPTS,
                profile: None,
            },
            &event_tx,
        );

        assert!(result.is_ok());
        match event_rx.recv().unwrap() {
            ReadEvent::Ready(FileReadOutcome::Direct(direct)) => {
                assert_eq!(direct.file.index, 1);
                assert_eq!(direct.size_at_open, payload.len() as u64);
                assert_eq!(direct.disposition, FileDisposition::Clean);
                assert_eq!(direct.retries, 0);
            }
            other => panic!("unexpected read event: {other:?}"),
        }
    }

    #[test]
    fn read_file_into_events_skips_unstable_file_after_retry_budget() {
        let _guard = ReadHookGuard::new();
        let temp = TestDir::new();
        let file = write_input_file(&temp, "alpha.bin", b"abcdef", 1);
        for _ in 0..3 {
            super::test_hooks::force_short_read_after_next_open(file.abs_path(), 3);
        }
        let (event_tx, event_rx) = bounded::<ReadEvent>(1);

        let result = read_file_into_events(
            ReaderTask {
                file,
                read_buffer_bytes: 8,
                max_attempts: MAX_SNAPSHOT_ATTEMPTS,
                profile: None,
            },
            &event_tx,
        );

        assert!(result.is_ok());
        match event_rx.recv().unwrap() {
            ReadEvent::Ready(FileReadOutcome::Skip(skipped)) => {
                assert_eq!(skipped.file.index, 1);
                assert_eq!(skipped.reason, FileSkipReason::UnstableAfterRetries);
                assert_eq!(skipped.retries, 2);
            }
            other => panic!("unexpected read event: {other:?}"),
        }
    }

    #[test]
    fn read_file_into_events_returns_err_when_ready_consumer_disconnects() {
        let temp = TestDir::new();
        let file = write_input_file(&temp, "empty.bin", b"", 2);
        let (event_tx, event_rx) = bounded::<ReadEvent>(0);
        drop(event_rx);

        let result = read_file_into_events(
            ReaderTask {
                file,
                read_buffer_bytes: 8,
                max_attempts: MAX_SNAPSHOT_ATTEMPTS,
                profile: None,
            },
            &event_tx,
        );

        assert!(result.is_err());
    }

    #[test]
    fn read_file_into_events_returns_err_when_skip_consumer_disconnects() {
        let temp = TestDir::new();
        let (event_tx, event_rx) = bounded::<ReadEvent>(0);
        drop(event_rx);

        let result = read_file_into_events(
            ReaderTask {
                file: InputFile::regular(
                    3,
                    temp.path().join("missing.bin"),
                    "missing.bin".to_owned(),
                ),
                read_buffer_bytes: 8,
                max_attempts: MAX_SNAPSHOT_ATTEMPTS,
                profile: None,
            },
            &event_tx,
        );

        assert!(result.is_err());
    }

    #[test]
    fn schedule_prefetch_reports_disconnected_task_channel() {
        let temp = TestDir::new();
        let pending = write_input_file(&temp, "alpha.bin", b"abc", 1);
        let mut pool = ReaderPool::new(1, 1, 1, 8, None);
        let (task_tx, task_rx) = bounded::<ReaderCommand>(0);
        drop(task_rx);
        pool.task_tx = task_tx;

        let error = pool
            .schedule_with_attempt_limit(pending, MAX_SNAPSHOT_ATTEMPTS)
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to schedule sender reader task")
        );
    }

    #[test]
    fn reader_pool_next_for_reports_pipeline_disconnect() {
        let mut pool = ReaderPool::new(0, 1, 1, 8, None);
        let (event_tx, event_rx) = bounded::<ReadEvent>(0);
        drop(event_tx);
        pool.event_rx = event_rx;

        let error = pool
            .next_for_with_scheduler(1, |_pool| Ok((0, false)))
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("sender reader pipeline disconnected unexpectedly")
        );
    }

    #[test]
    fn maybe_scale_down_restores_counters_when_stop_queue_is_full() {
        let mut pool = ReaderPool::new(1, 1, 2, 8, None);
        let (task_tx, task_rx) = bounded::<ReaderCommand>(1);
        task_tx.send(ReaderCommand::Stop).unwrap();
        pool.task_tx = task_tx;
        pool.task_rx = task_rx;
        pool.active_workers = 2;
        pool.min_workers = 1;
        pool.event_capacity = 1;
        pool.scaling.min_readers = 1;
        pool.scaling.target_workers = 2;
        pool.scaling.full_observations = 1;
        pool.event_tx.send(skipped_event(1)).unwrap();

        pool.maybe_scale_down().unwrap();

        assert_eq!(pool.active_workers, 2);
        assert_eq!(pool.scaling.target_workers, 2);
        assert_eq!(pool.scaling.scale_up_cooldown, 0);
    }

    #[test]
    fn maybe_scale_down_reports_disconnected_task_channel() {
        let mut pool = ReaderPool::new(1, 1, 2, 8, None);
        let (task_tx, task_rx) = bounded::<ReaderCommand>(0);
        drop(task_rx);
        pool.task_tx = task_tx;
        pool.active_workers = 2;
        pool.min_workers = 1;
        pool.event_capacity = 1;
        pool.scaling.min_readers = 1;
        pool.scaling.target_workers = 2;
        pool.scaling.full_observations = 1;
        pool.event_tx.send(skipped_event(1)).unwrap();

        let error = pool.maybe_scale_down().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to schedule sender reader shutdown")
        );
    }
}

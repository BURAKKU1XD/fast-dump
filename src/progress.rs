use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, bounded};
use log::Level;

const LOG_LIMIT: usize = 200;
const FINISHED_LIMIT: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub(crate) enum PipelineStage {
    Scan,
    Read,
    Plan,
    Compress,
    Data,
    Control,
    Storage,
}

impl PipelineStage {
    const COUNT: usize = 7;
    const SENDER: [Self; 6] = [
        Self::Scan,
        Self::Read,
        Self::Plan,
        Self::Compress,
        Self::Data,
        Self::Control,
    ];
    const RECEIVER: [Self; 3] = [Self::Data, Self::Control, Self::Storage];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Scan => "Scan",
            Self::Read => "Read",
            Self::Plan => "Plan",
            Self::Compress => "Compress",
            Self::Data => "Data",
            Self::Control => "Control",
            Self::Storage => "Storage",
        }
    }
}

// Один логический владелец на стадию. Read описывает упорядоченный reader,
// а чтение большого файла в dump-stream видно в строке Compress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum PipelineState {
    Idle,
    Scanning,
    Reading,
    Planning,
    Compressing,
    Sending,
    Receiving,
    Writing,
    Syncing,
    Finalizing,
    Recovering,
    Connecting,
    Handshake,
    WaitingScan,
    WaitingRead,
    WaitingPlan,
    WaitingCompression,
    WaitingData,
    WaitingControl,
    WaitingAck,
    WaitingConsumer,
    WaitingBuffer,
    WaitingLock,
    WaitingEvent,
    WaitingConnection,
    Done,
    Failed,
    Stopped,
}

impl PipelineState {
    const ALL: [Self; 28] = [
        Self::Idle,
        Self::Scanning,
        Self::Reading,
        Self::Planning,
        Self::Compressing,
        Self::Sending,
        Self::Receiving,
        Self::Writing,
        Self::Syncing,
        Self::Finalizing,
        Self::Recovering,
        Self::Connecting,
        Self::Handshake,
        Self::WaitingScan,
        Self::WaitingRead,
        Self::WaitingPlan,
        Self::WaitingCompression,
        Self::WaitingData,
        Self::WaitingControl,
        Self::WaitingAck,
        Self::WaitingConsumer,
        Self::WaitingBuffer,
        Self::WaitingLock,
        Self::WaitingEvent,
        Self::WaitingConnection,
        Self::Done,
        Self::Failed,
        Self::Stopped,
    ];

    fn from_packed(value: u64) -> Self {
        Self::ALL[(value & 0xff) as usize]
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Idle => "Idle",
            Self::Scanning => "Scanning directories",
            Self::Reading => "Reading source",
            Self::Planning => "Building bundle",
            Self::Compressing => "Tar / zstd",
            Self::Sending => "Writing socket",
            Self::Receiving => "Reading socket",
            Self::Writing => "Writing archive",
            Self::Syncing => "Syncing archive / WAL",
            Self::Finalizing => "Finalizing archive",
            Self::Recovering => "Recovering state",
            Self::Connecting => "Connecting",
            Self::Handshake => "Handshake",
            Self::WaitingScan => "Waiting scanner",
            Self::WaitingRead => "Waiting file readers",
            Self::WaitingPlan => "Waiting bundle plan",
            Self::WaitingCompression => "Waiting compressed data",
            Self::WaitingData => "Waiting data",
            Self::WaitingControl => "Waiting control",
            Self::WaitingAck => "Waiting peer ACK",
            Self::WaitingConsumer => "Waiting downstream",
            Self::WaitingBuffer => "Waiting free buffer",
            Self::WaitingLock => "Waiting storage lock",
            Self::WaitingEvent => "Waiting data / control",
            Self::WaitingConnection => "Waiting connection",
            Self::Done => "Done",
            Self::Failed => "Failed",
            Self::Stopped => "Stopped",
        }
    }

    pub(crate) fn is_waiting(self) -> bool {
        matches!(
            self,
            Self::WaitingScan
                | Self::WaitingRead
                | Self::WaitingPlan
                | Self::WaitingCompression
                | Self::WaitingData
                | Self::WaitingControl
                | Self::WaitingAck
                | Self::WaitingConsumer
                | Self::WaitingBuffer
                | Self::WaitingLock
                | Self::WaitingEvent
                | Self::WaitingConnection
        )
    }

    fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Failed | Self::Stopped)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PipelineSnapshot {
    pub(crate) stage: PipelineStage,
    pub(crate) state: PipelineState,
    pub(crate) elapsed: Duration,
}

pub(crate) struct PipelineGuard {
    progress: Option<Arc<SessionProgress>>,
    stage: PipelineStage,
    previous: PipelineState,
}

impl Drop for PipelineGuard {
    fn drop(&mut self) {
        if let Some(progress) = &self.progress {
            let state = PipelineState::from_packed(
                progress.pipelines[self.stage as usize].load(Ordering::Relaxed),
            );
            // Ошибка/завершение должны пережить выход из вложенных I/O scopes.
            if !state.is_terminal() {
                progress.set_stage(self.stage, self.previous);
            }
        }
    }
}

pub(crate) fn stage(
    control: Option<&crate::runtime::SessionControl>,
    stage: PipelineStage,
    state: PipelineState,
) -> PipelineGuard {
    match control.and_then(crate::runtime::SessionControl::progress) {
        Some(progress) => progress.stage(stage, state),
        None => PipelineGuard {
            progress: None,
            stage,
            previous: PipelineState::Idle,
        },
    }
}

pub(crate) fn set_stage(
    control: Option<&crate::runtime::SessionControl>,
    stage: PipelineStage,
    state: PipelineState,
) {
    if let Some(progress) = control.and_then(crate::runtime::SessionControl::progress) {
        progress.set_stage(stage, state);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Phase {
    Connecting,
    Listening,
    Preparing,
    Sending,
    Receiving,
    Finalizing,
    Completed,
    Warnings,
    Stopped,
    Failed,
}

impl Phase {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Connecting => "Connecting",
            Self::Listening => "Listening",
            Self::Preparing => "Preparing",
            Self::Sending => "Sending",
            Self::Receiving => "Receiving",
            Self::Finalizing => "Finalizing",
            Self::Completed => "Completed",
            Self::Warnings => "Completed with warnings",
            Self::Stopped => "Stopped",
            Self::Failed => "Failed",
        }
    }

    pub(crate) fn is_finished(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Warnings | Self::Stopped | Self::Failed
        )
    }

    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Listening,
            2 => Self::Preparing,
            3 => Self::Sending,
            4 => Self::Receiving,
            5 => Self::Finalizing,
            6 => Self::Completed,
            7 => Self::Warnings,
            8 => Self::Stopped,
            9 => Self::Failed,
            _ => Self::Connecting,
        }
    }
}

#[derive(Clone)]
pub(crate) struct LogEntry {
    pub(crate) level: Level,
    pub(crate) message: String,
}

#[derive(Clone)]
pub(crate) struct RetrySnapshot {
    pub(crate) number: u32,
    pub(crate) limit: u32,
    pub(crate) deadline: Instant,
    pub(crate) error: String,
}

#[derive(Clone)]
pub(crate) struct CurrentFileSnapshot {
    pub(crate) relative_path: String,
    pub(crate) size: u64,
    pub(crate) processed_bytes: u64,
}

#[derive(Clone)]
pub(crate) struct SessionSnapshot {
    pub(crate) id: u64,
    pub(crate) role: String,
    pub(crate) dump_id: String,
    pub(crate) peer: String,
    pub(crate) phase: Phase,
    pub(crate) elapsed: Duration,
    pub(crate) discovered: u64,
    pub(crate) scan_finished: bool,
    pub(crate) sent_entries: u64,
    pub(crate) committed_entries: u64,
    pub(crate) transferred_bytes: u64,
    pub(crate) raw_processed_bytes: u64,
    pub(crate) current_file: Option<CurrentFileSnapshot>,
    pub(crate) durable_bytes: u64,
    pub(crate) resumed_bytes: u64,
    pub(crate) skips: u64,
    pub(crate) retries: u64,
    pub(crate) bundles: u64,
    pub(crate) archive: Option<PathBuf>,
    pub(crate) error: Option<String>,
    pub(crate) pipelines: Vec<PipelineSnapshot>,
}

struct Identity {
    role: String,
    dump_id: String,
    peer: String,
    archive: Option<PathBuf>,
    error: Option<String>,
    finished: Option<Instant>,
    finished_raw_bytes: Option<u64>,
    source_file: SourceFileIdentity,
}

#[derive(Default)]
struct SourceFileIdentity {
    active: bool,
    relative_path: String,
    size: u64,
    base_raw_bytes: u64,
}

pub(crate) struct SourceFileGuard<'a> {
    progress: &'a SessionProgress,
}

impl SourceFileGuard<'_> {
    pub(crate) fn processed(&self, bytes: usize) {
        self.progress
            .raw_processed_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

impl Drop for SourceFileGuard<'_> {
    fn drop(&mut self) {
        self.progress
            .identity
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .source_file
            .active = false;
    }
}

pub(crate) struct SessionProgress {
    id: u64,
    started: Instant,
    identity: Mutex<Identity>,
    phase: AtomicU8,
    pipelines: [AtomicU64; PipelineStage::COUNT],
    pub(crate) discovered: AtomicU64,
    pub(crate) scan_finished: AtomicBool,
    pub(crate) sent_entries: AtomicU64,
    pub(crate) committed_entries: AtomicU64,
    pub(crate) transferred_bytes: AtomicU64,
    raw_processed_bytes: AtomicU64,
    pub(crate) durable_bytes: AtomicU64,
    pub(crate) resumed_bytes: AtomicU64,
    pub(crate) skips: AtomicU64,
    pub(crate) retries: AtomicU64,
    pub(crate) bundles: AtomicU64,
}

impl SessionProgress {
    fn new(id: u64, role: String, dump_id: &str, peer: &str) -> Self {
        Self {
            id,
            started: Instant::now(),
            identity: Mutex::new(Identity {
                role,
                dump_id: dump_id.into(),
                peer: peer.into(),
                archive: None,
                error: None,
                finished: None,
                finished_raw_bytes: None,
                source_file: SourceFileIdentity::default(),
            }),
            phase: AtomicU8::new(Phase::Connecting as u8),
            pipelines: std::array::from_fn(|_| AtomicU64::new(0)),
            discovered: AtomicU64::new(0),
            scan_finished: AtomicBool::new(false),
            sent_entries: AtomicU64::new(0),
            committed_entries: AtomicU64::new(0),
            transferred_bytes: AtomicU64::new(0),
            raw_processed_bytes: AtomicU64::new(0),
            durable_bytes: AtomicU64::new(0),
            resumed_bytes: AtomicU64::new(0),
            skips: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            bundles: AtomicU64::new(0),
        }
    }

    pub(crate) fn set_phase(&self, phase: Phase) {
        if phase.is_finished() {
            let mut identity = self.identity.lock().unwrap_or_else(|e| e.into_inner());
            identity.finished.get_or_insert_with(Instant::now);
            identity
                .finished_raw_bytes
                .get_or_insert_with(|| self.raw_processed_bytes.load(Ordering::Relaxed));
            identity.source_file.active = false;
        }
        self.phase.store(phase as u8, Ordering::Relaxed);
    }

    pub(crate) fn source_file(&self, relative_path: &str, size: u64) -> SourceFileGuard<'_> {
        let mut identity = self.identity.lock().unwrap_or_else(|e| e.into_inner());
        let file = &mut identity.source_file;
        // Единственный producer меняет identity между файлами. Snapshot держит этот
        // же lock при чтении total: путь и смещение не смешиваются с другим файлом.
        file.active = true;
        file.size = size;
        file.base_raw_bytes = self.raw_processed_bytes.load(Ordering::Relaxed);
        file.relative_path.clear();
        // Сохраняем capacity между мелкими файлами и ограничиваем только отображение.
        let mut end = relative_path.len().min(4096);
        while !relative_path.is_char_boundary(end) {
            end -= 1;
        }
        file.relative_path.push_str(&relative_path[..end]);
        SourceFileGuard { progress: self }
    }

    pub(crate) fn set_stage(&self, stage: PipelineStage, state: PipelineState) {
        let slot = &self.pipelines[stage as usize];
        if PipelineState::from_packed(slot.load(Ordering::Relaxed)) == state {
            return;
        }
        // Состояние и начало интервала читаются одним atomic load. Повторная
        // публикация того же состояния не вызывает clock и не сбрасывает таймер.
        let millis = self
            .started
            .elapsed()
            .as_millis()
            .min((u64::MAX >> 8) as u128) as u64;
        slot.store((millis << 8) | state as u64, Ordering::Relaxed);
    }

    pub(crate) fn stage(
        self: &Arc<Self>,
        stage: PipelineStage,
        state: PipelineState,
    ) -> PipelineGuard {
        let previous =
            PipelineState::from_packed(self.pipelines[stage as usize].load(Ordering::Relaxed));
        let progress = if previous == state {
            None
        } else {
            self.set_stage(stage, state);
            Some(Arc::clone(self))
        };
        PipelineGuard {
            progress,
            stage,
            previous,
        }
    }

    pub(crate) fn set_identity(&self, dump_id: &str, peer: &str) {
        let mut identity = self.identity.lock().unwrap_or_else(|e| e.into_inner());
        identity.dump_id = dump_id.into();
        identity.peer = peer.into();
    }

    pub(crate) fn set_archive(&self, archive: PathBuf) {
        self.identity
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .archive = Some(archive);
    }

    pub(crate) fn set_error(&self, error: String) {
        self.identity
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .error = Some(display_text(&error, 4096));
    }

    pub(crate) fn snapshot(&self) -> SessionSnapshot {
        let identity = self.identity.lock().unwrap_or_else(|e| e.into_inner());
        let raw_processed_bytes = identity
            .finished_raw_bytes
            .unwrap_or_else(|| self.raw_processed_bytes.load(Ordering::Relaxed));
        let source_file = &identity.source_file;
        let current_file = source_file.active.then(|| CurrentFileSnapshot {
            relative_path: source_file.relative_path.clone(),
            size: source_file.size,
            processed_bytes: raw_processed_bytes.saturating_sub(source_file.base_raw_bytes),
        });
        let phase = Phase::from_u8(self.phase.load(Ordering::Relaxed));
        let elapsed = identity
            .finished
            .unwrap_or_else(Instant::now)
            .duration_since(self.started);
        let stages: &[PipelineStage] = if identity.role == "sender" {
            &PipelineStage::SENDER
        } else {
            &PipelineStage::RECEIVER
        };
        let pipelines = stages
            .iter()
            .map(|&stage| {
                let packed = self.pipelines[stage as usize].load(Ordering::Relaxed);
                let recorded = PipelineState::from_packed(packed);
                let state = match phase {
                    Phase::Completed | Phase::Warnings => PipelineState::Done,
                    Phase::Stopped => PipelineState::Stopped,
                    Phase::Failed
                        if recorded != PipelineState::Failed && recorded != PipelineState::Done =>
                    {
                        PipelineState::Stopped
                    }
                    _ => recorded,
                };
                PipelineSnapshot {
                    stage,
                    state,
                    elapsed: if state == recorded {
                        elapsed.saturating_sub(Duration::from_millis(packed >> 8))
                    } else {
                        Duration::ZERO
                    },
                }
            })
            .collect();
        SessionSnapshot {
            id: self.id,
            role: identity.role.clone(),
            dump_id: identity.dump_id.clone(),
            peer: identity.peer.clone(),
            phase,
            elapsed,
            discovered: self.discovered.load(Ordering::Relaxed),
            scan_finished: self.scan_finished.load(Ordering::Relaxed),
            sent_entries: self.sent_entries.load(Ordering::Relaxed),
            committed_entries: self.committed_entries.load(Ordering::Relaxed),
            transferred_bytes: self.transferred_bytes.load(Ordering::Relaxed),
            raw_processed_bytes,
            current_file,
            durable_bytes: self.durable_bytes.load(Ordering::Relaxed),
            resumed_bytes: self.resumed_bytes.load(Ordering::Relaxed),
            skips: self.skips.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            bundles: self.bundles.load(Ordering::Relaxed),
            archive: identity.archive.clone(),
            error: identity.error.clone(),
            pipelines,
        }
    }
}

pub(crate) struct ProgressHub {
    next_id: AtomicU64,
    sessions: Mutex<VecDeque<Arc<SessionProgress>>>,
    log_tx: Sender<LogEntry>,
    log_rx: Receiver<LogEntry>,
    history: Mutex<VecDeque<LogEntry>>,
    retry: Mutex<Option<RetrySnapshot>>,
    pub(crate) ui_enabled: AtomicBool,
}

impl ProgressHub {
    pub(crate) fn new() -> Self {
        let (log_tx, log_rx) = bounded(LOG_LIMIT);
        Self {
            next_id: AtomicU64::new(1),
            sessions: Mutex::new(VecDeque::new()),
            log_tx,
            log_rx,
            history: Mutex::new(VecDeque::new()),
            retry: Mutex::new(None),
            ui_enabled: AtomicBool::new(true),
        }
    }

    pub(crate) fn session(&self, role: String, dump_id: &str, peer: &str) -> Arc<SessionProgress> {
        let progress = Arc::new(SessionProgress::new(
            self.next_id.fetch_add(1, Ordering::Relaxed),
            role,
            dump_id,
            peer,
        ));
        let mut sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune(&mut sessions);
        sessions.push_back(Arc::clone(&progress));
        progress
    }

    fn prune(sessions: &mut VecDeque<Arc<SessionProgress>>) {
        let mut finished = sessions
            .iter()
            .filter(|p| Phase::from_u8(p.phase.load(Ordering::Relaxed)).is_finished())
            .count();
        sessions.retain(|p| {
            if finished > FINISHED_LIMIT
                && Phase::from_u8(p.phase.load(Ordering::Relaxed)).is_finished()
            {
                finished -= 1;
                false
            } else {
                true
            }
        });
    }

    pub(crate) fn sessions(&self) -> Vec<Arc<SessionProgress>> {
        let mut sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune(&mut sessions);
        sessions.iter().cloned().collect()
    }

    pub(crate) fn set_retry(&self, retry: Option<RetrySnapshot>) {
        *self.retry.lock().unwrap_or_else(|e| e.into_inner()) = retry.map(|mut retry| {
            retry.error = display_text(&retry.error, 4096);
            retry
        });
    }

    pub(crate) fn retry(&self) -> Option<RetrySnapshot> {
        self.retry.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub(crate) fn log(&self, level: Level, message: &str) {
        // Переполненный журнал не создаёт backpressure для передачи.
        let _ = self.log_tx.try_send(LogEntry {
            level,
            message: display_text(message, 4096),
        });
    }

    pub(crate) fn logs(&self) -> Vec<LogEntry> {
        let mut history = self.history.lock().unwrap_or_else(|e| e.into_inner());
        while let Ok(entry) = self.log_rx.try_recv() {
            if history.len() == LOG_LIMIT {
                history.pop_front();
            }
            history.push_back(entry);
        }
        history.iter().cloned().collect()
    }
}

pub(crate) fn display_text(text: &str, max_chars: usize) -> String {
    // Имена и ошибки не должны выполнять escape-последовательности терминала.
    text.chars()
        .take(max_chars)
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_file_retries_reset_local_progress_and_freeze_at_session_end() {
        let progress = ProgressHub::new().session("sender".into(), "dump", "peer");
        {
            let source = progress.source_file("first", 100);
            source.processed(40);
            let snapshot = progress.snapshot();
            assert_eq!(snapshot.raw_processed_bytes, 40);
            assert_eq!(snapshot.current_file.unwrap().processed_bytes, 40);
        }
        assert!(progress.snapshot().current_file.is_none());
        let source = progress.source_file("first", 120);
        let snapshot = progress.snapshot();
        assert_eq!(snapshot.current_file.unwrap().processed_bytes, 0);
        source.processed(120);
        assert_eq!(progress.snapshot().raw_processed_bytes, 160);
        progress.set_phase(Phase::Stopped);
        source.processed(1);
        let snapshot = progress.snapshot();
        assert_eq!(snapshot.raw_processed_bytes, 160);
        assert!(snapshot.current_file.is_none());
    }

    #[test]
    fn source_file_snapshots_keep_identity_and_offset_together() {
        let progress = ProgressHub::new().session("sender".into(), "dump", "peer");
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                for size in 1..=1000 {
                    let source = progress.source_file(&size.to_string(), size);
                    source.processed(size as usize / 2);
                    std::thread::yield_now();
                    source.processed(size as usize - size as usize / 2);
                }
            });
            while !worker.is_finished() {
                if let Some(file) = progress.snapshot().current_file {
                    assert_eq!(file.relative_path.parse::<u64>().unwrap(), file.size);
                    assert!(file.processed_bytes <= file.size);
                }
            }
            worker.join().unwrap();
        });
        assert_eq!(progress.snapshot().raw_processed_bytes, 500_500);
        assert!(progress.snapshot().current_file.is_none());
    }

    #[test]
    fn pipeline_scopes_restore_outer_work_but_preserve_failure() {
        let progress = ProgressHub::new().session("sender".into(), "dump", "peer");
        let state = || progress.snapshot().pipelines[PipelineStage::Compress as usize].state;
        {
            let _compress = progress.stage(PipelineStage::Compress, PipelineState::Compressing);
            assert_eq!(state(), PipelineState::Compressing);
            {
                let _wait = progress.stage(PipelineStage::Compress, PipelineState::WaitingBuffer);
                assert_eq!(state(), PipelineState::WaitingBuffer);
            }
            assert_eq!(state(), PipelineState::Compressing);
        }
        assert_eq!(state(), PipelineState::Idle);
        {
            let _compress = progress.stage(PipelineStage::Compress, PipelineState::Compressing);
            let _read = progress.stage(PipelineStage::Compress, PipelineState::Reading);
            progress.set_stage(PipelineStage::Compress, PipelineState::Failed);
        }
        assert_eq!(state(), PipelineState::Failed);
    }

    #[test]
    fn repeated_wait_preserves_start_and_does_not_retain_progress() {
        let progress = ProgressHub::new().session("sender".into(), "dump", "peer");
        let slot = &progress.pipelines[PipelineStage::Data as usize];
        let packed = (123 << 8) | PipelineState::WaitingCompression as u64;
        slot.store(packed, Ordering::Relaxed);
        let references = Arc::strong_count(&progress);
        for _ in 0..100 {
            let _wait = progress.stage(PipelineStage::Data, PipelineState::WaitingCompression);
            progress.set_stage(PipelineStage::Data, PipelineState::WaitingCompression);
            assert_eq!(slot.load(Ordering::Relaxed), packed);
            assert_eq!(Arc::strong_count(&progress), references);
        }
    }

    #[test]
    fn completed_snapshots_freeze_timers_and_clear_live_waits() {
        for (phase, expected) in [
            (Phase::Completed, PipelineState::Done),
            (Phase::Warnings, PipelineState::Done),
            (Phase::Stopped, PipelineState::Stopped),
            (Phase::Failed, PipelineState::Stopped),
        ] {
            let mut progress = SessionProgress::new(1, "sender".into(), "dump", "peer");
            progress.started -= Duration::from_secs(60);
            let progress = Arc::new(progress);
            let _wait = progress.stage(PipelineStage::Control, PipelineState::WaitingAck);
            progress.set_stage(PipelineStage::Data, PipelineState::Failed);
            progress.set_phase(phase);
            let first = progress.snapshot();
            let second = progress.snapshot();
            assert_eq!(first.elapsed, second.elapsed);
            assert_eq!(first.pipelines.len(), 6);
            assert_eq!(
                first.pipelines[PipelineStage::Control as usize].state,
                expected
            );
            if phase == Phase::Failed {
                assert_eq!(
                    first.pipelines[PipelineStage::Data as usize].state,
                    PipelineState::Failed
                );
            }
            for (left, right) in first.pipelines.iter().zip(&second.pipelines) {
                assert_eq!(left.elapsed, right.elapsed);
                assert!(!left.state.is_waiting());
            }
        }
    }

    #[test]
    fn receiver_stages_are_separate_from_sender_and_plain_mode_is_unobserved() {
        let progress = ProgressHub::new().session("receiver".into(), "dump", "peer");
        let snapshot = progress.snapshot();
        assert_eq!(
            snapshot
                .pipelines
                .iter()
                .map(|p| p.stage)
                .collect::<Vec<_>>(),
            PipelineStage::RECEIVER
        );
        let control = crate::runtime::RunControl::new(None).session(
            crate::role::Role::Sender,
            "dump",
            "peer",
        );
        let guard = stage(Some(&control), PipelineStage::Data, PipelineState::Sending);
        assert!(guard.progress.is_none());
        set_stage(Some(&control), PipelineStage::Data, PipelineState::Failed);
        assert!(control.progress().is_none());
    }

    #[test]
    fn history_and_completed_sessions_are_bounded() {
        let hub = ProgressHub::new();
        let active = hub.session("sender".into(), "active", "peer");
        for _ in 0..1000 {
            hub.session("sender".into(), "done", "peer")
                .set_phase(Phase::Completed);
            hub.log(Level::Info, "hello\x1b[2J");
        }
        assert_eq!(hub.sessions().len(), FINISHED_LIMIT + 1);
        assert!(hub.sessions().iter().any(|p| Arc::ptr_eq(p, &active)));
        assert_eq!(hub.logs().len(), LOG_LIMIT);
        assert!(hub.logs().iter().all(|e| !e.message.contains('\x1b')));
    }

    #[test]
    fn retry_state_bounds_error_text_and_clears_after_waiting() {
        let hub = ProgressHub::new();
        assert!(hub.retry().is_none());
        hub.set_retry(Some(RetrySnapshot {
            number: 1,
            limit: 3,
            deadline: Instant::now(),
            error: format!("reset\x1b[2J{}", "x".repeat(5000)),
        }));
        let retry = hub.retry().unwrap();
        assert_eq!(retry.error.chars().count(), 4096);
        assert!(!retry.error.chars().any(char::is_control));
        hub.set_retry(None);
        assert!(hub.retry().is_none());
    }
}

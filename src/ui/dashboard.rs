mod render;
mod sampling;

use super::theme::Theme;
use super::widgets::safe_text;
use crate::progress::{LogEntry, ProgressHub, RetrySnapshot, SessionSnapshot};
use crossterm::event::KeyCode;
use std::cell::Cell;
use std::collections::VecDeque;
use std::time::Instant;

const HISTORY_SECONDS: u64 = 60;
const HISTORY_POINTS: usize = 60;
const RATE_SECONDS: u64 = 5;
const LOG_LINES: usize = 200;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Focus {
    Sessions,
    Details,
    Logs,
}

pub(super) struct Dashboard {
    theme: Theme,
    sessions: Vec<SessionSnapshot>,
    logs: Vec<LogEntry>,
    selected: usize,
    focus: Focus,
    log_offset: usize,
    history: VecDeque<(Instant, f64)>,
    last_sample: Option<(u64, Instant, u64)>,
    rate_samples: VecDeque<(Instant, u64)>,
    source_rate_samples: VecDeque<(Instant, u64)>,
    bucket: Option<(Instant, f64)>,
    rate: f64,
    source_rate: f64,
    cancelled: bool,
    receiver_listening: bool,
    finished: bool,
    error: Option<String>,
    retry: Option<RetrySnapshot>,
    help: bool,
    confirm_stop: bool,
    context: String,
    configuration: Vec<(String, String)>,
    details_offset: usize,
    details_scroll_limit: Cell<usize>,
}

impl Dashboard {
    pub fn new(theme: Theme) -> Self {
        Self {
            theme,
            sessions: Vec::new(),
            logs: Vec::new(),
            selected: 0,
            focus: Focus::Sessions,
            log_offset: 0,
            history: VecDeque::with_capacity(HISTORY_POINTS),
            last_sample: None,
            rate_samples: VecDeque::with_capacity(28),
            source_rate_samples: VecDeque::with_capacity(28),
            bucket: None,
            rate: 0.0,
            source_rate: 0.0,
            cancelled: false,
            receiver_listening: false,
            finished: false,
            error: None,
            retry: None,
            help: false,
            confirm_stop: false,
            context: "Waiting for runtime…".into(),
            configuration: Vec::new(),
            details_offset: 0,
            details_scroll_limit: Cell::new(0),
        }
    }

    pub fn set_context(&mut self, context: String) {
        self.context = context;
    }

    pub fn set_configuration(&mut self, configuration: Vec<(String, String)>) {
        self.configuration = configuration
            .into_iter()
            .map(|(name, value)| (safe_text(&name), safe_text(&value)))
            .collect();
    }

    pub fn refresh(&mut self, hub: &ProgressHub, cancelled: bool) {
        let selected_id = self.sessions.get(self.selected).map(|session| session.id);
        let followed_latest = self.sessions.is_empty() || self.selected + 1 == self.sessions.len();
        self.sessions = hub
            .sessions()
            .into_iter()
            .map(|session| session.snapshot())
            .collect();
        if let Some(index) = self
            .sessions
            .iter()
            .position(|session| Some(session.id) == selected_id)
        {
            self.selected = index;
        }
        self.selected = self.selected.min(self.sessions.len().saturating_sub(1));
        // Следуем новой попытке после завершения предыдущей, но сохраняем
        // ручной выбор старой сессии при просмотре истории.
        if followed_latest
            && (selected_id.is_none()
                || self
                    .sessions
                    .get(self.selected)
                    .is_some_and(|session| session.phase.is_finished()))
        {
            self.selected = self.sessions.len().saturating_sub(1);
        }
        if self.sessions.get(self.selected).map(|session| session.id) != selected_id {
            self.details_offset = 0;
        }
        self.retry = hub.retry();
        self.receiver_listening = hub.receiver_listening();
        self.logs = hub.logs();
        if self.logs.len() > LOG_LINES {
            self.logs.drain(..self.logs.len() - LOG_LINES);
        }
        self.log_offset = self.log_offset.min(self.logs.len().saturating_sub(1));
        self.cancelled = cancelled;
        self.sample(Instant::now());
    }

    pub fn finish(&mut self, error: Option<String>) {
        self.finished = true;
        if error.is_some() {
            self.focus = Focus::Details;
        }
        self.error = error;
        self.confirm_stop = false;
    }

    pub fn stop_requested(&mut self) {
        self.cancelled = true;
        self.confirm_stop = false;
    }

    // true означает подтверждённую остановку либо закрытие итогового экрана.
    pub fn key(&mut self, key: KeyCode, finished: bool) -> bool {
        if self.help {
            if matches!(key, KeyCode::F(1) | KeyCode::Esc | KeyCode::Enter) {
                self.help = false;
            }
            return false;
        }
        if self.confirm_stop {
            match key {
                KeyCode::Enter => return true,
                KeyCode::Esc | KeyCode::Char('n') => self.confirm_stop = false,
                _ => {}
            }
            return false;
        }
        match key {
            KeyCode::F(1) | KeyCode::Char('?') => self.help = true,
            KeyCode::Char('q') | KeyCode::Esc => {
                if finished {
                    return true;
                }
                if !self.cancelled {
                    self.confirm_stop = true;
                }
            }
            KeyCode::Enter if finished => return true,
            KeyCode::Tab | KeyCode::BackTab => {
                self.focus = match (self.focus, key == KeyCode::BackTab) {
                    (Focus::Sessions, false) | (Focus::Logs, true) => Focus::Details,
                    (Focus::Details, false) | (Focus::Sessions, true) => Focus::Logs,
                    _ => Focus::Sessions,
                };
            }
            KeyCode::Up if self.focus == Focus::Logs => {
                self.log_offset = (self.log_offset + 1).min(self.logs.len().saturating_sub(1));
            }
            KeyCode::Down if self.focus == Focus::Logs => {
                self.log_offset = self.log_offset.saturating_sub(1);
            }
            KeyCode::Up if self.focus == Focus::Details => {
                self.details_offset = self.details_offset.saturating_sub(1);
            }
            KeyCode::Down if self.focus == Focus::Details => {
                self.details_offset =
                    (self.details_offset + 1).min(self.details_scroll_limit.get());
            }
            KeyCode::Up => {
                self.selected = self.selected.saturating_sub(1);
                self.details_offset = 0;
            }
            KeyCode::Down => {
                self.selected = (self.selected + 1).min(self.sessions.len().saturating_sub(1));
                self.details_offset = 0;
            }
            KeyCode::PageUp => {
                self.focus = Focus::Logs;
                self.log_offset = (self.log_offset + 5).min(self.logs.len().saturating_sub(1));
            }
            KeyCode::PageDown => {
                self.focus = Focus::Logs;
                self.log_offset = self.log_offset.saturating_sub(5);
            }
            KeyCode::End => self.log_offset = 0,
            _ => {}
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::super::theme::ColorMode;
    use super::render::{average_speed, elapsed, retry_status};
    use super::*;
    use crate::progress::{Phase, PipelineState};
    use crate::progress::{PipelineSnapshot, PipelineStage};
    use crate::ui::widgets::bytes;
    use ratatui::layout::Rect;
    use ratatui::style::{Color, Modifier};
    use ratatui::{Terminal, backend::TestBackend};
    use std::time::Duration;

    fn session() -> SessionSnapshot {
        SessionSnapshot {
            id: 1,
            role: "sender".into(),
            dump_id: "測試-данные".into(),
            peer: "127.0.0.1:7000".into(),
            phase: Phase::Sending,
            scan_finished: false,
            elapsed: Duration::from_secs(65),
            discovered: 400,
            sent_entries: 100,
            committed_entries: 96,
            transferred_bytes: 12 * 1024 * 1024,
            raw_processed_bytes: 64 * 1024 * 1024,
            current_file: None,
            durable_bytes: 8 * 1024 * 1024,
            resumed_bytes: 4 * 1024 * 1024,
            skips: 1,
            retries: 2,
            bundles: 3,
            archive: Some("out/測試-данные.tar.zst.part".into()),
            error: None,
            pipelines: vec![
                pipeline(PipelineStage::Scan, PipelineState::Scanning, 5),
                pipeline(PipelineStage::Read, PipelineState::WaitingConsumer, 10),
                pipeline(PipelineStage::Plan, PipelineState::WaitingRead, 65),
                pipeline(PipelineStage::Compress, PipelineState::Compressing, 3661),
                pipeline(PipelineStage::Data, PipelineState::WaitingCompression, 61),
                pipeline(PipelineStage::Control, PipelineState::WaitingAck, 59),
            ],
        }
    }

    fn pipeline(stage: PipelineStage, state: PipelineState, seconds: u64) -> PipelineSnapshot {
        PipelineSnapshot {
            stage,
            state,
            elapsed: Duration::from_secs(seconds),
        }
    }

    fn screen(terminal: &Terminal<TestBackend>) -> String {
        terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect()
    }

    #[test]
    fn retry_countdown_keeps_dashboard_live_and_error_visible() {
        let hub = ProgressHub::new();
        hub.session("sender".into(), "retry-test", "peer")
            .set_phase(Phase::Failed);
        let now = Instant::now();
        let retry = RetrySnapshot {
            number: 2,
            limit: 3,
            deadline: now + Duration::from_secs(10),
            error: "connection reset".into(),
        };
        assert_eq!(retry_status(&retry, now), "Retry 2/3 in 10 s");
        assert_eq!(
            retry_status(&retry, now + Duration::from_millis(9200)),
            "Retry 2/3 in 1 s"
        );
        assert_eq!(
            retry_status(&retry, now + Duration::from_secs(11)),
            "Retry 2/3 in 0 s"
        );
        hub.set_retry(Some(retry));
        let mut state = Dashboard::new(Theme::new(ColorMode::Monochrome));
        state.refresh(&hub, false);
        for (width, height) in [(120, 32), (90, 26), (60, 20)] {
            let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
            terminal.draw(|frame| state.render(frame)).unwrap();
            let text = screen(&terminal);
            assert!(text.contains("LIVE"));
            assert!(text.contains("Retry 2/3 in "));
            assert!(text.contains("Last error: connection reset"));
        }
        hub.set_retry(None);
        state.refresh(&hub, false);
        assert!(state.retry.is_none());
    }

    #[test]
    fn new_attempt_follows_latest_session_without_displacing_history_selection() {
        let hub = ProgressHub::new();
        let first = hub.session("sender".into(), "first", "peer");
        let mut state = Dashboard::new(Theme::new(ColorMode::Monochrome));
        state.refresh(&hub, false);
        first.set_phase(Phase::Failed);
        let second = hub.session("sender".into(), "second", "peer");
        state.refresh(&hub, false);
        assert_eq!(state.sessions[state.selected].dump_id, "second");
        state.key(KeyCode::Up, false);
        assert_eq!(state.sessions[state.selected].dump_id, "first");
        second.set_phase(Phase::Failed);
        hub.session("sender".into(), "third", "peer");
        state.refresh(&hub, false);
        assert_eq!(state.sessions[state.selected].dump_id, "first");
    }

    #[test]
    fn dashboard_draws_unicode_at_supported_sizes_and_after_resize() {
        let mut state = Dashboard::new(Theme::new(ColorMode::TrueColor));
        state.sessions.push(session());
        let mut terminal = Terminal::new(TestBackend::new(120, 32)).unwrap();
        for (width, height) in [(120, 32), (90, 26), (60, 20), (30, 8), (120, 32)] {
            terminal.backend_mut().resize(width, height);
            terminal.resize(Rect::new(0, 0, width, height)).unwrap();
            terminal.draw(|frame| state.render(frame)).unwrap();
            let text = screen(&terminal);
            assert!(text.contains("fast-dump"));
            assert!(!text.contains("NaN"));
            if width >= 60 {
                assert!(text.contains("Durable archive"));
            } else {
                assert!(text.contains("Resize terminal"));
            }
        }
    }

    #[test]
    fn source_speed_is_independent_bounded_and_resets_between_sessions() {
        let mut state = Dashboard::new(Theme::new(ColorMode::Monochrome));
        let mut current = session();
        current.transferred_bytes = 0;
        current.raw_processed_bytes = 0;
        state.sessions.push(current);
        let start = Instant::now();
        state.sample(start);
        for tick in 1..=200 {
            state.sessions[0].raw_processed_bytes += 1000;
            state.sample(start + Duration::from_millis(tick * 200));
        }
        assert_eq!(state.rate, 0.0);
        assert!((state.source_rate - 5000.0).abs() < 0.01);
        assert!(state.source_rate_samples.len() <= 28);
        state.sessions[0].id = 2;
        state.sessions[0].raw_processed_bytes = 100;
        state.sample(start + Duration::from_secs(41));
        assert_eq!(state.source_rate, 0.0);
        assert_eq!(state.source_rate_samples.len(), 1);
        state.sessions[0].raw_processed_bytes += 1000;
        state.sample(start + Duration::from_secs(42));
        assert!(state.source_rate > 0.0);
        state.sessions[0].phase = Phase::Completed;
        state.sample(start + Duration::from_secs(43));
        assert_eq!(state.source_rate, 0.0);
    }

    #[test]
    fn details_show_source_file_size_progress_and_sanitized_path() {
        let mut state = Dashboard::new(Theme::new(ColorMode::Monochrome));
        let mut current = session();
        current.current_file = Some(crate::progress::CurrentFileSnapshot {
            relative_path: "folder/file\nname".into(),
            size: 128 * 1024 * 1024 * 1024 * 1024,
            processed_bytes: 2 * 1024 * 1024,
        });
        state.sessions.push(current);
        state.source_rate = 4.0 * 1024.0 * 1024.0 * 1024.0;
        let mut terminal = Terminal::new(TestBackend::new(160, 50)).unwrap();
        terminal.draw(|frame| state.render(frame)).unwrap();
        let text = screen(&terminal);
        assert!(text.contains("Source processing: 4.0 GiB/s"));
        assert!(text.contains("Current file: folder/file name"));
        assert!(text.contains("File size: 128.0 TiB (at open)"));
        assert!(text.contains("File processed: 2.0 MiB / 128.0 TiB"));
    }

    #[test]
    fn monochrome_uses_no_explicit_colors_and_preserves_selection() {
        let mut state = Dashboard::new(Theme::new(ColorMode::Monochrome));
        state.sessions.push(session());
        let mut terminal = Terminal::new(TestBackend::new(120, 32)).unwrap();
        terminal.draw(|frame| state.render(frame)).unwrap();
        let cells = &terminal.backend().buffer().content;
        assert!(
            cells
                .iter()
                .all(|cell| cell.fg == Color::Reset && cell.bg == Color::Reset)
        );
        assert!(
            cells
                .iter()
                .any(|cell| cell.modifier.contains(Modifier::REVERSED))
        );
    }

    #[test]
    fn pipeline_shows_every_sender_stage_and_long_state_times_after_resize() {
        let mut state = Dashboard::new(Theme::new(ColorMode::Monochrome));
        state.sessions.push(session());
        let mut terminal = Terminal::new(TestBackend::new(120, 32)).unwrap();
        for (width, height) in [(120, 32), (80, 24), (60, 20), (120, 32)] {
            terminal.backend_mut().resize(width, height);
            terminal.resize(Rect::new(0, 0, width, height)).unwrap();
            terminal.draw(|frame| state.render(frame)).unwrap();
            let text = screen(&terminal);
            assert!(text.contains("Pipeline · current state / time"));
            for stage in &state.sessions[0].pipelines {
                assert!(text.contains(stage.stage.label()), "missing stage: {text}");
                assert!(text.contains(stage.state.label()), "missing state: {text}");
                assert!(
                    text.contains(&elapsed(stage.elapsed)),
                    "missing time: {text}"
                );
            }
            if width == 120 {
                assert!(text.contains("Network throughput"));
                assert!(text.contains("Details"));
            }
        }
    }

    #[test]
    fn receiver_pipeline_distinguishes_socket_control_and_storage_operations() {
        let mut state = Dashboard::new(Theme::new(ColorMode::TrueColor));
        let mut receiver = session();
        receiver.role = "receiver".into();
        receiver.phase = Phase::Receiving;
        receiver.pipelines = vec![
            pipeline(PipelineStage::Data, PipelineState::Receiving, 1),
            pipeline(PipelineStage::Control, PipelineState::WaitingControl, 61),
            pipeline(PipelineStage::Storage, PipelineState::Syncing, 2),
        ];
        state.sessions.push(receiver);
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        terminal.draw(|frame| state.render(frame)).unwrap();
        let text = screen(&terminal);
        assert!(text.contains("Reading socket"));
        assert!(text.contains("Waiting control"));
        assert!(text.contains("Syncing archive / WAL"));
        assert!(text.contains("00:01:01"));
        assert!(!text.contains("Tar / zstd"));
        let cells = &terminal.backend().buffer().content;
        for (label, color) in [
            ("Waiting control", state.theme.amber),
            ("Syncing archive / WAL", state.theme.cyan),
        ] {
            let row = cells
                .chunks(80)
                .find(|row| {
                    row.iter()
                        .map(|cell| cell.symbol())
                        .collect::<String>()
                        .contains(label)
                })
                .unwrap();
            assert_eq!(row[12].fg, color);
        }
    }

    #[test]
    fn failed_pipeline_and_error_remain_accessible_in_narrow_terminal() {
        let mut state = Dashboard::new(Theme::new(ColorMode::Monochrome));
        let mut failed = session();
        failed.phase = Phase::Failed;
        failed.error = Some("Connection reset by peer".into());
        for pipeline in &mut failed.pipelines {
            pipeline.state = if pipeline.stage == PipelineStage::Data {
                PipelineState::Failed
            } else {
                PipelineState::Stopped
            };
        }
        state.sessions.push(failed);
        state.finish(Some("Connection reset by peer".into()));
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        terminal.draw(|frame| state.render(frame)).unwrap();
        assert!(screen(&terminal).contains("Connection reset by peer"));
        state.key(KeyCode::BackTab, true);
        terminal.draw(|frame| state.render(frame)).unwrap();
        let text = screen(&terminal);
        assert!(text.contains("Pipeline"));
        assert!(text.contains("Data       Failed"));
        assert!(text.contains("Control    Stopped"));
        assert!(!text.contains("Waiting peer ACK"));
        state.key(KeyCode::Tab, true);
        terminal.draw(|frame| state.render(frame)).unwrap();
        assert!(screen(&terminal).contains("Connection reset by peer"));
    }

    #[test]
    fn history_stays_bounded_and_changing_session_resets_rate() {
        let mut state = Dashboard::new(Theme::new(ColorMode::Monochrome));
        state.sessions.push(session());
        let start = Instant::now();
        for index in 0..1000 {
            state.sessions[0].transferred_bytes += 100;
            state.sample(start + Duration::from_millis(index * 200));
        }
        assert!(state.history.len() <= HISTORY_POINTS);
        assert!(
            state
                .history
                .iter()
                .all(|(_, rate)| (*rate - 500.0).abs() < 0.01)
        );
        assert!(state.rate_samples.len() <= 28);
        assert!((state.rate - 500.0).abs() < 0.01);
        state.sessions[0].id = 2;
        state.sample(start + Duration::from_secs(201));
        assert!(state.history.is_empty());
        assert_eq!(state.rate, 0.0);
    }

    #[test]
    fn speed_uses_a_five_second_window_and_graph_uses_one_second_buckets() {
        let mut state = Dashboard::new(Theme::new(ColorMode::Monochrome));
        let mut current = session();
        current.transferred_bytes = 0;
        current.elapsed = Duration::from_secs(10);
        state.sessions.push(current);
        let start = Instant::now();
        state.sample(start);
        for index in 1..=30 {
            if index <= 25 {
                state.sessions[0].transferred_bytes += 100;
            }
            state.sample(start + Duration::from_millis(index * 200));
        }
        assert!((state.rate - 400.0).abs() < 0.01);
        assert_eq!(state.history.len(), 6);
        assert!(state.history.back().unwrap().1.abs() < 0.01);
        for pair in state.history.make_contiguous().windows(2) {
            assert_eq!(pair[1].0.duration_since(pair[0].0), Duration::from_secs(1));
        }
        assert!((average_speed(&state.sessions[0]) - 250.0).abs() < 0.01);
    }

    #[test]
    fn long_redraw_gap_keeps_only_one_minute_without_an_artificial_rate_spike() {
        let mut state = Dashboard::new(Theme::new(ColorMode::Monochrome));
        let mut current = session();
        current.transferred_bytes = 0;
        state.sessions.push(current);
        let start = Instant::now();
        state.sample(start);
        state.sessions[0].transferred_bytes = 10_000;
        state.sample(start + Duration::from_secs(100));
        assert_eq!(state.history.len(), 60);
        assert!((state.rate - 100.0).abs() < 0.01);
        assert!(
            state
                .history
                .iter()
                .all(|(_, rate)| (*rate - 100.0).abs() < 0.01)
        );
    }

    #[test]
    fn stop_confirmation_and_help_are_separate_from_navigation() {
        let mut state = Dashboard::new(Theme::new(ColorMode::Monochrome));
        assert!(!state.key(KeyCode::Char('q'), false));
        assert!(state.confirm_stop);
        assert!(!state.key(KeyCode::Esc, false));
        assert!(!state.confirm_stop);
        state.key(KeyCode::F(1), false);
        assert!(!state.key(KeyCode::Enter, false));
        assert!(!state.help);
        state.key(KeyCode::Char('q'), false);
        assert!(state.key(KeyCode::Enter, false));
    }

    #[test]
    fn untrusted_terminal_text_cannot_inject_controls() {
        assert_eq!(safe_text("file\u{1b}[31m\r\nname\t"), "file [31m  name ");
        assert_eq!(bytes(1024), "1.0 KiB");
    }

    #[test]
    fn receiver_shows_received_and_durable_entries_without_remote_scan_totals() {
        let mut state = Dashboard::new(Theme::new(ColorMode::Monochrome));
        let mut receiver = session();
        receiver.role = "receiver".into();
        receiver.phase = Phase::Receiving;
        state.sessions.push(receiver);
        let mut terminal = Terminal::new(TestBackend::new(120, 32)).unwrap();
        terminal.draw(|frame| state.render(frame)).unwrap();
        let text = screen(&terminal);
        assert!(text.contains("100 received"));
        assert!(text.contains("96 confirmed"));
        assert!(!text.contains("400 found"));
        assert!(text.contains("Durable archive"));
    }

    #[test]
    fn scan_status_and_configuration_remain_available_in_small_details_panel() {
        let mut state = Dashboard::new(Theme::new(ColorMode::Monochrome));
        state.sessions.push(session());
        state.set_configuration(vec![
            ("Source".into(), "資料/\u{1b}[31mdata".into()),
            ("Compression".into(), "zstd level 3".into()),
            ("Threads".into(), "4".into()),
            ("Readers".into(), "2 requested".into()),
            ("Bundle target".into(), "64 MiB".into()),
        ]);
        let mut terminal = Terminal::new(TestBackend::new(120, 32)).unwrap();
        terminal.draw(|frame| state.render(frame)).unwrap();
        assert!(screen(&terminal).contains("Scan: Streaming"));
        state.sessions[0].scan_finished = true;
        terminal.draw(|frame| state.render(frame)).unwrap();
        assert!(screen(&terminal).contains("Scan: Complete"));
        assert!(!screen(&terminal).contains('\u{1b}'));
        terminal.backend_mut().resize(60, 20);
        terminal.resize(Rect::new(0, 0, 60, 20)).unwrap();
        state.focus = Focus::Details;
        terminal.draw(|frame| state.render(frame)).unwrap();
        assert!(state.details_scroll_limit.get() > 0);
        let mut found_configuration = false;
        for _ in 0..100 {
            state.key(KeyCode::Down, false);
            terminal.draw(|frame| state.render(frame)).unwrap();
            found_configuration |= screen(&terminal).contains("Bundle target: 64 MiB");
        }
        assert!(found_configuration);
        let text = screen(&terminal);
        assert!(text.contains("Speed"));
        assert!(text.contains("Sent"));
    }

    #[test]
    fn issue_path_appears_only_for_completed_archives_with_skips() {
        let mut state = Dashboard::new(Theme::new(ColorMode::Monochrome));
        let mut receiver = session();
        receiver.role = "receiver".into();
        receiver.phase = Phase::Receiving;
        receiver.archive = Some("out/dump.tar.zst.part".into());
        state.sessions.push(receiver);
        let mut terminal = Terminal::new(TestBackend::new(120, 32)).unwrap();
        terminal.draw(|frame| state.render(frame)).unwrap();
        assert!(!screen(&terminal).contains("Issues:"));
        state.sessions[0].phase = Phase::Warnings;
        state.sessions[0].archive = Some("out/dump.tar.zst".into());
        terminal.draw(|frame| state.render(frame)).unwrap();
        assert!(screen(&terminal).contains("Issues: out/dump.tar.zst.issues.tsv"));
        state.sessions[0].skips = 0;
        state.sessions[0].phase = Phase::Completed;
        terminal.draw(|frame| state.render(frame)).unwrap();
        assert!(!screen(&terminal).contains("Issues:"));
    }

    #[test]
    fn main_screen_shows_archive_and_listener_waiting_at_minimum_size() {
        let hub = ProgressHub::new();
        let mut state = Dashboard::new(Theme::new(ColorMode::Monochrome));
        let receiver = hub.session("receiver".into(), "dump", "peer");
        receiver.set_archive("out/dump.tar.zst".into());
        receiver
            .skips
            .store(2, std::sync::atomic::Ordering::Relaxed);
        receiver.set_phase(Phase::Receiving);
        hub.mark_receiver_listening();

        let mut terminal = Terminal::new(TestBackend::new(60, 20)).unwrap();
        state.refresh(&hub, false);
        terminal.draw(|frame| state.render(frame)).unwrap();
        assert!(!screen(&terminal).contains("Archive ready"));
        assert!(!screen(&terminal).contains("Waiting for the next transfer"));

        receiver.set_phase(Phase::Warnings);
        state.refresh(&hub, false);
        terminal.draw(|frame| state.render(frame)).unwrap();
        let result = screen(&terminal);
        assert!(result.contains("Archive ready · Skipped: 2"));
        assert!(result.contains("out/dump.tar.zst"));
        assert!(result.contains("Waiting for the next transfer"));

        state.refresh(&hub, true);
        terminal.draw(|frame| state.render(frame)).unwrap();
        assert!(!screen(&terminal).contains("Waiting for the next transfer"));
    }
}

use std::cell::Cell;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crossterm::event::KeyCode;
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols::Marker;
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Axis, Block, Borders, Chart, Clear, Dataset, GraphType, List, ListItem, ListState, Paragraph,
    Row, Table, Wrap,
};

use crate::progress::{
    LogEntry, Phase, PipelineState, ProgressHub, RetrySnapshot, SessionSnapshot,
};

use super::theme::Theme;

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
        self.logs = hub.logs();
        if self.logs.len() > LOG_LINES {
            self.logs.drain(..self.logs.len() - LOG_LINES);
        }
        self.log_offset = self.log_offset.min(self.logs.len().saturating_sub(1));
        self.cancelled = cancelled;
        self.sample(Instant::now());
    }

    fn sample(&mut self, now: Instant) {
        let Some((id, total, raw_total, finished)) =
            self.sessions.get(self.selected).map(|session| {
                (
                    session.id,
                    session.transferred_bytes,
                    session.raw_processed_bytes,
                    session.phase.is_finished(),
                )
            })
        else {
            return;
        };
        match self.last_sample {
            Some((previous_id, at, bytes)) if previous_id == id => {
                let elapsed = now.saturating_duration_since(at).as_secs_f64();
                // Клавиши могут вызвать redraw чаще таймера; samples остаются
                // ограничены пятью в секунду и не превращают ввод в ложные пики.
                if elapsed < 0.19 {
                    return;
                }
                self.add_buckets(at, now, total.saturating_sub(bytes) as f64 / elapsed);
                self.rate_samples.push_back((now, total));
                self.update_rate(now, total);
                self.source_rate_samples.push_back((now, raw_total));
                self.source_rate =
                    Self::sampled_rate(&mut self.source_rate_samples, now, raw_total);
            }
            _ => {
                self.history.clear();
                self.rate_samples.clear();
                self.rate_samples.push_back((now, total));
                self.source_rate_samples.clear();
                self.source_rate_samples.push_back((now, raw_total));
                self.bucket = Some((now, 0.0));
                self.rate = 0.0;
                self.source_rate = 0.0;
            }
        }
        if finished {
            self.source_rate = 0.0;
        }
        self.last_sample = Some((id, now, total));
        while self.history.len() > HISTORY_POINTS
            || self.history.front().is_some_and(|(at, _)| {
                now.saturating_duration_since(*at) > Duration::from_secs(HISTORY_SECONDS)
            })
        {
            self.history.pop_front();
        }
    }

    fn update_rate(&mut self, now: Instant, total: u64) {
        self.rate = Self::sampled_rate(&mut self.rate_samples, now, total);
    }

    fn sampled_rate(samples: &mut VecDeque<(Instant, u64)>, now: Instant, total: u64) -> f64 {
        let cutoff = now
            .checked_sub(Duration::from_secs(RATE_SECONDS))
            .unwrap_or(now);
        // Оставляем один sample перед границей окна. Интерполяция этого короткого
        // интервала удерживает окно ровно в 5 s даже при нерегулярном redraw.
        while samples.len() > 2 && samples[1].0 <= cutoff {
            samples.pop_front();
        }
        let (first_at, first_bytes) = samples[0];
        let mut transferred = total.saturating_sub(first_bytes) as f64;
        let started = first_at.max(cutoff);
        if first_at < cutoff
            && let Some(&(next_at, next_bytes)) = samples.get(1)
        {
            let interval = next_at.duration_since(first_at).as_secs_f64();
            let excluded = cutoff.duration_since(first_at).as_secs_f64() / interval;
            transferred -= next_bytes.saturating_sub(first_bytes) as f64 * excluded;
        }
        let elapsed = now.saturating_duration_since(started).as_secs_f64();
        if elapsed > 0.0 {
            transferred.max(0.0) / elapsed
        } else {
            0.0
        }
    }

    fn add_buckets(&mut self, at: Instant, now: Instant, rate: f64) {
        let mut cursor = at;
        if now.saturating_duration_since(at) >= Duration::from_secs(HISTORY_SECONDS) {
            // После долгой приостановки UI обрабатываем только видимую минуту,
            // а не создаём по bucket на каждую секунду вне экрана.
            self.history.clear();
            cursor = now - Duration::from_secs(HISTORY_SECONDS);
            self.bucket = Some((cursor, 0.0));
        }
        let (mut started, mut bytes) = self.bucket.unwrap_or((at, 0.0));
        while cursor < now {
            let boundary = started + Duration::from_secs(1);
            let end = now.min(boundary);
            bytes += rate * end.saturating_duration_since(cursor).as_secs_f64();
            if end == boundary {
                if self.history.len() == HISTORY_POINTS {
                    self.history.pop_front();
                }
                self.history.push_back((boundary, bytes));
                started = boundary;
                bytes = 0.0;
            }
            cursor = end;
        }
        self.bucket = Some((started, bytes));
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

    pub fn render(&self, frame: &mut Frame<'_>) {
        let area = frame.area();
        frame.render_widget(Block::default().style(self.theme.base()), area);
        if area.width < 60 || area.height < 20 {
            self.render_tiny(frame, area);
            return;
        }
        let wide = area.width >= 110 && area.height >= 30;
        let compact = area.width < 80;
        let short = area.height < 24;
        // На низком экране выбранная сессия остаётся в заголовке: место нужно
        // всем стадиям конвейера, иначе причина ожидания может оказаться скрыта.
        let sessions_height = if short {
            0
        } else {
            (self.sessions.len().clamp(1, if wide { 4 } else { 2 }) + 2) as u16
        };
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(if short { 2 } else { 3 }),
                Constraint::Length(sessions_height),
                Constraint::Length(if compact { 6 } else { 4 }),
                Constraint::Min(8),
                Constraint::Length(if wide { 5 } else { 3 }),
                Constraint::Length(1),
            ])
            .split(area);
        self.render_header(frame, rows[0]);
        if sessions_height > 0 {
            self.render_sessions(frame, rows[1]);
        }
        self.render_cards(frame, rows[2], compact);
        if wide {
            let middle =
                Layout::horizontal([Constraint::Percentage(55), Constraint::Percentage(45)])
                    .split(rows[3]);
            self.render_pipeline_and_graph(frame, middle[0]);
            self.render_details(frame, middle[1]);
        } else if self.focus == Focus::Details {
            self.render_details(frame, rows[3]);
        } else {
            self.render_pipeline_and_graph(frame, rows[3]);
        }
        self.render_logs(frame, rows[4]);
        let hint = if self.finished {
            " Enter close  F1 help  Tab focus  ↑↓ navigate  PgUp/PgDn logs"
        } else if self.cancelled {
            " Cancelling… waiting for current I/O and workers to finish"
        } else {
            " F1 help  Tab focus  ↑↓ navigate  PgUp/PgDn logs  Ctrl-C stop"
        };
        frame.render_widget(
            Paragraph::new(hint).style(self.theme.base().fg(self.theme.muted)),
            rows[5],
        );
        if self.help {
            render_dialog(
                frame,
                self.theme,
                " Keyboard help ",
                &[
                    "Tab / Shift-Tab     Move panel focus",
                    "Up / Down           Select session or scroll focused panel",
                    "PgUp / PgDn         Scroll activity log",
                    "End                 Follow latest log messages",
                    "F1 / ?              Open or close this help",
                    "Ctrl-C              Stop safely; keep durable progress",
                    "Q / Esc             Ask to stop; close completed result",
                    "Enter               Confirm stop / close result",
                    "",
                    "Pipeline timers show time in the current state, not CPU time.",
                    "Scanning overlaps sending. Totals and ETA are unknown.",
                    "Sent bytes may still await durable acknowledgement.",
                    "Stopped transfers resume by running the same command.",
                ],
            );
        } else if self.confirm_stop {
            render_dialog(
                frame,
                self.theme,
                " Stop transfer? ",
                &[
                    "Durably committed progress will be kept.",
                    "Current uncommitted data may be sent again on resume.",
                    "Stopping waits for the current filesystem operation.",
                    "",
                    "Enter: stop safely       Esc: continue transfer",
                ],
            );
        }
    }

    fn render_tiny(&self, frame: &mut Frame<'_>, area: Rect) {
        let mut lines = vec![
            Line::styled("fast-dump", self.theme.accent()),
            Line::from("Resize terminal to at least 60 × 20."),
            Line::from(if self.cancelled {
                "Cancelling safely…"
            } else {
                "Transfer continues."
            }),
        ];
        if let Some(retry) = &self.retry {
            lines.push(Line::from(retry_status(retry, Instant::now())));
        }
        if let Some(session) = self.sessions.get(self.selected) {
            lines.push(Line::from(format!(
                "{} · {}",
                safe_text(&session.dump_id),
                session.phase.label()
            )));
            lines.push(Line::from(format!(
                "Data {} · durable {}",
                bytes(session.transferred_bytes),
                bytes(session.durable_bytes)
            )));
        }
        lines.push(Line::from(if self.finished {
            "Enter: close"
        } else {
            "Ctrl-C: stop safely"
        }));
        frame.render_widget(
            Paragraph::new(lines)
                .style(self.theme.base())
                .wrap(Wrap { trim: false }),
            area,
        );
        if self.confirm_stop {
            render_dialog(
                frame,
                self.theme,
                " Stop? ",
                &["Enter: stop safely", "Esc: continue"],
            );
        } else if self.help {
            render_dialog(
                frame,
                self.theme,
                " Help ",
                &[
                    "Ctrl-C: stop safely",
                    "Enter: close result",
                    "F1 / Esc: close help",
                ],
            );
        }
    }

    fn render_header(&self, frame: &mut Frame<'_>, area: Rect) {
        let status = if self.cancelled && !self.finished {
            "CANCELLING"
        } else if self.finished && self.error.is_some() {
            if self.cancelled { "STOPPED" } else { "FAILED" }
        } else if self.finished {
            "FINISHED"
        } else {
            "LIVE"
        };
        let title = Line::from(vec![
            Span::styled(" fast-dump ", self.theme.accent()),
            Span::styled(
                " Resumable directory transfer",
                self.theme.base().fg(self.theme.muted),
            ),
            Span::styled(
                format!("  {status}"),
                self.theme.base().fg(if self.cancelled {
                    self.theme.amber
                } else {
                    self.theme.green
                }),
            ),
        ]);
        let subtitle = self.sessions.get(self.selected).map_or_else(
            || format!(" {}", safe_text(&self.context)),
            |session| {
                format!(
                    " {} · {} · {}",
                    safe_text(&session.role),
                    safe_text(&session.dump_id),
                    safe_text(&session.peer)
                )
            },
        );
        let mut lines = vec![title];
        if self.retry.is_none() || area.height >= 3 {
            lines.push(Line::from(subtitle));
        }
        if let Some(retry) = &self.retry {
            lines.push(Line::styled(
                format!(
                    " {} · Last error: {}",
                    retry_status(retry, Instant::now()),
                    safe_text(&retry.error)
                ),
                self.theme.base().fg(self.theme.amber),
            ));
        }
        frame.render_widget(Paragraph::new(lines).style(self.theme.base()), area);
    }

    fn render_sessions(&self, frame: &mut Frame<'_>, area: Rect) {
        let items = self
            .sessions
            .iter()
            .map(|session| {
                ListItem::new(Line::from(vec![
                    Span::raw(format!("{}  ", safe_text(&session.dump_id))),
                    Span::styled(
                        session.phase.label(),
                        self.theme
                            .panel()
                            .fg(phase_color(&session.phase, self.theme)),
                    ),
                    Span::styled(
                        format!(
                            "  {}  {}",
                            safe_text(&session.role),
                            bytes(session.transferred_bytes)
                        ),
                        self.theme.muted(),
                    ),
                ]))
            })
            .collect::<Vec<_>>();
        let items = if items.is_empty() {
            vec![ListItem::new("Waiting for a session…")]
        } else {
            items
        };
        let list = List::new(items)
            .block(panel(
                " Sessions ",
                self.theme,
                self.focus == Focus::Sessions,
            ))
            .style(self.theme.panel())
            .highlight_style(self.theme.selected())
            .highlight_symbol("› ");
        let mut state = ListState::default().with_selected(Some(self.selected));
        frame.render_stateful_widget(list, area, &mut state);
    }

    fn render_cards(&self, frame: &mut Frame<'_>, area: Rect, compact: bool) {
        let Some(session) = self.sessions.get(self.selected) else {
            return;
        };
        let sender = session.role == "sender";
        let cards = [
            (
                if sender {
                    " Speed · 5 s "
                } else {
                    " Write speed · 5 s "
                },
                format!("{}/s", bytes(self.rate as u64)),
                format!("avg {}/s · session", bytes(average_speed(session) as u64)),
                self.theme.cyan,
            ),
            (
                if sender { " Sent " } else { " Written " },
                bytes(session.transferred_bytes),
                format!(
                    "{} entries · {} confirmed",
                    session.sent_entries, session.committed_entries
                ),
                self.theme.violet,
            ),
            (
                " Durable archive ",
                bytes(session.durable_bytes),
                format!("{} resumed", bytes(session.resumed_bytes)),
                self.theme.green,
            ),
            (
                " Elapsed ",
                elapsed(session.elapsed),
                format!("{} skipped · {} retries", session.skips, session.retries),
                self.theme.amber,
            ),
        ];
        let regions = if compact {
            Layout::vertical([Constraint::Length(3), Constraint::Length(3)])
                .split(area)
                .iter()
                .flat_map(|row| {
                    Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
                        .split(*row)
                        .to_vec()
                })
                .collect::<Vec<_>>()
        } else {
            Layout::horizontal([Constraint::Percentage(25); 4])
                .split(area)
                .to_vec()
        };
        for ((title, value, detail, color), region) in cards.into_iter().zip(regions) {
            let text = if compact {
                vec![Line::styled(
                    value,
                    self.theme.panel().fg(color).add_modifier(Modifier::BOLD),
                )]
            } else {
                vec![
                    Line::styled(
                        value,
                        self.theme.panel().fg(color).add_modifier(Modifier::BOLD),
                    ),
                    Line::styled(detail, self.theme.muted()),
                ]
            };
            frame.render_widget(
                Paragraph::new(text)
                    .block(panel(title, self.theme, false))
                    .style(self.theme.panel()),
                region,
            );
        }
    }

    fn render_graph(&self, frame: &mut Frame<'_>, area: Rect) {
        let receiving = self
            .sessions
            .get(self.selected)
            .is_some_and(|session| session.role == "receiver");
        if area.height < 5 {
            frame.render_widget(
                Paragraph::new(format!(
                    "{} {}/s · compressed payload",
                    if receiving { "Writes" } else { "Network" },
                    bytes(self.rate as u64)
                ))
                .style(self.theme.panel())
                .block(panel(" Throughput ", self.theme, false)),
                area,
            );
            return;
        }
        let now = Instant::now();
        let points = self
            .history
            .iter()
            .map(|(at, rate)| (-now.saturating_duration_since(*at).as_secs_f64(), *rate))
            .collect::<Vec<_>>();
        let ceiling = points.iter().map(|(_, rate)| *rate).fold(1.0_f64, f64::max) * 1.1;
        let graph = Chart::new(vec![
            Dataset::default()
                .marker(Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(self.theme.cyan))
                .data(&points),
        ])
        .block(panel(
            if receiving {
                " Archive writes · 60 × 1 s "
            } else {
                " Network throughput · 60 × 1 s "
            },
            self.theme,
            false,
        ))
        .style(self.theme.panel())
        .x_axis(
            Axis::default()
                .bounds([-60.0, 0.0])
                .labels(["−60 s", "now"])
                .style(self.theme.muted()),
        )
        .y_axis(
            Axis::default()
                .bounds([0.0, ceiling])
                .labels(["0".to_owned(), format!("{}/s", bytes(ceiling as u64))])
                .style(self.theme.muted()),
        );
        frame.render_widget(graph, area);
    }

    fn render_pipeline_and_graph(&self, frame: &mut Frame<'_>, area: Rect) {
        let pipeline_height = self
            .sessions
            .get(self.selected)
            .map_or(3, |session| session.pipelines.len() as u16 + 2);
        if area.height >= pipeline_height + 5 {
            let rows = Layout::vertical([Constraint::Length(pipeline_height), Constraint::Min(5)])
                .split(area);
            self.render_pipeline(frame, rows[0]);
            self.render_graph(frame, rows[1]);
        } else {
            self.render_pipeline(frame, area);
        }
    }

    fn render_pipeline(&self, frame: &mut Frame<'_>, area: Rect) {
        let block = panel(" Pipeline · current state / time ", self.theme, false);
        let Some(session) = self.sessions.get(self.selected) else {
            frame.render_widget(
                Paragraph::new("Waiting for a session…")
                    .block(block)
                    .style(self.theme.panel()),
                area,
            );
            return;
        };
        let rows = session.pipelines.iter().map(|pipeline| {
            let color = match pipeline.state {
                PipelineState::Done => self.theme.green,
                PipelineState::Failed => self.theme.red,
                PipelineState::Stopped => self.theme.amber,
                state if state.is_waiting() => self.theme.amber,
                _ => self.theme.cyan,
            };
            Row::new(vec![
                Line::styled(pipeline.stage.label(), self.theme.panel()),
                Line::styled(pipeline.state.label(), self.theme.panel().fg(color)),
                Line::styled(elapsed(pipeline.elapsed), self.theme.muted()).right_aligned(),
            ])
        });
        frame.render_widget(
            Table::new(
                rows,
                [
                    Constraint::Length(10),
                    Constraint::Min(8),
                    Constraint::Length(8),
                ],
            )
            .column_spacing(1)
            .block(block)
            .style(self.theme.panel()),
            area,
        );
    }

    fn render_details(&self, frame: &mut Frame<'_>, area: Rect) {
        let mut lines = Vec::new();
        if let Some(retry) = &self.retry {
            lines.push(Line::styled(
                retry_status(retry, Instant::now()),
                self.theme.panel().fg(self.theme.amber),
            ));
            lines.push(Line::from(format!(
                "Last error: {}",
                safe_text(&retry.error)
            )));
        }
        if let Some(error) = &self.error {
            lines.push(Line::styled(
                safe_text(error),
                self.theme.panel().fg(self.theme.red),
            ));
        }
        if let Some(session) = self.sessions.get(self.selected) {
            if session.role == "sender" {
                let scan = if session.scan_finished {
                    "Complete"
                } else if session.phase.is_finished() {
                    "Incomplete"
                } else if matches!(session.phase, Phase::Connecting | Phase::Listening) {
                    "Pending"
                } else {
                    "Streaming"
                };
                lines.push(Line::from(format!(
                    "Scan: {scan} · Transfer: {}",
                    session.phase.label()
                )));
            } else {
                lines.push(Line::from(format!("Transfer: {}", session.phase.label())));
            }
            lines.push(Line::from(format!("Peer: {}", safe_text(&session.peer))));
            lines.push(Line::from(if session.role == "sender" {
                format!(
                    "Entries: {} sent · {} confirmed · {} found",
                    session.sent_entries, session.committed_entries, session.discovered
                )
            } else {
                format!(
                    "Entries: {} received · {} confirmed",
                    session.sent_entries, session.committed_entries
                )
            }));
            lines.push(Line::from(format!(
                "Bundles: {} · skips: {} · file retries: {}",
                session.bundles, session.skips, session.retries
            )));
            lines.push(Line::from(format!(
                "Speed: {}/s (5 s) · {}/s avg/session",
                bytes(self.rate as u64),
                bytes(average_speed(session) as u64)
            )));
            if session.role == "sender" {
                lines.push(Line::from(format!(
                    "Source processing: {}/s (5 s) · {} raw processed",
                    bytes(self.source_rate as u64),
                    bytes(session.raw_processed_bytes),
                )));
                if let Some(file) = &session.current_file {
                    lines.push(Line::from(format!(
                        "Current file: {}",
                        safe_text(&file.relative_path),
                    )));
                    lines.push(Line::from(format!(
                        "File size: {} (at open)",
                        bytes(file.size),
                    )));
                    lines.push(Line::from(format!(
                        "File processed: {} / {}",
                        bytes(file.processed_bytes),
                        bytes(file.size),
                    )));
                } else {
                    lines.push(Line::from("Current file: —"));
                }
            }
            if session.role == "receiver" {
                lines.push(Line::styled(
                    "Written this session includes retries; excludes discarded data.",
                    self.theme.muted(),
                ));
            }
            if let Some(archive) = &session.archive {
                lines.push(Line::from(format!(
                    "Archive: {}",
                    safe_text(&archive.to_string_lossy())
                )));
                if session.skips > 0 && matches!(session.phase, Phase::Completed | Phase::Warnings)
                {
                    let mut issues = archive.as_os_str().to_os_string();
                    issues.push(".issues.tsv");
                    lines.push(Line::from(format!(
                        "Issues: {}",
                        safe_text(&issues.to_string_lossy())
                    )));
                }
            }
            if let Some(error) = &session.error {
                lines.push(Line::styled(
                    safe_text(error),
                    self.theme.panel().fg(self.theme.red),
                ));
            }
        }
        for (name, value) in &self.configuration {
            lines.push(Line::from(format!("{name}: {value}")));
        }
        if !self.finished && !self.cancelled {
            lines.push(Line::styled(
                "Scanning and sending overlap; no fixed total or ETA.",
                self.theme.muted(),
            ));
        }
        let inner_width = usize::from(area.width.saturating_sub(2)).max(1);
        let lines = wrap_details(lines, inner_width);
        let max_scroll = lines
            .len()
            .saturating_sub(usize::from(area.height.saturating_sub(2)));
        self.details_scroll_limit.set(max_scroll);
        frame.render_widget(
            Paragraph::new(lines)
                .block(panel(
                    if self.focus == Focus::Details {
                        " Details · ↑↓ scroll "
                    } else {
                        " Details "
                    },
                    self.theme,
                    self.focus == Focus::Details,
                ))
                .style(self.theme.panel())
                .scroll((
                    self.details_offset
                        .min(max_scroll)
                        .min(usize::from(u16::MAX)) as u16,
                    0,
                )),
            area,
        );
    }

    fn render_logs(&self, frame: &mut Frame<'_>, area: Rect) {
        let visible = area.height.saturating_sub(2) as usize;
        let end = self.logs.len().saturating_sub(self.log_offset);
        let start = end.saturating_sub(visible);
        let mut lines = self.logs[start..end]
            .iter()
            .map(|entry| {
                let color = match entry.level {
                    log::Level::Error => self.theme.red,
                    log::Level::Warn => self.theme.amber,
                    log::Level::Info => self.theme.cyan,
                    _ => self.theme.muted,
                };
                Line::from(vec![
                    Span::styled(format!("{:<5} ", entry.level), self.theme.panel().fg(color)),
                    Span::raw(safe_text(&entry.message)),
                ])
            })
            .collect::<Vec<_>>();
        if lines.is_empty() {
            lines.push(Line::styled(
                "No warnings or errors. Detailed logs: --debug",
                self.theme.muted(),
            ));
        }
        let title = if self.log_offset == 0 {
            " Activity · following latest "
        } else {
            " Activity · End to follow "
        };
        frame.render_widget(
            Paragraph::new(lines)
                .block(panel(title, self.theme, self.focus == Focus::Logs))
                .style(self.theme.panel()),
            area,
        );
    }
}

pub(super) fn panel<'a>(title: &'a str, theme: Theme, active: bool) -> Block<'a> {
    Block::default()
        .borders(Borders::ALL)
        .title(title)
        .style(theme.panel())
        .border_style(Style::default().fg(if active { theme.cyan } else { theme.muted }))
}

pub(super) fn render_dialog(frame: &mut Frame<'_>, theme: Theme, title: &str, lines: &[&str]) {
    let screen = frame.area();
    let width = screen.width.saturating_sub(4).min(72);
    let height = screen.height.saturating_sub(2).min(lines.len() as u16 + 2);
    let area = Rect::new(
        screen.x + screen.width.saturating_sub(width) / 2,
        screen.y + screen.height.saturating_sub(height) / 2,
        width,
        height,
    );
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(
            lines
                .iter()
                .map(|line| Line::from(*line))
                .collect::<Vec<_>>(),
        )
        .block(panel(title, theme, true))
        .style(theme.panel())
        .wrap(Wrap { trim: false }),
        area,
    );
}

fn phase_color(phase: &Phase, theme: Theme) -> Color {
    match phase {
        Phase::Completed => theme.green,
        Phase::Warnings | Phase::Stopped => theme.amber,
        Phase::Failed => theme.red,
        _ => theme.cyan,
    }
}

fn retry_status(retry: &RetrySnapshot, now: Instant) -> String {
    let remaining = retry.deadline.saturating_duration_since(now);
    let seconds = remaining.as_secs() + u64::from(remaining.subsec_nanos() > 0);
    format!("Retry {}/{} in {seconds} s", retry.number, retry.limit)
}

pub(super) fn safe_text(text: &str) -> String {
    text.chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect()
}

pub(super) fn bytes(value: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut amount = value as f64;
    let mut unit = 0;
    while amount >= 1024.0 && unit + 1 < UNITS.len() {
        amount /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{value} B")
    } else {
        format!("{amount:.1} {}", UNITS[unit])
    }
}

fn elapsed(duration: Duration) -> String {
    let seconds = duration.as_secs();
    format!(
        "{:02}:{:02}:{:02}",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60
    )
}

fn average_speed(session: &SessionSnapshot) -> f64 {
    let elapsed = session.elapsed.as_secs_f64();
    if elapsed > 0.0 {
        session.transferred_bytes as f64 / elapsed
    } else {
        0.0
    }
}

fn wrap_details(lines: Vec<Line<'static>>, width: usize) -> Vec<Line<'static>> {
    let mut wrapped = Vec::new();
    for line in lines {
        let mut text = String::new();
        let mut used = 0;
        for character in line.spans.iter().flat_map(|span| span.content.chars()) {
            let columns = Span::raw(character.to_string()).width();
            if used + columns > width && !text.is_empty() {
                wrapped.push(Line::styled(std::mem::take(&mut text), line.style));
                used = 0;
            }
            text.push(character);
            used += columns;
        }
        wrapped.push(Line::styled(text, line.style));
    }
    wrapped
}

#[cfg(test)]
mod tests {
    use super::super::theme::ColorMode;
    use super::*;
    use crate::progress::{PipelineSnapshot, PipelineStage};
    use ratatui::{Terminal, backend::TestBackend};

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
}

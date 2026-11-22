use super::{Dashboard, Focus};
use crate::progress::{Phase, PipelineState, RetrySnapshot, SessionSnapshot};
use crate::ui::theme::Theme;
use crate::ui::widgets::{bytes, panel, render_dialog, safe_text};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols::Marker;
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Axis, Block, Chart, Dataset, GraphType, List, ListItem, ListState, Paragraph, Row, Table, Wrap,
};
use std::time::{Duration, Instant};

impl Dashboard {
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
        if let Some(lines) = self.completion_lines() {
            frame.render_widget(
                Paragraph::new(lines)
                    .block(panel(" Result ", self.theme, false))
                    .style(self.theme.panel())
                    .wrap(Wrap { trim: false }),
                rows[2],
            );
        } else {
            self.render_cards(frame, rows[2], compact);
        }
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
            } else if self.waiting_for_transfer() {
                "Waiting for a transfer"
            } else if self.finished {
                "Transfer ended."
            } else {
                "Transfer continues."
            }),
        ];
        if let Some(retry) = &self.retry {
            lines.push(Line::from(retry_status(retry, Instant::now())));
        }
        if let Some(result) = self.completion_lines() {
            lines.extend(result);
        } else if let Some(session) = self.sessions.get(self.selected) {
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
        let subtitle = if self.waiting_for_transfer() {
            if self.sessions.is_empty() {
                format!(" Waiting for a transfer · {}", safe_text(&self.context))
            } else {
                format!(
                    " Waiting for the next transfer · {}",
                    safe_text(&self.context)
                )
            }
        } else {
            self.sessions.get(self.selected).map_or_else(
                || format!(" {}", safe_text(&self.context)),
                |session| {
                    format!(
                        " {} · {} · {}",
                        safe_text(&session.role),
                        safe_text(&session.dump_id),
                        safe_text(&session.peer)
                    )
                },
            )
        };
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

    fn waiting_for_transfer(&self) -> bool {
        self.receiver_listening
            && !self.finished
            && !self.cancelled
            && self
                .sessions
                .iter()
                .all(|session| session.phase.is_finished())
    }

    fn completion_lines(&self) -> Option<Vec<Line<'static>>> {
        let session = self.sessions.get(self.selected)?;
        if !matches!(session.phase, Phase::Completed | Phase::Warnings) {
            return None;
        }
        let status = if session.archive.is_some() {
            "Archive ready"
        } else {
            "Transfer complete"
        };
        let mut lines = vec![Line::styled(
            format!("{status} · Skipped: {}", session.skips),
            self.theme
                .panel()
                .fg(phase_color(&session.phase, self.theme)),
        )];
        if let Some(archive) = &session.archive {
            lines.push(Line::from(safe_text(&archive.to_string_lossy())));
        }
        Some(lines)
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

fn phase_color(phase: &Phase, theme: Theme) -> Color {
    match phase {
        Phase::Completed => theme.green,
        Phase::Warnings | Phase::Stopped => theme.amber,
        Phase::Failed => theme.red,
        _ => theme.cyan,
    }
}

pub(super) fn retry_status(retry: &RetrySnapshot, now: Instant) -> String {
    let remaining = retry.deadline.saturating_duration_since(now);
    let seconds = remaining.as_secs() + u64::from(remaining.subsec_nanos() > 0);
    format!("Retry {}/{} in {seconds} s", retry.number, retry.limit)
}

pub(super) fn elapsed(duration: Duration) -> String {
    let seconds = duration.as_secs();
    format!(
        "{:02}:{:02}:{:02}",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60
    )
}

pub(super) fn average_speed(session: &SessionSnapshot) -> f64 {
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

use super::input::field_window;
use super::{Step, Wizard};
use crate::cli::Command;
use crate::ui::widgets::{panel, render_dialog, safe_text};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, List, ListItem, ListState, Paragraph, Wrap};

impl Wizard {
    pub(super) fn render(&self, frame: &mut Frame<'_>) {
        let area = frame.area();
        frame.render_widget(Block::default().style(self.theme.base()), area);
        if area.width < 60 || area.height < 20 {
            frame.render_widget(
                Paragraph::new(
                    "fast-dump setup\nResize to at least 60 × 20.\nEsc / Ctrl-C cancels setup.",
                )
                .style(self.theme.base())
                .wrap(Wrap { trim: false }),
                area,
            );
            return;
        }
        let rows = Layout::vertical([
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(4),
            Constraint::Length(2),
        ])
        .split(area);
        frame.render_widget(
            Paragraph::new(vec![
                Line::styled(" fast-dump / New transfer", self.theme.accent()),
                Line::styled(
                    " Set up this side of a sender / receiver pair.",
                    self.theme.base().fg(self.theme.muted),
                ),
            ]),
            rows[0],
        );
        if self.step == Step::Review {
            self.render_review(frame, rows[1]);
        } else if area.width >= 100 {
            let columns =
                Layout::horizontal([Constraint::Percentage(55), Constraint::Percentage(45)])
                    .split(rows[1]);
            self.render_form(frame, columns[0]);
            self.render_instructions(frame, columns[1]);
        } else {
            self.render_form(frame, rows[1]);
        }
        let message = self.error.as_deref().unwrap_or_else(|| self.instruction());
        frame.render_widget(
            Paragraph::new(safe_text(message))
                .block(panel(
                    if self.error.is_some() {
                        " Check this field "
                    } else {
                        " Tip "
                    },
                    self.theme,
                    false,
                ))
                .style(self.theme.panel().fg(if self.error.is_some() {
                    self.theme.red
                } else {
                    self.theme.muted
                }))
                .wrap(Wrap { trim: false }),
            rows[2],
        );
        let controls = if self.step == Step::Review {
            " Enter start transfer  Shift-Tab back  F1 help\n Esc / Ctrl-C cancel"
        } else {
            " Tab / Enter next  Shift-Tab back  ←→ choose  F1 help\n Esc / Ctrl-C cancel"
        };
        frame.render_widget(
            Paragraph::new(controls).style(self.theme.base().fg(self.theme.muted)),
            rows[3],
        );
        if self.help {
            render_dialog(
                frame,
                self.theme,
                " Setup help ",
                &[
                    "Sender reads files. Receiver writes the archive.",
                    "Push: sender connects; receiver listens.",
                    "Pull: sender listens; receiver connects.",
                    "Both sides must use the same token when one is set.",
                    "The token is an access check, not transport encryption.",
                    "",
                    "Tab / Enter advances after validating the current field.",
                    "Shift-Tab returns. Left / Right changes a choice.",
                    "Text fields support arrows, Home, End and paste.",
                    "Nothing is started until you accept the review screen.",
                    "Advanced tuning remains available through CLI options.",
                    "",
                    "F1 / Enter / Esc closes help.",
                ],
            );
        }
    }

    fn render_form(&self, frame: &mut Frame<'_>, area: ratatui::layout::Rect) {
        let steps = self.steps();
        let items = steps
            .iter()
            .map(|step| {
                let active = *step == self.step;
                let value = match step {
                    Step::Role => {
                        if self.sender {
                            "‹ Sender ›".to_owned()
                        } else {
                            "‹ Receiver ›".to_owned()
                        }
                    }
                    Step::Transport => {
                        if self.connect {
                            "‹ Connect to peer ›".to_owned()
                        } else {
                            "‹ Listen for peer ›".to_owned()
                        }
                    }
                    Step::Address => self.address.display(false, active),
                    Step::Path => self.path.display(false, active),
                    Step::DumpId => {
                        if self.dump_id.value.is_empty() && !active {
                            "(source directory name)".into()
                        } else {
                            self.dump_id.display(false, active)
                        }
                    }
                    Step::Compression => self.level.display(false, active),
                    Step::Token => {
                        if self.token.value.is_empty() && !active {
                            "(none)".into()
                        } else {
                            self.token.display(true, active)
                        }
                    }
                    Step::Review => "Review settings and start →".into(),
                };
                let cursor = if active {
                    match step {
                        Step::Address => Some(self.address.cursor),
                        Step::Path => Some(self.path.cursor),
                        Step::DumpId => Some(self.dump_id.cursor),
                        Step::Compression => Some(self.level.cursor),
                        Step::Token => Some(self.token.cursor),
                        _ => None,
                    }
                } else {
                    None
                };
                let value = field_window(&value, cursor, area.width.saturating_sub(19) as usize);
                ListItem::new(Line::from(vec![
                    Span::styled(format!("{:<14}", self.label(*step)), self.theme.muted()),
                    Span::raw(value),
                ]))
            })
            .collect::<Vec<_>>();
        let mut state =
            ListState::default().with_selected(steps.iter().position(|step| *step == self.step));
        frame.render_stateful_widget(
            List::new(items)
                .block(panel(" Transfer settings ", self.theme, true))
                .style(self.theme.panel())
                .highlight_style(self.theme.selected())
                .highlight_symbol("› "),
            area,
            &mut state,
        );
    }

    fn render_instructions(&self, frame: &mut Frame<'_>, area: ratatui::layout::Rect) {
        frame.render_widget(
            Paragraph::new(vec![
                Line::styled("One binary. Two sides.", self.theme.accent()),
                Line::from(""),
                Line::from("SENDER    source files → compressed data"),
                Line::from("RECEIVER  compressed data → .tar.zst"),
                Line::from(""),
                Line::styled(
                    "Push",
                    self.theme
                        .panel()
                        .fg(self.theme.violet)
                        .add_modifier(Modifier::BOLD),
                ),
                Line::from("Receiver listens; sender connects."),
                Line::styled(
                    "Pull",
                    self.theme
                        .panel()
                        .fg(self.theme.violet)
                        .add_modifier(Modifier::BOLD),
                ),
                Line::from("Sender listens; receiver connects."),
                Line::from(""),
                Line::from("Run the listening side first."),
                Line::from("Resume with the same source and dump ID."),
            ])
            .block(panel(" How it works ", self.theme, false))
            .style(self.theme.panel())
            .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn render_review(&self, frame: &mut Frame<'_>, area: ratatui::layout::Rect) {
        let mut lines = vec![
            Line::styled("Ready to start", self.theme.accent()),
            Line::from(""),
            Line::from(format!(
                "Role       {}",
                if self.sender { "Sender" } else { "Receiver" }
            )),
            Line::from(format!(
                "Transport  {} {}",
                if self.connect {
                    "Connect to"
                } else {
                    "Listen on"
                },
                safe_text(&self.address.value)
            )),
            Line::from(format!("Directory  {}", safe_text(&self.path.value))),
            Line::from(format!(
                "Retries    {}",
                crate::ui::retry_description(&self.retry_policy, !self.sender && !self.connect)
            )),
            Line::from(format!(
                "Token      {}",
                if self.token.value.is_empty() {
                    "None"
                } else {
                    "•••••••• (set)"
                }
            )),
        ];
        if let Ok(cli) = self.build_cli() {
            match cli.command {
                Command::Sender(args) => {
                    lines.push(Line::from(format!(
                        "Dump ID    {}",
                        safe_text(&args.resolve_dump_id().unwrap_or_default())
                    )));
                    if let Ok(options) = args.runtime_options() {
                        lines.push(Line::from(format!(
                            "Compression zstd level {} · {} threads",
                            options.level, options.threads
                        )));
                        lines.push(Line::from(format!(
                            "Defaults   {}–{} readers · {} MiB bundle target",
                            options.min_readers,
                            options.max_readers,
                            options.bundle_target_bytes / 1024 / 1024
                        )));
                    }
                }
                Command::Receiver(args) => {
                    lines.push(Line::from(format!(
                        "Defaults   {} concurrent session(s)",
                        args.max_sessions
                    )));
                }
            }
        }
        lines.push(Line::from(""));
        lines.push(Line::styled(
            "Enter starts transfer. Shift-Tab returns to settings.",
            self.theme.accent(),
        ));
        if area.height < 15 {
            lines.retain(|line| line.width() != 0);
            lines.remove(0);
        }
        frame.render_widget(
            Paragraph::new(lines)
                .block(panel(" Review ", self.theme, true))
                .style(self.theme.panel())
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn label(&self, step: Step) -> &'static str {
        match step {
            Step::Role => "Role",
            Step::Transport => "Transport",
            Step::Address => "Address",
            Step::Path => {
                if self.sender {
                    "Source"
                } else {
                    "Data directory"
                }
            }
            Step::DumpId => "Dump ID",
            Step::Compression => "Zstd level",
            Step::Token => "Token",
            Step::Review => "Start",
        }
    }

    fn instruction(&self) -> &'static str {
        match self.step {
            Step::Role => "Choose Sender on the source host, or Receiver on the archive host.",
            Step::Transport => "One peer listens; the other connects. Either role can listen.",
            Step::Address => {
                if self.connect {
                    "Enter the listening peer's IP and port. 127.0.0.1 is this computer; use the peer's network IP for another host."
                } else {
                    "Enter a local IP address and port to listen on. 0.0.0.0:7000 accepts IPv4 on all interfaces."
                }
            }
            Step::Path => {
                if self.sender {
                    "Enter the source directory path. Files are read without modifying the source."
                } else {
                    "Enter the directory for archives and resume state. It will be created when the receiver starts."
                }
            }
            Step::DumpId => {
                "Optional: leave empty to use the source directory name. Use the same ID when resuming."
            }
            Step::Compression => {
                "Default 3. Lower levels favor speed; higher levels favor compression. Negative levels are faster; 0 uses the zstd default."
            }
            Step::Token => {
                "Optional shared token. Input is masked; enter the same token on both peers."
            }
            Step::Review => {
                "No connection has been opened yet. Enter confirms these settings and starts the runtime."
            }
        }
    }
}

use super::input::field_window;
use super::{Step, Transport, Wizard};
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
                Line::styled(" fast-dump / Setup", self.theme.accent()),
                Line::styled(
                    " Start a relay or set up this side of a transfer.",
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
        let controls = match self.step {
            Step::Transport => " ←→ choose mode  Enter confirm  F1 help\n Esc / Ctrl-C cancel",
            Step::Review => " Enter start  Shift-Tab back  F1 help\n Esc / Ctrl-C cancel",
            _ => " Tab / Enter next  Shift-Tab back  ←→ choose  F1 help\n Esc / Ctrl-C cancel",
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
                    "For P2P, one peer listens and the other connects.",
                    "Choose who sends files after connecting.",
                    "Relay starts the server. Both peers choose Connect → Via relay.",
                    "Both sides must use the same token when one is set.",
                    "The token is an access check, not transport encryption.",
                    "",
                    "Enter confirms the transport. Tab / Enter advances in the form.",
                    "Shift-Tab returns. Left / Right changes a choice.",
                    "Text fields support arrows, Home, End and paste.",
                    "Nothing is started until you accept the review screen.",
                    "",
                    "F1 / Enter / Esc closes help.",
                ],
            );
        }
    }

    fn render_form(&self, frame: &mut Frame<'_>, area: ratatui::layout::Rect) {
        let steps = self.steps();
        let visible = match self.transport {
            None => vec![
                Step::Transport,
                Step::Address,
                Step::RelayPort,
                Step::AccessToken,
                Step::Token,
                Step::Review,
            ],
            Some(_) => steps.clone(),
        };
        let items = visible
            .iter()
            .copied()
            .map(|step| {
                let active = step == self.step;
                let enabled = steps.contains(&step);
                let label = Span::raw(format!("{:<16}", self.label(step)));
                if !enabled {
                    return ListItem::new(Line::from(vec![label, Span::raw("—")]))
                        .style(self.theme.muted().add_modifier(Modifier::DIM));
                }
                if step == Step::Transport {
                    let mut row = vec![label];
                    for transport in Transport::ALL {
                        row.push(if transport == self.transport_choice {
                            Span::styled(format!("[{}] ", transport.label()), self.theme.accent())
                        } else {
                            Span::styled(format!(" {}  ", transport.label()), self.theme.muted())
                        });
                    }
                    return ListItem::new(Line::from(row));
                }
                let input = match step {
                    Step::Address => self.transport.map(|mode| &self.addresses[mode as usize]),
                    Step::RelayPort => self.transport.map(|mode| self.relay_port_input(mode)),
                    Step::AccessToken => Some(&self.access_token),
                    Step::RelayCa => Some(&self.relay_ca),
                    Step::Token => Some(&self.token),
                    _ => None,
                };
                let value = match step {
                    Step::RelayTls if self.trust_first_key => "‹ Temporary relay ›".into(),
                    Step::RelayTls => "‹ CA certificate ›".into(),
                    Step::Connection => {
                        if self.transport == Some(Transport::ViaRelay) {
                            "‹ Via relay ›".to_owned()
                        } else {
                            "‹ Direct ›".to_owned()
                        }
                    }
                    Step::Token if self.token.value.is_empty() && !active => "(none)".into(),
                    Step::RelayCa if self.relay_ca.value.is_empty() && !active => {
                        "(public trust)".into()
                    }
                    Step::Review => "Review settings →".into(),
                    _ => input.map_or_else(String::new, |input| {
                        input.display(matches!(step, Step::Token | Step::AccessToken), active)
                    }),
                };
                let cursor = input.filter(|_| active).map(|input| input.cursor);
                let value = field_window(&value, cursor, area.width.saturating_sub(20) as usize);
                ListItem::new(Line::from(vec![label, Span::raw(value)]))
            })
            .collect::<Vec<_>>();
        let mut state =
            ListState::default().with_selected(visible.iter().position(|step| *step == self.step));
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
                Line::from(
                    "For P2P, start Listen on one peer and use Connect → Direct on the other.",
                ),
                Line::from("To use a relay, both peers choose Connect → Via relay."),
                Line::from("Once connected, either peer can offer to send files."),
            ])
            .block(panel(" How it works ", self.theme, false))
            .style(self.theme.panel())
            .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn render_review(&self, frame: &mut Frame<'_>, area: ratatui::layout::Rect) {
        let Some(transport) = self.transport else {
            return;
        };
        let mut lines = vec![
            Line::styled("Ready to start", self.theme.accent()),
            Line::from(""),
        ];
        if transport == Transport::Relay {
            lines.extend([
                Line::from("Mode       Relay server"),
                Line::from(format!(
                    "Listen on  {}",
                    safe_text(&self.endpoint(transport))
                )),
                Line::from("Client commands appear when the relay is ready."),
            ]);
        } else if transport == Transport::ViaRelay {
            lines.extend([
                Line::from(format!(
                    "Relay       {}",
                    safe_text(&self.endpoint(transport))
                )),
                Line::from("Relay credentials set"),
                Line::from("Choose a peer after connecting."),
            ]);
        } else {
            lines.extend([
                Line::from(format!(
                    "P2P         {} {}",
                    transport.label(),
                    safe_text(&self.endpoint(transport))
                )),
                Line::from(if self.token.value.is_empty() {
                    "Shared token not set"
                } else {
                    "Shared token set"
                }),
                Line::from("Choose what to send after connecting."),
            ]);
        }
        if matches!(transport, Transport::Connect | Transport::ViaRelay) {
            lines.push(Line::from(format!(
                "Retries     {}",
                crate::ui::retry_description(&self.retry_policy, false)
            )));
        }
        if area.height < 15 {
            lines.retain(|line| line.width() != 0);
            lines.remove(0);
        }
        frame.render_widget(
            Paragraph::new(lines)
                .block(panel(" Review ", self.theme, true))
                .style(self.theme.panel()),
            area,
        );
    }

    fn label(&self, step: Step) -> &'static str {
        match step {
            Step::Transport => "Transport",
            Step::Connection => "Connection",
            Step::Address => match self.transport {
                Some(Transport::Relay) => "Listen address",
                Some(Transport::ViaRelay) => "Relay address",
                Some(Transport::Listen) => "Listen on",
                Some(Transport::Connect) => "Peer address",
                None => "Address",
            },
            Step::RelayPort => "Relay port",
            Step::AccessToken => "Access token",
            Step::RelayTls => "Relay TLS",
            Step::RelayCa => "Relay CA file",
            Step::Token => "Token",
            Step::Review => "Start",
        }
    }

    fn instruction(&self) -> &'static str {
        match self.step {
            Step::Transport => {
                "Relay starts the server. Clients use Connect. Listen accepts a direct connection."
            }
            Step::Connection => {
                "Choose Direct to reach the peer or Via relay when both peers connect to the relay server."
            }
            Step::Address => match self.transport {
                Some(Transport::Relay) => {
                    "Choose a local IP. 0.0.0.0 listens on all IPv4 interfaces."
                }
                Some(Transport::ViaRelay) => {
                    "Enter the relay server's hostname or IP, such as relay.example. Port is a separate field."
                }
                Some(Transport::Listen) => {
                    "Enter a local IP and port to listen on. 0.0.0.0:7000 accepts IPv4 on all interfaces."
                }
                _ => "Enter the listening peer's IP and port. 127.0.0.1 is this computer.",
            },
            Step::RelayPort => "Enter the port the relay listens on. Default: 9876.",
            Step::AccessToken => {
                "Enter the 12-character access token or paste an existing invitation."
            }
            Step::RelayTls => {
                "Temporary relay learns the key on first connection. CA certificate verifies the server identity."
            }
            Step::RelayCa => {
                "Optional PEM file for a private CA. Leave empty to use the public trust store."
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

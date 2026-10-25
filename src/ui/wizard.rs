use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, List, ListItem, ListState, Paragraph, Wrap};

use crate::cli::{Cli, Command};
use crate::retry::RetryPolicy;
use crate::runtime::RunControl;

use super::dashboard::{panel, render_dialog, safe_text};
use super::terminal::TerminalGuard;
use super::theme::Theme;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    Role,
    Transport,
    Address,
    Path,
    DumpId,
    Compression,
    Token,
    Review,
}

#[derive(Default)]
struct Input {
    value: String,
    cursor: usize,
}

impl Input {
    fn new(value: &str) -> Self {
        Self {
            value: value.into(),
            cursor: value.chars().count(),
        }
    }

    fn byte_at(&self, cursor: usize) -> usize {
        self.value
            .char_indices()
            .nth(cursor)
            .map_or(self.value.len(), |(index, _)| index)
    }

    fn insert(&mut self, value: &str) {
        let value = value
            .chars()
            .filter(|ch| !ch.is_control())
            .collect::<String>();
        // Секреты и пути не попадают в event log. Ограничение относится только
        // к редактируемому UI-полю и предотвращает неограниченную вставку из clipboard.
        if self.value.len().saturating_add(value.len()) > 32 * 1024 {
            return;
        }
        let at = self.byte_at(self.cursor);
        self.value.insert_str(at, &value);
        self.cursor += value.chars().count();
    }

    fn key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char(ch)
                if !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                self.insert(&ch.to_string())
            }
            KeyCode::Backspace if self.cursor > 0 => {
                let start = self.byte_at(self.cursor - 1);
                let end = self.byte_at(self.cursor);
                self.value.replace_range(start..end, "");
                self.cursor -= 1;
            }
            KeyCode::Delete if self.cursor < self.value.chars().count() => {
                let start = self.byte_at(self.cursor);
                let end = self.byte_at(self.cursor + 1);
                self.value.replace_range(start..end, "");
            }
            KeyCode::Left => self.cursor = self.cursor.saturating_sub(1),
            KeyCode::Right => self.cursor = (self.cursor + 1).min(self.value.chars().count()),
            KeyCode::Home => self.cursor = 0,
            KeyCode::End => self.cursor = self.value.chars().count(),
            _ => {}
        }
    }

    fn display(&self, masked: bool, active: bool) -> String {
        let chars = if masked {
            self.value.chars().map(|_| '•').collect::<Vec<_>>()
        } else {
            self.value.chars().collect::<Vec<_>>()
        };
        let mut text = String::new();
        for (index, ch) in chars.iter().enumerate() {
            if active && index == self.cursor {
                text.push('▏');
            }
            text.push(*ch);
        }
        if active && self.cursor == chars.len() {
            text.push('▏');
        }
        text
    }
}

struct Wizard {
    debug: bool,
    retry_policy: RetryPolicy,
    sender: bool,
    connect: bool,
    step: Step,
    address: Input,
    inactive_address: Input,
    path: Input,
    dump_id: Input,
    level: Input,
    token: Input,
    error: Option<String>,
    help: bool,
    theme: Theme,
}

impl Wizard {
    fn new(debug: bool, retry_policy: RetryPolicy, theme: Theme) -> Self {
        Self {
            debug,
            retry_policy,
            sender: true,
            connect: true,
            step: Step::Role,
            address: Input::new("127.0.0.1:7000"),
            inactive_address: Input::new("0.0.0.0:7000"),
            path: Input::default(),
            dump_id: Input::default(),
            level: Input::new(&crate::cli::DEFAULT_LEVEL.to_string()),
            token: Input::default(),
            error: None,
            help: false,
            theme,
        }
    }

    fn set_connect(&mut self, connect: bool) {
        if self.connect != connect {
            // Адрес назначения и адрес привязки хранятся отдельно, чтобы смена
            // режима не переносила wildcard в Connect и не теряла введённый IP.
            std::mem::swap(&mut self.address, &mut self.inactive_address);
            self.connect = connect;
        }
    }

    fn steps(&self) -> Vec<Step> {
        let mut steps = vec![Step::Role, Step::Transport, Step::Address, Step::Path];
        if self.sender {
            steps.extend([Step::Compression, Step::DumpId]);
        }
        steps.extend([Step::Token, Step::Review]);
        steps
    }

    fn next(&mut self) {
        match self.validate_step() {
            Ok(()) => {
                let steps = self.steps();
                let index = steps
                    .iter()
                    .position(|step| *step == self.step)
                    .unwrap_or(0);
                self.step = steps[(index + 1).min(steps.len() - 1)];
                self.error = None;
            }
            Err(error) => self.error = Some(error.to_string()),
        }
    }

    fn back(&mut self) {
        let steps = self.steps();
        let index = steps
            .iter()
            .position(|step| *step == self.step)
            .unwrap_or(0);
        self.step = steps[index.saturating_sub(1)];
        self.error = None;
    }

    fn current_input(&mut self) -> Option<&mut Input> {
        match self.step {
            Step::Address => Some(&mut self.address),
            Step::Path => Some(&mut self.path),
            Step::DumpId => Some(&mut self.dump_id),
            Step::Compression => Some(&mut self.level),
            Step::Token => Some(&mut self.token),
            _ => None,
        }
    }

    fn validate_step(&self) -> Result<()> {
        match self.step {
            Step::Address => {
                self.address.value.parse::<SocketAddr>().context(
                    "Enter an IP address and port, for example 127.0.0.1:7000 or [::1]:7000",
                )?;
            }
            Step::Path => self.validate_path()?,
            Step::DumpId => {
                let cli = self.build_cli()?;
                if let Command::Sender(args) = cli.command {
                    args.resolve_dump_id()?;
                }
            }
            Step::Compression => {
                self.validated_level()?;
            }
            Step::Review => {
                self.validated_cli()?;
            }
            _ => {}
        }
        Ok(())
    }

    fn validate_path(&self) -> Result<()> {
        if self.path.value.trim().is_empty() {
            bail!(if self.sender {
                "Choose a source directory"
            } else {
                "Choose a destination directory"
            });
        }
        let path = Path::new(&self.path.value);
        if self.sender {
            if !path.is_dir() {
                bail!("Source must be an existing directory");
            }
        } else if path
            .try_exists()
            .context("Could not inspect destination directory")?
            && !path.is_dir()
        {
            bail!("Destination exists and is not a directory");
        }
        Ok(())
    }

    fn validated_level(&self) -> Result<i32> {
        let level = self
            .level
            .value
            .parse::<i32>()
            .context("Enter a whole number for the zstd level")?;
        let range = zstd::compression_level_range();
        if !range.contains(&level) {
            bail!(
                "Zstd level must be between {} and {} (default {})",
                range.start(),
                range.end(),
                crate::cli::DEFAULT_LEVEL
            );
        }
        Ok(level)
    }

    fn build_cli(&self) -> Result<Cli> {
        let mut args = vec!["fast-dump".to_owned()];
        if self.debug {
            args.push("--debug".into());
        }
        args.push(if self.sender { "sender" } else { "receiver" }.into());
        args.push(format!(
            "{}={}",
            if self.connect {
                "--connect"
            } else {
                "--listen"
            },
            self.address.value,
        ));
        args.push(format!(
            "{}={}",
            if self.sender {
                "--source"
            } else {
                "--data-dir"
            },
            self.path.value,
        ));
        if self.sender && !self.dump_id.value.is_empty() {
            args.push(format!("--dump-id={}", self.dump_id.value));
        }
        if self.sender {
            args.push(format!("--level={}", self.validated_level()?));
        }
        if !self.token.value.is_empty() {
            // Значение с ведущим '-' остаётся value, а не неизвестным флагом,
            // который clap мог бы повторить в диагностике вместе с секретом.
            args.push(format!("--token={}", self.token.value));
        }
        let mut cli = Cli::try_parse_from(args)?;
        cli.retries = Some(self.retry_policy.retries);
        cli.retry_delay_secs = u32::try_from(self.retry_policy.delay.as_secs())
            .context("retry delay exceeds the supported range")?;
        Ok(cli)
    }

    fn validated_cli(&self) -> Result<Cli> {
        self.validate_path()?;
        let cli = self.build_cli()?;
        match &cli.command {
            Command::Sender(args) => {
                args.resolve_dump_id()?;
                args.runtime_options()?;
            }
            Command::Receiver(args) => {
                args.validated_max_sessions()?;
            }
        }
        Ok(cli)
    }

    fn key(&mut self, key: KeyEvent) -> Result<Option<Cli>> {
        if self.help {
            if matches!(key.code, KeyCode::F(1) | KeyCode::Esc | KeyCode::Enter) {
                self.help = false;
            }
            return Ok(None);
        }
        match key.code {
            KeyCode::F(1) => self.help = true,
            KeyCode::BackTab => self.back(),
            KeyCode::Tab => self.next(),
            KeyCode::Enter if self.step == Step::Review => return self.validated_cli().map(Some),
            KeyCode::Enter => self.next(),
            KeyCode::Left | KeyCode::Right | KeyCode::Up | KeyCode::Down
                if self.step == Step::Role =>
            {
                self.sender = !self.sender;
                self.set_connect(self.sender);
                self.error = None;
            }
            KeyCode::Left | KeyCode::Right | KeyCode::Up | KeyCode::Down
                if self.step == Step::Transport =>
            {
                self.set_connect(!self.connect);
                self.error = None;
            }
            _ => {
                if let Some(input) = self.current_input() {
                    input.key(key);
                    self.error = None;
                }
            }
        }
        Ok(None)
    }

    fn render(&self, frame: &mut Frame<'_>) {
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
                super::retry_description(&self.retry_policy, !self.sender && !self.connect)
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

fn field_window(value: &str, cursor: Option<usize>, width: usize) -> String {
    if Span::raw(value).width() <= width {
        return value.to_owned();
    }
    let characters: Vec<_> = value.chars().collect();
    let mut start = cursor.unwrap_or(0).min(characters.len());
    let mut used = 0;
    while start > 0 {
        let next = Span::raw(characters[start - 1].to_string()).width();
        if used + next > width.saturating_sub(4) {
            break;
        }
        used += next;
        start -= 1;
    }
    let mut result = if start > 0 {
        "…".to_owned()
    } else {
        String::new()
    };
    used = usize::from(start > 0);
    for character in characters.iter().skip(start) {
        let next = Span::raw(character.to_string()).width();
        if used + next > width.saturating_sub(1) {
            result.push('…');
            break;
        }
        result.push(*character);
        used += next;
    }
    result
}

pub(crate) fn wizard(
    debug: bool,
    retry_policy: RetryPolicy,
    control: &RunControl,
) -> Result<Option<Cli>> {
    let mut terminal = TerminalGuard::enter()?;
    let mut wizard = Wizard::new(debug, retry_policy, Theme::detect());
    loop {
        if terminal.was_restored_after_panic() {
            bail!("setup interrupted by a worker panic");
        }
        if control.is_cancelled() {
            return Ok(None);
        }
        terminal
            .terminal
            .draw(|frame| wizard.render(frame))
            .context("could not draw setup wizard")?;
        if !event::poll(Duration::from_millis(200)).context("setup input failed")? {
            continue;
        }
        match event::read().context("could not read setup input")? {
            Event::Key(key) if key.kind != KeyEventKind::Release => {
                if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
                    control.cancel();
                    return Ok(None);
                }
                if key.code == KeyCode::Esc && !wizard.help {
                    return Ok(None);
                }
                match wizard.key(key) {
                    Ok(Some(cli)) if !control.is_cancelled() => return Ok(Some(cli)),
                    Ok(Some(_)) => return Ok(None),
                    Ok(None) => {}
                    Err(error) => wizard.error = Some(error.to_string()),
                }
            }
            Event::Paste(text) if !wizard.help => {
                if let Some(input) = wizard.current_input() {
                    input.insert(&text);
                    wizard.error = None;
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::theme::ColorMode;
    use super::*;
    use ratatui::{Terminal, backend::TestBackend};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn new_wizard(debug: bool, mode: ColorMode) -> Wizard {
        Wizard::new(
            debug,
            RetryPolicy::from_options(None, 10, true),
            Theme::new(mode),
        )
    }

    #[test]
    fn text_editing_keeps_unicode_boundaries_and_masks_secrets() {
        let mut input = Input::new("я界");
        input.key(key(KeyCode::Left));
        input.insert("é");
        assert_eq!(input.value, "яé界");
        input.key(key(KeyCode::Backspace));
        assert_eq!(input.value, "я界");
        input.key(key(KeyCode::Delete));
        assert_eq!(input.value, "я");
        assert_eq!(input.display(true, false), "•");
        input.insert("\u{1b}\n\rsecret");
        assert!(!input.value.chars().any(char::is_control));
    }

    #[test]
    fn wizard_uses_cli_defaults_and_validates_before_start() {
        let mut wizard = new_wizard(true, ColorMode::Monochrome);
        wizard.path = Input::new(".");
        wizard.dump_id = Input::new("wizard-test");
        wizard.token = Input::new("private-token");
        let cli = wizard.validated_cli().unwrap();
        assert!(cli.debug);
        assert_eq!(
            cli.retries,
            Some(RetryPolicy::from_options(None, 10, true).retries)
        );
        assert_eq!(cli.retry_delay_secs, 10);
        match cli.command {
            Command::Sender(args) => {
                assert_eq!(args.token.as_deref(), Some("private-token"));
                assert_eq!(args.frame_target_mb, 64);
                assert_eq!(args.level, 3);
                assert_eq!(args.runtime_options().unwrap().level, 3);
                assert!(args.transport.connect.is_some());
            }
            _ => panic!("expected sender"),
        }
        wizard.dump_id = Input::new("../bad");
        assert!(wizard.validated_cli().is_err());
        wizard.dump_id = Input::new("valid");
        wizard.address = Input::new("missing-port");
        assert!(wizard.validated_cli().is_err());
    }

    #[test]
    fn chosen_compression_level_reaches_runtime_and_remains_visible() {
        for level in [7, -1, 0] {
            let mut wizard = new_wizard(false, ColorMode::Monochrome);
            wizard.path = Input::new(".");
            wizard.dump_id = Input::new("compression-level");
            wizard.step = Step::Compression;
            wizard.key(key(KeyCode::Backspace)).unwrap();
            for character in level.to_string().chars() {
                wizard.key(key(KeyCode::Char(character))).unwrap();
            }
            let mut terminal = Terminal::new(TestBackend::new(60, 20)).unwrap();
            terminal.draw(|frame| wizard.render(frame)).unwrap();
            let screen = terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(|cell| cell.symbol())
                .collect::<String>();
            assert!(screen.contains("Zstd level"));
            assert!(screen.contains(&format!("{level}▏")));

            wizard.key(key(KeyCode::Enter)).unwrap();
            assert_eq!(wizard.step, Step::DumpId);
            wizard.key(key(KeyCode::BackTab)).unwrap();
            assert_eq!(wizard.step, Step::Compression);
            assert_eq!(wizard.level.value, level.to_string());
            wizard.step = Step::Review;
            terminal.draw(|frame| wizard.render(frame)).unwrap();
            let screen = terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(|cell| cell.symbol())
                .collect::<String>();
            assert!(screen.contains(&format!("Compression zstd level {level}")));
            assert!(screen.contains("10 retries · 10 s delay"));
            let Command::Sender(args) = wizard.key(key(KeyCode::Enter)).unwrap().unwrap().command
            else {
                panic!("expected sender");
            };
            assert_eq!(args.runtime_options().unwrap().level, level);
        }
    }

    #[test]
    fn invalid_compression_level_blocks_navigation_and_start() {
        let range = zstd::compression_level_range();
        for value in [
            String::new(),
            "fast".into(),
            "1.5".into(),
            "2147483648".into(),
            (range.start() - 1).to_string(),
            (range.end() + 1).to_string(),
        ] {
            let mut wizard = new_wizard(false, ColorMode::Monochrome);
            wizard.path = Input::new(".");
            wizard.dump_id = Input::new("compression-level");
            wizard.level = Input::new(&value);
            wizard.step = Step::Compression;
            wizard.next();
            assert_eq!(wizard.step, Step::Compression);
            assert!(wizard.error.is_some(), "accepted {value:?}");
            wizard.back();
            assert_eq!(wizard.step, Step::Path);
            wizard.next();
            assert_eq!(wizard.step, Step::Compression);
            wizard.level = Input::new("3");
            wizard.next();
            assert_eq!(wizard.step, Step::DumpId);
            wizard.level = Input::new(&value);
            wizard.step = Step::Review;
            assert!(wizard.key(key(KeyCode::Enter)).is_err());
        }
    }

    #[test]
    fn sender_compression_setting_survives_role_and_transport_switches() {
        let mut wizard = new_wizard(false, ColorMode::Monochrome);
        wizard.path = Input::new(".");
        wizard.dump_id = Input::new("compression-level");
        wizard.level = Input::new("7");
        wizard.step = Step::Transport;
        wizard.key(key(KeyCode::Right)).unwrap();
        assert_eq!(wizard.level.value, "7");
        wizard.step = Step::Role;
        wizard.key(key(KeyCode::Right)).unwrap();
        assert!(!wizard.sender);
        assert!(!wizard.steps().contains(&Step::Compression));
        assert_eq!(wizard.level.value, "7");
        assert!(matches!(
            wizard.validated_cli().unwrap().command,
            Command::Receiver(_)
        ));
        wizard.key(key(KeyCode::Left)).unwrap();
        assert!(wizard.sender);
        let Command::Sender(args) = wizard.validated_cli().unwrap().command else {
            panic!("expected sender");
        };
        assert_eq!(args.runtime_options().unwrap().level, 7);

        wizard.level = Input::new("invalid");
        wizard.key(key(KeyCode::Right)).unwrap();
        assert!(matches!(
            wizard.validated_cli().unwrap().command,
            Command::Receiver(_)
        ));
    }

    #[test]
    fn receiver_choice_defaults_to_listen_and_omits_sender_fields() {
        let mut wizard = new_wizard(false, ColorMode::Monochrome);
        wizard.key(key(KeyCode::Right)).unwrap();
        wizard.path = Input::new(".");
        assert!(!wizard.sender);
        assert!(!wizard.connect);
        assert!(!wizard.steps().contains(&Step::DumpId));
        assert!(!wizard.steps().contains(&Step::Compression));
        let Command::Receiver(args) = wizard.validated_cli().unwrap().command else {
            panic!("expected receiver")
        };
        assert_eq!(args.transport.listen, Some("0.0.0.0:7000".parse().unwrap()));
        assert_eq!(args.max_sessions, 1);

        wizard.address = Input::new("192.0.2.10:8123");
        wizard.step = Step::Transport;
        wizard.key(key(KeyCode::Right)).unwrap();
        assert!(wizard.connect);
        assert_eq!(wizard.address.value, "127.0.0.1:7000");
        wizard.address = Input::new("192.0.2.20:8123");
        wizard.key(key(KeyCode::Left)).unwrap();
        assert!(!wizard.connect);
        assert_eq!(wizard.address.value, "192.0.2.10:8123");
        wizard.key(key(KeyCode::Right)).unwrap();
        assert_eq!(wizard.address.value, "192.0.2.20:8123");
    }

    #[test]
    fn review_and_form_never_render_the_token() {
        let mut wizard = new_wizard(false, ColorMode::TrueColor);
        wizard.path = Input::new(".");
        wizard.dump_id = Input::new("測試-данные");
        wizard.token = Input::new("do-not-display-this-secret");
        for step in [Step::Token, Step::Review] {
            wizard.step = step;
            for (width, height) in [(120, 32), (90, 26), (60, 20), (25, 8)] {
                let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
                terminal.draw(|frame| wizard.render(frame)).unwrap();
                let screen = terminal
                    .backend()
                    .buffer()
                    .content
                    .iter()
                    .map(|cell| cell.symbol())
                    .collect::<String>();
                assert!(!screen.contains("do-not-display"));
                assert!(screen.contains("fast-dump"));
            }
        }
    }

    #[test]
    fn invalid_field_does_not_advance() {
        let mut wizard = new_wizard(false, ColorMode::Monochrome);
        wizard.step = Step::Address;
        wizard.address = Input::new("bad-address");
        wizard.next();
        assert_eq!(wizard.step, Step::Address);
        assert!(wizard.error.is_some());
    }

    #[test]
    fn token_that_looks_like_an_option_is_kept_as_a_value() {
        let mut wizard = new_wizard(false, ColorMode::Monochrome);
        wizard.path = Input::new(".");
        wizard.dump_id = Input::new("wizard-test");
        wizard.token = Input::new("--secret-token");
        let Command::Sender(args) = wizard.validated_cli().unwrap().command else {
            panic!("expected sender");
        };
        assert_eq!(args.token.as_deref(), Some("--secret-token"));
    }

    #[test]
    fn long_unicode_field_keeps_the_edit_cursor_visible() {
        let input = Input::new(&"界".repeat(100));
        let value = field_window(&input.display(false, true), Some(input.cursor), 24);
        assert!(value.contains('▏'));
        assert!(value.starts_with('…'));
        assert!(Span::raw(value).width() <= 24);
    }

    #[test]
    fn review_and_started_command_preserve_explicit_retry_policy() {
        for retries in [0, 2] {
            let mut wizard = Wizard::new(
                false,
                RetryPolicy::from_options(Some(retries), 7, true),
                Theme::new(ColorMode::Monochrome),
            );
            wizard.path = Input::new(".");
            wizard.dump_id = Input::new("retry-policy");
            wizard.step = Step::Review;
            let expected = if retries == 0 {
                "Disabled (0 retries)"
            } else {
                "2 retries · 7 s delay"
            };
            for (width, height) in [(120, 32), (60, 20)] {
                let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
                terminal.draw(|frame| wizard.render(frame)).unwrap();
                let screen = terminal
                    .backend()
                    .buffer()
                    .content
                    .iter()
                    .map(|cell| cell.symbol())
                    .collect::<String>();
                assert!(
                    screen.contains(expected),
                    "missing retry policy at {width}x{height}"
                );
            }
            let cli = wizard.key(key(KeyCode::Enter)).unwrap().unwrap();
            assert_eq!(cli.retries, Some(retries));
            assert_eq!(cli.retry_delay_secs, 7);
            wizard.sender = false;
            wizard.set_connect(false);
            let mut terminal = Terminal::new(TestBackend::new(120, 32)).unwrap();
            terminal.draw(|frame| wizard.render(frame)).unwrap();
            let screen = terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(|cell| cell.symbol())
                .collect::<String>();
            assert!(screen.contains("Persistent listener; accepts reconnects"));
        }
    }
}

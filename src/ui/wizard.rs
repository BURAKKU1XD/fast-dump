mod input;
mod render;

use input::Input;

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

use crate::cli::{Cli, Command};
use crate::retry::RetryPolicy;
use crate::runtime::RunControl;

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
    use super::input::field_window;
    use super::*;
    use ratatui::text::Span;
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

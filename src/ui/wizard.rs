mod input;
mod lobby;
mod render;

use input::Input;

use std::net::{Ipv6Addr, SocketAddr};
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

use crate::cli::Cli;
use crate::network::direct;
use crate::network::relay::access::{LobbySettings, credentials};
use crate::network::transport::TransportMode;
use crate::retry::RetryPolicy;
use crate::runtime::RunControl;

use super::terminal::TerminalGuard;
use super::theme::Theme;

pub(crate) use lobby::{Settings as ConnectionSettings, run as run_lobby};

pub(crate) enum Launch {
    Cli(Box<Cli>),
    Lobby(lobby::Settings),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    Transport,
    Connection,
    Address,
    RelayPort,
    AccessToken,
    Token,
    RelayCa,
    Review,
}

const DEFAULT_RELAY_PORT: &str = "9876";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Transport {
    Relay,
    Listen,
    Connect,
    ViaRelay,
}

impl Transport {
    const ALL: [Self; 3] = [Self::Relay, Self::Listen, Self::Connect];

    fn label(self) -> &'static str {
        match self {
            Self::Relay => "Relay",
            Self::Listen => "Listen",
            Self::Connect => "Connect",
            Self::ViaRelay => "Via relay",
        }
    }
}

struct Wizard {
    debug: bool,
    retry_policy: RetryPolicy,
    transport_choice: Transport,
    transport: Option<Transport>,
    step: Step,
    addresses: [Input; 4],
    relay_port: Input,
    relay_client_port: Input,
    access_token: Input,
    relay_ca: Input,
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
            transport_choice: Transport::Connect,
            transport: None,
            step: Step::Transport,
            addresses: [
                Input::new("0.0.0.0"),
                Input::new("0.0.0.0:7000"),
                Input::new("127.0.0.1:7000"),
                Input::default(),
            ],
            relay_port: Input::new(DEFAULT_RELAY_PORT),
            relay_client_port: Input::new(DEFAULT_RELAY_PORT),
            access_token: Input::default(),
            relay_ca: Input::default(),
            token: Input::default(),
            error: None,
            help: false,
            theme,
        }
    }

    fn steps(&self) -> Vec<Step> {
        let mut steps = vec![Step::Transport];
        let Some(transport) = self.transport else {
            return steps;
        };
        if transport == Transport::Relay {
            steps.extend([Step::Address, Step::RelayPort, Step::Review]);
            return steps;
        }
        if matches!(transport, Transport::Connect | Transport::ViaRelay) {
            steps.push(Step::Connection);
        }
        steps.push(Step::Address);
        if transport == Transport::ViaRelay {
            steps.extend([Step::RelayPort, Step::AccessToken]);
            if credentials(&self.access_token.value).is_ok_and(|value| !value.has_fingerprint()) {
                steps.push(Step::RelayCa);
            }
            steps.push(Step::Review);
            return steps;
        }
        steps.extend([Step::Token, Step::Review]);
        steps
    }

    fn next(&mut self) {
        match self.validate_step() {
            Ok(()) => {
                if self.step == Step::Transport {
                    self.transport = Some(self.transport_choice);
                }
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
            Step::Address => Some(&mut self.addresses[self.transport? as usize]),
            Step::RelayPort if self.transport == Some(Transport::Relay) => {
                Some(&mut self.relay_port)
            }
            Step::RelayPort => Some(&mut self.relay_client_port),
            Step::AccessToken => Some(&mut self.access_token),
            Step::RelayCa => Some(&mut self.relay_ca),
            Step::Token => Some(&mut self.token),
            _ => None,
        }
    }

    fn validate_step(&self) -> Result<()> {
        match self.step {
            Step::Address => {
                self.validate_address(self.transport.context("Choose a transport")?)?
            }
            Step::RelayPort => {
                anyhow::ensure!(
                    self.relay_port_input(self.transport.context("Choose a transport")?)
                        .value
                        .trim()
                        .parse::<u16>()
                        .is_ok_and(|port| port != 0),
                    "Enter a relay port between 1 and 65535"
                );
            }
            Step::AccessToken => {
                credentials(&self.access_token.value)?;
            }
            Step::RelayCa if !self.relay_ca.value.is_empty() => {
                Self::validate_file(&self.relay_ca, "Relay CA file")?;
            }
            Step::Review => {
                self.launch()?;
            }
            _ => {}
        }
        Ok(())
    }

    fn validate_address(&self, transport: Transport) -> Result<()> {
        let value = &self.addresses[transport as usize].value;
        match transport {
            Transport::Relay => {
                self.relay_endpoint(transport, DEFAULT_RELAY_PORT)
                    .parse::<SocketAddr>()
                    .context("Enter a local IP without a port, such as 0.0.0.0 or ::")?;
            }
            Transport::ViaRelay => {
                crate::network::relay::validate_endpoint(
                    &self.relay_endpoint(transport, DEFAULT_RELAY_PORT),
                )
                .context("Enter the relay hostname or IP without a port")?;
            }
            _ => {
                value.parse::<SocketAddr>().context(
                    "Enter an IP address and port, for example 127.0.0.1:7000 or [::1]:7000",
                )?;
            }
        }
        Ok(())
    }

    fn relay_port_input(&self, transport: Transport) -> &Input {
        if transport == Transport::Relay {
            &self.relay_port
        } else {
            &self.relay_client_port
        }
    }

    fn relay_endpoint(&self, transport: Transport, port: &str) -> String {
        let host = self.addresses[transport as usize].value.trim();
        if host.parse::<Ipv6Addr>().is_ok() {
            format!("[{host}]:{port}")
        } else {
            format!("{host}:{port}")
        }
    }

    fn endpoint(&self, transport: Transport) -> String {
        if matches!(transport, Transport::Relay | Transport::ViaRelay) {
            self.relay_endpoint(transport, self.relay_port_input(transport).value.trim())
        } else {
            self.addresses[transport as usize].value.clone()
        }
    }

    fn validate_file(input: &Input, label: &str) -> Result<()> {
        anyhow::ensure!(
            Path::new(&input.value).is_file(),
            "{label} must be an existing file"
        );
        Ok(())
    }

    fn validate_directory(value: &str, sender: bool) -> Result<()> {
        if value.trim().is_empty() {
            bail!(if sender {
                "Choose a source directory"
            } else {
                "Choose a destination directory"
            });
        }
        let path = Path::new(value);
        if sender {
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

    fn relay_command(&self) -> Result<Cli> {
        let mut args = vec!["fast-dump".to_owned()];
        if self.debug {
            args.push("--debug".into());
        }
        args.extend([
            "relay".into(),
            "--access".into(),
            format!("--listen={}", self.endpoint(Transport::Relay)),
        ]);
        Cli::try_parse_from(args).map_err(Into::into)
    }

    fn launch(&self) -> Result<Launch> {
        let transport = self.transport.context("Choose a transport")?;
        match transport {
            Transport::Relay => {
                return self.relay_command().map(|cli| Launch::Cli(Box::new(cli)));
            }
            Transport::Listen | Transport::Connect => {
                self.validate_address(transport)?;
                let addr = self.addresses[transport as usize].value.parse()?;
                let mode = if transport == Transport::Listen {
                    TransportMode::Listen(addr)
                } else {
                    TransportMode::Connect(addr)
                };
                return Ok(Launch::Lobby(lobby::Settings::Direct(direct::Settings {
                    mode,
                    token: self.token.value.clone(),
                })));
            }
            Transport::ViaRelay => {}
        }
        let endpoint = self.endpoint(Transport::ViaRelay);
        crate::network::relay::validate_endpoint(&endpoint)?;
        let credentials = credentials(&self.access_token.value)?;
        let ca_file = if credentials.has_fingerprint() || self.relay_ca.value.is_empty() {
            None
        } else {
            Self::validate_file(&self.relay_ca, "Relay CA file")?;
            Some(self.relay_ca.value.clone().into())
        };
        Ok(Launch::Lobby(lobby::Settings::Relay(LobbySettings {
            endpoint,
            token: self.access_token.value.clone(),
            ca_file,
        })))
    }

    fn key(&mut self, key: KeyEvent) -> Result<Option<Launch>> {
        if self.help {
            if matches!(key.code, KeyCode::F(1) | KeyCode::Esc | KeyCode::Enter) {
                self.help = false;
            }
            return Ok(None);
        }
        match key.code {
            KeyCode::F(1) => self.help = true,
            KeyCode::BackTab => self.back(),
            KeyCode::Tab if self.step != Step::Transport => self.next(),
            KeyCode::Enter if self.step == Step::Review => return self.launch().map(Some),
            KeyCode::Enter => self.next(),
            KeyCode::Left | KeyCode::Right | KeyCode::Up | KeyCode::Down
                if self.step == Step::Connection =>
            {
                self.transport = Some(if self.transport == Some(Transport::ViaRelay) {
                    Transport::Connect
                } else {
                    Transport::ViaRelay
                });
                self.error = None;
            }
            KeyCode::Left | KeyCode::Right | KeyCode::Up | KeyCode::Down
                if self.step == Step::Transport =>
            {
                let count = Transport::ALL.len();
                let offset = if matches!(key.code, KeyCode::Left | KeyCode::Up) {
                    count - 1
                } else {
                    1
                };
                self.transport_choice =
                    Transport::ALL[(self.transport_choice as usize + offset) % count];
                self.transport = None;
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
) -> Result<Option<Launch>> {
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
    use crate::cli::Command;
    use ratatui::text::Span;
    use ratatui::{Terminal, backend::TestBackend};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn new_wizard(debug: bool, mode: ColorMode) -> Wizard {
        let mut wizard = Wizard::new(
            debug,
            RetryPolicy::from_options(None, 10, true),
            Theme::new(mode),
        );
        wizard.key(key(KeyCode::Enter)).unwrap();
        wizard
    }

    fn start(wizard: &mut Wizard) -> Cli {
        let Launch::Cli(cli) = wizard.key(key(KeyCode::Enter)).unwrap().unwrap() else {
            panic!("expected transfer or relay command");
        };
        *cli
    }

    #[test]
    fn transport_is_confirmed_before_other_fields_enable() {
        let mut wizard = Wizard::new(
            false,
            RetryPolicy::from_options(None, 10, true),
            Theme::new(ColorMode::TrueColor),
        );
        let mut terminal = Terminal::new(TestBackend::new(60, 20)).unwrap();
        terminal.draw(|frame| wizard.render(frame)).unwrap();
        let buffer = terminal.backend().buffer();
        let disabled = buffer
            .content
            .iter()
            .find(|cell| cell.symbol() == "—")
            .unwrap();
        assert_eq!(disabled.fg, wizard.theme.muted);
        assert!(disabled.modifier.contains(ratatui::style::Modifier::DIM));
        assert_eq!(wizard.step, Step::Transport);
        assert_eq!(wizard.steps(), [Step::Transport]);

        wizard.key(key(KeyCode::Right)).unwrap();
        wizard.key(key(KeyCode::Tab)).unwrap();
        assert_eq!(wizard.transport, None);
        assert!(wizard.current_input().is_none());
        wizard.key(key(KeyCode::Enter)).unwrap();
        assert_eq!(wizard.transport, Some(Transport::Relay));
        assert_eq!(wizard.step, Step::Address);
        assert!(wizard.steps().contains(&Step::RelayPort));
        assert_eq!(wizard.relay_port.value, "9876");
        assert!(!wizard.steps().contains(&Step::AccessToken));
        assert!(!wizard.steps().contains(&Step::Token));

        wizard.key(key(KeyCode::BackTab)).unwrap();
        wizard.key(key(KeyCode::Right)).unwrap();
        wizard.key(key(KeyCode::Enter)).unwrap();
        assert_eq!(wizard.transport, Some(Transport::Listen));
        assert_eq!(wizard.current_input().unwrap().value, "0.0.0.0:7000");
        assert!(!wizard.steps().contains(&Step::RelayPort));
        assert!(!wizard.steps().contains(&Step::AccessToken));
    }

    #[test]
    fn relay_server_prefills_bind_address_and_has_no_role() {
        let mut wizard = new_wizard(false, ColorMode::Monochrome);
        wizard.step = Step::Transport;
        wizard.key(key(KeyCode::Right)).unwrap();
        wizard.key(key(KeyCode::Enter)).unwrap();
        assert_eq!(
            wizard.steps(),
            [
                Step::Transport,
                Step::Address,
                Step::RelayPort,
                Step::Review
            ]
        );
        let mut terminal = Terminal::new(TestBackend::new(60, 20)).unwrap();
        terminal.draw(|frame| wizard.render(frame)).unwrap();
        let screen = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(screen.contains("Listen address"));
        assert!(screen.contains("0.0.0.0"));
        assert!(screen.contains("9876"));
        assert!(!screen.contains("TLS certificate"));
        assert!(!screen.contains("TLS private key"));
        assert!(!screen.contains("‹ Send ›"));

        wizard.key(key(KeyCode::Enter)).unwrap();
        wizard.key(key(KeyCode::Enter)).unwrap();
        assert_eq!(wizard.step, Step::Review);
        let Command::Relay(args) = start(&mut wizard).command else {
            panic!("expected relay service");
        };
        assert_eq!(args.listen, "0.0.0.0:9876".parse().unwrap());
        assert!(args.config.is_none());
        assert!(args.certificate.is_none() && args.private_key.is_none());
        assert!(args.access);
    }

    #[test]
    fn relay_client_joins_by_token_before_choosing_a_role() {
        let mut wizard = new_wizard(false, ColorMode::Monochrome);
        assert_eq!(wizard.step, Step::Connection);
        wizard.key(key(KeyCode::Right)).unwrap();
        wizard.key(key(KeyCode::Enter)).unwrap();
        assert_eq!(wizard.step, Step::Address);
        assert_eq!(wizard.transport, Some(Transport::ViaRelay));
        assert!(wizard.current_input().unwrap().value.is_empty());
        wizard.addresses[Transport::ViaRelay as usize] = Input::new("relay.example");
        wizard.access_token = Input::new("23456789ABCD");
        wizard.relay_ca = Input::new("Cargo.toml");
        assert!(!wizard.steps().contains(&Step::Token));
        let mut terminal = Terminal::new(TestBackend::new(60, 20)).unwrap();
        for step in [Step::AccessToken, Step::Review] {
            wizard.step = step;
            terminal.draw(|frame| wizard.render(frame)).unwrap();
            let screen = terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(|cell| cell.symbol())
                .collect::<String>();
            assert!(!screen.contains("23456789ABCD"));
        }
        let Launch::Lobby(lobby::Settings::Relay(settings)) =
            wizard.key(key(KeyCode::Enter)).unwrap().unwrap()
        else {
            panic!("expected role-independent relay connection");
        };
        assert_eq!(settings.endpoint, "relay.example:9876");
        assert_eq!(settings.token, "23456789ABCD");
        assert_eq!(settings.ca_file.as_deref(), Some(Path::new("Cargo.toml")));

        let invitation = format!("FD1:{}:23456789ABCD", "a1".repeat(32));
        wizard.access_token = Input::new(&invitation);
        wizard.step = Step::AccessToken;
        wizard.key(key(KeyCode::Enter)).unwrap();
        assert_eq!(wizard.step, Step::Review);
        let Launch::Lobby(lobby::Settings::Relay(settings)) =
            wizard.key(key(KeyCode::Enter)).unwrap().unwrap()
        else {
            panic!("expected relay invitation");
        };
        assert_eq!(settings.token, invitation);
        assert!(settings.ca_file.is_none());
    }

    #[test]
    fn relay_address_and_port_validate_separately() {
        let mut wizard = new_wizard(false, ColorMode::Monochrome);
        wizard.transport = Some(Transport::ViaRelay);
        wizard.step = Step::Address;
        wizard.addresses[Transport::ViaRelay as usize] = Input::new("relay.example:9876");
        wizard.next();
        assert_eq!(wizard.step, Step::Address);
        assert!(wizard.error.is_some());

        wizard.addresses[Transport::ViaRelay as usize] = Input::new("2001:db8::1");
        wizard.next();
        assert_eq!(wizard.step, Step::RelayPort);
        wizard.relay_client_port = Input::new("0");
        wizard.next();
        assert_eq!(wizard.step, Step::RelayPort);
        assert!(wizard.error.is_some());

        wizard.relay_client_port = Input::new("7443");
        wizard.next();
        assert_eq!(wizard.step, Step::AccessToken);
        assert_eq!(wizard.endpoint(Transport::ViaRelay), "[2001:db8::1]:7443");
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
    fn changing_transport_keeps_each_address_and_direct_connect_has_no_role() {
        let mut wizard = new_wizard(false, ColorMode::Monochrome);
        wizard.step = Step::Transport;
        wizard.key(key(KeyCode::Left)).unwrap();
        wizard.key(key(KeyCode::Enter)).unwrap();
        assert_eq!(wizard.transport, Some(Transport::Listen));
        let Launch::Lobby(lobby::Settings::Direct(settings)) = wizard.launch().unwrap() else {
            panic!("expected role-independent direct listener")
        };
        assert!(
            matches!(settings.mode, TransportMode::Listen(addr) if addr == "0.0.0.0:7000".parse::<SocketAddr>().unwrap())
        );

        wizard.addresses[Transport::Listen as usize] = Input::new("192.0.2.10:8123");
        wizard.step = Step::Transport;
        wizard.key(key(KeyCode::Right)).unwrap();
        wizard.key(key(KeyCode::Enter)).unwrap();
        assert_eq!(wizard.transport, Some(Transport::Connect));
        assert_eq!(wizard.step, Step::Connection);
        wizard.key(key(KeyCode::Enter)).unwrap();
        assert_eq!(wizard.current_input().unwrap().value, "127.0.0.1:7000");
        wizard.addresses[Transport::Connect as usize] = Input::new("192.0.2.20:8123");
        wizard.key(key(KeyCode::BackTab)).unwrap();
        wizard.key(key(KeyCode::BackTab)).unwrap();
        wizard.key(key(KeyCode::Left)).unwrap();
        wizard.key(key(KeyCode::Enter)).unwrap();
        assert_eq!(wizard.current_input().unwrap().value, "192.0.2.10:8123");
        wizard.key(key(KeyCode::BackTab)).unwrap();
        wizard.key(key(KeyCode::Right)).unwrap();
        wizard.key(key(KeyCode::Enter)).unwrap();
        wizard.key(key(KeyCode::Enter)).unwrap();
        assert_eq!(wizard.current_input().unwrap().value, "192.0.2.20:8123");
    }

    #[test]
    fn review_and_form_never_render_the_token() {
        let mut wizard = new_wizard(false, ColorMode::TrueColor);
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
        wizard.addresses[Transport::Connect as usize] = Input::new("bad-address");
        wizard.next();
        assert_eq!(wizard.step, Step::Address);
        assert!(wizard.error.is_some());
    }

    #[test]
    fn token_that_looks_like_an_option_is_kept_as_a_value() {
        let mut wizard = new_wizard(false, ColorMode::Monochrome);
        wizard.token = Input::new("--secret-token");
        let Launch::Lobby(lobby::Settings::Direct(settings)) = wizard.launch().unwrap() else {
            panic!("expected P2P connection settings");
        };
        assert_eq!(settings.token, "--secret-token");
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
    fn review_shows_connection_retry_policy() {
        for retries in [0, 2] {
            let mut wizard = Wizard::new(
                false,
                RetryPolicy::from_options(Some(retries), 7, true),
                Theme::new(ColorMode::Monochrome),
            );
            wizard.key(key(KeyCode::Enter)).unwrap();
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
        }
    }
}

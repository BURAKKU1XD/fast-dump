use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use clap::Parser;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout};
use ratatui::widgets::{List, ListItem, ListState, Paragraph, Wrap};

use crate::cli::{Cli, Command, TransportArgs};
use crate::network::direct;
use crate::network::lobby::{Event as LobbyEvent, Peer, PeerId};
use crate::network::relay::access as relay;
use crate::network::transport::TransportMode;
use crate::progress::ProgressHub;
use crate::retry::RetryPolicy;
use crate::role::Role;
use crate::runtime::RunControl;
use crate::ui::terminal::TerminalGuard;
use crate::ui::widgets::{panel, safe_text};

use super::{Theme, Wizard, input::Input};

#[derive(Clone)]
pub(crate) enum Settings {
    Direct(direct::Settings),
    Relay(relay::LobbySettings),
}

enum Connection {
    Direct(direct::Lobby),
    Relay(relay::Lobby),
}

enum Ticket {
    Direct(direct::Ticket),
    Relay(relay::Ticket),
}

impl Ticket {
    fn peer(&self) -> PeerId {
        match self {
            Self::Direct(ticket) => ticket.peer,
            Self::Relay(ticket) => ticket.peer,
        }
    }

    fn run(self, cli: Cli, peer: &str, control: &RunControl) -> Result<()> {
        match self {
            Self::Direct(ticket) => crate::app::run_lobby_transfer(
                cli,
                ticket.role,
                peer,
                false,
                control,
                |dump_id, session| ticket.establish(dump_id, session),
            ),
            Self::Relay(ticket) => crate::app::run_lobby_transfer(
                cli,
                ticket.role,
                peer,
                true,
                control,
                |dump_id, session| ticket.establish(dump_id, session),
            ),
        }
    }
}

impl Connection {
    fn open(settings: Settings, control: &RunControl) -> Result<Self> {
        match settings {
            Settings::Direct(settings) => {
                direct::Lobby::connect(settings, control).map(Self::Direct)
            }
            Settings::Relay(settings) => relay::Lobby::connect(settings, control).map(Self::Relay),
        }
    }

    fn peers(&self) -> Vec<Peer> {
        match self {
            Self::Direct(lobby) => vec![lobby.peer().clone()],
            Self::Relay(lobby) => lobby.peers(),
        }
    }

    fn transport(&self) -> TransportArgs {
        let mut transport = TransportArgs::default();
        match self {
            Self::Direct(lobby) => match lobby.mode() {
                TransportMode::Listen(addr) => transport.listen = Some(addr),
                TransportMode::Connect(addr) => transport.connect = Some(addr),
            },
            Self::Relay(lobby) => transport.relay = Some(lobby.endpoint().to_owned()),
        }
        transport
    }

    fn offer(&self, peer: PeerId, source: &str) -> Result<()> {
        match self {
            Self::Direct(lobby) => lobby.offer(source),
            Self::Relay(lobby) => lobby.offer(peer, source),
        }
    }

    fn accept(&self, peer: PeerId) -> Result<()> {
        match self {
            Self::Direct(lobby) => lobby.accept(),
            Self::Relay(lobby) => lobby.accept(peer),
        }
    }

    fn decline(&self, peer: PeerId) -> Result<()> {
        match self {
            Self::Direct(lobby) => lobby.decline(),
            Self::Relay(lobby) => lobby.decline(peer),
        }
    }

    fn next(&mut self) -> Result<Option<LobbyEvent<Ticket>>> {
        match self {
            Self::Direct(lobby) => Ok(lobby.next()?.map(|event| event.map_ticket(Ticket::Direct))),
            Self::Relay(lobby) => Ok(lobby.next()?.map(|event| event.map_ticket(Ticket::Relay))),
        }
    }
}

enum Mode {
    Idle,
    Incoming { peer: u32, source: String },
    Path(Draft),
    Waiting { peer: u32, cli: Box<Cli> },
    Working(u32),
}

impl Mode {
    fn peer(&self) -> Option<u32> {
        match self {
            Self::Idle => None,
            Self::Incoming { peer, .. } | Self::Working(peer) | Self::Waiting { peer, .. } => {
                Some(*peer)
            }
            Self::Path(draft) => Some(draft.peer),
        }
    }
}

struct Screen {
    mode: Mode,
    selected: usize,
    notice: String,
    theme: Theme,
}

#[derive(Clone, Copy)]
enum Field {
    Path,
    DumpId,
    Compression,
}

struct Draft {
    peer: PeerId,
    role: Role,
    field: Field,
    path: Input,
    dump_id: Input,
    level: Input,
}

impl Draft {
    fn new(peer: PeerId, role: Role) -> Self {
        Self {
            peer,
            role,
            field: Field::Path,
            path: Input::default(),
            dump_id: Input::default(),
            level: Input::new(&crate::cli::DEFAULT_LEVEL.to_string()),
        }
    }

    fn input(&mut self) -> &mut Input {
        match self.field {
            Field::Path => &mut self.path,
            Field::DumpId => &mut self.dump_id,
            Field::Compression => &mut self.level,
        }
    }

    fn label(&self) -> (&'static str, &Input) {
        match self.field {
            Field::Path if self.role == Role::Sender => ("Source directory", &self.path),
            Field::Path => ("Destination directory", &self.path),
            Field::DumpId => ("Dump ID (optional)", &self.dump_id),
            Field::Compression => ("Zstd level", &self.level),
        }
    }
}

pub(crate) fn run(
    settings: Settings,
    retry: RetryPolicy,
    control: &RunControl,
    hub: Arc<ProgressHub>,
) -> Result<()> {
    let mut connection = None;
    crate::retry::run(retry, control, || {
        connection = Some(Connection::open(settings.clone(), control)?);
        Ok(())
    })?;
    let mut lobby = connection.expect("successful connection supplies a lobby");
    let mut terminal = TerminalGuard::enter()?;
    let mut screen = Screen {
        mode: Mode::Idle,
        selected: 0,
        notice: String::new(),
        theme: Theme::detect(),
    };
    loop {
        if control.is_cancelled() {
            return Ok(());
        }
        while let Some(event) = lobby.next()? {
            if let Some((ticket, cli)) = screen.update(&lobby, event)? {
                let peer = peer_label(ticket.peer(), &lobby.peers());
                let direct = matches!(&ticket, Ticket::Direct(_));
                drop(terminal);
                let result = crate::ui::run_with(
                    cli,
                    control.clone(),
                    hub.clone(),
                    true,
                    move |cli, control| ticket.run(cli, &peer, control),
                );
                if control.is_cancelled() {
                    return result;
                }
                terminal = TerminalGuard::enter()?;
                if direct {
                    screen.mode = Mode::Idle;
                }
                screen.notice = match result {
                    Ok(()) => "Transfer complete".into(),
                    Err(error) => format!("{error:#}"),
                };
            }
        }
        let peers = lobby.peers();
        screen.selected = screen.selected.min(peers.len().saturating_sub(1));
        terminal
            .terminal
            .draw(|frame| screen.render(frame, &lobby, &peers))?;
        if !event::poll(Duration::from_millis(100))? {
            continue;
        }
        match event::read()? {
            Event::Key(key) if key.kind != KeyEventKind::Release => {
                if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
                    control.cancel();
                    return Ok(());
                }
                match screen.key(&lobby, &peers, key) {
                    Ok(true) => return Ok(()),
                    Ok(false) => {}
                    Err(error) => screen.notice = error.to_string(),
                }
            }
            Event::Paste(text) => {
                if let Mode::Path(draft) = &mut screen.mode {
                    draft.input().insert(&text);
                }
            }
            _ => {}
        }
    }
}

impl Screen {
    fn update(
        &mut self,
        lobby: &Connection,
        event: LobbyEvent<Ticket>,
    ) -> Result<Option<(Ticket, Cli)>> {
        match event {
            LobbyEvent::Incoming { peer, source } => {
                if matches!(self.mode, Mode::Idle) {
                    self.mode = Mode::Incoming { peer, source };
                    self.notice.clear();
                } else {
                    lobby.decline(peer)?;
                }
            }
            LobbyEvent::Start(ticket) => {
                let Mode::Waiting { peer, cli } = std::mem::replace(&mut self.mode, Mode::Idle)
                else {
                    anyhow::bail!("unexpected relay transfer");
                };
                ensure!(
                    peer == ticket.peer(),
                    "transfer selected an unexpected peer"
                );
                self.mode = Mode::Working(peer);
                return Ok(Some((ticket, *cli)));
            }
            LobbyEvent::Declined(peer) | LobbyEvent::Busy(peer) => {
                if self.mode.peer() == Some(peer) {
                    self.mode = Mode::Idle;
                    self.notice = format!(
                        "{} is unavailable or declined the transfer",
                        peer_label(peer, &lobby.peers())
                    );
                }
            }
            LobbyEvent::Failed(peer) => {
                if self.mode.peer() == Some(peer) {
                    self.mode = Mode::Idle;
                    if self.notice.is_empty() {
                        self.notice = format!(
                            "Transfer with {} ended early",
                            peer_label(peer, &lobby.peers())
                        );
                    }
                }
            }
            LobbyEvent::Released(peer) => {
                if self.mode.peer() == Some(peer) {
                    self.mode = Mode::Idle;
                }
            }
        }
        Ok(None)
    }

    fn key(&mut self, lobby: &Connection, peers: &[Peer], key: KeyEvent) -> Result<bool> {
        match (&mut self.mode, key.code) {
            (Mode::Idle, KeyCode::Esc) => return Ok(true),
            (Mode::Idle, KeyCode::Up) => self.selected = self.selected.saturating_sub(1),
            (Mode::Idle, KeyCode::Down) => {
                self.selected = (self.selected + 1).min(peers.len().saturating_sub(1))
            }
            (Mode::Idle, KeyCode::Enter) => {
                if let Some(peer) = peers.get(self.selected) {
                    self.mode = Mode::Path(Draft::new(peer.id, Role::Sender));
                    self.notice.clear();
                }
            }
            (Mode::Incoming { peer, .. }, KeyCode::Enter) => {
                self.mode = Mode::Path(Draft::new(*peer, Role::Receiver));
            }
            (Mode::Incoming { peer, .. } | Mode::Waiting { peer, .. }, KeyCode::Esc) => {
                lobby.decline(*peer)?;
            }
            (Mode::Path(draft), KeyCode::Esc) => {
                if draft.role == Role::Receiver {
                    lobby.decline(draft.peer)?;
                }
                self.mode = Mode::Idle;
                self.notice.clear();
            }
            (Mode::Path(draft), KeyCode::F(2)) if draft.role == Role::Sender => {
                draft.field = match draft.field {
                    Field::Path => Field::DumpId,
                    Field::DumpId => Field::Compression,
                    Field::Compression => Field::Path,
                };
            }
            (Mode::Path(draft), KeyCode::Enter) => {
                let cli = transfer_command(lobby.transport(), draft)?;
                if let Command::Sender(args) = &cli.command {
                    let source = args
                        .source
                        .file_name()
                        .and_then(|name| name.to_str())
                        .map(str::to_owned)
                        .unwrap_or(args.resolve_dump_id()?);
                    lobby.offer(draft.peer, &source)?;
                } else {
                    lobby.accept(draft.peer)?;
                }
                self.mode = Mode::Waiting {
                    peer: draft.peer,
                    cli: Box::new(cli),
                };
                self.notice.clear();
            }
            (Mode::Path(draft), _) => draft.input().key(key),
            _ => {}
        }
        Ok(false)
    }

    fn render(&self, frame: &mut ratatui::Frame<'_>, lobby: &Connection, peers: &[Peer]) {
        let [header, list, footer] = Layout::vertical([
            Constraint::Length(2),
            Constraint::Min(1),
            Constraint::Length(5),
        ])
        .areas(frame.area());
        let connection = match lobby {
            Connection::Direct(peer) => {
                format!("Connected to {} · P2P", peer_label(peer.peer().id, peers))
            }
            Connection::Relay(_) if peers.is_empty() => "Waiting for peers · Relay".into(),
            Connection::Relay(_) => format!("Connected · Relay · {} peers available", peers.len()),
        };
        frame.render_widget(
            Paragraph::new(connection).style(self.theme.accent()),
            header,
        );
        let items = peers
            .iter()
            .map(|peer| ListItem::new(peer_label(peer.id, peers)));
        frame.render_stateful_widget(
            List::new(items)
                .block(panel(" Connected peers ", self.theme, true))
                .style(self.theme.panel())
                .highlight_symbol("› ")
                .highlight_style(self.theme.selected()),
            list,
            &mut ListState::default().with_selected(if peers.is_empty() {
                None
            } else {
                Some(self.selected)
            }),
        );
        let message = match &self.mode {
            Mode::Idle if peers.is_empty() => "Waiting for other clients · Esc disconnect".into(),
            Mode::Idle => "↑↓ choose peer · Enter send files · Esc disconnect".into(),
            Mode::Incoming { peer, source } => {
                let source = if source.is_empty() {
                    "a folder"
                } else {
                    source
                };
                format!(
                    "{} wants to send {source}\nEnter accept · Esc decline",
                    peer_label(*peer, peers)
                )
            }
            Mode::Path(draft) => {
                let (label, input) = draft.label();
                let controls = if draft.role == Role::Sender {
                    "Enter submit offer · F2 next setting · Esc cancel"
                } else {
                    "Enter choose destination and accept · Esc decline"
                };
                format!("{label}  {}\n{controls}", input.display(false, true))
            }
            Mode::Waiting { peer, .. } => format!(
                "Waiting for {} · Esc cancel offer",
                peer_label(*peer, peers)
            ),
            Mode::Working(peer) => format!("Finishing transfer with {}", peer_label(*peer, peers)),
        };
        frame.render_widget(
            Paragraph::new(format!("{}\n{}", safe_text(&self.notice), message))
                .style(self.theme.panel())
                .wrap(Wrap { trim: false }),
            footer,
        );
    }
}

fn peer_label(id: PeerId, peers: &[Peer]) -> String {
    let Some(peer) = peers
        .iter()
        .find(|peer| peer.id == id && !peer.name.is_empty())
    else {
        return format!("Peer #{id}");
    };
    let name = safe_text(&peer.name);
    if peers
        .iter()
        .any(|other| other.id != id && other.name.eq_ignore_ascii_case(&peer.name))
    {
        format!("{name} #{id}")
    } else {
        name
    }
}

fn transfer_command(transport: TransportArgs, draft: &Draft) -> Result<Cli> {
    let sender = draft.role == Role::Sender;
    let path = &draft.path.value;
    Wizard::validate_directory(path, sender)?;
    let mut cli_args = vec![
        "fast-dump".to_owned(),
        if sender {
            "sender".to_owned()
        } else {
            "receiver".to_owned()
        },
        "--connect=127.0.0.1:1".to_owned(),
        format!("{}={path}", if sender { "--source" } else { "--data-dir" }),
    ];
    if sender {
        cli_args.push(format!("--level={}", draft.level.value));
        if !draft.dump_id.value.is_empty() {
            cli_args.push(format!("--dump-id={}", draft.dump_id.value));
        }
    }
    let mut cli = Cli::try_parse_from(cli_args).context("invalid transfer setup")?;
    match &mut cli.command {
        Command::Sender(args) => {
            let range = zstd::compression_level_range();
            ensure!(
                range.contains(&args.level),
                "Zstd level must be between {} and {}",
                range.start(),
                range.end()
            );
            args.resolve_dump_id()?;
            args.runtime_options()?;
            args.transport = transport;
        }
        Command::Receiver(args) => args.transport = transport,
        Command::Relay(_) => unreachable!(),
    }
    Ok(cli)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hostname_labels_use_connection_ids_for_collisions_and_missing_names() {
        let peers = [
            Peer {
                id: 3,
                name: "laptop".into(),
            },
            Peer {
                id: 7,
                name: "LAPTOP".into(),
            },
            Peer {
                id: 9,
                name: "desktop".into(),
            },
        ];
        assert_eq!(peer_label(3, &peers), "laptop #3");
        assert_eq!(peer_label(7, &peers), "LAPTOP #7");
        assert_eq!(peer_label(9, &peers), "desktop");
        assert_eq!(peer_label(10, &peers), "Peer #10");
    }

    #[test]
    fn sender_options_apply_after_connecting_and_validate_before_submission() {
        let source = crate::tests::TestDir::new();
        let mut draft = Draft::new(1, Role::Sender);
        draft.path = Input::new(source.path().to_str().unwrap());
        draft.dump_id = Input::new("photos");
        for level in [-1, 0, 7] {
            draft.level = Input::new(&level.to_string());
            let cli = transfer_command(TransportArgs::default(), &draft).unwrap();
            let Command::Sender(args) = cli.command else {
                panic!("expected sender")
            };
            assert_eq!(args.runtime_options().unwrap().level, level);
            assert_eq!(args.resolve_dump_id().unwrap(), "photos");
        }
        let range = zstd::compression_level_range();
        for level in [
            "".into(),
            "fast".into(),
            "1.5".into(),
            (range.end() + 1).to_string(),
        ] {
            draft.level = Input::new(&level);
            assert!(transfer_command(TransportArgs::default(), &draft).is_err());
        }
        draft.level = Input::new("3");
        draft.dump_id = Input::new("../photos");
        assert!(transfer_command(TransportArgs::default(), &draft).is_err());
    }
}

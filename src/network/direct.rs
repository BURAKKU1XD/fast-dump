use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use ring::rand::{SecureRandom, SystemRandom};
use subtle::ConstantTimeEq;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use super::lobby::{ACCEPT, BODY_LEN, DECLINE, Event, Frame, OFFER, Peer, hostname};
use super::transport::{
    BoundListener, Session, TransportMode, configure_stream, connect_stream_controlled,
    establish_session_controlled, establish_session_on_listener,
};
use crate::role::Role;
use crate::runtime::{RunControl, SessionControl, SocketRegistration};

const MAGIC: &[u8; 4] = b"FDP1";
const LOCAL_ACCEPT: u8 = 26;
const MAX_TOKEN: usize = 4096;
const CONTROL_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(crate) struct Settings {
    pub(crate) mode: TransportMode,
    pub(crate) token: String,
}

pub(crate) struct Lobby {
    peer: Peer,
    link: Arc<Link>,
    actions: mpsc::Sender<Frame>,
    events: mpsc::Receiver<Frame>,
    worker: JoinHandle<Result<()>>,
    runtime: Option<tokio::runtime::Runtime>,
}

struct Link {
    mode: TransportMode,
    listener: Option<BoundListener>,
    peer_ip: std::net::IpAddr,
}

pub(crate) struct Ticket {
    link: Arc<Link>,
    token: String,
    pub(crate) role: Role,
    pub(crate) peer: u32,
}

impl Lobby {
    pub(crate) fn connect(settings: Settings, run: &RunControl) -> Result<Self> {
        let listener = match settings.mode {
            TransportMode::Listen(addr) => Some(BoundListener::bind(addr)?),
            TransportMode::Connect(_) => None,
        };
        Self::open(settings, run, listener)
    }

    fn open(settings: Settings, run: &RunControl, listener: Option<BoundListener>) -> Result<Self> {
        ensure!(settings.token.len() <= MAX_TOKEN, "P2P token is too long");
        let control = run.unobserved_session();
        let (stream, socket, name, listener) = match settings.mode {
            TransportMode::Listen(_) => {
                let listener = listener.context("P2P listen mode requires a bound listener")?;
                eprintln!("Waiting for a P2P peer on {}", listener.local_addr());
                listener.set_nonblocking(true)?;
                let (stream, registration, name) =
                    accept_peer(&listener, settings.token.as_bytes(), &control)?;
                (stream, registration, name, Some(listener))
            }
            TransportMode::Connect(addr) => {
                let (mut stream, registration) = connect_stream_controlled(addr, &control)?;
                configure_stream(&stream)?;
                let name = connect_peer(&mut stream, settings.token.as_bytes())?;
                (stream, registration, name, None)
            }
        };
        let peer = Peer { id: 1, name };
        let link = Arc::new(Link {
            mode: settings.mode,
            listener,
            peer_ip: stream.peer_addr()?.ip(),
        });
        stream.set_nonblocking(true)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let (actions, actions_rx) = mpsc::channel(8);
        let (events_tx, events) = mpsc::channel(16);
        let _runtime_guard = runtime.enter();
        let stream = tokio::net::TcpStream::from_std(stream)?;
        let worker = runtime.spawn(async move {
            let _session = control;
            let _socket = socket;
            messages(stream, actions_rx, events_tx).await
        });
        drop(_runtime_guard);
        Ok(Self {
            peer,
            link,
            actions,
            events,
            worker,
            runtime: Some(runtime),
        })
    }

    pub(crate) fn peer(&self) -> &Peer {
        &self.peer
    }

    pub(crate) fn mode(&self) -> TransportMode {
        self.link.mode
    }

    pub(crate) fn offer(&self, source: &str) -> Result<()> {
        self.send(Frame::with_text(OFFER, 1, source))
    }

    pub(crate) fn accept(&self) -> Result<()> {
        let mut frame = Frame::new(ACCEPT, 1);
        SystemRandom::new()
            .fill(&mut frame.body[..16])
            .map_err(|_| anyhow::anyhow!("generate direct transfer credential"))?;
        self.send(frame)
    }

    pub(crate) fn decline(&self) -> Result<()> {
        self.send(Frame::new(DECLINE, 1))
    }

    fn send(&self, frame: Frame) -> Result<()> {
        self.actions
            .try_send(frame)
            .map_err(|_| anyhow::anyhow!("P2P connection cannot accept another action"))
    }

    pub(crate) fn next(&mut self) -> Result<Option<Event<Ticket>>> {
        use mpsc::error::TryRecvError;
        let frame = match self.events.try_recv() {
            Ok(frame) => frame,
            Err(TryRecvError::Empty) => return Ok(None),
            Err(TryRecvError::Disconnected) => anyhow::bail!("P2P peer disconnected"),
        };
        Ok(Some(match frame.kind {
            OFFER => Event::Incoming {
                peer: 1,
                source: frame.text(),
            },
            ACCEPT | LOCAL_ACCEPT => Event::Start(Ticket {
                link: Arc::clone(&self.link),
                token: frame.body[..16]
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect(),
                role: if frame.kind == ACCEPT {
                    Role::Sender
                } else {
                    Role::Receiver
                },
                peer: 1,
            }),
            DECLINE => Event::Declined(1),
            _ => anyhow::bail!("unexpected P2P transfer message"),
        }))
    }
}

impl Drop for Lobby {
    fn drop(&mut self) {
        self.worker.abort();
        self.runtime.take().unwrap().shutdown_background();
    }
}

impl Ticket {
    pub(crate) fn establish(
        &self,
        dump_id: Option<&str>,
        control: &SessionControl,
    ) -> Result<(Session, Vec<SocketRegistration>)> {
        if let Some(listener) = &self.link.listener {
            establish_session_on_listener(
                listener,
                self.link.peer_ip,
                self.role,
                dump_id,
                Some(&self.token),
                control,
            )
        } else {
            establish_session_controlled(
                self.role,
                self.link.mode,
                dump_id,
                Some(&self.token),
                control,
            )
        }
    }
}

fn accept_peer(
    listener: &BoundListener,
    token: &[u8],
    control: &SessionControl,
) -> Result<(TcpStream, SocketRegistration, String)> {
    loop {
        control.check()?;
        let (mut stream, _) = match listener.try_accept_stream() {
            Ok(connection) => connection,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
                continue;
            }
            Err(error) => return Err(error).context("accept P2P peer"),
        };
        let registration = control.register(&stream)?;
        stream.set_nonblocking(false)?;
        configure_stream(&stream)?;
        if let Ok(name) = verify_peer(&mut stream, token) {
            return Ok((stream, registration, name));
        }
    }
}

fn connect_peer(stream: &mut TcpStream, token: &[u8]) -> Result<String> {
    stream.write_all(MAGIC)?;
    stream.write_all(&(token.len() as u16).to_be_bytes())?;
    stream.write_all(token)?;
    stream.write_all(&Frame::with_text(0, 0, &hostname()).body)?;
    let mut header = [0; 4];
    stream
        .read_exact(&mut header)
        .context("P2P peer rejected the connection or uses a different protocol")?;
    ensure!(&header == MAGIC, "unexpected P2P handshake");
    read_name(stream)
}

fn verify_peer(stream: &mut TcpStream, token: &[u8]) -> Result<String> {
    let mut header = [0; 6];
    stream.read_exact(&mut header)?;
    ensure!(&header[..4] == MAGIC, "unexpected P2P handshake");
    let length = usize::from(u16::from_be_bytes([header[4], header[5]]));
    ensure!(length <= MAX_TOKEN, "P2P token is too long");
    let mut received = vec![0; length];
    stream.read_exact(&mut received)?;
    ensure!(bool::from(received.ct_eq(token)), "P2P token mismatch");
    let name = read_name(stream)?;
    stream.write_all(MAGIC)?;
    stream.write_all(&Frame::with_text(0, 0, &hostname()).body)?;
    Ok(name)
}

fn read_name(stream: &mut TcpStream) -> Result<String> {
    let mut body = [0; BODY_LEN];
    stream.read_exact(&mut body)?;
    Ok(Frame {
        kind: 0,
        id: 0,
        body,
    }
    .text())
}

async fn messages(
    stream: tokio::net::TcpStream,
    mut actions: mpsc::Receiver<Frame>,
    events: mpsc::Sender<Frame>,
) -> Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    let incoming = async {
        loop {
            let frame = Frame::read(&mut reader).await?;
            ensure!(
                matches!(frame.kind, OFFER | ACCEPT | DECLINE),
                "unexpected P2P message"
            );
            events.send(frame).await.context("P2P screen closed")?;
        }
    };
    let outgoing = async {
        while let Some(frame) = actions.recv().await {
            timeout(CONTROL_TIMEOUT, frame.write(&mut writer)).await??;
            if frame.kind == ACCEPT {
                events
                    .send(Frame {
                        kind: LOCAL_ACCEPT,
                        ..frame
                    })
                    .await
                    .context("P2P screen closed")?;
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    tokio::select! { result = incoming => result, result = outgoing => result }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receiver;
    use crate::sender::{self, BundlePlanOptions};
    use crate::tests::{TestDir, read_archive_entries};

    fn next(lobby: &mut Lobby) -> Event<Ticket> {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(event) = lobby.next().unwrap() {
                return event;
            }
            assert!(std::time::Instant::now() < deadline, "P2P event timed out");
            thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn direct_connection_exchanges_hostnames_and_transfers_in_both_roles() {
        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();
        let program = RunControl::new(None);
        let watchdog_run = program.clone();
        let (done, completion) = std::sync::mpsc::channel::<()>();
        let watchdog = thread::spawn(move || {
            if completion.recv_timeout(Duration::from_secs(20))
                == Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            {
                watchdog_run.cancel();
            }
        });
        let server_program = program.clone();
        let server = thread::spawn(move || {
            Lobby::open(
                Settings {
                    mode: TransportMode::Listen(addr),
                    token: "local-test".into(),
                },
                &server_program,
                Some(listener),
            )
        });
        let mut connector = Lobby::connect(
            Settings {
                mode: TransportMode::Connect(addr),
                token: "local-test".into(),
            },
            &program,
        )
        .unwrap();
        let mut listener = server.join().unwrap().unwrap();
        let expected = Frame::with_text(0, 0, &hostname()).text();
        assert_eq!(connector.peer().name, expected);
        assert_eq!(listener.peer().name, expected);

        for reverse in [false, true] {
            let (from, to) = if reverse {
                (&mut connector, &mut listener)
            } else {
                (&mut listener, &mut connector)
            };
            from.offer("photos").unwrap();
            assert!(matches!(next(to), Event::Incoming { peer: 1, source } if source == "photos"));
            if reverse {
                // Отказ сохраняет подключение и не открывает каналы передачи.
                to.decline().unwrap();
                assert!(matches!(next(from), Event::Declined(1)));
                from.offer("photos").unwrap();
                assert!(matches!(next(to), Event::Incoming { .. }));
            }
            to.accept().unwrap();
            let Event::Start(sender) = next(from) else {
                panic!("expected sender ticket")
            };
            let Event::Start(receiver) = next(to) else {
                panic!("expected receiver ticket")
            };
            let source = TestDir::new();
            let target = TestDir::new();
            std::fs::write(source.path().join("file"), b"P2P payload").unwrap();
            let target_path = target.path().to_owned();
            let receiver_control = program.unobserved_session();
            let received = thread::spawn(move || {
                let (session, _sockets) = receiver.establish(None, &receiver_control)?;
                receiver::receive_dump(
                    session.control_stream,
                    session.data_stream,
                    target_path,
                    "photos",
                )
            });
            let control = program.unobserved_session();
            let (mut session, _sockets) = sender.establish(Some("photos"), &control).unwrap();
            sender::send_dump(
                &mut session.control_stream,
                &mut session.data_stream,
                source.path().to_owned(),
                "photos",
                BundlePlanOptions::default(),
            )
            .unwrap();
            let report = received.join().unwrap().unwrap();
            assert_eq!(
                read_archive_entries(&report.final_archive_path)[0].bytes,
                b"P2P payload"
            );
        }
        drop(done);
        watchdog.join().unwrap();
    }
}

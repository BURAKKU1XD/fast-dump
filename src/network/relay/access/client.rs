use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use tokio::io::{AsyncWriteExt, split};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_rustls::{TlsConnector, rustls::pki_types::ServerName};

use super::super::client::{
    ClientRuntime, RelayStream, Stream, connector, establish_channels, open_stream,
    within_handshake,
};
use super::*;
use crate::network::lobby::{Event, Peer, hostname};
use crate::network::{network_io, transport::Session};
use crate::role::Role;
use crate::runtime::{SessionControl, SocketRegistration};

#[derive(Clone)]
pub(crate) struct LobbySettings {
    pub(crate) endpoint: String,
    pub(crate) token: String,
    pub(crate) ca_file: Option<PathBuf>,
}

pub(crate) struct Lobby {
    pub(crate) id: PeerId,
    link: Arc<Link>,
    peers: watch::Receiver<Vec<Peer>>,
    actions: mpsc::Sender<Frame>,
    events: mpsc::Receiver<Frame>,
    worker: JoinHandle<Result<()>>,
}

struct Link {
    endpoint: String,
    address: SocketAddr,
    tls: TlsConnector,
    name: ServerName<'static>,
    runtime: Arc<ClientRuntime>,
}

pub(crate) struct Ticket {
    link: Arc<Link>,
    key: AttachKey,
    pub(crate) role: Role,
    pub(crate) peer: PeerId,
}

pub(crate) type LobbyEvent = Event<Ticket>;

impl Lobby {
    pub(crate) fn connect(settings: LobbySettings, run: &RunControl) -> Result<Self> {
        let credentials = credentials(&settings.token)?;
        let name = super::super::server_name(&settings.endpoint)?;
        let tls = if let Some(fingerprint) = credentials.fingerprint {
            ensure!(
                settings.ca_file.is_none(),
                "relay invitation already supplies the TLS identity"
            );
            super::super::identity::connector(fingerprint)
        } else {
            connector(settings.ca_file.as_deref())?
        };
        let runtime = ClientRuntime::create()?;
        let control = run.unobserved_session();
        let (stream, socket, id) = runtime.handle().block_on(async {
            within_handshake(&control, async {
                let (mut stream, socket) =
                    open_stream(&settings.endpoint, None, name.clone(), &tls, &control).await?;
                register(&mut stream, JOIN, credentials.token.as_bytes()).await?;
                let reply = Frame::read(&mut stream).await.map_err(network_io)?;
                match reply.kind {
                    JOINED if reply.id != 0 => {
                        if reply.body[0] & JOIN_METADATA != 0 {
                            Frame::with_text(PEER_NAME, 0, &hostname())
                                .write(&mut stream)
                                .await
                                .map_err(network_io)?;
                        }
                        Ok((stream, socket, reply.id))
                    }
                    DENIED => anyhow::bail!("relay rejected the access token"),
                    FULL => anyhow::bail!("Relay full"),
                    _ => anyhow::bail!("unexpected relay registration reply"),
                }
            })
            .await
        })?;
        let address = stream.get_ref().0.peer_addr()?;
        let (peers_tx, peers) = watch::channel(Vec::new());
        let (actions, actions_rx) = mpsc::channel(8);
        let (events_tx, events) = mpsc::channel(16);
        let worker = runtime.handle().spawn(async move {
            let _session = control;
            let _socket = socket;
            run_link(stream, peers_tx, actions_rx, events_tx).await
        });
        Ok(Self {
            id,
            link: Arc::new(Link {
                endpoint: settings.endpoint,
                address,
                tls,
                name,
                runtime,
            }),
            peers,
            actions,
            events,
            worker,
        })
    }

    pub(crate) fn peers(&self) -> Vec<Peer> {
        self.peers
            .borrow()
            .iter()
            .filter(|peer| peer.id != self.id)
            .cloned()
            .collect()
    }

    pub(crate) fn endpoint(&self) -> &str {
        &self.link.endpoint
    }

    pub(crate) fn offer(&self, peer: PeerId, source: &str) -> Result<()> {
        self.send(Frame::with_text(OFFER, peer, source))
    }

    pub(crate) fn accept(&self, peer: PeerId) -> Result<()> {
        self.send(Frame::new(ACCEPT, peer))
    }

    pub(crate) fn decline(&self, peer: PeerId) -> Result<()> {
        self.send(Frame::new(DECLINE, peer))
    }

    fn send(&self, frame: Frame) -> Result<()> {
        self.actions
            .try_send(frame)
            .map_err(|_| anyhow::anyhow!("relay connection cannot accept another action"))
    }

    pub(crate) fn next(&mut self) -> Result<Option<LobbyEvent>> {
        use mpsc::error::TryRecvError;
        let frame = match self.events.try_recv() {
            Ok(frame) => frame,
            Err(TryRecvError::Empty) => return Ok(None),
            Err(TryRecvError::Disconnected) => anyhow::bail!("relay disconnected"),
        };
        Ok(Some(match frame.kind {
            INCOMING => LobbyEvent::Incoming {
                peer: frame.id,
                source: frame.text(),
            },
            START_SEND | START_RECEIVE => LobbyEvent::Start(Ticket {
                link: Arc::clone(&self.link),
                key: frame.body[..TICKET_LEN].try_into().unwrap(),
                role: if frame.kind == START_SEND {
                    Role::Sender
                } else {
                    Role::Receiver
                },
                peer: frame.id,
            }),
            DECLINED => LobbyEvent::Declined(frame.id),
            BUSY => LobbyEvent::Busy(frame.id),
            RELEASED => LobbyEvent::Released(frame.id),
            FAILED => LobbyEvent::Failed(frame.id),
            _ => anyhow::bail!("unexpected relay lobby message"),
        }))
    }
}

impl Drop for Lobby {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

async fn register(stream: &mut Stream, kind: u8, credential: &[u8]) -> Result<()> {
    let mut request = [0; 5 + TICKET_LEN];
    request[..4].copy_from_slice(MAGIC);
    request[4] = kind;
    request[5..5 + credential.len()].copy_from_slice(credential);
    if kind == JOIN {
        request[5 + TOKEN_LEN] = JOIN_METADATA;
    }
    stream.write_all(&request).await.map_err(network_io)?;
    stream.flush().await.map_err(network_io)
}

async fn run_link(
    stream: Stream,
    peers: watch::Sender<Vec<Peer>>,
    mut actions: mpsc::Receiver<Frame>,
    events: mpsc::Sender<Frame>,
) -> Result<()> {
    let (mut read, mut write) = split(stream);
    let receive = async {
        loop {
            let frame = Frame::read(&mut read).await?;
            if frame.kind == PEERS {
                let count = frame.id as usize;
                ensure!(count <= MAX_CLIENTS, "invalid relay peer list");
                peers.send_modify(|list| {
                    *list = frame
                        .body
                        .chunks_exact(4)
                        .take(count)
                        .map(|bytes| {
                            let id = u32::from_be_bytes(bytes.try_into().unwrap());
                            list.iter()
                                .find(|peer| peer.id == id)
                                .cloned()
                                .unwrap_or_else(|| Peer {
                                    id,
                                    name: String::new(),
                                })
                        })
                        .collect();
                });
            } else if frame.kind == PEER_NAME {
                peers.send_if_modified(|list| {
                    if let Some(peer) = list.iter_mut().find(|peer| peer.id == frame.id) {
                        peer.name = frame.text();
                        true
                    } else {
                        false
                    }
                });
            } else {
                events.send(frame).await.context("lobby closed")?;
            }
        }
    };
    let send = async {
        while let Some(frame) = actions.recv().await {
            timeout(super::super::HANDSHAKE, frame.write(&mut write)).await??;
        }
        Ok::<_, anyhow::Error>(())
    };
    tokio::select! { result = receive => result, result = send => result }
}

impl Ticket {
    pub(crate) fn establish(
        &self,
        dump_id: Option<&str>,
        control: &SessionControl,
    ) -> Result<(Session<RelayStream>, [SocketRegistration; 2])> {
        let link = &self.link;
        let (ctrl, data) = link.runtime.handle().block_on(async {
            tokio::try_join!(
                self.attach(ATTACH_CONTROL, control),
                self.attach(ATTACH_DATA, control)
            )
        })?;
        establish_channels(
            [ctrl.0, data.0],
            [ctrl.1, data.1],
            Arc::clone(&link.runtime),
            self.role,
            dump_id,
            None,
            control,
        )
    }

    async fn attach(
        &self,
        kind: u8,
        control: &SessionControl,
    ) -> Result<(Stream, SocketRegistration)> {
        let link = &self.link;
        let (mut stream, socket) = within_handshake(control, async {
            let (mut stream, socket) = open_stream(
                &link.endpoint,
                Some(link.address),
                link.name.clone(),
                &link.tls,
                control,
            )
            .await?;
            register(&mut stream, kind, &self.key).await?;
            Ok((stream, socket))
        })
        .await?;
        let ready = timeout(
            super::super::ATTACH_DATA + super::super::HANDSHAKE,
            Frame::read(&mut stream),
        )
        .await
        .map_err(|_| super::super::timed_out("waiting for relay data attachment"))?
        .map_err(network_io)?;
        ensure!(ready.kind == ATTACHED, "unexpected relay attachment reply");
        Ok((stream, socket))
    }
}

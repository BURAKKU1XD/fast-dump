mod attachment;
mod bootstrap;
mod client;

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use ring::rand::{SecureRandom, SystemRandom};
use subtle::ConstantTimeEq;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout, timeout_at};
use tokio_rustls::{TlsAcceptor, server::TlsStream};

use super::RelayStatus;
use super::identity::{PinnedKey, PublicKey};
use crate::network::lobby::{
    ACCEPT, BUSY, DECLINE, DECLINED, FAILED, Frame, INCOMING, OFFER, PEER_NAME, PeerId, RELEASED,
};
use crate::runtime::RunControl;

#[cfg(test)]
pub(crate) use client::LobbyEvent;
pub(crate) use client::{Lobby, LobbySettings, Ticket};

const MAGIC: &[u8; 4] = b"FDA1";
const JOIN: u8 = 0;
const ATTACH_CONTROL: u8 = 1;
const ATTACH_DATA: u8 = 2;
const ALPHABET: &[u8; 32] = b"23456789ABCDEFGHJKLMNPQRSTUVWXYZ";
const TOKEN_LEN: usize = 12;
const TICKET_LEN: usize = 16;
const MAX_CLIENTS: usize = 15;
const MAX_TASKS: usize = MAX_CLIENTS * 2 + 64;
const REJECT_DELAY: Duration = Duration::from_secs(1);
const JOIN_METADATA: u8 = 1;
const JOIN_PEER_TLS: u8 = 2;
const JOIN_RELAY_KEY: u8 = 4;

type Stream = TlsStream<TcpStream>;
type Token = [u8; TOKEN_LEN];
type AttachKey = [u8; TICKET_LEN];
type Shared = Arc<Mutex<LobbyState>>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum TransferMode {
    Legacy,
    VerifiedCode,
    RelayKey,
}

impl TransferMode {
    fn from_features(features: u8) -> Self {
        if features & JOIN_PEER_TLS == 0 {
            Self::Legacy
        } else if features & JOIN_RELAY_KEY != 0 {
            Self::RelayKey
        } else {
            Self::VerifiedCode
        }
    }

    fn features(self) -> u8 {
        match self {
            Self::Legacy => 0,
            Self::VerifiedCode => JOIN_PEER_TLS,
            Self::RelayKey => JOIN_PEER_TLS | JOIN_RELAY_KEY,
        }
    }
}

const JOINED: u8 = 10;
const DENIED: u8 = 11;
const FULL: u8 = 12;
const ATTACHED: u8 = 13;
const PEERS: u8 = 14;
const START_SEND: u8 = 16;
const START_RECEIVE: u8 = 17;

pub(crate) const JOIN_RELAY_ARG: &str = "join-relay";
pub(crate) const ACCESS_TOKEN_ARG: &str = "access-token";
pub(crate) const INVITATION_ARG: &str = "invitation";

pub(crate) struct Credentials<'a> {
    token: &'a str,
    identity: Option<PinnedKey>,
}

impl Credentials<'_> {
    pub(crate) fn has_identity(&self) -> bool {
        self.identity.is_some()
    }
}

pub(crate) fn credentials(text: &str) -> Result<Credentials<'_>> {
    let text = text.trim();
    let (identity, token) = if let Some((version, invitation)) = text.split_once(':') {
        let (key, token) = invitation
            .split_once(':')
            .context("Invalid relay invitation")?;
        let identity = match version {
            "FD2" => PinnedKey::PublicKey(parse_invitation_key(key)?),
            "FD1" => PinnedKey::Fingerprint(parse_invitation_key(key)?),
            _ => anyhow::bail!("Unsupported relay invitation version"),
        };
        (Some(identity), token)
    } else {
        (None, text)
    };
    validate_access_token(token)?;
    Ok(Credentials { token, identity })
}

fn parse_invitation_key<const N: usize>(hex: &str) -> Result<[u8; N]> {
    ensure!(
        hex.len() == N * 2 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "Invalid relay invitation key"
    );
    let mut key = [0; N];
    for (byte, pair) in key.iter_mut().zip(hex.as_bytes().as_chunks::<2>().0) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(key)
}

pub(crate) fn validate_access_token(token: &str) -> Result<()> {
    ensure!(
        token.len() == TOKEN_LEN && token.bytes().all(|byte| ALPHABET.contains(&byte)),
        "Enter a 12-character relay access token"
    );
    Ok(())
}

fn random<const N: usize>() -> Result<[u8; N]> {
    let mut bytes = [0; N];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| anyhow::anyhow!("generate relay credentials"))?;
    Ok(bytes)
}

fn generate_token() -> Result<Token> {
    Ok(random::<TOKEN_LEN>()?.map(|byte| ALPHABET[usize::from(byte & 31)]))
}

pub(super) async fn listen(
    listener: TcpListener,
    tls: TlsAcceptor,
    control: &RunControl,
    temporary: bool,
    ready: impl FnOnce(RelayStatus),
) -> Result<()> {
    let token = generate_token()?;
    let code = std::str::from_utf8(&token)?;
    let listen = listener.local_addr()?;
    let clients = if temporary {
        Some(bootstrap::client_commands(listen, code).await)
    } else {
        None
    };
    control.check()?;
    ready(RelayStatus {
        listen,
        token: Some(code.to_owned()),
        clients,
    });
    listen_with_token(listener, tls, control, token).await
}

pub(super) async fn listen_with_token(
    listener: TcpListener,
    tls: TlsAcceptor,
    control: &RunControl,
    token: Token,
) -> Result<()> {
    let state = Arc::new(Mutex::new(LobbyState::default()));
    let mut tasks = JoinSet::new();
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    let result = loop {
        tokio::select! {
            _ = tick.tick() => {
                if let Err(error) = control.check() {
                    break Err(error);
                }
                state.lock().unwrap().expire(Instant::now());
            }
            result = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = result { break Err(error.into()); }
            }
            result = listener.accept(), if tasks.len() < MAX_TASKS => {
                let (socket, _) = match result {
                    Ok(connection) => connection,
                    Err(error) => break Err(error).context("accept relay connection"),
                };
                let state = Arc::clone(&state);
                let tls = tls.clone();
                tasks.spawn(async move {
                    if let Err(error) = register(socket, tls, token, state).await {
                        log::debug!("relay access connection ended: {error:#}");
                    }
                });
            }
        }
    };
    tasks.shutdown().await;
    result
}

async fn register(
    mut socket: TcpStream,
    tls: TlsAcceptor,
    token: Token,
    state: Shared,
) -> Result<()> {
    let deadline = Instant::now() + super::HANDSHAKE;
    socket.set_nodelay(true)?;
    let mut first = [0; 1];
    if timeout_at(deadline, socket.peek(&mut first)).await?? == 0 {
        return Ok(());
    }
    if first[0] == MAGIC[0] {
        let (kind, proof) = timeout_at(deadline, attachment::receive(&mut socket)).await??;
        return attach(
            &state,
            kind,
            Attachment::Peer(proof),
            TransferSocket::Tcp(socket),
        )
        .await;
    }
    let (mut stream, request) = timeout_at(deadline, async {
        let mut stream = tls.accept(socket).await?;
        let mut request = [0; 5 + TICKET_LEN];
        stream.read_exact(&mut request).await?;
        Ok::<_, io::Error>((stream, request))
    })
    .await??;
    ensure!(&request[..4] == MAGIC, "invalid relay access registration");
    match request[4] {
        JOIN => {
            if !bool::from(request[5..5 + TOKEN_LEN].ct_eq(&token)) {
                tokio::time::sleep(REJECT_DELAY).await;
                timeout(super::HANDSHAKE, Frame::new(DENIED, 0).write(&mut stream)).await??;
                return Ok(());
            }
            let (outgoing, incoming) = mpsc::channel(24);
            let (cancel, cancelled) = oneshot::channel();
            let id = state.lock().unwrap().join(Mailbox {
                outgoing,
                cancel: Mutex::new(Some(cancel)),
                metadata: request[5 + TOKEN_LEN] & JOIN_METADATA != 0,
                mode: TransferMode::from_features(request[5 + TOKEN_LEN]),
                name: None,
            });
            let Some(id) = id else {
                timeout(super::HANDSHAKE, Frame::new(FULL, 0).write(&mut stream)).await??;
                return Ok(());
            };
            let member = MemberGuard { state, id };
            tokio::select! {
                result = member.serve(stream, incoming) => result?,
                _ = cancelled => {}
            }
        }
        kind @ (ATTACH_CONTROL | ATTACH_DATA) => {
            attach(
                &state,
                kind,
                Attachment::Legacy(request[5..].try_into().unwrap()),
                TransferSocket::RelayTls(Box::new(stream)),
            )
            .await?;
        }
        _ => anyhow::bail!("unknown relay access registration"),
    }
    Ok(())
}

enum Attachment {
    Legacy(AttachKey),
    Peer(attachment::Proof),
}

enum TransferSocket {
    RelayTls(Box<Stream>),
    Tcp(TcpStream),
}

async fn attach(
    state: &Shared,
    kind: u8,
    credential: Attachment,
    socket: TransferSocket,
) -> Result<()> {
    let transfer = state.lock().unwrap().attach(credential, kind, socket)?;
    if let Some(transfer) = transfer {
        transfer.forward(state).await?;
    }
    Ok(())
}

struct MemberGuard {
    state: Shared,
    id: PeerId,
}

impl MemberGuard {
    async fn serve(&self, stream: Stream, mut incoming: mpsc::Receiver<Frame>) -> Result<()> {
        let (mut reader, mut writer) = tokio::io::split(stream);
        let read = async {
            loop {
                let command = Frame::read(&mut reader).await?;
                self.state.lock().unwrap().command(self.id, command)?;
            }
        };
        let write = async {
            while let Some(frame) = incoming.recv().await {
                timeout(super::HANDSHAKE, frame.write(&mut writer)).await??;
            }
            Ok::<_, anyhow::Error>(())
        };
        // Любая ошибка завершает всё подключение, включая незаконченный frame.
        tokio::select! { result = read => result, result = write => result }
    }
}

impl Drop for MemberGuard {
    fn drop(&mut self) {
        self.state.lock().unwrap().leave(self.id);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Pair {
    sender: PeerId,
    receiver: PeerId,
}

impl Pair {
    fn contains(self, id: PeerId) -> bool {
        self.sender == id || self.receiver == id
    }
}

struct Transfer {
    pair: Pair,
    keys: [AttachKey; 2],
    streams: [Option<TransferSocket>; 4],
    deadline: Instant,
    running: bool,
    peer_tls: bool,
    cancel: Option<oneshot::Sender<()>>,
}

struct Started {
    pair: Pair,
    streams: [TransferSocket; 4],
    cancel: oneshot::Receiver<()>,
}

struct Mailbox {
    outgoing: mpsc::Sender<Frame>,
    cancel: Mutex<Option<oneshot::Sender<()>>>,
    metadata: bool,
    mode: TransferMode,
    name: Option<String>,
}

impl Mailbox {
    fn send(&self, frame: Frame) -> Result<()> {
        if self.outgoing.try_send(frame).is_err() {
            // Освобождаем место и связанные предложения, если клиент перестал читать.
            drop(self.cancel.lock().unwrap().take());
            anyhow::bail!("relay client is not reading messages");
        }
        Ok(())
    }
}

impl Started {
    async fn forward(self, state: &Shared) -> Result<()> {
        let mut attached = false;
        use TransferSocket::{RelayTls, Tcp};
        let transfer = async {
            match self.streams {
                [RelayTls(sc), RelayTls(sd), RelayTls(rc), RelayTls(rd)] => {
                    forward_attached(*sc, *sd, *rc, *rd, &mut attached).await
                }
                [Tcp(sc), Tcp(sd), Tcp(rc), Tcp(rd)] => {
                    forward_attached(sc, sd, rc, rd, &mut attached).await
                }
                _ => unreachable!("a transfer has one attachment type"),
            }
        };
        let result = tokio::select! {
            result = transfer => result,
            _ = self.cancel => Err(anyhow::anyhow!("relay peer disconnected")),
        };
        // Релей знает о готовности каналов, а итог v4 определяют участники передачи.
        state.lock().unwrap().finish(self.pair, attached);
        result
    }
}

async fn forward_attached<S: AsyncRead + AsyncWrite + Unpin>(
    mut sc: S,
    mut sd: S,
    mut rc: S,
    mut rd: S,
    attached: &mut bool,
) -> Result<()> {
    timeout(super::HANDSHAKE, async {
        let ready = Frame::new(ATTACHED, 0);
        tokio::try_join!(
            ready.write(&mut sc),
            ready.write(&mut sd),
            ready.write(&mut rc),
            ready.write(&mut rd)
        )
    })
    .await??;
    *attached = true;
    super::server::forward(sc, rc, sd, rd).await
}

#[derive(Default)]
struct LobbyState {
    next_id: PeerId,
    members: BTreeMap<PeerId, Mailbox>,
    offers: Vec<Pair>,
    transfers: Vec<Transfer>,
}

impl LobbyState {
    fn join(&mut self, mailbox: Mailbox) -> Option<PeerId> {
        if self.members.len() >= MAX_CLIENTS {
            return None;
        }
        let id = self.next_id.checked_add(1)?;
        self.next_id = id;
        let mut reply = Frame::new(JOINED, id);
        if mailbox.metadata {
            reply.body[0] = JOIN_METADATA;
        }
        reply.body[0] |= mailbox.mode.features();
        mailbox.send(reply).ok()?;
        self.members.insert(id, mailbox);
        self.broadcast();
        let mailbox = &self.members[&id];
        if mailbox.metadata {
            for (peer, member) in &self.members {
                if let Some(name) = &member.name {
                    let _ = mailbox.send(Frame::with_text(PEER_NAME, *peer, name));
                }
            }
        }
        Some(id)
    }

    fn broadcast(&self) {
        let mut frame = Frame::new(PEERS, self.members.len() as PeerId);
        let slots = frame.body.as_chunks_mut::<4>().0;
        for (slot, id) in slots.iter_mut().zip(self.members.keys()) {
            *slot = id.to_be_bytes();
        }
        for member in self.members.values() {
            let _ = member.send(frame);
        }
    }

    fn notify(&self, to: PeerId, frame: Frame) -> Result<()> {
        self.members
            .get(&to)
            .context("relay peer disconnected")?
            .send(frame)
    }

    fn busy(&self, id: PeerId) -> bool {
        self.offers.iter().any(|pair| pair.contains(id))
            || self
                .transfers
                .iter()
                .any(|transfer| transfer.pair.contains(id))
    }

    fn command(&mut self, from: PeerId, frame: Frame) -> Result<()> {
        let to = frame.id;
        match frame.kind {
            PEER_NAME => {
                let member = self
                    .members
                    .get_mut(&from)
                    .context("relay peer disconnected")?;
                ensure!(
                    member.metadata && member.name.is_none(),
                    "unexpected relay hostname"
                );
                let name = frame.text();
                let update = Frame::with_text(PEER_NAME, from, &name);
                member.name = Some(name);
                for peer in self.members.values().filter(|peer| peer.metadata) {
                    let _ = peer.send(update);
                }
            }
            OFFER => {
                if self
                    .members
                    .get(&to)
                    .is_some_and(|recipient| self.members[&from].mode != recipient.mode)
                {
                    return self.notify(from, Frame::new(FAILED, to));
                }
                let incoming = if self.members[&from].mode != TransferMode::Legacy {
                    Frame::new(INCOMING, from)
                } else {
                    Frame::with_text(INCOMING, from, &frame.text())
                };
                if from == to
                    || self.busy(from)
                    || self.busy(to)
                    || self.notify(to, incoming).is_err()
                {
                    return self.notify(from, Frame::new(BUSY, to));
                }
                self.offers.push(Pair {
                    sender: from,
                    receiver: to,
                });
            }
            ACCEPT | DECLINE => {
                let Some(index) = self.offers.iter().position(|pair| {
                    pair.receiver == from && pair.sender == to
                        || frame.kind == DECLINE && pair.sender == from && pair.receiver == to
                }) else {
                    return self.notify(from, Frame::new(DECLINED, to));
                };
                let pair = self.offers.swap_remove(index);
                if frame.kind == DECLINE {
                    let _ = self.notify(to, Frame::new(DECLINED, from));
                    let _ = self.notify(from, Frame::new(DECLINED, to));
                } else {
                    self.accept(pair, &frame.body)?;
                }
            }
            _ => anyhow::bail!("unknown relay lobby command"),
        }
        Ok(())
    }

    fn accept(&mut self, pair: Pair, body: &[u8; crate::network::lobby::BODY_LEN]) -> Result<()> {
        let mode = self.members[&pair.receiver].mode;
        let keys = [random()?, random()?];
        let mut sender = Frame::new(START_SEND, pair.receiver);
        sender.body[..TICKET_LEN].copy_from_slice(&keys[0]);
        if mode == TransferMode::RelayKey {
            let receiver_key: &PublicKey = body[..size_of::<PublicKey>()].try_into().unwrap();
            sender.body[TICKET_LEN..].copy_from_slice(receiver_key);
        }
        let mut receiver = Frame::new(START_RECEIVE, pair.sender);
        receiver.body[..TICKET_LEN].copy_from_slice(&keys[1]);
        if self.notify(pair.sender, sender).is_err()
            || self.notify(pair.receiver, receiver).is_err()
        {
            let _ = self.notify(pair.sender, Frame::new(FAILED, pair.receiver));
            let _ = self.notify(pair.receiver, Frame::new(FAILED, pair.sender));
            return Ok(());
        }
        self.transfers.push(Transfer {
            pair,
            keys,
            streams: [None, None, None, None],
            deadline: Instant::now() + super::ATTACH_DATA,
            running: false,
            peer_tls: mode != TransferMode::Legacy,
            cancel: None,
        });
        Ok(())
    }

    fn attach(
        &mut self,
        credential: Attachment,
        kind: u8,
        stream: TransferSocket,
    ) -> Result<Option<Started>> {
        let (transfer, side) = self
            .transfers
            .iter_mut()
            .find_map(|transfer| {
                if transfer.running
                    || transfer.deadline <= Instant::now()
                    || transfer.peer_tls != matches!(credential, Attachment::Peer(_))
                {
                    return None;
                }
                transfer
                    .keys
                    .iter()
                    .position(|expected| match &credential {
                        Attachment::Legacy(key) => bool::from(expected.ct_eq(key)),
                        Attachment::Peer(proof) => proof.matches(expected),
                    })
                    .map(|side| (transfer, side))
            })
            .context("relay attachment expired")?;
        let slot = side * 2 + usize::from(kind == ATTACH_DATA);
        ensure!(
            transfer.streams[slot].is_none(),
            "duplicate relay attachment"
        );
        transfer.streams[slot] = Some(stream);
        if transfer.streams.iter().any(Option::is_none) {
            return Ok(None);
        }
        let streams = transfer
            .streams
            .each_mut()
            .map(|stream| stream.take().unwrap());
        let (cancel, cancelled) = oneshot::channel();
        transfer.running = true;
        transfer.cancel = Some(cancel);
        Ok(Some(Started {
            pair: transfer.pair,
            streams,
            cancel: cancelled,
        }))
    }

    fn finish(&mut self, pair: Pair, attached: bool) {
        let Some(index) = self
            .transfers
            .iter()
            .position(|transfer| transfer.pair == pair)
        else {
            return;
        };
        self.transfers.swap_remove(index);
        let code = if attached { RELEASED } else { FAILED };
        let _ = self.notify(pair.sender, Frame::new(code, pair.receiver));
        let _ = self.notify(pair.receiver, Frame::new(code, pair.sender));
    }

    fn expire(&mut self, now: Instant) {
        while let Some(transfer) = self
            .transfers
            .iter()
            .find(|transfer| !transfer.running && transfer.deadline <= now)
        {
            self.finish(transfer.pair, false);
        }
    }

    fn leave(&mut self, id: PeerId) {
        self.members.remove(&id);
        if let Some(index) = self.offers.iter().position(|pair| pair.contains(id)) {
            let pair = self.offers.swap_remove(index);
            let _ = self.notify(pair.sender, Frame::new(DECLINED, pair.receiver));
            let _ = self.notify(pair.receiver, Frame::new(DECLINED, pair.sender));
        }
        if let Some(transfer) = self
            .transfers
            .iter()
            .find(|transfer| transfer.pair.contains(id))
        {
            self.finish(transfer.pair, false);
        }
        self.broadcast();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relay_does_not_mix_different_transfer_modes() {
        let mut state = LobbyState::default();
        let mut join = |mode| {
            let (outgoing, incoming) = mpsc::channel(24);
            let id = state
                .join(Mailbox {
                    outgoing,
                    cancel: Mutex::new(None),
                    metadata: false,
                    mode,
                    name: None,
                })
                .unwrap();
            (id, incoming)
        };
        let (modern, mut modern_events) = join(TransferMode::RelayKey);
        let (verified, _verified_events) = join(TransferMode::VerifiedCode);
        let (legacy, _legacy_events) = join(TransferMode::Legacy);
        let (other_legacy, mut other_events) = join(TransferMode::Legacy);
        while modern_events.try_recv().is_ok() {}
        while other_events.try_recv().is_ok() {}

        for incompatible in [legacy, verified] {
            state
                .command(modern, Frame::new(OFFER, incompatible))
                .unwrap();
            assert_eq!(modern_events.try_recv().unwrap().kind, FAILED);
        }
        assert!(state.offers.is_empty());

        state
            .command(legacy, Frame::with_text(OFFER, other_legacy, "folder"))
            .unwrap();
        let incoming = other_events.try_recv().unwrap();
        assert_eq!((incoming.kind, incoming.id), (INCOMING, legacy));
        assert_eq!(incoming.text(), "folder");
        state
            .command(other_legacy, Frame::new(ACCEPT, legacy))
            .unwrap();
        assert_eq!(state.transfers.len(), 1);
        assert!(!state.transfers[0].peer_tls);
    }
}

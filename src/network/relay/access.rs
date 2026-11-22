mod bootstrap;
mod client;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use ring::rand::{SecureRandom, SystemRandom};
use subtle::ConstantTimeEq;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout};
use tokio_rustls::{TlsAcceptor, server::TlsStream};

use super::identity::Fingerprint;
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

type Stream = TlsStream<TcpStream>;
type Token = [u8; TOKEN_LEN];
type AttachKey = [u8; TICKET_LEN];
type Shared = Arc<Mutex<LobbyState>>;

const JOINED: u8 = 10;
const DENIED: u8 = 11;
const FULL: u8 = 12;
const ATTACHED: u8 = 13;
const PEERS: u8 = 14;
const START_SEND: u8 = 16;
const START_RECEIVE: u8 = 17;

const INVITATION_PREFIX: &str = "FD1:";
pub(crate) const JOIN_RELAY_ARG: &str = "join-relay";
pub(crate) const INVITATION_ARG: &str = "invitation";

pub(crate) struct Credentials<'a> {
    token: &'a str,
    fingerprint: Option<Fingerprint>,
}

impl Credentials<'_> {
    pub(crate) fn has_fingerprint(&self) -> bool {
        self.fingerprint.is_some()
    }
}

pub(crate) fn credentials(text: &str) -> Result<Credentials<'_>> {
    let text = text.trim();
    let (fingerprint, token) = if let Some(invitation) = text.strip_prefix(INVITATION_PREFIX) {
        let (pin, token) = invitation
            .split_once(':')
            .context("Invalid relay invitation")?;
        ensure!(
            pin.len() == 64 && pin.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "Invalid relay invitation fingerprint"
        );
        let mut fingerprint = [0; 32];
        for (byte, pair) in fingerprint.iter_mut().zip(pin.as_bytes().chunks_exact(2)) {
            *byte = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
        }
        (Some(fingerprint), token)
    } else {
        (None, text)
    };
    ensure!(
        token.len() == TOKEN_LEN && token.bytes().all(|byte| ALPHABET.contains(&byte)),
        "Paste a relay invitation or enter a 12-character access token"
    );
    Ok(Credentials { token, fingerprint })
}

pub(super) fn invitation(fingerprint: Fingerprint, token: &str) -> String {
    let mut text = String::from(INVITATION_PREFIX);
    for byte in fingerprint {
        write!(text, "{byte:02x}").unwrap();
    }
    text.push(':');
    text.push_str(token);
    text
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
    fingerprint: Option<Fingerprint>,
) -> Result<()> {
    let token = generate_token()?;
    let code = std::str::from_utf8(&token)?;
    if let Some(fingerprint) = fingerprint {
        let invitation = invitation(fingerprint, code);
        eprintln!("Relay invitation: {invitation}");
        eprintln!(
            "{}",
            bootstrap::instructions(listener.local_addr()?, &invitation)
        );
    } else {
        eprintln!("Access token: {code}");
    }
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

async fn register(socket: TcpStream, tls: TlsAcceptor, token: Token, state: Shared) -> Result<()> {
    let (mut stream, request) = timeout(super::HANDSHAKE, async {
        socket.set_nodelay(true)?;
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
            let key: AttachKey = request[5..].try_into().unwrap();
            let transfer = state.lock().unwrap().attach(key, kind, stream)?;
            if let Some(transfer) = transfer {
                transfer.forward(&state).await?;
            }
        }
        _ => anyhow::bail!("unknown relay access registration"),
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
    streams: [Option<Stream>; 4],
    deadline: Instant,
    running: bool,
    cancel: Option<oneshot::Sender<()>>,
}

struct Started {
    pair: Pair,
    streams: [Stream; 4],
    cancel: oneshot::Receiver<()>,
}

struct Mailbox {
    outgoing: mpsc::Sender<Frame>,
    cancel: Mutex<Option<oneshot::Sender<()>>>,
    metadata: bool,
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
        let [mut sc, mut sd, mut rc, mut rd] = self.streams;
        let mut attached = false;
        let transfer = async {
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
            attached = true;
            super::server::forward(sc, rc, sd, rd).await
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
        for (slot, id) in frame.body.chunks_exact_mut(4).zip(self.members.keys()) {
            slot.copy_from_slice(&id.to_be_bytes());
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
                if from == to
                    || self.busy(from)
                    || self.busy(to)
                    || self
                        .notify(to, Frame::with_text(INCOMING, from, &frame.text()))
                        .is_err()
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
                    self.accept(pair)?;
                }
            }
            _ => anyhow::bail!("unknown relay lobby command"),
        }
        Ok(())
    }

    fn accept(&mut self, pair: Pair) -> Result<()> {
        let keys = [random()?, random()?];
        let mut sender = Frame::new(START_SEND, pair.receiver);
        sender.body[..TICKET_LEN].copy_from_slice(&keys[0]);
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
            cancel: None,
        });
        Ok(())
    }

    fn attach(&mut self, key: AttachKey, kind: u8, stream: Stream) -> Result<Option<Started>> {
        let (transfer, side) = self
            .transfers
            .iter_mut()
            .find_map(|transfer| {
                if transfer.running || transfer.deadline <= Instant::now() {
                    return None;
                }
                transfer
                    .keys
                    .iter()
                    .position(|expected| bool::from(expected.ct_eq(&key)))
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

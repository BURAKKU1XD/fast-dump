use std::collections::{HashMap, hash_map::RandomState};
use std::hash::BuildHasher;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::network::protocol::wire::{self, ChannelKind};
use crate::role::Role;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const PENDING_CHANNEL_TIMEOUT: Duration = Duration::from_secs(5);
static NEXT_SESSION_NONCE: AtomicU64 = AtomicU64::new(1);
static SESSION_NONCE_HASHER: LazyLock<RandomState> = LazyLock::new(RandomState::new);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportMode {
    Listen(SocketAddr),
    Connect(SocketAddr),
}

pub trait CloneableStream: Read + Write + Send + 'static + Sized {
    fn try_clone_stream(&self) -> Result<Self>;

    // Должен прервать блокирующий I/O на всех клонах перед завершением reader-потоков.
    fn shutdown_stream(&self) -> Result<()>;
}

impl CloneableStream for TcpStream {
    fn try_clone_stream(&self) -> Result<Self> {
        self.try_clone()
            .context("failed to clone v4 transport stream")
    }

    fn shutdown_stream(&self) -> Result<()> {
        let result = self.shutdown(std::net::Shutdown::Both);
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawSocket;
            // Winsock recv может ждать асинхронный запрос даже у blocking socket.
            // shutdown запрещает новые операции, CancelIoEx снимает уже начатую.
            unsafe {
                windows_sys::Win32::System::IO::CancelIoEx(
                    self.as_raw_socket() as _,
                    std::ptr::null(),
                );
            }
        }
        result.context("failed to shut down v4 transport stream")
    }
}

#[derive(Debug)]
pub struct Session {
    pub control_stream: TcpStream,
    pub data_stream: TcpStream,
    pub local_role: Role,
    pub peer_role: Role,
    pub peer_dump_id: Option<String>,
    pub local_addr: SocketAddr,
    pub peer_addr: SocketAddr,
}

#[derive(Debug)]
pub(crate) struct ChannelSession {
    pub(crate) stream: TcpStream,
    pub(crate) local_role: Role,
    pub(crate) peer_role: Role,
    pub(crate) peer_dump_id: Option<String>,
    pub(crate) local_addr: SocketAddr,
    pub(crate) peer_addr: SocketAddr,
    pub(crate) channel_kind: ChannelKind,
    pub(crate) session_nonce: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PendingSessionKey {
    pub(crate) session_nonce: u64,
    pub(crate) peer_ip: IpAddr,
    pub(crate) peer_role: u8,
    pub(crate) peer_dump_id: Option<String>,
}

pub(crate) struct PendingChannels {
    created_at: Instant,
    control: Option<ChannelSession>,
    data: Option<ChannelSession>,
}

pub fn establish_session(
    role: Role,
    mode: TransportMode,
    dump_id: Option<&str>,
    token: Option<&str>,
) -> Result<Session> {
    match mode {
        TransportMode::Listen(addr) => listen_once(role, addr, dump_id, token),
        TransportMode::Connect(addr) => connect_once(role, addr, dump_id, token),
    }
}

pub fn listen_once(
    role: Role,
    addr: SocketAddr,
    dump_id: Option<&str>,
    token: Option<&str>,
) -> Result<Session> {
    BoundListener::bind(addr)?.accept(role, dump_id, token)
}

pub fn connect_once(
    role: Role,
    addr: SocketAddr,
    dump_id: Option<&str>,
    token: Option<&str>,
) -> Result<Session> {
    match role {
        Role::Sender => {
            let session_nonce = next_session_nonce();
            let control = connect_channel(
                role,
                addr,
                dump_id,
                token,
                Some((ChannelKind::Control, session_nonce)),
            )?;
            let data = connect_channel(
                role,
                addr,
                dump_id,
                token,
                Some((ChannelKind::Data, session_nonce)),
            )?;
            assemble_session(control, data)
        }
        Role::Receiver => {
            let first = connect_channel(role, addr, dump_id, token, None)?;
            let second = connect_channel(role, addr, dump_id, token, None)?;
            match (first.channel_kind, second.channel_kind) {
                (ChannelKind::Control, ChannelKind::Data) => assemble_session(first, second),
                (ChannelKind::Data, ChannelKind::Control) => assemble_session(second, first),
                _ => bail!("v4 peer opened duplicate channel kinds while establishing session"),
            }
        }
    }
}

fn connect_channel(
    role: Role,
    addr: SocketAddr,
    dump_id: Option<&str>,
    token: Option<&str>,
    local_channel: Option<(ChannelKind, u64)>,
) -> Result<ChannelSession> {
    let stream =
        TcpStream::connect(addr).with_context(|| format!("failed to connect to {addr}"))?;
    establish_stream_session(stream, role, dump_id, token, None, local_channel)
}

pub struct BoundListener {
    listener: TcpListener,
    local_addr: SocketAddr,
}

impl BoundListener {
    pub fn bind(addr: SocketAddr) -> Result<Self> {
        let listener =
            TcpListener::bind(addr).with_context(|| format!("failed to bind to {addr}"))?;
        let local_addr = listener
            .local_addr()
            .context("failed to query bound v4 listener address")?;
        Ok(Self {
            listener,
            local_addr,
        })
    }

    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn accept(
        &self,
        role: Role,
        dump_id: Option<&str>,
        token: Option<&str>,
    ) -> Result<Session> {
        match role {
            Role::Sender => {
                let session_nonce = next_session_nonce();
                let control = self.accept_channel(
                    role,
                    dump_id,
                    token,
                    Some((ChannelKind::Control, session_nonce)),
                )?;
                let data = self.accept_channel(
                    role,
                    dump_id,
                    token,
                    Some((ChannelKind::Data, session_nonce)),
                )?;
                assemble_session(control, data)
            }
            Role::Receiver => {
                let mut pending = HashMap::new();
                loop {
                    let channel = self.accept_channel(role, dump_id, token, None)?;
                    if let Some(session) = insert_pending_channel(&mut pending, channel)? {
                        return Ok(session);
                    }
                    drop_expired_pending_channels(&mut pending);
                }
            }
        }
    }

    pub(crate) fn accept_stream(&self) -> Result<(TcpStream, SocketAddr)> {
        self.listener
            .accept()
            .context("failed to accept incoming v4 connection")
    }

    pub(crate) fn try_accept_stream(&self) -> std::io::Result<(TcpStream, SocketAddr)> {
        self.listener.accept()
    }

    pub(crate) fn accept_channel(
        &self,
        role: Role,
        dump_id: Option<&str>,
        token: Option<&str>,
        local_channel: Option<(ChannelKind, u64)>,
    ) -> Result<ChannelSession> {
        let (stream, peer_addr) = self.accept_stream()?;
        establish_stream_session(stream, role, dump_id, token, Some(peer_addr), local_channel)
    }

    pub(crate) fn set_nonblocking(&self, nonblocking: bool) -> Result<()> {
        self.listener
            .set_nonblocking(nonblocking)
            .context("failed to configure v4 listener nonblocking mode")
    }
}

pub(crate) fn configure_stream(stream: &TcpStream) -> Result<()> {
    stream
        .set_nodelay(true)
        .context("failed to enable TCP_NODELAY for v4 session")?;
    stream
        .set_read_timeout(Some(HANDSHAKE_TIMEOUT))
        .context("failed to configure v4 read timeout")?;
    stream
        .set_write_timeout(Some(HANDSHAKE_TIMEOUT))
        .context("failed to configure v4 write timeout")?;
    Ok(())
}

fn clear_handshake_timeout(stream: &TcpStream) -> Result<()> {
    stream
        .set_read_timeout(None)
        .context("failed to clear v4 read timeout after handshake")?;
    stream
        .set_write_timeout(None)
        .context("failed to clear v4 write timeout after handshake")?;
    Ok(())
}

fn perform_handshake(
    stream: &mut TcpStream,
    role: Role,
    dump_id: Option<&str>,
    token: Option<&str>,
    local_channel: Option<(ChannelKind, u64)>,
) -> Result<HandshakeResult> {
    match role {
        Role::Sender => {
            let (channel_kind, session_nonce) = local_channel
                .context("v4 sender transport handshake requires a local channel identity")?;
            Ok(HandshakeResult {
                peer_role: wire::initiate_handshake(
                    stream,
                    role,
                    require_sender_dump_id(dump_id)?,
                    token,
                    channel_kind,
                    session_nonce,
                )?,
                peer_dump_id: None,
                channel_kind,
                session_nonce,
            })
        }
        Role::Receiver => {
            let hello = wire::respond_handshake_with_hello(stream, role, token)?;
            Ok(HandshakeResult {
                peer_role: hello.role,
                peer_dump_id: Some(hello.dump_id),
                channel_kind: hello.channel_kind,
                session_nonce: hello.session_nonce,
            })
        }
    }
}

fn require_sender_dump_id(dump_id: Option<&str>) -> Result<&str> {
    let dump_id = dump_id.unwrap_or_default();
    if dump_id.is_empty() {
        bail!("v4 sender transport handshake requires a non-empty dump_id");
    }
    crate::network::protocol::validate_dump_id(dump_id)?;
    Ok(dump_id)
}

pub(crate) fn establish_stream_session(
    mut stream: TcpStream,
    role: Role,
    dump_id: Option<&str>,
    token: Option<&str>,
    accepted_peer_addr: Option<SocketAddr>,
    local_channel: Option<(ChannelKind, u64)>,
) -> Result<ChannelSession> {
    stream
        .set_nonblocking(false)
        .context("failed to restore v4 stream to blocking mode")?;

    configure_stream(&stream)?;
    let handshake = perform_handshake(&mut stream, role, dump_id, token, local_channel)?;
    clear_handshake_timeout(&stream)?;

    let local_addr = stream
        .local_addr()
        .context("failed to query local v4 stream address")?;
    let peer_addr = match accepted_peer_addr {
        Some(peer_addr) => peer_addr,
        None => stream
            .peer_addr()
            .context("failed to query peer v4 stream address")?,
    };

    Ok(ChannelSession {
        stream,
        local_role: role,
        peer_role: handshake.peer_role,
        peer_dump_id: handshake.peer_dump_id,
        local_addr,
        peer_addr,
        channel_kind: handshake.channel_kind,
        session_nonce: handshake.session_nonce,
    })
}

pub(crate) fn assemble_session(control: ChannelSession, data: ChannelSession) -> Result<Session> {
    if control.channel_kind != ChannelKind::Control {
        bail!(
            "v4 expected control channel, got {:?}",
            control.channel_kind
        );
    }
    if data.channel_kind != ChannelKind::Data {
        bail!("v4 expected data channel, got {:?}", data.channel_kind);
    }
    if control.session_nonce != data.session_nonce {
        bail!(
            "v4 channel session nonce mismatch: control={} data={}",
            control.session_nonce,
            data.session_nonce
        );
    }
    if control.local_role != data.local_role {
        bail!("v4 local role mismatch between paired channels");
    }
    if control.peer_role != data.peer_role {
        bail!("v4 peer role mismatch between paired channels");
    }
    if control.peer_dump_id != data.peer_dump_id {
        bail!("v4 peer dump id mismatch between paired channels");
    }
    if control.peer_addr.ip() != data.peer_addr.ip() {
        bail!("v4 channel peer IP mismatch between paired channels");
    }

    Ok(Session {
        control_stream: control.stream,
        data_stream: data.stream,
        local_role: control.local_role,
        peer_role: control.peer_role,
        peer_dump_id: control.peer_dump_id,
        local_addr: control.local_addr,
        peer_addr: control.peer_addr,
    })
}

pub(crate) fn pending_session_key(channel: &ChannelSession) -> PendingSessionKey {
    PendingSessionKey {
        session_nonce: channel.session_nonce,
        peer_ip: channel.peer_addr.ip(),
        peer_role: channel.peer_role.as_u8(),
        peer_dump_id: channel.peer_dump_id.clone(),
    }
}

pub(crate) fn insert_pending_channel(
    pending: &mut HashMap<PendingSessionKey, PendingChannels>,
    channel: ChannelSession,
) -> Result<Option<Session>> {
    let key = pending_session_key(&channel);
    let entry = pending
        .entry(key.clone())
        .or_insert_with(|| PendingChannels {
            created_at: Instant::now(),
            control: None,
            data: None,
        });
    match channel.channel_kind {
        ChannelKind::Control => {
            if entry.control.is_some() {
                bail!(
                    "v4 duplicate control channel for session nonce {}",
                    key.session_nonce
                );
            }
            entry.control = Some(channel);
        }
        ChannelKind::Data => {
            if entry.data.is_some() {
                bail!(
                    "v4 duplicate data channel for session nonce {}",
                    key.session_nonce
                );
            }
            entry.data = Some(channel);
        }
    }

    if entry.control.is_some() && entry.data.is_some() {
        let PendingChannels { control, data, .. } = pending
            .remove(&key)
            .context("paired v4 channel session disappeared from pending map")?;
        return assemble_session(
            control.context("missing paired v4 control channel")?,
            data.context("missing paired v4 data channel")?,
        )
        .map(Some);
    }

    Ok(None)
}

pub(crate) fn drop_expired_pending_channels(
    pending: &mut HashMap<PendingSessionKey, PendingChannels>,
) {
    pending.retain(|_, channels| channels.created_at.elapsed() < PENDING_CHANNEL_TIMEOUT);
}

pub(crate) fn next_session_nonce() -> u64 {
    // Счётчик начинается с 1 в каждом процессе; случайный process seed исключает
    // систематическое смешивание каналов двух sender с одинаковым dump_id.
    SESSION_NONCE_HASHER.hash_one(NEXT_SESSION_NONCE.fetch_add(1, Ordering::Relaxed))
}

struct HandshakeResult {
    peer_role: Role,
    peer_dump_id: Option<String>,
    channel_kind: ChannelKind,
    session_nonce: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn channel(kind: ChannelKind, peer_addr: SocketAddr) -> ChannelSession {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let stream = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (_peer, _) = listener.accept().unwrap();
        ChannelSession {
            local_addr: stream.local_addr().unwrap(),
            stream,
            local_role: Role::Receiver,
            peer_role: Role::Sender,
            peer_dump_id: Some("same-dump".to_owned()),
            peer_addr,
            channel_kind: kind,
            session_nonce: 1,
        }
    }

    #[test]
    fn pending_channels_isolate_matching_nonces_from_different_peer_ips() {
        let mut pending = HashMap::new();
        for addr in ["127.0.0.2:1000", "127.0.0.3:1000"] {
            assert!(
                insert_pending_channel(
                    &mut pending,
                    channel(ChannelKind::Control, addr.parse().unwrap())
                )
                .unwrap()
                .is_none()
            );
        }
        assert_eq!(pending.len(), 2);
        for addr in ["127.0.0.3:1001", "127.0.0.2:1001"] {
            let addr = addr.parse::<SocketAddr>().unwrap();
            let session = insert_pending_channel(&mut pending, channel(ChannelKind::Data, addr))
                .unwrap()
                .unwrap();
            assert_eq!(session.peer_addr.ip(), addr.ip());
        }
        assert!(pending.is_empty());
    }

    #[test]
    fn session_nonce_uses_independent_process_seed_and_sequence() {
        let first_process = RandomState::new();
        let second_process = RandomState::new();
        let first: Vec<_> = (1_u64..=16)
            .map(|sequence| first_process.hash_one(sequence))
            .collect();
        let second: Vec<_> = (1_u64..=16)
            .map(|sequence| second_process.hash_one(sequence))
            .collect();
        assert_ne!(first, second);
        let generated: std::collections::HashSet<_> =
            (0..128).map(|_| next_session_nonce()).collect();
        assert_eq!(generated.len(), 128);
    }
}

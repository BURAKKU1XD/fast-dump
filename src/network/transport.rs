use std::collections::{HashMap, hash_map::RandomState};
use std::hash::BuildHasher;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::network::network_io;
use crate::network::protocol::wire::{self, ChannelKind};
use crate::progress::{self, Phase, PipelineStage, PipelineState};
use crate::role::Role;
use crate::runtime::{SessionControl, SocketRegistration};

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
pub struct Session<T = TcpStream> {
    pub control_stream: T,
    pub data_stream: T,
    pub local_role: Role,
    pub peer_role: Role,
    pub peer_dump_id: Option<String>,
    pub local_addr: SocketAddr,
    pub peer_addr: SocketAddr,
}

#[derive(Debug)]
pub(crate) struct ChannelSession<T = TcpStream> {
    pub(crate) stream: T,
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

/// Регистрации живут вместе с сессией и прерывают также частичный handshake.
pub(crate) fn establish_session_controlled(
    role: Role,
    mode: TransportMode,
    dump_id: Option<&str>,
    token: Option<&str>,
    control: &SessionControl,
) -> Result<(Session, Vec<SocketRegistration>)> {
    establish_session_inner(role, mode, dump_id, token, control, None)
}

pub(crate) fn establish_session_on_listener(
    listener: &BoundListener,
    peer: IpAddr,
    role: Role,
    dump_id: Option<&str>,
    token: Option<&str>,
    control: &SessionControl,
) -> Result<(Session, Vec<SocketRegistration>)> {
    establish_session_inner(
        role,
        TransportMode::Listen(listener.local_addr()),
        dump_id,
        token,
        control,
        Some((listener, peer)),
    )
}

fn establish_session_inner(
    role: Role,
    mode: TransportMode,
    dump_id: Option<&str>,
    token: Option<&str>,
    control: &SessionControl,
    prebound: Option<(&BoundListener, IpAddr)>,
) -> Result<(Session, Vec<SocketRegistration>)> {
    control.check()?;
    let _transport = progress::stage(
        Some(control),
        PipelineStage::Control,
        match mode {
            TransportMode::Connect(_) => PipelineState::Connecting,
            TransportMode::Listen(_) => PipelineState::WaitingConnection,
        },
    );
    let mut registrations = Vec::with_capacity(2);
    let result = (|| {
        let session_nonce = next_session_nonce();
        match mode {
            TransportMode::Connect(addr) => {
                if let Some(progress) = control.progress() {
                    progress.set_phase(Phase::Connecting);
                }
                let mut channels = Vec::with_capacity(2);
                for kind in [ChannelKind::Control, ChannelKind::Data] {
                    let (stream, registration) = connect_stream_controlled(addr, control)?;
                    registrations.push(registration);
                    let _handshake = progress::stage(
                        Some(control),
                        PipelineStage::Control,
                        PipelineState::Handshake,
                    );
                    channels.push(establish_stream_session(
                        stream,
                        role,
                        dump_id,
                        token,
                        None,
                        (role == Role::Sender).then_some((kind, session_nonce)),
                    )?);
                }
                let second = channels.pop().unwrap();
                let first = channels.pop().unwrap();
                match (first.channel_kind, second.channel_kind) {
                    (ChannelKind::Control, ChannelKind::Data) => assemble_session(first, second),
                    (ChannelKind::Data, ChannelKind::Control) => assemble_session(second, first),
                    _ => bail!("v4 peer opened duplicate channel kinds while establishing session"),
                }
            }
            TransportMode::Listen(addr) => {
                let owned;
                let listener = if let Some((listener, _)) = prebound {
                    listener
                } else {
                    owned = BoundListener::bind(addr)?;
                    &owned
                };
                listener.set_nonblocking(true)?;
                if let Some(progress) = control.progress() {
                    progress.set_phase(Phase::Listening);
                }
                let mut pending = HashMap::new();
                let mut sender_control = None;
                let mut pending_guards =
                    HashMap::<PendingSessionKey, Vec<SocketRegistration>>::new();
                let deadline = prebound.map(|_| Instant::now() + HANDSHAKE_TIMEOUT * 2);
                loop {
                    control.check()?;
                    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                        return Err(network_io(std::io::Error::from(
                            std::io::ErrorKind::TimedOut,
                        )))
                        .context("waiting for direct transfer channels");
                    }
                    drop_expired_pending_channels(&mut pending);
                    pending_guards.retain(|key, _| pending.contains_key(key));
                    let (stream, peer_addr) = match listener.try_accept_stream() {
                        Ok(connection) => connection,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(10));
                            continue;
                        }
                        Err(error) => {
                            return Err(network_io(error))
                                .context("failed to accept v4 connection");
                        }
                    };
                    if prebound.is_some_and(|(_, peer)| peer != peer_addr.ip()) {
                        continue;
                    }
                    let registration = control.register(&stream)?;
                    let kind = if registrations.is_empty() {
                        ChannelKind::Control
                    } else {
                        ChannelKind::Data
                    };
                    let handshake = progress::stage(
                        Some(control),
                        PipelineStage::Control,
                        PipelineState::Handshake,
                    );
                    let channel = establish_stream_session(
                        stream,
                        role,
                        dump_id,
                        token,
                        Some(peer_addr),
                        (role == Role::Sender).then_some((kind, session_nonce)),
                    )?;
                    drop(handshake);
                    let key = pending_session_key(&channel);
                    if role == Role::Sender {
                        registrations.push(registration);
                        if let Some(first) = sender_control.take() {
                            return assemble_session(first, channel);
                        }
                        sender_control = Some(channel);
                        continue;
                    } else {
                        pending_guards
                            .entry(key.clone())
                            .or_default()
                            .push(registration);
                    }
                    if let Some(session) = insert_pending_channel(&mut pending, channel)? {
                        if let Some(guards) = pending_guards.remove(&key) {
                            registrations.extend(guards);
                        }
                        return Ok(session);
                    }
                }
            }
        }
    })();
    if result.is_err() {
        progress::set_stage(
            Some(control),
            PipelineStage::Control,
            if control.is_cancelled() {
                PipelineState::Stopped
            } else {
                PipelineState::Failed
            },
        );
        control.check()?;
    }
    result.map(|session| (session, registrations))
}

pub(super) fn connect_stream_controlled(
    addr: SocketAddr,
    control: &SessionControl,
) -> Result<(TcpStream, SocketRegistration)> {
    use socket2::{Domain, Protocol, Socket, Type};

    control.check()?;
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_nonblocking(true)?;
    let connected = match socket.connect(&addr.into()) {
        Ok(()) => true,
        Err(error) if connect_is_pending(&error) => false,
        Err(error) => {
            return Err(network_io(error)).with_context(|| format!("failed to connect to {addr}"));
        }
    };
    let stream: TcpStream = socket.into();
    let registration = control.register(&stream)?;
    if !connected {
        loop {
            control.check()?;
            if !wait_connect_ready(&stream).map_err(network_io)? {
                continue;
            }
            control.check()?;
            if let Some(error) = stream.take_error().map_err(network_io)? {
                return Err(network_io(error))
                    .with_context(|| format!("failed to connect to {addr}"));
            }
            stream
                .peer_addr()
                .map_err(network_io)
                .with_context(|| format!("failed to connect to {addr}"))?;
            break;
        }
    }
    control.check()?;
    stream.set_nonblocking(false)?;
    Ok((stream, registration))
}

fn connect_is_pending(error: &std::io::Error) -> bool {
    if error.kind() == std::io::ErrorKind::WouldBlock {
        return true;
    }
    #[cfg(unix)]
    {
        error.raw_os_error() == Some(libc::EINPROGRESS)
    }
    #[cfg(windows)]
    {
        error.raw_os_error() == Some(windows_sys::Win32::Networking::WinSock::WSAEINPROGRESS)
    }
    #[cfg(not(any(unix, windows)))]
    {
        false
    }
}

#[cfg(unix)]
fn wait_connect_ready(stream: &TcpStream) -> std::io::Result<bool> {
    use std::os::fd::AsRawFd;
    let mut descriptor = libc::pollfd {
        fd: stream.as_raw_fd(),
        events: libc::POLLOUT,
        revents: 0,
    };
    // poll только наблюдает сокет; единственная попытка connect остаётся активной.
    let result = unsafe { libc::poll(&mut descriptor, 1, 50) };
    if result < 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::Interrupted {
            return Ok(false);
        }
        return Err(error);
    }
    Ok(result > 0)
}

#[cfg(windows)]
fn wait_connect_ready(stream: &TcpStream) -> std::io::Result<bool> {
    use std::os::windows::io::AsRawSocket;
    use windows_sys::Win32::Networking::WinSock::{POLLOUT, WSAGetLastError, WSAPOLLFD, WSAPoll};
    let mut descriptor = WSAPOLLFD {
        fd: stream.as_raw_socket() as _,
        events: POLLOUT,
        revents: 0,
    };
    let result = unsafe { WSAPoll(&mut descriptor, 1, 50) };
    if result < 0 {
        return Err(std::io::Error::from_raw_os_error(unsafe {
            WSAGetLastError()
        }));
    }
    Ok(result > 0)
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
    let stream = TcpStream::connect(addr)
        .map_err(network_io)
        .with_context(|| format!("failed to connect to {addr}"))?;
    establish_stream_session(stream, role, dump_id, token, None, local_channel)
}

pub struct BoundListener {
    listener: TcpListener,
    local_addr: SocketAddr,
}

impl BoundListener {
    pub fn bind(addr: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(addr).map_err(|error| super::listen_error(addr, error))?;
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
            .map_err(network_io)
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
    stream: &mut (impl Read + Write),
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
    stream: TcpStream,
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

    let local_addr = stream
        .local_addr()
        .context("failed to query local v4 stream address")?;
    let peer_addr = match accepted_peer_addr {
        Some(peer_addr) => peer_addr,
        None => stream
            .peer_addr()
            .context("failed to query peer v4 stream address")?,
    };

    let channel = handshake_channel(
        stream,
        role,
        dump_id,
        token,
        local_channel,
        local_addr,
        peer_addr,
    )?;
    clear_handshake_timeout(&channel.stream)?;
    Ok(channel)
}

pub(crate) fn handshake_channel<T: Read + Write>(
    mut stream: T,
    role: Role,
    dump_id: Option<&str>,
    token: Option<&str>,
    local_channel: Option<(ChannelKind, u64)>,
    local_addr: SocketAddr,
    peer_addr: SocketAddr,
) -> Result<ChannelSession<T>> {
    let handshake = perform_handshake(&mut stream, role, dump_id, token, local_channel)?;
    if let Some((expected, _)) = local_channel {
        anyhow::ensure!(
            handshake.channel_kind == expected,
            "v4 handshake channel kind mismatch"
        );
    }
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

pub(crate) fn assemble_session<T>(
    control: ChannelSession<T>,
    data: ChannelSession<T>,
) -> Result<Session<T>> {
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
    use crate::runtime::{Cancelled, RunControl};

    #[test]
    fn controlled_connect_preserves_both_channel_handshakes() {
        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();
        let server =
            std::thread::spawn(move || listener.accept(Role::Receiver, None, None).unwrap());
        let run = RunControl::new(None);
        let control = run.session(Role::Sender, "controlled", &addr.to_string());
        let (session, guards) = establish_session_controlled(
            Role::Sender,
            TransportMode::Connect(addr),
            Some("controlled"),
            None,
            &control,
        )
        .unwrap();
        let peer = server.join().unwrap();
        assert_eq!(peer.peer_dump_id.as_deref(), Some("controlled"));
        assert_eq!(session.peer_role, Role::Receiver);
        assert_eq!(guards.len(), 2);
    }

    #[test]
    fn controlled_connect_cancels_silent_handshake_without_peer_close() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let run = RunControl::new(Some(std::sync::Arc::new(
            crate::progress::ProgressHub::new(),
        )));
        let control = run.session(Role::Sender, "controlled", &addr.to_string());
        let progress = control.progress().unwrap().clone();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let result = establish_session_controlled(
                Role::Sender,
                TransportMode::Connect(addr),
                Some("controlled"),
                None,
                &control,
            )
            .map(|_| ());
            done_tx.send(result).unwrap();
        });
        let (mut peer, _) = listener.accept().unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        wire::read_message(&mut peer).unwrap();
        assert_eq!(
            progress
                .snapshot()
                .pipelines
                .iter()
                .find(|p| p.stage == PipelineStage::Control)
                .unwrap()
                .state,
            PipelineState::Handshake,
        );
        run.cancel();
        let completed = done_rx.recv_timeout(Duration::from_secs(2));
        let _ = peer.shutdown_stream();
        worker.join().unwrap();
        assert_eq!(
            progress
                .snapshot()
                .pipelines
                .iter()
                .find(|p| p.stage == PipelineStage::Control)
                .unwrap()
                .state,
            PipelineState::Stopped,
        );
        assert!(
            completed
                .expect("handshake did not observe cancellation")
                .unwrap_err()
                .is::<Cancelled>()
        );
    }

    #[test]
    fn controlled_listener_cancels_while_waiting_for_a_peer() {
        let run = RunControl::new(None);
        let control = run.session(Role::Sender, "controlled", "127.0.0.1:0");
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let result = establish_session_controlled(
                Role::Sender,
                TransportMode::Listen("127.0.0.1:0".parse().unwrap()),
                Some("controlled"),
                None,
                &control,
            )
            .map(|_| ());
            done_tx.send(result).unwrap();
        });
        started_rx.recv().unwrap();
        run.cancel();
        assert!(
            done_rx
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap_err()
                .is::<Cancelled>()
        );
        worker.join().unwrap();
    }

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

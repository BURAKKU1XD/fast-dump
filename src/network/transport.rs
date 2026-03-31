use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

use anyhow::{Context, Result, bail};

use super::protocol;
use crate::role::Role;

// Этот слой отвечает за TCP-соединение и его базовую конфигурацию.
// Семантика sender/receiver определяется выше по стеку через Role,
// а wire protocol передаётся в модуль `protocol`.

// Небольшой таймаут нужен, чтобы handshake не зависал бесконечно.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

// Режимы транспорта: либо слушаем входящее подключение,
// либо сами подключаемся к удалённой стороне.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportMode {
    Listen(SocketAddr),
    Connect(SocketAddr),
}

// Уже установленная transport-сессия.
// Помимо адресов и ролей наружу возвращаем сам поток, потому что следующий
// слой сразу продолжает wire protocol после handshake без переподключения.
#[derive(Debug)]
pub struct Session {
    pub stream: TcpStream,
    pub local_role: Role,
    pub peer_role: Role,
    pub peer_dump_id: Option<String>,
    pub local_addr: SocketAddr,
    pub peer_addr: SocketAddr,
}

// Общая точка входа для запуска транспорта.
// Она скрывает разницу между listen/connect, чтобы дальнейший код
// работал уже с установленной сессией, а не с деталями TCP-старта.
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

// В listen-режиме поднимаем listener и принимаем одно соединение.
pub fn listen_once(
    role: Role,
    addr: SocketAddr,
    dump_id: Option<&str>,
    token: Option<&str>,
) -> Result<Session> {
    BoundListener::bind(addr)?.accept(role, dump_id, token)
}

// В connect-режиме сами выходим наружу и делаем handshake.
pub fn connect_once(
    role: Role,
    addr: SocketAddr,
    dump_id: Option<&str>,
    token: Option<&str>,
) -> Result<Session> {
    let stream =
        TcpStream::connect(addr).with_context(|| format!("failed to connect to {addr}"))?;
    establish_stream_session(stream, role, dump_id, token, None)
}

// Отдельный тип удобен для тестов:
// можно bind'нуться на `127.0.0.1:0`, узнать реальный порт и только потом звать accept.
pub struct BoundListener {
    listener: TcpListener,
    local_addr: SocketAddr,
}

impl BoundListener {
    // Если в адресе был порт `0`, ОС выдаст свободный порт.
    // Поэтому мы сразу сохраняем фактический local_addr.
    pub fn bind(addr: SocketAddr) -> Result<Self> {
        let listener =
            TcpListener::bind(addr).with_context(|| format!("failed to bind to {addr}"))?;
        let local_addr = listener
            .local_addr()
            .context("failed to query bound listener address")?;

        Ok(Self {
            listener,
            local_addr,
        })
    }

    // Этот адрес нужен тестам и будущему orchestration-коду.
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    // Принимаем одно соединение и валидируем удалённую роль через handshake.
    // Метод теперь не consumes self, потому что receiver в listen-режиме может
    // обслуживать несколько последовательных или параллельных сессий.
    pub fn accept(
        &self,
        role: Role,
        dump_id: Option<&str>,
        token: Option<&str>,
    ) -> Result<Session> {
        let (stream, peer_addr) = self.accept_stream()?;
        establish_stream_session(stream, role, dump_id, token, Some(peer_addr))
    }

    pub(crate) fn accept_stream(&self) -> Result<(TcpStream, SocketAddr)> {
        self.listener
            .accept()
            .context("failed to accept incoming connection")
    }

    pub(crate) fn try_accept_stream(&self) -> std::io::Result<(TcpStream, SocketAddr)> {
        self.listener.accept()
    }

    pub(crate) fn set_nonblocking(&self, nonblocking: bool) -> Result<()> {
        self.listener
            .set_nonblocking(nonblocking)
            .context("failed to configure listener nonblocking mode")
    }
}

// Базовая конфигурация сокета для текущего этапа.
// TCP_NODELAY полезен здесь, потому что handshake состоит из маленьких пакетов.
pub(crate) fn configure_stream(stream: &TcpStream) -> Result<()> {
    stream
        .set_nodelay(true)
        .context("failed to enable TCP_NODELAY")?;
    stream
        .set_read_timeout(Some(HANDSHAKE_TIMEOUT))
        .context("failed to configure read timeout")?;
    stream
        .set_write_timeout(Some(HANDSHAKE_TIMEOUT))
        .context("failed to configure write timeout")?;

    Ok(())
}

// После handshake здесь начинается уже долгоживущий dump-поток.
// Короткий bootstrap-timeout в этой фазе привёл бы к ложным I/O timeout,
// поэтому возвращаем сокет в обычный blocking-режим.
pub(crate) fn clear_handshake_timeout(stream: &TcpStream) -> Result<()> {
    stream
        .set_read_timeout(None)
        .context("failed to clear read timeout after handshake")?;
    stream
        .set_write_timeout(None)
        .context("failed to clear write timeout after handshake")?;

    Ok(())
}

// Handshake привязан к application-role, а не к тому, кто открыл TCP-сокет.
// В push и pull именно sender первым объявляет dump_id в Hello,
// а receiver только подтверждает сессию.
fn perform_handshake(
    stream: &mut TcpStream,
    role: Role,
    dump_id: Option<&str>,
    token: Option<&str>,
) -> Result<HandshakeResult> {
    match role {
        Role::Sender => Ok(HandshakeResult {
            peer_role: protocol::initiate_handshake(
                stream,
                role,
                require_sender_dump_id(dump_id)?,
                token,
            )?,
            peer_dump_id: None,
        }),
        Role::Receiver => {
            let hello = protocol::respond_handshake_with_hello(stream, role, token)?;
            Ok(HandshakeResult {
                peer_role: hello.role,
                peer_dump_id: Some(hello.dump_id),
            })
        }
    }
}

fn require_sender_dump_id(dump_id: Option<&str>) -> Result<&str> {
    let dump_id = dump_id.unwrap_or_default();
    if dump_id.is_empty() {
        bail!("sender transport handshake requires a non-empty dump_id");
    }

    Ok(dump_id)
}

pub(crate) fn establish_stream_session(
    mut stream: TcpStream,
    role: Role,
    dump_id: Option<&str>,
    token: Option<&str>,
    accepted_peer_addr: Option<SocketAddr>,
) -> Result<Session> {
    stream
        .set_nonblocking(false)
        .context("failed to restore accepted stream to blocking mode")?;

    // Настройки сокета применяем до handshake, чтобы bootstrap шел под тем же timeout-контрактом
    // и для исходящих, и для принятых вручную сессий.
    configure_stream(&stream)?;

    let handshake = perform_handshake(&mut stream, role, dump_id, token)?;
    clear_handshake_timeout(&stream)?;

    let local_addr = stream
        .local_addr()
        .context("failed to query local stream address")?;
    let peer_addr = match accepted_peer_addr {
        Some(peer_addr) => peer_addr,
        None => stream
            .peer_addr()
            .context("failed to query peer stream address")?,
    };

    Ok(Session {
        stream,
        local_role: role,
        peer_role: handshake.peer_role,
        peer_dump_id: handshake.peer_dump_id,
        local_addr,
        peer_addr,
    })
}

struct HandshakeResult {
    peer_role: Role,
    peer_dump_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    use super::{
        BoundListener, HANDSHAKE_TIMEOUT, clear_handshake_timeout, configure_stream, connect_once,
    };
    use crate::role::Role;

    fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let accepted = thread::spawn(move || listener.accept().unwrap().0);
        let client = TcpStream::connect(addr).unwrap();
        let server = accepted.join().unwrap();

        (client, server)
    }

    #[test]
    fn bound_listener_reports_assigned_loopback_address() {
        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();

        assert!(addr.ip().is_loopback());
        assert_ne!(addr.port(), 0);
    }

    #[test]
    fn configure_stream_sets_expected_socket_options() {
        let (client, _server) = tcp_pair();

        configure_stream(&client).unwrap();

        assert!(client.nodelay().unwrap());
        assert_eq!(client.read_timeout().unwrap(), Some(HANDSHAKE_TIMEOUT));
        assert_eq!(client.write_timeout().unwrap(), Some(HANDSHAKE_TIMEOUT));
    }

    #[test]
    fn clear_handshake_timeout_restores_blocking_socket_mode() {
        let (client, _server) = tcp_pair();

        configure_stream(&client).unwrap();
        clear_handshake_timeout(&client).unwrap();

        assert_eq!(client.read_timeout().unwrap(), None);
        assert_eq!(client.write_timeout().unwrap(), None);
    }

    #[test]
    fn successful_session_clears_handshake_timeouts_on_both_peers() {
        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();

        let receiver = thread::spawn(move || listener.accept(Role::Receiver, None, None).unwrap());
        let sender_session = connect_once(Role::Sender, addr, Some("dump-01"), None).unwrap();
        let receiver_session = receiver.join().unwrap();

        assert_eq!(sender_session.stream.read_timeout().unwrap(), None);
        assert_eq!(sender_session.stream.write_timeout().unwrap(), None);
        assert_eq!(receiver_session.stream.read_timeout().unwrap(), None);
        assert_eq!(receiver_session.stream.write_timeout().unwrap(), None);
        assert_eq!(sender_session.peer_dump_id, None);
        assert_eq!(receiver_session.peer_dump_id.as_deref(), Some("dump-01"));
    }

    #[test]
    fn sender_session_requires_non_empty_dump_id_for_handshake() {
        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();

        let receiver =
            thread::spawn(move || listener.accept(Role::Receiver, None, None).unwrap_err());
        let error = connect_once(Role::Sender, addr, None, None).unwrap_err();
        let receiver_error = receiver.join().unwrap();

        assert!(
            error
                .to_string()
                .contains("sender transport handshake requires a non-empty dump_id")
        );
        assert!(
            receiver_error
                .to_string()
                .contains("failed to read packet header")
        );
    }

    #[test]
    fn connect_once_fails_when_peer_is_absent() {
        let probe = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let error = connect_once(Role::Sender, addr, Some("dump-01"), None).unwrap_err();

        assert!(error.to_string().contains("failed to connect"));
    }

    #[test]
    fn session_rejects_sender_with_wrong_token() {
        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();

        let receiver = thread::spawn(move || {
            listener
                .accept(Role::Receiver, None, Some("secret"))
                .unwrap_err()
        });
        let error = connect_once(Role::Sender, addr, Some("dump-01"), Some("wrong")).unwrap_err();
        let receiver_error = receiver.join().unwrap();

        assert!(
            error
                .to_string()
                .contains("peer rejected handshake: bad token")
        );
        assert!(receiver_error.to_string().contains("bad token"));
    }
}

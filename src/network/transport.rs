use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

use anyhow::{Context, Result};

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

// Минимальная информация о уже установленной сессии.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Session {
    pub local_role: Role,
    pub peer_role: Role,
    pub local_addr: SocketAddr,
    pub peer_addr: SocketAddr,
}

// Общая точка входа для запуска транспорта.
// Она скрывает разницу между listen/connect, чтобы дальнейший код
// работал уже с установленной сессией, а не с деталями TCP-старта.
pub fn establish_session(role: Role, mode: TransportMode) -> Result<Session> {
    match mode {
        TransportMode::Listen(addr) => listen_once(role, addr),
        TransportMode::Connect(addr) => connect_once(role, addr),
    }
}

// В listen-режиме поднимаем listener и принимаем одно соединение.
pub fn listen_once(role: Role, addr: SocketAddr) -> Result<Session> {
    BoundListener::bind(addr)?.accept(role)
}

// В connect-режиме сами выходим наружу и делаем handshake.
pub fn connect_once(role: Role, addr: SocketAddr) -> Result<Session> {
    let mut stream =
        TcpStream::connect(addr).with_context(|| format!("failed to connect to {addr}"))?;
    // Настройки сокета применяем до handshake, чтобы и чтение, и запись
    // первых пакетов уже подчинялись одному timeout-контракту.
    configure_stream(&stream)?;

    let peer_role = protocol::initiate_handshake(&mut stream, role)?;
    // Адреса читаем уже после успешного handshake, чтобы вернуть только
    // валидированную сессию, а не "сырой" сокет на полпути.
    let local_addr = stream
        .local_addr()
        .context("failed to query local stream address")?;
    let peer_addr = stream
        .peer_addr()
        .context("failed to query peer stream address")?;

    Ok(Session {
        local_role: role,
        peer_role,
        local_addr,
        peer_addr,
    })
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
    // Метод consumes self намеренно: текущий v1-transport обслуживает
    // одну сессию за вызов и делает это явно в типе API.
    pub fn accept(self, role: Role) -> Result<Session> {
        let (mut stream, peer_addr) = self
            .listener
            .accept()
            .context("failed to accept incoming connection")?;
        // Для входящего соединения применяем те же правила таймаутов,
        // что и для исходящего, чтобы handshake вёл себя симметрично.
        configure_stream(&stream)?;

        let peer_role = protocol::respond_handshake(&mut stream, role)?;
        let local_addr = stream
            .local_addr()
            .context("failed to query local stream address")?;

        Ok(Session {
            local_role: role,
            peer_role,
            local_addr,
            peer_addr,
        })
    }
}

// Базовая конфигурация сокета для текущего этапа.
// TCP_NODELAY полезен здесь, потому что handshake состоит из маленьких пакетов.
fn configure_stream(stream: &TcpStream) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    use super::{BoundListener, HANDSHAKE_TIMEOUT, configure_stream, connect_once};
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
    fn connect_once_fails_when_peer_is_absent() {
        let probe = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let error = connect_once(Role::Sender, addr).unwrap_err();

        assert!(error.to_string().contains("failed to connect"));
    }
}

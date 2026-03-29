use std::io::{Read, Write};

use anyhow::{Context, Result, bail};

use crate::role::Role;

// Этот модуль работает поверх уже открытого двустороннего потока.
// Благодаря generic `Read + Write` handshake можно тестировать отдельно от TCP.

// Фиксированные значения заголовка каждого пакета.
// По ним приёмник понимает, что читает именно наш протокол.
pub const MAGIC: u32 = 0x504D4446;
pub const VERSION: u16 = 1;

// Пока протокол минимальный: только handshake и ошибка.
// Позже сюда добавятся Resume/Frame/Ack сообщения.
// Значения enum являются wire-значениями, поэтому порядок и номера должны
// меняться только вместе с версией протокола.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
enum MessageKind {
    Hello = 1,
    HelloAck = 2,
    Error = 3,
}

impl MessageKind {
    // Преобразуем число из заголовка пакета в enum.
    fn from_u16(value: u16) -> Result<Self> {
        match value {
            1 => Ok(Self::Hello),
            2 => Ok(Self::HelloAck),
            3 => Ok(Self::Error),
            _ => bail!("unknown message kind: {value}"),
        }
    }
}

// Внутреннее представление прочитанного пакета.
// В коде выше по стеку уже не важно, как именно он пришёл по сети.
#[derive(Debug)]
struct Packet {
    kind: MessageKind,
    body: Vec<u8>,
}

// Handshake для стороны, которая сама инициировала TCP-подключение.
// Она первой отправляет Hello и ждёт подтверждение.
pub fn initiate_handshake<T>(stream: &mut T, local_role: Role) -> Result<Role>
where
    T: Read + Write,
{
    write_packet(stream, MessageKind::Hello, &[local_role.as_u8()])?;
    // Hello маленький, поэтому flush делаем явно: без него другая сторона
    // может ждать первый пакет, пока он сидит в буфере writer'а.
    stream.flush().context("failed to flush hello packet")?;

    let packet = read_packet(stream)?;
    match packet.kind {
        MessageKind::HelloAck => {
            let remote_role = parse_role(&packet.body).context("invalid hello ack")?;
            validate_peer_role(local_role, remote_role)?;
            Ok(remote_role)
        }
        MessageKind::Error => {
            let message = String::from_utf8(packet.body).context("peer sent a non-utf8 error")?;
            bail!("peer rejected handshake: {message}")
        }
        other => bail!("unexpected packet during handshake: {other:?}"),
    }
}

// Handshake для стороны, которая приняла входящее TCP-подключение.
// Она сначала читает Hello, валидирует роль и только потом отвечает.
pub fn respond_handshake<T>(stream: &mut T, local_role: Role) -> Result<Role>
where
    T: Read + Write,
{
    let packet = read_packet(stream)?;
    match packet.kind {
        MessageKind::Hello => {
            let remote_role = parse_role(&packet.body).context("invalid hello")?;
            if let Err(error) = validate_peer_role(local_role, remote_role) {
                // Ошибку отправляем best-effort: если peer уже отвалился,
                // нам всё равно важнее вернуть локальную причину сбоя.
                let _ = send_error(stream, &error.to_string());
                return Err(error);
            }

            write_packet(stream, MessageKind::HelloAck, &[local_role.as_u8()])?;
            // После валидации роли сразу подтверждаем handshake,
            // чтобы инициатор мог переходить к следующему этапу протокола.
            stream.flush().context("failed to flush hello ack")?;
            Ok(remote_role)
        }
        MessageKind::Error => {
            let message = String::from_utf8(packet.body).context("peer sent a non-utf8 error")?;
            bail!("peer reported an error before handshake: {message}")
        }
        other => {
            // Если peer начал не с Hello, пытаемся явно объяснить ожидание протокола.
            let _ = send_error(stream, "expected hello packet");
            bail!("unexpected packet before handshake: {other:?}")
        }
    }
}

// На текущем этапе противоположные роли обязательны:
// sender должен видеть receiver, а receiver должен видеть sender.
// Это правило не зависит от того, кто инициировал TCP-подключение.
fn validate_peer_role(local_role: Role, remote_role: Role) -> Result<()> {
    if local_role == remote_role {
        bail!(
            "handshake requires opposite roles, got local={} remote={}",
            local_role,
            remote_role
        );
    }

    Ok(())
}

// Тело Hello и HelloAck сейчас состоит ровно из одного байта с ролью.
fn parse_role(body: &[u8]) -> Result<Role> {
    let role = *body
        .first()
        .context("role packet body must contain exactly one byte")?;
    if body.len() != 1 {
        bail!("role packet body must contain exactly one byte");
    }

    Role::from_u8(role)
}

// Error-пакет нужен, чтобы удалённая сторона получила внятную причину отказа,
// а не просто закрытый сокет без объяснения.
fn send_error<T>(stream: &mut T, message: &str) -> Result<()>
where
    T: Write,
{
    write_packet(stream, MessageKind::Error, message.as_bytes())?;
    stream.flush().context("failed to flush error packet")
}

// Общая запись пакета: фиксированный заголовок + произвольное тело.
fn write_packet<T>(stream: &mut T, kind: MessageKind, body: &[u8]) -> Result<()>
where
    T: Write,
{
    // В заголовке длина хранится как u32, поэтому слишком большое тело
    // отсекаем до записи в поток.
    let body_len = u32::try_from(body.len()).context("packet body too large")?;

    // Формат заголовка фиксирован:
    // magic (4) + version (2) + kind (2) + body_len (4), всё little-endian.
    stream
        .write_all(&MAGIC.to_le_bytes())
        .context("failed to write packet magic")?;
    stream
        .write_all(&VERSION.to_le_bytes())
        .context("failed to write packet version")?;
    stream
        .write_all(&(kind as u16).to_le_bytes())
        .context("failed to write packet kind")?;
    stream
        .write_all(&body_len.to_le_bytes())
        .context("failed to write packet body length")?;
    stream
        .write_all(body)
        .context("failed to write packet body")?;

    Ok(())
}

// Общий код чтения пакета из сокета.
// Сначала читаем заголовок, потом уже выделяем буфер под тело.
fn read_packet<T>(stream: &mut T) -> Result<Packet>
where
    T: Read,
{
    // 12 байт = фиксированный заголовок пакета без тела.
    let mut header = [0_u8; 12];
    stream
        .read_exact(&mut header)
        .context("failed to read packet header")?;

    // Разбираем заголовок вручную по фиксированным смещениям.
    // Так мы не создаём промежуточные структуры и сразу валидируем wire-формат.
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if magic != MAGIC {
        bail!("invalid packet magic: {magic:#010x}");
    }

    let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
    if version != VERSION {
        bail!("unsupported protocol version: {version}");
    }

    let kind = u16::from_le_bytes(header[6..8].try_into().unwrap());
    let kind = MessageKind::from_u16(kind)?;
    let body_len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;

    // Тело дочитываем ровно в один буфер нужного размера.
    let mut body = vec![0_u8; body_len];
    stream
        .read_exact(&mut body)
        .context("failed to read packet body")?;

    Ok(Packet { kind, body })
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor, Read, Write};

    use super::{
        MAGIC, MessageKind, Packet, VERSION, initiate_handshake, parse_role, read_packet,
        respond_handshake, write_packet,
    };
    use crate::role::Role;

    struct ScriptedStream {
        incoming: Cursor<Vec<u8>>,
        written: Vec<u8>,
    }

    impl ScriptedStream {
        fn new(incoming: Vec<u8>) -> Self {
            Self {
                incoming: Cursor::new(incoming),
                written: Vec::new(),
            }
        }
    }

    impl Read for ScriptedStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.incoming.read(buf)
        }
    }

    impl Write for ScriptedStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.written.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn encode_packet(kind: MessageKind, body: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        write_packet(&mut bytes, kind, body).unwrap();
        bytes
    }

    fn decode_packet(bytes: &[u8]) -> Packet {
        read_packet(&mut Cursor::new(bytes.to_vec())).unwrap()
    }

    #[test]
    fn packet_roundtrip_preserves_kind_and_body() {
        let packet = decode_packet(&encode_packet(MessageKind::Hello, &[Role::Sender.as_u8()]));

        assert_eq!(packet.kind, MessageKind::Hello);
        assert_eq!(packet.body, vec![Role::Sender.as_u8()]);
    }

    #[test]
    fn read_packet_rejects_invalid_magic() {
        let mut bytes = encode_packet(MessageKind::Hello, &[Role::Sender.as_u8()]);
        bytes[..4].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());

        let error = read_packet(&mut Cursor::new(bytes)).unwrap_err();

        assert!(error.to_string().contains("invalid packet magic"));
    }

    #[test]
    fn read_packet_rejects_unsupported_version() {
        let mut bytes = encode_packet(MessageKind::Hello, &[Role::Sender.as_u8()]);
        bytes[4..6].copy_from_slice(&(VERSION + 1).to_le_bytes());

        let error = read_packet(&mut Cursor::new(bytes)).unwrap_err();

        assert!(
            error
                .to_string()
                .contains(&format!("unsupported protocol version: {}", VERSION + 1))
        );
    }

    #[test]
    fn read_packet_rejects_unknown_message_kind() {
        let mut bytes = encode_packet(MessageKind::Hello, &[Role::Sender.as_u8()]);
        bytes[6..8].copy_from_slice(&99_u16.to_le_bytes());

        let error = read_packet(&mut Cursor::new(bytes)).unwrap_err();

        assert!(error.to_string().contains("unknown message kind: 99"));
    }

    #[test]
    fn parse_role_rejects_empty_and_long_body() {
        let empty_error = parse_role(&[]).unwrap_err();
        assert!(
            empty_error
                .to_string()
                .contains("role packet body must contain exactly one byte")
        );

        let long_error = parse_role(&[Role::Sender.as_u8(), Role::Receiver.as_u8()]).unwrap_err();
        assert!(
            long_error
                .to_string()
                .contains("role packet body must contain exactly one byte")
        );
    }

    #[test]
    fn initiate_handshake_sends_hello_and_accepts_ack() {
        let mut stream = ScriptedStream::new(encode_packet(
            MessageKind::HelloAck,
            &[Role::Receiver.as_u8()],
        ));

        let remote_role = initiate_handshake(&mut stream, Role::Sender).unwrap();

        assert_eq!(remote_role, Role::Receiver);

        let packet = decode_packet(&stream.written);
        assert_eq!(packet.kind, MessageKind::Hello);
        assert_eq!(packet.body, vec![Role::Sender.as_u8()]);
    }

    #[test]
    fn initiate_handshake_rejects_same_role_ack() {
        let mut stream = ScriptedStream::new(encode_packet(
            MessageKind::HelloAck,
            &[Role::Sender.as_u8()],
        ));

        let error = initiate_handshake(&mut stream, Role::Sender).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("handshake requires opposite roles")
        );

        let packet = decode_packet(&stream.written);
        assert_eq!(packet.kind, MessageKind::Hello);
        assert_eq!(packet.body, vec![Role::Sender.as_u8()]);
    }

    #[test]
    fn initiate_handshake_surfaces_peer_error_packet() {
        let mut stream = ScriptedStream::new(encode_packet(MessageKind::Error, b"bad token"));

        let error = initiate_handshake(&mut stream, Role::Sender).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("peer rejected handshake: bad token")
        );

        let packet = decode_packet(&stream.written);
        assert_eq!(packet.kind, MessageKind::Hello);
        assert_eq!(packet.body, vec![Role::Sender.as_u8()]);
    }

    #[test]
    fn initiate_handshake_rejects_unexpected_packet_kind() {
        let mut stream =
            ScriptedStream::new(encode_packet(MessageKind::Hello, &[Role::Receiver.as_u8()]));

        let error = initiate_handshake(&mut stream, Role::Sender).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unexpected packet during handshake")
        );

        let packet = decode_packet(&stream.written);
        assert_eq!(packet.kind, MessageKind::Hello);
        assert_eq!(packet.body, vec![Role::Sender.as_u8()]);
    }

    #[test]
    fn respond_handshake_returns_remote_role_and_emits_ack() {
        let request = encode_packet(MessageKind::Hello, &[Role::Sender.as_u8()]);
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let remote_role = respond_handshake(&mut stream, Role::Receiver).unwrap();
        let bytes = stream.into_inner();

        assert_eq!(remote_role, Role::Sender);

        let reply = decode_packet(&bytes[request_len..]);
        assert_eq!(reply.kind, MessageKind::HelloAck);
        assert_eq!(reply.body, vec![Role::Receiver.as_u8()]);
    }

    #[test]
    fn respond_handshake_rejects_same_role_and_emits_error() {
        let request = encode_packet(MessageKind::Hello, &[Role::Sender.as_u8()]);
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = respond_handshake(&mut stream, Role::Sender).unwrap_err();
        let bytes = stream.into_inner();

        assert!(
            error
                .to_string()
                .contains("handshake requires opposite roles")
        );

        let reply = decode_packet(&bytes[request_len..]);
        assert_eq!(reply.kind, MessageKind::Error);

        let body = String::from_utf8(reply.body).unwrap();
        assert!(body.contains("handshake requires opposite roles"));
    }

    #[test]
    fn respond_handshake_surfaces_peer_error_packet() {
        let request = encode_packet(MessageKind::Error, b"bad token");
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = respond_handshake(&mut stream, Role::Receiver).unwrap_err();
        let bytes = stream.into_inner();

        assert!(
            error
                .to_string()
                .contains("peer reported an error before handshake: bad token")
        );
        assert_eq!(bytes.len(), request_len);
    }

    #[test]
    fn respond_handshake_rejects_unexpected_packet_and_emits_error() {
        let request = encode_packet(MessageKind::HelloAck, &[Role::Sender.as_u8()]);
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = respond_handshake(&mut stream, Role::Receiver).unwrap_err();
        let bytes = stream.into_inner();

        assert!(
            error
                .to_string()
                .contains("unexpected packet before handshake")
        );

        let reply = decode_packet(&bytes[request_len..]);
        assert_eq!(reply.kind, MessageKind::Error);
        assert_eq!(
            String::from_utf8(reply.body).unwrap(),
            "expected hello packet"
        );
    }

    #[test]
    fn encoded_packets_keep_expected_header_constants() {
        let bytes = encode_packet(MessageKind::Hello, &[Role::Sender.as_u8()]);

        assert_eq!(u32::from_le_bytes(bytes[0..4].try_into().unwrap()), MAGIC);
        assert_eq!(u16::from_le_bytes(bytes[4..6].try_into().unwrap()), VERSION);
    }
}

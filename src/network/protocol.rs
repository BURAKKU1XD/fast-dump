use std::io::{Cursor, Read, Write};

use anyhow::{Context, Result, bail};

use crate::role::Role;

// Этот модуль отвечает только за бинарный wire protocol поверх уже открытого потока.
// TCP-сокет и его параметры живут в transport-слое, а здесь остаются только
// message types, кодирование и декодирование.

// Фиксированные значения заголовка каждого пакета.
// По ним приёмник понимает, что читает именно наш протокол.
pub const MAGIC: u32 = 0x504D4446;
pub const VERSION: u16 = 2;
pub const MAX_FRAME_CHUNK_BYTES: usize = 1024 * 1024;

const MAX_CONTROL_BODY_BYTES: usize = 64 * 1024;

// Тип сообщения кодируется в заголовке пакета и остаётся стабильной частью wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum MessageKind {
    Hello = 1,
    HelloAck = 2,
    ResumeQuery = 3,
    ResumeState = 4,
    FrameStart = 5,
    FrameChunk = 6,
    FrameEnd = 7,
    Ack = 8,
    Finish = 9,
    FinishAck = 10,
    Error = 11,
    FrameAbort = 12,
    AbortAck = 13,
    IssueJournalChunk = 14,
}

impl MessageKind {
    fn from_u16(value: u16) -> Result<Self> {
        match value {
            1 => Ok(Self::Hello),
            2 => Ok(Self::HelloAck),
            3 => Ok(Self::ResumeQuery),
            4 => Ok(Self::ResumeState),
            5 => Ok(Self::FrameStart),
            6 => Ok(Self::FrameChunk),
            7 => Ok(Self::FrameEnd),
            8 => Ok(Self::Ack),
            9 => Ok(Self::Finish),
            10 => Ok(Self::FinishAck),
            11 => Ok(Self::Error),
            12 => Ok(Self::FrameAbort),
            13 => Ok(Self::AbortAck),
            14 => Ok(Self::IssueJournalChunk),
            _ => bail!("unknown message kind: {value}"),
        }
    }
}

impl MessageKind {
    const fn max_body_len(self) -> usize {
        match self {
            Self::FrameChunk => MAX_FRAME_CHUNK_BYTES,
            Self::FinishAck => 0,
            _ => MAX_CONTROL_BODY_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    pub role: Role,
    pub protocol_version: u16,
    pub dump_id: String,
    pub token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloAck {
    pub role: Role,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeQuery {
    pub dump_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeState {
    pub last_committed_frame: u64,
    pub last_committed_file_index: u64,
    pub last_committed_file_path: String,
    pub archive_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameStart {
    pub frame_no: u64,
    pub first_file_index: u64,
    pub first_file_path: String,
    pub level: i32,
    pub threads: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameChunk {
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameEnd {
    pub frame_no: u64,
    pub last_file_index: u64,
    pub last_file_path: String,
    pub compressed_len: u64,
    pub compressed_xxh3_64: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ack {
    pub frame_no: u64,
    pub last_file_index: u64,
    pub last_file_path: String,
    pub archive_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finish {
    pub final_frame_no: u64,
    pub final_file_index: u64,
    pub final_file_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameAbort {
    pub frame_no: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbortAck {
    pub frame_no: u64,
    pub archive_len: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IssueJournalReason {
    OpenFailed = 1,
    UnstableAfterRetries = 2,
}

impl IssueJournalReason {
    fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::OpenFailed),
            2 => Ok(Self::UnstableAfterRetries),
            _ => bail!("unknown issue journal reason: {value}"),
        }
    }

    const fn as_u8(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssueJournalEntry {
    pub file_index: u64,
    pub file_path: String,
    pub attempts: u32,
    pub reason: IssueJournalReason,
    pub last_error: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssueJournalChunk {
    pub entries: Vec<IssueJournalEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorMessage {
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Hello(Hello),
    HelloAck(HelloAck),
    ResumeQuery(ResumeQuery),
    ResumeState(ResumeState),
    FrameStart(FrameStart),
    FrameChunk(FrameChunk),
    FrameEnd(FrameEnd),
    Ack(Ack),
    Finish(Finish),
    FrameAbort(FrameAbort),
    AbortAck(AbortAck),
    IssueJournalChunk(IssueJournalChunk),
    FinishAck,
    Error(ErrorMessage),
}

impl Message {
    pub const fn kind(&self) -> MessageKind {
        match self {
            Self::Hello(_) => MessageKind::Hello,
            Self::HelloAck(_) => MessageKind::HelloAck,
            Self::ResumeQuery(_) => MessageKind::ResumeQuery,
            Self::ResumeState(_) => MessageKind::ResumeState,
            Self::FrameStart(_) => MessageKind::FrameStart,
            Self::FrameChunk(_) => MessageKind::FrameChunk,
            Self::FrameEnd(_) => MessageKind::FrameEnd,
            Self::Ack(_) => MessageKind::Ack,
            Self::Finish(_) => MessageKind::Finish,
            Self::FrameAbort(_) => MessageKind::FrameAbort,
            Self::AbortAck(_) => MessageKind::AbortAck,
            Self::IssueJournalChunk(_) => MessageKind::IssueJournalChunk,
            Self::FinishAck => MessageKind::FinishAck,
            Self::Error(_) => MessageKind::Error,
        }
    }
}

#[derive(Debug)]
struct Packet {
    kind: MessageKind,
    body: Vec<u8>,
}

struct BodyWriter {
    bytes: Vec<u8>,
}

impl BodyWriter {
    fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    fn write_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn write_u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn write_u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn write_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn write_i32(&mut self, value: i32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn write_string(&mut self, value: &str) -> Result<()> {
        let len = u32::try_from(value.len()).context("string field is too large")?;
        self.write_u32(len);
        self.bytes.extend_from_slice(value.as_bytes());
        Ok(())
    }

    fn write_path(&mut self, value: &str, field_name: &str) -> Result<()> {
        validate_relative_path(value, field_name)?;
        self.write_string(value)
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

struct BodyReader<'a> {
    cursor: Cursor<&'a [u8]>,
}

impl<'a> BodyReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            cursor: Cursor::new(bytes),
        }
    }

    fn read_u8(&mut self) -> Result<u8> {
        let mut buf = [0_u8; 1];
        self.cursor
            .read_exact(&mut buf)
            .context("failed to read u8 from message body")?;
        Ok(buf[0])
    }

    fn read_u16(&mut self) -> Result<u16> {
        let mut buf = [0_u8; 2];
        self.cursor
            .read_exact(&mut buf)
            .context("failed to read u16 from message body")?;
        Ok(u16::from_le_bytes(buf))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let mut buf = [0_u8; 4];
        self.cursor
            .read_exact(&mut buf)
            .context("failed to read u32 from message body")?;
        Ok(u32::from_le_bytes(buf))
    }

    fn read_u64(&mut self) -> Result<u64> {
        let mut buf = [0_u8; 8];
        self.cursor
            .read_exact(&mut buf)
            .context("failed to read u64 from message body")?;
        Ok(u64::from_le_bytes(buf))
    }

    fn read_i32(&mut self) -> Result<i32> {
        let mut buf = [0_u8; 4];
        self.cursor
            .read_exact(&mut buf)
            .context("failed to read i32 from message body")?;
        Ok(i32::from_le_bytes(buf))
    }

    fn read_string(&mut self) -> Result<String> {
        let len = self.read_u32()? as usize;
        let mut bytes = vec![0_u8; len];
        self.cursor
            .read_exact(&mut bytes)
            .context("failed to read string from message body")?;
        String::from_utf8(bytes).context("string field is not valid utf-8")
    }

    fn read_path(&mut self, field_name: &str) -> Result<String> {
        let value = self.read_string()?;
        validate_relative_path(&value, field_name)?;
        Ok(value)
    }

    fn finish(self) -> Result<()> {
        let consumed = self.cursor.position() as usize;
        let total = self.cursor.get_ref().len();
        if consumed != total {
            bail!(
                "message body contains unexpected trailing bytes: consumed {consumed} of {total}"
            );
        }
        Ok(())
    }
}

enum EncodedBody<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}

impl<'a> EncodedBody<'a> {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Owned(bytes) => bytes,
        }
    }
}

fn validate_relative_path(value: &str, field_name: &str) -> Result<()> {
    // Пустой путь разрешаем как sentinel там, где протоколу нужно уметь
    // описать состояние "ещё не было ни одного файла".
    if value.is_empty() {
        if matches!(field_name, "last_committed_file_path" | "final_file_path") {
            return Ok(());
        }
        bail!("{field_name} must not be empty");
    }
    if value.contains('\\') {
        bail!("{field_name} must use '/' separators");
    }
    if value.starts_with('/') {
        bail!("{field_name} must be relative");
    }
    for segment in value.split('/') {
        if segment.is_empty() {
            bail!("{field_name} must not contain empty path segments");
        }
        if matches!(segment, "." | "..") {
            bail!("{field_name} must not contain '.' or '..' segments");
        }
    }

    Ok(())
}

fn encode_message<'a>(message: &'a Message) -> Result<(MessageKind, EncodedBody<'a>)> {
    let kind = message.kind();
    let body = match message {
        Message::Hello(hello) => {
            let mut writer = BodyWriter::new();
            writer.write_u8(hello.role.as_u8());
            writer.write_u16(hello.protocol_version);
            writer.write_string(&hello.dump_id)?;
            writer.write_string(&hello.token)?;
            EncodedBody::Owned(writer.finish())
        }
        Message::HelloAck(hello_ack) => {
            let mut writer = BodyWriter::new();
            writer.write_u8(hello_ack.role.as_u8());
            EncodedBody::Owned(writer.finish())
        }
        Message::ResumeQuery(query) => {
            let mut writer = BodyWriter::new();
            writer.write_string(&query.dump_id)?;
            EncodedBody::Owned(writer.finish())
        }
        Message::ResumeState(state) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(state.last_committed_frame);
            writer.write_u64(state.last_committed_file_index);
            writer.write_path(&state.last_committed_file_path, "last_committed_file_path")?;
            writer.write_u64(state.archive_len);
            EncodedBody::Owned(writer.finish())
        }
        Message::FrameStart(frame_start) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(frame_start.frame_no);
            writer.write_u64(frame_start.first_file_index);
            writer.write_path(&frame_start.first_file_path, "first_file_path")?;
            writer.write_i32(frame_start.level);
            writer.write_u32(frame_start.threads);
            EncodedBody::Owned(writer.finish())
        }
        Message::FrameChunk(frame_chunk) => {
            if frame_chunk.bytes.len() > MAX_FRAME_CHUNK_BYTES {
                bail!(
                    "frame chunk is too large: {} > {} bytes",
                    frame_chunk.bytes.len(),
                    MAX_FRAME_CHUNK_BYTES
                );
            }
            EncodedBody::Borrowed(&frame_chunk.bytes)
        }
        Message::FrameEnd(frame_end) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(frame_end.frame_no);
            writer.write_u64(frame_end.last_file_index);
            writer.write_path(&frame_end.last_file_path, "last_file_path")?;
            writer.write_u64(frame_end.compressed_len);
            writer.write_u64(frame_end.compressed_xxh3_64);
            EncodedBody::Owned(writer.finish())
        }
        Message::Ack(ack) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(ack.frame_no);
            writer.write_u64(ack.last_file_index);
            writer.write_path(&ack.last_file_path, "last_file_path")?;
            writer.write_u64(ack.archive_len);
            EncodedBody::Owned(writer.finish())
        }
        Message::Finish(finish) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(finish.final_frame_no);
            writer.write_u64(finish.final_file_index);
            writer.write_path(&finish.final_file_path, "final_file_path")?;
            EncodedBody::Owned(writer.finish())
        }
        Message::FrameAbort(frame_abort) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(frame_abort.frame_no);
            EncodedBody::Owned(writer.finish())
        }
        Message::AbortAck(abort_ack) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(abort_ack.frame_no);
            writer.write_u64(abort_ack.archive_len);
            EncodedBody::Owned(writer.finish())
        }
        Message::IssueJournalChunk(chunk) => {
            let mut writer = BodyWriter::new();
            let entry_count =
                u32::try_from(chunk.entries.len()).context("too many issue journal entries")?;
            writer.write_u32(entry_count);
            for entry in &chunk.entries {
                writer.write_u64(entry.file_index);
                writer.write_path(&entry.file_path, "file_path")?;
                writer.write_u32(entry.attempts);
                writer.write_u8(entry.reason.as_u8());
                writer.write_string(&entry.last_error)?;
            }
            EncodedBody::Owned(writer.finish())
        }
        Message::FinishAck => EncodedBody::Owned(Vec::new()),
        Message::Error(error) => {
            let mut writer = BodyWriter::new();
            writer.write_string(&error.message)?;
            EncodedBody::Owned(writer.finish())
        }
    };

    Ok((kind, body))
}

fn decode_message(packet: Packet) -> Result<Message> {
    match packet.kind {
        MessageKind::Hello => {
            let mut reader = BodyReader::new(&packet.body);
            let role = Role::from_u8(reader.read_u8()?)?;
            let protocol_version = reader.read_u16()?;
            let dump_id = reader.read_string()?;
            let token = reader.read_string()?;
            reader.finish()?;
            Ok(Message::Hello(Hello {
                role,
                protocol_version,
                dump_id,
                token,
            }))
        }
        MessageKind::HelloAck => {
            let mut reader = BodyReader::new(&packet.body);
            let role = Role::from_u8(reader.read_u8()?)?;
            reader.finish()?;
            Ok(Message::HelloAck(HelloAck { role }))
        }
        MessageKind::ResumeQuery => {
            let mut reader = BodyReader::new(&packet.body);
            let dump_id = reader.read_string()?;
            reader.finish()?;
            Ok(Message::ResumeQuery(ResumeQuery { dump_id }))
        }
        MessageKind::ResumeState => {
            let mut reader = BodyReader::new(&packet.body);
            let last_committed_frame = reader.read_u64()?;
            let last_committed_file_index = reader.read_u64()?;
            let last_committed_file_path = reader.read_path("last_committed_file_path")?;
            let archive_len = reader.read_u64()?;
            reader.finish()?;
            Ok(Message::ResumeState(ResumeState {
                last_committed_frame,
                last_committed_file_index,
                last_committed_file_path,
                archive_len,
            }))
        }
        MessageKind::FrameStart => {
            let mut reader = BodyReader::new(&packet.body);
            let frame_no = reader.read_u64()?;
            let first_file_index = reader.read_u64()?;
            let first_file_path = reader.read_path("first_file_path")?;
            let level = reader.read_i32()?;
            let threads = reader.read_u32()?;
            reader.finish()?;
            Ok(Message::FrameStart(FrameStart {
                frame_no,
                first_file_index,
                first_file_path,
                level,
                threads,
            }))
        }
        MessageKind::FrameChunk => Ok(Message::FrameChunk(FrameChunk { bytes: packet.body })),
        MessageKind::FrameEnd => {
            let mut reader = BodyReader::new(&packet.body);
            let frame_no = reader.read_u64()?;
            let last_file_index = reader.read_u64()?;
            let last_file_path = reader.read_path("last_file_path")?;
            let compressed_len = reader.read_u64()?;
            let compressed_xxh3_64 = reader.read_u64()?;
            reader.finish()?;
            Ok(Message::FrameEnd(FrameEnd {
                frame_no,
                last_file_index,
                last_file_path,
                compressed_len,
                compressed_xxh3_64,
            }))
        }
        MessageKind::Ack => {
            let mut reader = BodyReader::new(&packet.body);
            let frame_no = reader.read_u64()?;
            let last_file_index = reader.read_u64()?;
            let last_file_path = reader.read_path("last_file_path")?;
            let archive_len = reader.read_u64()?;
            reader.finish()?;
            Ok(Message::Ack(Ack {
                frame_no,
                last_file_index,
                last_file_path,
                archive_len,
            }))
        }
        MessageKind::Finish => {
            let mut reader = BodyReader::new(&packet.body);
            let final_frame_no = reader.read_u64()?;
            let final_file_index = reader.read_u64()?;
            let final_file_path = reader.read_path("final_file_path")?;
            reader.finish()?;
            Ok(Message::Finish(Finish {
                final_frame_no,
                final_file_index,
                final_file_path,
            }))
        }
        MessageKind::FrameAbort => {
            let mut reader = BodyReader::new(&packet.body);
            let frame_no = reader.read_u64()?;
            reader.finish()?;
            Ok(Message::FrameAbort(FrameAbort { frame_no }))
        }
        MessageKind::AbortAck => {
            let mut reader = BodyReader::new(&packet.body);
            let frame_no = reader.read_u64()?;
            let archive_len = reader.read_u64()?;
            reader.finish()?;
            Ok(Message::AbortAck(AbortAck {
                frame_no,
                archive_len,
            }))
        }
        MessageKind::IssueJournalChunk => {
            let mut reader = BodyReader::new(&packet.body);
            let entry_count = reader.read_u32()? as usize;
            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                entries.push(IssueJournalEntry {
                    file_index: reader.read_u64()?,
                    file_path: reader.read_path("file_path")?,
                    attempts: reader.read_u32()?,
                    reason: IssueJournalReason::from_u8(reader.read_u8()?)?,
                    last_error: reader.read_string()?,
                });
            }
            reader.finish()?;
            Ok(Message::IssueJournalChunk(IssueJournalChunk { entries }))
        }
        MessageKind::FinishAck => {
            let reader = BodyReader::new(&packet.body);
            reader.finish()?;
            Ok(Message::FinishAck)
        }
        MessageKind::Error => {
            let mut reader = BodyReader::new(&packet.body);
            let message = reader.read_string()?;
            reader.finish()?;
            Ok(Message::Error(ErrorMessage { message }))
        }
    }
}

pub fn write_message<T>(stream: &mut T, message: &Message) -> Result<()>
where
    T: Write,
{
    let (kind, body) = encode_message(message)?;
    write_packet(stream, kind, body.as_slice())
}

pub fn read_message<T>(stream: &mut T) -> Result<Message>
where
    T: Read,
{
    decode_message(read_packet(stream)?)
}

// Sender после transport-handshake первым делом узнаёт, с какого места
// receiver готов продолжать dump. Это отдельный шаг поверх уже открытого потока.
pub fn request_resume_state<T>(stream: &mut T, dump_id: &str) -> Result<ResumeState>
where
    T: Read + Write,
{
    write_message(
        stream,
        &Message::ResumeQuery(ResumeQuery {
            dump_id: dump_id.to_owned(),
        }),
    )?;
    stream
        .flush()
        .context("failed to flush resume query to peer")?;

    match read_message(stream)? {
        Message::ResumeState(state) => Ok(state),
        Message::Error(error) => bail!("peer rejected resume query: {}", error.message),
        unexpected => bail!(
            "unexpected message while waiting for resume state: {:?}",
            unexpected.kind()
        ),
    }
}

pub fn initiate_handshake<T>(
    stream: &mut T,
    local_role: Role,
    dump_id: &str,
    token: Option<&str>,
) -> Result<Role>
where
    T: Read + Write,
{
    write_message(
        stream,
        &Message::Hello(Hello {
            role: local_role,
            protocol_version: VERSION,
            dump_id: dump_id.to_owned(),
            token: token.unwrap_or_default().to_owned(),
        }),
    )?;
    stream.flush().context("failed to flush hello packet")?;

    match read_message(stream)? {
        Message::HelloAck(hello_ack) => {
            validate_peer_role(local_role, hello_ack.role)?;
            Ok(hello_ack.role)
        }
        Message::Error(error) => bail!("peer rejected handshake: {}", error.message),
        other => bail!("unexpected message during handshake: {:?}", other.kind()),
    }
}

pub fn respond_handshake<T>(
    stream: &mut T,
    local_role: Role,
    expected_token: Option<&str>,
) -> Result<Role>
where
    T: Read + Write,
{
    Ok(respond_handshake_with_hello(stream, local_role, expected_token)?.role)
}

// Receiver читает полноценный `Hello`, потому что дальше по стеку могут
// понадобиться не только роль peer-а, но и dump_id из bootstrap-фазы.
pub fn respond_handshake_with_hello<T>(
    stream: &mut T,
    local_role: Role,
    expected_token: Option<&str>,
) -> Result<Hello>
where
    T: Read + Write,
{
    match read_message(stream)? {
        Message::Hello(hello) => {
            validate_hello_protocol_version(hello.protocol_version)
                .context("invalid hello protocol version")?;

            if let Err(error) = validate_peer_role(local_role, hello.role) {
                let _ = send_error(stream, &error.to_string());
                return Err(error);
            }

            if let Err(error) = validate_hello_token(&hello.token, expected_token) {
                let _ = send_error(stream, &error.to_string());
                return Err(error);
            }

            write_message(stream, &Message::HelloAck(HelloAck { role: local_role }))?;
            stream.flush().context("failed to flush hello ack")?;
            Ok(hello)
        }
        Message::Error(error) => {
            bail!("peer reported an error before handshake: {}", error.message)
        }
        other => {
            let _ = send_error(stream, "expected hello packet");
            bail!("unexpected message before handshake: {:?}", other.kind())
        }
    }
}

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

fn validate_hello_protocol_version(protocol_version: u16) -> Result<()> {
    if protocol_version != VERSION {
        bail!(
            "hello protocol version mismatch: expected {} got {}",
            VERSION,
            protocol_version
        );
    }

    Ok(())
}

fn validate_hello_token(token: &str, expected_token: Option<&str>) -> Result<()> {
    let Some(expected_token) = expected_token else {
        return Ok(());
    };

    if token != expected_token {
        bail!("bad token");
    }

    Ok(())
}

fn send_error<T>(stream: &mut T, message: &str) -> Result<()>
where
    T: Write,
{
    write_message(
        stream,
        &Message::Error(ErrorMessage {
            message: message.to_owned(),
        }),
    )?;
    stream.flush().context("failed to flush error packet")
}

fn write_packet<T>(stream: &mut T, kind: MessageKind, body: &[u8]) -> Result<()>
where
    T: Write,
{
    let body_len = u32::try_from(body.len()).context("packet body too large")?;
    let max_body_len = kind.max_body_len();
    if body.len() > max_body_len {
        bail!(
            "packet body is too large for {:?}: {} > {} bytes",
            kind,
            body.len(),
            max_body_len
        );
    }

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

fn read_packet<T>(stream: &mut T) -> Result<Packet>
where
    T: Read,
{
    let mut header = [0_u8; 12];
    stream
        .read_exact(&mut header)
        .context("failed to read packet header")?;

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
    let max_body_len = kind.max_body_len();
    if body_len > max_body_len {
        bail!(
            "packet body is too large for {:?}: {} > {} bytes",
            kind,
            body_len,
            max_body_len
        );
    }

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
        AbortAck, Ack, ErrorMessage, Finish, FrameAbort, FrameChunk, FrameEnd, FrameStart, Hello,
        HelloAck, IssueJournalChunk, IssueJournalEntry, IssueJournalReason, MAGIC,
        MAX_CONTROL_BODY_BYTES, MAX_FRAME_CHUNK_BYTES, Message, MessageKind, ResumeQuery,
        ResumeState, VERSION, initiate_handshake, read_message, request_resume_state,
        respond_handshake, write_message, write_packet,
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

    fn encode_message_to_bytes(message: &Message) -> Vec<u8> {
        let mut bytes = Vec::new();
        write_message(&mut bytes, message).unwrap();
        bytes
    }

    fn decode_message_from_bytes(bytes: &[u8]) -> Message {
        read_message(&mut Cursor::new(bytes.to_vec())).unwrap()
    }

    #[test]
    fn message_roundtrip_covers_all_protocol_message_variants() {
        let messages = vec![
            Message::Hello(Hello {
                role: Role::Sender,
                protocol_version: VERSION,
                dump_id: "dump-01".to_owned(),
                token: "secret".to_owned(),
            }),
            Message::HelloAck(HelloAck {
                role: Role::Receiver,
            }),
            Message::ResumeQuery(ResumeQuery {
                dump_id: "dump-01".to_owned(),
            }),
            Message::ResumeState(ResumeState {
                last_committed_frame: 7,
                last_committed_file_index: 15,
                last_committed_file_path: "root/file.txt".to_owned(),
                archive_len: 4096,
            }),
            Message::FrameStart(FrameStart {
                frame_no: 8,
                first_file_index: 16,
                first_file_path: "root/next.bin".to_owned(),
                level: 3,
                threads: 4,
            }),
            Message::FrameChunk(FrameChunk {
                bytes: vec![1, 2, 3, 4, 5],
            }),
            Message::FrameEnd(FrameEnd {
                frame_no: 8,
                last_file_index: 16,
                last_file_path: "root/next.bin".to_owned(),
                compressed_len: 1024,
                compressed_xxh3_64: 0xDEADBEEF_u64,
            }),
            Message::Ack(Ack {
                frame_no: 8,
                last_file_index: 16,
                last_file_path: "root/next.bin".to_owned(),
                archive_len: 8192,
            }),
            Message::Finish(Finish {
                final_frame_no: 8,
                final_file_index: 16,
                final_file_path: "root/next.bin".to_owned(),
            }),
            Message::FrameAbort(FrameAbort { frame_no: 8 }),
            Message::AbortAck(AbortAck {
                frame_no: 8,
                archive_len: 6144,
            }),
            Message::IssueJournalChunk(IssueJournalChunk {
                entries: vec![IssueJournalEntry {
                    file_index: 17,
                    file_path: "root/problem.bin".to_owned(),
                    attempts: 3,
                    reason: IssueJournalReason::UnstableAfterRetries,
                    last_error: "short read".to_owned(),
                }],
            }),
            Message::FinishAck,
            Message::Error(ErrorMessage {
                message: "bad token".to_owned(),
            }),
        ];

        for message in messages {
            let decoded = decode_message_from_bytes(&encode_message_to_bytes(&message));
            assert_eq!(decoded, message);
        }
    }

    #[test]
    fn read_message_rejects_invalid_magic() {
        let mut bytes = encode_message_to_bytes(&Message::HelloAck(HelloAck {
            role: Role::Receiver,
        }));
        bytes[..4].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());

        let error = read_message(&mut Cursor::new(bytes)).unwrap_err();

        assert!(error.to_string().contains("invalid packet magic"));
    }

    #[test]
    fn read_message_rejects_unsupported_version() {
        let mut bytes = encode_message_to_bytes(&Message::HelloAck(HelloAck {
            role: Role::Receiver,
        }));
        bytes[4..6].copy_from_slice(&(VERSION + 1).to_le_bytes());

        let error = read_message(&mut Cursor::new(bytes)).unwrap_err();

        assert!(
            error
                .to_string()
                .contains(&format!("unsupported protocol version: {}", VERSION + 1))
        );
    }

    #[test]
    fn read_message_rejects_older_protocol_version() {
        let mut bytes = encode_message_to_bytes(&Message::HelloAck(HelloAck {
            role: Role::Receiver,
        }));
        let older_version = VERSION.saturating_sub(1);
        bytes[4..6].copy_from_slice(&older_version.to_le_bytes());

        let error = read_message(&mut Cursor::new(bytes)).unwrap_err();

        assert!(
            error
                .to_string()
                .contains(&format!("unsupported protocol version: {older_version}"))
        );
    }

    #[test]
    fn read_message_rejects_unknown_message_kind() {
        let mut bytes = encode_message_to_bytes(&Message::HelloAck(HelloAck {
            role: Role::Receiver,
        }));
        bytes[6..8].copy_from_slice(&99_u16.to_le_bytes());

        let error = read_message(&mut Cursor::new(bytes)).unwrap_err();

        assert!(error.to_string().contains("unknown message kind: 99"));
    }

    #[test]
    fn read_message_rejects_oversized_control_body_before_allocation() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC.to_le_bytes());
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        bytes.extend_from_slice(&(MessageKind::HelloAck as u16).to_le_bytes());
        bytes.extend_from_slice(&((MAX_CONTROL_BODY_BYTES + 1) as u32).to_le_bytes());

        let error = read_message(&mut Cursor::new(bytes)).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("packet body is too large for HelloAck")
        );
    }

    #[test]
    fn read_message_rejects_oversized_frame_chunk_before_allocation() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC.to_le_bytes());
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        bytes.extend_from_slice(&(MessageKind::FrameChunk as u16).to_le_bytes());
        bytes.extend_from_slice(&((MAX_FRAME_CHUNK_BYTES + 1) as u32).to_le_bytes());

        let error = read_message(&mut Cursor::new(bytes)).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("packet body is too large for FrameChunk")
        );
    }

    #[test]
    fn write_message_rejects_non_normalized_relative_path() {
        let cases = [
            ("dir\\file.bin", "must use '/' separators"),
            ("/dir/file.bin", "must be relative"),
            ("./file.bin", "must not contain '.' or '..' segments"),
            ("../file.bin", "must not contain '.' or '..' segments"),
            ("dir//file.bin", "must not contain empty path segments"),
            ("dir/../file.bin", "must not contain '.' or '..' segments"),
            ("dir/", "must not contain empty path segments"),
            ("", "must not be empty"),
        ];

        for (path, expected_error) in cases {
            let invalid = Message::FrameStart(FrameStart {
                frame_no: 1,
                first_file_index: 0,
                first_file_path: path.to_owned(),
                level: 3,
                threads: 2,
            });

            let error = write_message(&mut Vec::new(), &invalid).unwrap_err();
            assert!(
                error.to_string().contains(expected_error),
                "{path}: {error}"
            );
        }
    }

    #[test]
    fn write_message_rejects_oversized_frame_chunk() {
        let invalid = Message::FrameChunk(FrameChunk {
            bytes: vec![7; MAX_FRAME_CHUNK_BYTES + 1],
        });

        let error = write_message(&mut Vec::new(), &invalid).unwrap_err();

        assert!(error.to_string().contains("frame chunk is too large"));
    }

    #[test]
    fn read_message_rejects_trailing_bytes_in_fixed_layout_body() {
        let mut bytes = Vec::new();
        write_packet(
            &mut bytes,
            MessageKind::HelloAck,
            &[Role::Receiver.as_u8(), 0xFF],
        )
        .unwrap();

        let error = read_message(&mut Cursor::new(bytes)).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("message body contains unexpected trailing bytes")
        );
    }

    #[test]
    fn read_message_rejects_invalid_role_value() {
        let mut bytes = Vec::new();
        write_packet(&mut bytes, MessageKind::HelloAck, &[9]).unwrap();

        let error = read_message(&mut Cursor::new(bytes)).unwrap_err();

        assert!(error.to_string().contains("unknown role value: 9"));
    }

    #[test]
    fn read_message_rejects_non_normalized_relative_path_on_decode() {
        let cases = [
            ("dir\\file.bin", "must use '/' separators"),
            ("./file.bin", "must not contain '.' or '..' segments"),
            ("dir//file.bin", "must not contain empty path segments"),
            ("dir/../file.bin", "must not contain '.' or '..' segments"),
        ];

        for (path, expected_error) in cases {
            let mut body = Vec::new();
            body.extend_from_slice(&1_u64.to_le_bytes());
            body.extend_from_slice(&0_u64.to_le_bytes());
            body.extend_from_slice(&(path.len() as u32).to_le_bytes());
            body.extend_from_slice(path.as_bytes());
            body.extend_from_slice(&3_i32.to_le_bytes());
            body.extend_from_slice(&2_u32.to_le_bytes());

            let mut bytes = Vec::new();
            write_packet(&mut bytes, MessageKind::FrameStart, &body).unwrap();

            let error = read_message(&mut Cursor::new(bytes)).unwrap_err();
            assert!(
                error.to_string().contains(expected_error),
                "{path}: {error}"
            );
        }
    }

    #[test]
    fn resume_state_allows_empty_last_committed_file_path() {
        let message = Message::ResumeState(ResumeState {
            last_committed_frame: 0,
            last_committed_file_index: 0,
            last_committed_file_path: String::new(),
            archive_len: 0,
        });

        let decoded = decode_message_from_bytes(&encode_message_to_bytes(&message));

        assert_eq!(decoded, message);
    }

    #[test]
    fn finish_allows_empty_final_file_path() {
        let message = Message::Finish(Finish {
            final_frame_no: 0,
            final_file_index: 0,
            final_file_path: String::new(),
        });

        let decoded = decode_message_from_bytes(&encode_message_to_bytes(&message));

        assert_eq!(decoded, message);
    }

    #[test]
    fn initiate_handshake_sends_hello_and_accepts_ack() {
        let mut stream =
            ScriptedStream::new(encode_message_to_bytes(&Message::HelloAck(HelloAck {
                role: Role::Receiver,
            })));

        let remote_role =
            initiate_handshake(&mut stream, Role::Sender, "dump-01", Some("secret")).unwrap();

        assert_eq!(remote_role, Role::Receiver);

        let message = decode_message_from_bytes(&stream.written);
        assert_eq!(
            message,
            Message::Hello(Hello {
                role: Role::Sender,
                protocol_version: VERSION,
                dump_id: "dump-01".to_owned(),
                token: "secret".to_owned(),
            })
        );
    }

    #[test]
    fn initiate_handshake_rejects_same_role_ack() {
        let mut stream =
            ScriptedStream::new(encode_message_to_bytes(&Message::HelloAck(HelloAck {
                role: Role::Sender,
            })));

        let error = initiate_handshake(&mut stream, Role::Sender, "dump-01", None).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("handshake requires opposite roles")
        );
    }

    #[test]
    fn initiate_handshake_surfaces_peer_error_packet() {
        let mut stream =
            ScriptedStream::new(encode_message_to_bytes(&Message::Error(ErrorMessage {
                message: "bad token".to_owned(),
            })));

        let error = initiate_handshake(&mut stream, Role::Sender, "dump-01", None).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("peer rejected handshake: bad token")
        );
    }

    #[test]
    fn initiate_handshake_rejects_unexpected_packet_kind() {
        let mut stream = ScriptedStream::new(encode_message_to_bytes(&Message::ResumeState(
            ResumeState {
                last_committed_frame: 0,
                last_committed_file_index: 0,
                last_committed_file_path: String::new(),
                archive_len: 0,
            },
        )));

        let error = initiate_handshake(&mut stream, Role::Sender, "dump-01", None).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unexpected message during handshake")
        );
    }

    #[test]
    fn request_resume_state_sends_query_and_returns_resume_state() {
        let expected = ResumeState {
            last_committed_frame: 7,
            last_committed_file_index: 19,
            last_committed_file_path: "root/file.bin".to_owned(),
            archive_len: 16384,
        };
        let mut stream = ScriptedStream::new(encode_message_to_bytes(&Message::ResumeState(
            expected.clone(),
        )));

        let state = request_resume_state(&mut stream, "dump-01").unwrap();

        assert_eq!(state, expected);
        assert_eq!(
            decode_message_from_bytes(&stream.written),
            Message::ResumeQuery(ResumeQuery {
                dump_id: "dump-01".to_owned(),
            })
        );
    }

    #[test]
    fn request_resume_state_surfaces_peer_error_packet() {
        let mut stream =
            ScriptedStream::new(encode_message_to_bytes(&Message::Error(ErrorMessage {
                message: "unknown dump".to_owned(),
            })));

        let error = request_resume_state(&mut stream, "dump-404").unwrap_err();

        assert!(
            error
                .to_string()
                .contains("peer rejected resume query: unknown dump")
        );
        assert_eq!(
            decode_message_from_bytes(&stream.written),
            Message::ResumeQuery(ResumeQuery {
                dump_id: "dump-404".to_owned(),
            })
        );
    }

    #[test]
    fn respond_handshake_returns_remote_role_and_emits_ack() {
        let request = encode_message_to_bytes(&Message::Hello(Hello {
            role: Role::Sender,
            protocol_version: VERSION,
            dump_id: "dump-01".to_owned(),
            token: "secret".to_owned(),
        }));
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let remote_role = respond_handshake(&mut stream, Role::Receiver, Some("secret")).unwrap();
        let bytes = stream.into_inner();

        assert_eq!(remote_role, Role::Sender);

        let reply = decode_message_from_bytes(&bytes[request_len..]);
        assert_eq!(
            reply,
            Message::HelloAck(HelloAck {
                role: Role::Receiver,
            })
        );
    }

    #[test]
    fn respond_handshake_rejects_same_role_and_emits_error() {
        let request = encode_message_to_bytes(&Message::Hello(Hello {
            role: Role::Sender,
            protocol_version: VERSION,
            dump_id: String::new(),
            token: String::new(),
        }));
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = respond_handshake(&mut stream, Role::Sender, None).unwrap_err();
        let bytes = stream.into_inner();

        assert!(
            error
                .to_string()
                .contains("handshake requires opposite roles")
        );

        let reply = decode_message_from_bytes(&bytes[request_len..]);
        assert_eq!(
            reply,
            Message::Error(ErrorMessage {
                message: "handshake requires opposite roles, got local=sender remote=sender"
                    .to_owned(),
            })
        );
    }

    #[test]
    fn respond_handshake_rejects_body_protocol_version_mismatch() {
        let request = encode_message_to_bytes(&Message::Hello(Hello {
            role: Role::Sender,
            protocol_version: VERSION + 1,
            dump_id: String::new(),
            token: String::new(),
        }));
        let mut stream = Cursor::new(request);

        let error = respond_handshake(&mut stream, Role::Receiver, None).unwrap_err();

        assert!(error.to_string().contains("invalid hello protocol version"));
    }

    #[test]
    fn respond_handshake_rejects_older_body_protocol_version_mismatch() {
        let request = encode_message_to_bytes(&Message::Hello(Hello {
            role: Role::Sender,
            protocol_version: VERSION.saturating_sub(1),
            dump_id: String::new(),
            token: String::new(),
        }));
        let mut stream = Cursor::new(request);

        let error = respond_handshake(&mut stream, Role::Receiver, None).unwrap_err();

        assert!(error.to_string().contains("invalid hello protocol version"));
    }

    #[test]
    fn respond_handshake_rejects_bad_token_and_emits_error() {
        let request = encode_message_to_bytes(&Message::Hello(Hello {
            role: Role::Sender,
            protocol_version: VERSION,
            dump_id: "dump-01".to_owned(),
            token: "wrong".to_owned(),
        }));
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = respond_handshake(&mut stream, Role::Receiver, Some("secret")).unwrap_err();
        let bytes = stream.into_inner();

        assert!(error.to_string().contains("bad token"));
        assert_eq!(
            decode_message_from_bytes(&bytes[request_len..]),
            Message::Error(ErrorMessage {
                message: "bad token".to_owned(),
            })
        );
    }

    #[test]
    fn respond_handshake_surfaces_peer_error_packet() {
        let request = encode_message_to_bytes(&Message::Error(ErrorMessage {
            message: "bad token".to_owned(),
        }));
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = respond_handshake(&mut stream, Role::Receiver, None).unwrap_err();
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
        let request = encode_message_to_bytes(&Message::FinishAck);
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = respond_handshake(&mut stream, Role::Receiver, None).unwrap_err();
        let bytes = stream.into_inner();

        assert!(
            error
                .to_string()
                .contains("unexpected message before handshake")
        );

        let reply = decode_message_from_bytes(&bytes[request_len..]);
        assert_eq!(
            reply,
            Message::Error(ErrorMessage {
                message: "expected hello packet".to_owned(),
            })
        );
    }

    #[test]
    fn encoded_packets_keep_expected_header_constants() {
        let bytes = encode_message_to_bytes(&Message::HelloAck(HelloAck {
            role: Role::Receiver,
        }));

        assert_eq!(u32::from_le_bytes(bytes[0..4].try_into().unwrap()), MAGIC);
        assert_eq!(u16::from_le_bytes(bytes[4..6].try_into().unwrap()), VERSION);
        assert_eq!(
            u16::from_le_bytes(bytes[6..8].try_into().unwrap()),
            MessageKind::HelloAck as u16
        );
    }
}

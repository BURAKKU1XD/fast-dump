use std::io::{Cursor, Read, Write};

use anyhow::{Context, Result, bail};

use crate::role::Role;

// Этот модуль отвечает только за бинарный wire protocol поверх уже открытого потока.
// TCP-сокет и его параметры живут в transport-слое, а здесь остаются только
// message types, кодирование и декодирование.

// Фиксированные значения заголовка каждого пакета.
// По ним приёмник понимает, что читает именно наш протокол.
pub const MAGIC: u32 = 0x504D4446;

pub const VERSION: u16 = 3;
pub const MAX_FRAME_CHUNK_BYTES: usize = 1024 * 1024;
pub const MAX_MANIFEST_CHUNK_BYTES: usize = 1024 * 1024;
// ManifestChunk несёт поверх payload ещё два optional u64 и payload_len: u32.
// CLI/runtime knobs управляют именно payload-частью, поэтому нужен отдельный effective cap.
pub const MANIFEST_CHUNK_OVERHEAD_BYTES: usize =
    (std::mem::size_of::<u64>() * 2) + std::mem::size_of::<u32>();
pub const MAX_MANIFEST_CHUNK_PAYLOAD_BYTES: usize =
    MAX_MANIFEST_CHUNK_BYTES - MANIFEST_CHUNK_OVERHEAD_BYTES;
pub const MAX_CONTROL_BODY_BYTES: usize = 64 * 1024;
pub const MAX_UNACKED_MANIFEST_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_UNACKED_MANIFEST_RECORDS: u64 = 64 * 1024;

// Тип сообщения кодируется в заголовке пакета и остаётся стабильной частью wire format.
// V3 сохраняет тот же packet header, но фиксирует отдельный набор kinds,
// потому что после ResumeState поток делится на manifest-поток, data frame-ы
// по work_ordinal и typed issue tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum MessageKind {
    Hello = 1,
    HelloAck = 2,
    ResumeQuery = 3,
    ResumeState = 4,
    ManifestStart = 5,
    ManifestChunk = 6,
    ManifestAck = 7,
    ManifestSeal = 8,
    FrameStart = 9,
    FrameChunk = 10,
    FrameEnd = 11,
    Ack = 12,
    FrameAbort = 13,
    AbortAck = 14,
    IssueChunk = 15,
    Finish = 16,
    FinishAck = 17,
    Error = 18,
}

impl MessageKind {
    fn from_u16(value: u16) -> Result<Self> {
        match value {
            1 => Ok(Self::Hello),
            2 => Ok(Self::HelloAck),
            3 => Ok(Self::ResumeQuery),
            4 => Ok(Self::ResumeState),
            5 => Ok(Self::ManifestStart),
            6 => Ok(Self::ManifestChunk),
            7 => Ok(Self::ManifestAck),
            8 => Ok(Self::ManifestSeal),
            9 => Ok(Self::FrameStart),
            10 => Ok(Self::FrameChunk),
            11 => Ok(Self::FrameEnd),
            12 => Ok(Self::Ack),
            13 => Ok(Self::FrameAbort),
            14 => Ok(Self::AbortAck),
            15 => Ok(Self::IssueChunk),
            16 => Ok(Self::Finish),
            17 => Ok(Self::FinishAck),
            18 => Ok(Self::Error),
            _ => bail!("unknown v3 message kind: {value}"),
        }
    }
}

impl MessageKind {
    const fn max_body_len(self) -> usize {
        match self {
            Self::ManifestChunk | Self::FrameChunk => MAX_MANIFEST_CHUNK_BYTES,
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
pub struct FrameChunk {
    pub bytes: Vec<u8>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestBoundary {
    pub manifest_bytes_persisted: u64,
    pub last_record_seq: Option<u64>,
    pub last_work_ordinal: Option<u64>,
    pub sealed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveBoundary {
    pub last_committed_frame: u64,
    pub last_committed_work_ordinal: Option<u64>,
    pub archive_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeState {
    pub manifest: ManifestBoundary,
    pub archive: ArchiveBoundary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestStart {
    pub base_manifest: ManifestBoundary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestChunk {
    pub bytes: Vec<u8>,
    pub last_record_seq: Option<u64>,
    pub last_work_ordinal: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestAck {
    pub committed_manifest: ManifestBoundary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestSeal {
    pub final_record_seq: Option<u64>,
    pub final_work_ordinal: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameStart {
    pub frame_no: u64,
    pub first_work_ordinal: u64,
    pub level: i32,
    pub threads: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameEnd {
    pub frame_no: u64,
    pub last_work_ordinal: u64,
    pub compressed_len: u64,
    pub compressed_xxh3_64: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ack {
    pub frame_no: u64,
    pub last_work_ordinal: u64,
    pub archive_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finish {
    pub final_frame_no: u64,
    pub final_work_ordinal: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IssueScope {
    File = 1,
    DiscoveryEntry = 2,
    DiscoverySubtree = 3,
    SenderState = 4,
}

impl IssueScope {
    fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::File),
            2 => Ok(Self::DiscoveryEntry),
            3 => Ok(Self::DiscoverySubtree),
            4 => Ok(Self::SenderState),
            _ => bail!("unknown v3 issue scope: {value}"),
        }
    }

    const fn as_u8(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IssueCode {
    OpenFailed = 1,
    UnstableAfterRetries = 2,
    UnsupportedEntry = 3,
    UnreadableSymlinkTarget = 4,
    VirtualFilesystem = 5,
    Io = 6,
    SenderStateEvent = 7,
    ReadDirFailed = 8,
    SourceInvalidated = 9,
}

impl IssueCode {
    fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::OpenFailed),
            2 => Ok(Self::UnstableAfterRetries),
            3 => Ok(Self::UnsupportedEntry),
            4 => Ok(Self::UnreadableSymlinkTarget),
            5 => Ok(Self::VirtualFilesystem),
            6 => Ok(Self::Io),
            7 => Ok(Self::SenderStateEvent),
            8 => Ok(Self::ReadDirFailed),
            9 => Ok(Self::SourceInvalidated),
            _ => bail!("unknown v3 issue code: {value}"),
        }
    }

    const fn as_u8(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssueEntry {
    pub scope: IssueScope,
    pub record_seq: Option<u64>,
    pub work_ordinal: Option<u64>,
    pub path: String,
    pub attempts: u32,
    pub code: IssueCode,
    pub detail: String,
    pub context: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssueChunk {
    pub entries: Vec<IssueEntry>,
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
    ManifestStart(ManifestStart),
    ManifestChunk(ManifestChunk),
    ManifestAck(ManifestAck),
    ManifestSeal(ManifestSeal),
    FrameStart(FrameStart),
    FrameChunk(FrameChunk),
    FrameEnd(FrameEnd),
    Ack(Ack),
    FrameAbort(FrameAbort),
    AbortAck(AbortAck),
    IssueChunk(IssueChunk),
    Finish(Finish),
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
            Self::ManifestStart(_) => MessageKind::ManifestStart,
            Self::ManifestChunk(_) => MessageKind::ManifestChunk,
            Self::ManifestAck(_) => MessageKind::ManifestAck,
            Self::ManifestSeal(_) => MessageKind::ManifestSeal,
            Self::FrameStart(_) => MessageKind::FrameStart,
            Self::FrameChunk(_) => MessageKind::FrameChunk,
            Self::FrameEnd(_) => MessageKind::FrameEnd,
            Self::Ack(_) => MessageKind::Ack,
            Self::FrameAbort(_) => MessageKind::FrameAbort,
            Self::AbortAck(_) => MessageKind::AbortAck,
            Self::IssueChunk(_) => MessageKind::IssueChunk,
            Self::Finish(_) => MessageKind::Finish,
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

    fn write_optional_u64(&mut self, value: Option<u64>) {
        self.write_u64(value.unwrap_or(0));
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

    fn read_optional_u64(&mut self) -> Result<Option<u64>> {
        match self.read_u64()? {
            0 => Ok(None),
            value => Ok(Some(value)),
        }
    }

    fn read_i32(&mut self) -> Result<i32> {
        let mut buf = [0_u8; 4];
        self.cursor
            .read_exact(&mut buf)
            .context("failed to read i32 from message body")?;
        Ok(i32::from_le_bytes(buf))
    }

    fn read_bool(&mut self, field_name: &str) -> Result<bool> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => bail!("{field_name} must be encoded as 0 or 1, got {value}"),
        }
    }

    fn read_string(&mut self) -> Result<String> {
        let len = self.read_u32()? as usize;
        let mut bytes = vec![0_u8; len];
        self.cursor
            .read_exact(&mut bytes)
            .context("failed to read string from message body")?;
        String::from_utf8(bytes).context("string field is not valid utf-8")
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

fn validate_manifest_boundary(boundary: &ManifestBoundary, field_name: &str) -> Result<()> {
    if boundary.last_work_ordinal.is_some() && boundary.last_record_seq.is_none() {
        bail!("{field_name} must not carry work_ordinal without record_seq");
    }
    if boundary.manifest_bytes_persisted == 0
        && (boundary.last_record_seq.is_some() || boundary.last_work_ordinal.is_some())
    {
        bail!("{field_name} must not carry progress when manifest_bytes_persisted is zero");
    }
    if boundary.manifest_bytes_persisted == 0 && boundary.sealed {
        bail!("{field_name} must not be sealed when manifest_bytes_persisted is zero");
    }

    Ok(())
}

fn write_manifest_boundary(
    writer: &mut BodyWriter,
    boundary: &ManifestBoundary,
    field_name: &str,
) -> Result<()> {
    validate_manifest_boundary(boundary, field_name)?;
    writer.write_u64(boundary.manifest_bytes_persisted);
    writer.write_optional_u64(boundary.last_record_seq);
    writer.write_optional_u64(boundary.last_work_ordinal);
    writer.write_u8(u8::from(boundary.sealed));
    Ok(())
}

fn read_manifest_boundary(
    reader: &mut BodyReader<'_>,
    field_name: &str,
) -> Result<ManifestBoundary> {
    let boundary = ManifestBoundary {
        manifest_bytes_persisted: reader.read_u64()?,
        last_record_seq: reader.read_optional_u64()?,
        last_work_ordinal: reader.read_optional_u64()?,
        sealed: reader.read_bool(field_name)?,
    };
    validate_manifest_boundary(&boundary, field_name)?;
    Ok(boundary)
}

fn validate_manifest_chunk(chunk: &ManifestChunk) -> Result<()> {
    validate_manifest_chunk_fields(
        chunk.bytes.len(),
        chunk.last_record_seq,
        chunk.last_work_ordinal,
    )
}

fn validate_manifest_chunk_fields(
    payload_len: usize,
    last_record_seq: Option<u64>,
    last_work_ordinal: Option<u64>,
) -> Result<()> {
    if payload_len == 0 {
        bail!("manifest chunk must not be empty");
    }
    if payload_len > MAX_MANIFEST_CHUNK_PAYLOAD_BYTES {
        bail!(
            "manifest chunk is too large: {} > {} bytes",
            payload_len,
            MAX_MANIFEST_CHUNK_PAYLOAD_BYTES
        );
    }
    if last_work_ordinal.is_some() && last_record_seq.is_none() {
        bail!("manifest chunk must not carry work_ordinal without record_seq");
    }
    Ok(())
}

fn validate_manifest_seal(seal: &ManifestSeal) -> Result<()> {
    if seal.final_work_ordinal.is_some() && seal.final_record_seq.is_none() {
        bail!("manifest seal must not carry final_work_ordinal without final_record_seq");
    }
    Ok(())
}

fn validate_issue_entry(entry: &IssueEntry) -> Result<()> {
    match entry.scope {
        IssueScope::File => {
            if entry.work_ordinal.is_none() {
                bail!("v3 file issue must carry work_ordinal");
            }
            if entry.record_seq.is_some() {
                bail!("v3 file issue must not carry record_seq");
            }
            if entry.attempts == 0 {
                bail!("v3 file issue must carry non-zero attempts");
            }
        }
        IssueScope::DiscoveryEntry | IssueScope::DiscoverySubtree | IssueScope::SenderState => {
            if entry.work_ordinal.is_some() {
                bail!("v3 discovery issue must not carry work_ordinal");
            }
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
            write_manifest_boundary(&mut writer, &state.manifest, "manifest")?;
            writer.write_u64(state.archive.last_committed_frame);
            writer.write_optional_u64(state.archive.last_committed_work_ordinal);
            writer.write_u64(state.archive.archive_len);
            EncodedBody::Owned(writer.finish())
        }
        Message::ManifestStart(start) => {
            let mut writer = BodyWriter::new();
            write_manifest_boundary(&mut writer, &start.base_manifest, "base_manifest")?;
            EncodedBody::Owned(writer.finish())
        }
        Message::ManifestChunk(chunk) => {
            validate_manifest_chunk(chunk)?;
            let mut writer = BodyWriter::new();
            writer.write_optional_u64(chunk.last_record_seq);
            writer.write_optional_u64(chunk.last_work_ordinal);
            let payload_len =
                u32::try_from(chunk.bytes.len()).context("manifest chunk payload is too large")?;
            writer.write_u32(payload_len);
            let mut body = writer.finish();
            body.extend_from_slice(&chunk.bytes);
            EncodedBody::Owned(body)
        }
        Message::ManifestAck(ack) => {
            let mut writer = BodyWriter::new();
            write_manifest_boundary(&mut writer, &ack.committed_manifest, "committed_manifest")?;
            EncodedBody::Owned(writer.finish())
        }
        Message::ManifestSeal(seal) => {
            validate_manifest_seal(seal)?;
            let mut writer = BodyWriter::new();
            writer.write_optional_u64(seal.final_record_seq);
            writer.write_optional_u64(seal.final_work_ordinal);
            EncodedBody::Owned(writer.finish())
        }
        Message::FrameStart(frame_start) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(frame_start.frame_no);
            writer.write_u64(frame_start.first_work_ordinal);
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
            writer.write_u64(frame_end.last_work_ordinal);
            writer.write_u64(frame_end.compressed_len);
            writer.write_u64(frame_end.compressed_xxh3_64);
            EncodedBody::Owned(writer.finish())
        }
        Message::Ack(ack) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(ack.frame_no);
            writer.write_u64(ack.last_work_ordinal);
            writer.write_u64(ack.archive_len);
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
        Message::IssueChunk(chunk) => {
            let mut writer = BodyWriter::new();
            let entry_count =
                u32::try_from(chunk.entries.len()).context("too many v3 issue entries")?;
            writer.write_u32(entry_count);
            for entry in &chunk.entries {
                validate_issue_entry(entry)?;
                writer.write_u8(entry.scope.as_u8());
                writer.write_optional_u64(entry.record_seq);
                writer.write_optional_u64(entry.work_ordinal);
                writer.write_string(&entry.path)?;
                writer.write_u32(entry.attempts);
                writer.write_u8(entry.code.as_u8());
                writer.write_string(&entry.detail)?;
                writer.write_string(&entry.context)?;
            }
            EncodedBody::Owned(writer.finish())
        }
        Message::Finish(finish) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(finish.final_frame_no);
            writer.write_optional_u64(finish.final_work_ordinal);
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
            let manifest = read_manifest_boundary(&mut reader, "manifest")?;
            let archive = ArchiveBoundary {
                last_committed_frame: reader.read_u64()?,
                last_committed_work_ordinal: reader.read_optional_u64()?,
                archive_len: reader.read_u64()?,
            };
            reader.finish()?;
            Ok(Message::ResumeState(ResumeState { manifest, archive }))
        }
        MessageKind::ManifestStart => {
            let mut reader = BodyReader::new(&packet.body);
            let base_manifest = read_manifest_boundary(&mut reader, "base_manifest")?;
            reader.finish()?;
            Ok(Message::ManifestStart(ManifestStart { base_manifest }))
        }
        MessageKind::ManifestChunk => {
            let mut reader = BodyReader::new(&packet.body);
            let last_record_seq = reader.read_optional_u64()?;
            let last_work_ordinal = reader.read_optional_u64()?;
            let payload_len = reader.read_u32()? as usize;
            let mut bytes = vec![0_u8; payload_len];
            reader
                .cursor
                .read_exact(&mut bytes)
                .context("failed to read v3 manifest chunk payload")?;
            reader.finish()?;
            let chunk = ManifestChunk {
                bytes,
                last_record_seq,
                last_work_ordinal,
            };
            validate_manifest_chunk(&chunk)?;
            Ok(Message::ManifestChunk(chunk))
        }
        MessageKind::ManifestAck => {
            let mut reader = BodyReader::new(&packet.body);
            let committed_manifest = read_manifest_boundary(&mut reader, "committed_manifest")?;
            reader.finish()?;
            Ok(Message::ManifestAck(ManifestAck { committed_manifest }))
        }
        MessageKind::ManifestSeal => {
            let mut reader = BodyReader::new(&packet.body);
            let seal = ManifestSeal {
                final_record_seq: reader.read_optional_u64()?,
                final_work_ordinal: reader.read_optional_u64()?,
            };
            reader.finish()?;
            validate_manifest_seal(&seal)?;
            Ok(Message::ManifestSeal(seal))
        }
        MessageKind::FrameStart => {
            let mut reader = BodyReader::new(&packet.body);
            let frame = FrameStart {
                frame_no: reader.read_u64()?,
                first_work_ordinal: reader.read_u64()?,
                level: reader.read_i32()?,
                threads: reader.read_u32()?,
            };
            reader.finish()?;
            Ok(Message::FrameStart(frame))
        }
        MessageKind::FrameChunk => Ok(Message::FrameChunk(FrameChunk { bytes: packet.body })),
        MessageKind::FrameEnd => {
            let mut reader = BodyReader::new(&packet.body);
            let frame = FrameEnd {
                frame_no: reader.read_u64()?,
                last_work_ordinal: reader.read_u64()?,
                compressed_len: reader.read_u64()?,
                compressed_xxh3_64: reader.read_u64()?,
            };
            reader.finish()?;
            Ok(Message::FrameEnd(frame))
        }
        MessageKind::Ack => {
            let mut reader = BodyReader::new(&packet.body);
            let ack = Ack {
                frame_no: reader.read_u64()?,
                last_work_ordinal: reader.read_u64()?,
                archive_len: reader.read_u64()?,
            };
            reader.finish()?;
            Ok(Message::Ack(ack))
        }
        MessageKind::FrameAbort => {
            let mut reader = BodyReader::new(&packet.body);
            let frame_no = reader.read_u64()?;
            reader.finish()?;
            Ok(Message::FrameAbort(FrameAbort { frame_no }))
        }
        MessageKind::AbortAck => {
            let mut reader = BodyReader::new(&packet.body);
            let abort_ack = AbortAck {
                frame_no: reader.read_u64()?,
                archive_len: reader.read_u64()?,
            };
            reader.finish()?;
            Ok(Message::AbortAck(abort_ack))
        }
        MessageKind::IssueChunk => {
            let mut reader = BodyReader::new(&packet.body);
            let entry_count = reader.read_u32()? as usize;
            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                let entry = IssueEntry {
                    scope: IssueScope::from_u8(reader.read_u8()?)?,
                    record_seq: reader.read_optional_u64()?,
                    work_ordinal: reader.read_optional_u64()?,
                    path: reader.read_string()?,
                    attempts: reader.read_u32()?,
                    code: IssueCode::from_u8(reader.read_u8()?)?,
                    detail: reader.read_string()?,
                    context: reader.read_string()?,
                };
                validate_issue_entry(&entry)?;
                entries.push(entry);
            }
            reader.finish()?;
            Ok(Message::IssueChunk(IssueChunk { entries }))
        }
        MessageKind::Finish => {
            let mut reader = BodyReader::new(&packet.body);
            let finish = Finish {
                final_frame_no: reader.read_u64()?,
                final_work_ordinal: reader.read_optional_u64()?,
            };
            reader.finish()?;
            Ok(Message::Finish(finish))
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

pub fn write_manifest_chunk_message<T>(
    stream: &mut T,
    bytes: &[u8],
    last_record_seq: Option<u64>,
    last_work_ordinal: Option<u64>,
) -> Result<()>
where
    T: Write,
{
    validate_manifest_chunk_fields(bytes.len(), last_record_seq, last_work_ordinal)?;

    let mut prefix = BodyWriter::new();
    prefix.write_optional_u64(last_record_seq);
    prefix.write_optional_u64(last_work_ordinal);
    prefix.write_u32(u32::try_from(bytes.len()).context("manifest chunk payload is too large")?);
    let prefix = prefix.finish();

    write_packet_parts(stream, MessageKind::ManifestChunk, &prefix, bytes)
}

pub fn read_message<T>(stream: &mut T) -> Result<Message>
where
    T: Read,
{
    decode_message(read_packet(stream)?)
}

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
        .context("failed to flush v3 resume query to peer")?;

    match read_message(stream)? {
        Message::ResumeState(state) => Ok(state),
        Message::Error(error) => bail!("peer rejected v3 resume query: {}", error.message),
        unexpected => bail!(
            "unexpected v3 message while waiting for resume state: {:?}",
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

fn packet_header(kind: MessageKind, body_len: u32) -> [u8; 12] {
    let mut header = [0_u8; 12];
    header[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    header[4..6].copy_from_slice(&VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&(kind as u16).to_le_bytes());
    header[8..12].copy_from_slice(&body_len.to_le_bytes());
    header
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
    let header = packet_header(kind, body_len);

    stream
        .write_all(&header)
        .context("failed to write packet header")?;
    stream
        .write_all(body)
        .context("failed to write packet body")?;

    Ok(())
}

fn write_packet_parts<T>(
    stream: &mut T,
    kind: MessageKind,
    first: &[u8],
    second: &[u8],
) -> Result<()>
where
    T: Write,
{
    let body_len = first
        .len()
        .checked_add(second.len())
        .context("packet body too large")?;
    let body_len = u32::try_from(body_len).context("packet body too large")?;
    let max_body_len = kind.max_body_len();
    if body_len as usize > max_body_len {
        bail!(
            "packet body is too large for {:?}: {} > {} bytes",
            kind,
            body_len,
            max_body_len
        );
    }
    let header = packet_header(kind, body_len);

    stream
        .write_all(&header)
        .context("failed to write packet header")?;
    stream
        .write_all(first)
        .context("failed to write first packet body segment")?;
    stream
        .write_all(second)
        .context("failed to write second packet body segment")?;

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

use std::io::{Cursor, Read, Write};

use anyhow::{Context, Result, bail};

use crate::role::Role;

// Этот модуль отвечает только за бинарный wire protocol поверх уже открытого потока.
// TCP-сокет и его параметры живут в transport-слое, а здесь остаются только
// message types, кодирование и декодирование.

// Фиксированные значения заголовка каждого пакета.
// По ним приёмник понимает, что читает именно наш протокол.
pub const MAGIC: u32 = 0x504D4446;
pub const VERSION_V2: u16 = 2;
pub const VERSION_V3: u16 = 3;
pub const VERSION: u16 = VERSION_V2;
pub const MAX_FRAME_CHUNK_BYTES: usize = 1024 * 1024;
pub const MAX_MANIFEST_CHUNK_BYTES: usize = 1024 * 1024;
pub const MAX_CONTROL_BODY_BYTES: usize = 64 * 1024;
pub const MAX_UNACKED_MANIFEST_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_UNACKED_MANIFEST_RECORDS: u64 = 64 * 1024;

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

// V3 сохраняет тот же packet header, но фиксирует отдельный набор kinds,
// потому что после ResumeState поток делится на manifest-поток, data frame-ы
// по work_ordinal и typed issue tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum V3MessageKind {
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

impl V3MessageKind {
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
            Self::FrameChunk => MAX_FRAME_CHUNK_BYTES,
            Self::FinishAck => 0,
            _ => MAX_CONTROL_BODY_BYTES,
        }
    }
}

impl V3MessageKind {
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
pub struct ManifestBoundaryV3 {
    pub manifest_bytes_persisted: u64,
    pub last_record_seq: Option<u64>,
    pub last_work_ordinal: Option<u64>,
    pub sealed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveBoundaryV3 {
    pub last_committed_frame: u64,
    pub last_committed_work_ordinal: Option<u64>,
    pub archive_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeStateV3 {
    pub manifest: ManifestBoundaryV3,
    pub archive: ArchiveBoundaryV3,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestStart {
    pub base_manifest: ManifestBoundaryV3,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestChunk {
    pub bytes: Vec<u8>,
    pub last_record_seq: Option<u64>,
    pub last_work_ordinal: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestAck {
    pub committed_manifest: ManifestBoundaryV3,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestSeal {
    pub final_record_seq: Option<u64>,
    pub final_work_ordinal: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameStartV3 {
    pub frame_no: u64,
    pub first_work_ordinal: u64,
    pub level: i32,
    pub threads: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameEndV3 {
    pub frame_no: u64,
    pub last_work_ordinal: u64,
    pub compressed_len: u64,
    pub compressed_xxh3_64: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckV3 {
    pub frame_no: u64,
    pub last_work_ordinal: u64,
    pub archive_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinishV3 {
    pub final_frame_no: u64,
    pub final_work_ordinal: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IssueScopeV3 {
    File = 1,
    DiscoveryEntry = 2,
    DiscoverySubtree = 3,
    SenderState = 4,
}

impl IssueScopeV3 {
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
pub enum IssueCodeV3 {
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

impl IssueCodeV3 {
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
pub struct IssueEntryV3 {
    pub scope: IssueScopeV3,
    pub record_seq: Option<u64>,
    pub work_ordinal: Option<u64>,
    pub path: String,
    pub attempts: u32,
    pub code: IssueCodeV3,
    pub detail: String,
    pub context: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssueChunkV3 {
    pub entries: Vec<IssueEntryV3>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum V3Message {
    Hello(Hello),
    HelloAck(HelloAck),
    ResumeQuery(ResumeQuery),
    ResumeState(ResumeStateV3),
    ManifestStart(ManifestStart),
    ManifestChunk(ManifestChunk),
    ManifestAck(ManifestAck),
    ManifestSeal(ManifestSeal),
    FrameStart(FrameStartV3),
    FrameChunk(FrameChunk),
    FrameEnd(FrameEndV3),
    Ack(AckV3),
    FrameAbort(FrameAbort),
    AbortAck(AbortAck),
    IssueChunk(IssueChunkV3),
    Finish(FinishV3),
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

impl V3Message {
    pub const fn kind(&self) -> V3MessageKind {
        match self {
            Self::Hello(_) => V3MessageKind::Hello,
            Self::HelloAck(_) => V3MessageKind::HelloAck,
            Self::ResumeQuery(_) => V3MessageKind::ResumeQuery,
            Self::ResumeState(_) => V3MessageKind::ResumeState,
            Self::ManifestStart(_) => V3MessageKind::ManifestStart,
            Self::ManifestChunk(_) => V3MessageKind::ManifestChunk,
            Self::ManifestAck(_) => V3MessageKind::ManifestAck,
            Self::ManifestSeal(_) => V3MessageKind::ManifestSeal,
            Self::FrameStart(_) => V3MessageKind::FrameStart,
            Self::FrameChunk(_) => V3MessageKind::FrameChunk,
            Self::FrameEnd(_) => V3MessageKind::FrameEnd,
            Self::Ack(_) => V3MessageKind::Ack,
            Self::FrameAbort(_) => V3MessageKind::FrameAbort,
            Self::AbortAck(_) => V3MessageKind::AbortAck,
            Self::IssueChunk(_) => V3MessageKind::IssueChunk,
            Self::Finish(_) => V3MessageKind::Finish,
            Self::FinishAck => V3MessageKind::FinishAck,
            Self::Error(_) => V3MessageKind::Error,
        }
    }
}

#[derive(Debug)]
struct Packet {
    kind: MessageKind,
    body: Vec<u8>,
}

#[derive(Debug)]
struct PacketV3 {
    kind: V3MessageKind,
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

fn validate_manifest_boundary_v3(boundary: &ManifestBoundaryV3, field_name: &str) -> Result<()> {
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

fn write_manifest_boundary_v3(
    writer: &mut BodyWriter,
    boundary: &ManifestBoundaryV3,
    field_name: &str,
) -> Result<()> {
    validate_manifest_boundary_v3(boundary, field_name)?;
    writer.write_u64(boundary.manifest_bytes_persisted);
    writer.write_optional_u64(boundary.last_record_seq);
    writer.write_optional_u64(boundary.last_work_ordinal);
    writer.write_u8(u8::from(boundary.sealed));
    Ok(())
}

fn read_manifest_boundary_v3(
    reader: &mut BodyReader<'_>,
    field_name: &str,
) -> Result<ManifestBoundaryV3> {
    let boundary = ManifestBoundaryV3 {
        manifest_bytes_persisted: reader.read_u64()?,
        last_record_seq: reader.read_optional_u64()?,
        last_work_ordinal: reader.read_optional_u64()?,
        sealed: reader.read_bool(field_name)?,
    };
    validate_manifest_boundary_v3(&boundary, field_name)?;
    Ok(boundary)
}

fn validate_manifest_chunk_v3(chunk: &ManifestChunk) -> Result<()> {
    if chunk.bytes.is_empty() {
        bail!("manifest chunk must not be empty");
    }
    if chunk.bytes.len() > MAX_MANIFEST_CHUNK_BYTES {
        bail!(
            "manifest chunk is too large: {} > {} bytes",
            chunk.bytes.len(),
            MAX_MANIFEST_CHUNK_BYTES
        );
    }
    if chunk.last_work_ordinal.is_some() && chunk.last_record_seq.is_none() {
        bail!("manifest chunk must not carry work_ordinal without record_seq");
    }
    Ok(())
}

fn validate_manifest_seal_v3(seal: &ManifestSeal) -> Result<()> {
    if seal.final_work_ordinal.is_some() && seal.final_record_seq.is_none() {
        bail!("manifest seal must not carry final_work_ordinal without final_record_seq");
    }
    Ok(())
}

fn validate_issue_entry_v3(entry: &IssueEntryV3) -> Result<()> {
    match entry.scope {
        IssueScopeV3::File => {
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
        IssueScopeV3::DiscoveryEntry
        | IssueScopeV3::DiscoverySubtree
        | IssueScopeV3::SenderState => {
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

fn encode_v3_message<'a>(message: &'a V3Message) -> Result<(V3MessageKind, EncodedBody<'a>)> {
    let kind = message.kind();
    let body = match message {
        V3Message::Hello(hello) => {
            let mut writer = BodyWriter::new();
            writer.write_u8(hello.role.as_u8());
            writer.write_u16(hello.protocol_version);
            writer.write_string(&hello.dump_id)?;
            writer.write_string(&hello.token)?;
            EncodedBody::Owned(writer.finish())
        }
        V3Message::HelloAck(hello_ack) => {
            let mut writer = BodyWriter::new();
            writer.write_u8(hello_ack.role.as_u8());
            EncodedBody::Owned(writer.finish())
        }
        V3Message::ResumeQuery(query) => {
            let mut writer = BodyWriter::new();
            writer.write_string(&query.dump_id)?;
            EncodedBody::Owned(writer.finish())
        }
        V3Message::ResumeState(state) => {
            let mut writer = BodyWriter::new();
            write_manifest_boundary_v3(&mut writer, &state.manifest, "manifest")?;
            writer.write_u64(state.archive.last_committed_frame);
            writer.write_optional_u64(state.archive.last_committed_work_ordinal);
            writer.write_u64(state.archive.archive_len);
            EncodedBody::Owned(writer.finish())
        }
        V3Message::ManifestStart(start) => {
            let mut writer = BodyWriter::new();
            write_manifest_boundary_v3(&mut writer, &start.base_manifest, "base_manifest")?;
            EncodedBody::Owned(writer.finish())
        }
        V3Message::ManifestChunk(chunk) => {
            validate_manifest_chunk_v3(chunk)?;
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
        V3Message::ManifestAck(ack) => {
            let mut writer = BodyWriter::new();
            write_manifest_boundary_v3(&mut writer, &ack.committed_manifest, "committed_manifest")?;
            EncodedBody::Owned(writer.finish())
        }
        V3Message::ManifestSeal(seal) => {
            validate_manifest_seal_v3(seal)?;
            let mut writer = BodyWriter::new();
            writer.write_optional_u64(seal.final_record_seq);
            writer.write_optional_u64(seal.final_work_ordinal);
            EncodedBody::Owned(writer.finish())
        }
        V3Message::FrameStart(frame_start) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(frame_start.frame_no);
            writer.write_u64(frame_start.first_work_ordinal);
            writer.write_i32(frame_start.level);
            writer.write_u32(frame_start.threads);
            EncodedBody::Owned(writer.finish())
        }
        V3Message::FrameChunk(frame_chunk) => {
            if frame_chunk.bytes.len() > MAX_FRAME_CHUNK_BYTES {
                bail!(
                    "frame chunk is too large: {} > {} bytes",
                    frame_chunk.bytes.len(),
                    MAX_FRAME_CHUNK_BYTES
                );
            }
            EncodedBody::Borrowed(&frame_chunk.bytes)
        }
        V3Message::FrameEnd(frame_end) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(frame_end.frame_no);
            writer.write_u64(frame_end.last_work_ordinal);
            writer.write_u64(frame_end.compressed_len);
            writer.write_u64(frame_end.compressed_xxh3_64);
            EncodedBody::Owned(writer.finish())
        }
        V3Message::Ack(ack) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(ack.frame_no);
            writer.write_u64(ack.last_work_ordinal);
            writer.write_u64(ack.archive_len);
            EncodedBody::Owned(writer.finish())
        }
        V3Message::FrameAbort(frame_abort) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(frame_abort.frame_no);
            EncodedBody::Owned(writer.finish())
        }
        V3Message::AbortAck(abort_ack) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(abort_ack.frame_no);
            writer.write_u64(abort_ack.archive_len);
            EncodedBody::Owned(writer.finish())
        }
        V3Message::IssueChunk(chunk) => {
            let mut writer = BodyWriter::new();
            let entry_count =
                u32::try_from(chunk.entries.len()).context("too many v3 issue entries")?;
            writer.write_u32(entry_count);
            for entry in &chunk.entries {
                validate_issue_entry_v3(entry)?;
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
        V3Message::Finish(finish) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(finish.final_frame_no);
            writer.write_optional_u64(finish.final_work_ordinal);
            EncodedBody::Owned(writer.finish())
        }
        V3Message::FinishAck => EncodedBody::Owned(Vec::new()),
        V3Message::Error(error) => {
            let mut writer = BodyWriter::new();
            writer.write_string(&error.message)?;
            EncodedBody::Owned(writer.finish())
        }
    };

    Ok((kind, body))
}

fn decode_v3_message(packet: PacketV3) -> Result<V3Message> {
    match packet.kind {
        V3MessageKind::Hello => {
            let mut reader = BodyReader::new(&packet.body);
            let role = Role::from_u8(reader.read_u8()?)?;
            let protocol_version = reader.read_u16()?;
            let dump_id = reader.read_string()?;
            let token = reader.read_string()?;
            reader.finish()?;
            Ok(V3Message::Hello(Hello {
                role,
                protocol_version,
                dump_id,
                token,
            }))
        }
        V3MessageKind::HelloAck => {
            let mut reader = BodyReader::new(&packet.body);
            let role = Role::from_u8(reader.read_u8()?)?;
            reader.finish()?;
            Ok(V3Message::HelloAck(HelloAck { role }))
        }
        V3MessageKind::ResumeQuery => {
            let mut reader = BodyReader::new(&packet.body);
            let dump_id = reader.read_string()?;
            reader.finish()?;
            Ok(V3Message::ResumeQuery(ResumeQuery { dump_id }))
        }
        V3MessageKind::ResumeState => {
            let mut reader = BodyReader::new(&packet.body);
            let manifest = read_manifest_boundary_v3(&mut reader, "manifest")?;
            let archive = ArchiveBoundaryV3 {
                last_committed_frame: reader.read_u64()?,
                last_committed_work_ordinal: reader.read_optional_u64()?,
                archive_len: reader.read_u64()?,
            };
            reader.finish()?;
            Ok(V3Message::ResumeState(ResumeStateV3 { manifest, archive }))
        }
        V3MessageKind::ManifestStart => {
            let mut reader = BodyReader::new(&packet.body);
            let base_manifest = read_manifest_boundary_v3(&mut reader, "base_manifest")?;
            reader.finish()?;
            Ok(V3Message::ManifestStart(ManifestStart { base_manifest }))
        }
        V3MessageKind::ManifestChunk => {
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
            validate_manifest_chunk_v3(&chunk)?;
            Ok(V3Message::ManifestChunk(chunk))
        }
        V3MessageKind::ManifestAck => {
            let mut reader = BodyReader::new(&packet.body);
            let committed_manifest = read_manifest_boundary_v3(&mut reader, "committed_manifest")?;
            reader.finish()?;
            Ok(V3Message::ManifestAck(ManifestAck { committed_manifest }))
        }
        V3MessageKind::ManifestSeal => {
            let mut reader = BodyReader::new(&packet.body);
            let seal = ManifestSeal {
                final_record_seq: reader.read_optional_u64()?,
                final_work_ordinal: reader.read_optional_u64()?,
            };
            reader.finish()?;
            validate_manifest_seal_v3(&seal)?;
            Ok(V3Message::ManifestSeal(seal))
        }
        V3MessageKind::FrameStart => {
            let mut reader = BodyReader::new(&packet.body);
            let frame = FrameStartV3 {
                frame_no: reader.read_u64()?,
                first_work_ordinal: reader.read_u64()?,
                level: reader.read_i32()?,
                threads: reader.read_u32()?,
            };
            reader.finish()?;
            Ok(V3Message::FrameStart(frame))
        }
        V3MessageKind::FrameChunk => Ok(V3Message::FrameChunk(FrameChunk { bytes: packet.body })),
        V3MessageKind::FrameEnd => {
            let mut reader = BodyReader::new(&packet.body);
            let frame = FrameEndV3 {
                frame_no: reader.read_u64()?,
                last_work_ordinal: reader.read_u64()?,
                compressed_len: reader.read_u64()?,
                compressed_xxh3_64: reader.read_u64()?,
            };
            reader.finish()?;
            Ok(V3Message::FrameEnd(frame))
        }
        V3MessageKind::Ack => {
            let mut reader = BodyReader::new(&packet.body);
            let ack = AckV3 {
                frame_no: reader.read_u64()?,
                last_work_ordinal: reader.read_u64()?,
                archive_len: reader.read_u64()?,
            };
            reader.finish()?;
            Ok(V3Message::Ack(ack))
        }
        V3MessageKind::FrameAbort => {
            let mut reader = BodyReader::new(&packet.body);
            let frame_no = reader.read_u64()?;
            reader.finish()?;
            Ok(V3Message::FrameAbort(FrameAbort { frame_no }))
        }
        V3MessageKind::AbortAck => {
            let mut reader = BodyReader::new(&packet.body);
            let abort_ack = AbortAck {
                frame_no: reader.read_u64()?,
                archive_len: reader.read_u64()?,
            };
            reader.finish()?;
            Ok(V3Message::AbortAck(abort_ack))
        }
        V3MessageKind::IssueChunk => {
            let mut reader = BodyReader::new(&packet.body);
            let entry_count = reader.read_u32()? as usize;
            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                let entry = IssueEntryV3 {
                    scope: IssueScopeV3::from_u8(reader.read_u8()?)?,
                    record_seq: reader.read_optional_u64()?,
                    work_ordinal: reader.read_optional_u64()?,
                    path: reader.read_string()?,
                    attempts: reader.read_u32()?,
                    code: IssueCodeV3::from_u8(reader.read_u8()?)?,
                    detail: reader.read_string()?,
                    context: reader.read_string()?,
                };
                validate_issue_entry_v3(&entry)?;
                entries.push(entry);
            }
            reader.finish()?;
            Ok(V3Message::IssueChunk(IssueChunkV3 { entries }))
        }
        V3MessageKind::Finish => {
            let mut reader = BodyReader::new(&packet.body);
            let finish = FinishV3 {
                final_frame_no: reader.read_u64()?,
                final_work_ordinal: reader.read_optional_u64()?,
            };
            reader.finish()?;
            Ok(V3Message::Finish(finish))
        }
        V3MessageKind::FinishAck => {
            let reader = BodyReader::new(&packet.body);
            reader.finish()?;
            Ok(V3Message::FinishAck)
        }
        V3MessageKind::Error => {
            let mut reader = BodyReader::new(&packet.body);
            let message = reader.read_string()?;
            reader.finish()?;
            Ok(V3Message::Error(ErrorMessage { message }))
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

pub fn write_message_v3<T>(stream: &mut T, message: &V3Message) -> Result<()>
where
    T: Write,
{
    let (kind, body) = encode_v3_message(message)?;
    write_packet_v3(stream, kind, body.as_slice())
}

pub fn read_message_v3<T>(stream: &mut T) -> Result<V3Message>
where
    T: Read,
{
    decode_v3_message(read_packet_v3(stream)?)
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

pub fn request_resume_state_v3<T>(stream: &mut T, dump_id: &str) -> Result<ResumeStateV3>
where
    T: Read + Write,
{
    write_message_v3(
        stream,
        &V3Message::ResumeQuery(ResumeQuery {
            dump_id: dump_id.to_owned(),
        }),
    )?;
    stream
        .flush()
        .context("failed to flush v3 resume query to peer")?;

    match read_message_v3(stream)? {
        V3Message::ResumeState(state) => Ok(state),
        V3Message::Error(error) => bail!("peer rejected v3 resume query: {}", error.message),
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

fn write_packet_v3<T>(stream: &mut T, kind: V3MessageKind, body: &[u8]) -> Result<()>
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
        .write_all(&VERSION_V3.to_le_bytes())
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

fn read_packet_v3<T>(stream: &mut T) -> Result<PacketV3>
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
    if version != VERSION_V3 {
        bail!("unsupported protocol version: {version}");
    }

    let kind = u16::from_le_bytes(header[6..8].try_into().unwrap());
    let kind = V3MessageKind::from_u16(kind)?;
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

    Ok(PacketV3 { kind, body })
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor, Read, Write};

    use super::{
        AbortAck, Ack, AckV3, ArchiveBoundaryV3, ErrorMessage, Finish, FinishV3, FrameAbort,
        FrameChunk, FrameEnd, FrameEndV3, FrameStart, FrameStartV3, Hello, HelloAck, IssueChunkV3,
        IssueCodeV3, IssueEntryV3, IssueJournalChunk, IssueJournalEntry, IssueJournalReason,
        IssueScopeV3, MAGIC, MAX_CONTROL_BODY_BYTES, MAX_FRAME_CHUNK_BYTES,
        MAX_MANIFEST_CHUNK_BYTES, ManifestAck, ManifestBoundaryV3, ManifestChunk, ManifestSeal,
        ManifestStart, Message, MessageKind, ResumeQuery, ResumeState, ResumeStateV3, V3Message,
        V3MessageKind, VERSION, VERSION_V3, initiate_handshake, read_message, read_message_v3,
        request_resume_state, request_resume_state_v3, respond_handshake, write_message,
        write_message_v3, write_packet, write_packet_v3,
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

    fn encode_v3_message_to_bytes(message: &V3Message) -> Vec<u8> {
        let mut bytes = Vec::new();
        write_message_v3(&mut bytes, message).unwrap();
        bytes
    }

    fn decode_v3_message_from_bytes(bytes: &[u8]) -> V3Message {
        read_message_v3(&mut Cursor::new(bytes.to_vec())).unwrap()
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
    fn v3_message_roundtrip_covers_all_protocol_message_variants() {
        let messages = vec![
            V3Message::Hello(Hello {
                role: Role::Sender,
                protocol_version: VERSION_V3,
                dump_id: "dump-03".to_owned(),
                token: "secret".to_owned(),
            }),
            V3Message::HelloAck(HelloAck {
                role: Role::Receiver,
            }),
            V3Message::ResumeQuery(ResumeQuery {
                dump_id: "dump-03".to_owned(),
            }),
            V3Message::ResumeState(ResumeStateV3 {
                manifest: ManifestBoundaryV3 {
                    manifest_bytes_persisted: 4096,
                    last_record_seq: Some(44),
                    last_work_ordinal: Some(21),
                    sealed: false,
                },
                archive: ArchiveBoundaryV3 {
                    last_committed_frame: 7,
                    last_committed_work_ordinal: Some(18),
                    archive_len: 8192,
                },
            }),
            V3Message::ManifestStart(ManifestStart {
                base_manifest: ManifestBoundaryV3 {
                    manifest_bytes_persisted: 4096,
                    last_record_seq: Some(44),
                    last_work_ordinal: Some(21),
                    sealed: false,
                },
            }),
            V3Message::ManifestChunk(ManifestChunk {
                bytes: vec![7, 8, 9, 10],
                last_record_seq: Some(45),
                last_work_ordinal: Some(22),
            }),
            V3Message::ManifestAck(ManifestAck {
                committed_manifest: ManifestBoundaryV3 {
                    manifest_bytes_persisted: 8192,
                    last_record_seq: Some(45),
                    last_work_ordinal: Some(22),
                    sealed: false,
                },
            }),
            V3Message::ManifestSeal(ManifestSeal {
                final_record_seq: Some(45),
                final_work_ordinal: Some(22),
            }),
            V3Message::FrameStart(FrameStartV3 {
                frame_no: 8,
                first_work_ordinal: 19,
                level: 3,
                threads: 4,
            }),
            V3Message::FrameChunk(FrameChunk {
                bytes: vec![1, 2, 3, 4, 5],
            }),
            V3Message::FrameEnd(FrameEndV3 {
                frame_no: 8,
                last_work_ordinal: 22,
                compressed_len: 1024,
                compressed_xxh3_64: 0xDEADBEEF_u64,
            }),
            V3Message::Ack(AckV3 {
                frame_no: 8,
                last_work_ordinal: 22,
                archive_len: 8192,
            }),
            V3Message::FrameAbort(FrameAbort { frame_no: 8 }),
            V3Message::AbortAck(AbortAck {
                frame_no: 8,
                archive_len: 6144,
            }),
            V3Message::IssueChunk(IssueChunkV3 {
                entries: vec![
                    IssueEntryV3 {
                        scope: IssueScopeV3::File,
                        record_seq: None,
                        work_ordinal: Some(22),
                        path: "root/problem.bin".to_owned(),
                        attempts: 3,
                        code: IssueCodeV3::UnstableAfterRetries,
                        detail: "short read".to_owned(),
                        context: "retry budget exhausted".to_owned(),
                    },
                    IssueEntryV3 {
                        scope: IssueScopeV3::DiscoverySubtree,
                        record_seq: Some(45),
                        work_ordinal: None,
                        path: "root/proc".to_owned(),
                        attempts: 0,
                        code: IssueCodeV3::VirtualFilesystem,
                        detail: "procfs".to_owned(),
                        context: "skipped during manifest build".to_owned(),
                    },
                ],
            }),
            V3Message::Finish(FinishV3 {
                final_frame_no: 8,
                final_work_ordinal: Some(22),
            }),
            V3Message::FinishAck,
            V3Message::Error(ErrorMessage {
                message: "bad token".to_owned(),
            }),
        ];

        for message in messages {
            let decoded = decode_v3_message_from_bytes(&encode_v3_message_to_bytes(&message));
            assert_eq!(decoded, message);
        }
    }

    #[test]
    fn write_message_v3_rejects_oversized_manifest_chunk() {
        let invalid = V3Message::ManifestChunk(ManifestChunk {
            bytes: vec![0_u8; MAX_MANIFEST_CHUNK_BYTES + 1],
            last_record_seq: Some(9),
            last_work_ordinal: Some(4),
        });

        let error = write_message_v3(&mut Vec::new(), &invalid).unwrap_err();

        assert!(error.to_string().contains("manifest chunk is too large"));
    }

    #[test]
    fn write_message_v3_rejects_file_issue_without_work_ordinal() {
        let invalid = V3Message::IssueChunk(IssueChunkV3 {
            entries: vec![IssueEntryV3 {
                scope: IssueScopeV3::File,
                record_seq: None,
                work_ordinal: None,
                path: "root/problem.bin".to_owned(),
                attempts: 1,
                code: IssueCodeV3::OpenFailed,
                detail: "access denied".to_owned(),
                context: String::new(),
            }],
        });

        let error = write_message_v3(&mut Vec::new(), &invalid).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("file issue must carry work_ordinal")
        );
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
    fn request_resume_state_v3_sends_query_and_returns_resume_state() {
        let expected = ResumeStateV3 {
            manifest: ManifestBoundaryV3 {
                manifest_bytes_persisted: 8192,
                last_record_seq: Some(44),
                last_work_ordinal: Some(21),
                sealed: true,
            },
            archive: ArchiveBoundaryV3 {
                last_committed_frame: 7,
                last_committed_work_ordinal: Some(19),
                archive_len: 16384,
            },
        };
        let mut stream = ScriptedStream::new(encode_v3_message_to_bytes(&V3Message::ResumeState(
            expected.clone(),
        )));

        let state = request_resume_state_v3(&mut stream, "dump-v3").unwrap();

        assert_eq!(state, expected);
        assert_eq!(
            decode_v3_message_from_bytes(&stream.written),
            V3Message::ResumeQuery(ResumeQuery {
                dump_id: "dump-v3".to_owned(),
            })
        );
    }

    #[test]
    fn request_resume_state_v3_surfaces_peer_error_packet() {
        let mut stream = ScriptedStream::new(encode_v3_message_to_bytes(&V3Message::Error(
            ErrorMessage {
                message: "unknown dump".to_owned(),
            },
        )));

        let error = request_resume_state_v3(&mut stream, "missing-v3").unwrap_err();

        assert!(
            error
                .to_string()
                .contains("peer rejected v3 resume query: unknown dump")
        );
        assert_eq!(
            decode_v3_message_from_bytes(&stream.written),
            V3Message::ResumeQuery(ResumeQuery {
                dump_id: "missing-v3".to_owned(),
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

    #[test]
    fn encoded_v3_packets_keep_expected_header_constants() {
        let bytes = encode_v3_message_to_bytes(&V3Message::HelloAck(HelloAck {
            role: Role::Receiver,
        }));

        assert_eq!(u32::from_le_bytes(bytes[0..4].try_into().unwrap()), MAGIC);
        assert_eq!(
            u16::from_le_bytes(bytes[4..6].try_into().unwrap()),
            VERSION_V3
        );
        assert_eq!(
            u16::from_le_bytes(bytes[6..8].try_into().unwrap()),
            V3MessageKind::HelloAck as u16
        );
    }

    #[test]
    fn read_message_v3_rejects_v2_packet_version() {
        let mut bytes = Vec::new();
        write_packet_v3(
            &mut bytes,
            V3MessageKind::HelloAck,
            &[Role::Receiver.as_u8()],
        )
        .unwrap();
        bytes[4..6].copy_from_slice(&VERSION.to_le_bytes());

        let error = read_message_v3(&mut Cursor::new(bytes)).unwrap_err();

        assert!(
            error
                .to_string()
                .contains(&format!("unsupported protocol version: {VERSION}"))
        );
    }
}

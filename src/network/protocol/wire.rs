use std::io::{self, Cursor, IoSlice, Read, Write};

use anyhow::{Context, Result, bail};

use crate::role::Role;

use super::{
    BundleCommitDescriptor, BundleEntryDescriptor, BundleEntryKind, BundlePlanDescriptor,
    ResumeState, SkipReason, SkippedEntry,
};

pub(crate) const MAGIC: u32 = 0x3456_4446;
pub(crate) const VERSION: u16 = 4;
pub(crate) const MAX_DATA_CHUNK_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_CONTROL_BODY_BYTES: usize = 4 * 1024 * 1024;
const DATA_HEADER_BYTES: usize = 13;
const MIN_SKIPPED_ENTRY_BYTES: usize = 8 + 1 + 4;
const MIN_BUNDLE_ENTRY_BYTES: usize = 8 + 4 + 1 + 8 + 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub(crate) enum ChannelKind {
    Control = 1,
    Data = 2,
}

impl ChannelKind {
    pub(crate) fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Control),
            2 => Ok(Self::Data),
            _ => bail!("unknown v4 channel kind: {value}"),
        }
    }

    pub(crate) const fn as_u8(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub(crate) enum MessageKind {
    Hello = 1,
    HelloAck = 2,
    ResumeQuery = 3,
    ResumeState = 4,
    SkipEntries = 5,
    SkipAck = 6,
    BundlePlan = 7,
    BundleData = 8,
    BundleCommit = 9,
    BundleAbort = 10,
    BundleAbortAck = 11,
    BundleAck = 12,
    Finish = 13,
    FinishAck = 14,
    Error = 15,
}

impl MessageKind {
    fn from_u16(value: u16) -> Result<Self> {
        match value {
            1 => Ok(Self::Hello),
            2 => Ok(Self::HelloAck),
            3 => Ok(Self::ResumeQuery),
            4 => Ok(Self::ResumeState),
            5 => Ok(Self::SkipEntries),
            6 => Ok(Self::SkipAck),
            7 => Ok(Self::BundlePlan),
            8 => Ok(Self::BundleData),
            9 => Ok(Self::BundleCommit),
            10 => Ok(Self::BundleAbort),
            11 => Ok(Self::BundleAbortAck),
            12 => Ok(Self::BundleAck),
            13 => Ok(Self::Finish),
            14 => Ok(Self::FinishAck),
            15 => Ok(Self::Error),
            _ => bail!("unknown v4 message kind: {value}"),
        }
    }

    const fn max_body_len(self) -> usize {
        match self {
            Self::BundleData => MAX_DATA_CHUNK_BYTES + DATA_HEADER_BYTES,
            _ => MAX_CONTROL_BODY_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Hello {
    pub(crate) role: Role,
    pub(crate) protocol_version: u16,
    pub(crate) dump_id: String,
    pub(crate) token: String,
    pub(crate) channel_kind: ChannelKind,
    pub(crate) session_nonce: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HelloAck {
    pub(crate) role: Role,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResumeQuery {
    pub(crate) dump_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SkipEntries {
    pub(crate) entries: Vec<SkippedEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SkipAck {
    pub(crate) total_terminal_skips: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BundlePlan {
    pub(crate) attempt_no: u32,
    pub(crate) descriptor: BundlePlanDescriptor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BundleData {
    pub(crate) bundle_no: u64,
    pub(crate) attempt_no: u32,
    pub(crate) is_last: bool,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BundleCommit {
    pub(crate) attempt_no: u32,
    pub(crate) descriptor: BundleCommitDescriptor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BundleAbort {
    pub(crate) bundle_no: u64,
    pub(crate) attempt_no: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BundleAbortAck {
    pub(crate) bundle_no: u64,
    pub(crate) attempt_no: u32,
    pub(crate) archive_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BundleAck {
    pub(crate) bundle_no: u64,
    pub(crate) last_work_ordinal: u64,
    pub(crate) archive_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Finish {
    pub(crate) final_bundle_no: u64,
    pub(crate) final_work_ordinal: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ErrorMessage {
    pub(crate) message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Message {
    Hello(Hello),
    HelloAck(HelloAck),
    ResumeQuery(ResumeQuery),
    ResumeState(ResumeState),
    SkipEntries(SkipEntries),
    SkipAck(SkipAck),
    BundlePlan(BundlePlan),
    BundleData(BundleData),
    BundleCommit(BundleCommit),
    BundleAbort(BundleAbort),
    BundleAbortAck(BundleAbortAck),
    BundleAck(BundleAck),
    Finish(Finish),
    FinishAck,
    Error(ErrorMessage),
}

impl Message {
    pub(crate) const fn kind(&self) -> MessageKind {
        match self {
            Self::Hello(_) => MessageKind::Hello,
            Self::HelloAck(_) => MessageKind::HelloAck,
            Self::ResumeQuery(_) => MessageKind::ResumeQuery,
            Self::ResumeState(_) => MessageKind::ResumeState,
            Self::SkipEntries(_) => MessageKind::SkipEntries,
            Self::SkipAck(_) => MessageKind::SkipAck,
            Self::BundlePlan(_) => MessageKind::BundlePlan,
            Self::BundleData(_) => MessageKind::BundleData,
            Self::BundleCommit(_) => MessageKind::BundleCommit,
            Self::BundleAbort(_) => MessageKind::BundleAbort,
            Self::BundleAbortAck(_) => MessageKind::BundleAbortAck,
            Self::BundleAck(_) => MessageKind::BundleAck,
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

    fn write_bool(&mut self, value: bool) {
        self.write_u8(u8::from(value));
    }

    fn write_optional_u64(&mut self, value: Option<u64>) {
        self.write_u64(value.unwrap_or(0));
    }

    fn write_string(&mut self, value: &str) -> Result<()> {
        let len = u32::try_from(value.len()).context("v4 string field is too large")?;
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
            .context("failed to read v4 u8 from message body")?;
        Ok(buf[0])
    }

    fn read_u16(&mut self) -> Result<u16> {
        let mut buf = [0_u8; 2];
        self.cursor
            .read_exact(&mut buf)
            .context("failed to read v4 u16 from message body")?;
        Ok(u16::from_le_bytes(buf))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let mut buf = [0_u8; 4];
        self.cursor
            .read_exact(&mut buf)
            .context("failed to read v4 u32 from message body")?;
        Ok(u32::from_le_bytes(buf))
    }

    fn read_u64(&mut self) -> Result<u64> {
        let mut buf = [0_u8; 8];
        self.cursor
            .read_exact(&mut buf)
            .context("failed to read v4 u64 from message body")?;
        Ok(u64::from_le_bytes(buf))
    }

    fn read_bool(&mut self, field_name: &str) -> Result<bool> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => bail!("{field_name} must be encoded as 0 or 1, got {value}"),
        }
    }

    fn read_optional_u64(&mut self) -> Result<Option<u64>> {
        match self.read_u64()? {
            0 => Ok(None),
            value => Ok(Some(value)),
        }
    }

    fn read_string(&mut self) -> Result<String> {
        let len = self.read_u32()? as usize;
        let start = self.cursor.position() as usize;
        let end = start
            .checked_add(len)
            .context("v4 string length overflow")?;
        // Внутренний length тоже недоверенный: лимит packet body сам по себе
        // не запрещает четырём байтам запросить гигабайты до проверки остатка.
        let bytes = self
            .cursor
            .get_ref()
            .get(start..end)
            .context("v4 string length exceeds remaining message body")?;
        let value = std::str::from_utf8(bytes)
            .context("v4 string field is not valid utf-8")?
            .to_owned();
        self.cursor.set_position(end as u64);
        Ok(value)
    }

    fn read_count(&mut self, minimum_entry_bytes: usize) -> Result<usize> {
        let count = self.read_u32()? as usize;
        let remaining = self.cursor.get_ref().len() - self.cursor.position() as usize;
        // Capacity ограничивается реально полученными байтами, ещё до выделения Vec.
        if count > remaining / minimum_entry_bytes {
            bail!(
                "v4 entry count exceeds remaining message body: {count} entries in {remaining} bytes"
            );
        }
        Ok(count)
    }

    fn finish(self) -> Result<()> {
        let consumed = self.cursor.position() as usize;
        let total = self.cursor.get_ref().len();
        if consumed != total {
            bail!(
                "v4 message body contains unexpected trailing bytes: consumed {consumed} of {total}"
            );
        }
        Ok(())
    }
}

fn validate_resume_state(state: &ResumeState) -> Result<()> {
    if state.last_committed_bundle_no == 0 && state.archive_len != 0 {
        bail!("v4 resume state must not carry archive_len without committed bundle");
    }
    if state.last_committed_bundle_no == 0 && state.last_committed_work_ordinal != 0 {
        bail!("v4 resume state must not carry committed work ordinal without bundle");
    }
    if state.last_committed_bundle_no == 0 && !state.last_committed_relative_path.is_empty() {
        bail!("v4 resume state must not carry committed path without bundle");
    }
    Ok(())
}

fn validate_skipped_entry(entry: &SkippedEntry) -> Result<()> {
    if entry.relative_path.is_empty() {
        bail!("v4 skipped entry path must not be empty");
    }
    Ok(())
}

fn validate_bundle_plan_descriptor(descriptor: &BundlePlanDescriptor) -> Result<()> {
    if descriptor.bundle_no == 0 {
        bail!("v4 bundle descriptor bundle_no must be at least 1");
    }
    if descriptor.entries.is_empty() {
        bail!("v4 bundle descriptor must contain at least one entry");
    }
    let first = descriptor.entries.first().unwrap();
    let last = descriptor.entries.last().unwrap();
    if descriptor.first_work_ordinal != first.work_ordinal {
        bail!("v4 bundle descriptor first_work_ordinal does not match first entry");
    }
    if descriptor.last_work_ordinal != last.work_ordinal {
        bail!("v4 bundle descriptor last_work_ordinal does not match last entry");
    }
    if descriptor.last_relative_path != last.relative_path {
        bail!("v4 bundle descriptor last_relative_path does not match last entry");
    }
    Ok(())
}

fn validate_bundle_commit_descriptor(descriptor: &BundleCommitDescriptor) -> Result<()> {
    if descriptor.bundle_no == 0 {
        bail!("v4 bundle commit bundle_no must be at least 1");
    }
    if descriptor.compressed_payload_bytes == 0 {
        bail!("v4 bundle commit compressed payload must be non-zero");
    }
    Ok(())
}

fn write_skip_reason(writer: &mut BodyWriter, reason: SkipReason) {
    let value = match reason {
        SkipReason::OpenFailed => 1,
        SkipReason::UnstableAfterRetries => 2,
        SkipReason::ReadSymlinkFailed => 3,
        SkipReason::UnsupportedEntry => 4,
    };
    writer.write_u8(value);
}

fn read_skip_reason(reader: &mut BodyReader<'_>) -> Result<SkipReason> {
    match reader.read_u8()? {
        1 => Ok(SkipReason::OpenFailed),
        2 => Ok(SkipReason::UnstableAfterRetries),
        3 => Ok(SkipReason::ReadSymlinkFailed),
        4 => Ok(SkipReason::UnsupportedEntry),
        value => bail!("unknown v4 skip reason: {value}"),
    }
}

fn write_skipped_entry(writer: &mut BodyWriter, entry: &SkippedEntry) -> Result<()> {
    validate_skipped_entry(entry)?;
    writer.write_u64(entry.work_ordinal);
    write_skip_reason(writer, entry.reason);
    writer.write_string(&entry.relative_path)?;
    Ok(())
}

fn read_skipped_entry(reader: &mut BodyReader<'_>) -> Result<SkippedEntry> {
    let entry = SkippedEntry {
        work_ordinal: reader.read_u64()?,
        reason: read_skip_reason(reader)?,
        relative_path: reader.read_string()?,
    };
    validate_skipped_entry(&entry)?;
    Ok(entry)
}

fn write_bundle_entry(writer: &mut BodyWriter, entry: &BundleEntryDescriptor) -> Result<()> {
    writer.write_u64(entry.work_ordinal);
    writer.write_string(&entry.relative_path)?;
    match &entry.kind {
        BundleEntryKind::Regular => writer.write_u8(1),
        BundleEntryKind::Symlink { target } => {
            writer.write_u8(2);
            writer.write_string(target)?;
        }
    }
    writer.write_u64(entry.size_at_open);
    writer.write_bool(entry.best_effort);
    Ok(())
}

fn read_bundle_entry(reader: &mut BodyReader<'_>) -> Result<BundleEntryDescriptor> {
    let work_ordinal = reader.read_u64()?;
    let relative_path = reader.read_string()?;
    let kind = match reader.read_u8()? {
        1 => BundleEntryKind::Regular,
        2 => BundleEntryKind::Symlink {
            target: reader.read_string()?,
        },
        value => bail!("unknown v4 bundle entry kind: {value}"),
    };
    Ok(BundleEntryDescriptor {
        work_ordinal,
        relative_path,
        kind,
        size_at_open: reader.read_u64()?,
        best_effort: reader.read_bool("best_effort")?,
    })
}

fn write_bundle_plan_descriptor(
    writer: &mut BodyWriter,
    descriptor: &BundlePlanDescriptor,
) -> Result<()> {
    validate_bundle_plan_descriptor(descriptor)?;
    writer.write_u64(descriptor.bundle_no);
    writer.write_u64(descriptor.first_work_ordinal);
    writer.write_u64(descriptor.last_work_ordinal);
    writer.write_string(&descriptor.last_relative_path)?;
    writer.write_u64(descriptor.raw_payload_bytes);
    writer.write_u32(
        u32::try_from(descriptor.entries.len()).context("too many v4 bundle descriptor entries")?,
    );
    for entry in &descriptor.entries {
        write_bundle_entry(writer, entry)?;
    }
    Ok(())
}

fn read_bundle_plan_descriptor(reader: &mut BodyReader<'_>) -> Result<BundlePlanDescriptor> {
    let descriptor = BundlePlanDescriptor {
        bundle_no: reader.read_u64()?,
        first_work_ordinal: reader.read_u64()?,
        last_work_ordinal: reader.read_u64()?,
        last_relative_path: reader.read_string()?,
        raw_payload_bytes: reader.read_u64()?,
        entries: {
            let count = reader.read_count(MIN_BUNDLE_ENTRY_BYTES)?;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                entries.push(read_bundle_entry(reader)?);
            }
            entries
        },
    };
    validate_bundle_plan_descriptor(&descriptor)?;
    Ok(descriptor)
}

fn write_bundle_commit_descriptor(
    writer: &mut BodyWriter,
    descriptor: BundleCommitDescriptor,
) -> Result<()> {
    validate_bundle_commit_descriptor(&descriptor)?;
    writer.write_u64(descriptor.bundle_no);
    writer.write_u64(descriptor.tar_payload_bytes);
    writer.write_u64(descriptor.compressed_payload_bytes);
    writer.write_u64(descriptor.compressed_xxh3_64);
    Ok(())
}

fn read_bundle_commit_descriptor(reader: &mut BodyReader<'_>) -> Result<BundleCommitDescriptor> {
    let descriptor = BundleCommitDescriptor {
        bundle_no: reader.read_u64()?,
        tar_payload_bytes: reader.read_u64()?,
        compressed_payload_bytes: reader.read_u64()?,
        compressed_xxh3_64: reader.read_u64()?,
    };
    validate_bundle_commit_descriptor(&descriptor)?;
    Ok(descriptor)
}

fn encode_message(message: &Message) -> Result<(MessageKind, Vec<u8>)> {
    let kind = message.kind();
    let body = match message {
        Message::Hello(hello) => {
            let mut writer = BodyWriter::new();
            writer.write_u8(hello.role.as_u8());
            writer.write_u16(hello.protocol_version);
            writer.write_string(&hello.dump_id)?;
            writer.write_string(&hello.token)?;
            writer.write_u8(hello.channel_kind.as_u8());
            writer.write_u64(hello.session_nonce);
            writer.finish()
        }
        Message::HelloAck(hello_ack) => {
            let mut writer = BodyWriter::new();
            writer.write_u8(hello_ack.role.as_u8());
            writer.finish()
        }
        Message::ResumeQuery(query) => {
            let mut writer = BodyWriter::new();
            writer.write_string(&query.dump_id)?;
            writer.finish()
        }
        Message::ResumeState(state) => {
            validate_resume_state(state)?;
            let mut writer = BodyWriter::new();
            writer.write_u64(state.last_committed_bundle_no);
            writer.write_u64(state.last_committed_work_ordinal);
            writer.write_string(&state.last_committed_relative_path)?;
            writer.write_u64(state.archive_len);
            writer.write_u32(
                u32::try_from(state.terminal_skips.len())
                    .context("too many v4 terminal skip entries")?,
            );
            for entry in &state.terminal_skips {
                write_skipped_entry(&mut writer, entry)?;
            }
            writer.finish()
        }
        Message::SkipEntries(chunk) => {
            let mut writer = BodyWriter::new();
            writer
                .write_u32(u32::try_from(chunk.entries.len()).context("too many v4 skip entries")?);
            for entry in &chunk.entries {
                write_skipped_entry(&mut writer, entry)?;
            }
            writer.finish()
        }
        Message::SkipAck(ack) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(ack.total_terminal_skips);
            writer.finish()
        }
        Message::BundlePlan(plan) => {
            let mut writer = BodyWriter::new();
            writer.write_u32(plan.attempt_no);
            write_bundle_plan_descriptor(&mut writer, &plan.descriptor)?;
            writer.finish()
        }
        Message::BundleData(data) => {
            if data.bytes.len() > MAX_DATA_CHUNK_BYTES {
                bail!(
                    "v4 bundle data chunk is too large: {} > {} bytes",
                    data.bytes.len(),
                    MAX_DATA_CHUNK_BYTES
                );
            }
            let mut writer = BodyWriter::new();
            writer.write_u64(data.bundle_no);
            writer.write_u32(data.attempt_no);
            writer.write_bool(data.is_last);
            writer.bytes.extend_from_slice(&data.bytes);
            writer.finish()
        }
        Message::BundleCommit(commit) => {
            let mut writer = BodyWriter::new();
            writer.write_u32(commit.attempt_no);
            write_bundle_commit_descriptor(&mut writer, commit.descriptor)?;
            writer.finish()
        }
        Message::BundleAbort(abort) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(abort.bundle_no);
            writer.write_u32(abort.attempt_no);
            writer.finish()
        }
        Message::BundleAbortAck(ack) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(ack.bundle_no);
            writer.write_u32(ack.attempt_no);
            writer.write_u64(ack.archive_len);
            writer.finish()
        }
        Message::BundleAck(ack) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(ack.bundle_no);
            writer.write_u64(ack.last_work_ordinal);
            writer.write_u64(ack.archive_len);
            writer.finish()
        }
        Message::Finish(finish) => {
            let mut writer = BodyWriter::new();
            writer.write_u64(finish.final_bundle_no);
            writer.write_optional_u64(finish.final_work_ordinal);
            writer.finish()
        }
        Message::FinishAck => Vec::new(),
        Message::Error(error) => {
            let mut writer = BodyWriter::new();
            writer.write_string(&error.message)?;
            writer.finish()
        }
    };
    Ok((kind, body))
}

fn decode_message(packet: Packet) -> Result<Message> {
    match packet.kind {
        MessageKind::Hello => {
            let mut reader = BodyReader::new(&packet.body);
            let hello = Hello {
                role: Role::from_u8(reader.read_u8()?)?,
                protocol_version: reader.read_u16()?,
                dump_id: reader.read_string()?,
                token: reader.read_string()?,
                channel_kind: ChannelKind::from_u8(reader.read_u8()?)?,
                session_nonce: reader.read_u64()?,
            };
            reader.finish()?;
            Ok(Message::Hello(hello))
        }
        MessageKind::HelloAck => {
            let mut reader = BodyReader::new(&packet.body);
            let hello_ack = HelloAck {
                role: Role::from_u8(reader.read_u8()?)?,
            };
            reader.finish()?;
            Ok(Message::HelloAck(hello_ack))
        }
        MessageKind::ResumeQuery => {
            let mut reader = BodyReader::new(&packet.body);
            let query = ResumeQuery {
                dump_id: reader.read_string()?,
            };
            reader.finish()?;
            Ok(Message::ResumeQuery(query))
        }
        MessageKind::ResumeState => {
            let mut reader = BodyReader::new(&packet.body);
            let state = ResumeState {
                last_committed_bundle_no: reader.read_u64()?,
                last_committed_work_ordinal: reader.read_u64()?,
                last_committed_relative_path: reader.read_string()?,
                archive_len: reader.read_u64()?,
                terminal_skips: {
                    let count = reader.read_count(MIN_SKIPPED_ENTRY_BYTES)?;
                    let mut entries = Vec::with_capacity(count);
                    for _ in 0..count {
                        entries.push(read_skipped_entry(&mut reader)?);
                    }
                    entries
                },
            };
            reader.finish()?;
            validate_resume_state(&state)?;
            Ok(Message::ResumeState(state))
        }
        MessageKind::SkipEntries => {
            let mut reader = BodyReader::new(&packet.body);
            let count = reader.read_count(MIN_SKIPPED_ENTRY_BYTES)?;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                entries.push(read_skipped_entry(&mut reader)?);
            }
            reader.finish()?;
            Ok(Message::SkipEntries(SkipEntries { entries }))
        }
        MessageKind::SkipAck => {
            let mut reader = BodyReader::new(&packet.body);
            let ack = SkipAck {
                total_terminal_skips: reader.read_u64()?,
            };
            reader.finish()?;
            Ok(Message::SkipAck(ack))
        }
        MessageKind::BundlePlan => {
            let mut reader = BodyReader::new(&packet.body);
            let attempt_no = reader.read_u32()?;
            let descriptor = read_bundle_plan_descriptor(&mut reader)?;
            reader.finish()?;
            Ok(Message::BundlePlan(BundlePlan {
                attempt_no,
                descriptor,
            }))
        }
        MessageKind::BundleData => {
            let mut reader = BodyReader::new(&packet.body);
            let bundle_no = reader.read_u64()?;
            let attempt_no = reader.read_u32()?;
            let is_last = reader.read_bool("is_last")?;
            let offset = reader.cursor.position() as usize;
            let bytes = packet.body[offset..].to_vec();
            reader.cursor.set_position(packet.body.len() as u64);
            reader.finish()?;
            Ok(Message::BundleData(BundleData {
                bundle_no,
                attempt_no,
                is_last,
                bytes,
            }))
        }
        MessageKind::BundleCommit => {
            let mut reader = BodyReader::new(&packet.body);
            let attempt_no = reader.read_u32()?;
            let descriptor = read_bundle_commit_descriptor(&mut reader)?;
            reader.finish()?;
            Ok(Message::BundleCommit(BundleCommit {
                attempt_no,
                descriptor,
            }))
        }
        MessageKind::BundleAbort => {
            let mut reader = BodyReader::new(&packet.body);
            let abort = BundleAbort {
                bundle_no: reader.read_u64()?,
                attempt_no: reader.read_u32()?,
            };
            reader.finish()?;
            Ok(Message::BundleAbort(abort))
        }
        MessageKind::BundleAbortAck => {
            let mut reader = BodyReader::new(&packet.body);
            let ack = BundleAbortAck {
                bundle_no: reader.read_u64()?,
                attempt_no: reader.read_u32()?,
                archive_len: reader.read_u64()?,
            };
            reader.finish()?;
            Ok(Message::BundleAbortAck(ack))
        }
        MessageKind::BundleAck => {
            let mut reader = BodyReader::new(&packet.body);
            let ack = BundleAck {
                bundle_no: reader.read_u64()?,
                last_work_ordinal: reader.read_u64()?,
                archive_len: reader.read_u64()?,
            };
            reader.finish()?;
            Ok(Message::BundleAck(ack))
        }
        MessageKind::Finish => {
            let mut reader = BodyReader::new(&packet.body);
            let finish = Finish {
                final_bundle_no: reader.read_u64()?,
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
            let error = ErrorMessage {
                message: reader.read_string()?,
            };
            reader.finish()?;
            Ok(Message::Error(error))
        }
    }
}

pub(crate) fn write_message<T>(stream: &mut T, message: &Message) -> Result<()>
where
    T: Write,
{
    let (kind, body) = encode_message(message)?;
    write_packet(stream, kind, &body)
}

pub(crate) fn write_bundle_data_message<T>(
    stream: &mut T,
    bundle_no: u64,
    attempt_no: u32,
    is_last: bool,
    bytes: &[u8],
) -> Result<()>
where
    T: Write,
{
    if bytes.len() > MAX_DATA_CHUNK_BYTES {
        bail!(
            "v4 bundle data chunk is too large: {} > {} bytes",
            bytes.len(),
            MAX_DATA_CHUNK_BYTES
        );
    }

    let body_len = bytes
        .len()
        .checked_add(DATA_HEADER_BYTES)
        .context("v4 bundle data packet body is too large")?;
    let body_len = u32::try_from(body_len).context("v4 bundle data packet body is too large")?;
    let mut header = [0_u8; 12 + DATA_HEADER_BYTES];
    header[..12].copy_from_slice(&packet_header(MessageKind::BundleData, body_len));
    header[12..20].copy_from_slice(&bundle_no.to_le_bytes());
    header[20..24].copy_from_slice(&attempt_no.to_le_bytes());
    header[24] = u8::from(is_last);
    write_header_and_body(stream, &header, bytes).context("failed to write v4 bundle data packet")
}

pub(crate) fn read_message<T>(stream: &mut T) -> Result<Message>
where
    T: Read,
{
    let (kind, body_len) = read_packet_header(stream)?;
    if kind == MessageKind::BundleData {
        return read_bundle_data_body(stream, body_len).map(Message::BundleData);
    }
    let mut body = vec![0_u8; body_len];
    stream
        .read_exact(&mut body)
        .context("failed to read v4 packet body")?;
    decode_message(Packet { kind, body })
}

fn read_bundle_data_body<T: Read>(stream: &mut T, body_len: usize) -> Result<BundleData> {
    let payload_len = body_len
        .checked_sub(DATA_HEADER_BYTES)
        .context("v4 bundle data body is shorter than its header")?;
    let mut header = [0_u8; DATA_HEADER_BYTES];
    stream
        .read_exact(&mut header)
        .context("failed to read v4 bundle data header")?;
    let mut reader = BodyReader::new(&header);
    let bundle_no = reader.read_u64()?;
    let attempt_no = reader.read_u32()?;
    let is_last = reader.read_bool("is_last")?;
    // Payload сразу попадает в итоговый буфер, без второго Vec и копирования packet body.
    let mut bytes = vec![0_u8; payload_len];
    stream
        .read_exact(&mut bytes)
        .context("failed to read v4 bundle data payload")?;
    Ok(BundleData {
        bundle_no,
        attempt_no,
        is_last,
        bytes,
    })
}

pub(crate) fn request_resume_state<T>(stream: &mut T, dump_id: &str) -> Result<ResumeState>
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
        .context("failed to flush v4 resume query to peer")?;

    match read_message(stream)? {
        Message::ResumeState(state) => Ok(state),
        Message::Error(error) => bail!("peer rejected v4 resume query: {}", error.message),
        unexpected => bail!(
            "unexpected v4 message while waiting for resume state: {:?}",
            unexpected.kind()
        ),
    }
}

pub(crate) fn initiate_handshake<T>(
    stream: &mut T,
    local_role: Role,
    dump_id: &str,
    token: Option<&str>,
    channel_kind: ChannelKind,
    session_nonce: u64,
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
            channel_kind,
            session_nonce,
        }),
    )?;
    stream.flush().context("failed to flush v4 hello packet")?;

    match read_message(stream)? {
        Message::HelloAck(hello_ack) => {
            validate_peer_role(local_role, hello_ack.role)?;
            Ok(hello_ack.role)
        }
        Message::Error(error) => bail!("peer rejected v4 handshake: {}", error.message),
        other => bail!("unexpected v4 message during handshake: {:?}", other.kind()),
    }
}

pub(crate) fn respond_handshake_with_hello<T>(
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
                .context("invalid v4 hello protocol version")?;

            if let Err(error) = validate_peer_role(local_role, hello.role) {
                let _ = send_error(stream, &error.to_string());
                return Err(error);
            }

            if let Err(error) = validate_hello_token(&hello.token, expected_token) {
                let _ = send_error(stream, &error.to_string());
                return Err(error);
            }

            if let Err(error) = super::validate_dump_id(&hello.dump_id) {
                let _ = send_error(stream, &error.to_string());
                return Err(error);
            }

            write_message(stream, &Message::HelloAck(HelloAck { role: local_role }))?;
            stream
                .flush()
                .context("failed to flush v4 hello ack packet")?;
            Ok(hello)
        }
        Message::Error(error) => {
            bail!("peer reported v4 error before handshake: {}", error.message)
        }
        other => {
            let _ = send_error(stream, "expected v4 hello packet");
            bail!("unexpected v4 message before handshake: {:?}", other.kind())
        }
    }
}

pub(crate) fn send_error<T>(stream: &mut T, message: &str) -> Result<()>
where
    T: Write,
{
    write_message(
        stream,
        &Message::Error(ErrorMessage {
            message: message.to_owned(),
        }),
    )?;
    stream.flush().context("failed to flush v4 error packet")
}

fn validate_peer_role(local_role: Role, remote_role: Role) -> Result<()> {
    if local_role == remote_role {
        bail!(
            "v4 handshake requires opposite roles, got local={} remote={}",
            local_role,
            remote_role
        );
    }
    Ok(())
}

fn validate_hello_protocol_version(protocol_version: u16) -> Result<()> {
    if protocol_version != VERSION {
        bail!(
            "v4 hello protocol version mismatch: expected {} got {}",
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
    let body_len = u32::try_from(body.len()).context("v4 packet body too large")?;
    let max_body_len = kind.max_body_len();
    if body.len() > max_body_len {
        bail!(
            "v4 packet body is too large for {:?}: {} > {} bytes",
            kind,
            body.len(),
            max_body_len
        );
    }

    let header = packet_header(kind, body_len);
    write_header_and_body(stream, &header, body).context("failed to write v4 packet")
}

fn write_header_and_body<T: Write>(stream: &mut T, header: &[u8], body: &[u8]) -> io::Result<()> {
    // TCP отправляет заголовок и payload одним syscall без объединяющего буфера.
    // Для обычного Write стандартный fallback продвигает первый непустой срез.
    let mut slices = [IoSlice::new(header), IoSlice::new(body)];
    let mut remaining = &mut slices[..];
    while !remaining.is_empty() {
        match stream.write_vectored(remaining) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => IoSlice::advance_slices(&mut remaining, written),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn read_packet_header<T>(stream: &mut T) -> Result<(MessageKind, usize)>
where
    T: Read,
{
    let mut header = [0_u8; 12];
    stream
        .read_exact(&mut header)
        .context("failed to read v4 packet header")?;

    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if magic != MAGIC {
        bail!("invalid v4 packet magic: {magic:#010x}");
    }

    let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
    if version != VERSION {
        bail!("unsupported v4 protocol version: {version}");
    }

    let kind = MessageKind::from_u16(u16::from_le_bytes(header[6..8].try_into().unwrap()))?;
    let body_len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
    let max_body_len = kind.max_body_len();
    if body_len > max_body_len {
        bail!(
            "v4 packet body is too large for {:?}: {} > {} bytes",
            kind,
            body_len,
            max_body_len
        );
    }

    Ok((kind, body_len))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    #[test]
    fn control_strings_reject_inner_lengths_beyond_received_body() {
        for length in [1, u32::MAX] {
            let body = length.to_le_bytes();
            for kind in [MessageKind::ResumeQuery, MessageKind::Error] {
                let mut packet = Vec::new();
                write_packet(&mut packet, kind, &body).unwrap();
                let error = read_message(&mut Cursor::new(packet)).unwrap_err();
                assert!(format!("{error:#}").contains("string length"));
            }
        }
        let mut writer = BodyWriter::new();
        writer.write_string("архив 🙂").unwrap();
        let body = writer.finish();
        let mut reader = BodyReader::new(&body);
        assert_eq!(reader.read_string().unwrap(), "архив 🙂");
        reader.finish().unwrap();
    }

    #[test]
    fn control_entry_counts_are_bounded_before_reserving_memory() {
        for count in [1, u32::MAX] {
            for kind in [
                MessageKind::SkipEntries,
                MessageKind::ResumeState,
                MessageKind::BundlePlan,
            ] {
                let mut body = BodyWriter::new();
                match kind {
                    MessageKind::ResumeState => {
                        body.write_u64(0);
                        body.write_u64(0);
                        body.write_string("").unwrap();
                        body.write_u64(0);
                    }
                    MessageKind::BundlePlan => {
                        body.write_u32(1);
                        body.write_u64(1);
                        body.write_u64(1);
                        body.write_u64(1);
                        body.write_string("file").unwrap();
                        body.write_u64(0);
                    }
                    _ => {}
                }
                body.write_u32(count);
                let mut packet = Vec::new();
                write_packet(&mut packet, kind, &body.finish()).unwrap();
                let error = read_message(&mut Cursor::new(packet)).unwrap_err();
                assert!(format!("{error:#}").contains("entry count exceeds"));
            }
        }
    }

    #[test]
    fn handshake_rejects_unsafe_dump_id_before_acknowledging() {
        struct Duplex {
            input: Cursor<Vec<u8>>,
            output: Vec<u8>,
        }
        impl Read for Duplex {
            fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
                self.input.read(bytes)
            }
        }
        impl Write for Duplex {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.output.write(bytes)
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        for dump_id in ["../outside", "dump:stream", "CON", ""] {
            let mut input = Vec::new();
            write_message(
                &mut input,
                &Message::Hello(Hello {
                    role: Role::Sender,
                    protocol_version: VERSION,
                    dump_id: dump_id.to_owned(),
                    token: String::new(),
                    channel_kind: ChannelKind::Control,
                    session_nonce: 1,
                }),
            )
            .unwrap();
            let mut duplex = Duplex {
                input: Cursor::new(input),
                output: Vec::new(),
            };
            assert!(respond_handshake_with_hello(&mut duplex, Role::Receiver, None).is_err());
            assert!(matches!(
                read_message(&mut Cursor::new(duplex.output)).unwrap(),
                Message::Error(_)
            ));
        }
    }

    #[derive(Clone, Copy)]
    enum WriteStep {
        Limit(usize),
        Fail(io::ErrorKind),
    }

    #[derive(Default)]
    struct ScriptedVectoredWriter {
        steps: VecDeque<WriteStep>,
        bytes: Vec<u8>,
        calls: usize,
    }

    impl Write for ScriptedVectoredWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            panic!("packet bypassed write_vectored");
        }

        fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
            self.calls += 1;
            let mut remaining = match self.steps.pop_front() {
                Some(WriteStep::Limit(limit)) => limit,
                Some(WriteStep::Fail(kind)) => return Err(kind.into()),
                None => usize::MAX,
            };
            let mut written = 0;
            for buf in bufs {
                let len = remaining.min(buf.len());
                self.bytes.extend_from_slice(&buf[..len]);
                remaining -= len;
                written += len;
            }
            Ok(written)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn reference_packet(message: &Message) -> Vec<u8> {
        let (kind, body) = encode_message(message).unwrap();
        let mut bytes = packet_header(kind, body.len() as u32).to_vec();
        bytes.extend_from_slice(&body);
        bytes
    }

    #[test]
    fn vectored_writes_preserve_partial_progress_across_header_and_payload() {
        let control = Message::ResumeQuery(ResumeQuery {
            dump_id: "all-files".into(),
        });
        let data = BundleData {
            bundle_no: 17,
            attempt_no: 3,
            is_last: true,
            bytes: vec![0xa5; 41],
        };
        for limit in [1, 11, 12, 13, 24, 25, 26, 29] {
            let steps = VecDeque::from([
                WriteStep::Fail(io::ErrorKind::Interrupted),
                WriteStep::Limit(limit),
                WriteStep::Fail(io::ErrorKind::Interrupted),
                WriteStep::Limit(limit),
            ]);
            let mut writer = ScriptedVectoredWriter {
                steps: steps.clone(),
                ..Default::default()
            };
            write_message(&mut writer, &control).unwrap();
            assert_eq!(writer.bytes, reference_packet(&control));

            let mut writer = ScriptedVectoredWriter {
                steps,
                ..Default::default()
            };
            write_bundle_data_message(
                &mut writer,
                data.bundle_no,
                data.attempt_no,
                data.is_last,
                &data.bytes,
            )
            .unwrap();
            assert_eq!(
                writer.bytes,
                reference_packet(&Message::BundleData(data.clone()))
            );
        }
    }

    #[test]
    fn vectored_writes_surface_write_zero_and_nonretryable_errors() {
        for step in [
            WriteStep::Limit(0),
            WriteStep::Fail(io::ErrorKind::BrokenPipe),
        ] {
            let expected_kind = match step {
                WriteStep::Limit(_) => io::ErrorKind::WriteZero,
                WriteStep::Fail(kind) => kind,
            };
            let mut writer = ScriptedVectoredWriter {
                steps: VecDeque::from([WriteStep::Limit(5), step]),
                ..Default::default()
            };
            let error = write_bundle_data_message(&mut writer, 1, 1, false, &[42; 32]).unwrap_err();
            assert_eq!(
                error.downcast_ref::<io::Error>().unwrap().kind(),
                expected_kind
            );
            assert_eq!(writer.bytes.len(), 5);
            assert_eq!(writer.calls, 2);
        }
    }

    #[test]
    fn vectored_transport_accepts_header_and_payload_in_one_call() {
        let control = Message::ResumeQuery(ResumeQuery {
            dump_id: "all-files".into(),
        });
        for message in [&control, &Message::FinishAck] {
            let mut writer = ScriptedVectoredWriter::default();
            write_message(&mut writer, message).unwrap();
            assert_eq!(writer.bytes, reference_packet(message));
            assert_eq!(writer.calls, 1);
        }
        for payload in [&[][..], &[42; 32][..]] {
            let mut writer = ScriptedVectoredWriter::default();
            write_bundle_data_message(&mut writer, 1, 1, true, payload).unwrap();
            assert_eq!(writer.calls, 1);
            assert_eq!(
                writer.bytes,
                reference_packet(&Message::BundleData(BundleData {
                    bundle_no: 1,
                    attempt_no: 1,
                    is_last: true,
                    bytes: payload.to_vec(),
                }))
            );
        }
    }

    #[test]
    fn scalar_write_fallback_preserves_complete_packet_bytes() {
        struct ScalarWriter(Vec<u8>);
        impl Write for ScalarWriter {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                let len = buf.len().min(7);
                self.0.extend_from_slice(&buf[..len]);
                Ok(len)
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut writer = ScalarWriter(Vec::new());
        write_bundle_data_message(&mut writer, 2, 3, true, &[0xab; 33]).unwrap();
        assert_eq!(
            writer.0,
            reference_packet(&Message::BundleData(BundleData {
                bundle_no: 2,
                attempt_no: 3,
                is_last: true,
                bytes: vec![0xab; 33],
            }))
        );
    }

    #[test]
    fn direct_bundle_data_codec_preserves_wire_bytes_at_payload_boundaries() {
        for size in [0, 1, 4097, MAX_DATA_CHUNK_BYTES] {
            for is_last in [false, true] {
                let data = BundleData {
                    bundle_no: u64::MAX - 1,
                    attempt_no: u32::MAX,
                    is_last,
                    bytes: (0..size).map(|n| n as u8).collect(),
                };
                let expected = Message::BundleData(data.clone());
                let mut generic = Vec::new();
                write_message(&mut generic, &expected).unwrap();
                let mut direct = Vec::new();
                write_bundle_data_message(
                    &mut direct,
                    data.bundle_no,
                    data.attempt_no,
                    data.is_last,
                    &data.bytes,
                )
                .unwrap();
                assert_eq!(direct, generic);
                assert_eq!(read_message(&mut Cursor::new(direct)).unwrap(), expected);
            }
        }
    }

    struct FragmentedReader(Cursor<Vec<u8>>);

    impl Read for FragmentedReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let len = buf.len().min(7);
            self.0.read(&mut buf[..len])
        }
    }

    #[test]
    fn direct_bundle_data_read_handles_fragmentation_and_next_packet() {
        let data = BundleData {
            bundle_no: 3,
            attempt_no: 2,
            is_last: true,
            bytes: vec![0xab; 1025],
        };
        let mut bytes = Vec::new();
        write_message(&mut bytes, &Message::BundleData(data.clone())).unwrap();
        write_message(&mut bytes, &Message::FinishAck).unwrap();
        let mut input = FragmentedReader(Cursor::new(bytes));
        assert_eq!(read_message(&mut input).unwrap(), Message::BundleData(data));
        assert_eq!(read_message(&mut input).unwrap(), Message::FinishAck);
    }

    #[test]
    fn direct_bundle_data_read_rejects_every_truncated_prefix() {
        let mut bytes = Vec::new();
        write_bundle_data_message(&mut bytes, 1, 1, true, &[0xa5; 32]).unwrap();
        for len in 0..bytes.len() {
            assert!(
                read_message(&mut Cursor::new(&bytes[..len])).is_err(),
                "length {len}"
            );
        }
    }

    #[test]
    fn direct_bundle_data_read_rejects_short_metadata_and_invalid_boolean() {
        for body_len in 0..DATA_HEADER_BYTES {
            let mut bytes = packet_header(MessageKind::BundleData, body_len as u32).to_vec();
            bytes.resize(12 + body_len, 0);
            assert!(read_message(&mut Cursor::new(bytes)).is_err());
        }
        for is_last in [2, 255] {
            let mut bytes = Vec::new();
            write_bundle_data_message(&mut bytes, 1, 1, false, &[]).unwrap();
            bytes[24] = is_last;
            assert!(read_message(&mut Cursor::new(bytes)).is_err());
        }
    }

    #[test]
    fn direct_bundle_data_read_retains_packet_header_validation() {
        let header = packet_header(MessageKind::BundleData, DATA_HEADER_BYTES as u32);
        for offset in [0, 4, 6] {
            let mut bytes = header;
            bytes[offset] = 0xff;
            assert!(read_message(&mut Cursor::new(bytes)).is_err());
        }
        let oversized = packet_header(
            MessageKind::BundleData,
            (MAX_DATA_CHUNK_BYTES + DATA_HEADER_BYTES + 1) as u32,
        );
        let error = read_message(&mut Cursor::new(oversized)).unwrap_err();
        assert!(error.to_string().contains("too large"));
    }
}

use super::messages::*;
use super::{MAX_DATA_CHUNK_BYTES, Packet};
use crate::network::protocol::{
    BundleCommitDescriptor, BundleEntryDescriptor, BundleEntryKind, BundlePlanDescriptor,
    ResumeState, SkipReason, SkippedEntry,
};
use crate::role::Role;
use anyhow::{Context, Result, bail};
use std::io::{Cursor, Read};

const MIN_SKIPPED_ENTRY_BYTES: usize = 8 + 1 + 4;
const MIN_BUNDLE_ENTRY_BYTES: usize = 8 + 4 + 1 + 8 + 1;

pub(super) struct BodyWriter {
    pub(super) bytes: Vec<u8>,
}

impl BodyWriter {
    pub(super) fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    pub(super) fn write_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub(super) fn write_u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(super) fn write_u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(super) fn write_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(super) fn write_bool(&mut self, value: bool) {
        self.write_u8(u8::from(value));
    }

    pub(super) fn write_optional_u64(&mut self, value: Option<u64>) {
        self.write_u64(value.unwrap_or(0));
    }

    pub(super) fn write_string(&mut self, value: &str) -> Result<()> {
        let len = u32::try_from(value.len()).context("v4 string field is too large")?;
        self.write_u32(len);
        self.bytes.extend_from_slice(value.as_bytes());
        Ok(())
    }

    pub(super) fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

pub(super) struct BodyReader<'a> {
    pub(super) cursor: Cursor<&'a [u8]>,
}

impl<'a> BodyReader<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self {
            cursor: Cursor::new(bytes),
        }
    }

    pub(super) fn read_u8(&mut self) -> Result<u8> {
        let mut buf = [0_u8; 1];
        self.cursor
            .read_exact(&mut buf)
            .context("failed to read v4 u8 from message body")?;
        Ok(buf[0])
    }

    pub(super) fn read_u16(&mut self) -> Result<u16> {
        let mut buf = [0_u8; 2];
        self.cursor
            .read_exact(&mut buf)
            .context("failed to read v4 u16 from message body")?;
        Ok(u16::from_le_bytes(buf))
    }

    pub(super) fn read_u32(&mut self) -> Result<u32> {
        let mut buf = [0_u8; 4];
        self.cursor
            .read_exact(&mut buf)
            .context("failed to read v4 u32 from message body")?;
        Ok(u32::from_le_bytes(buf))
    }

    pub(super) fn read_u64(&mut self) -> Result<u64> {
        let mut buf = [0_u8; 8];
        self.cursor
            .read_exact(&mut buf)
            .context("failed to read v4 u64 from message body")?;
        Ok(u64::from_le_bytes(buf))
    }

    pub(super) fn read_bool(&mut self, field_name: &str) -> Result<bool> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => bail!("{field_name} must be encoded as 0 or 1, got {value}"),
        }
    }

    pub(super) fn read_optional_u64(&mut self) -> Result<Option<u64>> {
        match self.read_u64()? {
            0 => Ok(None),
            value => Ok(Some(value)),
        }
    }

    pub(super) fn read_string(&mut self) -> Result<String> {
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

    pub(super) fn read_count(&mut self, minimum_entry_bytes: usize) -> Result<usize> {
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

    pub(super) fn finish(self) -> Result<()> {
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

pub(super) fn validate_resume_state(state: &ResumeState) -> Result<()> {
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

pub(super) fn validate_skipped_entry(entry: &SkippedEntry) -> Result<()> {
    if entry.relative_path.is_empty() {
        bail!("v4 skipped entry path must not be empty");
    }
    Ok(())
}

pub(super) fn validate_bundle_plan_descriptor(descriptor: &BundlePlanDescriptor) -> Result<()> {
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

pub(super) fn validate_bundle_commit_descriptor(descriptor: &BundleCommitDescriptor) -> Result<()> {
    if descriptor.bundle_no == 0 {
        bail!("v4 bundle commit bundle_no must be at least 1");
    }
    if descriptor.compressed_payload_bytes == 0 {
        bail!("v4 bundle commit compressed payload must be non-zero");
    }
    Ok(())
}

pub(super) fn write_skip_reason(writer: &mut BodyWriter, reason: SkipReason) {
    let value = match reason {
        SkipReason::OpenFailed => 1,
        SkipReason::UnstableAfterRetries => 2,
        SkipReason::ReadSymlinkFailed => 3,
        SkipReason::UnsupportedEntry => 4,
    };
    writer.write_u8(value);
}

pub(super) fn read_skip_reason(reader: &mut BodyReader<'_>) -> Result<SkipReason> {
    match reader.read_u8()? {
        1 => Ok(SkipReason::OpenFailed),
        2 => Ok(SkipReason::UnstableAfterRetries),
        3 => Ok(SkipReason::ReadSymlinkFailed),
        4 => Ok(SkipReason::UnsupportedEntry),
        value => bail!("unknown v4 skip reason: {value}"),
    }
}

pub(super) fn write_skipped_entry(writer: &mut BodyWriter, entry: &SkippedEntry) -> Result<()> {
    validate_skipped_entry(entry)?;
    writer.write_u64(entry.work_ordinal);
    write_skip_reason(writer, entry.reason);
    writer.write_string(&entry.relative_path)?;
    Ok(())
}

pub(super) fn read_skipped_entry(reader: &mut BodyReader<'_>) -> Result<SkippedEntry> {
    let entry = SkippedEntry {
        work_ordinal: reader.read_u64()?,
        reason: read_skip_reason(reader)?,
        relative_path: reader.read_string()?,
    };
    validate_skipped_entry(&entry)?;
    Ok(entry)
}

pub(super) fn write_bundle_entry(
    writer: &mut BodyWriter,
    entry: &BundleEntryDescriptor,
) -> Result<()> {
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

pub(super) fn read_bundle_entry(reader: &mut BodyReader<'_>) -> Result<BundleEntryDescriptor> {
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

pub(super) fn write_bundle_plan_descriptor(
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

pub(super) fn read_bundle_plan_descriptor(
    reader: &mut BodyReader<'_>,
) -> Result<BundlePlanDescriptor> {
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

pub(super) fn write_bundle_commit_descriptor(
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

pub(super) fn read_bundle_commit_descriptor(
    reader: &mut BodyReader<'_>,
) -> Result<BundleCommitDescriptor> {
    let descriptor = BundleCommitDescriptor {
        bundle_no: reader.read_u64()?,
        tar_payload_bytes: reader.read_u64()?,
        compressed_payload_bytes: reader.read_u64()?,
        compressed_xxh3_64: reader.read_u64()?,
    };
    validate_bundle_commit_descriptor(&descriptor)?;
    Ok(descriptor)
}

pub(super) fn encode_message(message: &Message) -> Result<(MessageKind, Vec<u8>)> {
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

pub(super) fn decode_message(packet: Packet) -> Result<Message> {
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

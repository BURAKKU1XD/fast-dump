use std::io::{Read, Seek, SeekFrom, Write};
use std::time::Instant;
use std::{fs::OpenOptions, path::Path};

use anyhow::{Context, Result, bail};
use log::debug;
use xxhash_rust::xxh3::Xxh3Default;

use crate::network::protocol::{
    self, AbortAck, Ack, AckV3, FrameEnd, FrameEndV3, FrameStart, FrameStartV3, Message, V3Message,
};

use super::ReceiverState;
use super::journal::{ReceiverJournal, recover_partial_archive};
use super::shared::{bytes_per_sec, send_protocol_error};

pub(crate) enum FrameReceiveResult {
    Committed(Ack),
    Aborted(AbortAck),
}

pub(crate) enum FrameReceiveResultV3 {
    Committed(AckV3),
    Aborted(AbortAck),
}

// Здесь receiver принимает один готовый zstd-frame и коммитит его целиком.
// Пока Ack не отправлен, journal не двигается, а `.part` можно безопасно
// откатить обратно к последнему committed archive_len.
pub fn receive_frame<T>(stream: &mut T, state: &mut ReceiverState) -> Result<Ack>
where
    T: Read + Write,
{
    let frame_start = match protocol::read_message(stream)? {
        Message::FrameStart(frame_start) => frame_start,
        Message::Error(error) => {
            bail!("peer aborted before frame transfer: {}", error.message);
        }
        unexpected => {
            send_protocol_error(stream, "expected frame start")?;
            bail!(
                "unexpected message before frame start: {:?}",
                unexpected.kind()
            );
        }
    };

    receive_frame_from_start(stream, state, frame_start)
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn receive_frame_v3<T>(stream: &mut T, state: &mut ReceiverState) -> Result<AckV3>
where
    T: Read + Write,
{
    let frame_start = match protocol::read_message_v3(stream)? {
        V3Message::FrameStart(frame_start) => frame_start,
        V3Message::Error(error) => {
            bail!("peer aborted before v3 frame transfer: {}", error.message);
        }
        unexpected => {
            protocol::write_message_v3(
                stream,
                &V3Message::Error(protocol::ErrorMessage {
                    message: "expected v3 frame start".to_owned(),
                }),
            )?;
            stream
                .flush()
                .context("failed to flush v3 frame-start error to peer")?;
            bail!(
                "unexpected v3 message before frame start: {:?}",
                unexpected.kind()
            );
        }
    };

    receive_frame_v3_from_start(stream, state, frame_start)
}

pub fn receive_frame_abort<T>(stream: &mut T, state: &mut ReceiverState) -> Result<AbortAck>
where
    T: Read + Write,
{
    let frame_start = match protocol::read_message(stream)? {
        Message::FrameStart(frame_start) => frame_start,
        Message::Error(error) => {
            bail!("peer aborted before frame transfer: {}", error.message);
        }
        unexpected => {
            send_protocol_error(stream, "expected frame start")?;
            bail!(
                "unexpected message before frame start: {:?}",
                unexpected.kind()
            );
        }
    };

    receive_frame_abort_from_start(stream, state, frame_start)
}

pub(crate) fn receive_frame_from_start<T>(
    stream: &mut T,
    state: &mut ReceiverState,
    frame_start: FrameStart,
) -> Result<Ack>
where
    T: Read + Write,
{
    match receive_frame_event_from_start(stream, state, frame_start)? {
        FrameReceiveResult::Committed(ack) => Ok(ack),
        FrameReceiveResult::Aborted(abort_ack) => bail!(
            "unexpected explicit frame abort while waiting for frame commit: frame_no={} archive_len={}",
            abort_ack.frame_no,
            abort_ack.archive_len
        ),
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn receive_frame_v3_from_start<T>(
    stream: &mut T,
    state: &mut ReceiverState,
    frame_start: FrameStartV3,
) -> Result<AckV3>
where
    T: Read + Write,
{
    match receive_frame_event_v3_from_start(stream, state, frame_start)? {
        FrameReceiveResultV3::Committed(ack) => Ok(ack),
        FrameReceiveResultV3::Aborted(abort_ack) => bail!(
            "unexpected explicit v3 frame abort while waiting for frame commit: frame_no={} archive_len={}",
            abort_ack.frame_no,
            abort_ack.archive_len
        ),
    }
}

pub(crate) fn receive_frame_event_from_start<T>(
    stream: &mut T,
    state: &mut ReceiverState,
    frame_start: FrameStart,
) -> Result<FrameReceiveResult>
where
    T: Read + Write,
{
    let frame_started = Instant::now();
    validate_frame_start(&state.journal, &frame_start).or_else(|error| {
        let message = error.to_string();
        send_protocol_error(stream, &message)?;
        Err(error)
    })?;
    debug!(
        "receiver frame start accepted: dump_id={} frame_no={} first_file_index={} first_file_path={} committed_archive_len={}",
        state.paths.dump_id(),
        frame_start.frame_no,
        frame_start.first_file_index,
        frame_start.first_file_path,
        state.journal.archive_len
    );

    let committed_journal = state.journal.clone();
    let partial_archive_path = state.paths.partial_archive_path().to_path_buf();
    let mut file = OpenOptions::new()
        .write(true)
        .open(&partial_archive_path)
        .with_context(|| {
            format!(
                "failed to open partial archive for frame append {}",
                partial_archive_path.display()
            )
        })?;
    file.seek(SeekFrom::Start(committed_journal.archive_len))
        .with_context(|| {
            format!(
                "failed to seek partial archive {} to committed offset {}",
                partial_archive_path.display(),
                committed_journal.archive_len
            )
        })?;

    let mut hasher = Xxh3Default::new();
    let mut compressed_len = 0_u64;
    let frame_end = loop {
        match protocol::read_message(stream) {
            Ok(Message::FrameChunk(frame_chunk)) => {
                file.write_all(&frame_chunk.bytes).with_context(|| {
                    format!(
                        "failed to append frame chunk into partial archive {}",
                        partial_archive_path.display()
                    )
                })?;
                hasher.update(&frame_chunk.bytes);
                compressed_len = compressed_len
                    .checked_add(frame_chunk.bytes.len() as u64)
                    .context("frame compressed length overflow")?;
            }
            Ok(Message::FrameEnd(frame_end)) => break frame_end,
            Ok(Message::FrameAbort(frame_abort)) => {
                if frame_abort.frame_no != frame_start.frame_no {
                    let message = format!(
                        "unexpected frame abort number: expected {} got {}",
                        frame_start.frame_no, frame_abort.frame_no
                    );
                    drop(file);
                    rollback_partial_frame(&partial_archive_path, committed_journal.archive_len)?;
                    state.journal = committed_journal;
                    send_protocol_error(stream, &message)?;
                    bail!("{message}");
                }

                drop(file);
                rollback_partial_frame(&partial_archive_path, committed_journal.archive_len)?;
                state.journal = committed_journal.clone();

                let abort_ack = AbortAck {
                    frame_no: frame_start.frame_no,
                    archive_len: committed_journal.archive_len,
                };
                protocol::write_message(stream, &Message::AbortAck(abort_ack.clone()))
                    .and_then(|_| stream.flush().context("failed to flush abort ack to peer"))?;
                return Ok(FrameReceiveResult::Aborted(abort_ack));
            }
            Ok(Message::Error(error)) => {
                drop(file);
                rollback_partial_frame(&partial_archive_path, committed_journal.archive_len)?;
                state.journal = committed_journal;
                bail!("peer aborted during frame transfer: {}", error.message);
            }
            Ok(unexpected) => {
                let message = "expected frame chunk or frame end".to_owned();
                drop(file);
                rollback_partial_frame(&partial_archive_path, committed_journal.archive_len)?;
                state.journal = committed_journal;
                send_protocol_error(stream, &message)?;
                bail!(
                    "unexpected message while receiving frame {}: {:?}",
                    frame_start.frame_no,
                    unexpected.kind()
                );
            }
            Err(error) => {
                drop(file);
                rollback_partial_frame(&partial_archive_path, committed_journal.archive_len)?;
                state.journal = committed_journal;
                return Err(error);
            }
        }
    };

    if let Err(error) =
        validate_frame_end(&frame_start, &frame_end, compressed_len, hasher.digest())
    {
        let message = error.to_string();
        drop(file);
        rollback_partial_frame(&partial_archive_path, committed_journal.archive_len)?;
        state.journal = committed_journal.clone();
        send_protocol_error(stream, &message)?;
        return Err(error);
    }

    file.sync_all().with_context(|| {
        format!(
            "failed to sync partial archive after frame commit {}",
            partial_archive_path.display()
        )
    })?;
    drop(file);

    let new_archive_len = committed_journal
        .archive_len
        .checked_add(compressed_len)
        .context("archive length overflow after frame commit")?;
    let ack = Ack {
        frame_no: frame_end.frame_no,
        last_file_index: frame_end.last_file_index,
        last_file_path: frame_end.last_file_path.clone(),
        archive_len: new_archive_len,
    };

    state.journal.last_committed_frame = frame_end.frame_no;
    state.journal.last_committed_file_index = frame_end.last_file_index;
    state.journal.last_committed_file_path = frame_end.last_file_path.clone();
    state.journal.archive_len = new_archive_len;

    if let Err(error) = state.persist_journal() {
        rollback_partial_frame(&partial_archive_path, committed_journal.archive_len)?;
        state.journal = committed_journal;
        return Err(error.context("failed to persist receiver journal after frame commit"));
    }

    if let Err(error) = protocol::write_message(stream, &Message::Ack(ack.clone()))
        .and_then(|_| stream.flush().context("failed to flush frame ack to peer"))
    {
        rollback_partial_frame(&partial_archive_path, committed_journal.archive_len)?;
        state.journal = committed_journal;
        state
            .persist_journal()
            .context("failed to restore receiver journal after frame ack delivery failure")?;
        return Err(error.context("failed to deliver frame ack to peer"));
    }
    let elapsed = frame_started.elapsed();
    debug!(
        "receiver frame committed: dump_id={} frame_no={} last_file_index={} last_file_path={} compressed_len={} archive_len={} elapsed_ms={} write_bytes_per_sec={}",
        state.paths.dump_id(),
        ack.frame_no,
        ack.last_file_index,
        ack.last_file_path,
        compressed_len,
        ack.archive_len,
        elapsed.as_millis(),
        bytes_per_sec(compressed_len, elapsed)
    );

    Ok(FrameReceiveResult::Committed(ack))
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn receive_frame_event_v3_from_start<T>(
    stream: &mut T,
    state: &mut ReceiverState,
    frame_start: FrameStartV3,
) -> Result<FrameReceiveResultV3>
where
    T: Read + Write,
{
    let frame_started = Instant::now();
    validate_frame_start_v3(&state.journal, &frame_start).or_else(|error| {
        let message = error.to_string();
        protocol::write_message_v3(
            stream,
            &V3Message::Error(protocol::ErrorMessage {
                message: message.clone(),
            }),
        )?;
        stream
            .flush()
            .context("failed to flush v3 frame-start validation error")?;
        Err(error)
    })?;
    debug!(
        "receiver v3 frame start accepted: dump_id={} frame_no={} first_work_ordinal={} committed_archive_len={} committed_work_ordinal={:?}",
        state.paths.dump_id(),
        frame_start.frame_no,
        frame_start.first_work_ordinal,
        state.journal.archive_len,
        state.journal.last_committed_work_ordinal
    );

    let committed_journal = state.journal.clone();
    let partial_archive_path = state.paths.partial_archive_path().to_path_buf();
    let mut file = OpenOptions::new()
        .write(true)
        .open(&partial_archive_path)
        .with_context(|| {
            format!(
                "failed to open partial archive for v3 frame append {}",
                partial_archive_path.display()
            )
        })?;
    file.seek(SeekFrom::Start(committed_journal.archive_len))
        .with_context(|| {
            format!(
                "failed to seek partial archive {} to committed offset {}",
                partial_archive_path.display(),
                committed_journal.archive_len
            )
        })?;

    let mut hasher = Xxh3Default::new();
    let mut compressed_len = 0_u64;
    let frame_end = loop {
        match protocol::read_message_v3(stream) {
            Ok(V3Message::FrameChunk(frame_chunk)) => {
                file.write_all(&frame_chunk.bytes).with_context(|| {
                    format!(
                        "failed to append v3 frame chunk into partial archive {}",
                        partial_archive_path.display()
                    )
                })?;
                hasher.update(&frame_chunk.bytes);
                compressed_len = compressed_len
                    .checked_add(frame_chunk.bytes.len() as u64)
                    .context("v3 frame compressed length overflow")?;
            }
            Ok(V3Message::FrameEnd(frame_end)) => break frame_end,
            Ok(V3Message::FrameAbort(frame_abort)) => {
                if frame_abort.frame_no != frame_start.frame_no {
                    let message = format!(
                        "unexpected v3 frame abort number: expected {} got {}",
                        frame_start.frame_no, frame_abort.frame_no
                    );
                    drop(file);
                    rollback_partial_frame(&partial_archive_path, committed_journal.archive_len)?;
                    state.journal = committed_journal;
                    protocol::write_message_v3(
                        stream,
                        &V3Message::Error(protocol::ErrorMessage {
                            message: message.clone(),
                        }),
                    )?;
                    stream
                        .flush()
                        .context("failed to flush v3 abort-number error")?;
                    bail!("{message}");
                }

                drop(file);
                rollback_partial_frame(&partial_archive_path, committed_journal.archive_len)?;
                state.journal = committed_journal.clone();

                let abort_ack = AbortAck {
                    frame_no: frame_start.frame_no,
                    archive_len: committed_journal.archive_len,
                };
                protocol::write_message_v3(stream, &V3Message::AbortAck(abort_ack.clone()))
                    .and_then(|_| {
                        stream
                            .flush()
                            .context("failed to flush v3 abort ack to peer")
                    })?;
                return Ok(FrameReceiveResultV3::Aborted(abort_ack));
            }
            Ok(V3Message::Error(error)) => {
                drop(file);
                rollback_partial_frame(&partial_archive_path, committed_journal.archive_len)?;
                state.journal = committed_journal;
                bail!("peer aborted during v3 frame transfer: {}", error.message);
            }
            Ok(unexpected) => {
                let message = "expected v3 frame chunk or frame end".to_owned();
                drop(file);
                rollback_partial_frame(&partial_archive_path, committed_journal.archive_len)?;
                state.journal = committed_journal;
                protocol::write_message_v3(
                    stream,
                    &V3Message::Error(protocol::ErrorMessage {
                        message: message.clone(),
                    }),
                )?;
                stream
                    .flush()
                    .context("failed to flush v3 frame receive error")?;
                bail!(
                    "unexpected v3 message while receiving frame {}: {:?}",
                    frame_start.frame_no,
                    unexpected.kind()
                );
            }
            Err(error) => {
                drop(file);
                rollback_partial_frame(&partial_archive_path, committed_journal.archive_len)?;
                state.journal = committed_journal;
                return Err(error);
            }
        }
    };

    if let Err(error) = validate_frame_end_v3(
        &committed_journal,
        &frame_start,
        &frame_end,
        compressed_len,
        hasher.digest(),
    ) {
        let message = error.to_string();
        drop(file);
        rollback_partial_frame(&partial_archive_path, committed_journal.archive_len)?;
        state.journal = committed_journal.clone();
        protocol::write_message_v3(
            stream,
            &V3Message::Error(protocol::ErrorMessage {
                message: message.clone(),
            }),
        )?;
        stream
            .flush()
            .context("failed to flush v3 frame-end validation error")?;
        return Err(error);
    }

    file.sync_all().with_context(|| {
        format!(
            "failed to sync partial archive after v3 frame commit {}",
            partial_archive_path.display()
        )
    })?;
    drop(file);

    let new_archive_len = committed_journal
        .archive_len
        .checked_add(compressed_len)
        .context("archive length overflow after v3 frame commit")?;
    let ack = AckV3 {
        frame_no: frame_end.frame_no,
        last_work_ordinal: frame_end.last_work_ordinal,
        archive_len: new_archive_len,
    };

    state.journal.last_committed_frame = frame_end.frame_no;
    state.journal.last_committed_work_ordinal = Some(frame_end.last_work_ordinal);
    state.journal.archive_len = new_archive_len;

    if let Err(error) = state.persist_journal() {
        rollback_partial_frame(&partial_archive_path, committed_journal.archive_len)?;
        state.journal = committed_journal;
        return Err(error.context("failed to persist receiver journal after v3 frame commit"));
    }

    if let Err(error) =
        protocol::write_message_v3(stream, &V3Message::Ack(ack.clone())).and_then(|_| {
            stream
                .flush()
                .context("failed to flush v3 frame ack to peer")
        })
    {
        rollback_partial_frame(&partial_archive_path, committed_journal.archive_len)?;
        state.journal = committed_journal;
        state
            .persist_journal()
            .context("failed to restore receiver journal after v3 frame ack delivery failure")?;
        return Err(error.context("failed to deliver v3 frame ack to peer"));
    }
    let elapsed = frame_started.elapsed();
    debug!(
        "receiver v3 frame committed: dump_id={} frame_no={} last_work_ordinal={} compressed_len={} archive_len={} elapsed_ms={} write_bytes_per_sec={}",
        state.paths.dump_id(),
        ack.frame_no,
        ack.last_work_ordinal,
        compressed_len,
        ack.archive_len,
        elapsed.as_millis(),
        bytes_per_sec(compressed_len, elapsed)
    );

    Ok(FrameReceiveResultV3::Committed(ack))
}

fn receive_frame_abort_from_start<T>(
    stream: &mut T,
    state: &mut ReceiverState,
    frame_start: FrameStart,
) -> Result<AbortAck>
where
    T: Read + Write,
{
    match receive_frame_event_from_start(stream, state, frame_start)? {
        FrameReceiveResult::Committed(ack) => bail!(
            "unexpected committed frame while waiting for explicit abort: frame_no={} archive_len={}",
            ack.frame_no,
            ack.archive_len
        ),
        FrameReceiveResult::Aborted(abort_ack) => Ok(abort_ack),
    }
}

fn validate_frame_start(journal: &ReceiverJournal, frame_start: &FrameStart) -> Result<()> {
    let expected_frame_no = journal
        .last_committed_frame
        .checked_add(1)
        .context("frame number overflow while validating frame start")?;
    if frame_start.frame_no != expected_frame_no {
        bail!(
            "unexpected frame number: expected {} got {}",
            expected_frame_no,
            frame_start.frame_no
        );
    }

    let expected_first_file_index = journal
        .last_committed_file_index
        .checked_add(1)
        .context("file index overflow while validating frame start")?;
    if frame_start.first_file_index != expected_first_file_index {
        bail!(
            "unexpected first file index: expected {} got {}",
            expected_first_file_index,
            frame_start.first_file_index
        );
    }
    if frame_start.threads == 0 {
        bail!("frame threads must be at least 1");
    }

    Ok(())
}

fn validate_frame_end(
    frame_start: &FrameStart,
    frame_end: &FrameEnd,
    compressed_len: u64,
    compressed_xxh3_64: u64,
) -> Result<()> {
    if frame_end.frame_no != frame_start.frame_no {
        bail!(
            "frame end number mismatch: start={} end={}",
            frame_start.frame_no,
            frame_end.frame_no
        );
    }
    if frame_end.last_file_index < frame_start.first_file_index {
        bail!("frame end file index must not move backwards");
    }
    if frame_end.compressed_len != compressed_len {
        bail!("frame compressed length mismatch");
    }
    if frame_end.compressed_xxh3_64 != compressed_xxh3_64 {
        bail!("frame checksum mismatch");
    }

    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
fn validate_frame_start_v3(journal: &ReceiverJournal, frame_start: &FrameStartV3) -> Result<()> {
    let expected_frame_no = journal
        .last_committed_frame
        .checked_add(1)
        .context("frame number overflow while validating v3 frame start")?;
    if frame_start.frame_no != expected_frame_no {
        bail!(
            "unexpected v3 frame number: expected {} got {}",
            expected_frame_no,
            frame_start.frame_no
        );
    }

    let expected_first_work_ordinal = journal
        .last_committed_work_ordinal
        .unwrap_or(0)
        .checked_add(1)
        .context("work ordinal overflow while validating v3 frame start")?;
    if frame_start.first_work_ordinal != expected_first_work_ordinal {
        bail!(
            "unexpected first work ordinal: expected {} got {}",
            expected_first_work_ordinal,
            frame_start.first_work_ordinal
        );
    }
    if journal.last_manifest_work_ordinal.unwrap_or(0) < frame_start.first_work_ordinal {
        bail!(
            "v3 frame start exceeds durable manifest boundary: first_work_ordinal={} durable_manifest_work_ordinal={:?}",
            frame_start.first_work_ordinal,
            journal.last_manifest_work_ordinal
        );
    }
    if frame_start.threads == 0 {
        bail!("v3 frame threads must be at least 1");
    }

    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
fn validate_frame_end_v3(
    journal: &ReceiverJournal,
    frame_start: &FrameStartV3,
    frame_end: &FrameEndV3,
    compressed_len: u64,
    compressed_xxh3_64: u64,
) -> Result<()> {
    if frame_end.frame_no != frame_start.frame_no {
        bail!(
            "v3 frame end number mismatch: start={} end={}",
            frame_start.frame_no,
            frame_end.frame_no
        );
    }
    if frame_end.last_work_ordinal < frame_start.first_work_ordinal {
        bail!("v3 frame end work ordinal must not move backwards");
    }
    if frame_end.last_work_ordinal > journal.last_manifest_work_ordinal.unwrap_or(0) {
        bail!(
            "v3 frame end exceeds durable manifest boundary: last_work_ordinal={} durable_manifest_work_ordinal={:?}",
            frame_end.last_work_ordinal,
            journal.last_manifest_work_ordinal
        );
    }
    if frame_end.compressed_len != compressed_len {
        bail!("v3 frame compressed length mismatch");
    }
    if frame_end.compressed_xxh3_64 != compressed_xxh3_64 {
        bail!("v3 frame checksum mismatch");
    }

    Ok(())
}

fn rollback_partial_frame(path: &Path, archive_len: u64) -> Result<()> {
    recover_partial_archive(path, archive_len)
        .with_context(|| format!("failed to roll back partial frame in {}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{self, Cursor},
    };

    use super::*;
    use crate::network::protocol::{
        AckV3, ErrorMessage, FrameChunk, FrameEndV3, FrameStartV3, Message, ResumeState, V3Message,
    };
    use crate::receiver::ReceiverJournal;
    use crate::receiver::journal::journal_temp_path;
    use crate::receiver::shared::test_hooks;
    use crate::receiver::test_support::*;
    use crate::sender::prepare_dump;

    struct FlushFailingStream {
        read: Cursor<Vec<u8>>,
        written: Vec<u8>,
    }

    impl FlushFailingStream {
        fn new(read_bytes: Vec<u8>) -> Self {
            Self {
                read: Cursor::new(read_bytes),
                written: Vec::new(),
            }
        }
    }

    impl Read for FlushFailingStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.read.read(buf)
        }
    }

    impl Write for FlushFailingStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.written.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::other("injected flush failure"))
        }
    }

    fn sample_frame_start() -> FrameStart {
        FrameStart {
            frame_no: 1,
            first_file_index: 1,
            first_file_path: "root/file.bin".to_owned(),
            level: 3,
            threads: 2,
        }
    }

    fn sample_frame_start_v3() -> FrameStartV3 {
        FrameStartV3 {
            frame_no: 1,
            first_work_ordinal: 1,
            level: 3,
            threads: 2,
        }
    }

    fn encode_message_v3(message: &V3Message) -> Vec<u8> {
        let mut bytes = Vec::new();
        protocol::write_message_v3(&mut bytes, message).unwrap();
        bytes
    }

    fn decode_messages_v3(bytes: &[u8]) -> Vec<V3Message> {
        let mut cursor = Cursor::new(bytes.to_vec());
        let mut messages = Vec::new();
        while (cursor.position() as usize) < bytes.len() {
            messages.push(protocol::read_message_v3(&mut cursor).unwrap());
        }
        messages
    }

    fn frame_end_v3(frame_no: u64, last_work_ordinal: u64, chunks: &[&[u8]]) -> FrameEndV3 {
        let mut hasher = Xxh3Default::new();
        let mut compressed_len = 0_u64;
        for chunk in chunks {
            hasher.update(chunk);
            compressed_len += chunk.len() as u64;
        }
        FrameEndV3 {
            frame_no,
            last_work_ordinal,
            compressed_len,
            compressed_xxh3_64: hasher.digest(),
        }
    }

    #[test]
    fn receive_frame_surfaces_peer_error_before_frame_start() {
        let temp = TestDir::new();
        let request = encode_message(&Message::Error(ErrorMessage {
            message: "cancelled".to_owned(),
        }));
        let mut stream = Cursor::new(request);
        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();

        let error = receive_frame(&mut stream, &mut state).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("peer aborted before frame transfer: cancelled")
        );
        assert_eq!(
            stream.into_inner().len(),
            encode_message(&Message::Error(ErrorMessage {
                message: "cancelled".to_owned(),
            }))
            .len()
        );
    }

    #[test]
    fn receive_frame_rejects_unexpected_message_before_frame_start_and_emits_error() {
        let temp = TestDir::new();
        let request = encode_message(&Message::FinishAck);
        let request_len = request.len();
        let mut stream = Cursor::new(request);
        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();

        let error = receive_frame(&mut stream, &mut state).unwrap_err();
        let bytes = stream.into_inner();

        assert!(
            error
                .to_string()
                .contains("unexpected message before frame start: FinishAck")
        );
        assert_eq!(
            decode_reply(&bytes, request_len),
            Message::Error(ErrorMessage {
                message: "expected frame start".to_owned(),
            })
        );
    }

    #[test]
    fn receive_frame_from_start_rejects_unexpected_frame_number_and_emits_error() {
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        let mut stream = Cursor::new(Vec::new());
        let mut frame_start = sample_frame_start();
        frame_start.frame_no = 2;

        let error = receive_frame_from_start(&mut stream, &mut state, frame_start).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unexpected frame number: expected 1 got 2")
        );
        assert_eq!(
            decode_messages(&stream.into_inner()),
            vec![Message::Error(ErrorMessage {
                message: "unexpected frame number: expected 1 got 2".to_owned(),
            })]
        );
        assert_eq!(state.journal.last_committed_frame, 0);
    }

    #[test]
    fn receive_frame_commits_archive_and_persists_journal() {
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        let chunks = [b"abc".as_slice(), b"def".as_slice()];
        let request = [
            encode_message(&Message::FrameStart(FrameStart {
                frame_no: 1,
                first_file_index: 1,
                first_file_path: "root/file.bin".to_owned(),
                level: 3,
                threads: 2,
            })),
            encode_message(&Message::FrameChunk(FrameChunk {
                bytes: chunks[0].to_vec(),
            })),
            encode_message(&Message::FrameChunk(FrameChunk {
                bytes: chunks[1].to_vec(),
            })),
            encode_message(&Message::FrameEnd(frame_end(
                1,
                1,
                "root/file.bin",
                &chunks,
            ))),
        ]
        .concat();
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let ack = receive_frame(&mut stream, &mut state).unwrap();
        let bytes = stream.into_inner();

        assert_eq!(
            ack,
            Ack {
                frame_no: 1,
                last_file_index: 1,
                last_file_path: "root/file.bin".to_owned(),
                archive_len: 6,
            }
        );
        assert_eq!(state.journal.last_committed_frame, 1);
        assert_eq!(state.journal.last_committed_file_index, 1);
        assert_eq!(state.journal.last_committed_file_path, "root/file.bin");
        assert_eq!(state.journal.archive_len, 6);
        assert_eq!(
            fs::read(state.paths.partial_archive_path()).unwrap(),
            b"abcdef"
        );
        assert_eq!(
            ReceiverJournal::load(state.paths.journal_path()).unwrap(),
            state.journal
        );
        assert_eq!(decode_reply(&bytes, request_len), Message::Ack(ack));
    }

    #[test]
    fn receive_frame_v3_commits_archive_and_persists_work_ordinal_boundary() {
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-v3").unwrap();
        state
            .commit_manifest_chunk(b"manifest", Some(2), Some(2), false)
            .unwrap();
        let chunks = [b"abc".as_slice(), b"def".as_slice()];
        let request = [
            encode_message_v3(&V3Message::FrameStart(sample_frame_start_v3())),
            encode_message_v3(&V3Message::FrameChunk(FrameChunk {
                bytes: chunks[0].to_vec(),
            })),
            encode_message_v3(&V3Message::FrameChunk(FrameChunk {
                bytes: chunks[1].to_vec(),
            })),
            encode_message_v3(&V3Message::FrameEnd(frame_end_v3(1, 2, &chunks))),
        ]
        .concat();
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let ack = receive_frame_v3(&mut stream, &mut state).unwrap();
        let bytes = stream.into_inner();

        assert_eq!(
            ack,
            AckV3 {
                frame_no: 1,
                last_work_ordinal: 2,
                archive_len: 6,
            }
        );
        assert_eq!(state.journal.last_committed_frame, 1);
        assert_eq!(state.journal.last_committed_work_ordinal, Some(2));
        assert_eq!(state.journal.archive_len, 6);
        assert_eq!(
            fs::read(state.paths.partial_archive_path()).unwrap(),
            b"abcdef"
        );
        assert_eq!(
            decode_messages_v3(&bytes[request_len..]),
            vec![V3Message::Ack(ack)]
        );
    }

    #[test]
    fn receive_frame_v3_accepts_zero_chunk_skip_only_progress() {
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-v3").unwrap();
        state
            .commit_manifest_chunk(b"manifest", Some(1), Some(1), false)
            .unwrap();
        let request = [
            encode_message_v3(&V3Message::FrameStart(FrameStartV3 {
                frame_no: 1,
                first_work_ordinal: 1,
                level: 3,
                threads: 1,
            })),
            encode_message_v3(&V3Message::FrameEnd(FrameEndV3 {
                frame_no: 1,
                last_work_ordinal: 1,
                compressed_len: 0,
                compressed_xxh3_64: Xxh3Default::new().digest(),
            })),
        ]
        .concat();
        let mut stream = Cursor::new(request);

        let ack = receive_frame_v3(&mut stream, &mut state).unwrap();

        assert_eq!(ack.archive_len, 0);
        assert_eq!(state.journal.last_committed_frame, 1);
        assert_eq!(state.journal.last_committed_work_ordinal, Some(1));
        assert_eq!(state.journal.archive_len, 0);
        assert_eq!(
            fs::metadata(state.paths.partial_archive_path())
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn receive_frame_v3_rejects_frame_end_beyond_durable_manifest_boundary() {
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-v3").unwrap();
        state
            .commit_manifest_chunk(b"manifest", Some(1), Some(1), false)
            .unwrap();
        let chunks = [b"abc".as_slice()];
        let request = [
            encode_message_v3(&V3Message::FrameStart(FrameStartV3 {
                frame_no: 1,
                first_work_ordinal: 1,
                level: 3,
                threads: 1,
            })),
            encode_message_v3(&V3Message::FrameChunk(FrameChunk {
                bytes: chunks[0].to_vec(),
            })),
            encode_message_v3(&V3Message::FrameEnd(frame_end_v3(1, 2, &chunks))),
        ]
        .concat();
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = receive_frame_v3(&mut stream, &mut state).unwrap_err();
        let bytes = stream.into_inner();

        assert!(
            error
                .to_string()
                .contains("v3 frame end exceeds durable manifest boundary")
        );
        assert_eq!(
            decode_messages_v3(&bytes[request_len..]),
            vec![V3Message::Error(protocol::ErrorMessage {
                message: "v3 frame end exceeds durable manifest boundary: last_work_ordinal=2 durable_manifest_work_ordinal=Some(1)".to_owned(),
            })]
        );
        assert_eq!(state.journal.last_committed_frame, 0);
        assert_eq!(state.journal.last_committed_work_ordinal, None);
        assert_eq!(state.journal.archive_len, 0);
    }

    #[test]
    fn receive_frame_rolls_back_partial_archive_when_journal_replace_fails() {
        let _guard = FsFailureGuard::new();
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        let previous_journal = state.journal.clone();
        let chunks = [b"abc".as_slice(), b"def".as_slice()];
        let request = [
            encode_message(&Message::FrameStart(FrameStart {
                frame_no: 1,
                first_file_index: 1,
                first_file_path: "root/file.bin".to_owned(),
                level: 3,
                threads: 2,
            })),
            encode_message(&Message::FrameChunk(FrameChunk {
                bytes: chunks[0].to_vec(),
            })),
            encode_message(&Message::FrameChunk(FrameChunk {
                bytes: chunks[1].to_vec(),
            })),
            encode_message(&Message::FrameEnd(frame_end(
                1,
                1,
                "root/file.bin",
                &chunks,
            ))),
        ]
        .concat();
        let temp_journal_path = journal_temp_path(state.paths.journal_path()).unwrap();
        test_hooks::fail_next_rename(&temp_journal_path, state.paths.journal_path());
        let mut stream = Cursor::new(request);

        let error = receive_frame(&mut stream, &mut state).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to persist receiver journal after frame commit")
        );
        assert_eq!(state.journal, previous_journal);
        assert_eq!(
            fs::metadata(state.paths.partial_archive_path())
                .unwrap()
                .len(),
            previous_journal.archive_len
        );
        assert_eq!(
            ReceiverJournal::load(state.paths.journal_path()).unwrap(),
            previous_journal
        );
        assert!(!temp_journal_path.exists());
    }

    #[test]
    fn receive_frame_rolls_back_partial_archive_on_checksum_mismatch() {
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        let request = [
            encode_message(&Message::FrameStart(FrameStart {
                frame_no: 1,
                first_file_index: 1,
                first_file_path: "root/file.bin".to_owned(),
                level: 3,
                threads: 2,
            })),
            encode_message(&Message::FrameChunk(FrameChunk {
                bytes: b"abcdef".to_vec(),
            })),
            encode_message(&Message::FrameEnd(FrameEnd {
                frame_no: 1,
                last_file_index: 1,
                last_file_path: "root/file.bin".to_owned(),
                compressed_len: 6,
                compressed_xxh3_64: 0,
            })),
        ]
        .concat();
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = receive_frame(&mut stream, &mut state).unwrap_err();
        let bytes = stream.into_inner();

        assert!(error.to_string().contains("frame checksum mismatch"));
        assert_eq!(state.journal.archive_len, 0);
        assert_eq!(
            fs::metadata(state.paths.partial_archive_path())
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            decode_reply(&bytes, request_len),
            Message::Error(ErrorMessage {
                message: "frame checksum mismatch".to_owned(),
            })
        );
    }

    #[test]
    fn receive_frame_rolls_back_partial_archive_on_peer_error_mid_frame() {
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        let request = [
            encode_message(&Message::FrameStart(sample_frame_start())),
            encode_message(&Message::FrameChunk(FrameChunk {
                bytes: b"abcdef".to_vec(),
            })),
            encode_message(&Message::Error(ErrorMessage {
                message: "cancelled".to_owned(),
            })),
        ]
        .concat();
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = receive_frame(&mut stream, &mut state).unwrap_err();
        let bytes = stream.into_inner();

        assert!(
            error
                .to_string()
                .contains("peer aborted during frame transfer: cancelled")
        );
        assert_eq!(state.journal.archive_len, 0);
        assert_eq!(
            fs::metadata(state.paths.partial_archive_path())
                .unwrap()
                .len(),
            0
        );
        assert_eq!(bytes.len(), request_len);
    }

    #[test]
    fn receive_frame_rejects_unexpected_message_mid_frame_and_emits_error() {
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        let request = [
            encode_message(&Message::FrameStart(sample_frame_start())),
            encode_message(&Message::FinishAck),
        ]
        .concat();
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = receive_frame(&mut stream, &mut state).unwrap_err();
        let bytes = stream.into_inner();

        assert!(
            error
                .to_string()
                .contains("unexpected message while receiving frame 1: FinishAck")
        );
        assert_eq!(
            decode_reply(&bytes, request_len),
            Message::Error(ErrorMessage {
                message: "expected frame chunk or frame end".to_owned(),
            })
        );
        assert_eq!(
            fs::metadata(state.paths.partial_archive_path())
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn receive_frame_rolls_back_partial_archive_on_disconnect_before_frame_end() {
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        let request = [
            encode_message(&Message::FrameStart(FrameStart {
                frame_no: 1,
                first_file_index: 1,
                first_file_path: "root/file.bin".to_owned(),
                level: 3,
                threads: 2,
            })),
            encode_message(&Message::FrameChunk(FrameChunk {
                bytes: b"abcdef".to_vec(),
            })),
        ]
        .concat();
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = receive_frame(&mut stream, &mut state).unwrap_err();
        let bytes = stream.into_inner();

        assert!(error.to_string().contains("failed to read packet header"));
        assert_eq!(state.journal.archive_len, 0);
        assert_eq!(
            fs::metadata(state.paths.partial_archive_path())
                .unwrap()
                .len(),
            0
        );
        assert_eq!(bytes.len(), request_len);
    }

    #[test]
    fn receive_frame_persists_monotonic_archive_len_under_load() {
        let source = TestDir::new();
        fs::create_dir_all(source.path().join("nested")).unwrap();
        fs::write(source.path().join("alpha.txt"), b"alpha").unwrap();
        fs::write(source.path().join("nested").join("beta.txt"), b"beta").unwrap();
        fs::write(source.path().join("nested").join("gamma.txt"), b"gamma").unwrap();
        fs::write(source.path().join("nested").join("delta.txt"), b"delta").unwrap();
        let prepared = prepare_dump(
            source.path().to_path_buf(),
            &ResumeState {
                last_committed_frame: 0,
                last_committed_file_index: 0,
                last_committed_file_path: String::new(),
                archive_len: 0,
            },
            3,
            1,
            1,
        )
        .unwrap();
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        let mut previous_archive_len = 0_u64;

        for frame in &prepared.frames {
            let ack =
                receive_frame(&mut Cursor::new(encode_prepared_frame(frame)), &mut state).unwrap();
            let persisted = ReceiverJournal::load(state.paths.journal_path()).unwrap();

            assert!(ack.archive_len > previous_archive_len);
            assert_eq!(persisted.archive_len, ack.archive_len);
            assert_eq!(persisted.last_committed_frame, ack.frame_no);
            assert_eq!(persisted.last_committed_file_index, ack.last_file_index);
            assert_eq!(persisted.last_committed_file_path, ack.last_file_path);
            previous_archive_len = ack.archive_len;
        }
    }

    #[test]
    fn receive_frame_rolls_back_when_ack_flush_fails() {
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        let previous_journal = state.journal.clone();
        let chunks = [b"abc".as_slice(), b"def".as_slice()];
        let request = [
            encode_message(&Message::FrameChunk(FrameChunk {
                bytes: chunks[0].to_vec(),
            })),
            encode_message(&Message::FrameChunk(FrameChunk {
                bytes: chunks[1].to_vec(),
            })),
            encode_message(&Message::FrameEnd(frame_end(
                1,
                1,
                "root/file.bin",
                &chunks,
            ))),
        ]
        .concat();
        let mut stream = FlushFailingStream::new(request);

        let error =
            receive_frame_from_start(&mut stream, &mut state, sample_frame_start()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to deliver frame ack to peer")
        );
        assert_eq!(state.journal, previous_journal);
        assert_eq!(
            ReceiverJournal::load(state.paths.journal_path()).unwrap(),
            previous_journal
        );
        assert_eq!(
            fs::metadata(state.paths.partial_archive_path())
                .unwrap()
                .len(),
            previous_journal.archive_len
        );
        assert!(!stream.written.is_empty());
    }

    #[test]
    fn validate_frame_start_rejects_first_file_index_gap() {
        let journal = ReceiverJournal {
            protocol_version: protocol::VERSION,
            dump_id: "dump-01".to_owned(),
            last_committed_frame: 7,
            last_committed_file_index: 3,
            last_committed_file_path: "root/file.bin".to_owned(),
            last_committed_work_ordinal: None,
            archive_len: 10,
            manifest_bytes_persisted: 0,
            last_manifest_record_seq: None,
            last_manifest_work_ordinal: None,
            manifest_sealed: false,
        };
        let mut frame_start = sample_frame_start();
        frame_start.frame_no = 8;
        frame_start.first_file_index = 9;

        let error = validate_frame_start(&journal, &frame_start).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unexpected first file index: expected 4 got 9")
        );
    }

    #[test]
    fn validate_frame_start_rejects_zero_threads() {
        let journal = ReceiverJournal::initial("dump-01").unwrap();
        let mut frame_start = sample_frame_start();
        frame_start.threads = 0;

        let error = validate_frame_start(&journal, &frame_start).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("frame threads must be at least 1")
        );
    }

    #[test]
    fn validate_frame_start_rejects_frame_number_overflow() {
        let journal = ReceiverJournal {
            protocol_version: protocol::VERSION,
            dump_id: "dump-01".to_owned(),
            last_committed_frame: u64::MAX,
            last_committed_file_index: 0,
            last_committed_file_path: String::new(),
            last_committed_work_ordinal: None,
            archive_len: 0,
            manifest_bytes_persisted: 0,
            last_manifest_record_seq: None,
            last_manifest_work_ordinal: None,
            manifest_sealed: false,
        };

        let error = validate_frame_start(&journal, &sample_frame_start()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("frame number overflow while validating frame start")
        );
    }

    #[test]
    fn validate_frame_start_rejects_file_index_overflow() {
        let journal = ReceiverJournal {
            protocol_version: protocol::VERSION,
            dump_id: "dump-01".to_owned(),
            last_committed_frame: 0,
            last_committed_file_index: u64::MAX,
            last_committed_file_path: String::new(),
            last_committed_work_ordinal: None,
            archive_len: 0,
            manifest_bytes_persisted: 0,
            last_manifest_record_seq: None,
            last_manifest_work_ordinal: None,
            manifest_sealed: false,
        };

        let error = validate_frame_start(&journal, &sample_frame_start()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("file index overflow while validating frame start")
        );
    }

    #[test]
    fn validate_frame_end_rejects_frame_number_mismatch() {
        let frame_start = sample_frame_start();
        let mut frame_end = frame_end(2, 1, "root/file.bin", &[b"abc"]);
        frame_end.frame_no = 2;

        let error = validate_frame_end(&frame_start, &frame_end, 3, frame_end.compressed_xxh3_64)
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("frame end number mismatch: start=1 end=2")
        );
    }

    #[test]
    fn validate_frame_end_rejects_backwards_file_index() {
        let frame_start = sample_frame_start();
        let frame_end = frame_end(1, 0, "root/file.bin", &[b"abc"]);

        let error = validate_frame_end(&frame_start, &frame_end, 3, frame_end.compressed_xxh3_64)
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("frame end file index must not move backwards")
        );
    }

    #[test]
    fn validate_frame_end_rejects_length_mismatch() {
        let frame_start = sample_frame_start();
        let frame_end = frame_end(1, 1, "root/file.bin", &[b"abc"]);

        let error = validate_frame_end(&frame_start, &frame_end, 2, frame_end.compressed_xxh3_64)
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("frame compressed length mismatch")
        );
    }
}

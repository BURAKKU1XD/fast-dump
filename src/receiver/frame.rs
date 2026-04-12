use std::io::{Read, Seek, SeekFrom, Write};
use std::time::{Duration, Instant};
use std::{fs::OpenOptions, path::Path};

use anyhow::{Context, Result, bail};
use log::debug;
use xxhash_rust::xxh3::Xxh3Default;

use crate::network::protocol::{self, AbortAck, Ack, FrameEnd, FrameStart, Message};

use super::journal::{ReceiverJournal, recover_partial_archive};
use super::shared::bytes_per_sec;
use super::{ReceiverProfile, ReceiverState};

pub(crate) enum FrameReceiveResult {
    Committed(Ack),
    Aborted(AbortAck),
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn receive_frame<T>(stream: &mut T, state: &mut ReceiverState) -> Result<Ack>
where
    T: Read + Write,
{
    let frame_start = match protocol::read_message(stream)? {
        Message::FrameStart(frame_start) => frame_start,
        Message::Error(error) => {
            bail!("peer aborted before v3 frame transfer: {}", error.message);
        }
        unexpected => {
            protocol::write_message(
                stream,
                &Message::Error(protocol::ErrorMessage {
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

    receive_frame_from_start(stream, state, frame_start)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn receive_frame_from_start<T>(
    stream: &mut T,
    state: &mut ReceiverState,
    frame_start: FrameStart,
) -> Result<Ack>
where
    T: Read + Write,
{
    let mut profile = ReceiverProfile::default();
    match receive_frame_event_v3_from_start(stream, state, frame_start, &mut profile)? {
        FrameReceiveResult::Committed(ack) => Ok(ack),
        FrameReceiveResult::Aborted(abort_ack) => bail!(
            "unexpected explicit v3 frame abort while waiting for frame commit: frame_no={} archive_len={}",
            abort_ack.frame_no,
            abort_ack.archive_len
        ),
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn receive_frame_event_v3_from_start<T>(
    stream: &mut T,
    state: &mut ReceiverState,
    frame_start: FrameStart,
    profile: &mut ReceiverProfile,
) -> Result<FrameReceiveResult>
where
    T: Read + Write,
{
    let frame_started = Instant::now();
    validate_frame_start(&state.journal, &frame_start).or_else(|error| {
        let message = error.to_string();
        protocol::write_message(
            stream,
            &Message::Error(protocol::ErrorMessage {
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
    let mut frame_write_duration = Duration::ZERO;
    let frame_end = loop {
        match protocol::read_message(stream) {
            Ok(Message::FrameChunk(frame_chunk)) => {
                let write_started = Instant::now();
                file.write_all(&frame_chunk.bytes).with_context(|| {
                    format!(
                        "failed to append v3 frame chunk into partial archive {}",
                        partial_archive_path.display()
                    )
                })?;
                frame_write_duration += write_started.elapsed();
                hasher.update(&frame_chunk.bytes);
                compressed_len = compressed_len
                    .checked_add(frame_chunk.bytes.len() as u64)
                    .context("v3 frame compressed length overflow")?;
            }
            Ok(Message::FrameEnd(frame_end)) => break frame_end,
            Ok(Message::FrameAbort(frame_abort)) => {
                if frame_abort.frame_no != frame_start.frame_no {
                    let message = format!(
                        "unexpected v3 frame abort number: expected {} got {}",
                        frame_start.frame_no, frame_abort.frame_no
                    );
                    drop(file);
                    rollback_partial_frame(&partial_archive_path, committed_journal.archive_len)?;
                    state.journal = committed_journal;
                    protocol::write_message(
                        stream,
                        &Message::Error(protocol::ErrorMessage {
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
                protocol::write_message(stream, &Message::AbortAck(abort_ack.clone())).and_then(
                    |_| {
                        stream
                            .flush()
                            .context("failed to flush v3 abort ack to peer")
                    },
                )?;
                return Ok(FrameReceiveResult::Aborted(abort_ack));
            }
            Ok(Message::Error(error)) => {
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
                protocol::write_message(
                    stream,
                    &Message::Error(protocol::ErrorMessage {
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

    if let Err(error) = validate_frame_end(
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
        protocol::write_message(
            stream,
            &Message::Error(protocol::ErrorMessage {
                message: message.clone(),
            }),
        )?;
        stream
            .flush()
            .context("failed to flush v3 frame-end validation error")?;
        return Err(error);
    }

    let sync_started = Instant::now();
    file.sync_all().with_context(|| {
        format!(
            "failed to sync partial archive after v3 frame commit {}",
            partial_archive_path.display()
        )
    })?;
    let frame_sync_duration = sync_started.elapsed();
    drop(file);

    let new_archive_len = committed_journal
        .archive_len
        .checked_add(compressed_len)
        .context("archive length overflow after v3 frame commit")?;
    let ack = Ack {
        frame_no: frame_end.frame_no,
        last_work_ordinal: frame_end.last_work_ordinal,
        archive_len: new_archive_len,
    };

    state.journal.last_committed_frame = frame_end.frame_no;
    state.journal.last_committed_work_ordinal = Some(frame_end.last_work_ordinal);
    state.journal.archive_len = new_archive_len;

    let persist_started = Instant::now();
    if let Err(error) = state.persist_journal() {
        rollback_partial_frame(&partial_archive_path, committed_journal.archive_len)?;
        state.journal = committed_journal;
        return Err(error.context("failed to persist receiver journal after v3 frame commit"));
    }
    let frame_journal_persist_duration = persist_started.elapsed();

    let ack_started = Instant::now();
    if let Err(error) = protocol::write_message(stream, &Message::Ack(ack.clone())).and_then(|_| {
        stream
            .flush()
            .context("failed to flush v3 frame ack to peer")
    }) {
        rollback_partial_frame(&partial_archive_path, committed_journal.archive_len)?;
        state.journal = committed_journal;
        state
            .persist_journal()
            .context("failed to restore receiver journal after v3 frame ack delivery failure")?;
        return Err(error.context("failed to deliver v3 frame ack to peer"));
    }
    let frame_ack_flush_duration = ack_started.elapsed();
    let elapsed = frame_started.elapsed();
    profile.record_frame_write(frame_write_duration);
    profile.record_frame_sync(frame_sync_duration);
    profile.record_frame_journal_persist(frame_journal_persist_duration);
    profile.record_frame_ack_flush(frame_ack_flush_duration);
    profile.record_frame_commit(compressed_len, elapsed);
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

    Ok(FrameReceiveResult::Committed(ack))
}

#[cfg_attr(not(test), allow(dead_code))]
fn validate_frame_start(journal: &ReceiverJournal, frame_start: &FrameStart) -> Result<()> {
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
fn validate_frame_end(
    journal: &ReceiverJournal,
    frame_start: &FrameStart,
    frame_end: &FrameEnd,
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

use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Result, bail};
use log::debug;

use crate::network::protocol::{
    self, Finish, ManifestChunk, ManifestSeal, ManifestStart, Message, ResumeState,
};

use super::finish::{
    append_issue_journal_chunk, discard_issue_journal_temp, receive_finish_message,
};
use super::frame::receive_frame_event_v3_from_start;
use super::journal::ReceiverManifestBoundary;
use super::resume::respond_resume_query_checked;
use super::shared::{
    ReceiverTempFileAppender, ReceiverTempFileKind, receiver_storage_exhausted_protocol_message,
};
use super::{ReceiverProfile, ReceiverState};

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiveReport {
    pub resume_state: ResumeState,
    pub state: ReceiverState,
    pub finish: Finish,
    pub final_archive_path: PathBuf,
    pub profile: ReceiverProfile,
}

#[derive(Debug)]
struct ManifestReceiveState {
    started: bool,
    ack_records: u64,
    ack_bytes: usize,
    last_acked_record_seq: u64,
    bytes_since_ack: usize,
    appender: ReceiverTempFileAppender,
    durable_boundary: ReceiverManifestBoundary,
    staged_boundary: ReceiverManifestBoundary,
}

fn receiver_manifest_boundary(state: &ReceiverState) -> protocol::ManifestBoundary {
    let boundary = state.journal.manifest_boundary();
    protocol::ManifestBoundary {
        manifest_bytes_persisted: boundary.manifest_bytes_persisted,
        last_record_seq: boundary.last_manifest_record_seq,
        last_work_ordinal: boundary.last_manifest_work_ordinal,
        sealed: boundary.manifest_sealed,
    }
}

fn send_protocol_error<T>(stream: &mut T, message: impl Into<String>) -> Result<()>
where
    T: Write,
{
    protocol::write_message(
        stream,
        &Message::Error(protocol::ErrorMessage {
            message: message.into(),
        }),
    )
}

impl ManifestReceiveState {
    fn new(state: &ReceiverState, ack_records: u64, ack_bytes: usize) -> Result<Self> {
        if ack_records == 0 {
            bail!("manifest ack cadence must require at least one record");
        }
        if ack_records > protocol::MAX_UNACKED_MANIFEST_RECORDS {
            bail!(
                "manifest ack record window exceeds protocol cap: {} > {}",
                ack_records,
                protocol::MAX_UNACKED_MANIFEST_RECORDS
            );
        }
        if ack_bytes == 0 {
            bail!("manifest ack cadence must require at least one byte");
        }
        if ack_bytes > protocol::MAX_UNACKED_MANIFEST_BYTES {
            bail!(
                "manifest ack byte window exceeds protocol cap: {} > {}",
                ack_bytes,
                protocol::MAX_UNACKED_MANIFEST_BYTES
            );
        }

        let durable_boundary = state.journal.manifest_boundary();
        let appender = ReceiverTempFileAppender::open(
            state.paths.manifest_path(),
            ReceiverTempFileKind::Manifest,
        )?;
        if appender.current_len() != durable_boundary.manifest_bytes_persisted {
            bail!(
                "receiver manifest appender length {} does not match committed boundary {}",
                appender.current_len(),
                durable_boundary.manifest_bytes_persisted
            );
        }

        Ok(Self {
            started: false,
            ack_records,
            ack_bytes,
            last_acked_record_seq: durable_boundary.last_manifest_record_seq.unwrap_or(0),
            bytes_since_ack: 0,
            appender,
            durable_boundary,
            staged_boundary: durable_boundary,
        })
    }

    fn current_boundary(&self, state: &ReceiverState) -> protocol::ManifestBoundary {
        receiver_manifest_boundary(state)
    }

    fn handle_start<T>(
        &mut self,
        stream: &mut T,
        state: &ReceiverState,
        start: ManifestStart,
    ) -> Result<protocol::ManifestBoundary>
    where
        T: Read + Write,
    {
        let current = self.current_boundary(state);
        if start.base_manifest != current {
            send_protocol_error(stream, "v3 manifest start base boundary mismatch")?;
            bail!("v3 manifest start base boundary mismatch");
        }
        self.started = true;
        self.last_acked_record_seq = current.last_record_seq.unwrap_or(0);
        self.bytes_since_ack = 0;
        Ok(current)
    }

    fn handle_chunk<T>(
        &mut self,
        stream: &mut T,
        state: &mut ReceiverState,
        chunk: ManifestChunk,
        profile: &mut ReceiverProfile,
    ) -> Result<protocol::ManifestBoundary>
    where
        T: Read + Write,
    {
        let stage_started = Instant::now();
        let boundary = match state.stage_manifest_chunk_with_appender(
            self.staged_boundary,
            &mut self.appender,
            &chunk.bytes,
            chunk.last_record_seq,
            chunk.last_work_ordinal,
            false,
        ) {
            Ok(boundary) => boundary,
            Err(error) => {
                if let Some(message) = receiver_storage_exhausted_protocol_message(&error) {
                    send_protocol_error(stream, message)?;
                }
                return Err(error);
            }
        };
        profile.record_manifest_stage(chunk.bytes.len() as u64, stage_started.elapsed());
        self.staged_boundary = boundary;
        self.bytes_since_ack = self.bytes_since_ack.saturating_add(chunk.bytes.len());
        let records_since_ack = self
            .staged_boundary
            .last_manifest_record_seq
            .unwrap_or(self.last_acked_record_seq)
            .saturating_sub(self.last_acked_record_seq);
        if self.bytes_since_ack >= self.ack_bytes || records_since_ack >= self.ack_records {
            let boundary = self.commit_staged_boundary(stream, state, profile)?;
            self.last_acked_record_seq = boundary
                .last_record_seq
                .unwrap_or(self.last_acked_record_seq);
            self.bytes_since_ack = 0;
        }
        Ok(protocol::ManifestBoundary {
            manifest_bytes_persisted: self.staged_boundary.manifest_bytes_persisted,
            last_record_seq: self.staged_boundary.last_manifest_record_seq,
            last_work_ordinal: self.staged_boundary.last_manifest_work_ordinal,
            sealed: self.staged_boundary.manifest_sealed,
        })
    }

    fn handle_seal<T>(
        &mut self,
        stream: &mut T,
        state: &mut ReceiverState,
        seal: ManifestSeal,
        profile: &mut ReceiverProfile,
    ) -> Result<protocol::ManifestBoundary>
    where
        T: Read + Write,
    {
        let stage_started = Instant::now();
        let boundary = match state.stage_manifest_chunk_with_appender(
            self.staged_boundary,
            &mut self.appender,
            &[],
            seal.final_record_seq,
            seal.final_work_ordinal,
            true,
        ) {
            Ok(boundary) => boundary,
            Err(error) => {
                if let Some(message) = receiver_storage_exhausted_protocol_message(&error) {
                    send_protocol_error(stream, message)?;
                }
                return Err(error);
            }
        };
        profile.record_manifest_stage(0, stage_started.elapsed());
        self.staged_boundary = boundary;
        let boundary = self.commit_staged_boundary(stream, state, profile)?;
        self.last_acked_record_seq = boundary
            .last_record_seq
            .unwrap_or(self.last_acked_record_seq);
        self.bytes_since_ack = 0;
        Ok(boundary)
    }

    fn commit_staged_boundary<T>(
        &mut self,
        stream: &mut T,
        state: &mut ReceiverState,
        profile: &mut ReceiverProfile,
    ) -> Result<protocol::ManifestBoundary>
    where
        T: Write,
    {
        let committed_bytes = self
            .staged_boundary
            .manifest_bytes_persisted
            .saturating_sub(self.durable_boundary.manifest_bytes_persisted);
        let commit_started = Instant::now();
        let boundary = match state.commit_manifest_boundary_with_appender(
            &mut self.appender,
            self.durable_boundary,
            self.staged_boundary,
        ) {
            Ok(boundary) => boundary,
            Err(error) => {
                if let Some(message) = receiver_storage_exhausted_protocol_message(&error) {
                    send_protocol_error(stream, message)?;
                }
                return Err(error);
            }
        };
        profile.record_manifest_commit(committed_bytes, commit_started.elapsed());
        self.durable_boundary = boundary;
        let boundary = protocol::ManifestBoundary {
            manifest_bytes_persisted: boundary.manifest_bytes_persisted,
            last_record_seq: boundary.last_manifest_record_seq,
            last_work_ordinal: boundary.last_manifest_work_ordinal,
            sealed: boundary.manifest_sealed,
        };
        let ack_started = Instant::now();
        protocol::write_message(
            stream,
            &Message::ManifestAck(protocol::ManifestAck {
                committed_manifest: boundary.clone(),
            }),
        )?;
        profile.record_manifest_ack_flush(ack_started.elapsed());
        Ok(boundary)
    }
}

fn receive_issue_tail_from_chunk<T>(
    stream: &mut T,
    state: &ReceiverState,
    first_chunk: protocol::IssueChunk,
    profile: &mut ReceiverProfile,
) -> Result<(usize, protocol::Message)>
where
    T: Read + Write,
{
    let issue_started = Instant::now();
    discard_issue_journal_temp(state)?;
    if let Err(error) = append_issue_journal_chunk(state, &first_chunk.entries) {
        if let Some(message) = receiver_storage_exhausted_protocol_message(&error) {
            let _ = discard_issue_journal_temp(state);
            send_protocol_error(stream, message)?;
        }
        let _ = discard_issue_journal_temp(state);
        return Err(error);
    }

    let mut chunks_received = 1_usize;
    loop {
        match protocol::read_message(stream)? {
            protocol::Message::IssueChunk(chunk) => {
                if let Err(error) = append_issue_journal_chunk(state, &chunk.entries) {
                    if let Some(message) = receiver_storage_exhausted_protocol_message(&error) {
                        let _ = discard_issue_journal_temp(state);
                        send_protocol_error(stream, message)?;
                    }
                    let _ = discard_issue_journal_temp(state);
                    return Err(error);
                }
                chunks_received = chunks_received.saturating_add(1);
            }
            protocol::Message::Error(error) => {
                let _ = discard_issue_journal_temp(state);
                bail!("peer aborted during v3 issue tail: {}", error.message);
            }
            message => {
                profile.record_issue_tail(chunks_received as u64, issue_started.elapsed());
                return Ok((chunks_received, message));
            }
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn receive_dump<T>(
    stream: &mut T,
    data_dir: impl Into<PathBuf>,
    manifest_ack_records: u64,
    manifest_ack_bytes: usize,
) -> Result<ReceiveReport>
where
    T: Read + Write,
{
    receive_dump_checked(
        stream,
        data_dir,
        None,
        manifest_ack_records,
        manifest_ack_bytes,
    )
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn receive_dump_checked<T>(
    stream: &mut T,
    data_dir: impl Into<PathBuf>,
    expected_dump_id: Option<&str>,
    manifest_ack_records: u64,
    manifest_ack_bytes: usize,
) -> Result<ReceiveReport>
where
    T: Read + Write,
{
    let session_started = Instant::now();
    let mut state = respond_resume_query_checked(stream, data_dir, expected_dump_id)?;
    let mut profile = ReceiverProfile::default();
    let resume_state = state.resume_state();
    let mut manifest = ManifestReceiveState::new(&state, manifest_ack_records, manifest_ack_bytes)?;
    let mut manifest_started = false;
    debug!(
        "receiver v3 dump session ready: dump_id={} resume_frame={} resume_work_ordinal={:?} archive_len={} manifest_record_seq={:?} manifest_work_ordinal={:?} manifest_sealed={}",
        state.paths.dump_id(),
        resume_state.archive.last_committed_frame,
        resume_state.archive.last_committed_work_ordinal,
        resume_state.archive.archive_len,
        resume_state.manifest.last_record_seq,
        resume_state.manifest.last_work_ordinal,
        resume_state.manifest.sealed
    );

    loop {
        match protocol::read_message(stream)? {
            Message::ManifestStart(start) => {
                let boundary = manifest.handle_start(stream, &state, start)?;
                manifest_started = true;
                debug!(
                    "receiver v3 manifest transfer started: dump_id={} record_seq={:?} work_ordinal={:?} sealed={}",
                    state.paths.dump_id(),
                    boundary.last_record_seq,
                    boundary.last_work_ordinal,
                    boundary.sealed
                );
            }
            Message::ManifestChunk(chunk) => {
                if !manifest_started {
                    send_protocol_error(
                        stream,
                        "expected v3 manifest start before manifest chunk",
                    )?;
                    bail!("unexpected v3 manifest chunk before manifest start");
                }
                let boundary = manifest.handle_chunk(stream, &mut state, chunk, &mut profile)?;
                debug!(
                    "receiver v3 manifest boundary advanced: dump_id={} record_seq={:?} work_ordinal={:?} sealed={}",
                    state.paths.dump_id(),
                    boundary.last_record_seq,
                    boundary.last_work_ordinal,
                    boundary.sealed
                );
            }
            Message::ManifestSeal(seal) => {
                if !manifest_started {
                    send_protocol_error(stream, "expected v3 manifest start before manifest seal")?;
                    bail!("unexpected v3 manifest seal before manifest start");
                }
                let boundary = manifest.handle_seal(stream, &mut state, seal, &mut profile)?;
                debug!(
                    "receiver v3 manifest sealed: dump_id={} record_seq={:?} work_ordinal={:?}",
                    state.paths.dump_id(),
                    boundary.last_record_seq,
                    boundary.last_work_ordinal
                );
            }
            Message::FrameStart(frame_start) => {
                if !manifest_started {
                    send_protocol_error(stream, "expected v3 manifest start before frame start")?;
                    bail!("unexpected v3 frame start before manifest start");
                }
                match receive_frame_event_v3_from_start(
                    stream,
                    &mut state,
                    frame_start,
                    &mut profile,
                )? {
                    super::frame::FrameReceiveResult::Committed(_) => {}
                    super::frame::FrameReceiveResult::Aborted(_) => {}
                }
            }
            Message::IssueChunk(first_chunk) => {
                if !manifest_started {
                    send_protocol_error(stream, "expected v3 manifest start before issue tail")?;
                    bail!("unexpected v3 issue tail before manifest start");
                }
                if !state.journal.manifest_sealed {
                    send_protocol_error(stream, "v3 issue tail requires sealed manifest boundary")?;
                    bail!("unexpected v3 issue tail before manifest seal");
                }
                let (chunks_received, next_message) =
                    receive_issue_tail_from_chunk(stream, &state, first_chunk, &mut profile)?;
                debug!(
                    "receiver v3 issue tail received: dump_id={} chunks={}",
                    state.paths.dump_id(),
                    chunks_received
                );
                match next_message {
                    Message::Finish(finish) => {
                        if !state.journal.manifest_sealed {
                            send_protocol_error(
                                stream,
                                "v3 finish requires sealed manifest boundary",
                            )?;
                            bail!("unexpected v3 finish before manifest seal");
                        }
                        let finish = receive_finish_message(stream, &state, finish, &mut profile)?;
                        let final_archive_path = state.paths.final_archive_path().to_path_buf();
                        profile.record_total(session_started.elapsed());
                        debug!(
                            "receiver v3 dump session completed: dump_id={} final_frame_no={} final_work_ordinal={:?} total_elapsed_ms={}",
                            state.paths.dump_id(),
                            finish.final_frame_no,
                            finish.final_work_ordinal,
                            session_started.elapsed().as_millis()
                        );
                        return Ok(ReceiveReport {
                            resume_state,
                            state,
                            finish,
                            final_archive_path,
                            profile,
                        });
                    }
                    Message::Error(error) => {
                        let _ = discard_issue_journal_temp(&state);
                        bail!("peer aborted during v3 issue tail: {}", error.message);
                    }
                    unexpected => {
                        let _ = discard_issue_journal_temp(&state);
                        send_protocol_error(stream, "expected v3 finish after typed issue tail")?;
                        bail!(
                            "unexpected v3 message after issue tail: {:?}",
                            unexpected.kind()
                        );
                    }
                }
            }
            Message::Finish(finish) => {
                if !manifest_started {
                    send_protocol_error(stream, "expected v3 manifest start before finish")?;
                    bail!("unexpected v3 finish before manifest start");
                }
                if !state.journal.manifest_sealed {
                    send_protocol_error(stream, "v3 finish requires sealed manifest boundary")?;
                    bail!("unexpected v3 finish before manifest seal");
                }
                let finish = receive_finish_message(stream, &state, finish, &mut profile)?;
                let final_archive_path = state.paths.final_archive_path().to_path_buf();
                profile.record_total(session_started.elapsed());
                debug!(
                    "receiver v3 dump session completed: dump_id={} final_frame_no={} final_work_ordinal={:?} total_elapsed_ms={}",
                    state.paths.dump_id(),
                    finish.final_frame_no,
                    finish.final_work_ordinal,
                    session_started.elapsed().as_millis()
                );
                return Ok(ReceiveReport {
                    resume_state,
                    state,
                    finish,
                    final_archive_path,
                    profile,
                });
            }
            Message::Error(error) => {
                let _ = discard_issue_journal_temp(&state);
                bail!("peer aborted during v3 dump session: {}", error.message);
            }
            unexpected => {
                send_protocol_error(
                    stream,
                    "expected v3 manifest, frame, issue tail, or finish message",
                )?;
                bail!(
                    "unexpected v3 message during dump session: {:?}",
                    unexpected.kind()
                );
            }
        }
    }
}

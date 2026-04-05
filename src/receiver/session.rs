use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Result, bail};
use log::debug;

use crate::network::protocol::{
    self, FinishV3, ManifestChunk, ManifestSeal, ManifestStart, Message, ResumeStateV3, V3Message,
};

use super::ReceiverState;
use super::finish::{
    append_issue_journal_v3_chunk, discard_issue_journal_v3_temp, receive_finish_message,
    receive_finish_message_v3,
};
use super::frame::{
    FrameReceiveResult, receive_frame_event_from_start, receive_frame_event_v3_from_start,
};
use super::resume::{respond_resume_query_checked, respond_resume_query_v3_checked};
use super::shared::{receiver_storage_exhausted_protocol_message, send_protocol_error};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiveReport {
    pub resume_state: protocol::ResumeState,
    pub state: ReceiverState,
    pub finish: protocol::Finish,
    pub final_archive_path: PathBuf,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiveReportV3 {
    pub resume_state: ResumeStateV3,
    pub state: ReceiverState,
    pub finish: FinishV3,
    pub final_archive_path: PathBuf,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct ManifestReceiveStateV3 {
    started: bool,
    ack_records: u64,
    ack_bytes: usize,
    last_acked_record_seq: u64,
    bytes_since_ack: usize,
}

#[cfg_attr(not(test), allow(dead_code))]
fn receiver_manifest_boundary_v3(state: &ReceiverState) -> protocol::ManifestBoundaryV3 {
    let boundary = state.journal.manifest_boundary();
    protocol::ManifestBoundaryV3 {
        manifest_bytes_persisted: boundary.manifest_bytes_persisted,
        last_record_seq: boundary.last_manifest_record_seq,
        last_work_ordinal: boundary.last_manifest_work_ordinal,
        sealed: boundary.manifest_sealed,
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn send_protocol_error_v3<T>(stream: &mut T, message: impl Into<String>) -> Result<()>
where
    T: Write,
{
    protocol::write_message_v3(
        stream,
        &protocol::V3Message::Error(protocol::ErrorMessage {
            message: message.into(),
        }),
    )
}

#[cfg_attr(not(test), allow(dead_code))]
impl ManifestReceiveStateV3 {
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

        Ok(Self {
            started: false,
            ack_records,
            ack_bytes,
            last_acked_record_seq: state.journal.last_manifest_record_seq.unwrap_or(0),
            bytes_since_ack: 0,
        })
    }

    fn current_boundary(&self, state: &ReceiverState) -> protocol::ManifestBoundaryV3 {
        receiver_manifest_boundary_v3(state)
    }

    fn handle_start<T>(
        &mut self,
        stream: &mut T,
        state: &ReceiverState,
        start: ManifestStart,
    ) -> Result<protocol::ManifestBoundaryV3>
    where
        T: Read + Write,
    {
        let current = self.current_boundary(state);
        if start.base_manifest != current {
            send_protocol_error_v3(stream, "v3 manifest start base boundary mismatch")?;
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
    ) -> Result<protocol::ManifestBoundaryV3>
    where
        T: Read + Write,
    {
        let boundary = match state.commit_manifest_chunk(
            &chunk.bytes,
            chunk.last_record_seq,
            chunk.last_work_ordinal,
            false,
        ) {
            Ok(boundary) => boundary,
            Err(error) => {
                if let Some(message) = receiver_storage_exhausted_protocol_message(&error) {
                    send_protocol_error_v3(stream, message)?;
                }
                return Err(error);
            }
        };
        let boundary = protocol::ManifestBoundaryV3 {
            manifest_bytes_persisted: boundary.manifest_bytes_persisted,
            last_record_seq: boundary.last_manifest_record_seq,
            last_work_ordinal: boundary.last_manifest_work_ordinal,
            sealed: boundary.manifest_sealed,
        };
        self.bytes_since_ack = self.bytes_since_ack.saturating_add(chunk.bytes.len());
        let records_since_ack = boundary
            .last_record_seq
            .unwrap_or(self.last_acked_record_seq)
            .saturating_sub(self.last_acked_record_seq);
        if self.bytes_since_ack >= self.ack_bytes || records_since_ack >= self.ack_records {
            protocol::write_message_v3(
                stream,
                &V3Message::ManifestAck(protocol::ManifestAck {
                    committed_manifest: boundary.clone(),
                }),
            )?;
            self.last_acked_record_seq = boundary
                .last_record_seq
                .unwrap_or(self.last_acked_record_seq);
            self.bytes_since_ack = 0;
        }
        Ok(boundary)
    }

    fn handle_seal<T>(
        &mut self,
        stream: &mut T,
        state: &mut ReceiverState,
        seal: ManifestSeal,
    ) -> Result<protocol::ManifestBoundaryV3>
    where
        T: Read + Write,
    {
        let boundary = match state.commit_manifest_chunk(
            &[],
            seal.final_record_seq,
            seal.final_work_ordinal,
            true,
        ) {
            Ok(boundary) => boundary,
            Err(error) => {
                if let Some(message) = receiver_storage_exhausted_protocol_message(&error) {
                    send_protocol_error_v3(stream, message)?;
                }
                return Err(error);
            }
        };
        let boundary = protocol::ManifestBoundaryV3 {
            manifest_bytes_persisted: boundary.manifest_bytes_persisted,
            last_record_seq: boundary.last_manifest_record_seq,
            last_work_ordinal: boundary.last_manifest_work_ordinal,
            sealed: boundary.manifest_sealed,
        };
        protocol::write_message_v3(
            stream,
            &V3Message::ManifestAck(protocol::ManifestAck {
                committed_manifest: boundary.clone(),
            }),
        )?;
        self.last_acked_record_seq = boundary
            .last_record_seq
            .unwrap_or(self.last_acked_record_seq);
        self.bytes_since_ack = 0;
        Ok(boundary)
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn receive_manifest_v3<T>(
    stream: &mut T,
    state: &mut ReceiverState,
    ack_records: u64,
    ack_bytes: usize,
) -> Result<protocol::ManifestBoundaryV3>
where
    T: Read + Write,
{
    let mut manifest = ManifestReceiveStateV3::new(state, ack_records, ack_bytes)?;
    let start = match protocol::read_message_v3(stream)? {
        protocol::V3Message::ManifestStart(start) => start,
        protocol::V3Message::Error(error) => {
            bail!(
                "peer aborted before v3 manifest transfer: {}",
                error.message
            )
        }
        unexpected => {
            send_protocol_error_v3(stream, "expected v3 manifest start")?;
            bail!(
                "unexpected message before v3 manifest transfer: {:?}",
                unexpected.kind()
            );
        }
    };
    let current = manifest.handle_start(stream, state, start)?;
    if current.sealed {
        return Ok(current);
    }

    loop {
        match protocol::read_message_v3(stream)? {
            protocol::V3Message::ManifestChunk(chunk) => {
                let _ = manifest.handle_chunk(stream, state, chunk)?;
            }
            protocol::V3Message::ManifestSeal(seal) => {
                return manifest.handle_seal(stream, state, seal);
            }
            protocol::V3Message::Error(error) => {
                bail!(
                    "peer aborted during v3 manifest transfer: {}",
                    error.message
                );
            }
            unexpected => {
                send_protocol_error_v3(
                    stream,
                    "expected v3 manifest chunk or manifest seal during manifest transfer",
                )?;
                bail!(
                    "unexpected message during v3 manifest transfer: {:?}",
                    unexpected.kind()
                );
            }
        }
    }
}

// Этот helper держит весь receiver-side session loop:
// после ResumeQuery он последовательно принимает committed frame-ы до Finish.
#[cfg_attr(not(test), allow(dead_code))]
fn receive_issue_tail_v3_from_chunk<T>(
    stream: &mut T,
    state: &ReceiverState,
    first_chunk: protocol::IssueChunkV3,
) -> Result<(usize, protocol::V3Message)>
where
    T: Read + Write,
{
    discard_issue_journal_v3_temp(state)?;
    if let Err(error) = append_issue_journal_v3_chunk(state, &first_chunk.entries) {
        if let Some(message) = receiver_storage_exhausted_protocol_message(&error) {
            let _ = discard_issue_journal_v3_temp(state);
            send_protocol_error_v3(stream, message)?;
        }
        let _ = discard_issue_journal_v3_temp(state);
        return Err(error);
    }

    let mut chunks_received = 1_usize;
    loop {
        match protocol::read_message_v3(stream)? {
            protocol::V3Message::IssueChunk(chunk) => {
                if let Err(error) = append_issue_journal_v3_chunk(state, &chunk.entries) {
                    if let Some(message) = receiver_storage_exhausted_protocol_message(&error) {
                        let _ = discard_issue_journal_v3_temp(state);
                        send_protocol_error_v3(stream, message)?;
                    }
                    let _ = discard_issue_journal_v3_temp(state);
                    return Err(error);
                }
                chunks_received = chunks_received.saturating_add(1);
            }
            protocol::V3Message::Error(error) => {
                let _ = discard_issue_journal_v3_temp(state);
                bail!("peer aborted during v3 issue tail: {}", error.message);
            }
            message => return Ok((chunks_received, message)),
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn receive_issue_tail_v3<T>(
    stream: &mut T,
    state: &ReceiverState,
) -> Result<(usize, protocol::V3Message)>
where
    T: Read + Write,
{
    discard_issue_journal_v3_temp(state)?;
    match protocol::read_message_v3(stream)? {
        protocol::V3Message::IssueChunk(first_chunk) => {
            receive_issue_tail_v3_from_chunk(stream, state, first_chunk)
        }
        protocol::V3Message::Error(error) => {
            let _ = discard_issue_journal_v3_temp(state);
            bail!("peer aborted during v3 issue tail: {}", error.message);
        }
        message => Ok((0, message)),
    }
}

pub fn receive_dump<T>(stream: &mut T, data_dir: impl Into<PathBuf>) -> Result<ReceiveReport>
where
    T: Read + Write,
{
    receive_dump_checked(stream, data_dir, None)
}

pub fn receive_dump_checked<T>(
    stream: &mut T,
    data_dir: impl Into<PathBuf>,
    expected_dump_id: Option<&str>,
) -> Result<ReceiveReport>
where
    T: Read + Write,
{
    let session_started = Instant::now();
    let mut state = respond_resume_query_checked(stream, data_dir, expected_dump_id)?;
    let resume_state = state.resume_state();
    let mut issue_journal = Vec::new();
    debug!(
        "receiver dump session ready: dump_id={} resume_frame={} resume_file_index={} archive_len={}",
        state.paths.dump_id(),
        resume_state.last_committed_frame,
        resume_state.last_committed_file_index,
        resume_state.archive_len
    );

    loop {
        match protocol::read_message(stream)? {
            Message::FrameStart(frame_start) => {
                match receive_frame_event_from_start(stream, &mut state, frame_start)? {
                    FrameReceiveResult::Committed(_) | FrameReceiveResult::Aborted(_) => {}
                }
            }
            Message::IssueJournalChunk(chunk) => {
                issue_journal.extend(chunk.entries);
            }
            Message::Finish(finish) => {
                let finish = receive_finish_message(stream, &state, finish, &issue_journal)?;
                let final_archive_path = state.paths.final_archive_path().to_path_buf();
                debug!(
                    "receiver dump session completed: dump_id={} final_frame_no={} final_file_index={} issue_count={} total_elapsed_ms={}",
                    state.paths.dump_id(),
                    finish.final_frame_no,
                    finish.final_file_index,
                    issue_journal.len(),
                    session_started.elapsed().as_millis()
                );
                return Ok(ReceiveReport {
                    resume_state,
                    state,
                    finish,
                    final_archive_path,
                });
            }
            Message::Error(error) => {
                bail!("peer aborted during dump session: {}", error.message);
            }
            unexpected => {
                send_protocol_error(
                    stream,
                    "expected frame start, issue journal chunk, or finish",
                )?;
                bail!(
                    "unexpected message during dump session: {:?}",
                    unexpected.kind()
                );
            }
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn receive_dump_v3<T>(
    stream: &mut T,
    data_dir: impl Into<PathBuf>,
    manifest_ack_records: u64,
    manifest_ack_bytes: usize,
) -> Result<ReceiveReportV3>
where
    T: Read + Write,
{
    receive_dump_v3_checked(
        stream,
        data_dir,
        None,
        manifest_ack_records,
        manifest_ack_bytes,
    )
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn receive_dump_v3_checked<T>(
    stream: &mut T,
    data_dir: impl Into<PathBuf>,
    expected_dump_id: Option<&str>,
    manifest_ack_records: u64,
    manifest_ack_bytes: usize,
) -> Result<ReceiveReportV3>
where
    T: Read + Write,
{
    let session_started = Instant::now();
    let mut state = respond_resume_query_v3_checked(stream, data_dir, expected_dump_id)?;
    let resume_state = state.resume_state_v3();
    let mut manifest =
        ManifestReceiveStateV3::new(&state, manifest_ack_records, manifest_ack_bytes)?;
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
        match protocol::read_message_v3(stream)? {
            V3Message::ManifestStart(start) => {
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
            V3Message::ManifestChunk(chunk) => {
                if !manifest_started {
                    send_protocol_error_v3(
                        stream,
                        "expected v3 manifest start before manifest chunk",
                    )?;
                    bail!("unexpected v3 manifest chunk before manifest start");
                }
                let boundary = manifest.handle_chunk(stream, &mut state, chunk)?;
                debug!(
                    "receiver v3 manifest boundary advanced: dump_id={} record_seq={:?} work_ordinal={:?} sealed={}",
                    state.paths.dump_id(),
                    boundary.last_record_seq,
                    boundary.last_work_ordinal,
                    boundary.sealed
                );
            }
            V3Message::ManifestSeal(seal) => {
                if !manifest_started {
                    send_protocol_error_v3(
                        stream,
                        "expected v3 manifest start before manifest seal",
                    )?;
                    bail!("unexpected v3 manifest seal before manifest start");
                }
                let boundary = manifest.handle_seal(stream, &mut state, seal)?;
                debug!(
                    "receiver v3 manifest sealed: dump_id={} record_seq={:?} work_ordinal={:?}",
                    state.paths.dump_id(),
                    boundary.last_record_seq,
                    boundary.last_work_ordinal
                );
            }
            V3Message::FrameStart(frame_start) => {
                if !manifest_started {
                    send_protocol_error_v3(
                        stream,
                        "expected v3 manifest start before frame start",
                    )?;
                    bail!("unexpected v3 frame start before manifest start");
                }
                match receive_frame_event_v3_from_start(stream, &mut state, frame_start)? {
                    super::frame::FrameReceiveResultV3::Committed(_) => {}
                    super::frame::FrameReceiveResultV3::Aborted(_) => {}
                }
            }
            V3Message::IssueChunk(first_chunk) => {
                if !manifest_started {
                    send_protocol_error_v3(stream, "expected v3 manifest start before issue tail")?;
                    bail!("unexpected v3 issue tail before manifest start");
                }
                if !state.journal.manifest_sealed {
                    send_protocol_error_v3(
                        stream,
                        "v3 issue tail requires sealed manifest boundary",
                    )?;
                    bail!("unexpected v3 issue tail before manifest seal");
                }
                let (chunks_received, next_message) =
                    receive_issue_tail_v3_from_chunk(stream, &state, first_chunk)?;
                debug!(
                    "receiver v3 issue tail received: dump_id={} chunks={}",
                    state.paths.dump_id(),
                    chunks_received
                );
                match next_message {
                    V3Message::Finish(finish) => {
                        if !state.journal.manifest_sealed {
                            send_protocol_error_v3(
                                stream,
                                "v3 finish requires sealed manifest boundary",
                            )?;
                            bail!("unexpected v3 finish before manifest seal");
                        }
                        let finish = receive_finish_message_v3(stream, &state, finish)?;
                        let final_archive_path = state.paths.final_archive_path().to_path_buf();
                        debug!(
                            "receiver v3 dump session completed: dump_id={} final_frame_no={} final_work_ordinal={:?} total_elapsed_ms={}",
                            state.paths.dump_id(),
                            finish.final_frame_no,
                            finish.final_work_ordinal,
                            session_started.elapsed().as_millis()
                        );
                        return Ok(ReceiveReportV3 {
                            resume_state,
                            state,
                            finish,
                            final_archive_path,
                        });
                    }
                    V3Message::Error(error) => {
                        let _ = discard_issue_journal_v3_temp(&state);
                        bail!("peer aborted during v3 issue tail: {}", error.message);
                    }
                    unexpected => {
                        let _ = discard_issue_journal_v3_temp(&state);
                        send_protocol_error_v3(
                            stream,
                            "expected v3 finish after typed issue tail",
                        )?;
                        bail!(
                            "unexpected v3 message after issue tail: {:?}",
                            unexpected.kind()
                        );
                    }
                }
            }
            V3Message::Finish(finish) => {
                if !manifest_started {
                    send_protocol_error_v3(stream, "expected v3 manifest start before finish")?;
                    bail!("unexpected v3 finish before manifest start");
                }
                if !state.journal.manifest_sealed {
                    send_protocol_error_v3(stream, "v3 finish requires sealed manifest boundary")?;
                    bail!("unexpected v3 finish before manifest seal");
                }
                let finish = receive_finish_message_v3(stream, &state, finish)?;
                let final_archive_path = state.paths.final_archive_path().to_path_buf();
                debug!(
                    "receiver v3 dump session completed: dump_id={} final_frame_no={} final_work_ordinal={:?} total_elapsed_ms={}",
                    state.paths.dump_id(),
                    finish.final_frame_no,
                    finish.final_work_ordinal,
                    session_started.elapsed().as_millis()
                );
                return Ok(ReceiveReportV3 {
                    resume_state,
                    state,
                    finish,
                    final_archive_path,
                });
            }
            V3Message::Error(error) => {
                let _ = discard_issue_journal_v3_temp(&state);
                bail!("peer aborted during v3 dump session: {}", error.message);
            }
            unexpected => {
                send_protocol_error_v3(
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

#[cfg(test)]
mod tests {
    use std::{fs, io::Cursor};

    use super::*;
    use crate::network::protocol::{
        AbortAck, Ack, ErrorMessage, FrameChunk, FrameStart, IssueChunkV3, IssueCodeV3,
        IssueEntryV3, IssueJournalChunk, IssueJournalEntry, IssueJournalReason, IssueScopeV3,
        ManifestAck, ManifestBoundaryV3, ManifestChunk, ManifestSeal, ManifestStart, Message,
        ResumeQuery, ResumeState, V3Message,
    };
    use crate::receiver::shared::{ReceiverTempFileKind, test_hooks};
    use crate::receiver::test_support::*;
    use crate::sender::prepare_dump;
    use crate::sender::source::manifest::{
        ManifestRecord, ManifestRecordKind, ManifestRecordPayload, ManifestRecordSeq,
        ManifestSpoolWriter, ManifestWorkOrdinal,
    };
    use xxhash_rust::xxh3::Xxh3Default;

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

    fn manifest_file_record(record_seq: u64, work_ordinal: u64, name: &str) -> ManifestRecord {
        ManifestRecord::new(
            ManifestRecordSeq(record_seq),
            ManifestRecordKind::File,
            0,
            name,
            Some(ManifestWorkOrdinal(work_ordinal)),
            ManifestRecordPayload::None,
        )
        .unwrap()
    }

    fn typed_issue_entry(scope: IssueScopeV3, path: &str, code: IssueCodeV3) -> IssueEntryV3 {
        IssueEntryV3 {
            scope,
            record_seq: match scope {
                IssueScopeV3::File => None,
                IssueScopeV3::DiscoveryEntry => Some(7),
                IssueScopeV3::DiscoverySubtree => Some(8),
                IssueScopeV3::SenderState => Some(9),
            },
            work_ordinal: match scope {
                IssueScopeV3::File => Some(3),
                _ => None,
            },
            path: path.to_owned(),
            attempts: if matches!(scope, IssueScopeV3::File) {
                2
            } else {
                0
            },
            code,
            detail: format!("detail:{path}"),
            context: format!("context:{path}"),
        }
    }

    #[test]
    fn receive_manifest_v3_batches_durable_acks_and_seals_manifest_state() {
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path(), "dump-v3-manifest").unwrap();
        let base_manifest = ManifestBoundaryV3 {
            manifest_bytes_persisted: 0,
            last_record_seq: None,
            last_work_ordinal: None,
            sealed: false,
        };
        let records = [
            manifest_file_record(1, 1, "alpha.txt"),
            manifest_file_record(2, 2, "beta.txt"),
            manifest_file_record(3, 3, "gamma.txt"),
        ];
        let encoded_first = ManifestSpoolWriter::encode_record(&records[0]).unwrap();
        let encoded_second = ManifestSpoolWriter::encode_record(&records[1]).unwrap();
        let encoded_third = ManifestSpoolWriter::encode_record(&records[2]).unwrap();
        let mut stream = ScriptedStream::new(
            [
                encode_message_v3(&V3Message::ManifestStart(ManifestStart {
                    base_manifest: base_manifest.clone(),
                })),
                encode_message_v3(&V3Message::ManifestChunk(ManifestChunk {
                    bytes: [encoded_first, encoded_second].concat(),
                    last_record_seq: Some(2),
                    last_work_ordinal: Some(2),
                })),
                encode_message_v3(&V3Message::ManifestChunk(ManifestChunk {
                    bytes: encoded_third,
                    last_record_seq: Some(3),
                    last_work_ordinal: Some(3),
                })),
                encode_message_v3(&V3Message::ManifestSeal(ManifestSeal {
                    final_record_seq: Some(3),
                    final_work_ordinal: Some(3),
                })),
            ]
            .concat(),
        );

        let committed =
            super::receive_manifest_v3(&mut stream, &mut state, 2, 1024 * 1024).unwrap();

        assert_eq!(committed.last_record_seq, Some(3));
        assert_eq!(committed.last_work_ordinal, Some(3));
        assert!(committed.sealed);
        assert_eq!(state.journal.last_manifest_record_seq, Some(3));
        assert_eq!(state.journal.last_manifest_work_ordinal, Some(3));
        assert!(state.journal.manifest_sealed);
        let acks = decode_messages_v3(&stream.written);
        assert_eq!(acks.len(), 2);
        let V3Message::ManifestAck(ManifestAck {
            committed_manifest: first_ack,
        }) = &acks[0]
        else {
            panic!("expected first manifest ack");
        };
        assert_eq!(first_ack.last_record_seq, Some(2));
        assert_eq!(first_ack.last_work_ordinal, Some(2));
        assert!(!first_ack.sealed);
        let V3Message::ManifestAck(ManifestAck {
            committed_manifest: final_ack,
        }) = &acks[1]
        else {
            panic!("expected final manifest ack");
        };
        assert_eq!(final_ack, &committed);
    }

    #[test]
    fn receive_manifest_v3_short_circuits_when_matching_base_is_already_sealed() {
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path(), "dump-v3-sealed").unwrap();
        state
            .commit_manifest_chunk(b"alpha", Some(1), Some(1), true)
            .unwrap();
        let committed = super::receiver_manifest_boundary_v3(&state);
        let before_bytes = fs::read(state.paths.manifest_path()).unwrap();
        let mut stream = ScriptedStream::new(encode_message_v3(&V3Message::ManifestStart(
            ManifestStart {
                base_manifest: committed.clone(),
            },
        )));

        let observed = super::receive_manifest_v3(&mut stream, &mut state, 2, 1024).unwrap();

        assert_eq!(observed, committed);
        assert!(stream.written.is_empty());
        assert_eq!(fs::read(state.paths.manifest_path()).unwrap(), before_bytes);
    }

    #[test]
    fn receive_manifest_v3_rejects_base_boundary_mismatch_and_emits_error() {
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path(), "dump-v3-mismatch").unwrap();
        let mut stream = ScriptedStream::new(encode_message_v3(&V3Message::ManifestStart(
            ManifestStart {
                base_manifest: ManifestBoundaryV3 {
                    manifest_bytes_persisted: 17,
                    last_record_seq: Some(1),
                    last_work_ordinal: Some(1),
                    sealed: false,
                },
            },
        )));

        let error = super::receive_manifest_v3(&mut stream, &mut state, 2, 1024).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("v3 manifest start base boundary mismatch")
        );
        assert_eq!(
            decode_messages_v3(&stream.written),
            vec![V3Message::Error(ErrorMessage {
                message: "v3 manifest start base boundary mismatch".to_owned(),
            })]
        );
        assert_eq!(
            super::receiver_manifest_boundary_v3(&state).manifest_bytes_persisted,
            0
        );
    }

    #[test]
    fn receive_manifest_v3_emits_explicit_error_on_storage_full_without_advancing_boundary() {
        let _guard = FsFailureGuard::new();
        let temp = TestDir::new();
        let mut state = ReceiverState::open(temp.path(), "dump-v3-manifest-full").unwrap();
        test_hooks::fail_next_append_after_write(state.paths.manifest_path());
        let mut stream = ScriptedStream::new(
            [
                encode_message_v3(&V3Message::ManifestStart(ManifestStart {
                    base_manifest: ManifestBoundaryV3 {
                        manifest_bytes_persisted: 0,
                        last_record_seq: None,
                        last_work_ordinal: None,
                        sealed: false,
                    },
                })),
                encode_message_v3(&V3Message::ManifestChunk(ManifestChunk {
                    bytes: b"alpha".to_vec(),
                    last_record_seq: Some(1),
                    last_work_ordinal: Some(1),
                })),
            ]
            .concat(),
        );

        let error = super::receive_manifest_v3(&mut stream, &mut state, 2, 1024).unwrap_err();
        let replies = decode_messages_v3(&stream.written);

        assert!(!error.to_string().is_empty());
        assert_eq!(replies.len(), 1);
        let V3Message::Error(ErrorMessage { message }) = &replies[0] else {
            panic!("expected v3 error");
        };
        assert!(message.contains("receiver manifest storage exhausted during sync"));
        assert!(message.contains("committed boundaries preserved"));
        assert_eq!(
            super::receiver_manifest_boundary_v3(&state).manifest_bytes_persisted,
            0
        );
        assert_eq!(fs::metadata(state.paths.manifest_path()).unwrap().len(), 0);
    }

    #[test]
    fn receive_issue_tail_v3_persists_typed_chunks_and_returns_next_message() {
        let temp = TestDir::new();
        let state = ReceiverState::open(temp.path(), "dump-v3-issues").unwrap();
        let finish = V3Message::Finish(protocol::FinishV3 {
            final_frame_no: 3,
            final_work_ordinal: Some(3),
        });
        let mut stream = ScriptedStream::new(
            [
                encode_message_v3(&V3Message::IssueChunk(IssueChunkV3 {
                    entries: vec![
                        typed_issue_entry(IssueScopeV3::File, "alpha.txt", IssueCodeV3::OpenFailed),
                        typed_issue_entry(
                            IssueScopeV3::DiscoveryEntry,
                            "fifo.sock",
                            IssueCodeV3::UnsupportedEntry,
                        ),
                    ],
                })),
                encode_message_v3(&V3Message::IssueChunk(IssueChunkV3 {
                    entries: vec![typed_issue_entry(
                        IssueScopeV3::SenderState,
                        "sender-state",
                        IssueCodeV3::SenderStateEvent,
                    )],
                })),
                encode_message_v3(&finish),
            ]
            .concat(),
        );

        let (chunks_received, next_message) =
            super::receive_issue_tail_v3(&mut stream, &state).unwrap();

        assert_eq!(chunks_received, 2);
        assert_eq!(next_message, finish);
        assert!(stream.written.is_empty());
        let temp_path = state.paths.issue_journal_temp_path();
        let contents = fs::read_to_string(&temp_path).unwrap();
        assert!(
            contents
                .contains("scope\trecord_seq\twork_ordinal\tattempts\tcode\tpath\tdetail\tcontext")
        );
        assert!(contents.contains("file\t\t3\t2\topen_failed\talpha.txt"));
        assert!(contents.contains("discovery_entry\t7\t\t0\tunsupported_entry\tfifo.sock"));
        assert!(contents.contains("sender_state\t9\t\t0\tsender_state_event\tsender-state"));
        assert!(!state.paths.issue_journal_path().exists());
    }

    #[test]
    fn receive_issue_tail_v3_emits_explicit_error_on_quota_exhaustion_and_discards_temp_file() {
        let _guard = FsFailureGuard::new();
        let temp = TestDir::new();
        let state = ReceiverState::open(temp.path(), "dump-v3-issue-quota").unwrap();
        test_hooks::set_quota_override(ReceiverTempFileKind::IssueJournalTemp, 8);
        let finish = V3Message::Finish(protocol::FinishV3 {
            final_frame_no: 1,
            final_work_ordinal: Some(1),
        });
        let mut stream = ScriptedStream::new(
            [
                encode_message_v3(&V3Message::IssueChunk(IssueChunkV3 {
                    entries: vec![typed_issue_entry(
                        IssueScopeV3::File,
                        "alpha.txt",
                        IssueCodeV3::OpenFailed,
                    )],
                })),
                encode_message_v3(&finish),
            ]
            .concat(),
        );

        let error = super::receive_issue_tail_v3(&mut stream, &state).unwrap_err();
        let replies = decode_messages_v3(&stream.written);

        assert!(error.to_string().contains("quota exceeded"));
        assert_eq!(replies.len(), 1);
        let V3Message::Error(ErrorMessage { message }) = &replies[0] else {
            panic!("expected v3 error");
        };
        assert!(message.contains("receiver issue-tail temp storage quota exhausted"));
        assert!(message.contains("committed boundaries preserved"));
        assert!(!state.paths.issue_journal_temp_path().exists());
        assert!(!state.paths.issue_journal_path().exists());
    }

    #[test]
    fn receive_dump_v3_accepts_interleaved_manifest_frames_issue_tail_and_finish() {
        let temp = TestDir::new();
        let dump_id = "dump-v3-session";
        let first_record = manifest_file_record(1, 1, "alpha.txt");
        let second_record = manifest_file_record(2, 2, "beta.txt");
        let first_bytes = ManifestSpoolWriter::encode_record(&first_record).unwrap();
        let second_bytes = ManifestSpoolWriter::encode_record(&second_record).unwrap();
        let chunk = b"abc".as_slice();
        let mut hasher = Xxh3Default::new();
        hasher.update(chunk);
        let request = [
            encode_message_v3(&V3Message::ResumeQuery(ResumeQuery {
                dump_id: dump_id.to_owned(),
            })),
            encode_message_v3(&V3Message::ManifestStart(ManifestStart {
                base_manifest: ManifestBoundaryV3 {
                    manifest_bytes_persisted: 0,
                    last_record_seq: None,
                    last_work_ordinal: None,
                    sealed: false,
                },
            })),
            encode_message_v3(&V3Message::ManifestChunk(ManifestChunk {
                bytes: first_bytes,
                last_record_seq: Some(1),
                last_work_ordinal: Some(1),
            })),
            encode_message_v3(&V3Message::FrameStart(protocol::FrameStartV3 {
                frame_no: 1,
                first_work_ordinal: 1,
                level: 3,
                threads: 1,
            })),
            encode_message_v3(&V3Message::FrameChunk(FrameChunk {
                bytes: chunk.to_vec(),
            })),
            encode_message_v3(&V3Message::FrameEnd(protocol::FrameEndV3 {
                frame_no: 1,
                last_work_ordinal: 1,
                compressed_len: chunk.len() as u64,
                compressed_xxh3_64: hasher.digest(),
            })),
            encode_message_v3(&V3Message::ManifestChunk(ManifestChunk {
                bytes: second_bytes,
                last_record_seq: Some(2),
                last_work_ordinal: Some(2),
            })),
            encode_message_v3(&V3Message::ManifestSeal(ManifestSeal {
                final_record_seq: Some(2),
                final_work_ordinal: Some(2),
            })),
            encode_message_v3(&V3Message::FrameStart(protocol::FrameStartV3 {
                frame_no: 2,
                first_work_ordinal: 2,
                level: 3,
                threads: 1,
            })),
            encode_message_v3(&V3Message::FrameEnd(protocol::FrameEndV3 {
                frame_no: 2,
                last_work_ordinal: 2,
                compressed_len: 0,
                compressed_xxh3_64: Xxh3Default::new().digest(),
            })),
            encode_message_v3(&V3Message::IssueChunk(IssueChunkV3 {
                entries: vec![IssueEntryV3 {
                    scope: IssueScopeV3::File,
                    record_seq: None,
                    work_ordinal: Some(2),
                    path: "beta.txt".to_owned(),
                    attempts: 2,
                    code: IssueCodeV3::OpenFailed,
                    detail: "detail:beta.txt".to_owned(),
                    context: String::new(),
                }],
            })),
            encode_message_v3(&V3Message::Finish(protocol::FinishV3 {
                final_frame_no: 2,
                final_work_ordinal: Some(2),
            })),
        ]
        .concat();
        let mut stream = ScriptedStream::new(request);

        let report = super::receive_dump_v3(&mut stream, temp.path(), 1, 1024 * 1024).unwrap();
        let replies = decode_messages_v3(&stream.written);

        assert_eq!(report.finish.final_frame_no, 2);
        assert_eq!(report.finish.final_work_ordinal, Some(2));
        assert!(report.final_archive_path.exists());
        assert_eq!(
            fs::read(report.state.paths.partial_archive_path()).unwrap_or_default(),
            Vec::<u8>::new()
        );
        let issue_journal = fs::read_to_string(report.state.paths.issue_journal_path()).unwrap();
        assert!(issue_journal.contains("file\t\t2\t2\topen_failed\tbeta.txt"));
        assert!(!report.state.paths.issue_journal_temp_path().exists());

        assert!(matches!(replies[0], V3Message::ResumeState(_)));
        assert!(matches!(replies[1], V3Message::ManifestAck(_)));
        assert!(matches!(replies[2], V3Message::Ack(_)));
        assert!(matches!(replies[3], V3Message::ManifestAck(_)));
        assert!(matches!(replies[4], V3Message::ManifestAck(_)));
        assert!(matches!(replies[5], V3Message::Ack(_)));
        assert_eq!(replies[6], V3Message::FinishAck);
    }

    #[test]
    fn receiver_state_survives_repeated_restarts_between_frames_and_finalize() {
        let source = TestDir::new();
        fs::create_dir_all(source.path().join("nested")).unwrap();
        fs::write(source.path().join("alpha.txt"), b"alpha").unwrap();
        fs::write(source.path().join("nested").join("beta.txt"), b"beta").unwrap();
        fs::write(source.path().join("nested").join("gamma.txt"), b"gamma").unwrap();
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
        let ack1 = crate::receiver::receive_frame(
            &mut Cursor::new(encode_prepared_frame(&prepared.frames[0])),
            &mut state,
        )
        .unwrap();
        drop(state);

        let mut reopened = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        assert_eq!(reopened.journal.last_committed_frame, ack1.frame_no);
        assert_eq!(
            reopened.journal.last_committed_file_index,
            ack1.last_file_index
        );
        assert_eq!(reopened.journal.archive_len, ack1.archive_len);
        crate::receiver::receive_frame(
            &mut Cursor::new(encode_prepared_frame(&prepared.frames[1])),
            &mut reopened,
        )
        .unwrap();
        drop(reopened);

        let mut reopened_again = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        crate::receiver::receive_frame(
            &mut Cursor::new(encode_prepared_frame(&prepared.frames[2])),
            &mut reopened_again,
        )
        .unwrap();

        let mut finish_stream = Cursor::new(encode_message(&Message::Finish(protocol::Finish {
            final_frame_no: prepared.final_frame_no,
            final_file_index: prepared.final_file_index,
            final_file_path: prepared.final_file_path.clone(),
        })));
        crate::receiver::receive_finish(&mut finish_stream, &reopened_again).unwrap();

        assert_eq!(
            archive_entries(reopened_again.paths.final_archive_path()),
            vec![
                ("alpha.txt".to_owned(), b"alpha".to_vec()),
                ("nested/beta.txt".to_owned(), b"beta".to_vec()),
                ("nested/gamma.txt".to_owned(), b"gamma".to_vec()),
            ]
        );
    }

    #[test]
    fn receive_dump_surfaces_peer_error_after_resume_state() {
        let temp = TestDir::new();
        let request = [
            encode_message(&Message::ResumeQuery(ResumeQuery {
                dump_id: "dump-01".to_owned(),
            })),
            encode_message(&Message::Error(ErrorMessage {
                message: "cancelled".to_owned(),
            })),
        ]
        .concat();
        let mut stream = ScriptedStream::new(request);

        let error = receive_dump(&mut stream, temp.path().to_path_buf()).unwrap_err();
        let replies = decode_messages(&stream.written);

        assert!(
            error
                .to_string()
                .contains("peer aborted during dump session: cancelled")
        );
        assert_eq!(replies.len(), 1);
        assert!(matches!(replies[0], Message::ResumeState(_)));
    }

    #[test]
    fn receive_dump_rejects_unexpected_message_after_resume_state_and_emits_error() {
        let temp = TestDir::new();
        let request = [
            encode_message(&Message::ResumeQuery(ResumeQuery {
                dump_id: "dump-01".to_owned(),
            })),
            encode_message(&Message::FinishAck),
        ]
        .concat();
        let mut stream = ScriptedStream::new(request);

        let error = receive_dump(&mut stream, temp.path().to_path_buf()).unwrap_err();
        let replies = decode_messages(&stream.written);

        assert!(
            error
                .to_string()
                .contains("unexpected message during dump session: FinishAck")
        );
        assert_eq!(replies.len(), 2);
        assert!(matches!(replies[0], Message::ResumeState(_)));
        assert_eq!(
            replies[1],
            Message::Error(ErrorMessage {
                message: "expected frame start, issue journal chunk, or finish".to_owned(),
            })
        );
    }

    #[test]
    fn receive_dump_runs_full_session_until_finish() {
        let source = TestDir::new();
        fs::create_dir_all(source.path().join("nested")).unwrap();
        fs::write(source.path().join("alpha.txt"), b"alpha").unwrap();
        fs::write(source.path().join("nested").join("beta.txt"), b"beta").unwrap();

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
        let receiver_dir = TestDir::new();

        let mut request = encode_message(&Message::ResumeQuery(ResumeQuery {
            dump_id: "dump-01".to_owned(),
        }));
        for frame in &prepared.frames {
            request.extend_from_slice(&encode_message(&Message::FrameStart(FrameStart {
                frame_no: frame.outbound.frame_no,
                first_file_index: frame.outbound.first_file_index,
                first_file_path: frame.outbound.first_file_path.clone(),
                level: frame.outbound.level,
                threads: frame.outbound.threads,
            })));
            for chunk in &frame.chunks {
                request.extend_from_slice(&encode_message(&Message::FrameChunk(FrameChunk {
                    bytes: chunk.clone(),
                })));
            }
            let chunk_refs: Vec<&[u8]> = frame.chunks.iter().map(Vec::as_slice).collect();
            request.extend_from_slice(&encode_message(&Message::FrameEnd(frame_end(
                frame.outbound.frame_no,
                frame.outbound.last_file_index,
                &frame.outbound.last_file_path,
                &chunk_refs,
            ))));
        }
        request.extend_from_slice(&encode_message(&Message::Finish(protocol::Finish {
            final_frame_no: prepared.final_frame_no,
            final_file_index: prepared.final_file_index,
            final_file_path: prepared.final_file_path.clone(),
        })));

        let mut stream = ScriptedStream::new(request);

        let report = receive_dump(&mut stream, receiver_dir.path().to_path_buf()).unwrap();
        let replies = decode_messages(&stream.written);
        let mut expected_replies = vec![Message::ResumeState(report.resume_state.clone())];
        let mut archive_len = 0_u64;
        for frame in &prepared.frames {
            archive_len += frame
                .chunks
                .iter()
                .map(|chunk| chunk.len() as u64)
                .sum::<u64>();
            expected_replies.push(Message::Ack(Ack {
                frame_no: frame.outbound.frame_no,
                last_file_index: frame.outbound.last_file_index,
                last_file_path: frame.outbound.last_file_path.clone(),
                archive_len,
            }));
        }
        expected_replies.push(Message::FinishAck);

        assert_eq!(report.state.paths.dump_id(), "dump-01");
        assert_eq!(report.finish.final_frame_no, 2);
        assert_eq!(report.finish.final_file_index, 2);
        assert_eq!(report.finish.final_file_path, "nested/beta.txt");
        assert!(report.final_archive_path.exists());
        assert!(!report.state.paths.partial_archive_path().exists());
        assert!(!report.state.paths.journal_path().exists());
        assert_eq!(replies, expected_replies);
        assert_eq!(
            archive_entries(&report.final_archive_path),
            vec![
                ("alpha.txt".to_owned(), b"alpha".to_vec()),
                ("nested/beta.txt".to_owned(), b"beta".to_vec()),
            ]
        );
    }

    #[test]
    fn receive_dump_persists_uploaded_issue_journal_before_finish() {
        let receiver_dir = TestDir::new();
        let mut request = encode_message(&Message::ResumeQuery(ResumeQuery {
            dump_id: "dump-issues".to_owned(),
        }));
        request.extend_from_slice(&encode_message(&Message::IssueJournalChunk(
            IssueJournalChunk {
                entries: vec![
                    IssueJournalEntry {
                        file_index: 2,
                        file_path: "beta.txt".to_owned(),
                        attempts: 3,
                        reason: IssueJournalReason::UnstableAfterRetries,
                        last_error: "short read".to_owned(),
                    },
                    IssueJournalEntry {
                        file_index: 3,
                        file_path: "nested/gamma.txt".to_owned(),
                        attempts: 1,
                        reason: IssueJournalReason::OpenFailed,
                        last_error: "access denied".to_owned(),
                    },
                ],
            },
        )));
        request.extend_from_slice(&encode_message(&Message::Finish(protocol::Finish {
            final_frame_no: 0,
            final_file_index: 0,
            final_file_path: String::new(),
        })));

        let mut stream = ScriptedStream::new(request);

        let report = receive_dump(&mut stream, receiver_dir.path().to_path_buf()).unwrap();
        let replies = decode_messages(&stream.written);
        let issue_journal_path = receiver_dir.path().join("dump-issues.tar.zst.issues.tsv");
        let issue_journal = fs::read_to_string(&issue_journal_path).unwrap();

        assert_eq!(
            replies,
            vec![
                Message::ResumeState(report.resume_state.clone()),
                Message::FinishAck
            ]
        );
        assert!(report.final_archive_path.exists());
        assert!(issue_journal_path.exists());
        assert!(issue_journal.contains("file_index\tattempts\treason\tfile_path\tlast_error"));
        assert!(issue_journal.contains("2\t3\tunstable_after_retries\tbeta.txt\tshort read"));
        assert!(issue_journal.contains("3\t1\topen_failed\tnested/gamma.txt\taccess denied"));
    }

    #[test]
    fn receive_dump_rolls_back_explicitly_aborted_frame_and_continues() {
        let receiver_dir = TestDir::new();

        let frame1 = FrameStart {
            frame_no: 1,
            first_file_index: 1,
            first_file_path: "root/a.bin".to_owned(),
            level: 3,
            threads: 2,
        };
        let frame2 = FrameStart {
            frame_no: 2,
            first_file_index: 2,
            first_file_path: "root/b.bin".to_owned(),
            level: 3,
            threads: 2,
        };

        let mut request = encode_message(&Message::ResumeQuery(ResumeQuery {
            dump_id: "dump-02".to_owned(),
        }));
        request.extend_from_slice(&encode_message(&Message::FrameStart(frame1.clone())));
        request.extend_from_slice(&encode_message(&Message::FrameChunk(FrameChunk {
            bytes: b"aaa".to_vec(),
        })));
        request.extend_from_slice(&encode_message(&Message::FrameEnd(frame_end(
            frame1.frame_no,
            1,
            "root/a.bin",
            &[b"aaa".as_slice()],
        ))));
        request.extend_from_slice(&encode_message(&Message::FrameStart(frame2.clone())));
        request.extend_from_slice(&encode_message(&Message::FrameChunk(FrameChunk {
            bytes: b"partial".to_vec(),
        })));
        request.extend_from_slice(&encode_message(&Message::FrameAbort(
            protocol::FrameAbort {
                frame_no: frame2.frame_no,
            },
        )));
        request.extend_from_slice(&encode_message(&Message::FrameStart(frame2.clone())));
        request.extend_from_slice(&encode_message(&Message::FrameChunk(FrameChunk {
            bytes: b"bbbb".to_vec(),
        })));
        request.extend_from_slice(&encode_message(&Message::FrameEnd(frame_end(
            frame2.frame_no,
            2,
            "root/b.bin",
            &[b"bbbb".as_slice()],
        ))));
        request.extend_from_slice(&encode_message(&Message::Finish(protocol::Finish {
            final_frame_no: 2,
            final_file_index: 2,
            final_file_path: "root/b.bin".to_owned(),
        })));

        let mut stream = ScriptedStream::new(request);

        let report = receive_dump(&mut stream, receiver_dir.path().to_path_buf()).unwrap();
        let replies = decode_messages(&stream.written);

        assert_eq!(report.state.journal.archive_len, 7);
        assert_eq!(
            replies,
            vec![
                Message::ResumeState(report.resume_state.clone()),
                Message::Ack(Ack {
                    frame_no: 1,
                    last_file_index: 1,
                    last_file_path: "root/a.bin".to_owned(),
                    archive_len: 3,
                }),
                Message::AbortAck(AbortAck {
                    frame_no: 2,
                    archive_len: 3,
                }),
                Message::Ack(Ack {
                    frame_no: 2,
                    last_file_index: 2,
                    last_file_path: "root/b.bin".to_owned(),
                    archive_len: 7,
                }),
                Message::FinishAck,
            ]
        );
    }
}

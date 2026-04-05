use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use log::debug;

use crate::network::protocol::{
    self, Finish, FinishV3, IssueCodeV3, IssueEntryV3, IssueJournalEntry, IssueJournalReason,
    IssueScopeV3, Message, V3Message,
};

use super::ReceiverState;
use super::journal::recover_partial_archive;
use super::shared::{
    ReceiverTempFileKind, append_temp_file_payload, remove_file_path, rename_path,
    send_protocol_error,
};

// Finish фиксирует, что sender больше не будет присылать новые frame-ы в этой
// сессии. Перед FinishAck receiver сам дописывает tar end blocks как отдельный
// локальный zstd-frame, fsync'ает результат и переводит `.part` в финальный архив.
pub fn receive_finish<T>(stream: &mut T, state: &ReceiverState) -> Result<Finish>
where
    T: Read + Write,
{
    let finish = match protocol::read_message(stream)? {
        Message::Finish(finish) => finish,
        Message::Error(error) => {
            bail!("peer aborted before finish: {}", error.message);
        }
        unexpected => {
            send_protocol_error(stream, "expected finish message")?;
            bail!("unexpected message before finish: {:?}", unexpected.kind());
        }
    };

    receive_finish_message(stream, state, finish, &[])
}

pub(crate) fn receive_finish_message<T>(
    stream: &mut T,
    state: &ReceiverState,
    finish: Finish,
    issue_journal: &[IssueJournalEntry],
) -> Result<Finish>
where
    T: Read + Write,
{
    let finish_started = Instant::now();
    validate_finish(&state.journal, &finish).or_else(|error| {
        let message = error.to_string();
        send_protocol_error(stream, &message)?;
        Err(error)
    })?;

    if let Err(error) = finalize_archive(state) {
        let message = error.to_string();
        let _ = send_protocol_error(stream, &message);
        return Err(error.context("failed to finalize receiver archive"));
    }
    if let Err(error) = persist_issue_journal(state, issue_journal) {
        let message = error.to_string();
        let _ = send_protocol_error(stream, &message);
        return Err(error.context("failed to persist receiver issue journal"));
    }

    protocol::write_message(stream, &Message::FinishAck)?;
    stream
        .flush()
        .context("failed to flush finish ack to peer")?;
    debug!(
        "receiver finish acknowledged: dump_id={} final_frame_no={} final_file_index={} final_file_path={} issue_count={} elapsed_ms={}",
        state.paths.dump_id(),
        finish.final_frame_no,
        finish.final_file_index,
        finish.final_file_path,
        issue_journal.len(),
        finish_started.elapsed().as_millis()
    );

    Ok(finish)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn receive_finish_message_v3<T>(
    stream: &mut T,
    state: &ReceiverState,
    finish: FinishV3,
) -> Result<FinishV3>
where
    T: Read + Write,
{
    let finish_started = Instant::now();
    validate_finish_v3(&state.journal, &finish).or_else(|error| {
        let message = error.to_string();
        protocol::write_message_v3(
            stream,
            &V3Message::Error(protocol::ErrorMessage {
                message: message.clone(),
            }),
        )?;
        stream
            .flush()
            .context("failed to flush v3 finish validation error")?;
        Err(error)
    })?;

    if let Err(error) = finalize_archive(state) {
        let message = error.to_string();
        let _ = protocol::write_message_v3(
            stream,
            &V3Message::Error(protocol::ErrorMessage {
                message: message.clone(),
            }),
        );
        let _ = stream.flush();
        return Err(error.context("failed to finalize receiver archive"));
    }

    if let Err(error) = finalize_issue_journal_v3(state) {
        let message = error.to_string();
        let _ = protocol::write_message_v3(
            stream,
            &V3Message::Error(protocol::ErrorMessage {
                message: message.clone(),
            }),
        );
        let _ = stream.flush();
        return Err(error.context("failed to finalize receiver typed issue journal"));
    }

    protocol::write_message_v3(stream, &V3Message::FinishAck)?;
    stream
        .flush()
        .context("failed to flush v3 finish ack to peer")?;
    debug!(
        "receiver v3 finish acknowledged: dump_id={} final_frame_no={} final_work_ordinal={:?} elapsed_ms={}",
        state.paths.dump_id(),
        finish.final_frame_no,
        finish.final_work_ordinal,
        finish_started.elapsed().as_millis()
    );

    Ok(finish)
}

fn persist_issue_journal(state: &ReceiverState, issue_journal: &[IssueJournalEntry]) -> Result<()> {
    let issue_journal_path = state.paths.issue_journal_path();
    if issue_journal.is_empty() {
        if issue_journal_path.exists() {
            fs::remove_file(issue_journal_path).with_context(|| {
                format!(
                    "failed to remove stale receiver issue journal {}",
                    issue_journal_path.display()
                )
            })?;
        }
        return Ok(());
    }

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(issue_journal_path)
        .with_context(|| {
            format!(
                "failed to create receiver issue journal {}",
                issue_journal_path.display()
            )
        })?;
    file.write_all(b"file_index\tattempts\treason\tfile_path\tlast_error\n")
        .with_context(|| {
            format!(
                "failed to write receiver issue journal header {}",
                issue_journal_path.display()
            )
        })?;
    for entry in issue_journal {
        writeln!(
            file,
            "{}\t{}\t{}\t{}\t{}",
            entry.file_index,
            entry.attempts,
            issue_journal_reason(entry.reason),
            escape_issue_field(&entry.file_path),
            escape_issue_field(&entry.last_error)
        )
        .with_context(|| {
            format!(
                "failed to append receiver issue journal entry {}",
                issue_journal_path.display()
            )
        })?;
    }
    file.sync_all().with_context(|| {
        format!(
            "failed to fsync receiver issue journal {}",
            issue_journal_path.display()
        )
    })
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn append_issue_journal_v3_chunk(
    state: &ReceiverState,
    entries: &[IssueEntryV3],
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }

    let issue_journal_temp_path = state.paths.issue_journal_temp_path();
    let current_len = match fs::metadata(&issue_journal_temp_path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to inspect receiver typed issue journal temp file {}",
                    issue_journal_temp_path.display()
                )
            });
        }
    };
    let needs_header = current_len == 0;
    let mut payload = Vec::new();
    if needs_header {
        payload.extend_from_slice(
            b"scope\trecord_seq\twork_ordinal\tattempts\tcode\tpath\tdetail\tcontext\n",
        );
    }
    for entry in entries {
        writeln!(
            payload,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            issue_scope_v3(entry.scope),
            entry
                .record_seq
                .map(|value| value.to_string())
                .unwrap_or_default(),
            entry
                .work_ordinal
                .map(|value| value.to_string())
                .unwrap_or_default(),
            entry.attempts,
            issue_code_v3(entry.code),
            escape_issue_field(&entry.path),
            escape_issue_field(&entry.detail),
            escape_issue_field(&entry.context)
        )
        .with_context(|| {
            format!(
                "failed to buffer receiver typed issue journal entry {}",
                issue_journal_temp_path.display()
            )
        })?;
    }
    append_temp_file_payload(
        &issue_journal_temp_path,
        &payload,
        ReceiverTempFileKind::IssueJournalTemp,
    )?;
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn finalize_issue_journal_v3(state: &ReceiverState) -> Result<()> {
    let issue_journal_temp_path = state.paths.issue_journal_temp_path();
    let issue_journal_path = state.paths.issue_journal_path();
    if !issue_journal_temp_path.exists() {
        if issue_journal_path.exists() {
            remove_file_path(issue_journal_path).with_context(|| {
                format!(
                    "failed to remove stale receiver typed issue journal {}",
                    issue_journal_path.display()
                )
            })?;
        }
        return Ok(());
    }
    if issue_journal_path.exists() {
        remove_file_path(issue_journal_path).with_context(|| {
            format!(
                "failed to remove stale receiver typed issue journal {}",
                issue_journal_path.display()
            )
        })?;
    }
    rename_path(&issue_journal_temp_path, issue_journal_path).with_context(|| {
        format!(
            "failed to rename receiver typed issue journal {} to {}",
            issue_journal_temp_path.display(),
            issue_journal_path.display()
        )
    })
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn discard_issue_journal_v3_temp(state: &ReceiverState) -> Result<()> {
    let issue_journal_temp_path = state.paths.issue_journal_temp_path();
    if issue_journal_temp_path.exists() {
        remove_file_path(&issue_journal_temp_path).with_context(|| {
            format!(
                "failed to remove receiver typed issue journal temp file {}",
                issue_journal_temp_path.display()
            )
        })?;
    }
    Ok(())
}

fn issue_journal_reason(reason: IssueJournalReason) -> &'static str {
    match reason {
        IssueJournalReason::OpenFailed => "open_failed",
        IssueJournalReason::UnstableAfterRetries => "unstable_after_retries",
    }
}

fn issue_scope_v3(scope: IssueScopeV3) -> &'static str {
    match scope {
        IssueScopeV3::File => "file",
        IssueScopeV3::DiscoveryEntry => "discovery_entry",
        IssueScopeV3::DiscoverySubtree => "discovery_subtree",
        IssueScopeV3::SenderState => "sender_state",
    }
}

fn issue_code_v3(code: IssueCodeV3) -> &'static str {
    match code {
        IssueCodeV3::OpenFailed => "open_failed",
        IssueCodeV3::UnstableAfterRetries => "unstable_after_retries",
        IssueCodeV3::UnsupportedEntry => "unsupported_entry",
        IssueCodeV3::UnreadableSymlinkTarget => "unreadable_symlink_target",
        IssueCodeV3::VirtualFilesystem => "virtual_filesystem",
        IssueCodeV3::Io => "io",
        IssueCodeV3::SenderStateEvent => "sender_state_event",
        IssueCodeV3::ReadDirFailed => "read_dir_failed",
        IssueCodeV3::SourceInvalidated => "source_invalidated",
    }
}

fn escape_issue_field(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\r', "\\r")
        .replace('\n', "\\n")
}

fn validate_finish(journal: &super::ReceiverJournal, finish: &Finish) -> Result<()> {
    if finish.final_frame_no != journal.last_committed_frame {
        bail!(
            "finish frame number mismatch: expected {} got {}",
            journal.last_committed_frame,
            finish.final_frame_no
        );
    }
    if finish.final_file_index != journal.last_committed_file_index {
        bail!(
            "finish file index mismatch: expected {} got {}",
            journal.last_committed_file_index,
            finish.final_file_index
        );
    }
    if finish.final_file_path != journal.last_committed_file_path {
        bail!(
            "finish file path mismatch: expected {} got {}",
            journal.last_committed_file_path,
            finish.final_file_path
        );
    }

    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
fn validate_finish_v3(journal: &super::ReceiverJournal, finish: &FinishV3) -> Result<()> {
    if finish.final_frame_no != journal.last_committed_frame {
        bail!(
            "v3 finish frame number mismatch: expected {} got {}",
            journal.last_committed_frame,
            finish.final_frame_no
        );
    }
    if finish.final_work_ordinal != journal.last_committed_work_ordinal {
        bail!(
            "v3 finish work ordinal mismatch: expected {:?} got {:?}",
            journal.last_committed_work_ordinal,
            finish.final_work_ordinal
        );
    }

    Ok(())
}

fn finalize_archive(state: &ReceiverState) -> Result<()> {
    let finalize_started = Instant::now();
    let partial_archive_path = state.paths.partial_archive_path();
    let final_archive_path = state.paths.final_archive_path();
    let journal_path = state.paths.journal_path();

    if final_archive_path.exists() {
        bail!(
            "final archive already exists and would be overwritten: {}",
            final_archive_path.display()
        );
    }

    let file = OpenOptions::new()
        .append(true)
        .open(partial_archive_path)
        .with_context(|| {
            format!(
                "failed to open partial archive for finalization {}",
                partial_archive_path.display()
            )
        })?;
    let mut encoder = zstd::stream::write::Encoder::new(file, 0)
        .context("failed to create zstd encoder for tar end blocks")?;
    encoder
        .write_all(&[0_u8; 1024])
        .context("failed to append tar end blocks into final zstd frame")?;
    let file = encoder
        .finish()
        .context("failed to finish zstd frame for tar end blocks")?;
    file.sync_all().with_context(|| {
        format!(
            "failed to sync finalized receiver archive {}",
            partial_archive_path.display()
        )
    })?;

    rename_path(partial_archive_path, final_archive_path).with_context(|| {
        format!(
            "failed to rename finalized archive {} to {}",
            partial_archive_path.display(),
            final_archive_path.display()
        )
    })?;

    if journal_path.exists()
        && let Err(error) = remove_file_path(journal_path)
    {
        rename_path(final_archive_path, partial_archive_path).with_context(|| {
            format!(
                "failed to roll finalized archive back from {} to {}",
                final_archive_path.display(),
                partial_archive_path.display()
            )
        })?;
        recover_partial_archive(partial_archive_path, state.journal.archive_len)?;
        return Err(error).with_context(|| {
            format!(
                "failed to remove receiver journal after finalization {}",
                journal_path.display()
            )
        });
    }

    debug!(
        "receiver archive finalized: dump_id={} final_archive={} elapsed_ms={}",
        state.paths.dump_id(),
        final_archive_path.display(),
        finalize_started.elapsed().as_millis()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Cursor};

    use super::*;
    use crate::network::protocol::{
        ErrorMessage, FinishV3, IssueCodeV3, IssueEntryV3, IssueScopeV3, Message, ResumeState,
        V3Message,
    };
    use crate::receiver::shared::test_hooks;
    use crate::receiver::test_support::*;
    use crate::sender::prepare_dump;

    fn committed_state(temp: &TestDir) -> ReceiverState {
        let mut state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        state.journal.last_committed_frame = 2;
        state.journal.last_committed_file_index = 5;
        state.journal.last_committed_file_path = "root/file.bin".to_owned();
        state.journal.archive_len = 6;
        state.persist_journal().unwrap();
        fs::write(state.paths.partial_archive_path(), b"abcdef").unwrap();
        state
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
    fn receive_finish_surfaces_peer_error_before_finish() {
        let temp = TestDir::new();
        let state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        let request = encode_message(&Message::Error(ErrorMessage {
            message: "cancelled".to_owned(),
        }));
        let mut stream = Cursor::new(request);

        let error = receive_finish(&mut stream, &state).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("peer aborted before finish: cancelled")
        );
    }

    #[test]
    fn receive_finish_rejects_unexpected_message_before_finish_and_emits_error() {
        let temp = TestDir::new();
        let state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        let request = encode_message(&Message::FinishAck);
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = receive_finish(&mut stream, &state).unwrap_err();
        let bytes = stream.into_inner();

        assert!(
            error
                .to_string()
                .contains("unexpected message before finish: FinishAck")
        );
        assert_eq!(
            decode_reply(&bytes, request_len),
            Message::Error(ErrorMessage {
                message: "expected finish message".to_owned(),
            })
        );
    }

    #[test]
    fn receive_finish_accepts_current_committed_boundary_and_emits_ack() {
        let temp = TestDir::new();
        let state = committed_state(&temp);

        let request = encode_message(&Message::Finish(protocol::Finish {
            final_frame_no: 2,
            final_file_index: 5,
            final_file_path: "root/file.bin".to_owned(),
        }));
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let finish = receive_finish(&mut stream, &state).unwrap();
        let bytes = stream.into_inner();

        assert_eq!(finish.final_frame_no, 2);
        assert_eq!(finish.final_file_index, 5);
        assert_eq!(finish.final_file_path, "root/file.bin");
        assert_eq!(decode_reply(&bytes, request_len), Message::FinishAck);
        assert!(state.paths.final_archive_path().exists());
        assert!(!state.paths.partial_archive_path().exists());
        assert!(!state.paths.journal_path().exists());
    }

    #[test]
    fn receive_finish_message_v3_accepts_matching_work_ordinal_and_emits_ack() {
        let temp = TestDir::new();
        let mut state = committed_state(&temp);
        state.journal.last_committed_work_ordinal = Some(5);
        state.persist_journal().unwrap();
        let mut stream = Cursor::new(Vec::new());

        let finish = receive_finish_message_v3(
            &mut stream,
            &state,
            FinishV3 {
                final_frame_no: 2,
                final_work_ordinal: Some(5),
            },
        )
        .unwrap();

        assert_eq!(finish.final_frame_no, 2);
        assert_eq!(finish.final_work_ordinal, Some(5));
        assert_eq!(
            protocol::read_message_v3(&mut Cursor::new(stream.into_inner())).unwrap(),
            V3Message::FinishAck
        );
        assert!(state.paths.final_archive_path().exists());
        assert!(!state.paths.partial_archive_path().exists());
        assert!(!state.paths.journal_path().exists());
    }

    #[test]
    fn receive_finish_message_v3_rejects_work_ordinal_mismatch_and_emits_error() {
        let temp = TestDir::new();
        let mut state = committed_state(&temp);
        state.journal.last_committed_work_ordinal = Some(5);
        state.persist_journal().unwrap();
        let mut stream = Cursor::new(Vec::new());

        let error = receive_finish_message_v3(
            &mut stream,
            &state,
            FinishV3 {
                final_frame_no: 2,
                final_work_ordinal: Some(6),
            },
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("v3 finish work ordinal mismatch: expected Some(5) got Some(6)")
        );
        assert_eq!(
            protocol::read_message_v3(&mut Cursor::new(stream.into_inner())).unwrap(),
            V3Message::Error(ErrorMessage {
                message: "v3 finish work ordinal mismatch: expected Some(5) got Some(6)".to_owned(),
            })
        );
        assert!(state.paths.partial_archive_path().exists());
        assert!(!state.paths.final_archive_path().exists());
    }

    #[test]
    fn finalize_issue_journal_v3_promotes_temp_file_after_successful_finish() {
        let _guard = FsFailureGuard::new();
        let temp = TestDir::new();
        let state = ReceiverState::open(temp.path().to_path_buf(), "dump-issues").unwrap();

        append_issue_journal_v3_chunk(
            &state,
            &[
                typed_issue_entry(IssueScopeV3::File, "alpha.txt", IssueCodeV3::OpenFailed),
                typed_issue_entry(
                    IssueScopeV3::DiscoverySubtree,
                    "proc",
                    IssueCodeV3::VirtualFilesystem,
                ),
            ],
        )
        .unwrap();
        finalize_issue_journal_v3(&state).unwrap();

        let final_path = state.paths.issue_journal_path();
        assert!(final_path.exists());
        assert!(!state.paths.issue_journal_temp_path().exists());
        let contents = fs::read_to_string(final_path).unwrap();
        assert!(
            contents
                .contains("scope\trecord_seq\twork_ordinal\tattempts\tcode\tpath\tdetail\tcontext")
        );
        assert!(contents.contains("file\t\t3\t2\topen_failed\talpha.txt"));
        assert!(contents.contains("discovery_subtree\t8\t\t0\tvirtual_filesystem\tproc"));
    }

    #[test]
    fn discard_issue_journal_v3_temp_removes_partial_tail() {
        let _guard = FsFailureGuard::new();
        let temp = TestDir::new();
        let state = ReceiverState::open(temp.path().to_path_buf(), "dump-issues").unwrap();

        append_issue_journal_v3_chunk(
            &state,
            &[typed_issue_entry(
                IssueScopeV3::SenderState,
                "sender-state",
                IssueCodeV3::SenderStateEvent,
            )],
        )
        .unwrap();
        assert!(state.paths.issue_journal_temp_path().exists());

        discard_issue_journal_v3_temp(&state).unwrap();

        assert!(!state.paths.issue_journal_temp_path().exists());
        assert!(!state.paths.issue_journal_path().exists());
    }

    #[test]
    fn append_issue_journal_v3_chunk_rejects_quota_exhaustion_without_growing_temp_file() {
        let _guard = FsFailureGuard::new();
        let temp = TestDir::new();
        let state = ReceiverState::open(temp.path().to_path_buf(), "dump-issues").unwrap();

        append_issue_journal_v3_chunk(
            &state,
            &[typed_issue_entry(
                IssueScopeV3::File,
                "alpha.txt",
                IssueCodeV3::OpenFailed,
            )],
        )
        .unwrap();
        let temp_path = state.paths.issue_journal_temp_path();
        let committed_len = fs::metadata(&temp_path).unwrap().len();

        test_hooks::set_quota_override(ReceiverTempFileKind::IssueJournalTemp, committed_len + 16);

        let error = append_issue_journal_v3_chunk(
            &state,
            &[typed_issue_entry(
                IssueScopeV3::DiscoverySubtree,
                "nested/proc",
                IssueCodeV3::VirtualFilesystem,
            )],
        )
        .unwrap_err();

        assert!(error.to_string().contains("quota exceeded"));
        assert_eq!(fs::metadata(&temp_path).unwrap().len(), committed_len);
    }

    #[test]
    fn append_issue_journal_v3_chunk_rolls_back_partial_tail_after_storage_full() {
        let _guard = FsFailureGuard::new();
        let temp = TestDir::new();
        let state = ReceiverState::open(temp.path().to_path_buf(), "dump-issues").unwrap();
        let temp_path = state.paths.issue_journal_temp_path();

        test_hooks::fail_next_append_after_write(&temp_path);

        let error = append_issue_journal_v3_chunk(
            &state,
            &[typed_issue_entry(
                IssueScopeV3::SenderState,
                "sender-state",
                IssueCodeV3::SenderStateEvent,
            )],
        )
        .unwrap_err();

        assert!(!error.to_string().is_empty());
        assert_eq!(fs::metadata(&temp_path).unwrap().len(), 0);
    }

    #[test]
    fn receive_finish_restores_partial_archive_when_journal_cleanup_fails() {
        let _guard = FsFailureGuard::new();
        let temp = TestDir::new();
        let state = committed_state(&temp);
        test_hooks::fail_next_remove_file(state.paths.journal_path());

        let request = encode_message(&Message::Finish(protocol::Finish {
            final_frame_no: 2,
            final_file_index: 5,
            final_file_path: "root/file.bin".to_owned(),
        }));
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = receive_finish(&mut stream, &state).unwrap_err();
        let bytes = stream.into_inner();

        assert!(
            error
                .to_string()
                .contains("failed to finalize receiver archive")
        );
        assert!(state.paths.partial_archive_path().exists());
        assert!(!state.paths.final_archive_path().exists());
        assert!(state.paths.journal_path().exists());
        assert_eq!(
            fs::metadata(state.paths.partial_archive_path())
                .unwrap()
                .len(),
            6
        );
        assert_eq!(
            decode_reply(&bytes, request_len),
            Message::Error(ErrorMessage {
                message: format!(
                    "failed to remove receiver journal after finalization {}",
                    state.paths.journal_path().display()
                ),
            })
        );

        let reopened = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        assert_eq!(reopened.journal.archive_len, 6);
        assert_eq!(
            fs::metadata(reopened.paths.partial_archive_path())
                .unwrap()
                .len(),
            6
        );
    }

    #[test]
    fn receive_finish_rejects_mismatched_boundary_and_emits_error() {
        let temp = TestDir::new();
        let state = committed_state(&temp);

        let request = encode_message(&Message::Finish(protocol::Finish {
            final_frame_no: 3,
            final_file_index: 5,
            final_file_path: "root/file.bin".to_owned(),
        }));
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = receive_finish(&mut stream, &state).unwrap_err();
        let bytes = stream.into_inner();

        assert!(error.to_string().contains("finish frame number mismatch"));
        assert_eq!(
            decode_reply(&bytes, request_len),
            Message::Error(ErrorMessage {
                message: "finish frame number mismatch: expected 2 got 3".to_owned(),
            })
        );
    }

    #[test]
    fn receive_finish_rejects_file_index_mismatch_and_emits_error() {
        let temp = TestDir::new();
        let state = committed_state(&temp);

        let request = encode_message(&Message::Finish(protocol::Finish {
            final_frame_no: 2,
            final_file_index: 7,
            final_file_path: "root/file.bin".to_owned(),
        }));
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = receive_finish(&mut stream, &state).unwrap_err();
        let bytes = stream.into_inner();

        assert!(error.to_string().contains("finish file index mismatch"));
        assert_eq!(
            decode_reply(&bytes, request_len),
            Message::Error(ErrorMessage {
                message: "finish file index mismatch: expected 5 got 7".to_owned(),
            })
        );
        assert!(state.paths.partial_archive_path().exists());
        assert!(state.paths.journal_path().exists());
    }

    #[test]
    fn receive_finish_rejects_file_path_mismatch_and_emits_error() {
        let temp = TestDir::new();
        let state = committed_state(&temp);

        let request = encode_message(&Message::Finish(protocol::Finish {
            final_frame_no: 2,
            final_file_index: 5,
            final_file_path: "root/other.bin".to_owned(),
        }));
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = receive_finish(&mut stream, &state).unwrap_err();
        let bytes = stream.into_inner();

        assert!(error.to_string().contains("finish file path mismatch"));
        assert_eq!(
            decode_reply(&bytes, request_len),
            Message::Error(ErrorMessage {
                message: "finish file path mismatch: expected root/file.bin got root/other.bin"
                    .to_owned(),
            })
        );
        assert!(state.paths.partial_archive_path().exists());
        assert!(state.paths.journal_path().exists());
    }

    #[test]
    fn receive_finish_reports_existing_final_archive_before_overwrite() {
        let temp = TestDir::new();
        let state = committed_state(&temp);
        fs::write(state.paths.final_archive_path(), b"existing").unwrap();

        let request = encode_message(&Message::Finish(protocol::Finish {
            final_frame_no: 2,
            final_file_index: 5,
            final_file_path: "root/file.bin".to_owned(),
        }));
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = receive_finish(&mut stream, &state).unwrap_err();
        let bytes = stream.into_inner();

        assert!(
            error
                .to_string()
                .contains("failed to finalize receiver archive")
        );
        assert_eq!(
            decode_reply(&bytes, request_len),
            Message::Error(ErrorMessage {
                message: format!(
                    "final archive already exists and would be overwritten: {}",
                    state.paths.final_archive_path().display()
                ),
            })
        );
        assert!(state.paths.final_archive_path().exists());
        assert!(state.paths.partial_archive_path().exists());
        assert!(state.paths.journal_path().exists());
    }

    #[test]
    fn receive_finish_reports_missing_partial_archive_during_finalization() {
        let temp = TestDir::new();
        let state = committed_state(&temp);
        fs::remove_file(state.paths.partial_archive_path()).unwrap();

        let request = encode_message(&Message::Finish(protocol::Finish {
            final_frame_no: 2,
            final_file_index: 5,
            final_file_path: "root/file.bin".to_owned(),
        }));
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = receive_finish(&mut stream, &state).unwrap_err();
        let bytes = stream.into_inner();

        assert!(
            error
                .to_string()
                .contains("failed to finalize receiver archive")
        );
        assert_eq!(
            decode_reply(&bytes, request_len),
            Message::Error(ErrorMessage {
                message: format!(
                    "failed to open partial archive for finalization {}",
                    state.paths.partial_archive_path().display()
                ),
            })
        );
        assert!(!state.paths.final_archive_path().exists());
        assert!(state.paths.journal_path().exists());
    }

    #[test]
    fn receive_finish_after_resume_session_finalizes_archive_with_resumed_boundary() {
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
        let temp = TestDir::new();

        let mut initial_state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        crate::receiver::receive_frame(
            &mut Cursor::new(encode_prepared_frame(&prepared.frames[0])),
            &mut initial_state,
        )
        .unwrap();
        drop(initial_state);

        let mut resumed_state = ReceiverState::open(temp.path().to_path_buf(), "dump-01").unwrap();
        crate::receiver::receive_frame(
            &mut Cursor::new(encode_prepared_frame(&prepared.frames[1])),
            &mut resumed_state,
        )
        .unwrap();

        let request = encode_message(&Message::Finish(protocol::Finish {
            final_frame_no: prepared.final_frame_no,
            final_file_index: prepared.final_file_index,
            final_file_path: prepared.final_file_path.clone(),
        }));
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let finish = receive_finish(&mut stream, &resumed_state).unwrap();
        let bytes = stream.into_inner();

        assert_eq!(finish.final_frame_no, prepared.final_frame_no);
        assert_eq!(finish.final_file_index, prepared.final_file_index);
        assert_eq!(finish.final_file_path, prepared.final_file_path);
        assert_eq!(decode_reply(&bytes, request_len), Message::FinishAck);
        assert_eq!(
            archive_entries(resumed_state.paths.final_archive_path()),
            vec![
                ("alpha.txt".to_owned(), b"alpha".to_vec()),
                ("nested/beta.txt".to_owned(), b"beta".to_vec()),
            ]
        );
    }
}

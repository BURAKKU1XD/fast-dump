use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use log::debug;

use crate::network::protocol::{self, Finish, Message};

use super::ReceiverState;
use super::journal::recover_partial_archive;
use super::shared::{remove_file_path, rename_path, send_protocol_error};

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

    receive_finish_message(stream, state, finish)
}

pub(crate) fn receive_finish_message<T>(
    stream: &mut T,
    state: &ReceiverState,
    finish: Finish,
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

    protocol::write_message(stream, &Message::FinishAck)?;
    stream
        .flush()
        .context("failed to flush finish ack to peer")?;
    debug!(
        "receiver finish acknowledged: dump_id={} final_frame_no={} final_file_index={} final_file_path={} elapsed_ms={}",
        state.paths.dump_id(),
        finish.final_frame_no,
        finish.final_file_index,
        finish.final_file_path,
        finish_started.elapsed().as_millis()
    );

    Ok(finish)
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
    use crate::network::protocol::{ErrorMessage, Message, ResumeState};
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

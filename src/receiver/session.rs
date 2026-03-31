use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Result, bail};
use log::debug;

use crate::network::protocol::{self, Message};

use super::ReceiverState;
use super::finish::receive_finish_message;
use super::frame::{FrameReceiveResult, receive_frame_event_from_start};
use super::resume::respond_resume_query_checked;
use super::shared::send_protocol_error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiveReport {
    pub resume_state: protocol::ResumeState,
    pub state: ReceiverState,
    pub finish: protocol::Finish,
    pub final_archive_path: PathBuf,
}

// Этот helper держит весь receiver-side session loop:
// после ResumeQuery он последовательно принимает committed frame-ы до Finish.
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

#[cfg(test)]
mod tests {
    use std::{fs, io::Cursor};

    use super::*;
    use crate::network::protocol::{
        AbortAck, Ack, ErrorMessage, FrameChunk, FrameStart, IssueJournalChunk, IssueJournalEntry,
        IssueJournalReason, Message, ResumeQuery, ResumeState,
    };
    use crate::receiver::test_support::*;
    use crate::sender::prepare_dump;

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

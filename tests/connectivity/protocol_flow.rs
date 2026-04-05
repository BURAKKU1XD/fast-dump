use std::fs;
use std::io::Write;
use std::thread;

use crate::support::{TestDir, spawn_receiver_dump_once_v3};
use fast_dump::network::protocol::{
    AbortAck, Ack, FrameAbort, FrameChunk, FrameStart, IssueChunkV3, IssueCodeV3, IssueEntryV3,
    IssueJournalChunk, IssueJournalEntry, IssueJournalReason, IssueScopeV3, ManifestBoundaryV3,
    ManifestChunk, ManifestSeal, ManifestStart, Message, ResumeState, V3Message, read_message,
    read_message_v3, request_resume_state, request_resume_state_v3, write_message,
    write_message_v3,
};
use fast_dump::network::transport::{BoundListener, connect_once};
use fast_dump::receiver::{
    ReceiverPaths, receive_dump, receive_finish, receive_frame, receive_frame_abort,
    respond_resume_query,
};
use fast_dump::role::Role;
use fast_dump::sender::{OutboundFrame, prepare_dump, send_finish, send_frame};

#[cfg(unix)]
fn create_symlink(path: &std::path::Path, target: &std::path::Path) {
    std::os::unix::fs::symlink(target, path).unwrap();
}

fn empty_manifest_boundary_v3() -> ManifestBoundaryV3 {
    ManifestBoundaryV3 {
        manifest_bytes_persisted: 0,
        last_record_seq: None,
        last_work_ordinal: None,
        sealed: false,
    }
}

fn sender_state_issue_v3(record_seq: u64, path: &str) -> IssueEntryV3 {
    IssueEntryV3 {
        scope: IssueScopeV3::SenderState,
        record_seq: Some(record_seq),
        work_ordinal: None,
        path: path.to_owned(),
        attempts: 0,
        code: IssueCodeV3::SenderStateEvent,
        detail: "state-transition".to_owned(),
        context: "manifest recovery".to_owned(),
    }
}

#[test]
fn frame_protocol_commits_single_and_multi_frame_sequences() {
    {
        let label = "single-frame";
        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();
        let data_dir = TestDir::new();
        let receiver_dir = data_dir.path().to_path_buf();
        let frame = OutboundFrame {
            frame_no: 1,
            first_file_index: 1,
            first_file_path: "root/a.bin".to_owned(),
            last_file_index: 2,
            last_file_path: "root/b.bin".to_owned(),
            level: 3,
            threads: 2,
        };

        let receiver = thread::spawn(move || {
            let mut session = listener.accept(Role::Receiver, None, None).unwrap();
            let mut state = respond_resume_query(&mut session.stream, receiver_dir).unwrap();
            let ack = receive_frame(&mut session.stream, &mut state).unwrap();
            (
                ack,
                state.journal.archive_len,
                state.journal.last_committed_frame,
            )
        });

        let mut sender_session = connect_once(Role::Sender, addr, Some("dump-05"), None).unwrap();
        let resume_state = request_resume_state(&mut sender_session.stream, "dump-05").unwrap();
        assert_eq!(resume_state.archive_len, 0, "{label}");

        let ack = send_frame(
            &mut sender_session.stream,
            &frame,
            [b"abc".as_slice(), b"def".as_slice()],
        )
        .unwrap();
        let (receiver_ack, archive_len, committed_frame) = receiver.join().unwrap();

        assert_eq!(
            ack,
            Ack {
                frame_no: 1,
                last_file_index: 2,
                last_file_path: "root/b.bin".to_owned(),
                archive_len: 6,
            },
            "{label}"
        );
        assert_eq!(receiver_ack, ack, "{label}");
        assert_eq!(archive_len, 6, "{label}");
        assert_eq!(committed_frame, 1, "{label}");
    }

    {
        let label = "multi-frame";
        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();
        let data_dir = TestDir::new();
        let receiver_dir = data_dir.path().to_path_buf();
        let frame1 = OutboundFrame {
            frame_no: 1,
            first_file_index: 1,
            first_file_path: "root/a.bin".to_owned(),
            last_file_index: 1,
            last_file_path: "root/a.bin".to_owned(),
            level: 3,
            threads: 2,
        };
        let frame2 = OutboundFrame {
            frame_no: 2,
            first_file_index: 2,
            first_file_path: "root/b.bin".to_owned(),
            last_file_index: 3,
            last_file_path: "root/c.bin".to_owned(),
            level: 3,
            threads: 2,
        };

        let receiver = thread::spawn(move || {
            let mut session = listener.accept(Role::Receiver, None, None).unwrap();
            let mut state = respond_resume_query(&mut session.stream, receiver_dir).unwrap();
            let ack1 = receive_frame(&mut session.stream, &mut state).unwrap();
            let ack2 = receive_frame(&mut session.stream, &mut state).unwrap();
            let finish = receive_finish(&mut session.stream, &state).unwrap();
            (ack1, ack2, finish, state.journal.archive_len)
        });

        let mut sender_session = connect_once(Role::Sender, addr, Some("dump-06"), None).unwrap();
        let resume_state = request_resume_state(&mut sender_session.stream, "dump-06").unwrap();
        assert_eq!(resume_state.archive_len, 0, "{label}");

        let ack1 = send_frame(&mut sender_session.stream, &frame1, [b"aaa".as_slice()]).unwrap();
        let ack2 = send_frame(
            &mut sender_session.stream,
            &frame2,
            [b"bbbb".as_slice(), b"cc".as_slice()],
        )
        .unwrap();
        let finish = send_finish(
            &mut sender_session.stream,
            frame2.frame_no,
            frame2.last_file_index,
            &frame2.last_file_path,
        )
        .unwrap();

        let (receiver_ack1, receiver_ack2, receiver_finish, archive_len) = receiver.join().unwrap();

        assert_eq!(ack1.archive_len, 3, "{label}");
        assert_eq!(ack2.archive_len, 9, "{label}");
        assert_eq!(receiver_ack1, ack1, "{label}");
        assert_eq!(receiver_ack2, ack2, "{label}");
        assert_eq!(finish.final_frame_no, 2, "{label}");
        assert_eq!(finish.final_file_index, 3, "{label}");
        assert_eq!(finish.final_file_path, "root/c.bin", "{label}");
        assert_eq!(receiver_finish, finish, "{label}");
        assert_eq!(archive_len, 9, "{label}");
    }
}

#[test]
fn resume_query_discards_abandoned_v3_temp_artifacts_before_replying() {
    let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr();
    let data_dir = TestDir::new();
    let dump_id = "dump-stale-v3-protocol";
    let receiver_dir = data_dir.path().to_path_buf();
    let paths = ReceiverPaths::new(receiver_dir.clone(), dump_id).unwrap();
    fs::write(paths.manifest_path(), b"stale-manifest-tail").unwrap();
    fs::write(paths.issue_journal_temp_path(), b"stale-issue-tail").unwrap();

    let receiver = thread::spawn(move || {
        let mut session = listener.accept(Role::Receiver, None, None).unwrap();
        let state = respond_resume_query(&mut session.stream, receiver_dir).unwrap();
        (
            state.journal.manifest_bytes_persisted,
            fs::metadata(state.paths.manifest_path()).unwrap().len(),
            state.paths.issue_journal_temp_path().exists(),
        )
    });

    let mut sender_session = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    let resume_state = request_resume_state(&mut sender_session.stream, dump_id).unwrap();
    let (manifest_boundary, manifest_len, issue_temp_exists) = receiver.join().unwrap();

    assert_eq!(resume_state.archive_len, 0);
    assert_eq!(resume_state.last_committed_frame, 0);
    assert_eq!(resume_state.last_committed_file_index, 0);
    assert_eq!(manifest_boundary, 0);
    assert_eq!(manifest_len, 0);
    assert!(!issue_temp_exists);
}

#[test]
fn sender_pipeline_roundtrips_into_valid_tar_zst_archive() {
    let source_dir = TestDir::new();
    fs::create_dir_all(source_dir.path().join("nested")).unwrap();
    fs::write(source_dir.path().join("alpha.txt"), b"alpha").unwrap();
    fs::write(source_dir.path().join("nested").join("beta.txt"), b"beta").unwrap();

    let prepared = prepare_dump(
        source_dir.path().to_path_buf(),
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

    let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr();
    let receiver_dir = TestDir::new();
    let receiver_path = receiver_dir.path().to_path_buf();
    let expected_frame_count = prepared.frames.len();

    let receiver = thread::spawn(move || {
        let mut session = listener.accept(Role::Receiver, None, None).unwrap();
        let mut state = respond_resume_query(&mut session.stream, receiver_path).unwrap();
        for _ in 0..expected_frame_count {
            receive_frame(&mut session.stream, &mut state).unwrap();
        }
        let finish = receive_finish(&mut session.stream, &state).unwrap();
        (finish, state.paths.final_archive_path().to_path_buf())
    });

    let mut sender_session = connect_once(Role::Sender, addr, Some("dump-07"), None).unwrap();
    let resume_state = request_resume_state(&mut sender_session.stream, "dump-06").unwrap();
    assert_eq!(resume_state.archive_len, 0);

    for frame in &prepared.frames {
        let chunk_refs: Vec<&[u8]> = frame.chunks.iter().map(Vec::as_slice).collect();
        send_frame(&mut sender_session.stream, &frame.outbound, chunk_refs).unwrap();
    }
    let finish = send_finish(
        &mut sender_session.stream,
        prepared.final_frame_no,
        prepared.final_file_index,
        &prepared.final_file_path,
    )
    .unwrap();

    let (receiver_finish, archive_path) = receiver.join().unwrap();
    assert_eq!(receiver_finish, finish);
    assert!(archive_path.exists());
    assert!(!archive_path.with_extension("zst.part").exists());

    let file = fs::File::open(&archive_path).unwrap();
    let decoder = zstd::stream::read::Decoder::new(file).unwrap();
    let mut archive = tar::Archive::new(decoder);
    let mut entries = Vec::new();
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        let path = entry.path().unwrap().to_string_lossy().into_owned();
        let mut bytes = Vec::new();
        std::io::copy(&mut entry, &mut bytes).unwrap();
        entries.push((path, bytes));
    }

    assert_eq!(
        entries,
        vec![
            ("alpha.txt".to_owned(), b"alpha".to_vec()),
            ("nested/beta.txt".to_owned(), b"beta".to_vec()),
        ]
    );
}

#[cfg(unix)]
#[test]
fn sender_pipeline_roundtrips_symlink_entries_into_valid_tar_zst_archive() {
    let source_dir = TestDir::new();
    fs::write(source_dir.path().join("alpha.txt"), b"alpha").unwrap();
    create_symlink(
        &source_dir.path().join("alias.txt"),
        std::path::Path::new("alpha.txt"),
    );

    let prepared = prepare_dump(
        source_dir.path().to_path_buf(),
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

    let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr();
    let receiver_dir = TestDir::new();
    let receiver_path = receiver_dir.path().to_path_buf();
    let expected_frame_count = prepared.frames.len();

    let receiver = thread::spawn(move || {
        let mut session = listener.accept(Role::Receiver, None, None).unwrap();
        let mut state = respond_resume_query(&mut session.stream, receiver_path).unwrap();
        for _ in 0..expected_frame_count {
            receive_frame(&mut session.stream, &mut state).unwrap();
        }
        let finish = receive_finish(&mut session.stream, &state).unwrap();
        (finish, state.paths.final_archive_path().to_path_buf())
    });

    let dump_id = "dump-07-symlink";
    let mut sender_session = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    let resume_state = request_resume_state(&mut sender_session.stream, dump_id).unwrap();
    assert_eq!(resume_state.archive_len, 0);

    for frame in &prepared.frames {
        let chunk_refs: Vec<&[u8]> = frame.chunks.iter().map(Vec::as_slice).collect();
        send_frame(&mut sender_session.stream, &frame.outbound, chunk_refs).unwrap();
    }
    let finish = send_finish(
        &mut sender_session.stream,
        prepared.final_frame_no,
        prepared.final_file_index,
        &prepared.final_file_path,
    )
    .unwrap();

    let (receiver_finish, archive_path) = receiver.join().unwrap();
    assert_eq!(receiver_finish, finish);
    assert!(archive_path.exists());

    let file = fs::File::open(&archive_path).unwrap();
    let decoder = zstd::stream::read::Decoder::new(file).unwrap();
    let mut archive = tar::Archive::new(decoder);
    let mut entries = Vec::new();
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        let path = entry.path().unwrap().to_string_lossy().into_owned();
        let is_symlink = entry.header().entry_type().is_symlink();
        let link_name = entry
            .link_name()
            .unwrap()
            .map(|path| path.to_string_lossy().into_owned());
        let mut bytes = Vec::new();
        std::io::copy(&mut entry, &mut bytes).unwrap();
        entries.push((path, is_symlink, link_name, bytes));
    }

    assert_eq!(
        entries,
        vec![
            (
                "alias.txt".to_owned(),
                true,
                Some("alpha.txt".to_owned()),
                Vec::new(),
            ),
            ("alpha.txt".to_owned(), false, None, b"alpha".to_vec()),
        ]
    );
}

#[test]
fn frame_protocol_aborts_uncommitted_frame_and_keeps_committed_archive_len() {
    let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr();
    let data_dir = TestDir::new();
    let receiver_dir = data_dir.path().to_path_buf();
    let frame1 = OutboundFrame {
        frame_no: 1,
        first_file_index: 1,
        first_file_path: "root/a.bin".to_owned(),
        last_file_index: 1,
        last_file_path: "root/a.bin".to_owned(),
        level: 3,
        threads: 2,
    };
    let frame2 = OutboundFrame {
        frame_no: 2,
        first_file_index: 2,
        first_file_path: "root/b.bin".to_owned(),
        last_file_index: 2,
        last_file_path: "root/b.bin".to_owned(),
        level: 3,
        threads: 2,
    };

    let receiver = thread::spawn(move || {
        let mut session = listener.accept(Role::Receiver, None, None).unwrap();
        let mut state = respond_resume_query(&mut session.stream, receiver_dir).unwrap();
        let ack1 = receive_frame(&mut session.stream, &mut state).unwrap();
        let abort_ack = receive_frame_abort(&mut session.stream, &mut state).unwrap();
        let ack2 = receive_frame(&mut session.stream, &mut state).unwrap();
        let finish = receive_finish(&mut session.stream, &state).unwrap();
        (ack1, abort_ack, ack2, finish, state.journal.archive_len)
    });

    let mut sender_session = connect_once(Role::Sender, addr, Some("dump-08"), None).unwrap();
    let resume_state = request_resume_state(&mut sender_session.stream, "dump-08").unwrap();
    assert_eq!(resume_state.archive_len, 0);

    let ack1 = send_frame(&mut sender_session.stream, &frame1, [b"aaa".as_slice()]).unwrap();

    write_message(
        &mut sender_session.stream,
        &Message::FrameStart(FrameStart {
            frame_no: frame2.frame_no,
            first_file_index: frame2.first_file_index,
            first_file_path: frame2.first_file_path.clone(),
            level: frame2.level,
            threads: frame2.threads,
        }),
    )
    .unwrap();
    write_message(
        &mut sender_session.stream,
        &Message::FrameChunk(FrameChunk {
            bytes: b"partial".to_vec(),
        }),
    )
    .unwrap();
    write_message(
        &mut sender_session.stream,
        &Message::FrameAbort(FrameAbort {
            frame_no: frame2.frame_no,
        }),
    )
    .unwrap();

    let abort_ack = match read_message(&mut sender_session.stream).unwrap() {
        Message::AbortAck(ack) => ack,
        unexpected => panic!("expected AbortAck, got {:?}", unexpected.kind()),
    };

    let ack2 = send_frame(&mut sender_session.stream, &frame2, [b"bbbb".as_slice()]).unwrap();
    let finish = send_finish(
        &mut sender_session.stream,
        frame2.frame_no,
        frame2.last_file_index,
        &frame2.last_file_path,
    )
    .unwrap();

    let (receiver_ack1, receiver_abort_ack, receiver_ack2, receiver_finish, archive_len) =
        receiver.join().unwrap();

    assert_eq!(ack1.archive_len, 3);
    assert_eq!(
        abort_ack,
        AbortAck {
            frame_no: 2,
            archive_len: 3,
        }
    );
    assert_eq!(ack2.archive_len, 7);
    assert_eq!(receiver_ack1, ack1);
    assert_eq!(receiver_abort_ack, abort_ack);
    assert_eq!(receiver_ack2, ack2);
    assert_eq!(receiver_finish, finish);
    assert_eq!(archive_len, 7);
}

#[test]
fn frame_protocol_rebuilds_after_aborting_shared_frame_attempt() {
    let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr();
    let data_dir = TestDir::new();
    let receiver_dir = data_dir.path().to_path_buf();
    let alpha_frame = OutboundFrame {
        frame_no: 1,
        first_file_index: 1,
        first_file_path: "alpha.txt".to_owned(),
        last_file_index: 1,
        last_file_path: "alpha.txt".to_owned(),
        level: 3,
        threads: 1,
    };
    let payload_frame = OutboundFrame {
        frame_no: 2,
        first_file_index: 2,
        first_file_path: "payload.bin".to_owned(),
        last_file_index: 2,
        last_file_path: "payload.bin".to_owned(),
        level: 3,
        threads: 1,
    };
    let tail_frame = OutboundFrame {
        frame_no: 3,
        first_file_index: 3,
        first_file_path: "zeta.txt".to_owned(),
        last_file_index: 3,
        last_file_path: "zeta.txt".to_owned(),
        level: 3,
        threads: 1,
    };

    let receiver = thread::spawn(move || {
        let mut session = listener.accept(Role::Receiver, None, None).unwrap();
        let mut state = respond_resume_query(&mut session.stream, receiver_dir).unwrap();
        let abort_ack = receive_frame_abort(&mut session.stream, &mut state).unwrap();
        let ack1 = receive_frame(&mut session.stream, &mut state).unwrap();
        let ack2 = receive_frame(&mut session.stream, &mut state).unwrap();
        let ack3 = receive_frame(&mut session.stream, &mut state).unwrap();
        let finish = receive_finish(&mut session.stream, &state).unwrap();
        (
            abort_ack,
            ack1,
            ack2,
            ack3,
            finish,
            state.journal.archive_len,
        )
    });

    let mut sender_session = connect_once(Role::Sender, addr, Some("dump-08b"), None).unwrap();
    let resume_state = request_resume_state(&mut sender_session.stream, "dump-08b").unwrap();
    assert_eq!(resume_state.archive_len, 0);

    write_message(
        &mut sender_session.stream,
        &Message::FrameStart(FrameStart {
            frame_no: alpha_frame.frame_no,
            first_file_index: alpha_frame.first_file_index,
            first_file_path: alpha_frame.first_file_path.clone(),
            level: alpha_frame.level,
            threads: alpha_frame.threads,
        }),
    )
    .unwrap();
    write_message(
        &mut sender_session.stream,
        &Message::FrameChunk(FrameChunk {
            bytes: b"alpha+payload-partial".to_vec(),
        }),
    )
    .unwrap();
    write_message(
        &mut sender_session.stream,
        &Message::FrameAbort(FrameAbort {
            frame_no: alpha_frame.frame_no,
        }),
    )
    .unwrap();

    let abort_ack = match read_message(&mut sender_session.stream).unwrap() {
        Message::AbortAck(ack) => ack,
        unexpected => panic!("expected AbortAck, got {:?}", unexpected.kind()),
    };

    let ack1 = send_frame(
        &mut sender_session.stream,
        &alpha_frame,
        [b"aaa".as_slice()],
    )
    .unwrap();
    let ack2 = send_frame(
        &mut sender_session.stream,
        &payload_frame,
        [b"bbbb".as_slice()],
    )
    .unwrap();
    let ack3 = send_frame(&mut sender_session.stream, &tail_frame, [b"cc".as_slice()]).unwrap();
    let finish = send_finish(
        &mut sender_session.stream,
        tail_frame.frame_no,
        tail_frame.last_file_index,
        &tail_frame.last_file_path,
    )
    .unwrap();

    let (
        receiver_abort_ack,
        receiver_ack1,
        receiver_ack2,
        receiver_ack3,
        receiver_finish,
        archive_len,
    ) = receiver.join().unwrap();

    assert_eq!(
        abort_ack,
        AbortAck {
            frame_no: 1,
            archive_len: 0,
        }
    );
    assert_eq!(ack1.archive_len, 3);
    assert_eq!(ack2.archive_len, 7);
    assert_eq!(ack3.archive_len, 9);
    assert_eq!(receiver_abort_ack, abort_ack);
    assert_eq!(receiver_ack1, ack1);
    assert_eq!(receiver_ack2, ack2);
    assert_eq!(receiver_ack3, ack3);
    assert_eq!(receiver_finish, finish);
    assert_eq!(archive_len, 9);
}

#[test]
fn protocol_tail_persists_issue_journal_before_finish() {
    let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr();
    let data_dir = TestDir::new();
    let receiver_dir = data_dir.path().to_path_buf();

    let receiver = thread::spawn(move || {
        let mut session = listener.accept(Role::Receiver, None, None).unwrap();
        let report = receive_dump(&mut session.stream, receiver_dir.clone()).unwrap();
        let issue_journal_path = receiver_dir.join("dump-09.tar.zst.issues.tsv");
        let issue_journal = fs::read_to_string(&issue_journal_path).unwrap();
        (report, issue_journal, issue_journal_path)
    });

    let mut sender_session = connect_once(Role::Sender, addr, Some("dump-09"), None).unwrap();
    let resume_state = request_resume_state(&mut sender_session.stream, "dump-09").unwrap();
    assert_eq!(resume_state.archive_len, 0);

    write_message(
        &mut sender_session.stream,
        &Message::IssueJournalChunk(IssueJournalChunk {
            entries: vec![IssueJournalEntry {
                file_index: 4,
                file_path: "broken/file.bin".to_owned(),
                attempts: 3,
                reason: IssueJournalReason::UnstableAfterRetries,
                last_error: "short read".to_owned(),
            }],
        }),
    )
    .unwrap();
    let finish = send_finish(&mut sender_session.stream, 0, 0, "").unwrap();

    let (report, issue_journal, issue_journal_path) = receiver.join().unwrap();
    assert_eq!(report.finish, finish);
    assert!(report.final_archive_path.exists());
    assert!(issue_journal_path.exists());
    assert!(issue_journal.contains("4\t3\tunstable_after_retries\tbroken/file.bin\tshort read"));
}

#[test]
fn protocol_v3_rejects_frame_before_manifest_start() {
    let _env = crate::support::EnvVarGuard::set(&[]);
    let receiver_dir = TestDir::new();
    let (addr, receiver) = spawn_receiver_dump_once_v3(receiver_dir.path(), 1, 1);

    let mut sender_session =
        connect_once(Role::Sender, addr, Some("dump-v3-order-frame-first"), None).unwrap();
    let resume_state =
        request_resume_state_v3(&mut sender_session.stream, "dump-v3-order-frame-first").unwrap();
    assert_eq!(resume_state.manifest, empty_manifest_boundary_v3());
    assert_eq!(resume_state.archive.archive_len, 0);

    write_message_v3(
        &mut sender_session.stream,
        &V3Message::FrameStart(fast_dump::network::protocol::FrameStartV3 {
            frame_no: 1,
            first_work_ordinal: 1,
            level: 3,
            threads: 1,
        }),
    )
    .unwrap();
    sender_session.stream.flush().unwrap();

    let reply = read_message_v3(&mut sender_session.stream).unwrap();
    let V3Message::Error(error) = reply else {
        panic!("expected protocol error, got {reply:?}");
    };
    assert!(
        error
            .message
            .contains("expected v3 manifest start before frame start")
    );

    drop(sender_session);
    let error = receiver.join().unwrap().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unexpected v3 frame start before manifest start")
    );
}

#[test]
fn protocol_v3_rejects_issue_tail_before_manifest_seal() {
    let _env = crate::support::EnvVarGuard::set(&[]);
    let receiver_dir = TestDir::new();
    let dump_id = "dump-v3-order-issue-before-seal";
    let (addr, receiver) = spawn_receiver_dump_once_v3(receiver_dir.path(), 1, 1);

    let mut sender_session = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    let resume_state = request_resume_state_v3(&mut sender_session.stream, dump_id).unwrap();
    assert_eq!(resume_state.manifest, empty_manifest_boundary_v3());

    write_message_v3(
        &mut sender_session.stream,
        &V3Message::ManifestStart(ManifestStart {
            base_manifest: empty_manifest_boundary_v3(),
        }),
    )
    .unwrap();
    write_message_v3(
        &mut sender_session.stream,
        &V3Message::ManifestChunk(ManifestChunk {
            last_record_seq: Some(1),
            last_work_ordinal: None,
            bytes: b"manifest-record".to_vec(),
        }),
    )
    .unwrap();
    sender_session.stream.flush().unwrap();

    let ack = read_message_v3(&mut sender_session.stream).unwrap();
    assert!(matches!(ack, V3Message::ManifestAck(_)), "reply={ack:?}");

    write_message_v3(
        &mut sender_session.stream,
        &V3Message::IssueChunk(IssueChunkV3 {
            entries: vec![sender_state_issue_v3(1, "sender-state")],
        }),
    )
    .unwrap();
    sender_session.stream.flush().unwrap();

    let reply = read_message_v3(&mut sender_session.stream).unwrap();
    let V3Message::Error(error) = reply else {
        panic!("expected protocol error, got {reply:?}");
    };
    assert!(
        error
            .message
            .contains("v3 issue tail requires sealed manifest boundary")
    );

    drop(sender_session);
    let error = receiver.join().unwrap().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unexpected v3 issue tail before manifest seal")
    );
    let paths = ReceiverPaths::new(receiver_dir.path().to_path_buf(), dump_id).unwrap();
    assert!(!paths.issue_journal_temp_path().exists());
    assert!(!paths.issue_journal_path().exists());
}

#[test]
fn protocol_v3_persists_typed_issue_journal_before_finish() {
    let _env = crate::support::EnvVarGuard::set(&[]);
    let receiver_dir = TestDir::new();
    let dump_id = "dump-v3-issues";
    let (addr, receiver) = spawn_receiver_dump_once_v3(receiver_dir.path(), 1, 1);

    let mut sender_session = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    let resume_state = request_resume_state_v3(&mut sender_session.stream, dump_id).unwrap();
    assert_eq!(resume_state.manifest, empty_manifest_boundary_v3());

    write_message_v3(
        &mut sender_session.stream,
        &V3Message::ManifestStart(ManifestStart {
            base_manifest: empty_manifest_boundary_v3(),
        }),
    )
    .unwrap();
    write_message_v3(
        &mut sender_session.stream,
        &V3Message::ManifestChunk(ManifestChunk {
            last_record_seq: Some(1),
            last_work_ordinal: None,
            bytes: b"manifest-record".to_vec(),
        }),
    )
    .unwrap();
    sender_session.stream.flush().unwrap();
    let reply = read_message_v3(&mut sender_session.stream).unwrap();
    assert!(
        matches!(reply, V3Message::ManifestAck(_)),
        "reply={reply:?}"
    );

    write_message_v3(
        &mut sender_session.stream,
        &V3Message::ManifestSeal(ManifestSeal {
            final_record_seq: Some(1),
            final_work_ordinal: None,
        }),
    )
    .unwrap();
    sender_session.stream.flush().unwrap();
    let reply = read_message_v3(&mut sender_session.stream).unwrap();
    assert!(
        matches!(reply, V3Message::ManifestAck(_)),
        "reply={reply:?}"
    );

    write_message_v3(
        &mut sender_session.stream,
        &V3Message::IssueChunk(IssueChunkV3 {
            entries: vec![sender_state_issue_v3(1, "sender-state")],
        }),
    )
    .unwrap();
    write_message_v3(
        &mut sender_session.stream,
        &V3Message::Finish(fast_dump::network::protocol::FinishV3 {
            final_frame_no: 0,
            final_work_ordinal: None,
        }),
    )
    .unwrap();
    sender_session.stream.flush().unwrap();

    let reply = read_message_v3(&mut sender_session.stream).unwrap();
    assert!(matches!(reply, V3Message::FinishAck));

    let report = receiver.join().unwrap().unwrap();
    let issue_journal_path = receiver_dir
        .path()
        .join(format!("{dump_id}.tar.zst.issues.tsv"));
    let issue_journal = fs::read_to_string(&issue_journal_path).unwrap();
    assert!(report.final_archive_path.exists());
    assert!(issue_journal_path.exists());
    assert!(issue_journal.contains("scope\trecord_seq\twork_ordinal\tattempts\tcode\tpath"));
    assert!(issue_journal.contains(
        "sender_state\t1\t\t0\tsender_state_event\tsender-state\tstate-transition\tmanifest recovery"
    ));
}

#[test]
fn protocol_v3_discards_typed_issue_temp_after_unexpected_message() {
    let _env = crate::support::EnvVarGuard::set(&[]);
    let receiver_dir = TestDir::new();
    let dump_id = "dump-v3-issues-discard";
    let (addr, receiver) = spawn_receiver_dump_once_v3(receiver_dir.path(), 1, 1);

    let mut sender_session = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    let resume_state = request_resume_state_v3(&mut sender_session.stream, dump_id).unwrap();
    assert_eq!(resume_state.manifest, empty_manifest_boundary_v3());

    write_message_v3(
        &mut sender_session.stream,
        &V3Message::ManifestStart(ManifestStart {
            base_manifest: empty_manifest_boundary_v3(),
        }),
    )
    .unwrap();
    write_message_v3(
        &mut sender_session.stream,
        &V3Message::ManifestChunk(ManifestChunk {
            last_record_seq: Some(1),
            last_work_ordinal: None,
            bytes: b"manifest-record".to_vec(),
        }),
    )
    .unwrap();
    sender_session.stream.flush().unwrap();
    let reply = read_message_v3(&mut sender_session.stream).unwrap();
    assert!(
        matches!(reply, V3Message::ManifestAck(_)),
        "reply={reply:?}"
    );

    write_message_v3(
        &mut sender_session.stream,
        &V3Message::ManifestSeal(ManifestSeal {
            final_record_seq: Some(1),
            final_work_ordinal: None,
        }),
    )
    .unwrap();
    sender_session.stream.flush().unwrap();
    let reply = read_message_v3(&mut sender_session.stream).unwrap();
    assert!(
        matches!(reply, V3Message::ManifestAck(_)),
        "reply={reply:?}"
    );

    write_message_v3(
        &mut sender_session.stream,
        &V3Message::IssueChunk(IssueChunkV3 {
            entries: vec![sender_state_issue_v3(1, "sender-state")],
        }),
    )
    .unwrap();
    write_message_v3(
        &mut sender_session.stream,
        &V3Message::FrameStart(fast_dump::network::protocol::FrameStartV3 {
            frame_no: 1,
            first_work_ordinal: 1,
            level: 3,
            threads: 1,
        }),
    )
    .unwrap();
    sender_session.stream.flush().unwrap();

    let reply = read_message_v3(&mut sender_session.stream).unwrap();
    let V3Message::Error(error) = reply else {
        panic!("expected protocol error, got {reply:?}");
    };
    assert!(
        error
            .message
            .contains("expected v3 finish after typed issue tail")
    );

    drop(sender_session);
    let error = receiver.join().unwrap().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unexpected v3 message after issue tail")
    );

    let paths = ReceiverPaths::new(receiver_dir.path().to_path_buf(), dump_id).unwrap();
    assert!(!paths.issue_journal_temp_path().exists());
    assert!(!paths.issue_journal_path().exists());
}

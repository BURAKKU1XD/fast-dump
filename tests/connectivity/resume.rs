use std::collections::BTreeMap;
use std::fs;
use std::time::Duration;

use crate::support::*;
use fast_dump::network::protocol::{
    AbortAck, FrameAbort, FrameChunk, FrameStart, Message, read_message, request_resume_state,
    write_message,
};
use fast_dump::network::transport::connect_once;
use fast_dump::receiver::{ReceiverJournal, ReceiverPaths};
use fast_dump::role::Role;
use fast_dump::sender::{prepare_dump, send_dump, send_frame};
use socket2::SockRef;

#[test]
fn push_mode_rolls_back_unacked_frame_on_disconnect_and_resumes_from_last_commit() {
    let source_dir = TestDir::new();
    let mut expected = BTreeMap::new();
    insert_expected_random_file(&mut expected, source_dir.path(), "alpha.bin", 16 * 1024, 1);
    insert_expected_random_file(&mut expected, source_dir.path(), "beta.bin", 16 * 1024, 2);
    insert_expected_random_file(&mut expected, source_dir.path(), "gamma.bin", 16 * 1024, 3);

    let dump_id = "dump-rollback-before-ack";
    let options = default_send_options(1);
    let prepared = prepare_dump(
        source_dir.path().to_path_buf(),
        &empty_resume_state(),
        3,
        1,
        1,
    )
    .unwrap();
    assert_eq!(prepared.frames.len(), 3);

    let receiver_dir = TestDir::new();
    let paths = ReceiverPaths::new(receiver_dir.path().to_path_buf(), dump_id).unwrap();

    let (addr1, receiver1) = spawn_receiver_dump_once(receiver_dir.path());
    let mut sender1 = connect_once(Role::Sender, addr1, Some(dump_id), None).unwrap();
    assert_eq!(
        request_resume_state(&mut sender1.stream, dump_id).unwrap(),
        empty_resume_state()
    );
    let ack1 = send_frame(
        &mut sender1.stream,
        &prepared.frames[0].outbound,
        prepared.frames[0].chunks.iter().map(Vec::as_slice),
    )
    .unwrap();
    drop(sender1);
    assert!(receiver1.join().unwrap().is_err());

    let journal_after_first_frame = ReceiverJournal::load(paths.journal_path()).unwrap();
    assert_eq!(journal_after_first_frame.last_committed_frame, 1);
    assert_eq!(
        journal_after_first_frame.last_committed_file_path,
        prepared.frames[0].outbound.last_file_path
    );
    assert_eq!(journal_after_first_frame.archive_len, ack1.archive_len);

    let (addr2, receiver2) = spawn_receiver_dump_once(receiver_dir.path());
    let mut sender2 = connect_once(Role::Sender, addr2, Some(dump_id), None).unwrap();
    let resume_after_first_frame = request_resume_state(&mut sender2.stream, dump_id).unwrap();
    assert_eq!(resume_after_first_frame.last_committed_frame, 1);
    assert_eq!(resume_after_first_frame.archive_len, ack1.archive_len);
    send_prepared_frame_without_waiting_for_ack(&mut sender2.stream, &prepared.frames[1]);
    SockRef::from(&sender2.stream)
        .set_linger(Some(Duration::from_secs(0)))
        .unwrap();
    drop(sender2);
    assert!(receiver2.join().unwrap().is_err());

    let journal_after_rollback = ReceiverJournal::load(paths.journal_path()).unwrap();
    assert_eq!(journal_after_rollback, journal_after_first_frame);
    assert_eq!(
        fs::metadata(paths.partial_archive_path()).unwrap().len(),
        ack1.archive_len
    );

    let (addr3, receiver3) = spawn_receiver_dump_once(receiver_dir.path());
    let mut sender3 = connect_once(Role::Sender, addr3, Some(dump_id), None).unwrap();
    let report = send_dump(
        &mut sender3.stream,
        source_dir.path().to_path_buf(),
        dump_id,
        &options,
    )
    .unwrap();
    let receive_report = receiver3.join().unwrap().unwrap();

    assert_eq!(report.resume_state.last_committed_frame, 1);
    assert_eq!(report.resume_state.last_committed_file_index, 1);
    assert_eq!(report.sent_frames, 2);
    assert_eq!(report.final_frame_no, 3);
    assert_eq!(receive_report.finish.final_frame_no, 3);
    assert_archive_hashes(&receive_report.final_archive_path, &expected);
}

#[test]
fn push_mode_resumes_cleanly_after_aborting_shared_frame_attempt() {
    let source_dir = TestDir::new();
    let mut expected = BTreeMap::new();
    insert_expected_file(&mut expected, source_dir.path(), "alpha.txt", b"alpha");
    insert_expected_random_file(
        &mut expected,
        source_dir.path(),
        "payload.bin",
        256 * 1024,
        41,
    );
    insert_expected_file(&mut expected, source_dir.path(), "zeta.txt", b"tail");

    let dump_id = "dump-aborted-shared-frame";
    let options = default_send_options(16 * 1024 * 1024);
    let receiver_dir = TestDir::new();
    let paths = ReceiverPaths::new(receiver_dir.path().to_path_buf(), dump_id).unwrap();

    let (addr1, receiver1) = spawn_receiver_dump_once(receiver_dir.path());
    let mut sender1 = connect_once(Role::Sender, addr1, Some(dump_id), None).unwrap();
    assert_eq!(
        request_resume_state(&mut sender1.stream, dump_id).unwrap(),
        empty_resume_state()
    );

    write_message(
        &mut sender1.stream,
        &Message::FrameStart(FrameStart {
            frame_no: 1,
            first_file_index: 1,
            first_file_path: "alpha.txt".to_owned(),
            level: 3,
            threads: 1,
        }),
    )
    .unwrap();
    write_message(
        &mut sender1.stream,
        &Message::FrameChunk(FrameChunk {
            bytes: b"alpha+payload-partial".to_vec(),
        }),
    )
    .unwrap();
    write_message(
        &mut sender1.stream,
        &Message::FrameAbort(FrameAbort { frame_no: 1 }),
    )
    .unwrap();

    let abort_ack = match read_message(&mut sender1.stream).unwrap() {
        Message::AbortAck(ack) => ack,
        unexpected => panic!("expected AbortAck, got {:?}", unexpected.kind()),
    };
    assert_eq!(
        abort_ack,
        AbortAck {
            frame_no: 1,
            archive_len: 0,
        }
    );

    drop(sender1);
    assert!(receiver1.join().unwrap().is_err());

    let (addr2, receiver2) = spawn_receiver_dump_once(receiver_dir.path());
    let mut sender2 = connect_once(Role::Sender, addr2, Some(dump_id), None).unwrap();
    assert_eq!(
        request_resume_state(&mut sender2.stream, dump_id).unwrap(),
        empty_resume_state()
    );
    drop(sender2);
    assert!(receiver2.join().unwrap().is_err());

    if paths.partial_archive_path().exists() {
        assert_eq!(fs::metadata(paths.partial_archive_path()).unwrap().len(), 0);
    }

    let (addr3, receiver3) = spawn_receiver_dump_once(receiver_dir.path());
    let mut sender3 = connect_once(Role::Sender, addr3, Some(dump_id), None).unwrap();
    let report = send_dump(
        &mut sender3.stream,
        source_dir.path().to_path_buf(),
        dump_id,
        &options,
    )
    .unwrap();
    let receive_report = receiver3.join().unwrap().unwrap();

    assert_eq!(report.resume_state, empty_resume_state());
    assert_eq!(report.sent_frames, 1);
    assert_eq!(report.final_frame_no, 1);
    assert_archive_hashes(&receive_report.final_archive_path, &expected);
}

#[test]
fn push_mode_resumes_after_multiple_committed_frames() {
    let source_dir = TestDir::new();
    let mut expected = BTreeMap::new();
    for index in 0..4_u64 {
        insert_expected_random_file(
            &mut expected,
            source_dir.path(),
            &format!("resume/file-{index}.bin"),
            12 * 1024,
            10 + index,
        );
    }

    let dump_id = "dump-resume-after-acks";
    let options = default_send_options(1);
    let prepared = prepare_dump(
        source_dir.path().to_path_buf(),
        &empty_resume_state(),
        3,
        1,
        1,
    )
    .unwrap();
    assert_eq!(prepared.frames.len(), 4);

    let receiver_dir = TestDir::new();
    let paths = ReceiverPaths::new(receiver_dir.path().to_path_buf(), dump_id).unwrap();

    let (addr1, receiver1) = spawn_receiver_dump_once(receiver_dir.path());
    let mut sender1 = connect_once(Role::Sender, addr1, Some(dump_id), None).unwrap();
    assert_eq!(
        request_resume_state(&mut sender1.stream, dump_id).unwrap(),
        empty_resume_state()
    );
    let _ack1 = send_frame(
        &mut sender1.stream,
        &prepared.frames[0].outbound,
        prepared.frames[0].chunks.iter().map(Vec::as_slice),
    )
    .unwrap();
    let ack2 = send_frame(
        &mut sender1.stream,
        &prepared.frames[1].outbound,
        prepared.frames[1].chunks.iter().map(Vec::as_slice),
    )
    .unwrap();
    drop(sender1);
    assert!(receiver1.join().unwrap().is_err());

    let journal_after_disconnect = ReceiverJournal::load(paths.journal_path()).unwrap();
    assert_eq!(journal_after_disconnect.last_committed_frame, 2);
    assert_eq!(journal_after_disconnect.last_committed_file_index, 2);
    assert_eq!(journal_after_disconnect.archive_len, ack2.archive_len);

    let (addr2, receiver2) = spawn_receiver_dump_once(receiver_dir.path());
    let mut sender2 = connect_once(Role::Sender, addr2, Some(dump_id), None).unwrap();
    let report = send_dump(
        &mut sender2.stream,
        source_dir.path().to_path_buf(),
        dump_id,
        &options,
    )
    .unwrap();
    let receive_report = receiver2.join().unwrap().unwrap();

    assert_eq!(report.resume_state.last_committed_frame, 2);
    assert_eq!(report.resume_state.last_committed_file_index, 2);
    assert_eq!(report.sent_frames, 2);
    assert_eq!(report.final_frame_no, 4);
    assert_eq!(receive_report.finish.final_frame_no, 4);
    assert_archive_hashes(&receive_report.final_archive_path, &expected);
}

#[test]
fn push_mode_rolls_back_mid_frame_disconnect_to_last_commit_and_resumes() {
    let source_dir = TestDir::new();
    let mut expected = BTreeMap::new();
    insert_expected_random_file(&mut expected, source_dir.path(), "alpha.bin", 16 * 1024, 21);
    insert_expected_random_file(&mut expected, source_dir.path(), "beta.bin", 16 * 1024, 22);
    insert_expected_random_file(&mut expected, source_dir.path(), "gamma.bin", 16 * 1024, 23);

    let dump_id = "dump-mid-frame-disconnect";
    let options = default_send_options(1);
    let prepared = prepare_dump(
        source_dir.path().to_path_buf(),
        &empty_resume_state(),
        3,
        1,
        1,
    )
    .unwrap();
    assert_eq!(prepared.frames.len(), 3);

    let receiver_dir = TestDir::new();
    let paths = ReceiverPaths::new(receiver_dir.path().to_path_buf(), dump_id).unwrap();

    let (addr1, receiver1) = spawn_receiver_dump_once(receiver_dir.path());
    let mut sender1 = connect_once(Role::Sender, addr1, Some(dump_id), None).unwrap();
    assert_eq!(
        request_resume_state(&mut sender1.stream, dump_id).unwrap(),
        empty_resume_state()
    );
    let ack1 = send_frame(
        &mut sender1.stream,
        &prepared.frames[0].outbound,
        prepared.frames[0].chunks.iter().map(Vec::as_slice),
    )
    .unwrap();
    send_partial_prepared_frame(&mut sender1.stream, &prepared.frames[1], 1);
    SockRef::from(&sender1.stream)
        .set_linger(Some(Duration::from_secs(0)))
        .unwrap();
    drop(sender1);
    assert!(receiver1.join().unwrap().is_err());

    let journal_after_disconnect = ReceiverJournal::load(paths.journal_path()).unwrap();
    assert_eq!(journal_after_disconnect.last_committed_frame, 1);
    assert_eq!(journal_after_disconnect.last_committed_file_index, 1);
    assert_eq!(journal_after_disconnect.archive_len, ack1.archive_len);
    assert_eq!(
        fs::metadata(paths.partial_archive_path()).unwrap().len(),
        ack1.archive_len
    );

    let (addr2, receiver2) = spawn_receiver_dump_once(receiver_dir.path());
    let mut sender2 = connect_once(Role::Sender, addr2, Some(dump_id), None).unwrap();
    let report = send_dump(
        &mut sender2.stream,
        source_dir.path().to_path_buf(),
        dump_id,
        &options,
    )
    .unwrap();
    let receive_report = receiver2.join().unwrap().unwrap();

    assert_eq!(report.resume_state.last_committed_frame, 1);
    assert_eq!(report.resume_state.last_committed_file_index, 1);
    assert_eq!(report.sent_frames, 2);
    assert_eq!(report.final_frame_no, 3);
    assert_eq!(receive_report.finish.final_frame_no, 3);
    assert_archive_hashes(&receive_report.final_archive_path, &expected);
}

#[test]
fn push_mode_rolls_back_checksum_mismatch_to_last_commit_and_resumes_after_restart() {
    let source_dir = TestDir::new();
    let mut expected = BTreeMap::new();
    insert_expected_random_file(&mut expected, source_dir.path(), "alpha.bin", 16 * 1024, 31);
    insert_expected_random_file(&mut expected, source_dir.path(), "beta.bin", 16 * 1024, 32);
    insert_expected_random_file(&mut expected, source_dir.path(), "gamma.bin", 16 * 1024, 33);

    let dump_id = "dump-checksum-mismatch";
    let options = default_send_options(1);
    let prepared = prepare_dump(
        source_dir.path().to_path_buf(),
        &empty_resume_state(),
        3,
        1,
        1,
    )
    .unwrap();
    assert_eq!(prepared.frames.len(), 3);

    let receiver_dir = TestDir::new();
    let paths = ReceiverPaths::new(receiver_dir.path().to_path_buf(), dump_id).unwrap();

    let (addr1, receiver1) = spawn_receiver_dump_once(receiver_dir.path());
    let mut sender1 = connect_once(Role::Sender, addr1, Some(dump_id), None).unwrap();
    assert_eq!(
        request_resume_state(&mut sender1.stream, dump_id).unwrap(),
        empty_resume_state()
    );
    let ack1 = send_frame(
        &mut sender1.stream,
        &prepared.frames[0].outbound,
        prepared.frames[0].chunks.iter().map(Vec::as_slice),
    )
    .unwrap();
    send_prepared_frame_with_bad_checksum(&mut sender1.stream, &prepared.frames[1]);
    let reply = read_message(&mut sender1.stream).unwrap();
    match reply {
        Message::Error(error) => {
            assert!(error.message.contains("frame checksum mismatch"));
        }
        other => panic!("unexpected reply after checksum mismatch: {other:?}"),
    }
    drop(sender1);
    assert!(receiver1.join().unwrap().is_err());

    let journal_after_corruption = ReceiverJournal::load(paths.journal_path()).unwrap();
    assert_eq!(journal_after_corruption.last_committed_frame, 1);
    assert_eq!(journal_after_corruption.last_committed_file_index, 1);
    assert_eq!(journal_after_corruption.archive_len, ack1.archive_len);
    assert_eq!(
        fs::metadata(paths.partial_archive_path()).unwrap().len(),
        ack1.archive_len
    );

    let (addr2, receiver2) = spawn_receiver_dump_once(receiver_dir.path());
    let mut sender2 = connect_once(Role::Sender, addr2, Some(dump_id), None).unwrap();
    let report = send_dump(
        &mut sender2.stream,
        source_dir.path().to_path_buf(),
        dump_id,
        &options,
    )
    .unwrap();
    let receive_report = receiver2.join().unwrap().unwrap();

    assert_eq!(report.resume_state.last_committed_frame, 1);
    assert_eq!(report.resume_state.last_committed_file_index, 1);
    assert_eq!(report.sent_frames, 2);
    assert_eq!(report.final_frame_no, 3);
    assert_eq!(receive_report.finish.final_frame_no, 3);
    assert_archive_hashes(&receive_report.final_archive_path, &expected);
}

#[test]
fn push_mode_rejects_resume_when_sender_source_tree_changed() {
    let source_dir = TestDir::new();
    fs::write(source_dir.path().join("alpha.txt"), b"alpha").unwrap();
    fs::write(source_dir.path().join("beta.txt"), b"beta").unwrap();

    let dump_id = "dump-resume-mismatch";
    let options = default_send_options(1);
    let prepared = prepare_dump(
        source_dir.path().to_path_buf(),
        &empty_resume_state(),
        3,
        1,
        1,
    )
    .unwrap();
    assert_eq!(prepared.frames.len(), 2);

    let receiver_dir = TestDir::new();
    let paths = ReceiverPaths::new(receiver_dir.path().to_path_buf(), dump_id).unwrap();

    let (addr1, receiver1) = spawn_receiver_dump_once(receiver_dir.path());
    let mut sender1 = connect_once(Role::Sender, addr1, Some(dump_id), None).unwrap();
    assert_eq!(
        request_resume_state(&mut sender1.stream, dump_id).unwrap(),
        empty_resume_state()
    );
    let ack1 = send_frame(
        &mut sender1.stream,
        &prepared.frames[0].outbound,
        prepared.frames[0].chunks.iter().map(Vec::as_slice),
    )
    .unwrap();
    drop(sender1);
    assert!(receiver1.join().unwrap().is_err());

    let committed_journal = ReceiverJournal::load(paths.journal_path()).unwrap();
    assert_eq!(committed_journal.last_committed_frame, 1);
    assert_eq!(committed_journal.archive_len, ack1.archive_len);

    fs::remove_file(source_dir.path().join("alpha.txt")).unwrap();
    fs::write(source_dir.path().join("aardvark.txt"), b"aardvark").unwrap();

    let (addr2, receiver2) = spawn_receiver_dump_once(receiver_dir.path());
    let mut sender2 = connect_once(Role::Sender, addr2, Some(dump_id), None).unwrap();
    let error = send_dump(
        &mut sender2.stream,
        source_dir.path().to_path_buf(),
        dump_id,
        &options,
    )
    .unwrap_err();
    drop(sender2);
    assert!(receiver2.join().unwrap().is_err());

    assert!(
        error
            .to_string()
            .contains("sender source no longer matches resume state"),
        "{error:#}"
    );
    assert_eq!(
        ReceiverJournal::load(paths.journal_path()).unwrap(),
        committed_journal
    );
}

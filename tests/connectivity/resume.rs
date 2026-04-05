use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::time::Duration;

use crate::support::*;
use fast_dump::network::protocol::{
    AbortAck, FrameAbort, FrameChunk, FrameEndV3, FrameStart, ManifestBoundaryV3, ManifestChunk,
    ManifestSeal, ManifestStart, Message, V3Message, read_message, read_message_v3,
    request_resume_state, request_resume_state_v3, write_message, write_message_v3,
};
use fast_dump::network::transport::connect_once;
use fast_dump::receiver::{ReceiverJournal, ReceiverPaths, ReceiverState};
use fast_dump::role::Role;
use fast_dump::sender::{prepare_dump, send_dump, send_dump_v3, send_frame};
use socket2::SockRef;
use xxhash_rust::xxh3::Xxh3Default;

fn empty_manifest_boundary_v3() -> ManifestBoundaryV3 {
    ManifestBoundaryV3 {
        manifest_bytes_persisted: 0,
        last_record_seq: None,
        last_work_ordinal: None,
        sealed: false,
    }
}

fn xxh3_64(bytes: &[u8]) -> u64 {
    let mut hasher = Xxh3Default::new();
    hasher.update(bytes);
    hasher.digest()
}

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
fn receiver_state_open_discards_abandoned_v3_temp_artifacts_without_existing_journal() {
    let receiver_dir = TestDir::new();
    let dump_id = "dump-stale-v3-temp";
    let paths = ReceiverPaths::new(receiver_dir.path().to_path_buf(), dump_id).unwrap();
    fs::write(paths.manifest_path(), b"stale-manifest-tail").unwrap();
    fs::write(paths.issue_journal_temp_path(), b"stale-issue-tail").unwrap();

    let state = ReceiverState::open(receiver_dir.path().to_path_buf(), dump_id).unwrap();

    assert_eq!(state.journal.last_committed_frame, 0);
    assert_eq!(state.journal.last_committed_file_index, 0);
    assert_eq!(state.journal.archive_len, 0);
    assert_eq!(state.journal.manifest_bytes_persisted, 0);
    assert_eq!(fs::metadata(state.paths.manifest_path()).unwrap().len(), 0);
    assert!(!state.paths.issue_journal_temp_path().exists());
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

#[test]
fn push_mode_v3_resumes_from_last_committed_work_ordinal() {
    let _env = EnvVarGuard::set(&[]);
    let receiver_dir = TestDir::new();
    let dump_id = "dump-v3-resume";
    let paths = ReceiverPaths::new(receiver_dir.path().to_path_buf(), dump_id).unwrap();

    let (addr1, receiver1) = spawn_receiver_dump_once_v3(receiver_dir.path(), 1, 1);
    let mut sender1 = connect_once(Role::Sender, addr1, Some(dump_id), None).unwrap();
    let resume1 = request_resume_state_v3(&mut sender1.stream, dump_id).unwrap();
    assert_eq!(resume1.manifest, empty_manifest_boundary_v3());
    assert_eq!(resume1.archive.archive_len, 0);

    write_message_v3(
        &mut sender1.stream,
        &V3Message::ManifestStart(ManifestStart {
            base_manifest: empty_manifest_boundary_v3(),
        }),
    )
    .unwrap();
    write_message_v3(
        &mut sender1.stream,
        &V3Message::ManifestChunk(ManifestChunk {
            last_record_seq: Some(1),
            last_work_ordinal: Some(1),
            bytes: b"manifest-1".to_vec(),
        }),
    )
    .unwrap();
    sender1.stream.flush().unwrap();
    let reply = read_message_v3(&mut sender1.stream).unwrap();
    assert!(
        matches!(reply, V3Message::ManifestAck(_)),
        "reply={reply:?}"
    );

    write_message_v3(
        &mut sender1.stream,
        &V3Message::ManifestChunk(ManifestChunk {
            last_record_seq: Some(2),
            last_work_ordinal: Some(2),
            bytes: b"manifest-2".to_vec(),
        }),
    )
    .unwrap();
    sender1.stream.flush().unwrap();
    let reply = read_message_v3(&mut sender1.stream).unwrap();
    assert!(
        matches!(reply, V3Message::ManifestAck(_)),
        "reply={reply:?}"
    );

    write_message_v3(
        &mut sender1.stream,
        &V3Message::ManifestSeal(ManifestSeal {
            final_record_seq: Some(2),
            final_work_ordinal: Some(2),
        }),
    )
    .unwrap();
    sender1.stream.flush().unwrap();
    let reply = read_message_v3(&mut sender1.stream).unwrap();
    assert!(
        matches!(reply, V3Message::ManifestAck(_)),
        "reply={reply:?}"
    );

    write_message_v3(
        &mut sender1.stream,
        &V3Message::FrameStart(fast_dump::network::protocol::FrameStartV3 {
            frame_no: 1,
            first_work_ordinal: 1,
            level: 3,
            threads: 1,
        }),
    )
    .unwrap();
    write_message_v3(
        &mut sender1.stream,
        &V3Message::FrameChunk(FrameChunk {
            bytes: b"abc".to_vec(),
        }),
    )
    .unwrap();
    write_message_v3(
        &mut sender1.stream,
        &V3Message::FrameEnd(FrameEndV3 {
            frame_no: 1,
            last_work_ordinal: 1,
            compressed_len: 3,
            compressed_xxh3_64: xxh3_64(b"abc"),
        }),
    )
    .unwrap();
    sender1.stream.flush().unwrap();

    let ack1 = read_message_v3(&mut sender1.stream).unwrap();
    let V3Message::Ack(ack1) = ack1 else {
        panic!("expected v3 ack, got {ack1:?}");
    };
    assert_eq!(ack1.frame_no, 1);
    assert_eq!(ack1.last_work_ordinal, 1);
    assert_eq!(ack1.archive_len, 3);

    drop(sender1);
    assert!(receiver1.join().unwrap().is_err());

    let journal_after_first_session = ReceiverJournal::load(paths.journal_path()).unwrap();
    assert!(journal_after_first_session.manifest_sealed);
    assert_eq!(
        journal_after_first_session.last_manifest_record_seq,
        Some(2)
    );
    assert_eq!(
        journal_after_first_session.last_manifest_work_ordinal,
        Some(2)
    );
    assert_eq!(journal_after_first_session.last_committed_frame, 1);
    assert_eq!(
        journal_after_first_session.last_committed_work_ordinal,
        Some(1)
    );
    assert_eq!(journal_after_first_session.archive_len, 3);

    let (addr2, receiver2) = spawn_receiver_dump_once_v3(receiver_dir.path(), 1, 1);
    let mut sender2 = connect_once(Role::Sender, addr2, Some(dump_id), None).unwrap();
    let resume2 = request_resume_state_v3(&mut sender2.stream, dump_id).unwrap();
    assert_eq!(resume2.manifest.last_record_seq, Some(2));
    assert_eq!(resume2.manifest.last_work_ordinal, Some(2));
    assert!(resume2.manifest.sealed);
    assert_eq!(resume2.archive.last_committed_frame, 1);
    assert_eq!(resume2.archive.last_committed_work_ordinal, Some(1));
    assert_eq!(resume2.archive.archive_len, 3);

    write_message_v3(
        &mut sender2.stream,
        &V3Message::ManifestStart(ManifestStart {
            base_manifest: resume2.manifest.clone(),
        }),
    )
    .unwrap();
    write_message_v3(
        &mut sender2.stream,
        &V3Message::FrameStart(fast_dump::network::protocol::FrameStartV3 {
            frame_no: 2,
            first_work_ordinal: 2,
            level: 3,
            threads: 1,
        }),
    )
    .unwrap();
    write_message_v3(
        &mut sender2.stream,
        &V3Message::FrameChunk(FrameChunk {
            bytes: b"def".to_vec(),
        }),
    )
    .unwrap();
    write_message_v3(
        &mut sender2.stream,
        &V3Message::FrameEnd(FrameEndV3 {
            frame_no: 2,
            last_work_ordinal: 2,
            compressed_len: 3,
            compressed_xxh3_64: xxh3_64(b"def"),
        }),
    )
    .unwrap();
    sender2.stream.flush().unwrap();

    let ack2 = read_message_v3(&mut sender2.stream).unwrap();
    let V3Message::Ack(ack2) = ack2 else {
        panic!("expected v3 ack, got {ack2:?}");
    };
    assert_eq!(ack2.frame_no, 2);
    assert_eq!(ack2.last_work_ordinal, 2);
    assert_eq!(ack2.archive_len, 6);

    write_message_v3(
        &mut sender2.stream,
        &V3Message::Finish(fast_dump::network::protocol::FinishV3 {
            final_frame_no: 2,
            final_work_ordinal: Some(2),
        }),
    )
    .unwrap();
    sender2.stream.flush().unwrap();
    assert!(matches!(
        read_message_v3(&mut sender2.stream).unwrap(),
        V3Message::FinishAck
    ));

    let receive_report = receiver2.join().unwrap().unwrap();
    assert_eq!(
        receive_report
            .resume_state
            .archive
            .last_committed_work_ordinal,
        Some(1)
    );
    assert_eq!(receive_report.finish.final_frame_no, 2);
    assert_eq!(receive_report.finish.final_work_ordinal, Some(2));
    assert_eq!(
        receive_report.state.journal.last_committed_work_ordinal,
        Some(2)
    );
    assert!(receive_report.final_archive_path.exists());
}

#[test]
fn push_mode_v3_sender_manifest_spool_quota_failure_aborts_before_transfer() {
    let _env = EnvVarGuard::set(&[("FAST_DUMP_TEST_SENDER_MANIFEST_SPOOL_MAX_BYTES", "1")]);
    let source_dir = TestDir::new();
    fs::write(source_dir.path().join("alpha.txt"), b"alpha").unwrap();

    let receiver_dir = TestDir::new();
    let dump_id = "dump-v3-sender-spool-quota";
    let paths = ReceiverPaths::new(receiver_dir.path().to_path_buf(), dump_id).unwrap();
    let options = default_send_options(1);

    let (addr, receiver) = spawn_receiver_dump_once_v3(receiver_dir.path(), 1, 1);
    let mut sender = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    let error = send_dump_v3(
        &mut sender.stream,
        source_dir.path().to_path_buf(),
        dump_id,
        &options,
    )
    .unwrap_err();
    drop(sender);
    assert!(receiver.join().unwrap().is_err());

    assert!(
        error
            .to_string()
            .contains("sender manifest spool build failed within quota")
    );
    let journal = ReceiverJournal::load(paths.journal_path()).unwrap();
    assert_eq!(journal.manifest_bytes_persisted, 0);
    assert_eq!(journal.last_manifest_record_seq, None);
    assert_eq!(journal.last_manifest_work_ordinal, None);
    assert_eq!(journal.archive_len, 0);
    if paths.manifest_path().exists() {
        assert_eq!(fs::metadata(paths.manifest_path()).unwrap().len(), 0);
    }
    if paths.partial_archive_path().exists() {
        assert_eq!(fs::metadata(paths.partial_archive_path()).unwrap().len(), 0);
    }
}

#[test]
fn push_mode_v3_receiver_manifest_quota_failure_preserves_committed_boundary() {
    let _env = EnvVarGuard::set(&[("FAST_DUMP_TEST_RECEIVER_MANIFEST_MAX_BYTES", "8")]);
    let receiver_dir = TestDir::new();
    let dump_id = "dump-v3-receiver-manifest-quota";
    let paths = ReceiverPaths::new(receiver_dir.path().to_path_buf(), dump_id).unwrap();

    let (addr, receiver) = spawn_receiver_dump_once_v3(receiver_dir.path(), 1, 1);
    let mut sender = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    let resume_state = request_resume_state_v3(&mut sender.stream, dump_id).unwrap();
    assert_eq!(resume_state.manifest, empty_manifest_boundary_v3());

    write_message_v3(
        &mut sender.stream,
        &V3Message::ManifestStart(ManifestStart {
            base_manifest: empty_manifest_boundary_v3(),
        }),
    )
    .unwrap();
    write_message_v3(
        &mut sender.stream,
        &V3Message::ManifestChunk(ManifestChunk {
            last_record_seq: Some(1),
            last_work_ordinal: Some(1),
            bytes: b"manifest-overflow".to_vec(),
        }),
    )
    .unwrap();
    sender.stream.flush().unwrap();

    let reply = read_message_v3(&mut sender.stream).unwrap();
    let V3Message::Error(error) = reply else {
        panic!("expected v3 error, got {reply:?}");
    };
    assert!(
        error
            .message
            .contains("receiver manifest storage quota exhausted")
    );

    drop(sender);
    let receive_error = receiver.join().unwrap().unwrap_err();
    assert!(receive_error.to_string().contains("quota exceeded"));

    let journal = ReceiverJournal::load(paths.journal_path()).unwrap();
    assert_eq!(journal.manifest_bytes_persisted, 0);
    assert_eq!(journal.last_manifest_record_seq, None);
    assert_eq!(journal.last_manifest_work_ordinal, None);
    assert!(!journal.manifest_sealed);
    assert_eq!(journal.archive_len, 0);
    if paths.manifest_path().exists() {
        assert_eq!(fs::metadata(paths.manifest_path()).unwrap().len(), 0);
    }
    if paths.partial_archive_path().exists() {
        assert_eq!(fs::metadata(paths.partial_archive_path()).unwrap().len(), 0);
    }
}

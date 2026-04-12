use std::fs;
use std::io::Write;

use crate::support::*;
use fast_dump::network::protocol::{
    FrameChunk, FrameEnd, ManifestBoundary, ManifestChunk, ManifestSeal, ManifestStart, Message,
    read_message, request_resume_state, write_message,
};
use fast_dump::network::transport::connect_once;
use fast_dump::receiver::{ReceiverJournal, ReceiverPaths};
use fast_dump::role::Role;
use fast_dump::sender::send_dump;
use xxhash_rust::xxh3::Xxh3Default;

fn empty_manifest_boundary() -> ManifestBoundary {
    ManifestBoundary {
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
fn push_mode_v3_resumes_from_last_committed_work_ordinal() {
    let _env = EnvVarGuard::set(&[]);
    let receiver_dir = TestDir::new();
    let dump_id = "dump-v3-resume";
    let paths = ReceiverPaths::new(receiver_dir.path().to_path_buf(), dump_id).unwrap();

    let (addr1, receiver1) = spawn_receiver_dump_once(receiver_dir.path(), 1, 1);
    let mut sender1 = connect_once(Role::Sender, addr1, Some(dump_id), None).unwrap();
    let resume1 = request_resume_state(&mut sender1.stream, dump_id).unwrap();
    assert_eq!(resume1.manifest, empty_manifest_boundary());
    assert_eq!(resume1.archive.archive_len, 0);

    write_message(
        &mut sender1.stream,
        &Message::ManifestStart(ManifestStart {
            base_manifest: empty_manifest_boundary(),
        }),
    )
    .unwrap();
    write_message(
        &mut sender1.stream,
        &Message::ManifestChunk(ManifestChunk {
            last_record_seq: Some(1),
            last_work_ordinal: Some(1),
            bytes: b"manifest-1".to_vec(),
        }),
    )
    .unwrap();
    sender1.stream.flush().unwrap();
    let reply = read_message(&mut sender1.stream).unwrap();
    assert!(matches!(reply, Message::ManifestAck(_)), "reply={reply:?}");

    write_message(
        &mut sender1.stream,
        &Message::ManifestChunk(ManifestChunk {
            last_record_seq: Some(2),
            last_work_ordinal: Some(2),
            bytes: b"manifest-2".to_vec(),
        }),
    )
    .unwrap();
    sender1.stream.flush().unwrap();
    let reply = read_message(&mut sender1.stream).unwrap();
    assert!(matches!(reply, Message::ManifestAck(_)), "reply={reply:?}");

    write_message(
        &mut sender1.stream,
        &Message::ManifestSeal(ManifestSeal {
            final_record_seq: Some(2),
            final_work_ordinal: Some(2),
        }),
    )
    .unwrap();
    sender1.stream.flush().unwrap();
    let reply = read_message(&mut sender1.stream).unwrap();
    assert!(matches!(reply, Message::ManifestAck(_)), "reply={reply:?}");

    write_message(
        &mut sender1.stream,
        &Message::FrameStart(fast_dump::network::protocol::FrameStart {
            frame_no: 1,
            first_work_ordinal: 1,
            level: 3,
            threads: 1,
        }),
    )
    .unwrap();
    write_message(
        &mut sender1.stream,
        &Message::FrameChunk(FrameChunk {
            bytes: b"abc".to_vec(),
        }),
    )
    .unwrap();
    write_message(
        &mut sender1.stream,
        &Message::FrameEnd(FrameEnd {
            frame_no: 1,
            last_work_ordinal: 1,
            compressed_len: 3,
            compressed_xxh3_64: xxh3_64(b"abc"),
        }),
    )
    .unwrap();
    sender1.stream.flush().unwrap();

    let ack1 = read_message(&mut sender1.stream).unwrap();
    let Message::Ack(ack1) = ack1 else {
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

    let (addr2, receiver2) = spawn_receiver_dump_once(receiver_dir.path(), 1, 1);
    let mut sender2 = connect_once(Role::Sender, addr2, Some(dump_id), None).unwrap();
    let resume2 = request_resume_state(&mut sender2.stream, dump_id).unwrap();
    assert_eq!(resume2.manifest.last_record_seq, Some(2));
    assert_eq!(resume2.manifest.last_work_ordinal, Some(2));
    assert!(resume2.manifest.sealed);
    assert_eq!(resume2.archive.last_committed_frame, 1);
    assert_eq!(resume2.archive.last_committed_work_ordinal, Some(1));
    assert_eq!(resume2.archive.archive_len, 3);

    write_message(
        &mut sender2.stream,
        &Message::ManifestStart(ManifestStart {
            base_manifest: resume2.manifest.clone(),
        }),
    )
    .unwrap();
    write_message(
        &mut sender2.stream,
        &Message::FrameStart(fast_dump::network::protocol::FrameStart {
            frame_no: 2,
            first_work_ordinal: 2,
            level: 3,
            threads: 1,
        }),
    )
    .unwrap();
    write_message(
        &mut sender2.stream,
        &Message::FrameChunk(FrameChunk {
            bytes: b"def".to_vec(),
        }),
    )
    .unwrap();
    write_message(
        &mut sender2.stream,
        &Message::FrameEnd(FrameEnd {
            frame_no: 2,
            last_work_ordinal: 2,
            compressed_len: 3,
            compressed_xxh3_64: xxh3_64(b"def"),
        }),
    )
    .unwrap();
    sender2.stream.flush().unwrap();

    let ack2 = read_message(&mut sender2.stream).unwrap();
    let Message::Ack(ack2) = ack2 else {
        panic!("expected v3 ack, got {ack2:?}");
    };
    assert_eq!(ack2.frame_no, 2);
    assert_eq!(ack2.last_work_ordinal, 2);
    assert_eq!(ack2.archive_len, 6);

    write_message(
        &mut sender2.stream,
        &Message::Finish(fast_dump::network::protocol::Finish {
            final_frame_no: 2,
            final_work_ordinal: Some(2),
        }),
    )
    .unwrap();
    sender2.stream.flush().unwrap();
    assert!(matches!(
        read_message(&mut sender2.stream).unwrap(),
        Message::FinishAck
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

    let (addr, receiver) = spawn_receiver_dump_once(receiver_dir.path(), 1, 1);
    let mut sender = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    let error = send_dump(
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

    let (addr, receiver) = spawn_receiver_dump_once(receiver_dir.path(), 1, 1);
    let mut sender = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    let resume_state = request_resume_state(&mut sender.stream, dump_id).unwrap();
    assert_eq!(resume_state.manifest, empty_manifest_boundary());

    write_message(
        &mut sender.stream,
        &Message::ManifestStart(ManifestStart {
            base_manifest: empty_manifest_boundary(),
        }),
    )
    .unwrap();
    write_message(
        &mut sender.stream,
        &Message::ManifestChunk(ManifestChunk {
            last_record_seq: Some(1),
            last_work_ordinal: Some(1),
            bytes: b"manifest-overflow".to_vec(),
        }),
    )
    .unwrap();
    sender.stream.flush().unwrap();

    let reply = read_message(&mut sender.stream).unwrap();
    let Message::Error(error) = reply else {
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

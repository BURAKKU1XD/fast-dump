use std::fs;
use std::io::Write;

use crate::support::{EnvVarGuard, TestDir, spawn_receiver_dump_once};
use fast_dump::network::protocol::{
    IssueChunk, IssueCode, IssueEntry, IssueScope, ManifestBoundary, ManifestChunk, ManifestSeal,
    ManifestStart, Message, read_message, request_resume_state, write_message,
};
use fast_dump::network::transport::connect_once;
use fast_dump::receiver::ReceiverPaths;
use fast_dump::role::Role;

fn empty_manifest_boundary() -> ManifestBoundary {
    ManifestBoundary {
        manifest_bytes_persisted: 0,
        last_record_seq: None,
        last_work_ordinal: None,
        sealed: false,
    }
}

fn sender_state_issue(record_seq: u64, path: &str) -> IssueEntry {
    IssueEntry {
        scope: IssueScope::SenderState,
        record_seq: Some(record_seq),
        work_ordinal: None,
        path: path.to_owned(),
        attempts: 0,
        code: IssueCode::SenderStateEvent,
        detail: "state-transition".to_owned(),
        context: "manifest recovery".to_owned(),
    }
}

#[test]
fn protocol_v3_rejects_frame_before_manifest_start() {
    let _env = EnvVarGuard::set(&[]);
    let receiver_dir = TestDir::new();
    let (addr, receiver) = spawn_receiver_dump_once(receiver_dir.path(), 1, 1);

    let mut sender_session =
        connect_once(Role::Sender, addr, Some("dump-v3-order-frame-first"), None).unwrap();
    let resume_state =
        request_resume_state(&mut sender_session.stream, "dump-v3-order-frame-first").unwrap();
    assert_eq!(resume_state.manifest, empty_manifest_boundary());
    assert_eq!(resume_state.archive.archive_len, 0);

    write_message(
        &mut sender_session.stream,
        &Message::FrameStart(fast_dump::network::protocol::FrameStart {
            frame_no: 1,
            first_work_ordinal: 1,
            level: 3,
            threads: 1,
        }),
    )
    .unwrap();
    sender_session.stream.flush().unwrap();

    let reply = read_message(&mut sender_session.stream).unwrap();
    let Message::Error(error) = reply else {
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
    let _env = EnvVarGuard::set(&[]);
    let receiver_dir = TestDir::new();
    let dump_id = "dump-v3-order-issue-before-seal";
    let (addr, receiver) = spawn_receiver_dump_once(receiver_dir.path(), 1, 1);

    let mut sender_session = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    let resume_state = request_resume_state(&mut sender_session.stream, dump_id).unwrap();
    assert_eq!(resume_state.manifest, empty_manifest_boundary());

    write_message(
        &mut sender_session.stream,
        &Message::ManifestStart(ManifestStart {
            base_manifest: empty_manifest_boundary(),
        }),
    )
    .unwrap();
    write_message(
        &mut sender_session.stream,
        &Message::ManifestChunk(ManifestChunk {
            last_record_seq: Some(1),
            last_work_ordinal: None,
            bytes: b"manifest-record".to_vec(),
        }),
    )
    .unwrap();
    sender_session.stream.flush().unwrap();

    let ack = read_message(&mut sender_session.stream).unwrap();
    assert!(matches!(ack, Message::ManifestAck(_)), "reply={ack:?}");

    write_message(
        &mut sender_session.stream,
        &Message::IssueChunk(IssueChunk {
            entries: vec![sender_state_issue(1, "sender-state")],
        }),
    )
    .unwrap();
    sender_session.stream.flush().unwrap();

    let reply = read_message(&mut sender_session.stream).unwrap();
    let Message::Error(error) = reply else {
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
    let _env = EnvVarGuard::set(&[]);
    let receiver_dir = TestDir::new();
    let dump_id = "dump-v3-issues";
    let (addr, receiver) = spawn_receiver_dump_once(receiver_dir.path(), 1, 1);

    let mut sender_session = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    let resume_state = request_resume_state(&mut sender_session.stream, dump_id).unwrap();
    assert_eq!(resume_state.manifest, empty_manifest_boundary());

    write_message(
        &mut sender_session.stream,
        &Message::ManifestStart(ManifestStart {
            base_manifest: empty_manifest_boundary(),
        }),
    )
    .unwrap();
    write_message(
        &mut sender_session.stream,
        &Message::ManifestChunk(ManifestChunk {
            last_record_seq: Some(1),
            last_work_ordinal: None,
            bytes: b"manifest-record".to_vec(),
        }),
    )
    .unwrap();
    sender_session.stream.flush().unwrap();
    let reply = read_message(&mut sender_session.stream).unwrap();
    assert!(matches!(reply, Message::ManifestAck(_)), "reply={reply:?}");

    write_message(
        &mut sender_session.stream,
        &Message::ManifestSeal(ManifestSeal {
            final_record_seq: Some(1),
            final_work_ordinal: None,
        }),
    )
    .unwrap();
    sender_session.stream.flush().unwrap();
    let reply = read_message(&mut sender_session.stream).unwrap();
    assert!(matches!(reply, Message::ManifestAck(_)), "reply={reply:?}");

    write_message(
        &mut sender_session.stream,
        &Message::IssueChunk(IssueChunk {
            entries: vec![sender_state_issue(1, "sender-state")],
        }),
    )
    .unwrap();
    write_message(
        &mut sender_session.stream,
        &Message::Finish(fast_dump::network::protocol::Finish {
            final_frame_no: 0,
            final_work_ordinal: None,
        }),
    )
    .unwrap();
    sender_session.stream.flush().unwrap();

    let reply = read_message(&mut sender_session.stream).unwrap();
    assert!(matches!(reply, Message::FinishAck));

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
    let _env = EnvVarGuard::set(&[]);
    let receiver_dir = TestDir::new();
    let dump_id = "dump-v3-issues-discard";
    let (addr, receiver) = spawn_receiver_dump_once(receiver_dir.path(), 1, 1);

    let mut sender_session = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    let resume_state = request_resume_state(&mut sender_session.stream, dump_id).unwrap();
    assert_eq!(resume_state.manifest, empty_manifest_boundary());

    write_message(
        &mut sender_session.stream,
        &Message::ManifestStart(ManifestStart {
            base_manifest: empty_manifest_boundary(),
        }),
    )
    .unwrap();
    write_message(
        &mut sender_session.stream,
        &Message::ManifestChunk(ManifestChunk {
            last_record_seq: Some(1),
            last_work_ordinal: None,
            bytes: b"manifest-record".to_vec(),
        }),
    )
    .unwrap();
    sender_session.stream.flush().unwrap();
    let reply = read_message(&mut sender_session.stream).unwrap();
    assert!(matches!(reply, Message::ManifestAck(_)), "reply={reply:?}");

    write_message(
        &mut sender_session.stream,
        &Message::ManifestSeal(ManifestSeal {
            final_record_seq: Some(1),
            final_work_ordinal: None,
        }),
    )
    .unwrap();
    sender_session.stream.flush().unwrap();
    let reply = read_message(&mut sender_session.stream).unwrap();
    assert!(matches!(reply, Message::ManifestAck(_)), "reply={reply:?}");

    write_message(
        &mut sender_session.stream,
        &Message::IssueChunk(IssueChunk {
            entries: vec![sender_state_issue(1, "sender-state")],
        }),
    )
    .unwrap();
    write_message(
        &mut sender_session.stream,
        &Message::FrameStart(fast_dump::network::protocol::FrameStart {
            frame_no: 1,
            first_work_ordinal: 1,
            level: 3,
            threads: 1,
        }),
    )
    .unwrap();
    sender_session.stream.flush().unwrap();

    let reply = read_message(&mut sender_session.stream).unwrap();
    let Message::Error(error) = reply else {
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

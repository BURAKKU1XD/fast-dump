use std::fs;
use std::process::Command;
use std::thread;

use crate::support::*;
use fast_dump::network::protocol::{ResumeState, request_resume_state};
use fast_dump::network::transport::{
    BoundListener, TransportMode, connect_once, establish_session,
};
use fast_dump::receiver::{receive_dump, respond_resume_query_checked};
use fast_dump::role::Role;

#[derive(Debug, Clone, Copy)]
struct SessionCase {
    label: &'static str,
    listener_role: Role,
    connector_role: Role,
    use_dispatch_wrapper: bool,
    dump_id: &'static str,
}

fn dump_id_for_role(role: Role, dump_id: &'static str) -> Option<&'static str> {
    match role {
        Role::Sender => Some(dump_id),
        Role::Receiver => None,
    }
}

fn assert_zero_resume_state(label: &str, state: &ResumeState) {
    assert_eq!(state.manifest.manifest_bytes_persisted, 0, "{label}");
    assert_eq!(state.manifest.last_record_seq, None, "{label}");
    assert_eq!(state.manifest.last_work_ordinal, None, "{label}");
    assert!(!state.manifest.sealed, "{label}");
    assert_eq!(state.archive.last_committed_frame, 0, "{label}");
    assert_eq!(state.archive.last_committed_work_ordinal, None, "{label}");
    assert_eq!(state.archive.archive_len, 0, "{label}");
}

#[test]
fn transport_establishes_sessions_for_push_and_pull_direct_and_dispatch_paths() {
    for case in [
        SessionCase {
            label: "push/direct",
            listener_role: Role::Receiver,
            connector_role: Role::Sender,
            use_dispatch_wrapper: false,
            dump_id: "dump-01",
        },
        SessionCase {
            label: "pull/direct",
            listener_role: Role::Sender,
            connector_role: Role::Receiver,
            use_dispatch_wrapper: false,
            dump_id: "dump-02",
        },
        SessionCase {
            label: "push/dispatch",
            listener_role: Role::Receiver,
            connector_role: Role::Sender,
            use_dispatch_wrapper: true,
            dump_id: "dump-04",
        },
        SessionCase {
            label: "pull/dispatch",
            listener_role: Role::Sender,
            connector_role: Role::Receiver,
            use_dispatch_wrapper: true,
            dump_id: "dump-05",
        },
    ] {
        let listener_dump_id = dump_id_for_role(case.listener_role, case.dump_id);
        let connector_dump_id = dump_id_for_role(case.connector_role, case.dump_id);

        let (connector_session, listener_session) = if case.use_dispatch_wrapper {
            let addr = reserve_loopback_addr();
            let listener = thread::spawn(move || {
                establish_session(
                    case.listener_role,
                    TransportMode::Listen(addr),
                    listener_dump_id,
                    None,
                )
            });
            let connector =
                connect_with_retry(case.connector_role, addr, connector_dump_id, None).unwrap();
            let accepted = listener.join().unwrap().unwrap();
            (connector, accepted)
        } else {
            let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = listener.local_addr();
            let accepted =
                thread::spawn(move || listener.accept(case.listener_role, listener_dump_id, None));
            let connector =
                connect_once(case.connector_role, addr, connector_dump_id, None).unwrap();
            let accepted = accepted.join().unwrap().unwrap();
            (connector, accepted)
        };

        assert_eq!(
            connector_session.local_role, case.connector_role,
            "{}",
            case.label
        );
        assert_eq!(
            connector_session.peer_role, case.listener_role,
            "{}",
            case.label
        );
        assert_eq!(
            listener_session.local_role, case.listener_role,
            "{}",
            case.label
        );
        assert_eq!(
            listener_session.peer_role, case.connector_role,
            "{}",
            case.label
        );
    }
}

#[test]
fn matching_roles_are_rejected() {
    let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr();

    let sender = thread::spawn(move || listener.accept(Role::Sender, Some("dump-03"), None));
    let result = connect_once(Role::Sender, addr, Some("dump-03"), None);
    let listener_result = sender.join().unwrap();

    assert!(result.is_err());
    assert!(listener_result.is_err());
}

#[test]
fn transport_bootstraps_resume_state_for_push_and_pull_after_handshake() {
    {
        let label = "push/bootstrap";
        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();
        let data_dir = TestDir::new();
        let receiver_dir = data_dir.path().to_path_buf();

        let receiver = thread::spawn(move || {
            let mut session = listener.accept(Role::Receiver, None, None).unwrap();
            let peer_dump_id = session.peer_dump_id.clone();
            let state = respond_resume_query_checked(
                &mut session.stream,
                receiver_dir,
                session.peer_dump_id.as_deref(),
            )
            .unwrap();
            (
                session.local_role,
                session.peer_role,
                peer_dump_id,
                state.resume_state(),
            )
        });

        let mut sender_session =
            connect_once(Role::Sender, addr, Some("dump-bootstrap-push"), None).unwrap();
        let resume_state =
            request_resume_state(&mut sender_session.stream, "dump-bootstrap-push").unwrap();
        let (receiver_role, peer_role, peer_dump_id, receiver_resume_state) =
            receiver.join().unwrap();

        assert_eq!(sender_session.local_role, Role::Sender, "{label}");
        assert_eq!(sender_session.peer_role, Role::Receiver, "{label}");
        assert_eq!(receiver_role, Role::Receiver, "{label}");
        assert_eq!(peer_role, Role::Sender, "{label}");
        assert_eq!(sender_session.peer_dump_id, None, "{label}");
        assert_eq!(
            peer_dump_id.as_deref(),
            Some("dump-bootstrap-push"),
            "{label}"
        );
        assert_zero_resume_state(label, &resume_state);
        assert_eq!(receiver_resume_state, resume_state, "{label}");
    }

    {
        let label = "pull/bootstrap";
        let addr = reserve_loopback_addr();
        let data_dir = TestDir::new();
        let receiver_dir = data_dir.path().to_path_buf();

        let sender = thread::spawn(move || {
            let mut session = establish_session(
                Role::Sender,
                TransportMode::Listen(addr),
                Some("dump-bootstrap-pull"),
                None,
            )
            .unwrap();
            let resume_state =
                request_resume_state(&mut session.stream, "dump-bootstrap-pull").unwrap();
            (session.local_role, session.peer_role, resume_state)
        });

        let mut receiver_session = connect_with_retry(Role::Receiver, addr, None, None).unwrap();
        let receiver_state = respond_resume_query_checked(
            &mut receiver_session.stream,
            receiver_dir,
            receiver_session.peer_dump_id.as_deref(),
        )
        .unwrap();
        let (sender_role, peer_role, sender_resume_state) = sender.join().unwrap();

        assert_eq!(receiver_session.local_role, Role::Receiver, "{label}");
        assert_eq!(receiver_session.peer_role, Role::Sender, "{label}");
        assert_eq!(
            receiver_session.peer_dump_id.as_deref(),
            Some("dump-bootstrap-pull"),
            "{label}"
        );
        assert_eq!(sender_role, Role::Sender, "{label}");
        assert_eq!(peer_role, Role::Receiver, "{label}");
        assert_zero_resume_state(label, &sender_resume_state);
        assert_eq!(
            receiver_state.resume_state(),
            sender_resume_state,
            "{label}"
        );
        assert_eq!(
            receiver_state.paths.dump_id(),
            "dump-bootstrap-pull",
            "{label}"
        );
    }
}

#[test]
fn push_mode_rejects_resume_query_dump_id_mismatch_after_handshake() {
    let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr();
    let data_dir = TestDir::new();
    let receiver_dir = data_dir.path().to_path_buf();

    let receiver = thread::spawn(move || {
        let mut session = listener.accept(Role::Receiver, None, None).unwrap();
        let peer_dump_id = session.peer_dump_id.clone();
        let error = respond_resume_query_checked(
            &mut session.stream,
            receiver_dir,
            session.peer_dump_id.as_deref(),
        )
        .unwrap_err();
        (peer_dump_id, error.to_string())
    });

    let mut sender_session = connect_once(Role::Sender, addr, Some("dump-09"), None).unwrap();
    let error = request_resume_state(&mut sender_session.stream, "dump-10").unwrap_err();
    let (peer_dump_id, receiver_error) = receiver.join().unwrap();

    assert_eq!(peer_dump_id.as_deref(), Some("dump-09"));
    assert!(
        error
            .to_string()
            .contains("peer rejected v3 resume query: resume dump_id mismatch: hello announced dump-09 but resume query requested dump-10")
    );
    assert!(receiver_error.contains(
        "resume dump_id mismatch: hello announced dump-09 but resume query requested dump-10"
    ));
}

#[test]
fn push_mode_token_authentication_accepts_matching_token_and_rejects_mismatches() {
    {
        let source_dir = TestDir::new();
        fs::write(source_dir.path().join("alpha.txt"), b"alpha").unwrap();

        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();
        let receiver_dir = TestDir::new();
        let receiver_path = receiver_dir.path().to_path_buf();

        let receiver = thread::spawn(move || {
            let mut session = listener
                .accept(Role::Receiver, None, Some("secret-token"))
                .unwrap();
            receive_dump(&mut session.stream, receiver_path, 65_536, 8 * 1024 * 1024).unwrap()
        });

        let output = Command::new(binary_path())
            .args([
                "sender",
                "--connect",
                &addr.to_string(),
                "--source",
                source_dir.path().to_str().unwrap(),
                "--dump-id",
                "dump-token-ok",
                "--token",
                "secret-token",
            ])
            .output()
            .unwrap();

        let report = receiver.join().unwrap();

        assert!(
            output.status.success(),
            "status={:?}, stderr={}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            archive_entries(&report.final_archive_path),
            vec![("alpha.txt".to_owned(), b"alpha".to_vec())]
        );
    }

    {
        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();

        let receiver = thread::spawn(move || {
            listener
                .accept(Role::Receiver, None, Some("secret"))
                .unwrap_err()
        });
        let error = connect_once(Role::Sender, addr, Some("dump-08"), Some("wrong")).unwrap_err();
        let receiver_error = receiver.join().unwrap();

        assert!(
            error
                .to_string()
                .contains("peer rejected handshake: bad token")
        );
        assert!(receiver_error.to_string().contains("bad token"));
    }

    {
        let source_dir = TestDir::new();
        fs::write(source_dir.path().join("alpha.txt"), b"alpha").unwrap();

        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();

        let receiver = thread::spawn(move || {
            listener
                .accept(Role::Receiver, None, Some("secret-token"))
                .unwrap_err()
        });

        let output = Command::new(binary_path())
            .args([
                "sender",
                "--connect",
                &addr.to_string(),
                "--source",
                source_dir.path().to_str().unwrap(),
                "--dump-id",
                "dump-token-fail",
                "--token",
                "wrong-token",
            ])
            .output()
            .unwrap();

        let receiver_error = receiver.join().unwrap();

        assert!(
            !output.status.success(),
            "stdout={}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("peer rejected handshake: bad token"),
            "stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(receiver_error.to_string().contains("bad token"));
    }
}

#[test]
fn receiver_binary_rejects_second_sender_when_max_sessions_is_one() {
    let addr = reserve_loopback_addr();
    let receiver_dir = TestDir::new();
    let mut receiver = Command::new(binary_path())
        .args([
            "receiver",
            "--listen",
            &addr.to_string(),
            "--data-dir",
            receiver_dir.path().to_str().unwrap(),
            "--max-sessions",
            "1",
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    thread::sleep(std::time::Duration::from_millis(250));

    let blocker = connect_once(Role::Sender, addr, Some("dump-blocker"), None).unwrap();
    let error = connect_once(Role::Sender, addr, Some("dump-busy"), None).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("peer rejected handshake: receiver reached max_sessions limit (1)")
    );

    drop(blocker);
    let _ = receiver.kill();
    let _ = receiver.wait_with_output().unwrap();
}

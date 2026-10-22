use std::fs;
use std::process::Command;
use std::thread;

use crate::support::*;
use fast_dump::network::transport::{
    BoundListener, TransportMode, connect_once, establish_session,
};
use fast_dump::receiver::receive_dump_profile;
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
fn push_mode_token_authentication_accepts_matching_token_and_rejects_mismatches() {
    {
        let source_dir = TestDir::new();
        fs::write(source_dir.path().join("alpha.txt"), b"alpha").unwrap();

        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();
        let receiver_dir = TestDir::new();
        let receiver_path = receiver_dir.path().to_path_buf();

        let receiver = thread::spawn(move || {
            let session = listener
                .accept(Role::Receiver, None, Some("secret-token"))
                .unwrap();
            let peer_dump_id = session.peer_dump_id.clone();
            receive_dump_profile(
                session.control_stream,
                session.data_stream,
                receiver_path,
                peer_dump_id.as_deref().unwrap(),
            )
            .unwrap()
            .0
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
                .contains("peer rejected v4 handshake: bad token")
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
            String::from_utf8_lossy(&output.stderr)
                .contains("peer rejected v4 handshake: bad token"),
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
    let source_dir = TestDir::new();
    fs::write(source_dir.path().join("alpha.txt"), b"alpha").unwrap();
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
    let output = Command::new(binary_path())
        .args([
            "sender",
            "--connect",
            &addr.to_string(),
            "--source",
            source_dir.path().to_str().unwrap(),
            "--dump-id",
            "dump-busy",
        ])
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "stdout={}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("peer rejected v4 resume query: receiver reached max_sessions limit (1)"),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    drop(blocker);
    let _ = receiver.kill();
    let _ = receiver.wait_with_output().unwrap();
}

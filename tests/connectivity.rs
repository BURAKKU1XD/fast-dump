use std::net::{SocketAddr, TcpListener};
use std::thread;
use std::time::Duration;

use anyhow::{Result, bail};
use fast_dump::network::transport::{
    BoundListener, TransportMode, connect_once, establish_session,
};
use fast_dump::role::Role;

fn reserve_loopback_addr() -> SocketAddr {
    let probe = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);
    addr
}

fn connect_with_retry(
    role: Role,
    addr: SocketAddr,
) -> Result<fast_dump::network::transport::Session> {
    let mut last_error = None;

    for _ in 0..40 {
        match establish_session(role, TransportMode::Connect(addr)) {
            Ok(session) => return Ok(session),
            Err(error) => {
                last_error = Some(error);
                thread::sleep(Duration::from_millis(10));
            }
        }
    }

    match last_error {
        Some(error) => Err(error),
        None => bail!("connect retry loop finished without an attempt"),
    }
}

// Сценарий push:
// sender сам подключается к receiver.
#[test]
fn push_mode_establishes_sender_to_receiver_connection() {
    let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr();

    let receiver = thread::spawn(move || listener.accept(Role::Receiver));
    let sender_session = connect_once(Role::Sender, addr).unwrap();
    let receiver_session = receiver.join().unwrap().unwrap();

    assert_eq!(sender_session.local_role, Role::Sender);
    assert_eq!(sender_session.peer_role, Role::Receiver);
    assert_eq!(receiver_session.local_role, Role::Receiver);
    assert_eq!(receiver_session.peer_role, Role::Sender);
}

// Сценарий pull:
// receiver подключается к sender, но роли по данным не меняются.
#[test]
fn pull_mode_establishes_receiver_to_sender_connection() {
    let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr();

    let sender = thread::spawn(move || listener.accept(Role::Sender));
    let receiver_session = connect_once(Role::Receiver, addr).unwrap();
    let sender_session = sender.join().unwrap().unwrap();

    assert_eq!(receiver_session.local_role, Role::Receiver);
    assert_eq!(receiver_session.peer_role, Role::Sender);
    assert_eq!(sender_session.local_role, Role::Sender);
    assert_eq!(sender_session.peer_role, Role::Receiver);
}

// Негативный тест: два sender не должны договариваться между собой.
#[test]
fn matching_roles_are_rejected() {
    let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr();

    let sender = thread::spawn(move || listener.accept(Role::Sender));
    let result = connect_once(Role::Sender, addr);
    let listener_result = sender.join().unwrap();

    assert!(result.is_err());
    assert!(listener_result.is_err());
}

#[test]
fn push_mode_establishes_session_via_dispatch_wrapper() {
    let addr = reserve_loopback_addr();

    let receiver =
        thread::spawn(move || establish_session(Role::Receiver, TransportMode::Listen(addr)));
    let sender_session = connect_with_retry(Role::Sender, addr).unwrap();
    let receiver_session = receiver.join().unwrap().unwrap();

    assert_eq!(sender_session.local_role, Role::Sender);
    assert_eq!(sender_session.peer_role, Role::Receiver);
    assert_eq!(receiver_session.local_role, Role::Receiver);
    assert_eq!(receiver_session.peer_role, Role::Sender);
}

#[test]
fn pull_mode_establishes_session_via_dispatch_wrapper() {
    let addr = reserve_loopback_addr();

    let sender =
        thread::spawn(move || establish_session(Role::Sender, TransportMode::Listen(addr)));
    let receiver_session = connect_with_retry(Role::Receiver, addr).unwrap();
    let sender_session = sender.join().unwrap().unwrap();

    assert_eq!(receiver_session.local_role, Role::Receiver);
    assert_eq!(receiver_session.peer_role, Role::Sender);
    assert_eq!(sender_session.local_role, Role::Sender);
    assert_eq!(sender_session.peer_role, Role::Receiver);
}

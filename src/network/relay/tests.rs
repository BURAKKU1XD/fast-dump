use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

use super::access::{Lobby, LobbyEvent, LobbySettings};
use super::{Key, Role, establish, server};
use crate::cli::TransportArgs;
use crate::network::is_retryable_error;
use crate::receiver;
use crate::runtime::{Cancelled, RunControl};
use crate::sender::{self, BundlePlanOptions};
use crate::tests::{TestDir, read_archive_entries, send_first_bundle};

const DUMP_ID: &str = "relayed-dump";
const SENDER_KEY: Key = [0x11; 32];
const RECEIVER_KEY: Key = [0x22; 32];
const ACCESS_TOKEN: &str = "23456789ABCD";

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/relay")
        .join(name)
}

struct Relay {
    addr: SocketAddr,
    control: RunControl,
    thread: Option<JoinHandle<anyhow::Result<()>>>,
}

impl Relay {
    fn start() -> Self {
        Self::start_mode(false)
    }

    fn start_mode(access: bool) -> Self {
        let certificate = CertificateDer::from_pem_file(fixture("server.pem")).unwrap();
        let key = PrivateKeyDer::from_pem_file(fixture("server-key.pem")).unwrap();
        let tls = server::acceptor(vec![certificate], key).unwrap();
        Self::start_with_tls(tls, access)
    }

    fn start_with_tls(tls: TlsAcceptor, access: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let control = RunControl::new(None);
        let worker_control = control.clone();
        let thread = thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    let listener = tokio::net::TcpListener::from_std(listener)?;
                    if access {
                        super::access::listen_with_token(
                            listener,
                            tls,
                            &worker_control,
                            ACCESS_TOKEN.as_bytes().try_into().unwrap(),
                        )
                        .await
                    } else {
                        server::listen(
                            listener,
                            tls,
                            [("test".to_owned(), [SENDER_KEY, RECEIVER_KEY])].into(),
                            &worker_control,
                        )
                        .await
                    }
                })
        });
        Self {
            addr,
            control,
            thread: Some(thread),
        }
    }

    fn args(&self, key: &Path) -> TransportArgs {
        TransportArgs {
            relay: Some(self.addr.to_string()),
            room: Some("test".into()),
            relay_token_file: Some(key.to_owned()),
            relay_ca_file: Some(fixture("ca.pem")),
            ..TransportArgs::default()
        }
    }

    fn join(&self, token: &str, run: &RunControl) -> anyhow::Result<Lobby> {
        Lobby::connect(
            LobbySettings {
                endpoint: self.addr.to_string(),
                token: token.to_owned(),
                ca_file: Some(fixture("ca.pem")),
            },
            run,
        )
    }

    fn join_invitation(&self, invitation: &str, run: &RunControl) -> anyhow::Result<Lobby> {
        Lobby::connect(
            LobbySettings {
                endpoint: self.addr.to_string(),
                token: invitation.to_owned(),
                ca_file: None,
            },
            run,
        )
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.control.cancel();
        let _ = self.thread.take().unwrap().join();
    }
}

#[test]
fn relay_access_delays_wrong_token_and_allows_fifteen_clients() {
    let relay = Relay::start_mode(true);
    let run = RunControl::new(None);
    let start = std::time::Instant::now();
    let mut clients: Vec<_> = (0..15)
        .map(|_| relay.join(ACCESS_TOKEN, &run).unwrap())
        .collect();
    // Пятнадцать TLS-подключений подряд не должны ждать задержку отказа.
    assert!(start.elapsed() < Duration::from_secs(10));
    let full = relay.join(ACCESS_TOKEN, &run).err().unwrap();
    assert_eq!(full.to_string(), "Relay full");

    let start = std::time::Instant::now();
    let denied = relay.join("23456789ABCE", &run).err().unwrap();
    assert!(start.elapsed() >= Duration::from_secs(1));
    assert_eq!(denied.to_string(), "relay rejected the access token");
    drop(clients.pop());
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match relay.join(ACCESS_TOKEN, &run) {
            Ok(_) => break,
            Err(error)
                if error.to_string() == "Relay full" && std::time::Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("{error:#}"),
        }
    }
}

fn next_event(lobby: &mut Lobby) -> LobbyEvent {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(event) = lobby.next().unwrap() {
            return event;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "relay event timed out"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn relay_access_can_reverse_roles_without_reconnecting() {
    let (tls, fingerprint) = super::identity::generate().unwrap();
    let relay = Relay::start_with_tls(tls, true);
    let invitation = super::access::invitation(fingerprint, ACCESS_TOKEN);
    let run = RunControl::new(None);
    let watchdog_run = run.clone();
    let (finished, completion) = mpsc::channel::<()>();
    let watchdog = thread::spawn(move || {
        if completion.recv_timeout(Duration::from_secs(20)) == Err(mpsc::RecvTimeoutError::Timeout)
        {
            watchdog_run.cancel();
        }
    });
    let mut first = relay.join_invitation(&invitation, &run).unwrap();
    let mut second = relay.join_invitation(&invitation, &run).unwrap();
    let mut unrelated = relay.join_invitation(&invitation, &run).unwrap();
    let ids = (first.id, second.id);
    let name =
        crate::network::lobby::Frame::with_text(0, 0, &crate::network::lobby::hostname()).text();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !first
        .peers()
        .iter()
        .any(|peer| peer.id == second.id && peer.name == name)
    {
        assert!(
            std::time::Instant::now() < deadline,
            "relay did not show the peer hostname"
        );
        thread::sleep(Duration::from_millis(5));
    }
    for reverse in [false, true] {
        let (from, to) = if reverse {
            (&mut second, &mut first)
        } else {
            (&mut first, &mut second)
        };
        from.offer(to.id, "payload").unwrap();
        assert!(
            matches!(next_event(to), LobbyEvent::Incoming { peer, source } if peer == from.id && source == "payload")
        );
        assert!(unrelated.next().unwrap().is_none());
        to.accept(from.id).unwrap();
        let LobbyEvent::Start(send_ticket) = next_event(from) else {
            panic!("no sender ticket")
        };
        let LobbyEvent::Start(receive_ticket) = next_event(to) else {
            panic!("no receiver ticket")
        };
        assert_eq!(send_ticket.role, Role::Sender);
        assert_eq!(receive_ticket.role, Role::Receiver);
        let source = TestDir::new();
        let target = TestDir::new();
        fs::write(source.path().join("payload"), b"relay data").unwrap();
        let output = target.path().to_owned();
        let receiver_control = run.unobserved_session();
        let receive = thread::spawn(move || {
            let (session, _sockets) = receive_ticket.establish(None, &receiver_control)?;
            receiver::receive_dump(session.control_stream, session.data_stream, output, DUMP_ID)
        });
        let sender_control = run.unobserved_session();
        {
            let (mut session, _sockets) = send_ticket
                .establish(Some(DUMP_ID), &sender_control)
                .unwrap();
            sender::send_dump(
                &mut session.control_stream,
                &mut session.data_stream,
                source.path().to_owned(),
                DUMP_ID,
                BundlePlanOptions::default(),
            )
            .unwrap();
        }
        let received = receive.join().unwrap().unwrap();
        let entries = read_archive_entries(&received.final_archive_path);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].bytes, b"relay data");
        assert!(matches!(next_event(from), LobbyEvent::Released(id) if id == to.id));
        assert!(matches!(next_event(to), LobbyEvent::Released(id) if id == from.id));
    }
    assert_eq!((first.id, second.id), ids);
    drop(finished);
    watchdog.join().unwrap();
}

#[test]
fn relay_invitation_rejects_another_generated_public_key() {
    let (tls, fingerprint) = super::identity::generate().unwrap();
    let relay = Relay::start_with_tls(tls, true);
    let (_, other_fingerprint) = super::identity::generate().unwrap();
    assert_ne!(fingerprint, other_fingerprint);
    let run = RunControl::new(None);
    let wrong = super::access::invitation(other_fingerprint, ACCESS_TOKEN);
    let error = relay.join_invitation(&wrong, &run).err().unwrap();
    assert!(format!("{error:#}").contains("relay TLS handshake"));
    let right = super::access::invitation(fingerprint, ACCESS_TOKEN);
    assert_eq!(relay.join_invitation(&right, &run).unwrap().id, 1);
}

fn start_receiver(
    args: TransportArgs,
    output: PathBuf,
    run: &RunControl,
) -> JoinHandle<anyhow::Result<receiver::ReceiveReport>> {
    let control = run.unobserved_session();
    thread::spawn(move || {
        let (session, _sockets) = establish(&args, Role::Receiver, None, None, &control)?;
        receiver::receive_dump(
            session.control_stream,
            session.data_stream,
            output,
            session.peer_dump_id.as_deref().unwrap(),
        )
    })
}

#[test]
fn relay_restart_resumes_durable_bundle_and_delivers_finish_ack() {
    let source = TestDir::new();
    let target = TestDir::new();
    let keys = TestDir::new();
    let sender_key = keys.path().join("sender.key");
    let receiver_key = keys.path().join("receiver.key");
    fs::write(&sender_key, "11".repeat(32)).unwrap();
    fs::write(&receiver_key, "22".repeat(32)).unwrap();
    let first = vec![0x45; 640 * 1024];
    let second = vec![0x67; 640 * 1024];
    fs::write(source.path().join("first"), &first).unwrap();
    fs::write(source.path().join("second"), &second).unwrap();
    let options = BundlePlanOptions {
        bundle_target_bytes: 1024 * 1024,
        ..BundlePlanOptions::default()
    };

    let run = RunControl::new(None);
    let watchdog_run = run.clone();
    let (finished, completion) = mpsc::channel::<()>();
    let watchdog = thread::spawn(move || {
        if completion.recv_timeout(Duration::from_secs(20)) == Err(mpsc::RecvTimeoutError::Timeout)
        {
            watchdog_run.cancel();
        }
    });

    let relay = Relay::start();
    let receive = start_receiver(relay.args(&receiver_key), target.path().to_owned(), &run);
    let control = run.unobserved_session();
    let (mut session, _sockets) = establish(
        &relay.args(&sender_key),
        Role::Sender,
        Some(DUMP_ID),
        None,
        &control,
    )
    .unwrap();
    send_first_bundle(&mut session, source.path(), DUMP_ID, options, false);
    drop(relay);
    assert!(receive.join().unwrap().is_err());
    drop(session);

    let relay = Relay::start();
    let receive = start_receiver(relay.args(&receiver_key), target.path().to_owned(), &run);
    let (mut session, _sockets) = establish(
        &relay.args(&sender_key),
        Role::Sender,
        Some(DUMP_ID),
        None,
        &control,
    )
    .unwrap();
    let report = sender::send_dump(
        &mut session.control_stream,
        &mut session.data_stream,
        source.path().to_owned(),
        DUMP_ID,
        options,
    )
    .unwrap();
    assert_eq!(report.bundles_sent, 1);
    let received = receive.join().unwrap().unwrap();
    drop(finished);
    watchdog.join().unwrap();
    let entries = read_archive_entries(&received.final_archive_path);
    assert_eq!(entries.len(), 2);
    assert_eq!(
        (entries[0].path.as_str(), entries[0].bytes.as_slice()),
        ("first", first.as_slice())
    );
    assert_eq!(
        (entries[1].path.as_str(), entries[1].bytes.as_slice()),
        ("second", second.as_slice())
    );
}

#[test]
fn relay_rejects_key_for_the_opposite_role_without_retrying() {
    let relay = Relay::start();
    let keys = TestDir::new();
    let key = keys.path().join("receiver.key");
    fs::write(&key, "22".repeat(32)).unwrap();
    let control = RunControl::new(None).unobserved_session();
    let error = establish(
        &relay.args(&key),
        Role::Sender,
        Some(DUMP_ID),
        None,
        &control,
    )
    .err()
    .expect("role key rejection");
    assert!(
        error
            .to_string()
            .contains("relay rejected room or role key"),
        "{error:#}"
    );
    assert!(!is_retryable_error(&error));
}

#[test]
fn relay_requires_certificate_to_match_the_endpoint_name() {
    let relay = Relay::start();
    let keys = TestDir::new();
    let key = keys.path().join("sender.key");
    fs::write(&key, "11".repeat(32)).unwrap();
    let mut args = relay.args(&key);
    args.relay = Some(format!("localhost:{}", relay.addr.port()));
    let control = RunControl::new(None).unobserved_session();
    let error = establish(&args, Role::Sender, Some(DUMP_ID), None, &control)
        .err()
        .expect("certificate name rejection");
    assert!(
        error.to_string().contains("relay TLS handshake"),
        "{error:#}"
    );
    assert!(!is_retryable_error(&error));
}

#[test]
fn relay_wait_for_second_peer_is_cancellable() {
    let relay = Relay::start();
    let keys = TestDir::new();
    let key = keys.path().join("sender.key");
    fs::write(&key, "11".repeat(32)).unwrap();
    let args = relay.args(&key);
    let run = RunControl::new(None);
    let control = run.unobserved_session();
    let worker = thread::spawn(move || {
        establish(&args, Role::Sender, Some(DUMP_ID), None, &control)
            .err()
            .expect("cancelled pairing")
    });
    thread::sleep(Duration::from_millis(100));
    run.cancel();
    assert!(worker.join().unwrap().is::<Cancelled>());
}

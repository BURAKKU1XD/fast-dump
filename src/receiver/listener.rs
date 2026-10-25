use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::thread;
use std::time::Duration;
use std::{io::Read, net::Shutdown};

use anyhow::{Context, Result, bail};
use crossbeam_channel::{TryRecvError, unbounded};
use log::debug;

use crate::network::protocol::wire;
use crate::network::transport::{self, BoundListener, PendingChannels, PendingSessionKey, Session};
use crate::role::Role;
use crate::runtime::{RunControl, SessionControl, SocketRegistration};

use super::network::receive_dump_profile_controlled;

fn reject_busy_session(stream: &mut std::net::TcpStream, max_sessions: usize) {
    let _ = wire::send_error(
        stream,
        &format!("receiver reached max_sessions limit ({max_sessions})"),
    );
    // Небольшой drain уже прилетевших байт помогает вернуть peer'у Error,
    // а не TCP reset, если сокет закроется до того, как стек дочитает Hello.
    let _ = stream.set_read_timeout(Some(Duration::from_millis(25)));
    let _ = stream.shutdown(Shutdown::Write);
    let mut discard = [0_u8; 1024];
    loop {
        match stream.read(&mut discard) {
            Ok(0) => break,
            Ok(_) => continue,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                break;
            }
            Err(_) => break,
        }
    }
}

struct SessionSlotGuard {
    active_sessions: Arc<AtomicUsize>,
    terminated_tx: crossbeam_channel::Sender<()>,
}

impl Drop for SessionSlotGuard {
    fn drop(&mut self) {
        self.active_sessions.fetch_sub(1, Ordering::SeqCst);
        let _ = self.terminated_tx.send(());
    }
}

pub(crate) fn serve_listener(
    listener: BoundListener,
    data_dir: impl Into<PathBuf>,
    max_sessions: usize,
    token: Option<String>,
) -> Result<()> {
    let data_dir = data_dir.into();
    let _ = serve_listener_until(listener, data_dir, max_sessions, token, None)?;
    Ok(())
}

pub(crate) fn serve_listener_until(
    listener: BoundListener,
    data_dir: PathBuf,
    max_sessions: usize,
    token: Option<String>,
    stop_after_terminated_sessions: Option<usize>,
) -> Result<usize> {
    serve_listener_inner(
        listener,
        data_dir,
        max_sessions,
        token,
        stop_after_terminated_sessions,
        &RunControl::new(None),
    )
}

pub(crate) fn serve_listener_controlled(
    listener: BoundListener,
    data_dir: PathBuf,
    max_sessions: usize,
    token: Option<String>,
    run: &RunControl,
) -> Result<()> {
    serve_listener_inner(listener, data_dir, max_sessions, token, None, run).map(|_| ())
}

struct ListenerWorkers {
    control: SessionControl,
    handles: Vec<(SessionControl, thread::JoinHandle<()>)>,
}

impl ListenerWorkers {
    fn reap(&mut self) {
        let mut index = 0;
        while index < self.handles.len() {
            if self.handles[index].1.is_finished() {
                let _ = self.handles.swap_remove(index).1.join();
            } else {
                index += 1;
            }
        }
    }
}

impl Drop for ListenerWorkers {
    fn drop(&mut self) {
        // Этот control владеет регистрациями всех listener-сокетов; остановка
        // снимает сетевые ожидания до join даже при ошибке accept или панике.
        self.control.cancel();
        for (control, _) in &self.handles {
            control.cancel();
        }
        for (_, handle) in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

fn serve_listener_inner(
    listener: BoundListener,
    data_dir: PathBuf,
    max_sessions: usize,
    token: Option<String>,
    stop_after_terminated_sessions: Option<usize>,
    run: &RunControl,
) -> Result<usize> {
    debug!(
        "v4 receiver listener entering serve loop: addr={} data_dir={} max_sessions={}",
        listener.local_addr(),
        data_dir.display(),
        max_sessions
    );
    if max_sessions == 0 {
        bail!("max_sessions must be at least 1");
    }

    listener.set_nonblocking(true)?;
    let active_sessions = Arc::new(AtomicUsize::new(0));
    let (terminated_tx, terminated_rx) = unbounded::<()>();
    let mut terminated_sessions = 0_usize;
    let mut pending_channels = HashMap::<PendingSessionKey, PendingChannels>::new();
    let mut pending_guards = HashMap::<PendingSessionKey, Vec<SocketRegistration>>::new();
    let listener_control = run.unobserved_session();
    let mut workers = ListenerWorkers {
        control: listener_control.clone(),
        handles: Vec::new(),
    };

    loop {
        run.check()?;
        workers.reap();
        terminated_sessions += drain_terminated_sessions(&terminated_rx);
        transport::drop_expired_pending_channels(&mut pending_channels);
        pending_guards.retain(|key, _| pending_channels.contains_key(key));
        if stop_after_terminated_sessions.is_some_and(|limit| {
            terminated_sessions >= limit && active_sessions.load(Ordering::SeqCst) == 0
        }) {
            return Ok(terminated_sessions);
        }

        match listener.try_accept_stream() {
            Ok((stream, peer_addr)) => {
                let registration = listener_control.register(&stream)?;
                terminated_sessions += drain_terminated_sessions(&terminated_rx);
                match transport::establish_stream_session(
                    stream,
                    Role::Receiver,
                    None,
                    token.as_deref(),
                    Some(peer_addr),
                    None,
                ) {
                    Ok(channel) => {
                        let key = transport::pending_session_key(&channel);
                        pending_guards
                            .entry(key.clone())
                            .or_default()
                            .push(registration);
                        match transport::insert_pending_channel(&mut pending_channels, channel) {
                            Ok(Some(mut session)) => {
                                let registrations = pending_guards.remove(&key).unwrap_or_default();
                                if active_sessions.load(Ordering::SeqCst) >= max_sessions {
                                    debug!(
                                        "v4 receiver listener rejected session from {} because max_sessions={} is reached",
                                        peer_addr, max_sessions
                                    );
                                    reject_busy_session(&mut session.control_stream, max_sessions);
                                    continue;
                                }

                                active_sessions.fetch_add(1, Ordering::SeqCst);
                                let session_data_dir = data_dir.clone();
                                let session_active = active_sessions.clone();
                                let session_terminated_tx = terminated_tx.clone();
                                let session_control = run.session(
                                    Role::Receiver,
                                    session.peer_dump_id.as_deref().unwrap_or_default(),
                                    &peer_addr.to_string(),
                                );
                                let worker_control = session_control.clone();
                                let slot_guard = SessionSlotGuard {
                                    active_sessions: session_active,
                                    terminated_tx: session_terminated_tx,
                                };
                                let handle = thread::Builder::new().name("dump-receive".to_owned()).spawn(move || {
                                    let _slot_guard = slot_guard;
                                    let _registrations = registrations;
                                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        run_listener_session(session, session_data_dir, &session_control)
                                    })).unwrap_or_else(|_| Err(anyhow::anyhow!("receiver session panicked")));
                                    session_control.finish(&result);
                                    if let Err(error) = result
                                        && !session_control.is_cancelled() {
                                        session_control.log(log::Level::Error, &format!("v4 receiver session {peer_addr} failed: {error:#}"));
                                    }
                                }).context("failed to spawn receiver session")?;
                                workers.handles.push((worker_control, handle));
                            }
                            Ok(None) => {}
                            Err(error) => {
                                pending_channels.remove(&key);
                                pending_guards.remove(&key);
                                run.log(log::Level::Error, &format!("v4 receiver failed to pair incoming channel from {peer_addr}: {error:#}"));
                            }
                        }
                    }
                    Err(error) => {
                        run.check()?;
                        run.log(log::Level::Error, &format!("v4 receiver failed to establish incoming channel from {peer_addr}: {error:#}"));
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => {
                return Err(error).context("failed to accept incoming v4 receiver connection");
            }
        }
    }
}

fn drain_terminated_sessions(terminated_rx: &crossbeam_channel::Receiver<()>) -> usize {
    let mut terminated = 0_usize;
    loop {
        match terminated_rx.try_recv() {
            Ok(()) => terminated += 1,
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => return terminated,
        }
    }
}

fn run_listener_session(
    session: Session,
    data_dir: PathBuf,
    control: &SessionControl,
) -> Result<()> {
    let _control_registration = control.register(&session.control_stream)?;
    let _data_registration = control.register(&session.data_stream)?;
    let peer_addr = session.peer_addr;
    let peer_dump_id = session.peer_dump_id.clone();
    let (report, _) = receive_dump_profile_controlled(
        session.control_stream,
        session.data_stream,
        data_dir,
        peer_dump_id.as_deref().unwrap(),
        control,
    )?;
    debug!(
        "v4 receiver listener session completed: peer={} dump_id={} final_archive={}",
        peer_addr,
        peer_dump_id.as_deref().unwrap_or_default(),
        report.final_archive_path.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, thread};

    use super::*;
    use crate::network::transport::connect_once;
    use crate::sender::{BundlePlanOptions, send_dump};
    use crate::tests::{TestDir, read_archive_entries};

    #[test]
    fn cancelling_listener_joins_active_sessions_and_interrupts_pending_handshake() {
        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();
        let directory = TestDir::new();
        let data_dir = directory.path().to_owned();
        let run = RunControl::new(None);
        let worker_run = run.clone();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = thread::spawn(move || {
            done_tx
                .send(serve_listener_controlled(
                    listener,
                    data_dir,
                    2,
                    None,
                    &worker_run,
                ))
                .unwrap();
        });
        let mut first = connect_once(Role::Sender, addr, Some("first"), None).unwrap();
        wire::request_resume_state(&mut first.control_stream, "first").unwrap();
        let mut second = connect_once(Role::Sender, addr, Some("second"), None).unwrap();
        wire::request_resume_state(&mut second.control_stream, "second").unwrap();
        let pending = std::net::TcpStream::connect(addr).unwrap();
        run.cancel();
        let completed = done_rx.recv_timeout(Duration::from_secs(2));
        // Peers ещё открыты: успех означает, что listener сам завершил своих readers.
        assert!(
            completed
                .expect("listener did not join cancelled sessions")
                .unwrap_err()
                .is::<crate::runtime::Cancelled>()
        );
        worker.join().unwrap();
        for dump_id in ["first", "second"] {
            let receiver = super::super::session::ReceiverSession::open(
                directory.path().to_owned(),
                dump_id,
                1,
            )
            .unwrap();
            assert_eq!(receiver.resume_state().archive_len, 0);
        }
        drop((first, second, pending));
    }

    #[test]
    fn serve_listener_rejects_new_session_when_max_sessions_is_reached() {
        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();
        let receiver_dir = TestDir::new();
        let receiver_path = receiver_dir.path().to_path_buf();

        let server = thread::spawn(move || {
            serve_listener_until(listener, receiver_path, 1, None, Some(1)).unwrap()
        });

        let blocker = connect_once(Role::Sender, addr, Some("dump-blocker"), None).unwrap();
        let mut busy = connect_once(Role::Sender, addr, Some("dump-busy"), None).unwrap();
        let error = wire::request_resume_state(&mut busy.control_stream, "dump-busy").unwrap_err();

        assert!(
            error
                .to_string()
                .contains("peer rejected v4 resume query: receiver reached max_sessions limit (1)")
        );

        drop(blocker);
        assert_eq!(server.join().unwrap(), 1);
    }

    #[test]
    fn serve_listener_reuses_freed_session_slot_for_next_sender() {
        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();
        let receiver_dir = TestDir::new();
        let receiver_path = receiver_dir.path().to_path_buf();

        let source_a = TestDir::new();
        fs::write(source_a.path().join("alpha.txt"), b"alpha").unwrap();
        let source_b = TestDir::new();
        fs::create_dir_all(source_b.path().join("nested")).unwrap();
        fs::write(source_b.path().join("nested").join("beta.txt"), b"beta").unwrap();

        let server = thread::spawn(move || {
            serve_listener_until(listener, receiver_path, 1, None, Some(2)).unwrap()
        });

        let mut first_sender = connect_once(Role::Sender, addr, Some("dump-10"), None).unwrap();
        let first_report = send_dump(
            &mut first_sender.control_stream,
            &mut first_sender.data_stream,
            source_a.path().to_path_buf(),
            "dump-10",
            BundlePlanOptions {
                bundle_target_bytes: 1,
                compression_level: 3,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(first_report.final_bundle_no, 1);

        let mut second_sender = connect_once(Role::Sender, addr, Some("dump-11"), None).unwrap();
        let second_report = send_dump(
            &mut second_sender.control_stream,
            &mut second_sender.data_stream,
            source_b.path().to_path_buf(),
            "dump-11",
            BundlePlanOptions {
                bundle_target_bytes: 1,
                compression_level: 3,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(second_report.final_bundle_no, 1);

        assert_eq!(server.join().unwrap(), 2);
        assert_eq!(
            read_archive_entries(&receiver_dir.path().join("dump-10.tar.zst"))
                .into_iter()
                .filter(|entry| entry.entry_type.is_file())
                .map(|entry| (entry.path, entry.bytes))
                .collect::<Vec<_>>(),
            vec![("alpha.txt".to_owned(), b"alpha".to_vec())]
        );
        assert_eq!(
            read_archive_entries(&receiver_dir.path().join("dump-11.tar.zst"))
                .into_iter()
                .filter(|entry| entry.entry_type.is_file())
                .map(|entry| (entry.path, entry.bytes))
                .collect::<Vec<_>>(),
            vec![("nested/beta.txt".to_owned(), b"beta".to_vec())]
        );
    }

    #[test]
    fn serve_listener_rejects_zero_max_sessions() {
        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let temp = TestDir::new();

        let error = serve_listener_until(listener, temp.path().to_path_buf(), 0, None, Some(0))
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("max_sessions must be at least 1")
        );
    }
}

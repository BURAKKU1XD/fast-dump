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

use crate::network::transport::{self, BoundListener};
use crate::role::Role;

use super::session::receive_dump_checked;
use super::shared::send_protocol_error;

fn reject_busy_session(stream: &mut std::net::TcpStream, max_sessions: usize) {
    let _ = send_protocol_error(
        stream,
        &format!("receiver reached max_sessions limit ({max_sessions})"),
    );
    // Если закрыть сокет сразу с непрочитанным Hello от клиента, TCP может
    // превратить ответ в reset и клиент увидит EOF вместо protocol Error.
    // Небольшой drain уже присланных байт делает отказ детерминированным.
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

// В listen-режиме receiver превращается в небольшой сервер:
// listener остаётся открытым, а каждая dump-сессия обслуживается в отдельном потоке,
// пока число активных сессий не упрётся в `max_sessions`.
pub fn serve_listener(
    listener: BoundListener,
    data_dir: impl Into<PathBuf>,
    max_sessions: usize,
    token: Option<String>,
    manifest_ack_records: u64,
    manifest_ack_bytes: usize,
) -> Result<()> {
    let data_dir = data_dir.into();
    debug!(
        "receiver listener entering serve loop: addr={} data_dir={} max_sessions={} manifest_ack_records={} manifest_ack_bytes={}",
        listener.local_addr(),
        data_dir.display(),
        max_sessions,
        manifest_ack_records,
        manifest_ack_bytes
    );
    let _ = serve_listener_until(
        listener,
        data_dir,
        max_sessions,
        token,
        manifest_ack_records,
        manifest_ack_bytes,
        None,
    )?;
    Ok(())
}

pub(crate) fn serve_listener_until(
    listener: BoundListener,
    data_dir: PathBuf,
    max_sessions: usize,
    token: Option<String>,
    manifest_ack_records: u64,
    manifest_ack_bytes: usize,
    stop_after_terminated_sessions: Option<usize>,
) -> Result<usize> {
    if max_sessions == 0 {
        bail!("max_sessions must be at least 1");
    }

    debug!(
        "receiver listener configured: addr={} data_dir={} max_sessions={} manifest_ack_records={} manifest_ack_bytes={}",
        listener.local_addr(),
        data_dir.display(),
        max_sessions,
        manifest_ack_records,
        manifest_ack_bytes
    );
    listener.set_nonblocking(true)?;
    let active_sessions = Arc::new(AtomicUsize::new(0));
    let (terminated_tx, terminated_rx) = unbounded::<()>();
    let mut terminated_sessions = 0_usize;

    loop {
        terminated_sessions += drain_terminated_sessions(&terminated_rx);
        if stop_after_terminated_sessions.is_some_and(|limit| {
            terminated_sessions >= limit && active_sessions.load(Ordering::SeqCst) == 0
        }) {
            return Ok(terminated_sessions);
        }

        match listener.try_accept_stream() {
            Ok((mut stream, peer_addr)) => {
                terminated_sessions += drain_terminated_sessions(&terminated_rx);
                stream
                    .set_nonblocking(false)
                    .context("failed to restore accepted receiver stream to blocking mode")?;
                transport::configure_stream(&stream)?;

                if active_sessions.load(Ordering::SeqCst) >= max_sessions {
                    debug!(
                        "receiver listener rejected session from {} because max_sessions={} is reached",
                        peer_addr, max_sessions
                    );
                    reject_busy_session(&mut stream, max_sessions);
                    continue;
                }

                active_sessions.fetch_add(1, Ordering::SeqCst);
                debug!(
                    "receiver listener accepted session from {} active_sessions={}",
                    peer_addr,
                    active_sessions.load(Ordering::SeqCst)
                );
                let session_data_dir = data_dir.clone();
                let session_active = active_sessions.clone();
                let session_terminated_tx = terminated_tx.clone();
                let session_token = token.clone();
                let session_manifest_ack_records = manifest_ack_records;
                let session_manifest_ack_bytes = manifest_ack_bytes;
                thread::spawn(move || {
                    let _slot_guard = SessionSlotGuard {
                        active_sessions: session_active,
                        terminated_tx: session_terminated_tx,
                    };
                    if let Err(error) = run_listener_session(
                        stream,
                        peer_addr,
                        session_data_dir,
                        session_token,
                        session_manifest_ack_records,
                        session_manifest_ack_bytes,
                    ) {
                        eprintln!("receiver session {} failed: {error:#}", peer_addr);
                    }
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => {
                return Err(error).context("failed to accept incoming receiver connection");
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
    stream: std::net::TcpStream,
    peer_addr: std::net::SocketAddr,
    data_dir: PathBuf,
    token: Option<String>,
    manifest_ack_records: u64,
    manifest_ack_bytes: usize,
) -> Result<()> {
    debug!("receiver listener session establishing transport for peer={peer_addr}");
    let mut session = transport::establish_stream_session(
        stream,
        Role::Receiver,
        None,
        token.as_deref(),
        Some(peer_addr),
    )?;
    let report = receive_dump_checked(
        &mut session.stream,
        data_dir,
        session.peer_dump_id.as_deref(),
        manifest_ack_records,
        manifest_ack_bytes,
    )?;
    debug!(
        "receiver listener session completed: peer={} dump_id={} final_archive={}",
        peer_addr,
        report.state.paths.dump_id(),
        report.final_archive_path.display()
    );

    println!(
        "{} connected to {} at {}; dump_id={}; resume frame={} work_ordinal={:?} archive_len={}; final frame={} work_ordinal={:?} archive={}",
        session.local_role,
        session.peer_role,
        session.peer_addr,
        report.state.paths.dump_id(),
        report.resume_state.archive.last_committed_frame,
        report.resume_state.archive.last_committed_work_ordinal,
        report.resume_state.archive.archive_len,
        report.finish.final_frame_no,
        report.finish.final_work_ordinal,
        report.final_archive_path.display()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, thread};

    use super::*;
    use crate::network::transport::connect_once;
    use crate::receiver::test_support::*;
    use crate::sender::send_dump;

    #[test]
    fn serve_listener_rejects_new_session_when_max_sessions_is_reached() {
        let _guard = FsFailureGuard::new();
        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();
        let receiver_dir = TestDir::new();
        let receiver_path = receiver_dir.path().to_path_buf();

        let server = thread::spawn(move || {
            let options = sample_send_options();
            serve_listener_until(
                listener,
                receiver_path,
                1,
                None,
                options.manifest_ack_records,
                options.manifest_ack_bytes,
                Some(1),
            )
            .unwrap()
        });

        let blocker = connect_once(Role::Sender, addr, Some("dump-blocker"), None).unwrap();

        let error = connect_once(Role::Sender, addr, Some("dump-busy"), None).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("peer rejected handshake: receiver reached max_sessions limit (1)")
        );

        drop(blocker);
        assert_eq!(server.join().unwrap(), 1);
    }

    #[test]
    fn serve_listener_reuses_freed_session_slot_for_next_sender() {
        let _guard = FsFailureGuard::new();
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
            let options = sample_send_options();
            serve_listener_until(
                listener,
                receiver_path,
                1,
                None,
                options.manifest_ack_records,
                options.manifest_ack_bytes,
                Some(2),
            )
            .unwrap()
        });

        let mut first_sender = connect_once(Role::Sender, addr, Some("dump-10"), None).unwrap();
        let first_report = send_dump(
            &mut first_sender.stream,
            source_a.path().to_path_buf(),
            "dump-10",
            &sample_send_options(),
        )
        .unwrap();
        assert_eq!(first_report.sent_frames, 1);

        let mut second_sender = connect_once(Role::Sender, addr, Some("dump-11"), None).unwrap();
        let second_report = send_dump(
            &mut second_sender.stream,
            source_b.path().to_path_buf(),
            "dump-11",
            &sample_send_options(),
        )
        .unwrap();
        assert_eq!(second_report.sent_frames, 1);

        assert_eq!(server.join().unwrap(), 2);
        assert_eq!(
            archive_entries(&receiver_dir.path().join("dump-10.tar.zst")),
            vec![("alpha.txt".to_owned(), b"alpha".to_vec())]
        );
        assert_eq!(
            archive_entries(&receiver_dir.path().join("dump-11.tar.zst")),
            vec![("nested/beta.txt".to_owned(), b"beta".to_vec())]
        );
    }

    #[test]
    fn serve_listener_rejects_zero_max_sessions() {
        let _guard = FsFailureGuard::new();
        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let temp = TestDir::new();
        let options = sample_send_options();

        let error = serve_listener_until(
            listener,
            temp.path().to_path_buf(),
            0,
            None,
            options.manifest_ack_records,
            options.manifest_ack_bytes,
            Some(0),
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("max_sessions must be at least 1")
        );
    }
}

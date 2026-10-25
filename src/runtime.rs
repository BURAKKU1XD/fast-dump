use std::collections::HashMap;
use std::fmt;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;

use anyhow::{Context, Result};

use crate::network::transport::CloneableStream;
use crate::progress::{Phase, ProgressHub, SessionProgress};
use crate::role::Role;

#[derive(Debug)]
pub(crate) struct Cancelled;

impl fmt::Display for Cancelled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("operation cancelled")
    }
}
impl std::error::Error for Cancelled {}

#[derive(Clone)]
pub(crate) struct RunControl(Arc<RunInner>);

struct RunInner {
    cancelled: AtomicBool,
    sessions: Mutex<Vec<Weak<SessionInner>>>,
    progress: Option<Arc<ProgressHub>>,
    summaries: Mutex<Vec<String>>,
    retry_wait: Mutex<()>,
    retry_wakeup: Condvar,
}

impl RunControl {
    pub(crate) fn new(progress: Option<Arc<ProgressHub>>) -> Self {
        Self(Arc::new(RunInner {
            cancelled: AtomicBool::new(false),
            sessions: Mutex::new(Vec::new()),
            progress,
            summaries: Mutex::new(Vec::new()),
            retry_wait: Mutex::new(()),
            retry_wakeup: Condvar::new(),
        }))
    }

    pub(crate) fn session(&self, role: Role, dump_id: &str, peer: &str) -> SessionControl {
        self.make_session(
            self.0
                .progress
                .as_ref()
                .map(|hub| hub.session(role.to_string(), dump_id, peer)),
        )
    }

    pub(crate) fn unobserved_session(&self) -> SessionControl {
        self.make_session(None)
    }

    fn make_session(&self, progress: Option<Arc<SessionProgress>>) -> SessionControl {
        let mut sessions = self.0.sessions.lock().unwrap_or_else(|e| e.into_inner());
        sessions.retain(|session| session.strong_count() != 0);
        let inner = Arc::new(SessionInner {
            cancelled: Arc::new(AtomicBool::new(self.is_cancelled())),
            sockets: Mutex::new(HashMap::new()),
            next_socket: AtomicU64::new(1),
            progress,
            hub: self.0.progress.clone(),
        });
        sessions.push(Arc::downgrade(&inner));
        SessionControl(inner)
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Acquire)
    }
    pub(crate) fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(Cancelled.into())
        } else {
            Ok(())
        }
    }

    pub(crate) fn cancel(&self) {
        let wait = self.0.retry_wait.lock().unwrap_or_else(|e| e.into_inner());
        self.0.cancelled.store(true, Ordering::Release);
        self.0.retry_wakeup.notify_all();
        drop(wait);
        let sessions: Vec<_> = self
            .0
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        for session in sessions {
            SessionControl(session).cancel();
        }
    }

    pub(crate) fn progress(&self) -> Option<Arc<ProgressHub>> {
        self.0.progress.clone()
    }

    pub(crate) fn wait_for_retry(&self, delay: Duration) -> Result<()> {
        // Проверка флага и ожидание под одним mutex исключают потерю сигнала отмены.
        let wait = self.0.retry_wait.lock().unwrap_or_else(|e| e.into_inner());
        let _wait = self
            .0
            .retry_wakeup
            .wait_timeout_while(wait, delay, |_| !self.is_cancelled())
            .unwrap_or_else(|e| e.into_inner());
        self.check()
    }

    pub(crate) fn disable_ui(&self) {
        if let Some(hub) = &self.0.progress {
            hub.ui_enabled.store(false, Ordering::Release);
        }
        self.flush_summaries();
    }

    pub(crate) fn summary(&self, summary: String) {
        if self
            .0
            .progress
            .as_ref()
            .is_some_and(|hub| hub.ui_enabled.load(Ordering::Acquire))
        {
            self.0
                .summaries
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(summary);
        } else {
            println!("{summary}");
        }
    }

    pub(crate) fn flush_summaries(&self) {
        for summary in self
            .0
            .summaries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
        {
            println!("{summary}");
        }
    }

    pub(crate) fn log(&self, level: log::Level, message: &str) {
        route_log(self.0.progress.as_ref(), level, message);
    }
}

#[derive(Clone)]
pub(crate) struct SessionControl(Arc<SessionInner>);

struct SessionInner {
    cancelled: Arc<AtomicBool>,
    sockets: Mutex<HashMap<u64, Arc<TcpStream>>>,
    next_socket: AtomicU64,
    progress: Option<Arc<SessionProgress>>,
    hub: Option<Arc<ProgressHub>>,
}

impl SessionControl {
    pub(crate) fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Acquire)
    }
    pub(crate) fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(Cancelled.into())
        } else {
            Ok(())
        }
    }
    pub(crate) fn progress(&self) -> Option<&Arc<SessionProgress>> {
        self.0.progress.as_ref()
    }

    pub(crate) fn register(&self, stream: &TcpStream) -> Result<SocketRegistration> {
        let socket = Arc::new(
            stream
                .try_clone()
                .context("failed to register cancellable socket")?,
        );
        let mut sockets = self.0.sockets.lock().unwrap_or_else(|e| e.into_inner());
        if self.is_cancelled() {
            drop(sockets);
            let _ = socket.shutdown_stream();
            return Err(Cancelled.into());
        }
        let id = self.0.next_socket.fetch_add(1, Ordering::Relaxed);
        sockets.insert(id, socket);
        Ok(SocketRegistration {
            session: Arc::downgrade(&self.0),
            id,
        })
    }

    pub(crate) fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::Release);
        let sockets: Vec<_> = self
            .0
            .sockets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect();
        // shutdown может войти в ОС; реестр в это время должен оставаться доступен.
        for socket in sockets {
            let _ = socket.shutdown_stream();
        }
    }

    pub(crate) fn set_identity(&self, dump_id: &str, peer: &str) {
        if let Some(progress) = self.progress() {
            progress.set_identity(dump_id, peer);
        }
    }

    pub(crate) fn finish(&self, result: &Result<()>) {
        if let Some(progress) = self.progress() {
            let phase = match result {
                Ok(()) if progress.skips.load(Ordering::Relaxed) > 0 => Phase::Warnings,
                Ok(()) => Phase::Completed,
                Err(_) if self.is_cancelled() => Phase::Stopped,
                Err(error) => {
                    progress.set_error(format!("{error:#}"));
                    Phase::Failed
                }
            };
            progress.set_phase(phase);
            let snapshot = progress.snapshot();
            self.log(
                if matches!(phase, Phase::Warnings | Phase::Stopped) {
                    log::Level::Warn
                } else if phase == Phase::Failed {
                    log::Level::Error
                } else {
                    log::Level::Info
                },
                &format!("{}: {}", snapshot.dump_id, phase.label()),
            );
        }
    }

    pub(crate) fn log(&self, level: log::Level, message: &str) {
        route_log(self.0.hub.as_ref(), level, message);
    }
}

pub(crate) struct SocketRegistration {
    session: Weak<SessionInner>,
    id: u64,
}

impl Drop for SocketRegistration {
    fn drop(&mut self) {
        if let Some(session) = self.session.upgrade() {
            session
                .sockets
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&self.id);
        }
    }
}

fn route_log(hub: Option<&Arc<ProgressHub>>, level: log::Level, message: &str) {
    if let Some(hub) = hub.filter(|hub| hub.ui_enabled.load(Ordering::Acquire)) {
        hub.log(level, message);
    } else {
        eprintln!("{message}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn cancellation_wakes_registered_read_and_rejects_late_registration() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (socket, _) = listener.accept().unwrap();
        let run = RunControl::new(None);
        let session = run.session(Role::Sender, "dump", "peer");
        let registration = session.register(&socket).unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let reader = thread::spawn(move || peer.read(&mut [0]));
        run.cancel();
        assert_eq!(reader.join().unwrap().unwrap(), 0);
        assert!(session.register(&socket).is_err());
        assert!(run.session(Role::Receiver, "late", "peer").is_cancelled());
        drop(registration);
        assert!(session.0.sockets.lock().unwrap().is_empty());
    }

    #[test]
    fn local_session_cancellation_does_not_stop_listener_or_siblings() {
        let run = RunControl::new(None);
        let first = run.session(Role::Receiver, "first", "peer");
        let second = run.session(Role::Receiver, "second", "peer");
        first.cancel();
        assert!(!run.is_cancelled());
        assert!(!second.is_cancelled());
        run.cancel();
        assert!(second.is_cancelled());
    }
}

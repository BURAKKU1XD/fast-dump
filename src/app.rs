use std::net::SocketAddr;

use anyhow::Result;
use log::debug;

use crate::cli::{Cli, Command, ReceiverArgs, SenderArgs, SenderRuntimeOptions};
use crate::network::relay;
use crate::network::transport::{
    BoundListener, CloneableStream, Session, TransportMode, establish_session_controlled,
};
use crate::receiver;
use crate::role::Role;
use crate::runtime::{RunControl, SessionControl};
use crate::sender;
use crate::telemetry;

// Основной orchestration живёт в библиотеке, чтобы его можно было вызывать из
// тестов напрямую без отдельного бинарника.
pub fn run(cli: Cli) -> Result<()> {
    telemetry::init_logging(cli.debug);
    run_controlled(cli, &RunControl::new(None))
}

pub fn main_entry() -> std::process::ExitCode {
    crate::launch::entry()
}

pub(crate) fn run_controlled(cli: Cli, control: &RunControl) -> Result<()> {
    validate(&cli)?;
    if let Command::Relay(args) = cli.command {
        return relay::serve(args, control);
    }
    let runtime = RealRuntime {
        control: control.clone(),
    };
    let policy = crate::retry::RetryPolicy::from_cli(&cli, false);
    // Listener уже изолирует ошибки сессий; его перезапуск оборвал бы соседние dump.
    if policy.retries == 0
        || matches!(&cli.command, Command::Receiver(args) if args.transport.listen.is_some())
    {
        return run_with(&runtime, cli);
    }
    crate::retry::run(policy, control, || run_with(&runtime, cli.clone()))
}

pub(crate) fn validate(cli: &Cli) -> Result<()> {
    anyhow::ensure!(
        cli.retry_delay_secs > 0,
        "retry_delay_secs must be at least 1"
    );
    match &cli.command {
        Command::Sender(args) => {
            args.resolve_dump_id()?;
            args.runtime_options()?;
            args.transport.validate()?;
        }
        Command::Receiver(args) => {
            args.validated_max_sessions()?;
            args.transport.validate()?;
        }
        Command::Relay(_) => {}
    }
    Ok(())
}

pub(crate) fn run_lobby_transfer<S: CloneableStream, R>(
    cli: Cli,
    role: Role,
    peer: &str,
    relayed: bool,
    run: &RunControl,
    establish: impl FnOnce(Option<&str>, &SessionControl) -> Result<(Session<S>, R)>,
) -> Result<()> {
    anyhow::ensure!(cli.command.role() == Some(role), "transfer role mismatch");
    let runtime = RealRuntime {
        control: run.clone(),
    };
    let dump_id = match &cli.command {
        Command::Sender(args) => Some(args.resolve_dump_id()?),
        _ => None,
    };
    let control = run.session(role, dump_id.as_deref().unwrap_or(""), peer);
    let result = (|| {
        let (session, _sockets) = establish(dump_id.as_deref(), &control)?;
        match cli.command {
            Command::Sender(args) => {
                let options = args.runtime_options()?;
                runtime.send_session(session, args, dump_id.unwrap(), options, &control, relayed)
            }
            Command::Receiver(args) => runtime.receive_session(session, args, &control, relayed),
            Command::Relay(_) => unreachable!("relay listener has no transfer role"),
        }
    })();
    control.finish(&result);
    result
}

trait Runtime {
    fn run_sender(
        &self,
        args: SenderArgs,
        transport_mode: Option<TransportMode>,
        dump_id: String,
        runtime: SenderRuntimeOptions,
    ) -> Result<()>;

    fn run_receiver_connect(
        &self,
        args: ReceiverArgs,
        transport_mode: Option<TransportMode>,
        max_sessions: usize,
    ) -> Result<()>;

    fn run_receiver_listen(
        &self,
        args: ReceiverArgs,
        addr: SocketAddr,
        max_sessions: usize,
    ) -> Result<()>;
}

fn run_with<R>(runtime: &R, cli: Cli) -> Result<()>
where
    R: Runtime,
{
    match cli.command {
        Command::Sender(args) => {
            let dump_id = args.resolve_dump_id()?;
            let runtime_options = args.runtime_options()?;
            let transport_mode = if args.transport.relay.is_some() {
                None
            } else {
                Some(args.transport.mode()?)
            };
            runtime.run_sender(args, transport_mode, dump_id, runtime_options)
        }
        Command::Receiver(args) => {
            let max_sessions = args.validated_max_sessions()?;
            if args.transport.relay.is_some() {
                return runtime.run_receiver_connect(args, None, max_sessions);
            }
            match args.transport.mode()? {
                connect @ TransportMode::Connect(_) => {
                    runtime.run_receiver_connect(args, Some(connect), max_sessions)
                }
                TransportMode::Listen(addr) => {
                    runtime.run_receiver_listen(args, addr, max_sessions)
                }
            }
        }
        Command::Relay(_) => unreachable!("relay service is dispatched before transfer retries"),
    }
}

struct RealRuntime {
    control: RunControl,
}

impl Runtime for RealRuntime {
    fn run_sender(
        &self,
        args: SenderArgs,
        transport_mode: Option<TransportMode>,
        dump_id: String,
        runtime: SenderRuntimeOptions,
    ) -> Result<()> {
        let destination = transport_label(transport_mode, &args.transport);
        debug!(
            "sender starting: mode={destination} dump_id={dump_id} source={} level={} threads={} bundle_target_bytes={} min_readers={} max_readers={} read_buffer_bytes={}",
            args.source.display(),
            runtime.level,
            runtime.threads,
            runtime.bundle_target_bytes,
            runtime.min_readers,
            runtime.max_readers,
            runtime.read_buffer_bytes
        );
        let control = self.control.session(Role::Sender, &dump_id, &destination);
        let result = (|| {
            if let Some(mode) = transport_mode {
                let (session, _sockets) = establish_session_controlled(
                    Role::Sender,
                    mode,
                    Some(&dump_id),
                    args.token.as_deref(),
                    &control,
                )?;
                self.send_session(session, args, dump_id, runtime, &control, false)
            } else {
                let (session, _sockets) = relay::establish(
                    &args.transport,
                    Role::Sender,
                    Some(&dump_id),
                    args.token.as_deref(),
                    &control,
                )?;
                self.send_session(session, args, dump_id, runtime, &control, true)
            }
        })();
        control.finish(&result);
        result
    }

    fn run_receiver_connect(
        &self,
        args: ReceiverArgs,
        transport_mode: Option<TransportMode>,
        _max_sessions: usize,
    ) -> Result<()> {
        let destination = transport_label(transport_mode, &args.transport);
        debug!(
            "receiver starting: mode={destination} data_dir={}",
            args.data_dir.display()
        );
        let control = self.control.session(Role::Receiver, "", &destination);
        let result = (|| {
            if let Some(mode) = transport_mode {
                let (session, _sockets) = establish_session_controlled(
                    Role::Receiver,
                    mode,
                    None,
                    args.token.as_deref(),
                    &control,
                )?;
                self.receive_session(session, args, &control, false)
            } else {
                let (session, _sockets) = relay::establish(
                    &args.transport,
                    Role::Receiver,
                    None,
                    args.token.as_deref(),
                    &control,
                )?;
                self.receive_session(session, args, &control, true)
            }
        })();
        control.finish(&result);
        result
    }

    fn run_receiver_listen(
        &self,
        args: ReceiverArgs,
        addr: SocketAddr,
        max_sessions: usize,
    ) -> Result<()> {
        debug!(
            "receiver starting in listen mode: addr={} data_dir={} max_sessions={}",
            addr,
            args.data_dir.display(),
            max_sessions
        );
        let listener = BoundListener::bind(addr)?;
        if self.control.progress().is_some() {
            self.control.log(
                log::Level::Info,
                &format!("Listening on {}", listener.local_addr()),
            );
        }
        receiver::serve_listener_controlled(
            listener,
            args.data_dir,
            max_sessions,
            args.token,
            &self.control,
        )
    }
}

impl RealRuntime {
    fn send_session<T: CloneableStream>(
        &self,
        mut session: Session<T>,
        args: SenderArgs,
        dump_id: String,
        runtime: SenderRuntimeOptions,
        control: &SessionControl,
        relayed: bool,
    ) -> Result<()> {
        let connection = record_connection(&session, &dump_id, control, relayed);
        let options = sender::BundlePlanOptions {
            bundle_target_bytes: runtime.bundle_target_bytes,
            compression_level: runtime.level,
            compression_threads: runtime.threads,
            // Явные границы имеют приоритет, затем планировщик учитывает число файлов.
            readers: sender::BundlePlanOptions::default()
                .readers
                .clamp(runtime.min_readers, runtime.max_readers),
        };
        let (report, profile) = sender::send_dump_controlled(
            &mut session.control_stream,
            &mut session.data_stream,
            args.source,
            &dump_id,
            options,
            control,
        )?;
        let status = if report.terminal_skip_count == 0 {
            "clean"
        } else {
            "completed_with_warnings"
        };
        self.control.summary(format!(
            "{connection}; dump_id={}; status={status}; files_total_discovered={}; files_sent={}; files_skipped={}; bundles_sent={}; final_bundle={}; final_work_ordinal={}",
            report.dump_id, profile.discovered_entries, profile.bundled_entries, report.terminal_skip_count,
            report.bundles_sent, report.final_bundle_no, report.final_work_ordinal,
        ));
        Ok(())
    }

    fn receive_session<T: CloneableStream>(
        &self,
        session: Session<T>,
        args: ReceiverArgs,
        control: &SessionControl,
        relayed: bool,
    ) -> Result<()> {
        let dump_id = session
            .peer_dump_id
            .as_deref()
            .expect("v4 receiver handshake supplies dump ID");
        let connection = record_connection(&session, dump_id, control, relayed);
        let (report, _) = receiver::receive_dump_profile_controlled(
            session.control_stream,
            session.data_stream,
            args.data_dir,
            dump_id,
            control,
        )?;
        self.control.summary(format!(
            "{connection}; dump_id={dump_id}; final_bundle={}; final_work_ordinal={}; terminal_skips={}; archive={}",
            report.resume_state.last_committed_bundle_no, report.resume_state.last_committed_work_ordinal,
            report.resume_state.terminal_skips.len(), report.final_archive_path.display(),
        ));
        Ok(())
    }
}

fn transport_label(mode: Option<TransportMode>, args: &crate::cli::TransportArgs) -> String {
    match mode {
        Some(mode) => format!("{mode:?}"),
        None => format!("relay {}", args.relay.as_deref().unwrap_or_default()),
    }
}

fn record_connection<T>(
    session: &Session<T>,
    dump_id: &str,
    control: &SessionControl,
    relayed: bool,
) -> String {
    let endpoint = if relayed {
        format!("via relay {}", session.peer_addr)
    } else {
        format!("at {}", session.peer_addr)
    };
    control.set_identity(
        dump_id,
        &if relayed {
            format!("relay {}", session.peer_addr)
        } else {
            session.peer_addr.to_string()
        },
    );
    if control.progress().is_some() {
        control.log(
            log::Level::Info,
            &format!("Connected to {} {endpoint}", session.peer_role),
        );
    }
    format!(
        "{} connected to {} {endpoint}",
        session.local_role, session.peer_role
    )
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::net::SocketAddr;
    use std::path::PathBuf;

    use anyhow::{Result, bail};
    use clap::Parser;

    use super::{Runtime, run_with};
    use crate::cli::{Cli, ReceiverArgs, SenderArgs, SenderRuntimeOptions};
    use crate::network::transport::TransportMode;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Call {
        Sender {
            transport_mode: TransportMode,
            dump_id: String,
            runtime: SenderRuntimeOptions,
            source: PathBuf,
            token: Option<String>,
        },
        ReceiverConnect {
            transport_mode: TransportMode,
            max_sessions: usize,
            data_dir: PathBuf,
            token: Option<String>,
        },
        ReceiverListen {
            addr: SocketAddr,
            max_sessions: usize,
            data_dir: PathBuf,
            token: Option<String>,
        },
    }

    #[derive(Default)]
    struct RecordingRuntime {
        calls: RefCell<Vec<Call>>,
        fail_receiver_listen: bool,
    }

    impl Runtime for RecordingRuntime {
        fn run_sender(
            &self,
            args: SenderArgs,
            transport_mode: Option<TransportMode>,
            dump_id: String,
            runtime: SenderRuntimeOptions,
        ) -> Result<()> {
            self.calls.borrow_mut().push(Call::Sender {
                transport_mode: transport_mode.expect("direct test transport"),
                dump_id,
                runtime,
                source: args.source,
                token: args.token,
            });
            Ok(())
        }

        fn run_receiver_connect(
            &self,
            args: ReceiverArgs,
            transport_mode: Option<TransportMode>,
            max_sessions: usize,
        ) -> Result<()> {
            self.calls.borrow_mut().push(Call::ReceiverConnect {
                transport_mode: transport_mode.expect("direct test transport"),
                max_sessions,
                data_dir: args.data_dir,
                token: args.token,
            });
            Ok(())
        }

        fn run_receiver_listen(
            &self,
            args: ReceiverArgs,
            addr: SocketAddr,
            max_sessions: usize,
        ) -> Result<()> {
            self.calls.borrow_mut().push(Call::ReceiverListen {
                addr,
                max_sessions,
                data_dir: args.data_dir,
                token: args.token,
            });
            if self.fail_receiver_listen {
                bail!("listener failed");
            }
            Ok(())
        }
    }

    #[test]
    fn run_dispatches_sender_command_with_validated_runtime() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7001",
            "--source",
            "input",
            "--dump-id",
            "dump-01",
            "--token",
            "secret",
            "--level",
            "7",
            "--threads",
            "1",
            "--frame-target-mb",
            "3",
        ])
        .unwrap();
        let runtime = RecordingRuntime::default();

        run_with(&runtime, cli).unwrap();

        assert_eq!(
            runtime.calls.into_inner(),
            vec![Call::Sender {
                transport_mode: TransportMode::Connect("127.0.0.1:7001".parse().unwrap()),
                dump_id: "dump-01".to_owned(),
                runtime: SenderRuntimeOptions {
                    level: 7,
                    threads: 1,
                    bundle_target_bytes: 3 * 1024 * 1024,
                    min_readers: 1,
                    max_readers: crate::sender::BundlePlanOptions::default().readers,
                    read_buffer_bytes: 1024 * 1024,
                },
                source: PathBuf::from("input"),
                token: Some("secret".to_owned()),
            }]
        );
    }

    #[test]
    fn run_dispatches_receiver_connect_command_after_validation() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "receiver",
            "--connect",
            "127.0.0.1:7002",
            "--data-dir",
            "state",
            "--token",
            "secret",
            "--max-sessions",
            "3",
        ])
        .unwrap();
        let runtime = RecordingRuntime::default();

        run_with(&runtime, cli).unwrap();

        assert_eq!(
            runtime.calls.into_inner(),
            vec![Call::ReceiverConnect {
                transport_mode: TransportMode::Connect("127.0.0.1:7002".parse().unwrap()),
                max_sessions: 3,
                data_dir: PathBuf::from("state"),
                token: Some("secret".to_owned()),
            }]
        );
    }

    #[test]
    fn run_dispatches_receiver_listen_command() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "receiver",
            "--listen",
            "127.0.0.1:7003",
            "--data-dir",
            "state",
            "--token",
            "secret",
            "--max-sessions",
            "2",
        ])
        .unwrap();
        let runtime = RecordingRuntime::default();

        run_with(&runtime, cli).unwrap();

        assert_eq!(
            runtime.calls.into_inner(),
            vec![Call::ReceiverListen {
                addr: "127.0.0.1:7003".parse().unwrap(),
                max_sessions: 2,
                data_dir: PathBuf::from("state"),
                token: Some("secret".to_owned()),
            }]
        );
    }

    #[test]
    fn run_validates_receiver_max_sessions_before_dispatch() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "receiver",
            "--connect",
            "127.0.0.1:7004",
            "--data-dir",
            "state",
            "--max-sessions",
            "0",
        ])
        .unwrap();
        let runtime = RecordingRuntime::default();

        let error = run_with(&runtime, cli).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("max_sessions must be at least 1")
        );
        assert!(runtime.calls.into_inner().is_empty());
    }

    #[test]
    fn run_propagates_receiver_listen_runtime_error() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "receiver",
            "--listen",
            "127.0.0.1:7005",
            "--data-dir",
            "state",
        ])
        .unwrap();
        let runtime = RecordingRuntime {
            fail_receiver_listen: true,
            ..Default::default()
        };

        let error = run_with(&runtime, cli).unwrap_err();

        assert!(error.to_string().contains("listener failed"));
        assert_eq!(runtime.calls.borrow().len(), 1);
    }
}

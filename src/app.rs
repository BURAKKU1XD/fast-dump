use std::net::SocketAddr;

use anyhow::Result;
use log::debug;

use crate::cli::{Cli, Command, ReceiverArgs, SenderArgs, SenderRuntimeOptions};
use crate::network::transport::{BoundListener, TransportMode, establish_session};
use crate::receiver::{receive_dump_checked, serve_listener};
use crate::role::Role;
use crate::sender::{SendOptions, send_dump};
use crate::telemetry;

// Основной orchestration-шаг живёт в библиотеке, чтобы его можно было
// тестировать напрямую без запуска отдельного бинарника и без настоящего
// долгоживущего listener-процесса.
pub fn run(cli: Cli) -> Result<()> {
    telemetry::init_logging(cli.debug);
    run_with(&RealRuntime, cli)
}

trait Runtime {
    fn run_sender(
        &self,
        args: SenderArgs,
        transport_mode: TransportMode,
        dump_id: String,
        runtime: SenderRuntimeOptions,
    ) -> Result<()>;

    fn run_receiver_connect(
        &self,
        args: ReceiverArgs,
        transport_mode: TransportMode,
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
            let transport_mode = args.transport.mode()?;
            runtime.run_sender(args, transport_mode, dump_id, runtime_options)
        }
        Command::Receiver(args) => {
            let transport_mode = args.transport.mode()?;
            let max_sessions = args.validated_max_sessions()?;

            match transport_mode {
                TransportMode::Connect(_) => {
                    runtime.run_receiver_connect(args, transport_mode, max_sessions)
                }
                TransportMode::Listen(addr) => {
                    runtime.run_receiver_listen(args, addr, max_sessions)
                }
            }
        }
    }
}

struct RealRuntime;

impl Runtime for RealRuntime {
    fn run_sender(
        &self,
        args: SenderArgs,
        transport_mode: TransportMode,
        dump_id: String,
        runtime: SenderRuntimeOptions,
    ) -> Result<()> {
        debug!(
            "sender starting: mode={transport_mode:?} dump_id={} source={} level={} threads={} min_readers={} max_readers={} read_buffer_bytes={} frame_target_bytes={}",
            dump_id,
            args.source.display(),
            runtime.level,
            runtime.threads,
            runtime.min_readers,
            runtime.max_readers,
            runtime.read_buffer_bytes,
            runtime.frame_target_bytes
        );
        let mut session = establish_session(
            Role::Sender,
            transport_mode,
            Some(&dump_id),
            args.token.as_deref(),
        )?;
        let options = SendOptions {
            level: runtime.level,
            threads: runtime.threads,
            frame_target_bytes: runtime.frame_target_bytes,
            min_readers: runtime.min_readers,
            max_readers: runtime.max_readers,
            read_buffer_bytes: runtime.read_buffer_bytes,
        };
        let report = send_dump(&mut session.stream, args.source, &dump_id, &options)?;

        println!(
            "{} connected to {} at {}; dump_id={}; resume frame={} file={} archive_len={}; status={}; files_total_discovered={}; files_sent_clean={}; files_sent_best_effort={}; files_skipped_open_failed={}; files_skipped_unstable_after_retries={}; retries_total={}; bytes_sent_raw={}; bytes_sent_best_effort_raw={}; frames_sent={}; final_archive_bytes={}; final frame={} file={}",
            session.local_role,
            session.peer_role,
            session.peer_addr,
            report.dump_id,
            report.resume_state.last_committed_frame,
            report.resume_state.last_committed_file_index,
            report.resume_state.archive_len,
            report.status,
            report.files_total_discovered,
            report.files_sent_clean,
            report.files_sent_best_effort,
            report.files_skipped_open_failed,
            report.files_skipped_unstable_after_retries,
            report.retries_total,
            report.bytes_sent_raw,
            report.bytes_sent_best_effort_raw,
            report.sent_frames,
            report.final_archive_bytes,
            report.final_frame_no,
            report.final_file_path
        );

        Ok(())
    }

    fn run_receiver_connect(
        &self,
        args: ReceiverArgs,
        transport_mode: TransportMode,
        _max_sessions: usize,
    ) -> Result<()> {
        debug!(
            "receiver starting in connect mode: mode={transport_mode:?} data_dir={}",
            args.data_dir.display()
        );
        let mut session =
            establish_session(Role::Receiver, transport_mode, None, args.token.as_deref())?;
        let report = receive_dump_checked(
            &mut session.stream,
            args.data_dir,
            session.peer_dump_id.as_deref(),
        )?;

        println!(
            "{} connected to {} at {}; dump_id={}; resume frame={} file={} archive_len={}; final frame={} file={} archive={}",
            session.local_role,
            session.peer_role,
            session.peer_addr,
            report.state.paths.dump_id(),
            report.resume_state.last_committed_frame,
            report.resume_state.last_committed_file_index,
            report.resume_state.archive_len,
            report.finish.final_frame_no,
            report.finish.final_file_path,
            report.final_archive_path.display()
        );

        Ok(())
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
        serve_listener(listener, args.data_dir, max_sessions, args.token)
    }
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
            transport_mode: TransportMode,
            dump_id: String,
            runtime: SenderRuntimeOptions,
        ) -> Result<()> {
            self.calls.borrow_mut().push(Call::Sender {
                transport_mode,
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
            transport_mode: TransportMode,
            max_sessions: usize,
        ) -> Result<()> {
            self.calls.borrow_mut().push(Call::ReceiverConnect {
                transport_mode,
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
            "--threads",
            "3",
            "--min-readers",
            "4",
            "--max-readers",
            "5",
            "--read-buffer-kb",
            "2",
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
                    level: 3,
                    threads: 3,
                    min_readers: 4,
                    max_readers: 5,
                    read_buffer_bytes: 2 * 1024,
                    frame_target_bytes: 3 * 1024 * 1024,
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

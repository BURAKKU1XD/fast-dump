use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{ArgGroup, Args, Parser, Subcommand, ValueHint};

use crate::network::transport::TransportMode;
use crate::role::Role;

const DEFAULT_LEVEL: i32 = 3;
const DEFAULT_FRAME_TARGET_MB: u64 = 64;
const DEFAULT_THREADS: &str = "auto";
const DEFAULT_MIN_READERS: usize = 2;
const DEFAULT_MAX_READERS: usize = 32;
const DEFAULT_READ_BUFFER_KB: u64 = 1024;
const DEFAULT_MAX_SESSIONS: usize = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SenderRuntimeOptions {
    pub level: i32,
    pub threads: u32,
    pub bundle_target_bytes: usize,
    pub min_readers: usize,
    pub max_readers: usize,
    pub read_buffer_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadCountArg {
    Auto,
    Explicit(u32),
}

// Корневой CLI parser задаёт только shape аргументов.
#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct Cli {
    #[arg(long, global = true)]
    pub debug: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Sender(SenderArgs),
    Receiver(ReceiverArgs),
}

impl Command {
    pub const fn role(&self) -> Role {
        match self {
            Self::Sender(_) => Role::Sender,
            Self::Receiver(_) => Role::Receiver,
        }
    }

    pub fn transport_mode(&self) -> Result<TransportMode> {
        match self {
            Self::Sender(args) => args.transport.mode(),
            Self::Receiver(args) => args.transport.mode(),
        }
    }
}

#[derive(Debug, Args)]
pub struct SenderArgs {
    #[command(flatten)]
    pub transport: TransportArgs,
    #[arg(long, value_hint = ValueHint::DirPath)]
    pub source: PathBuf,
    #[arg(long)]
    pub dump_id: Option<String>,
    #[arg(long)]
    pub token: Option<String>,
    #[arg(short = 'l', long, default_value_t = DEFAULT_LEVEL)]
    pub level: i32,
    #[arg(long, default_value = DEFAULT_THREADS, value_parser = parse_thread_count_arg)]
    pub threads: ThreadCountArg,
    #[arg(long, default_value_t = DEFAULT_MIN_READERS)]
    pub min_readers: usize,
    #[arg(long, default_value_t = DEFAULT_MAX_READERS)]
    pub max_readers: usize,
    #[arg(long, default_value_t = DEFAULT_READ_BUFFER_KB)]
    pub read_buffer_kb: u64,
    #[arg(long, default_value_t = DEFAULT_FRAME_TARGET_MB)]
    pub frame_target_mb: u64,
}

#[derive(Debug, Args)]
pub struct ReceiverArgs {
    #[command(flatten)]
    pub transport: TransportArgs,
    #[arg(long, value_hint = ValueHint::DirPath)]
    pub data_dir: PathBuf,
    #[arg(long)]
    pub token: Option<String>,
    #[arg(long, default_value_t = DEFAULT_MAX_SESSIONS)]
    pub max_sessions: usize,
}

#[derive(Debug, Clone, Args)]
#[command(group(
    ArgGroup::new("transport")
        .required(true)
        .args(["listen", "connect"])
))]
pub struct TransportArgs {
    #[arg(long)]
    pub listen: Option<SocketAddr>,
    #[arg(long)]
    pub connect: Option<SocketAddr>,
}

impl TransportArgs {
    pub fn mode(&self) -> Result<TransportMode> {
        match (self.listen, self.connect) {
            (Some(addr), None) => Ok(TransportMode::Listen(addr)),
            (None, Some(addr)) => Ok(TransportMode::Connect(addr)),
            _ => bail!("exactly one of --listen or --connect must be provided"),
        }
    }
}

impl SenderArgs {
    // dump_id нужен уже в transport bootstrap, поэтому безопасный fallback
    // берём из имени source-каталога.
    pub fn resolve_dump_id(&self) -> Result<String> {
        if let Some(dump_id) = &self.dump_id {
            crate::network::protocol::validate_dump_id(dump_id)?;
            return Ok(dump_id.clone());
        }

        match self.source.file_name().and_then(|value| value.to_str()) {
            Some(value) if !value.is_empty() && !matches!(value, "." | "..") => {
                crate::network::protocol::validate_dump_id(value)?;
                Ok(value.to_owned())
            }
            _ => bail!(
                "--dump-id is required when --source does not end with a normal path component"
            ),
        }
    }

    pub fn runtime_options(&self) -> Result<SenderRuntimeOptions> {
        if self.frame_target_mb == 0 {
            bail!("frame_target_mb must be at least 1");
        }
        let threads = match self.threads {
            ThreadCountArg::Auto => std::thread::available_parallelism()
                .map(|parallelism| parallelism.get().saturating_sub(1).max(1) as u32)
                .unwrap_or(1),
            ThreadCountArg::Explicit(0) => bail!("threads must be at least 1"),
            ThreadCountArg::Explicit(value) => value,
        };
        if self.min_readers == 0 {
            bail!("min_readers must be at least 1");
        }
        if self.max_readers == 0 {
            bail!("max_readers must be at least 1");
        }
        if self.min_readers > self.max_readers {
            bail!(
                "min_readers must not be greater than max_readers: {} > {}",
                self.min_readers,
                self.max_readers
            );
        }
        if self.read_buffer_kb == 0 {
            bail!("read_buffer_kb must be at least 1");
        }

        let bundle_target_bytes = self
            .frame_target_mb
            .checked_mul(1024 * 1024)
            .context("frame_target_mb is too large")?;
        let bundle_target_bytes = usize::try_from(bundle_target_bytes)
            .context("frame_target_mb is too large for this platform")?;
        let read_buffer_bytes = self
            .read_buffer_kb
            .checked_mul(1024)
            .context("read_buffer_kb is too large")?;
        let read_buffer_bytes = usize::try_from(read_buffer_bytes)
            .context("read_buffer_kb is too large for this platform")?;

        Ok(SenderRuntimeOptions {
            level: self.level,
            threads,
            bundle_target_bytes,
            min_readers: self.min_readers,
            max_readers: self.max_readers,
            read_buffer_bytes,
        })
    }
}

impl ReceiverArgs {
    pub fn validated_max_sessions(&self) -> Result<usize> {
        if self.max_sessions == 0 {
            bail!("max_sessions must be at least 1");
        }

        Ok(self.max_sessions)
    }
}

fn parse_thread_count_arg(raw: &str) -> std::result::Result<ThreadCountArg, String> {
    if raw.eq_ignore_ascii_case("auto") {
        return Ok(ThreadCountArg::Auto);
    }

    let value = raw.parse::<u32>().map_err(|_| {
        format!("invalid thread count `{raw}`; expected `auto` or a positive integer")
    })?;
    Ok(ThreadCountArg::Explicit(value))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::{Parser, error::ErrorKind};

    use super::{Cli, Command, ThreadCountArg, TransportArgs};
    use crate::network::transport::TransportMode;
    use crate::role::Role;

    #[test]
    fn sender_cli_parses_connect_mode() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7001",
            "--source",
            "input",
        ])
        .unwrap();

        assert_eq!(cli.command.role(), Role::Sender);
        assert_eq!(
            cli.command.transport_mode().unwrap(),
            TransportMode::Connect("127.0.0.1:7001".parse().unwrap())
        );

        let Command::Sender(args) = cli.command else {
            panic!("expected sender command");
        };
        assert_eq!(args.source, PathBuf::from("input"));
        assert_eq!(args.resolve_dump_id().unwrap(), "input");
        assert_eq!(args.threads, ThreadCountArg::Auto);
        assert_eq!(args.min_readers, 2);
        assert_eq!(args.max_readers, 32);
        assert_eq!(args.read_buffer_kb, 1024);

        let runtime = args.runtime_options().unwrap();
        let expected_threads = std::thread::available_parallelism()
            .map(|parallelism| parallelism.get().saturating_sub(1).max(1) as u32)
            .unwrap_or(1);
        assert_eq!(runtime.threads, expected_threads);
        assert_eq!(runtime.bundle_target_bytes, 64 * 1024 * 1024);
        assert_eq!(runtime.min_readers, 2);
        assert_eq!(runtime.max_readers, 32);
        assert_eq!(runtime.read_buffer_bytes, 1024 * 1024);
    }

    #[test]
    fn sender_cli_parses_explicit_dump_id() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7009",
            "--source",
            "input",
            "--dump-id",
            "dump-01",
        ])
        .unwrap();

        let Command::Sender(args) = cli.command else {
            panic!("expected sender command");
        };
        assert_eq!(args.resolve_dump_id().unwrap(), "dump-01");
    }

    #[test]
    fn sender_rejects_unsafe_explicit_and_derived_dump_ids() {
        for dump_id in ["../outside", "dump:stream", "CON", ""] {
            let cli = Cli::try_parse_from([
                "fast-dump",
                "sender",
                "--connect",
                "127.0.0.1:7009",
                "--source",
                "input",
                "--dump-id",
                dump_id,
            ])
            .unwrap();
            let Command::Sender(args) = cli.command else {
                panic!()
            };
            assert!(args.resolve_dump_id().is_err());
        }
        let cli = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7009",
            "--source",
            "NUL",
        ])
        .unwrap();
        let Command::Sender(args) = cli.command else {
            panic!()
        };
        assert!(args.resolve_dump_id().is_err());
    }

    #[test]
    fn sender_dump_id_requires_override_for_non_normal_source_path() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7010",
            "--source",
            ".",
        ])
        .unwrap();

        let Command::Sender(args) = cli.command else {
            panic!("expected sender command");
        };

        let error = args.resolve_dump_id().unwrap_err();
        assert!(error.to_string().contains(
            "--dump-id is required when --source does not end with a normal path component"
        ));
    }

    #[test]
    fn sender_cli_accepts_explicit_runtime_overrides() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7012",
            "--source",
            "input",
            "--level",
            "7",
            "--threads",
            "5",
            "--min-readers",
            "3",
            "--max-readers",
            "9",
            "--read-buffer-kb",
            "64",
            "--frame-target-mb",
            "5",
        ])
        .unwrap();

        let Command::Sender(args) = cli.command else {
            panic!("expected sender command");
        };
        let runtime = args.runtime_options().unwrap();

        assert_eq!(runtime.level, 7);
        assert_eq!(runtime.threads, 5);
        assert_eq!(runtime.bundle_target_bytes, 5 * 1024 * 1024);
        assert_eq!(runtime.min_readers, 3);
        assert_eq!(runtime.max_readers, 9);
        assert_eq!(runtime.read_buffer_bytes, 64 * 1024);
    }

    #[test]
    fn sender_cli_rejects_zero_frame_target_in_runtime_validation() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7013",
            "--source",
            "input",
            "--frame-target-mb",
            "0",
        ])
        .unwrap();

        let Command::Sender(args) = cli.command else {
            panic!("expected sender command");
        };
        let error = args.runtime_options().unwrap_err();
        assert!(error.to_string().contains("frame_target_mb"));
    }

    #[test]
    fn sender_cli_accepts_explicit_auto_threads() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7014",
            "--source",
            "input",
            "--threads",
            "auto",
        ])
        .unwrap();

        let Command::Sender(args) = cli.command else {
            panic!("expected sender command");
        };
        assert_eq!(args.threads, ThreadCountArg::Auto);
        assert!(args.runtime_options().unwrap().threads >= 1);
    }

    #[test]
    fn sender_cli_rejects_zero_threads_in_runtime_validation() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7017",
            "--source",
            "input",
            "--threads",
            "0",
        ])
        .unwrap();

        let Command::Sender(args) = cli.command else {
            panic!("expected sender command");
        };
        let error = args.runtime_options().unwrap_err();
        assert!(error.to_string().contains("threads"));
    }

    #[test]
    fn sender_cli_rejects_zero_readers_and_inverted_reader_bounds() {
        let zero_min = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7018",
            "--source",
            "input",
            "--min-readers",
            "0",
        ])
        .unwrap();
        let Command::Sender(args) = zero_min.command else {
            panic!("expected sender command");
        };
        let error = args.runtime_options().unwrap_err();
        assert!(error.to_string().contains("min_readers"));

        let zero_max = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7019",
            "--source",
            "input",
            "--max-readers",
            "0",
        ])
        .unwrap();
        let Command::Sender(args) = zero_max.command else {
            panic!("expected sender command");
        };
        let error = args.runtime_options().unwrap_err();
        assert!(error.to_string().contains("max_readers"));

        let inverted = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7020",
            "--source",
            "input",
            "--min-readers",
            "5",
            "--max-readers",
            "4",
        ])
        .unwrap();
        let Command::Sender(args) = inverted.command else {
            panic!("expected sender command");
        };
        let error = args.runtime_options().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("min_readers must not be greater")
        );
    }

    #[test]
    fn sender_cli_rejects_zero_read_buffer_in_runtime_validation() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7021",
            "--source",
            "input",
            "--read-buffer-kb",
            "0",
        ])
        .unwrap();

        let Command::Sender(args) = cli.command else {
            panic!("expected sender command");
        };
        let error = args.runtime_options().unwrap_err();
        assert!(error.to_string().contains("read_buffer_kb"));
    }

    #[test]
    fn receiver_cli_parses_listen_mode() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "receiver",
            "--listen",
            "127.0.0.1:7002",
            "--data-dir",
            "state",
        ])
        .unwrap();

        assert_eq!(cli.command.role(), Role::Receiver);
        assert_eq!(
            cli.command.transport_mode().unwrap(),
            TransportMode::Listen("127.0.0.1:7002".parse().unwrap())
        );

        let Command::Receiver(args) = cli.command else {
            panic!("expected receiver command");
        };
        assert_eq!(args.data_dir, PathBuf::from("state"));
    }

    #[test]
    fn receiver_cli_uses_contract_defaults() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "receiver",
            "--listen",
            "127.0.0.1:7015",
            "--data-dir",
            "state",
        ])
        .unwrap();

        let Command::Receiver(args) = cli.command else {
            panic!("expected receiver command");
        };
        assert_eq!(args.max_sessions, 1);
        assert_eq!(args.token, None);
    }

    #[test]
    fn receiver_cli_rejects_zero_max_sessions_in_runtime_validation() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "receiver",
            "--listen",
            "127.0.0.1:7016",
            "--data-dir",
            "state",
            "--max-sessions",
            "0",
        ])
        .unwrap();

        let Command::Receiver(args) = cli.command else {
            panic!("expected receiver command");
        };
        let error = args.validated_max_sessions().unwrap_err();
        assert!(error.to_string().contains("max_sessions"));
    }

    #[test]
    fn sender_cli_parses_listen_mode_for_pull() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--listen",
            "127.0.0.1:7007",
            "--source",
            "input",
        ])
        .unwrap();

        assert_eq!(cli.command.role(), Role::Sender);
        assert_eq!(
            cli.command.transport_mode().unwrap(),
            TransportMode::Listen("127.0.0.1:7007".parse().unwrap())
        );
    }

    #[test]
    fn receiver_cli_parses_connect_mode_for_pull() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "receiver",
            "--connect",
            "127.0.0.1:7008",
            "--data-dir",
            "state",
        ])
        .unwrap();

        assert_eq!(cli.command.role(), Role::Receiver);
        assert_eq!(
            cli.command.transport_mode().unwrap(),
            TransportMode::Connect("127.0.0.1:7008".parse().unwrap())
        );
    }

    #[test]
    fn clap_rejects_missing_transport_selector() {
        let error = Cli::try_parse_from(["fast-dump", "sender", "--source", "input"]).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn clap_rejects_both_transport_selectors() {
        let error = Cli::try_parse_from([
            "fast-dump",
            "receiver",
            "--listen",
            "127.0.0.1:7003",
            "--connect",
            "127.0.0.1:7004",
            "--data-dir",
            "state",
        ])
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::ArgumentConflict);
    }

    #[test]
    fn transport_args_mode_rejects_invalid_manual_state() {
        let error = TransportArgs {
            listen: Some("127.0.0.1:7005".parse().unwrap()),
            connect: Some("127.0.0.1:7006".parse().unwrap()),
        }
        .mode()
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("exactly one of --listen or --connect")
        );

        let error = TransportArgs {
            listen: None,
            connect: None,
        }
        .mode()
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("exactly one of --listen or --connect")
        );
    }

    #[test]
    fn global_debug_flag_is_accepted_before_subcommand() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "--debug",
            "sender",
            "--connect",
            "127.0.0.1:7009",
            "--source",
            "input",
        ])
        .unwrap();

        assert!(cli.debug);
        assert!(matches!(cli.command, Command::Sender(_)));
    }
}

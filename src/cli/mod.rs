use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{ArgGroup, Args, Parser, Subcommand, ValueHint};

use crate::network::transport::TransportMode;
use crate::role::Role;

pub(crate) const DEFAULT_LEVEL: i32 = 3;
const DEFAULT_FRAME_TARGET_MB: u64 = 64;
const DEFAULT_THREADS: &str = "auto";
const DEFAULT_MIN_READERS: usize = 1;
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
#[derive(Debug, Clone, Parser)]
#[command(
    author,
    version,
    about = "Stream a resumable directory dump to a remote .tar.zst archive",
    long_about = "Stream a directory to a remote host using resumable, durable bundle boundaries.\nRun without a command in a terminal to open the setup wizard.\nSender and receiver are named by data flow, independently of who connects.",
    after_help = "Examples:\n  fast-dump\n  fast-dump receiver --listen 0.0.0.0:7001 --data-dir ./dumps\n  fast-dump sender --connect 192.168.1.20:7001 --source ./data\n  fast-dump --no-gui sender --connect 192.168.1.20:7001 --source ./data\n\nAfter interruption, repeat the same commands to resume durable progress.\nUse 'help sender' or 'help receiver' for role-specific options."
)]
pub struct Cli {
    #[arg(
        long,
        global = true,
        help = "Enable diagnostic logging (shown in the dashboard Log tab)"
    )]
    pub debug: bool,

    #[arg(
        long,
        global = true,
        help = "Disable the terminal dashboard and wizard; keep plain output"
    )]
    pub no_gui: bool,

    #[arg(
        long,
        global = true,
        value_name = "N",
        help = "Extra attempts after network errors; default: 10 in GUI, 0 in plain mode (0 disables)"
    )]
    pub retries: Option<u32>,

    #[arg(
        long,
        global = true,
        default_value_t = 10,
        value_name = "SECONDS",
        value_parser = clap::value_parser!(u32).range(1..),
        help = "Delay between network retry attempts"
    )]
    pub retry_delay_secs: u32,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    #[command(
        about = "Read a source directory, compress it, and send its contents",
        after_help = "Push: sender --connect <IP:PORT> --source <DIR>\nPull: sender --listen <IP:PORT> --source <DIR>\nPair with a receiver using the opposite transport selector.\nRepeat the same source and dump ID to resume; completed archives are preserved."
    )]
    Sender(SenderArgs),
    #[command(
        about = "Receive a dump and save a durable, resumable .tar.zst archive",
        after_help = "Push: receiver --listen <IP:PORT> --data-dir <DIR>\nPull: receiver --connect <IP:PORT> --data-dir <DIR>\nListen mode keeps accepting sessions until stopped.\nKeep partial archive, WAL, catalog, and lock files together to resume."
    )]
    Receiver(ReceiverArgs),
    #[command(about = "Forward paired dump sessions over TLS")]
    Relay(RelayArgs),
}

impl Command {
    pub const fn role(&self) -> Option<Role> {
        match self {
            Self::Sender(_) => Some(Role::Sender),
            Self::Receiver(_) => Some(Role::Receiver),
            Self::Relay(_) => None,
        }
    }

    pub fn transport_mode(&self) -> Result<TransportMode> {
        match self {
            Self::Sender(args) => args.transport.mode(),
            Self::Receiver(args) => args.transport.mode(),
            Self::Relay(_) => bail!("relay service has no peer transport selector"),
        }
    }
}

#[derive(Debug, Clone, Args)]
#[command(group(ArgGroup::new("relay_tls").args(["config", "certificate"])))]
pub struct RelayArgs {
    #[arg(
        long,
        value_name = "IP:PORT",
        help = "Listen for outbound TLS connections from both peers"
    )]
    pub listen: SocketAddr,
    #[arg(long, value_hint = ValueHint::FilePath, required_unless_present = "access", help = "TOML with TLS certificate paths and relay rooms")]
    pub config: Option<PathBuf>,
    #[arg(long, value_hint = ValueHint::FilePath, requires_all = ["access", "private_key"], help = "TLS certificate chain PEM for access-token relay")]
    pub certificate: Option<PathBuf>,
    #[arg(long, value_hint = ValueHint::FilePath, requires = "certificate", help = "TLS private key PEM for access-token relay")]
    pub private_key: Option<PathBuf>,
    #[arg(
        long,
        help = "Generate an access token and allow up to 15 connected clients"
    )]
    pub access: bool,
}

#[derive(Debug, Clone, Args)]
pub struct SenderArgs {
    #[command(flatten)]
    pub transport: TransportArgs,
    #[arg(long, value_hint = ValueHint::DirPath, help = "Source directory; files and readable symlinks are included")]
    pub source: PathBuf,
    #[arg(
        long,
        help = "Portable archive name; defaults to the final source path component"
    )]
    pub dump_id: Option<String>,
    #[arg(long, help = "Shared handshake token; must match the other peer")]
    pub token: Option<String>,
    #[arg(short = 'l', long, default_value_t = DEFAULT_LEVEL, help = "Zstd compression level")]
    pub level: i32,
    #[arg(long, default_value = DEFAULT_THREADS, value_parser = parse_thread_count_arg, help = "Zstd workers; auto uses logical parallelism minus one, minimum 1")]
    pub threads: ThreadCountArg,
    #[arg(long, default_value_t = DEFAULT_MIN_READERS, help = "Minimum requested file readers; small pending sets cap actual workers")]
    pub min_readers: usize,
    #[arg(long, default_value_t = crate::cpu::available_physical_cores(), help = "Maximum requested file readers; defaults to detected physical cores")]
    pub max_readers: usize,
    #[arg(long, default_value_t = DEFAULT_READ_BUFFER_KB, help = "Legacy read buffer in KiB; validated but does not tune the v4 data path")]
    pub read_buffer_kb: u64,
    #[arg(long, default_value_t = DEFAULT_FRAME_TARGET_MB, help = "Bundle target and large-file streaming threshold in MiB")]
    pub frame_target_mb: u64,
}

#[derive(Debug, Clone, Args)]
pub struct ReceiverArgs {
    #[command(flatten)]
    pub transport: TransportArgs,
    #[arg(long, value_hint = ValueHint::DirPath, help = "Directory for archives and durable resume state")]
    pub data_dir: PathBuf,
    #[arg(long, help = "Shared handshake token; must match the other peer")]
    pub token: Option<String>,
    #[arg(long, default_value_t = DEFAULT_MAX_SESSIONS, help = "Maximum concurrent receiver sessions in listen mode")]
    pub max_sessions: usize,
}

#[derive(Debug, Clone, Default, Args)]
#[command(group(
    ArgGroup::new("transport")
        .required(true)
        .args(["listen", "connect", "relay"])
))]
pub struct TransportArgs {
    #[arg(
        long,
        value_name = "IP:PORT",
        help = "Listen for the other peer (IPv6 uses [address]:port)"
    )]
    pub listen: Option<SocketAddr>,
    #[arg(
        long,
        value_name = "IP:PORT",
        help = "Connect to the other peer (IPv6 uses [address]:port)"
    )]
    pub connect: Option<SocketAddr>,
    #[arg(long, value_name = "HOST:PORT", requires_all = ["room", "relay_token_file"], help = "Connect through a TLS relay")]
    pub relay: Option<String>,
    #[arg(long, requires = "relay", help = "Room configured at the relay")]
    pub room: Option<String>,
    #[arg(long, requires = "relay", value_hint = ValueHint::FilePath, help = "File containing the relay key for this role")]
    pub relay_token_file: Option<PathBuf>,
    #[arg(long, requires = "relay", value_hint = ValueHint::FilePath, help = "PEM trust anchors for a privately issued relay certificate")]
    pub relay_ca_file: Option<PathBuf>,
}

impl TransportArgs {
    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(relay) = &self.relay {
            anyhow::ensure!(
                self.listen.is_none() && self.connect.is_none(),
                "exactly one transport selector is required"
            );
            crate::network::relay::validate_endpoint(relay)?;
            crate::network::relay::validate_room(
                self.room
                    .as_deref()
                    .context("--room is required with --relay")?,
            )?;
            anyhow::ensure!(
                self.relay_token_file.is_some(),
                "--relay-token-file is required with --relay"
            );
        } else {
            self.mode()?;
            anyhow::ensure!(
                self.room.is_none()
                    && self.relay_token_file.is_none()
                    && self.relay_ca_file.is_none(),
                "relay options require --relay"
            );
        }
        Ok(())
    }

    pub fn mode(&self) -> Result<TransportMode> {
        match (self.listen, self.connect, &self.relay) {
            (Some(addr), None, None) => Ok(TransportMode::Listen(addr)),
            (None, Some(addr), None) => Ok(TransportMode::Connect(addr)),
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
    fn global_retry_flags_parse_on_either_side_of_command() {
        for before in [true, false] {
            let mut argv = vec!["fast-dump"];
            let flags = ["--retries", "0", "--retry-delay-secs", "7"];
            if before {
                argv.extend(flags);
            }
            argv.extend(["sender", "--connect", "127.0.0.1:7001", "--source", "input"]);
            if !before {
                argv.extend(flags);
            }
            let cli = Cli::try_parse_from(argv).unwrap();
            assert_eq!(cli.retries, Some(0));
            assert_eq!(cli.retry_delay_secs, 7);
        }
        let cli = Cli::try_parse_from([
            "fast-dump",
            "receiver",
            "--listen",
            "0.0.0.0:7001",
            "--data-dir",
            "state",
        ])
        .unwrap();
        assert_eq!(cli.retries, None);
        assert_eq!(cli.retry_delay_secs, 10);
    }

    #[test]
    fn retry_flags_reject_invalid_values() {
        for (flag, value) in [
            ("--retries", "-1"),
            ("--retries", "4294967296"),
            ("--retry-delay-secs", "0"),
            ("--retry-delay-secs", "-1"),
            ("--retry-delay-secs", "invalid"),
        ] {
            assert!(
                Cli::try_parse_from([
                    "fast-dump",
                    "receiver",
                    "--connect",
                    "127.0.0.1:7001",
                    "--data-dir",
                    "state",
                    flag,
                    value
                ])
                .is_err()
            );
        }
    }

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

        assert_eq!(cli.command.role(), Some(Role::Sender));
        assert_eq!(
            cli.command.transport_mode().unwrap(),
            TransportMode::Connect("127.0.0.1:7001".parse().unwrap())
        );

        let Command::Sender(args) = cli.command else {
            panic!("expected sender command");
        };
        assert_eq!(args.source, PathBuf::from("input"));
        assert_eq!(args.resolve_dump_id().unwrap(), "input");
        assert_eq!(args.level, 3);
        assert_eq!(args.threads, ThreadCountArg::Auto);
        assert_eq!(args.min_readers, 1);
        let default_readers = crate::sender::BundlePlanOptions::default().readers;
        assert_eq!(args.max_readers, default_readers);
        assert_eq!(args.read_buffer_kb, 1024);

        let runtime = args.runtime_options().unwrap();
        let expected_threads = std::thread::available_parallelism()
            .map(|parallelism| parallelism.get().saturating_sub(1).max(1) as u32)
            .unwrap_or(1);
        assert_eq!(runtime.threads, expected_threads);
        assert_eq!(runtime.level, 3);
        assert_eq!(runtime.bundle_target_bytes, 64 * 1024 * 1024);
        assert_eq!(runtime.min_readers, 1);
        assert_eq!(runtime.max_readers, default_readers);
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

        assert_eq!(cli.command.role(), Some(Role::Receiver));
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

        assert_eq!(cli.command.role(), Some(Role::Sender));
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

        assert_eq!(cli.command.role(), Some(Role::Receiver));
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
    fn relay_cli_requires_a_room_and_role_key_and_is_exclusive() {
        let args = [
            "fast-dump",
            "sender",
            "--source",
            ".",
            "--relay",
            "relay.example:7443",
        ];
        assert_eq!(
            Cli::try_parse_from(args).unwrap_err().kind(),
            ErrorKind::MissingRequiredArgument
        );
        let complete =
            args.into_iter()
                .chain(["--room", "backup", "--relay-token-file", "sender.key"]);
        let cli = Cli::try_parse_from(complete.clone()).unwrap();
        let Command::Sender(sender) = cli.command else {
            panic!("expected sender")
        };
        sender.transport.validate().unwrap();
        assert_eq!(
            Cli::try_parse_from(complete.chain(["--connect", "127.0.0.1:7443"]))
                .unwrap_err()
                .kind(),
            ErrorKind::ArgumentConflict
        );
        let cli = Cli::try_parse_from([
            "fast-dump",
            "relay",
            "--listen",
            "127.0.0.1:7443",
            "--config",
            "relay.toml",
        ])
        .unwrap();
        assert!(matches!(cli.command, Command::Relay(_)));
    }

    #[test]
    fn access_relay_accepts_pem_paths_instead_of_toml() {
        let base = ["fast-dump", "relay", "--listen=0.0.0.0:9876"];
        assert!(Cli::try_parse_from(base).is_err());
        let automatic = Cli::try_parse_from(base.into_iter().chain(["--access"])).unwrap();
        let Command::Relay(automatic) = automatic.command else {
            panic!("expected relay");
        };
        assert!(automatic.access && automatic.config.is_none());
        assert!(automatic.certificate.is_none() && automatic.private_key.is_none());
        let paths = ["--certificate=fullchain.pem", "--private-key=privkey.pem"];
        let cli = Cli::try_parse_from(base.into_iter().chain(["--access"]).chain(paths)).unwrap();
        let Command::Relay(args) = cli.command else {
            panic!("expected relay");
        };
        assert!(args.access && args.config.is_none());
        assert_eq!(args.certificate, Some(PathBuf::from("fullchain.pem")));
        assert_eq!(args.private_key, Some(PathBuf::from("privkey.pem")));
        assert_eq!(
            Cli::try_parse_from(base.into_iter().chain(paths))
                .unwrap_err()
                .kind(),
            ErrorKind::MissingRequiredArgument
        );
        assert_eq!(
            Cli::try_parse_from(
                base.into_iter()
                    .chain(["--access", "--certificate=fullchain.pem"])
            )
            .unwrap_err()
            .kind(),
            ErrorKind::MissingRequiredArgument
        );
        assert_eq!(
            Cli::try_parse_from(
                base.into_iter()
                    .chain(["--access", "--config=relay.toml"])
                    .chain(paths)
            )
            .unwrap_err()
            .kind(),
            ErrorKind::ArgumentConflict
        );
    }

    #[test]
    fn transport_args_mode_rejects_invalid_manual_state() {
        let error = TransportArgs {
            listen: Some("127.0.0.1:7005".parse().unwrap()),
            connect: Some("127.0.0.1:7006".parse().unwrap()),
            ..TransportArgs::default()
        }
        .mode()
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("exactly one of --listen or --connect")
        );

        let error = TransportArgs::default().mode().unwrap_err();

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

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{ArgGroup, Args, Parser, Subcommand, ValueHint};

use crate::network::{protocol, transport::TransportMode};
use crate::role::Role;
use crate::sender::SenderSource;

const DEFAULT_LEVEL: i32 = 3;
const DEFAULT_MIN_READERS: usize = 2;
const DEFAULT_MAX_READERS: usize = 8;
const DEFAULT_READ_BUFFER_KB: usize = 1024;
const DEFAULT_FRAME_TARGET_MB: u64 = 64;
const DEFAULT_MANIFEST_CHUNK_KB: usize = 512;
const DEFAULT_MANIFEST_ACK_RECORDS: u64 = 4_096;
const DEFAULT_MANIFEST_ACK_KB: usize = 8192;
const DEFAULT_MAX_SESSIONS: usize = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SenderRuntimeOptions {
    pub level: i32,
    pub threads: u32,
    pub min_readers: usize,
    pub max_readers: usize,
    pub read_buffer_bytes: usize,
    pub frame_target_bytes: u64,
    pub manifest_chunk_bytes: usize,
    pub manifest_ack_records: u64,
    pub manifest_ack_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverRuntimeOptions {
    pub manifest_ack_records: u64,
    pub manifest_ack_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadCountArg {
    Auto,
    Fixed(u32),
}

impl ThreadCountArg {
    pub fn resolve(&self) -> u32 {
        match self {
            Self::Auto => std::thread::available_parallelism()
                .map(|count| count.get().saturating_sub(1).max(1) as u32)
                .unwrap_or(1),
            Self::Fixed(value) => *value,
        }
    }
}

fn parse_thread_count_arg(value: &str) -> std::result::Result<ThreadCountArg, String> {
    if value.eq_ignore_ascii_case("auto") {
        return Ok(ThreadCountArg::Auto);
    }

    value
        .parse::<u32>()
        .map(ThreadCountArg::Fixed)
        .map_err(|_| "threads must be a positive integer or 'auto'".to_owned())
}

// Корневой CLI-парсер.
// Clap строит его по derive-макросам и атрибутам ниже.
// Здесь мы держим именно shape входных аргументов, а не runtime-логику.
#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct Cli {
    #[arg(long, global = true)]
    pub debug: bool,

    #[command(subcommand)]
    pub command: Command,
}

// Два главных режима приложения: sender и receiver.
#[derive(Debug, Subcommand)]
pub enum Command {
    Sender(SenderArgs),
    Receiver(ReceiverArgs),
}

impl Command {
    // Сразу преобразуем CLI-команду в логическую роль.
    // Это позволяет остальному коду работать уже не с конкретным subcommand,
    // а с общей моделью sender/receiver.
    pub const fn role(&self) -> Role {
        match self {
            Self::Sender(_) => Role::Sender,
            Self::Receiver(_) => Role::Receiver,
        }
    }

    // Здесь CLI-аргументы переходят в runtime-представление транспорта.
    // После этого transport-слою не нужно знать ничего про clap.
    pub fn transport_mode(&self) -> Result<TransportMode> {
        match self {
            Self::Sender(args) => args.transport.mode(),
            Self::Receiver(args) => args.transport.mode(),
        }
    }
}

// Параметры sender.
// Помимо source и dump_id здесь уже лежат contract-level настройки компрессии
// и reader bounds, чтобы runtime не тащил захардкоженные defaults.
#[derive(Debug, Args)]
pub struct SenderArgs {
    // Оба subcommand используют одну и ту же transport-валидацию,
    // поэтому флаги вынесены в общий вложенный тип.
    #[command(flatten)]
    pub transport: TransportArgs,
    // Это корень, относительно которого позже будет строиться tar-поток.
    #[arg(long, value_hint = ValueHint::DirPath)]
    pub source: PathBuf,
    // Явный dump_id удобен для предсказуемого resume между разными запусками.
    // Если его не передали, временно используем имя каталога source.
    #[arg(long)]
    pub dump_id: Option<String>,
    #[arg(long)]
    pub token: Option<String>,
    #[arg(short = 'l', long, default_value_t = DEFAULT_LEVEL)]
    pub level: i32,
    #[arg(
        short = 't',
        long,
        value_parser = parse_thread_count_arg
    )]
    pub threads: Option<ThreadCountArg>,
    #[arg(long, default_value_t = DEFAULT_MIN_READERS)]
    pub min_readers: usize,
    #[arg(long, default_value_t = DEFAULT_MAX_READERS)]
    pub max_readers: usize,
    #[arg(long)]
    pub read_buffer_kb: Option<usize>,
    #[arg(long)]
    pub frame_target_mb: Option<u64>,
    #[arg(long, default_value_t = DEFAULT_MANIFEST_CHUNK_KB)]
    pub manifest_chunk_kb: usize,
    #[arg(long, default_value_t = DEFAULT_MANIFEST_ACK_RECORDS)]
    pub manifest_ack_records: u64,
    #[arg(long, default_value_t = DEFAULT_MANIFEST_ACK_KB)]
    pub manifest_ack_kb: usize,
}

// Параметры receiver.
#[derive(Debug, Args)]
pub struct ReceiverArgs {
    // Повторно используем ту же transport-модель, что и у sender.
    #[command(flatten)]
    pub transport: TransportArgs,
    // В этом каталоге receiver будет хранить `.part`-архив и журнал возобновления.
    #[arg(long, value_hint = ValueHint::DirPath)]
    pub data_dir: PathBuf,
    #[arg(long)]
    pub token: Option<String>,
    #[arg(long, default_value_t = DEFAULT_MAX_SESSIONS)]
    pub max_sessions: usize,
    #[arg(long, default_value_t = DEFAULT_MANIFEST_ACK_RECORDS)]
    pub manifest_ack_records: u64,
    #[arg(long, default_value_t = DEFAULT_MANIFEST_ACK_KB)]
    pub manifest_ack_kb: usize,
}

// Невалидные комбинации `--listen/--connect` лучше отсекать ещё на этапе
// разбора CLI, чтобы runtime-код работал только с осмысленными состояниями.
#[derive(Debug, Clone, Args)]
#[command(group(
    ArgGroup::new("transport")
        .required(true)
        .args(["listen", "connect"])
))]
pub struct TransportArgs {
    // В этом режиме узел слушает входящее TCP-подключение.
    #[arg(long)]
    pub listen: Option<SocketAddr>,
    // В этом режиме узел сам инициирует TCP-подключение.
    #[arg(long)]
    pub connect: Option<SocketAddr>,
}

impl TransportArgs {
    // Два Option превращаем в один строгий enum.
    // Это удобнее для дальнейшей логики в transport-модуле.
    pub fn mode(&self) -> Result<TransportMode> {
        match (self.listen, self.connect) {
            (Some(addr), None) => Ok(TransportMode::Listen(addr)),
            (None, Some(addr)) => Ok(TransportMode::Connect(addr)),
            _ => bail!("exactly one of --listen or --connect must be provided"),
        }
    }
}

impl SenderArgs {
    fn resolved_threads_arg(&self) -> ThreadCountArg {
        self.threads.unwrap_or(ThreadCountArg::Auto)
    }

    fn resolved_read_buffer_kb(&self) -> usize {
        self.read_buffer_kb.unwrap_or(DEFAULT_READ_BUFFER_KB)
    }

    fn resolved_frame_target_mb(&self) -> u64 {
        self.frame_target_mb.unwrap_or(DEFAULT_FRAME_TARGET_MB)
    }
    // Пока dump_id нужен уже на этапе bootstrap-resume, а отдельного генератора
    // идентификаторов ещё нет. Поэтому безопасный fallback берём из имени source.
    pub fn resolve_dump_id(&self) -> Result<String> {
        if let Some(dump_id) = &self.dump_id {
            return Ok(dump_id.clone());
        }

        match self.source.file_name().and_then(|value| value.to_str()) {
            Some(value) if !value.is_empty() && !matches!(value, "." | "..") => {
                Ok(value.to_owned())
            }
            _ => bail!(
                "--dump-id is required when --source does not end with a normal path component"
            ),
        }
    }

    // Здесь CLI-значения приводятся к runtime-настройкам sender и сразу валидируются,
    // чтобы остальной код не таскал по стеку сырой clap-состояние.
    pub fn runtime_options(&self) -> Result<SenderRuntimeOptions> {
        let threads = self.resolved_threads_arg().resolve();
        let read_buffer_kb = self.resolved_read_buffer_kb();
        let frame_target_mb = self.resolved_frame_target_mb();
        if threads == 0 {
            bail!("threads must be at least 1");
        }
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
        if read_buffer_kb == 0 {
            bail!("read_buffer_kb must be at least 1");
        }
        if frame_target_mb == 0 {
            bail!("frame_target_mb must be at least 1");
        }
        if self.manifest_chunk_kb == 0 {
            bail!("manifest_chunk_kb must be at least 1");
        }
        if self.manifest_ack_records == 0 {
            bail!("manifest_ack_records must be at least 1");
        }
        if self.manifest_ack_kb == 0 {
            bail!("manifest_ack_kb must be at least 1");
        }

        let read_buffer_bytes = read_buffer_kb
            .checked_mul(1024)
            .context("read_buffer_kb is too large")?;
        let frame_target_bytes = frame_target_mb
            .checked_mul(1024 * 1024)
            .context("frame_target_mb is too large")?;
        let manifest_chunk_bytes = self
            .manifest_chunk_kb
            .checked_mul(1024)
            .context("manifest_chunk_kb is too large")?;
        let manifest_ack_bytes = self
            .manifest_ack_kb
            .checked_mul(1024)
            .context("manifest_ack_kb is too large")?;
        if manifest_chunk_bytes > protocol::MAX_MANIFEST_CHUNK_PAYLOAD_BYTES {
            bail!(
                "manifest_chunk_kb exceeds protocol cap: {} > {} KiB",
                self.manifest_chunk_kb,
                protocol::MAX_MANIFEST_CHUNK_PAYLOAD_BYTES / 1024
            );
        }
        if self.manifest_ack_records > protocol::MAX_UNACKED_MANIFEST_RECORDS {
            bail!(
                "manifest_ack_records exceeds protocol cap: {} > {}",
                self.manifest_ack_records,
                protocol::MAX_UNACKED_MANIFEST_RECORDS
            );
        }
        if manifest_ack_bytes > protocol::MAX_UNACKED_MANIFEST_BYTES {
            bail!(
                "manifest_ack_kb exceeds protocol cap: {} > {} KiB",
                self.manifest_ack_kb,
                protocol::MAX_UNACKED_MANIFEST_BYTES / 1024
            );
        }
        if manifest_ack_bytes < manifest_chunk_bytes {
            bail!(
                "manifest_ack_kb must not be smaller than manifest_chunk_kb: {} < {}",
                self.manifest_ack_kb,
                self.manifest_chunk_kb
            );
        }

        Ok(SenderRuntimeOptions {
            level: self.level,
            threads,
            min_readers: self.min_readers,
            max_readers: self.max_readers,
            read_buffer_bytes,
            frame_target_bytes,
            manifest_chunk_bytes,
            manifest_ack_records: self.manifest_ack_records,
            manifest_ack_bytes,
        })
    }

    pub fn sender_source(&self) -> SenderSource {
        SenderSource::ordinary_path(self.source.clone())
    }
}

impl ReceiverArgs {
    // Receiver-сервер бессмысленно запускать без хотя бы одного доступного session slot.
    pub fn validated_max_sessions(&self) -> Result<usize> {
        if self.max_sessions == 0 {
            bail!("max_sessions must be at least 1");
        }

        Ok(self.max_sessions)
    }

    // Для active v3 receiver локально выбирает cadence durable ManifestAck.
    // Эти значения должны быть валидны по hard caps протокола и не могут быть нулевыми.
    pub fn runtime_options(&self) -> Result<ReceiverRuntimeOptions> {
        if self.manifest_ack_records == 0 {
            bail!("manifest_ack_records must be at least 1");
        }
        if self.manifest_ack_kb == 0 {
            bail!("manifest_ack_kb must be at least 1");
        }

        let manifest_ack_bytes = self
            .manifest_ack_kb
            .checked_mul(1024)
            .context("manifest_ack_kb is too large")?;
        if self.manifest_ack_records > protocol::MAX_UNACKED_MANIFEST_RECORDS {
            bail!(
                "manifest_ack_records exceeds protocol cap: {} > {}",
                self.manifest_ack_records,
                protocol::MAX_UNACKED_MANIFEST_RECORDS
            );
        }
        if manifest_ack_bytes > protocol::MAX_UNACKED_MANIFEST_BYTES {
            bail!(
                "manifest_ack_kb exceeds protocol cap: {} > {} KiB",
                self.manifest_ack_kb,
                protocol::MAX_UNACKED_MANIFEST_BYTES / 1024
            );
        }

        Ok(ReceiverRuntimeOptions {
            manifest_ack_records: self.manifest_ack_records,
            manifest_ack_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::{Parser, error::ErrorKind};

    use super::{Cli, Command, ThreadCountArg, TransportArgs};
    use crate::network::transport::TransportMode;
    use crate::role::Role;
    use crate::sender::SenderSourceKind;

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
    fn sender_cli_resolves_runtime_defaults() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7011",
            "--source",
            "input",
        ])
        .unwrap();

        let Command::Sender(args) = cli.command else {
            panic!("expected sender command");
        };
        let runtime = args.runtime_options().unwrap();

        assert_eq!(args.threads, None);
        assert_eq!(args.read_buffer_kb, None);
        assert_eq!(args.frame_target_mb, None);
        assert_eq!(runtime.level, 3);
        assert_eq!(
            runtime.threads,
            std::thread::available_parallelism()
                .map(|count| count.get().saturating_sub(1).max(1) as u32)
                .unwrap_or(1)
        );
        assert_eq!(runtime.min_readers, 2);
        assert_eq!(runtime.max_readers, 8);
        assert_eq!(runtime.read_buffer_bytes, 1024 * 1024);
        assert_eq!(runtime.frame_target_bytes, 64 * 1024 * 1024);
        assert_eq!(runtime.manifest_chunk_bytes, 512 * 1024);
        assert_eq!(runtime.manifest_ack_records, 4_096);
        assert_eq!(runtime.manifest_ack_bytes, 8 * 1024 * 1024);
    }

    #[test]
    fn sender_cli_uses_ordinary_path_backend_by_default() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7017",
            "--source",
            "input",
        ])
        .unwrap();

        let Command::Sender(args) = cli.command else {
            panic!("expected sender command");
        };
        let source = args.sender_source();

        assert_eq!(source.kind(), SenderSourceKind::OrdinaryPath);
        assert_eq!(source.path(), PathBuf::from("input").as_path());
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
            "4",
            "--min-readers",
            "3",
            "--max-readers",
            "9",
            "--read-buffer-kb",
            "2048",
            "--frame-target-mb",
            "5",
            "--manifest-chunk-kb",
            "512",
            "--manifest-ack-records",
            "2048",
            "--manifest-ack-kb",
            "2048",
        ])
        .unwrap();

        let Command::Sender(args) = cli.command else {
            panic!("expected sender command");
        };
        let runtime = args.runtime_options().unwrap();

        assert_eq!(args.threads, Some(ThreadCountArg::Fixed(4)));
        assert_eq!(args.read_buffer_kb, Some(2048));
        assert_eq!(args.frame_target_mb, Some(5));
        assert_eq!(runtime.level, 7);
        assert_eq!(runtime.threads, 4);
        assert_eq!(runtime.min_readers, 3);
        assert_eq!(runtime.max_readers, 9);
        assert_eq!(runtime.read_buffer_bytes, 2 * 1024 * 1024);
        assert_eq!(runtime.frame_target_bytes, 5 * 1024 * 1024);
        assert_eq!(runtime.manifest_chunk_bytes, 512 * 1024);
        assert_eq!(runtime.manifest_ack_records, 2048);
        assert_eq!(runtime.manifest_ack_bytes, 2 * 1024 * 1024);
    }

    #[test]
    fn sender_cli_rejects_invalid_runtime_bounds() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7013",
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

        let cli = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7014",
            "--source",
            "input",
            "--min-readers",
            "5",
            "--max-readers",
            "4",
        ])
        .unwrap();

        let Command::Sender(args) = cli.command else {
            panic!("expected sender command");
        };
        let error = args.runtime_options().unwrap_err();
        assert!(error.to_string().contains("min_readers"));

        let cli = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7015",
            "--source",
            "input",
            "--manifest-chunk-kb",
            "1024",
        ])
        .unwrap();

        let Command::Sender(args) = cli.command else {
            panic!("expected sender command");
        };
        let error = args.runtime_options().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("manifest_chunk_kb exceeds protocol cap")
        );

        let cli = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7016",
            "--source",
            "input",
            "--manifest-chunk-kb",
            "512",
            "--manifest-ack-kb",
            "256",
        ])
        .unwrap();

        let Command::Sender(args) = cli.command else {
            panic!("expected sender command");
        };
        let error = args.runtime_options().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("manifest_ack_kb must not be smaller than manifest_chunk_kb")
        );
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
    fn sender_cli_keeps_explicit_auto_threads_distinct_from_omitted_default() {
        let cli = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--connect",
            "127.0.0.1:7018",
            "--source",
            "input",
            "--threads",
            "auto",
        ])
        .unwrap();

        let Command::Sender(args) = cli.command else {
            panic!("expected sender command");
        };

        assert_eq!(args.threads, Some(ThreadCountArg::Auto));
        assert_eq!(
            args.runtime_options().unwrap().threads,
            ThreadCountArg::Auto.resolve()
        );
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

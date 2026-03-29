use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::{ArgGroup, Args, Parser, Subcommand, ValueHint};

use crate::network::transport::TransportMode;
use crate::role::Role;

// Корневой CLI-парсер.
// Clap строит его по derive-макросам и атрибутам ниже.
// Здесь мы держим именно shape входных аргументов, а не runtime-логику.
#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct Cli {
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

    // А здесь CLI-аргументы уже переходят в runtime-представление транспорта.
    // После этого transport-слою не нужно знать ничего про clap.
    pub fn transport_mode(&self) -> Result<TransportMode> {
        match self {
            Self::Sender(args) => args.transport.mode(),
            Self::Receiver(args) => args.transport.mode(),
        }
    }
}

// Параметры sender.
// Пока нужен только source и способ установить соединение.
#[derive(Debug, Args)]
pub struct SenderArgs {
    // Оба subcommand используют одну и ту же transport-валидацию,
    // поэтому флаги вынесены в общий вложенный тип.
    #[command(flatten)]
    pub transport: TransportArgs,
    // Это корень, относительно которого позже будет строиться tar-поток.
    #[arg(long, value_hint = ValueHint::DirPath)]
    pub source: PathBuf,
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
    // Дополнительная проверка остаётся полезной: она защищает даже если
    // этот тип когда-нибудь создадут не через clap.
    pub fn mode(&self) -> Result<TransportMode> {
        match (self.listen, self.connect) {
            (Some(addr), None) => Ok(TransportMode::Listen(addr)),
            (None, Some(addr)) => Ok(TransportMode::Connect(addr)),
            _ => bail!("exactly one of --listen or --connect must be provided"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::{Parser, error::ErrorKind};

    use super::{Cli, Command, TransportArgs};
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
}

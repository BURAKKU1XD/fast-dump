use std::io::{self, IsTerminal};
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use clap::{Arg, ArgGroup, ArgMatches, CommandFactory, FromArgMatches};

use crate::cli::{Cli, Command};
use crate::network::relay::{
    access::{
        ACCESS_TOKEN_ARG, INVITATION_ARG, JOIN_RELAY_ARG, LobbySettings, credentials,
        validate_access_token,
    },
    validate_endpoint,
};
use crate::progress::ProgressHub;
use crate::retry::RetryPolicy;
use crate::runtime::RunControl;
use crate::ui::ConnectionSettings;

fn command() -> clap::Command {
    const RELAY_CREDENTIAL: &str = "relay-credential";
    Cli::command()
        .subcommand_required(false)
        .arg_required_else_help(false)
        .group(ArgGroup::new(RELAY_CREDENTIAL).args([ACCESS_TOKEN_ARG, INVITATION_ARG]))
        .arg(
            Arg::new(JOIN_RELAY_ARG)
                .long(JOIN_RELAY_ARG)
                .value_name("HOST:PORT")
                .requires(RELAY_CREDENTIAL)
                .conflicts_with("no_gui")
                .help("Connect directly to a relay lobby in the terminal"),
        )
        .arg(
            Arg::new(ACCESS_TOKEN_ARG)
                .long(ACCESS_TOKEN_ARG)
                .value_name("TOKEN")
                .requires(JOIN_RELAY_ARG)
                .help("12-character token for a temporary relay whose key is learned on first connection"),
        )
        .arg(
            Arg::new(INVITATION_ARG)
                .long(INVITATION_ARG)
                .value_name("STRING")
                .requires(JOIN_RELAY_ARG)
                .help("Invitation from the relay operator"),
        )
}

fn use_dashboard(no_gui: bool, stdin: bool, stdout: bool, stderr: bool, dumb: bool) -> bool {
    !no_gui && stdin && stdout && stderr && !dumb
}

fn relay_join_settings(matches: &ArgMatches) -> Result<Option<ConnectionSettings>> {
    let Some(endpoint) = matches.get_one::<String>(JOIN_RELAY_ARG) else {
        return Ok(None);
    };
    ensure!(
        matches.subcommand_name().is_none(),
        "--join-relay opens the lobby and cannot be combined with a subcommand"
    );
    validate_endpoint(endpoint)?;
    let access_token = matches.get_one::<String>(ACCESS_TOKEN_ARG);
    let (token, trust_first_key) = if let Some(token) = access_token {
        validate_access_token(token)?;
        (token, true)
    } else {
        let invitation = matches
            .get_one::<String>(INVITATION_ARG)
            .context("missing relay access token or invitation")?;
        credentials(invitation)?;
        (invitation, false)
    };
    Ok(Some(ConnectionSettings::Relay(LobbySettings {
        endpoint: endpoint.clone(),
        token: token.clone(),
        ca_file: None,
        trust_first_key,
    })))
}

fn run_lobby(
    settings: ConnectionSettings,
    debug: bool,
    policy: RetryPolicy,
    control: &RunControl,
    hub: Arc<ProgressHub>,
) -> ExitCode {
    crate::telemetry::init_logging_with_progress(debug, Some(hub.clone()));
    let result = crate::ui::run_lobby(settings, policy, control, hub);
    if control.is_cancelled() {
        return ExitCode::from(130);
    }
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Connection: {error:#}");
            ExitCode::FAILURE
        }
    }
}

pub(crate) fn entry() -> ExitCode {
    let matches = command().get_matches();
    let interactive = use_dashboard(
        matches.get_flag("no_gui"),
        io::stdin().is_terminal(),
        io::stdout().is_terminal(),
        io::stderr().is_terminal(),
        std::env::var_os("TERM").is_some_and(|term| term == "dumb"),
    );
    let retain_result = matches.subcommand().is_none();
    let policy = RetryPolicy::from_options(
        matches.get_one::<u32>("retries").copied(),
        *matches
            .get_one::<u32>("retry_delay_secs")
            .expect("default retry delay"),
        interactive,
    );
    let hub = interactive.then(|| Arc::new(ProgressHub::new()));
    let control = RunControl::new(hub.clone());
    let signal_control = control.clone();
    if let Err(error) = ctrlc::set_handler(move || signal_control.cancel()) {
        eprintln!("Error installing the interrupt handler: {error}");
        return ExitCode::FAILURE;
    }
    match relay_join_settings(&matches) {
        Ok(Some(settings)) => {
            let Some(hub) = hub else {
                eprintln!("Joining a relay lobby requires an interactive terminal.");
                return ExitCode::from(2);
            };
            return run_lobby(settings, matches.get_flag("debug"), policy, &control, hub);
        }
        Ok(None) => {}
        Err(error) => {
            eprintln!("Relay: {error:#}");
            return ExitCode::from(2);
        }
    }
    if retain_result {
        let Some(hub) = hub else {
            command()
                .error(
                    clap::error::ErrorKind::MissingSubcommand,
                    "a sender, receiver, or relay command is required outside the interactive wizard",
                )
                .exit();
        };
        let debug = matches.get_flag("debug");
        crate::telemetry::init_logging_with_progress(debug, Some(hub.clone()));
        let mut relay = false;
        let result = crate::ui::wizard(debug, policy, &control, |launch| match launch {
            crate::ui::Launch::Cli(cli) => {
                relay = matches!(&cli.command, Command::Relay(_));
                run_cli(*cli, policy, &control, Some(hub.clone()), true)
            }
            crate::ui::Launch::Lobby(settings) => {
                relay = false;
                crate::ui::run_lobby(settings, policy, &control, hub.clone()).context("Connection")
            }
        });
        return finish_cli(result, &control, relay);
    }
    let cli = match Cli::from_arg_matches(&matches) {
        Ok(cli) => cli,
        Err(error) => error.exit(),
    };
    let relay = matches!(&cli.command, Command::Relay(_));
    crate::telemetry::init_logging_with_progress(cli.debug, hub.clone());
    let result = run_cli(cli, policy, &control, hub, false);
    finish_cli(result, &control, relay)
}

fn run_cli(
    mut cli: Cli,
    policy: RetryPolicy,
    control: &RunControl,
    hub: Option<Arc<ProgressHub>>,
    retain_result: bool,
) -> Result<()> {
    cli.retries = Some(policy.retries);
    cli.retry_delay_secs = policy.delay.as_secs() as u32;
    crate::app::validate(&cli)?;
    match hub {
        Some(hub) => crate::ui::run(cli, control.clone(), hub, retain_result),
        None => crate::app::run_controlled(cli, control),
    }
}

fn finish_cli(result: Result<()>, control: &RunControl, relay: bool) -> ExitCode {
    control.disable_ui();
    match result {
        Ok(()) if control.is_cancelled() => ExitCode::from(130),
        Ok(()) => ExitCode::SUCCESS,
        Err(_) if control.is_cancelled() => {
            if relay {
                eprintln!("Relay stopped.");
            } else {
                eprintln!(
                    "Stopped. Durable progress is preserved; repeat the same command to resume an unfinished dump."
                );
            }
            ExitCode::from(130)
        }
        Err(error) => {
            eprintln!("Error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_fully_interactive_terminals_enable_dashboard() {
        assert!(use_dashboard(false, true, true, true, false));
        for bits in 0..8 {
            if bits != 7 {
                assert!(!use_dashboard(
                    false,
                    bits & 1 != 0,
                    bits & 2 != 0,
                    bits & 4 != 0,
                    false
                ));
            }
        }
        assert!(!use_dashboard(true, true, true, true, false));
        assert!(!use_dashboard(false, true, true, true, true));
    }

    #[test]
    fn launcher_allows_wizard_but_keeps_explicit_commands_strict() {
        assert!(
            command()
                .try_get_matches_from(["fast-dump"])
                .unwrap()
                .subcommand()
                .is_none()
        );
        assert!(
            command()
                .try_get_matches_from(["fast-dump", "sender"])
                .is_err()
        );
        let matches = command()
            .try_get_matches_from([
                "fast-dump",
                "--no-gui",
                "sender",
                "--source",
                "input",
                "--listen",
                "127.0.0.1:7001",
            ])
            .unwrap();
        assert!(Cli::from_arg_matches(&matches).unwrap().no_gui);
        let matches = command()
            .try_get_matches_from([
                "fast-dump",
                "sender",
                "--source",
                "input",
                "--listen",
                "127.0.0.1:7001",
                "--no-gui",
            ])
            .unwrap();
        assert!(Cli::from_arg_matches(&matches).unwrap().no_gui);
    }

    #[test]
    fn join_arguments_open_relay_lobby_without_a_transfer_role() {
        let token = "23456789ABCD";
        let matches = command()
            .try_get_matches_from([
                "fast-dump",
                "--join-relay",
                "relay.example:9876",
                "--access-token",
                token,
            ])
            .unwrap();
        let Some(ConnectionSettings::Relay(settings)) = relay_join_settings(&matches).unwrap()
        else {
            panic!("expected settings for a relay lobby");
        };
        assert_eq!(settings.endpoint, "relay.example:9876");
        assert_eq!(settings.token, token);
        assert!(settings.ca_file.is_none());
        assert!(settings.trust_first_key);
    }

    #[test]
    fn join_arguments_require_both_values_and_an_interactive_lobby() {
        let invitation = "23456789ABCD";
        assert!(
            command()
                .try_get_matches_from(["fast-dump", "--join-relay", "relay.example:9876"])
                .is_err()
        );
        assert!(
            command()
                .try_get_matches_from(["fast-dump", "--invitation", invitation])
                .is_err()
        );
        let join = [
            "fast-dump",
            "--join-relay",
            "relay.example:9876",
            "--invitation",
            invitation,
        ];
        let matches = command().try_get_matches_from(join).unwrap();
        let Some(ConnectionSettings::Relay(settings)) = relay_join_settings(&matches).unwrap()
        else {
            panic!("expected settings for a relay lobby");
        };
        assert!(!settings.trust_first_key);
        assert!(
            command()
                .try_get_matches_from(join.into_iter().chain(["--access-token", invitation]))
                .is_err()
        );
        let invalid = command()
            .try_get_matches_from([
                "fast-dump",
                "--join-relay",
                "relay.example:9876",
                "--access-token",
                "123",
            ])
            .unwrap();
        assert!(relay_join_settings(&invalid).is_err());
        assert!(
            command()
                .try_get_matches_from(join.into_iter().chain(["--no-gui"]))
                .is_err()
        );
        let with_command = command().try_get_matches_from(join.into_iter().chain([
            "sender",
            "--source",
            ".",
            "--listen",
            "127.0.0.1:7000",
        ]));
        if let Ok(matches) = with_command {
            assert!(relay_join_settings(&matches).is_err());
        }
    }

    #[test]
    fn help_and_version_exit_before_terminal_setup() {
        for args in [
            vec!["fast-dump", "--help"],
            vec!["fast-dump", "help", "sender"],
            vec!["fast-dump", "--version"],
        ] {
            let error = command().try_get_matches_from(args).unwrap_err();
            assert!(matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            ));
            assert_eq!(error.exit_code(), 0);
        }
    }

    #[test]
    fn wizard_preserves_global_retry_overrides() {
        let matches = command()
            .try_get_matches_from(["fast-dump", "--retries", "0", "--retry-delay-secs", "3"])
            .unwrap();
        assert!(matches.subcommand().is_none());
        let policy = RetryPolicy::from_options(
            matches.get_one::<u32>("retries").copied(),
            *matches.get_one::<u32>("retry_delay_secs").unwrap(),
            true,
        );
        assert_eq!(policy.retries, 0);
        assert_eq!(policy.delay.as_secs(), 3);
    }
}

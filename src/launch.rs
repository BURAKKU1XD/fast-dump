use std::io::{self, IsTerminal};
use std::process::ExitCode;
use std::sync::Arc;

use clap::{CommandFactory, FromArgMatches};

use crate::cli::Cli;
use crate::progress::ProgressHub;
use crate::retry::RetryPolicy;
use crate::runtime::RunControl;

fn command() -> clap::Command {
    Cli::command()
        .subcommand_required(false)
        .arg_required_else_help(false)
}

fn use_dashboard(no_gui: bool, stdin: bool, stdout: bool, stderr: bool, dumb: bool) -> bool {
    !no_gui && stdin && stdout && stderr && !dumb
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
    let mut cli = if retain_result {
        if !interactive {
            command()
                .error(
                    clap::error::ErrorKind::MissingSubcommand,
                    "a sender or receiver command is required outside the interactive wizard",
                )
                .exit();
        }
        match crate::ui::wizard(matches.get_flag("debug"), policy, &control) {
            Ok(Some(cli)) => cli,
            Ok(None) => {
                return if control.is_cancelled() {
                    ExitCode::from(130)
                } else {
                    ExitCode::SUCCESS
                };
            }
            Err(error) => {
                eprintln!(
                    "Could not open the setup wizard: {error:#}\nUse --help or pass a complete command with --no-gui."
                );
                return ExitCode::FAILURE;
            }
        }
    } else {
        match Cli::from_arg_matches(&matches) {
            Ok(cli) => cli,
            Err(error) => error.exit(),
        }
    };
    cli.retries = Some(policy.retries);
    cli.retry_delay_secs = policy.delay.as_secs() as u32;
    if let Err(error) = crate::app::validate(&cli) {
        eprintln!("Error: {error:#}");
        return ExitCode::FAILURE;
    }
    crate::telemetry::init_logging_with_progress(cli.debug, hub.clone());
    let result = match hub {
        Some(hub) => crate::ui::run(cli, control.clone(), hub, retain_result),
        None => crate::app::run_controlled(cli, &control),
    };
    control.disable_ui();
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) if control.is_cancelled() => {
            eprintln!(
                "Stopped. Durable progress is preserved; repeat the same command to resume an unfinished dump."
            );
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

mod dashboard;
mod relay;
mod terminal;
mod theme;
mod widgets;
mod wizard;

use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};

use crate::cli::{Cli, Command};
use crate::network::ListenError;
use crate::progress::ProgressHub;
use crate::retry::RetryPolicy;
use crate::runtime::RunControl;

use dashboard::Dashboard;
use terminal::TerminalGuard;
use theme::Theme;

pub(crate) use wizard::{ConnectionSettings, Launch, run_lobby, wizard};

const DRAW_INTERVAL: Duration = Duration::from_millis(200);

// UI владеет stdin/terminal, worker — runtime. Guard не оставляет transfer
// в фоне после ошибки ввода или panic; сбой отрисовки разрешает текстовый fallback.
struct RuntimeWorker {
    handle: Option<JoinHandle<Result<()>>>,
    control: RunControl,
    panicked: bool,
}

impl RuntimeWorker {
    fn join(&mut self) -> Result<()> {
        match self
            .handle
            .take()
            .expect("transfer worker joined twice")
            .join()
        {
            Ok(result) => result,
            Err(_) => {
                self.panicked = true;
                Err(anyhow!("runtime worker panicked"))
            }
        }
    }

    fn finished(&self) -> bool {
        self.handle.as_ref().is_none_or(JoinHandle::is_finished)
    }
}

impl Drop for RuntimeWorker {
    fn drop(&mut self) {
        if self.handle.is_some() {
            self.control.cancel();
            let _ = self.join();
        }
    }
}

pub(crate) fn run(
    cli: Cli,
    control: RunControl,
    hub: Arc<ProgressHub>,
    retain_result: bool,
) -> Result<()> {
    let result = if let Command::Relay(args) = cli.command {
        relay::run(args, control.clone())
    } else {
        run_with(
            cli,
            control.clone(),
            hub,
            retain_result,
            crate::app::run_controlled,
        )
    };
    if !retain_result
        && !control.is_cancelled()
        && let Err(error) = &result
        && let Some(error) = error.downcast_ref::<ListenError>()
        && let Err(ui_error) = show_listen_error(error, &control)
    {
        log::debug!("could not present listener error: {ui_error:#}");
    }
    result
}

fn show_listen_error(error: &ListenError, control: &RunControl) -> Result<()> {
    let mut terminal = TerminalGuard::enter()?;
    let theme = Theme::detect();
    let message = error.to_string();
    let mut redraw = true;
    while !control.is_cancelled() {
        if redraw {
            terminal.terminal.draw(|frame| {
                widgets::render_dialog(
                    frame,
                    theme,
                    " Startup error ",
                    &[
                        &message,
                        "",
                        "Choose another listen address or port.",
                        "Enter / Esc closes.",
                    ],
                );
            })?;
            redraw = false;
        }
        if event::poll(DRAW_INTERVAL)? {
            match event::read()? {
                Event::Resize(_, _) => redraw = true,
                Event::Key(key) if key.kind != KeyEventKind::Release => {
                    if key.code == KeyCode::Char('c')
                        && key.modifiers.contains(KeyModifiers::CONTROL)
                    {
                        control.cancel();
                    }
                    if matches!(key.code, KeyCode::Enter | KeyCode::Esc) {
                        break;
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn run_with(
    cli: Cli,
    control: RunControl,
    hub: Arc<ProgressHub>,
    retain_result: bool,
    run: impl FnOnce(Cli, &RunControl) -> Result<()> + Send + 'static,
) -> Result<()> {
    let mut terminal = match TerminalGuard::enter() {
        Ok(terminal) => terminal,
        Err(error) => {
            control.disable_ui();
            eprintln!("Terminal UI unavailable; continuing in text mode: {error:#}");
            return run(cli, &control);
        }
    };
    let args = match &cli.command {
        Command::Sender(args) => &args.transport,
        Command::Receiver(args) => &args.transport,
        Command::Relay(_) => return run(cli, &control),
    };
    let transport = if let Some(relay) = &args.relay {
        if let Some(room) = &args.room {
            format!("relay {relay} · room {room}")
        } else {
            format!("relay {relay}")
        }
    } else {
        match args.mode()? {
            crate::network::transport::TransportMode::Connect(address) => {
                format!("connect to {address}")
            }
            crate::network::transport::TransportMode::Listen(address) => {
                format!("listen on {address}")
            }
        }
    };
    let context = format!(
        "{} · {transport}",
        cli.command.role().context("missing transfer role")?
    );
    let configuration = configuration(&cli)?;
    let worker_control = control.clone();
    let handle = thread::Builder::new()
        .name("dump-runtime".into())
        .spawn(move || run(cli, &worker_control))
        .context("could not start transfer worker")?;
    let mut worker = RuntimeWorker {
        handle: Some(handle),
        control: control.clone(),
        panicked: false,
    };
    let mut dashboard = Dashboard::new(Theme::detect());
    dashboard.set_context(context);
    dashboard.set_configuration(configuration);
    let mut result = None;
    let mut next_draw = Instant::now();

    loop {
        if terminal.was_restored_after_panic() {
            drop(terminal);
            control.disable_ui();
            eprintln!("Terminal restored after a worker panic; switching to text mode.");
            return result.unwrap_or_else(|| worker.join());
        }
        if result.is_none() && worker.finished() {
            let completed = worker.join();
            if !retain_result || worker.panicked || control.is_cancelled() {
                return completed;
            }
            dashboard.finish(completed.as_ref().err().map(|error| format!("{error:#}")));
            result = Some(completed);
            next_draw = Instant::now();
        }
        if control.is_cancelled()
            && let Some(completed) = result.take()
        {
            return completed;
        }
        if Instant::now() >= next_draw {
            dashboard.refresh(&hub, control.is_cancelled());
            if terminal.was_restored_after_panic() {
                continue;
            }
            if let Err(error) = terminal.terminal.draw(|frame| dashboard.render(frame)) {
                drop(terminal);
                control.disable_ui();
                eprintln!("Terminal drawing failed; continuing in text mode: {error}");
                return result.unwrap_or_else(|| worker.join());
            }
            next_draw = Instant::now() + DRAW_INTERVAL;
        }

        let wait = next_draw.saturating_duration_since(Instant::now());
        let input = (|| -> Result<Option<Event>> {
            if !event::poll(wait).context("terminal input failed")? {
                return Ok(None);
            }
            event::read()
                .context("could not read terminal input")
                .map(Some)
        })();
        let event = match input {
            Ok(Some(event)) => event,
            Ok(None) => continue,
            Err(error) => {
                // Ошибка stdin сначала освобождает экран; ожидание filesystem
                // cleanup может быть долгим и не должно оставлять raw terminal.
                drop(terminal);
                control.disable_ui();
                control.cancel();
                if worker.handle.is_some() {
                    let _ = worker.join();
                }
                return Err(error);
            }
        };
        match event {
            Event::Key(key) if key.kind != KeyEventKind::Release => {
                let interrupt =
                    key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL);
                if interrupt {
                    if result.is_some() {
                        return result.take().unwrap();
                    }
                    control.cancel();
                    dashboard.stop_requested();
                } else if dashboard.key(key.code, result.is_some()) {
                    if let Some(result) = result.take() {
                        return result;
                    }
                    control.cancel();
                    dashboard.stop_requested();
                }
            }
            // Autoresize произойдёт на ближайшем кадре. Поток событий/автоповтор
            // клавиш не должен увеличивать частоту отрисовки выше пяти FPS.
            Event::Resize(_, _) => {}
            _ => {}
        }
    }
}

fn configuration(cli: &Cli) -> Result<Vec<(String, String)>> {
    let mut values = match &cli.command {
        Command::Sender(args) => {
            let options = args.runtime_options()?;
            let requested_readers = crate::sender::BundlePlanOptions::default()
                .readers
                .clamp(options.min_readers, options.max_readers);
            vec![
                ("Source".into(), args.source.to_string_lossy().into_owned()),
                (
                    "Compression".into(),
                    format!("zstd level {}", options.level),
                ),
                ("Threads".into(), options.threads.to_string()),
                (
                    "Readers".into(),
                    format!(
                        "{} requested (bounds {}–{})",
                        requested_readers, options.min_readers, options.max_readers
                    ),
                ),
                (
                    "Bundle target".into(),
                    format!("{} MiB", options.bundle_target_bytes / 1024 / 1024),
                ),
            ]
        }
        Command::Receiver(args) => vec![
            (
                "Data directory".into(),
                args.data_dir.to_string_lossy().into_owned(),
            ),
            (
                "Max sessions".into(),
                args.validated_max_sessions()?.to_string(),
            ),
        ],
        Command::Relay(_) => Vec::new(),
    };
    let persistent =
        matches!(&cli.command, Command::Receiver(args) if args.transport.listen.is_some());
    values.push((
        "Retry policy".into(),
        retry_description(&RetryPolicy::from_cli(cli, true), persistent),
    ));
    Ok(values)
}

fn retry_description(policy: &RetryPolicy, persistent: bool) -> String {
    if persistent {
        "Persistent listener; accepts reconnects".into()
    } else if policy.retries == 0 {
        "Disabled (0 retries)".into()
    } else {
        format!(
            "{} retries · {} s delay",
            policy.retries,
            policy.delay.as_secs()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn dropping_ui_worker_requests_cancellation_and_waits_for_cleanup() {
        let control = RunControl::new(None);
        let child_control = control.clone();
        let cleaned_up = Arc::new(AtomicBool::new(false));
        let child_cleanup = Arc::clone(&cleaned_up);
        let handle = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(3);
            while !child_control.is_cancelled() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
            child_cleanup.store(true, Ordering::Release);
            Ok(())
        });
        drop(RuntimeWorker {
            handle: Some(handle),
            control: control.clone(),
            panicked: false,
        });
        assert!(control.is_cancelled());
        assert!(cleaned_up.load(Ordering::Acquire));
    }

    #[test]
    fn joining_for_text_fallback_does_not_cancel_runtime() {
        let control = RunControl::new(None);
        let child_control = control.clone();
        let mut worker = RuntimeWorker {
            handle: Some(thread::spawn(move || {
                assert!(!child_control.is_cancelled());
                Ok(())
            })),
            control: control.clone(),
            panicked: false,
        };
        worker.join().unwrap();
        drop(worker);
        assert!(!control.is_cancelled());
    }

    #[test]
    fn configuration_exposes_effective_settings_without_tokens() {
        use clap::Parser;
        let cli = Cli::try_parse_from([
            "fast-dump",
            "sender",
            "--source",
            "input",
            "--connect",
            "127.0.0.1:7000",
            "--threads",
            "3",
            "--min-readers",
            "2",
            "--max-readers",
            "2",
            "--frame-target-mb",
            "8",
            "--level",
            "5",
            "--token",
            "secret-value",
            "--retries",
            "0",
        ])
        .unwrap();
        let values = configuration(&cli).unwrap();
        assert!(values.contains(&("Source".into(), "input".into())));
        assert!(values.contains(&("Compression".into(), "zstd level 5".into())));
        assert!(values.contains(&("Threads".into(), "3".into())));
        assert!(values.contains(&("Bundle target".into(), "8 MiB".into())));
        assert!(values.contains(&("Retry policy".into(), "Disabled (0 retries)".into())));
        assert!(
            values
                .iter()
                .all(|(name, value)| !name.contains("Token") && !value.contains("secret-value"))
        );
        let cli = Cli::try_parse_from([
            "fast-dump",
            "receiver",
            "--data-dir",
            "output",
            "--listen",
            "127.0.0.1:7000",
            "--max-sessions",
            "4",
        ])
        .unwrap();
        assert_eq!(
            configuration(&cli).unwrap(),
            vec![
                ("Data directory".into(), "output".into()),
                ("Max sessions".into(), "4".into()),
                (
                    "Retry policy".into(),
                    "Persistent listener; accepts reconnects".into()
                )
            ]
        );
        let cli = Cli::try_parse_from([
            "fast-dump",
            "receiver",
            "--data-dir",
            "output",
            "--connect",
            "127.0.0.1:7000",
            "--retries",
            "2",
            "--retry-delay-secs",
            "7",
        ])
        .unwrap();
        assert!(
            configuration(&cli)
                .unwrap()
                .contains(&("Retry policy".into(), "2 retries · 7 s delay".into()))
        );
    }
}

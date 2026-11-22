use std::sync::mpsc;
use std::thread;

use anyhow::{Context, Result};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
#[cfg(not(windows))]
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Wrap};

use crate::cli::RelayArgs;
use crate::network::relay::{self, LINUX_CLIENT, MANUAL_ADDRESS, RelayStatus, WINDOWS_CLIENT};
use crate::runtime::RunControl;

use super::widgets::panel;
use super::{DRAW_INTERVAL, RuntimeWorker, TerminalGuard, Theme};

pub(super) fn run(args: RelayArgs, control: RunControl) -> Result<()> {
    let initial_terminal = match TerminalGuard::enter() {
        Ok(terminal) => terminal,
        Err(error) => {
            control.disable_ui();
            eprintln!("Terminal UI unavailable; continuing in text mode: {error:#}");
            return relay::serve(args, &control);
        }
    };
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let worker_control = control.clone();
    let handle = thread::Builder::new()
        .name("relay-runtime".into())
        .spawn(move || {
            relay::serve_with_status(args, &worker_control, |status| {
                let _ = ready_tx.send(status);
            })
        })
        .context("could not start relay worker")?;
    let mut worker = RuntimeWorker {
        handle: Some(handle),
        control: control.clone(),
        panicked: false,
    };
    let mut terminal = initial_terminal;
    let theme = Theme::detect();
    let mut status = None;
    let mut redraw = true;
    while !worker.finished() && !control.is_cancelled() {
        if terminal.was_restored_after_panic() {
            drop(terminal);
            control.disable_ui();
            return worker.join();
        }
        if let Ok(ready) = ready_rx.try_recv() {
            status = Some(ready);
            redraw = true;
        }
        if redraw {
            terminal
                .terminal
                .draw(|frame| render(frame, theme, status.as_ref()))
                .context("could not draw relay screen")?;
            redraw = false;
        }
        match next_event()? {
            Some(Event::Resize(_, _)) => redraw = true,
            Some(Event::Key(key)) => match relay_key(key, true) {
                Some(RelayKey::Stop) => control.cancel(),
                Some(RelayKey::Toggle) => {
                    let Some(ready) = status.as_ref().filter(|ready| ready.clients.is_some())
                    else {
                        continue;
                    };
                    drop(terminal);
                    control.disable_ui();
                    // Печатаем в основном экране до включения raw mode, чтобы терминал
                    // сам переносил длинные команды и обрабатывал переводы строк.
                    ready.print();
                    eprintln!("\nP returns to the relay screen. Ctrl+C stops the relay.");
                    let restored = wait_for_screen(&worker, &control).and_then(|reopen| {
                        if !reopen || worker.finished() || control.is_cancelled() {
                            return Ok(None);
                        }
                        control.enable_ui();
                        TerminalGuard::enter().map(Some)
                    });
                    terminal = match restored {
                        Ok(Some(terminal)) => terminal,
                        Ok(None) => return worker.join(),
                        Err(error) => {
                            control.disable_ui();
                            eprintln!(
                                "Relay screen unavailable: {error:#}\nCtrl+C stops the relay."
                            );
                            return worker.join();
                        }
                    };
                    redraw = true;
                }
                None => {}
            },
            _ => {}
        }
    }
    drop(terminal);
    worker.join()
}

enum RelayKey {
    Stop,
    Toggle,
}

fn relay_key(key: KeyEvent, screen: bool) -> Option<RelayKey> {
    if key.kind == KeyEventKind::Release {
        return None;
    }
    match key.code {
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => Some(RelayKey::Stop),
        KeyCode::Esc if screen => Some(RelayKey::Stop),
        KeyCode::Char('p' | 'P')
            if key.kind == KeyEventKind::Press
                && !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
        {
            Some(RelayKey::Toggle)
        }
        _ => None,
    }
}

fn next_event() -> Result<Option<Event>> {
    if event::poll(DRAW_INTERVAL).context("relay terminal input failed")? {
        return event::read()
            .map(Some)
            .context("could not read relay terminal input");
    }
    Ok(None)
}

fn wait_for_screen(worker: &RuntimeWorker, control: &RunControl) -> Result<bool> {
    // Windows читает события напрямую из очереди консоли. Оставляем Ctrl+C
    // системному обработчику, а на Unix отключаем построчный ввод для клавиши P.
    #[cfg(not(windows))]
    let _raw_input = {
        struct RawInputGuard;
        impl Drop for RawInputGuard {
            fn drop(&mut self) {
                let _ = disable_raw_mode();
            }
        }
        enable_raw_mode().context("could not enable relay terminal input")?;
        RawInputGuard
    };

    while !worker.finished() && !control.is_cancelled() {
        if let Some(Event::Key(key)) = next_event()? {
            match relay_key(key, false) {
                Some(RelayKey::Stop) => control.cancel(),
                Some(RelayKey::Toggle) => return Ok(true),
                None => {}
            }
        }
    }
    Ok(false)
}

fn render(frame: &mut Frame<'_>, theme: Theme, status: Option<&RelayStatus>) {
    let area = frame.area();
    frame.render_widget(Paragraph::default().style(theme.base()), area);

    let mut details = Vec::new();
    let mut commands = Vec::new();
    if let Some(status) = status {
        details.push(Line::styled("Running", theme.accent().fg(theme.green)));
        details.push(Line::from(vec![
            Span::styled("Listening on  ", theme.muted()),
            Span::raw(status.listen.to_string()),
        ]));
        if let Some(clients) = &status.clients {
            details.push(Line::from(vec![
                Span::styled("Connect to    ", theme.muted()),
                Span::raw(clients.endpoint.as_str()),
            ]));
        }
        if let Some(token) = &status.token {
            details.push(Line::from(vec![
                Span::styled("Access token  ", theme.muted()),
                Span::styled(token.as_str(), theme.accent()),
            ]));
        }
        if let Some(clients) = &status.clients {
            if clients.needs_address {
                commands.push(Line::styled(MANUAL_ADDRESS, theme.muted().fg(theme.amber)));
                commands.push(Line::default());
            }
            commands.extend([
                Line::styled(WINDOWS_CLIENT, theme.accent()),
                Line::raw(clients.windows.as_str()),
                Line::default(),
                Line::styled(LINUX_CLIENT, theme.accent()),
                Line::raw(clients.linux.as_str()),
            ]);
        }
    } else {
        details.push(Line::raw("Starting relay…"));
    }

    let [header, connection, clients, footer] = Layout::vertical([
        Constraint::Length(2),
        Constraint::Length(details.len() as u16 + 2),
        Constraint::Min(0),
        Constraint::Length(1),
    ])
    .areas(area);
    frame.render_widget(
        Paragraph::new(" fast-dump / Relay").style(theme.accent()),
        header,
    );
    frame.render_widget(
        Paragraph::new(details).block(panel(" Relay ", theme, true)),
        connection,
    );
    if !commands.is_empty() {
        frame.render_widget(Paragraph::new(commands).wrap(Wrap { trim: false }), clients);
    }
    let keys = if status.is_some_and(|ready| ready.clients.is_some()) {
        " P plain text for copying   Esc / Ctrl-C stop relay"
    } else {
        " Esc / Ctrl-C stop relay"
    };
    frame.render_widget(Paragraph::new(keys).style(theme.muted()), footer);
}

#[cfg(test)]
mod tests {
    use ratatui::{Terminal, backend::TestBackend};

    use super::*;
    use crate::network::relay::ClientCommands;
    use crate::ui::theme::ColorMode;

    #[test]
    fn relay_shortcuts_work_in_both_views() {
        for screen in [true, false] {
            for (code, modifiers) in [
                (KeyCode::Char('p'), KeyModifiers::NONE),
                (KeyCode::Char('P'), KeyModifiers::SHIFT),
            ] {
                assert!(matches!(
                    relay_key(KeyEvent::new(code, modifiers), screen),
                    Some(RelayKey::Toggle)
                ));
            }
            for kind in [KeyEventKind::Repeat, KeyEventKind::Release] {
                let key = KeyEvent::new_with_kind(KeyCode::Char('p'), KeyModifiers::NONE, kind);
                assert!(relay_key(key, screen).is_none());
            }
            for modifiers in [KeyModifiers::CONTROL, KeyModifiers::ALT] {
                assert!(relay_key(KeyEvent::new(KeyCode::Char('p'), modifiers), screen).is_none());
            }
            let interrupt = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
            assert!(matches!(relay_key(interrupt, screen), Some(RelayKey::Stop)));
        }

        let escape = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
        assert!(matches!(relay_key(escape, true), Some(RelayKey::Stop)));
        assert!(relay_key(escape, false).is_none());
    }

    #[test]
    fn relay_screen_shows_the_connection_and_both_commands() {
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        let theme = Theme::new(ColorMode::Monochrome);
        let status = RelayStatus {
            listen: "0.0.0.0:9876".parse().unwrap(),
            token: Some("23456789ABCD".into()),
            clients: Some(ClientCommands {
                endpoint: "relay.example:9876".into(),
                needs_address: true,
                windows: concat!(
                    "curl.exe -fL https://github.com/BURAKKU1XD/fast-dump/releases/latest/download/",
                    "fast-dump-x86_64-pc-windows-msvc.exe -o fast-dump.exe && ",
                    ".\\fast-dump.exe --join-relay \"relay.example:9876\" --access-token \"23456789ABCD\""
                )
                .into(),
                linux: concat!(
                    "curl -fL https://github.com/BURAKKU1XD/fast-dump/releases/latest/download/",
                    "fast-dump-x86_64-unknown-linux-gnu -o fast-dump && chmod u+x fast-dump && ",
                    "./fast-dump --join-relay 'relay.example:9876' --access-token '23456789ABCD'"
                )
                .into(),
            }),
        };
        terminal
            .draw(|frame| render(frame, theme, Some(&status)))
            .unwrap();
        let buffer = terminal.backend().buffer();
        let mut screen = String::new();
        for row in buffer.content().chunks(usize::from(buffer.area.width)) {
            screen.extend(row.iter().map(|cell| cell.symbol()));
            screen.push('\n');
        }
        for text in [
            "fast-dump / Relay",
            "Running",
            "0.0.0.0:9876",
            "relay.example:9876",
            "23456789ABCD",
            MANUAL_ADDRESS,
            WINDOWS_CLIENT,
            LINUX_CLIENT,
            "\ncurl.exe -fL",
            "\ncurl -fL",
            "P plain text for copying",
            "Esc / Ctrl-C stop relay",
        ] {
            assert!(screen.contains(text), "{text}");
        }
        assert_eq!(screen.matches("23456789ABCD").count(), 3);
        let (_, command_section) = screen.split_once(MANUAL_ADDRESS).unwrap();
        assert!(!command_section.contains('│'));
    }
}

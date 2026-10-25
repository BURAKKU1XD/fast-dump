use std::io::{self, Stderr};
use std::panic::PanicHookInfo;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};
use crossterm::cursor::{Hide, Show};
use crossterm::event::{DisableBracketedPaste, EnableBracketedPaste};
use crossterm::execute;
use crossterm::style::{Attribute, ResetColor, SetAttribute};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::{Terminal, backend::CrosstermBackend};

pub(super) struct TerminalGuard {
    pub terminal: Terminal<CrosstermBackend<Stderr>>,
    panic_restored: Arc<AtomicBool>,
    _panic_hook: PanicHookGuard,
}

impl TerminalGuard {
    pub fn enter() -> Result<Self> {
        let panic_restored = Arc::new(AtomicBool::new(false));
        let panic_hook = PanicHookGuard::install(Arc::clone(&panic_restored));
        enable_raw_mode().context("could not enable terminal input")?;
        let result = (|| {
            let mut stderr = io::stderr();
            execute!(stderr, EnterAlternateScreen, EnableBracketedPaste, Hide)?;
            let mut terminal = Terminal::new(CrosstermBackend::new(stderr))?;
            terminal.clear()?;
            Ok(Self {
                terminal,
                panic_restored,
                _panic_hook: panic_hook,
            })
        })();
        if result.is_err() {
            restore();
        }
        result
    }

    pub fn was_restored_after_panic(&self) -> bool {
        self.panic_restored.load(Ordering::Acquire)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        restore();
    }
}

fn restore() {
    // Восстановление идемпотентно: panic hook возвращает терминал до печати panic,
    // а Drop страхует обычный выход и ошибки создания backend.
    let _ = disable_raw_mode();
    let _ = execute!(
        io::stderr(),
        DisableBracketedPaste,
        SetAttribute(Attribute::Reset),
        ResetColor,
        Show,
        LeaveAlternateScreen
    );
}

type PanicHook = Box<dyn Fn(&PanicHookInfo<'_>) + Send + Sync + 'static>;

struct PanicHookGuard {
    previous: Option<Arc<PanicHook>>,
}

impl PanicHookGuard {
    fn install(panic_restored: Arc<AtomicBool>) -> Self {
        let previous = Arc::new(std::panic::take_hook());
        let forwarded = Arc::clone(&previous);
        std::panic::set_hook(Box::new(move |info| {
            // Panic вложенного session worker может быть перехвачен runtime.
            // UI всё равно должен прекратить redraw после возврата main screen.
            panic_restored.store(true, Ordering::Release);
            restore();
            forwarded(info);
        }));
        Self {
            previous: Some(previous),
        }
    }
}

impl Drop for PanicHookGuard {
    fn drop(&mut self) {
        // set_hook нельзя вызывать во время unwinding. Установленный hook уже
        // восстановил экран; процесс всё равно завершает текущую UI-сессию.
        if std::thread::panicking() {
            return;
        }
        drop(std::panic::take_hook());
        if let Some(previous) = self.previous.take()
            && let Ok(previous) = Arc::try_unwrap(previous)
        {
            std::panic::set_hook(previous);
        }
    }
}

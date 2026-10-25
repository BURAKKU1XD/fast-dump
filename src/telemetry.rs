use std::io::Write;
use std::sync::Arc;
use std::sync::Once;
use std::sync::atomic::Ordering;

use log::LevelFilter;

static LOGGER_INIT: Once = Once::new();

// Единая точка инициализации логирования нужна, чтобы `--debug` одинаково
// включал наблюдаемость и в бинарнике, и в интеграционных сценариях.
pub fn init_logging(enabled: bool) {
    init_logging_with_progress(enabled, None);
}

pub(crate) fn init_logging_with_progress(
    enabled: bool,
    progress: Option<Arc<crate::progress::ProgressHub>>,
) {
    if !enabled && progress.is_none() {
        return;
    }

    LOGGER_INIT.call_once(|| {
        let mut builder = env_logger::Builder::new();
        builder
            .filter_level(if enabled {
                LevelFilter::Debug
            } else {
                LevelFilter::Warn
            })
            .format_timestamp_millis()
            .format(move |buf, record| {
                if let Some(hub) = progress
                    .as_ref()
                    .filter(|hub| hub.ui_enabled.load(Ordering::Acquire))
                {
                    hub.log(record.level(), &record.args().to_string());
                    return Ok(());
                }
                let thread = std::thread::current();
                let thread_name = thread.name().unwrap_or("unnamed");
                writeln!(
                    buf,
                    "{} {:<5} [{} {}] {}",
                    buf.timestamp_millis(),
                    record.level(),
                    record.module_path().unwrap_or("fast_dump"),
                    thread_name,
                    record.args()
                )
            });

        let _ = builder.try_init();
    });
}

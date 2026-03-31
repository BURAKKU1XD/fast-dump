use std::io::Write;
use std::sync::Once;

use log::LevelFilter;

static LOGGER_INIT: Once = Once::new();

// Единая точка инициализации логирования нужна, чтобы `--debug` одинаково
// включал наблюдаемость и в бинарнике, и в интеграционных сценариях.
pub fn init_logging(enabled: bool) {
    if !enabled {
        return;
    }

    LOGGER_INIT.call_once(|| {
        let mut builder = env_logger::Builder::new();
        builder
            .filter_level(LevelFilter::Debug)
            .format_timestamp_millis()
            .format(|buf, record| {
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

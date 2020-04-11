use crate::error::{gs_error, GraphSurgeError};
use chrono::Local;
use log::{Level, Log, Metadata, Record};

struct GSLogger {
    level: Level,
}

impl Log for GSLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.level
    }

    #[allow(clippy::print_stdout)]
    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!(
                "{} {:<5} {}",
                Local::now().format("%Y-%m-%d %H:%M:%S%.6f"),
                record.level().to_string(),
                record.args()
            );
        }
    }

    fn flush(&self) {}
}

pub fn init_logger_with_level(level: Level) -> Result<(), GraphSurgeError> {
    let logger = GSLogger { level };
    log::set_boxed_logger(Box::new(logger))
        .map_err(|e| gs_error(format!("Could not set logger: {}", e)))?;
    log::set_max_level(level.to_level_filter());
    Ok(())
}

extern crate slog_term;
extern crate chrono;

use std::{io, thread};
use slog::{Logger, Drain};

const TIMESTAMP_FORMAT: &'static str = "%Y-%m-%d %H:%M:%S%.3f";

pub fn create_logger(name: &str) -> Logger {
    let logger_name = name.to_string();
    let custom_format = move |io: &mut dyn io::Write| -> io::Result<()> {
        write!(io,
            "{} {:?} {}",
            chrono::Utc::now().format(TIMESTAMP_FORMAT),
            thread::current().id(),
            logger_name,
        )
    };

    let decorator = slog_term::PlainSyncDecorator::new(io::stdout());
    let drain = slog_term::FullFormat::new(decorator)
        .use_custom_timestamp(custom_format)
        .build()
        .fuse();

    let logger = Logger::root(drain, o!());
    logger
}



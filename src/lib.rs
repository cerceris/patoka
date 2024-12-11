#[macro_use]
extern crate slog;

use actix::prelude::*;
use clap::{App, Arg, crate_version};

use crate::{
    core::{env, app_state},
    worker::{dispatcher, router, processor, task_tree},
};

pub mod center;
#[macro_use]
pub mod control;
pub mod core;
pub mod storage;
#[macro_use]
pub mod worker;
pub mod transport;
pub mod utils;

pub fn run_app<F>(app_name: &str, run_tasks: F)
where
    F: FnOnce() + 'static
{
    let matches = App::new(app_name)
        .version(crate_version!())
        .arg(Arg::with_name("config")
            .short('c')
            .long("config")
            .value_name("FILE")
            .help("Configuration file")
            .takes_value(true)
        )
        .get_matches();

    let config = matches.value_of("config").unwrap_or("cfg/patoka.toml");
    if let Err(_) = env::load(config) {
        std::process::exit(0);
    }

    let system = System::new();

    system.block_on(async {
        app_state::start();
        dispatcher::start();
        router::start();
        task_tree::start();
        processor::start();
        center::router::start();
        run_tasks();
    });

    system.run();
}

use actix::prelude::*;
use bb8;
use bb8_postgres::PostgresConnectionManager;
use lazy_static::lazy_static;
use num_cpus;
use slog::Logger;
use std::{
    str::FromStr,
    sync::{Mutex, RwLock}
};
use tokio_postgres;

use crate::{
    core::{arbiter_pool, logger::create_logger},
    env,
};

pub type Pool = bb8::Pool<PostgresConnectionManager<tokio_postgres::NoTls>>;

pub struct DbExecutor {
    pub pool: Pool,
    pub log: Logger,
}

impl DbExecutor {
    pub fn new(pool: Pool, log: Logger) -> Self {
        Self {
            pool,
            log
        }
    }
}

lazy_static! {
    static ref DB_EXECUTOR_POOL: DbExecutorPool = DbExecutorPool::new();

    static ref DB_POOL: RwLock<Option<Pool>> = RwLock::new(None);
}

impl Actor for DbExecutor {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Started.");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Stopped.");
    }
}

pub fn run() -> Addr<DbExecutor> {
    DB_EXECUTOR_POOL.next()
}

pub async fn init() {
    let db_config: String = env::get_var("app.db").parse().unwrap();
    let cfg = tokio_postgres::config::Config::from_str(&db_config).unwrap();
    let manager = PostgresConnectionManager::new(cfg, tokio_postgres::NoTls);
    let pool = Pool::builder().build(manager).await.unwrap();
    *DB_POOL.write().unwrap() = Some(pool);
}

pub struct DbExecutorPool {
    executors: Vec<Addr<DbExecutor>>,
    capacity: usize,
    next_to_use: Mutex<usize>,
    log: Logger,
}

impl DbExecutorPool {
    pub fn new() -> Self {
        let log = create_logger("db_executor_pool");
        let capacity = num_cpus::get();

        let pp = &*DB_POOL.read().unwrap();
        let p = pp.as_ref().unwrap();

        let mut executors = Vec::new();
        for i in 0..capacity {
            let pool = p.clone();
            let log = create_logger(&format!("db_executor_{}", i));

            executors.push(
                DbExecutor::start_in_arbiter(
                    &arbiter_pool::next(),
                    move |_| { DbExecutor::new(pool, log) },
                )
            );
        }

        Self {
            executors,
            capacity,
            next_to_use: Mutex::new(0),
            log,
        }
    }

    pub fn next(&self) -> Addr<DbExecutor> {
        let mut n = self.next_to_use.lock().unwrap();
        let i: usize = *n;

        *n = if i + 1 >= self.capacity { 0 } else { i + 1};

        self.executors[i].clone()
    }
}

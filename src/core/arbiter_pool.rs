use actix::prelude::*;
use lazy_static::lazy_static;
use num_cpus;
use slog::Logger;
use std::sync::Mutex;

use crate::core::logger::create_logger;

lazy_static! {
    static ref ARBITER_POOL: Mutex<ArbiterPool> =
        Mutex::new(ArbiterPool::new());
}

struct ArbiterPool {
    arbiters: Vec<Arbiter>,
    next_to_use: usize,
    log: Logger,
}

impl ArbiterPool {
    pub fn new() -> Self {
        let mut arbiter_pool = ArbiterPool {
            arbiters: Vec::new(),
            next_to_use: 0,
            log: create_logger("arbiter_pool"),
        };

        arbiter_pool.launch(num_cpus::get());

        arbiter_pool
    }

    pub fn launch(&mut self, size: usize) {
        for _i in 0..size {
            let addr = Arbiter::new();
            self.arbiters.push(addr);
        }

        info!(self.log, "Created {} arbiters.", self.arbiters.len());
    }

    pub fn next(&mut self) -> ArbiterHandle {
        let arb = &self.arbiters[self.next_to_use];

        self.next_to_use += 1;
        if self.next_to_use >= self.arbiters.len() {
            self.next_to_use = 0;
        }

        arb.handle()
    }
}

pub fn next() -> ArbiterHandle {
    let mut arbiter_pool = ARBITER_POOL.lock().unwrap();
    arbiter_pool.next()
}

use actix::prelude::*;
use slog::Logger;
use std::fmt;

use crate::worker::{
    plugin::WorkerPlugin,
    reprocessor::{self, WorkerReady, TaskReprocessor},
};

#[derive(Clone, PartialEq, Copy)]
pub enum WS {
    /// Worker process is starting.
    Starting,

    /// Worker is preparing for its job.
    Preparing,

    /// Worker is ready to accept a new task.
    Ready,

    /// Worker is executing a task.
    Busy,

    /// Worker process it terminating.
    Exiting,

    /// Error occured in the worker.
    Error,

    /// The default state at the moment of creation.
    Initial,
}

impl WS {
    pub fn as_str(ws: &WS) -> &'static str {
        match ws {
            WS::Starting => "starting",
            WS::Preparing => "preparing",
            WS::Ready => "ready",
            WS::Busy => "busy",
            WS::Exiting => "exiting",
            WS::Error => "error",
            WS::Initial => "initial",
        }
    }
}

impl fmt::Debug for WS {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", WS::as_str(&self))
    }
}

pub struct WorkerState {
    id: String,
    current_state: WS,
    plugin: WorkerPlugin,
    log: Logger,
    task_reprocessor: Addr<TaskReprocessor>,
}

impl WorkerState {
    pub fn new(id: String, log: Logger) -> Self {
        Self {
            id,
            current_state: WS::Initial,
            plugin: WorkerPlugin::None,
            log,
            task_reprocessor: reprocessor::start(),
        }
    }

    pub fn current_state(&self) -> WS {
        self.current_state
    }

    pub fn is_starting(&self) -> bool {
        self.is(WS::Starting)
    }

    pub fn is_preparing(&self) -> bool {
        self.is(WS::Preparing)
    }

    pub fn is_ready(&self) -> bool {
        self.is(WS::Ready)
    }

    pub fn is_busy(&self) -> bool {
        self.is(WS::Busy)
    }

    pub fn is_exiting(&self) -> bool {
        self.is(WS::Exiting)
    }

    pub fn is_error(&self) -> bool {
        self.is(WS::Error)
    }

    pub fn is_initial(&self) -> bool {
        self.is(WS::Initial)
    }

    pub fn starting(&mut self) {
        self.set(WS::Starting);
    }

    pub fn preparing(&mut self) {
        self.set(WS::Preparing);
    }

    pub fn ready(&mut self) {
        self.set(WS::Ready);
        self.task_reprocessor.do_send(WorkerReady {
            worker_id: self.id.clone(),
        });
    }

    pub fn busy(&mut self) {
        self.set(WS::Busy);
    }

    pub fn exiting(&mut self) {
        self.set(WS::Exiting);
    }

    pub fn error(&mut self) {
        self.set(WS::Error);
    }

    pub fn initial(&mut self) {
        self.set(WS::Initial);
    }

    fn is(&self, state: WS) -> bool {
        self.current_state == state
    }

    fn set(&mut self, state: WS) {
        if self.current_state == state {
            return;
        }
        debug!(self.log, "[STATE] ({:?}) => ({:?})", self.current_state, state);
        self.current_state = state;
    }

    pub fn is_plugin(&self, plugin: WorkerPlugin) -> bool {
        self.plugin == plugin
    }

    pub fn plugin(&mut self, plugin: WorkerPlugin) {
        if self.plugin == plugin {
            return;
        }
        debug!(self.log, "[PLUGIN] ({:?}) => ({:?})", self.plugin, plugin);
        self.plugin = plugin;
    }
}

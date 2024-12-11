use actix::prelude::*;
use slog::Logger;
use std::collections::HashMap;

use crate::{
    core::logger::create_logger,
    worker::{
        controller::{WorkerController},
        backend_connector::{self, WorkerBackendConnector},
        worker_message::*,
    },
    transport::message::*,
};

pub struct RegisterController {
    pub controller_id: String,
    pub controller_addr: Addr<WorkerController>,
}

impl Message for RegisterController {
    type Result = ();
}

pub struct TaskDispatcher {
    log: Logger,
    router_addr: Addr<WorkerBackendConnector>,
    controllers: HashMap<String, Addr<WorkerController>>
}

impl TaskDispatcher {
    fn send_to_controller(&self, msg: WorkerMessage) {
        if let Some(addr) = self.controllers.get(&msg.payload.worker_id) {
            addr.do_send(msg);
        } else {
            warn!(
                self.log,
                "Unable to send a message to an unregistered controller \
                    [WORKER ID] {}",
                msg.payload.worker_id,
            );
        }
    }
}

impl Default for TaskDispatcher {
    fn default() -> Self {
        Self {
            log: create_logger("task_dispatcher"),
            router_addr: backend_connector::start(),
            controllers: HashMap::new(),
        }
    }
}

impl Actor for TaskDispatcher {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Task Dispatcher started.");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Task Dispatcher stopped.");
    }
}

impl Supervised for TaskDispatcher {}

impl SystemService for TaskDispatcher {
    fn service_started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Task Dispatcher system service started.")
    }
}

impl Handler<RawMessage> for TaskDispatcher {
    type Result = ();

    fn handle(
        &mut self,
        msg: RawMessage,
        _ctx: &mut Self::Context
    ) -> Self::Result {

        match RawMessage::to::<WorkerMessagePayload>(msg) {
            Ok(worker_message) => {
                /*trace!(
                    self.log,
                    "Received a worker message: {}",
                    worker_message.payload.header()
                );*/

                match worker_message.payload.dest {
                    Dest::Controller | Dest::Client => {
                        self.send_to_controller(worker_message);
                    },
                    Dest::Worker => {
                        warn!(self.log, "Not expecting dest Worker.");
                    }
                    _ => {
                        warn!(self.log, "Unknown message dest.");
                    }
                }
            },
            Err(e) => {
                warn!(self.log, "Invalid raw worker message: {}", e);
            }
        }

    }
}

impl Handler<WorkerMessage> for TaskDispatcher {
    type Result = ();

    fn handle(
        &mut self,
        msg: WorkerMessage,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        match msg.payload.dest {
            Dest::Controller | Dest::Client => {
                self.send_to_controller(msg);
            },
            Dest::Worker => {
                self.router_addr.do_send(RawMessage::from(msg));
            },
            _ => {
                warn!(self.log, "Unknown message dest.");
            }
        }
    }
}

impl Handler<RegisterController> for TaskDispatcher {
    type Result = ();

    fn handle(
        &mut self,
        msg: RegisterController,
        _ctx: &mut Self::Context
    ) -> Self::Result {

        info!(self.log, "Registering [CONTROLLER ID] {}.", msg.controller_id);

        self.controllers.insert(msg.controller_id, msg.controller_addr);
    }
}

pub fn start() -> Addr<TaskDispatcher> {
    TaskDispatcher::from_registry()
}


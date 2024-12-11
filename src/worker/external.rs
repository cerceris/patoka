use actix::prelude::*;
use slog::Logger;

use crate::core::logger::create_logger;
use crate::worker::external_message::*;
use crate::worker::worker_message::*;
use crate::transport::message::*;

pub struct ExternalDispatcher {
    log: Logger,
    //router_addr: Addr<ExternalBackendConnector>,
}

impl ExternalDispatcher {
}

impl Default for ExternalDispatcher {
    fn default() -> Self {
        Self {
            log: create_logger("external_dispatcher"),
            //router_addr: start_external_backend_connector(),
        }
    }
}

impl Actor for ExternalDispatcher {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "External Dispatcher started.");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "External Dispatcher stopped.");
    }
}

impl Supervised for ExternalDispatcher {}

impl SystemService for ExternalDispatcher {
    fn service_started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "External Dispatcher system service started.")
    }
}

impl Handler<RawMessage> for ExternalDispatcher {
    type Result = ();

    fn handle(
        &mut self,
        msg: RawMessage,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        match RawMessage::to::<ExternalMessagePayload>(msg) {
            Ok(external_message) => {
                /*trace!(
                    self.log,
                    "Received an external message: {}",
                    external_message.payload.header()
                );*/

                match external_message.payload.dest {
                    Dest::ExternalIn => {
                        //self.send_to_controller(worker_message);
                    },
                    Dest::ExternalOut => {
                        //warn!(self.log, "Not expecting dest Worker.");
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

impl Handler<ExternalMessage> for ExternalDispatcher {
    type Result = ();

    fn handle(
        &mut self,
        msg: ExternalMessage,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        match msg.payload.dest {
            Dest::ExternalIn => {
                //self.send_to_controller(msg);
            },
            Dest::ExternalOut => {
                //self.router_addr.do_send(RawWorkerMessage::from(msg));
            },
            _ => {
                warn!(self.log, "Unknown message dest.");
            }
        }
    }
}

pub struct RegularUpdateMessage {
}

pub struct KeepAliveRequestMessage {
}

pub struct KeepAliveResponseMessage {
}

pub fn start() -> Addr<ExternalDispatcher> {
    let addr = ExternalDispatcher::from_registry();
    addr
}

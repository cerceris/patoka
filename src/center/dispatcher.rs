use actix::prelude::*;
use slog::Logger;
use std::collections::HashMap;

use crate::{
    center::{
        connector::{self, CenterConnector},
        message::*
    },
    control::{
        message::*,
        registry::{self, *},
    },
    core::logger::create_logger,
    transport::message::*,
};

pub struct RegisterEntity {
    pub entity_id: String,
    pub entity_addr: Recipient<CenterMessage>,
}

impl Message for RegisterEntity {
    type Result = ();
}

pub struct CenterDispatcher {
    log: Logger,
    router_addr: Addr<CenterConnector>,
    entities: HashMap<String, Recipient<CenterMessage>>,
    control_registry_addr: Addr<ControlRegistry>,
}

impl CenterDispatcher {
    fn send_to_entity(&self, msg: CenterMessage) {
        if let Some(addr) = self.entities.get(&msg.payload.entity_id) {
            addr.do_send(msg);
        } else {
            warn!(
                self.log,
                "Unable to send a message to an unregistered [ENTITY ID] {}",
                msg.payload.entity_id,
            );
        }
    }

    fn handle_control_msg(&self, msg: ControlMessage) {
        self.control_registry_addr.do_send(msg);
    }
}

impl Default for CenterDispatcher {
    fn default() -> Self {
        Self {
            log: create_logger("center_dispatcher"),
            router_addr: connector::start(),
            entities: HashMap::new(),
            control_registry_addr: registry::start(),
        }
    }
}

impl Actor for CenterDispatcher {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Center Dispatcher started.");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Center Dispatcher stopped.");
    }
}

impl Supervised for CenterDispatcher {}

impl SystemService for CenterDispatcher {
    fn service_started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Center Dispatcher system service started.")
    }
}

impl Handler<RawMessage> for CenterDispatcher {
    type Result = ();

    fn handle(
        &mut self,
        msg: RawMessage,
        _ctx: &mut Self::Context
    ) -> Self::Result {

        match RawMessage::to::<CenterMessagePayload>(msg) {
            Ok(center_message) => {
                trace!(
                    self.log,
                    "Received a center message: {}",
                    center_message.payload.header()
                );

                match center_message.payload.dest {
                    Dest::App => {
                        match center_message.payload.subject {
                            Subject::Control => {
                                /*trace!(
                                    self.log,
                                    "{:?}",
                                    center_message
                                );*/

                                self.handle_control_msg(
                                    serde_json::from_value(
                                        center_message.payload.data
                                    ).unwrap()
                                );
                            },
                            _ => {
                                self.send_to_entity(center_message);
                            }
                        }
                    },
                    Dest::Center => {
                        warn!(self.log, "Not expecting dest Center.");
                    }
                    _ => {
                        warn!(self.log, "Unknown message dest.");
                    }
                }
            },
            Err(e) => {
                warn!(self.log, "Invalid raw center message: {}", e);
            }
        }
    }
}

impl Handler<CenterMessage> for CenterDispatcher {
    type Result = ();

    fn handle(
        &mut self,
        msg: CenterMessage,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        if msg.payload.subject == Subject::Control {
            self.handle_control_msg(
                serde_json::from_value(msg.payload.data).unwrap()
            );

            return;
        }

        match msg.payload.dest {
            Dest::App => {
                self.send_to_entity(msg);
            },
            Dest::Center => {
                self.router_addr.do_send(RawMessage::from(msg));
            },
            _ => {
                warn!(self.log, "Unknown message dest.");
            }
        }
    }
}

impl Handler<RegisterEntity> for CenterDispatcher {
    type Result = ();

    fn handle(
        &mut self,
        msg: RegisterEntity,
        _ctx: &mut Self::Context
    ) -> Self::Result {

        info!(self.log, "Registering [ENTITY ID] {}.", msg.entity_id);

        self.entities.insert(msg.entity_id, msg.entity_addr);
    }
}

pub fn start() -> Addr<CenterDispatcher> {
    CenterDispatcher::from_registry()
}

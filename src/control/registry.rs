use actix::prelude::*;
use slog::Logger;
use std::collections::HashMap;

use crate::{
    center::{
        connector::{self, CenterConnector},
        message::*,
    },
    control::message::*,
    core::logger::create_logger,
    transport::message::*,
};

pub struct RegisterEntity {
    pub entity_id: String,
    pub entity_addr: Recipient<ControlMessage>,
}

impl Message for RegisterEntity {
    type Result = ();
}

pub struct ControlRegistry {
    log: Logger,
    router_addr: Addr<CenterConnector>,
    entities: HashMap<String, Recipient<ControlMessage>>
}

impl ControlRegistry {
    fn send_to_entity(&self, msg: ControlMessage) {
        let dest_id = msg.dest();

        if let Some(addr) = self.entities.get(dest_id) {
            addr.do_send(msg);
        } else {
            warn!(
                self.log,
                "Unable to send a message to an unregistered [ENTITY ID] {}",
                dest_id,
            );
        }
    }
}

impl Default for ControlRegistry {
    fn default() -> Self {
        Self {
            log: create_logger("control_registry"),
            router_addr: connector::start(),
            entities: HashMap::new(),
        }
    }
}

impl Actor for ControlRegistry {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Control Registry started.");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Control Registry stopped.");
    }
}

impl Supervised for ControlRegistry {}

impl SystemService for ControlRegistry {
    fn service_started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Control Registry system service started.")
    }
}

impl Handler<ControlMessage> for ControlRegistry {
    type Result = ();

    fn handle(
        &mut self,
        msg: ControlMessage,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        self.send_to_entity(msg);
    }
}

impl Handler<RegisterEntity> for ControlRegistry {
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

pub fn register(entity_id: String, entity_addr: Recipient<ControlMessage>) {
    start().do_send(
        RegisterEntity {
            entity_id,
            entity_addr,
        }
    );
}

pub fn send(msg: ControlMessage) {
    start().do_send(msg);
}

pub fn start() -> Addr<ControlRegistry> {
    ControlRegistry::from_registry()
}

use actix::prelude::*;
use slog::Logger;
use std::collections::HashMap;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

use crate::{
    core::logger::create_logger,
    transport::{
        message::RawMessage,
        router::RawMessageRecipient,
    },
};

type ArcAtomicBool = Arc<AtomicBool>;

pub enum RegistryValue {
    /// The router's `running` property.
    Running(ArcAtomicBool),

    /// Connector to either the router's backend or frontend.
    Connector(RawMessageRecipient),
}

pub struct RegisterRouterControlLinkMessage {
    /// The router's BE address.
    pub address: String,

    /// Link used to communicate with the router.
    pub control_link: RegistryValue,
}

impl Message for RegisterRouterControlLinkMessage {
    type Result = ();
}

pub struct RouterRegistry {
    log: Logger,
    running_map: HashMap<String, ArcAtomicBool>,
    connectors: HashMap<String, RawMessageRecipient>,
}

impl Default for RouterRegistry {
    fn default() -> Self {
        Self {
            log: create_logger("router_registry"),
            running_map: HashMap::new(),
            connectors: HashMap::new(),
        }
    }
}

impl Actor for RouterRegistry {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Router Registry started.");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Router Registry stopped.");
    }
}

impl Supervised for RouterRegistry {}

impl SystemService for RouterRegistry {
    fn service_started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Router Registry system service started.")
    }
}

impl Handler<RegisterRouterControlLinkMessage> for RouterRegistry {
    type Result = ();

    fn handle(
        &mut self,
        msg: RegisterRouterControlLinkMessage,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        match msg.control_link {
            RegistryValue::Running(running) => {
                info!(
                    self.log,
                    "Register 'running' for [ROUTER ADDRESS] {}",
                    msg.address,
                );
                self.running_map.insert(msg.address, running);
            },
            RegistryValue::Connector(connector) => {
                info!(
                    self.log,
                    "Register connector for [ROUTER ADDRESS] {}",
                    msg.address,
                );
                self.connectors.insert(msg.address, connector);
            }
        }
    }
}

pub struct StopRouterMessage {
    pub address: String,
}

impl Message for StopRouterMessage {
    type Result = ();
}

impl Handler<StopRouterMessage> for RouterRegistry {
    type Result = ();

    fn handle(
        &mut self,
        msg: StopRouterMessage,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        if let Some(running) = self.running_map.get(&msg.address) {
            info!(
                self.log,
                "Stopping [ROUTER ADDRESS] {}",
                msg.address,
            );
            running.store(false, Ordering::Relaxed);
        }
        if let Some(connector) = self.connectors.get(&msg.address) {
            // Send a message to "wake up" the router.
            info!(
                self.log,
                "Sending message to stop [ROUTER ADDRESS] {}",
                msg.address,
            );
            connector.do_send(RawMessage::dummy());
        }
    }
}

pub fn start() -> Addr<RouterRegistry> {
    RouterRegistry::from_registry()
}

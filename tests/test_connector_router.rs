use patoka;
use actix;
#[macro_use]
extern crate slog;
use lazy_static::lazy_static;
use slog::Logger;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}, Mutex};
use std::{thread, time::Duration};

use actix::prelude::*;
use patoka::core::logger::create_logger;
use patoka::transport::connector::*;
use patoka::transport::message::*;
use patoka::transport::router::*;
use patoka::transport::router_registry;
use patoka::transport::router_registry::*;

lazy_static! {
    pub static ref TEST_STR: Mutex<String>  = Mutex::new(String::new());
}

struct BackendConnectorParameters;

/// Connects to the router's BE.
impl ConnectorParameters for BackendConnectorParameters {
    fn name() -> &'static str {
        "backend_connector"
    }

    fn router() -> &'static str {
        "inproc://router_be"
    }
}

type BackendConnector = Connector<BackendConnectorParameters>;

struct DispatcherA {
    log: Logger,
}

impl Default for DispatcherA {
    fn default() -> Self {
        Self {
            log: create_logger("dispatcher_a"),
        }
    }
}

impl Supervised for DispatcherA {}

impl Actor for DispatcherA {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "DispatcherA started.");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "DispatcherA stopped.");
    }
}

impl SystemService for DispatcherA {
    fn service_started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "DispatcherA system service started.");
    }
}

impl Handler<RawMessage> for DispatcherA {
    type Result = ();

    fn handle(
        &mut self,
        msg: RawMessage,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        info!(self.log, "Received raw message: {}", msg.body);

        if msg.body == "pingpongping" {
            let mut test_str = TEST_STR.lock().unwrap();
            *test_str += &msg.body;
        }

        if msg.body == "stop" || msg.body == "pingpongping" {
            info!(self.log, "Send STOP command to the routers.");

            router_registry::start().do_send(StopRouterMessage {
                address: "inproc://router_be".to_string(),
            });

            router_registry::start().do_send(StopRouterMessage {
                address: "inproc://router_be_active".to_string(),
            });

            System::current().stop();
            return;
        }

        if msg.body == "ping" {
            let be_addr = BackendConnector::from_registry();
            be_addr.do_send(
                RawMessage {
                    identity: msg.identity,
                    body: msg.body + "pong",
                }
            );

            return;
        }

        let mut test_str = TEST_STR.lock().unwrap();
        *test_str += &msg.body;
    }
}

struct FrontendConnectorParameters;

/// Connects to the router's FE using the active router as mediator.
impl ConnectorParameters for FrontendConnectorParameters {
    fn name() -> &'static str {
        "frontend_connector"
    }

    fn router() -> &'static str {
        "inproc://router_be_active"
    }
}

type FrontendConnector = Connector<FrontendConnectorParameters>;

struct DispatcherB {
    log: Logger,
}

impl Default for DispatcherB {
    fn default() -> Self {
        Self {
            log: create_logger("dispatcher_b"),
        }
    }
}

impl Supervised for DispatcherB {}

impl Actor for DispatcherB {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "DispatcherB started.");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "DispatcherB stopped.");
    }
}

impl SystemService for DispatcherB {
    fn service_started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "DispatcherB system service started.");
    }
}

impl Handler<RawMessage> for DispatcherB {
    type Result = ();

    fn handle(
        &mut self,
        msg: RawMessage,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        info!(self.log, "Received raw message: {}", msg.body);

        let fe_addr = FrontendConnector::from_registry();
        fe_addr.do_send(RawMessage::with_body(&(msg.body + "ping")));
    }
}

fn reset_test_str() {
    let mut test_str = TEST_STR.lock().unwrap();
    *test_str = String::new();
}

#[test]
fn test_frontend() {

    reset_test_str();

    let system = System::new();

    system.block_on(async {
        let dispatchera_addr = DispatcherA::from_registry().into();

        // A regular (passive) router. Listens on the FE and BE.
        MessageRouter::start(
            create_logger("message_router"),
            dispatchera_addr,
            "inproc://router_fe".to_string(),
            "inproc://router_be".to_string(),
            false,
        );

        let dispatcherb_addr = DispatcherB::from_registry().into();

        // An active router. Connects to the FE, listens on the BE.
        MessageRouter::start(
            create_logger("message_router_active"),
            dispatcherb_addr,
            "inproc://router_fe".to_string(),
            "inproc://router_be_active".to_string(),
            true,
        );

        let fe_addr = FrontendConnector::from_registry();

        // Just to register in the registry to stop the router.
        let be_addr = BackendConnector::from_registry();

        fe_addr.do_send(RawMessage::with_body("aaa"));
        fe_addr.do_send(RawMessage::with_body("bbb"));
        fe_addr.do_send(RawMessage::with_body("ccc"));
        fe_addr.do_send(RawMessage::with_body("stop"));

    });

    system.run();

    let test_str = TEST_STR.lock().unwrap();
    assert_eq!(*test_str, "aaabbbccc");
}

#[test]
fn test_full() {
    // Two-way communication.

    reset_test_str();

    let system = System::new();

    system.block_on(async {
        let dispatchera_addr = DispatcherA::from_registry().into();

        // A regular (passive) router. Listens on the FE and BE.
        MessageRouter::start(
            create_logger("message_router"),
            dispatchera_addr,
            "inproc://router_fe".to_string(),
            "inproc://router_be".to_string(),
            false,
        );

        let dispatcherb_addr = DispatcherB::from_registry().into();

        // An active router. Connects to the FE, listens on the BE.
        MessageRouter::start(
            create_logger("message_router_active"),
            dispatcherb_addr,
            "inproc://router_fe".to_string(),
            "inproc://router_be_active".to_string(),
            true,
        );

        let fe_addr = FrontendConnector::from_registry();

        // Just to register in the registry to stop the router.
        let be_addr = BackendConnector::from_registry();

        fe_addr.do_send(RawMessage::with_body("ping"));

    });

    system.run();

    let test_str = TEST_STR.lock().unwrap();
    assert_eq!(*test_str, "pingpongping");
}

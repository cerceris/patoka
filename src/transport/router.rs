use actix::prelude::*;
use lazy_static::lazy_static;
use slog::Logger;

use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::thread;

use zmq;

use crate::transport::{
    message::{Identity, RawMessage},
    router_registry::{self, *},
};

pub type RawMessageRecipient = Recipient<RawMessage>;

lazy_static! {
    pub static ref CONTEXT: zmq::Context = zmq::Context::new();
}

pub struct MessageRouter {
    log: Logger,
    dispatcher_addr: RawMessageRecipient,
    frontend_address: String,
    backend_address: String,
    running: Arc<AtomicBool>,

    /// If `true`, connect to the `frontend_address`. Otherwise, listen on it.
    active_mode: bool,
}

impl MessageRouter {

    pub fn start(
        log: Logger,
        dispatcher_addr: Recipient<RawMessage>,
        frontend_address: String,
        backend_address: String,
        active_mode: bool,
    ) {
        let mut router = MessageRouter::new(
            log,
            dispatcher_addr,
            frontend_address,
            backend_address,
            active_mode,
        );

        // Register `running` to make itself controllable from outside.
        let registry_addr = router_registry::start();

        registry_addr.do_send(RegisterRouterControlLinkMessage {
            address: router.backend_address.clone(),
            control_link: RegistryValue::Running(router.running.clone()),
        });

        thread::spawn(move || {
            router.start_internal();
        });
    }

    pub fn new(
        log: Logger,
        dispatcher_addr: Recipient<RawMessage>,
        frontend_address: String,
        backend_address: String,
        active_mode: bool,
    ) -> Self {
        Self {
            log,
            dispatcher_addr,
            frontend_address,
            backend_address,
            running: Arc::new(AtomicBool::new(true)),
            active_mode,
        }
    }

    fn start_internal(&mut self) {
        let fe_type = if self.active_mode { zmq::DEALER } else { zmq::ROUTER };
        let frontend_socket = CONTEXT.socket(fe_type).unwrap();
        let backend_socket = CONTEXT.socket(zmq::ROUTER).unwrap();

        if self.active_mode {
            match frontend_socket.connect(&self.frontend_address) {
                Ok(_) => {
                    info!(
                        self.log,
                        "Connected to [FRONTEND ADDRESS] {}.",
                        &self.frontend_address,
                    );
                },
                Err(_) => {
                    error!(
                        self.log,
                        "Failed to connect to [FRONTEND ADDRESS] {}.",
                        &self.frontend_address,
                    );

                    return;
                }

            };

        } else {
            info!(
                self.log,
                "Bind to [FRONTEND ADDRESS] {}",
                &self.frontend_address
            );

            frontend_socket.bind(&self.frontend_address)
                .expect("Failed to bind router FE");
        }

        info!(self.log, "Bind to [BACKEND ADDRESS] {}", &self.backend_address);

        backend_socket.bind(&self.backend_address)
            .expect("Failed to bind router BE");

        info!(self.log, "Message Router started.");

        loop {
            let mut items = [
                frontend_socket.as_poll_item(zmq::POLLIN),
                backend_socket.as_poll_item(zmq::POLLIN),
            ];

            let rc = zmq::poll(&mut items, -1).unwrap();

            if rc == -1 || !self.running.load(Ordering::Relaxed) {
                info!(self.log, "Exiting loop.");
                break;
            }

            if items[0].is_readable() {
                // Active router has the FE of type DEALER.
                // DEALER has no identity part.
                let identity = if self.active_mode { Identity::new() }
                    else { frontend_socket.recv_msg(0).unwrap() };

                //trace!(self.log, "[FE] Identity: {:?}.", identity);

                let mut body_msg = frontend_socket.recv_msg(0).unwrap();
                let mut more = body_msg.get_more();
                if more {
                    // Skip the previous part since it is likely a
                    // `RawMessage`.`identity` which is irrelevant for
                    // the FE. This is the case, for example, when `Connector`
                    // communicates with the router through the chain:
                    // connector <-> BE active router FE <-> FE this router BE.
                    // 2020-03-07: Should not happen. See the BE section below.
                    body_msg = frontend_socket.recv_msg(0).unwrap();
                    assert!(false);
                }

                more = body_msg.get_more();
                if more {
                    warn!(self.log, "[FE] Expecting more data.");
                    assert!(false);
                }

                if let Some(body) = body_msg.as_str() {
                    //debug!(self.log, "[FE] Body:\n\n'{}'\n", body);

                    let msg = RawMessage::new(identity, body);
                    self.dispatcher_addr.do_send(msg);
                }
                else {
                    assert!(false);
                }

            }

            if items[1].is_readable() {
                let _connector_identity = backend_socket.recv_msg(0).unwrap();
                /*trace!(
                    self.log,
                    "[BE] Connector Identity: {:?}.",
                    connector_identity
                );*/

                let identity = backend_socket.recv_msg(0).unwrap();
                //trace!(self.log, "[BE] Identity: {:?}.", identity);

                let body_msg = backend_socket.recv_msg(0).unwrap();
                let more = body_msg.get_more();
                if more {
                    warn!(self.log, "[BE] Expecting more data.");
                    assert!(false);
                }

                /*if let Some(body) = body_msg.as_str() {
                    trace!(self.log, "[BE] Body:\n\n'{}'\n", body);
                }*/

                // 2020-03-07: Do not send identity in the active mode.
                if !self.active_mode {
                    frontend_socket.send(identity, zmq::SNDMORE).unwrap();
                }

                frontend_socket.send(body_msg, 0).unwrap();
            }
        }

        info!(self.log, "Message Router stopped.");
    }
}


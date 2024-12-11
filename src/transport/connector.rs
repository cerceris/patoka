use actix::prelude::*;
use slog::Logger;
use std::{marker::PhantomData};

use crate::{
    core::logger::create_logger,
    transport::{
        message::{RawMessage},
        router::CONTEXT,
        router_registry::{self, *},
    },
};

/// Trait is used to create a unique (per process) instance of the connector
/// that is created and managed by Actix as a `SystemService`.
pub trait ConnectorParameters {
    /// Connector name.
    fn name() -> &'static str;

    /// Router ZMQ address.
    fn router() -> &'static str;
}

pub struct Connector<P> {
    log: Logger,
    socket: zmq::Socket,
    phantom: PhantomData<P>,
}

impl<P> Default for Connector<P>
where
    P: 'static + ConnectorParameters
{
    fn default() -> Self {
        Self {
            log: create_logger(P::name()),
            socket: CONTEXT.socket(zmq::DEALER).unwrap(),
            phantom: PhantomData::default(),
        }
    }
}

impl<P> Actor for Connector<P>
where
    P: 'static + ConnectorParameters + Unpin
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(self.log, "Started.");

        // Register itself to be used to control the router.
        let registry_addr = router_registry::start();

        registry_addr.do_send(RegisterRouterControlLinkMessage {
            address: P::router().to_string(),
            control_link: RegistryValue::Connector(ctx.address().into()),
        });

        match self.socket.connect(P::router()) {
            Ok(_) => {
                info!(
                    self.log,
                    "Connected to [ROUTER ADDRESS] {}.",
                    P::router(),
                );
            },
            Err(_) => {
                error!(
                    self.log,
                    "Failed to connect to [ROUTER ADDRESS] {}.",
                    P::router(),
                );
            }
        }
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Stopped.");
    }

}

impl<P> Supervised for Connector<P>
where
    P: 'static + ConnectorParameters + Unpin
{
}

impl<P> SystemService for Connector<P>
where
    P: 'static + ConnectorParameters + Unpin
{
    fn service_started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "System service started.")
    }
}

impl<P> Handler<RawMessage> for Connector<P>
where
    P: 'static + ConnectorParameters + Unpin,
{
    type Result = ();

    /// Sends `msg` to the router.
    fn handle(
        &mut self,
        msg: RawMessage,
        _ctx: &mut Self::Context
    ) -> Self::Result {

        /*trace!(
            self.log,
            "Sending a raw worker message to the router."
        );*/

        self.socket.send(msg.identity, zmq::SNDMORE).unwrap();

        let body_msg = zmq::Message::from(msg.body.as_bytes());
        self.socket.send(body_msg, 0).unwrap();
    }
}


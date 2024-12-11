use crate::{
    core::{env, logger::create_logger},
    transport::router::MessageRouter,
    worker::dispatcher,
};

pub fn start() {
    let router_port = env::get_var("general.router_port");
    let frontend_address = "tcp://*:".to_string() + &router_port;

    let backend_address = "inproc://router".to_string();

    MessageRouter::start(
        create_logger("worker_message_router"),
        dispatcher::start().into(),
        frontend_address,
        backend_address,
        false,
    );
}

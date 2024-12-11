use crate::{
    center::dispatcher,
    core::{env, logger::create_logger},
    transport::router::MessageRouter,
};

pub fn start() {
    let center_addr =
        match env::get_opt_var("center.address") {
            Some(v) => { v },
            None => { String::new() },
        };

    let frontend_address = center_addr;

    let backend_address = "inproc://center_router".to_string();

    MessageRouter::start(
        create_logger("center_message_router"),
        dispatcher::start().into(),
        frontend_address,
        backend_address,
        true,
    );
}

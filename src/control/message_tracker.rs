use actix::prelude::*;
use serde_derive::{Deserialize, Serialize};
use slog::Logger;
use std::collections::HashMap;

use crate::{
    center::send::*,
    control::message::*,
    core::{
        logger::create_logger,
        timestamp::{now, Timestamp},
    },
    worker::tracker::dismiss_task_question,
};

#[derive(Clone)]
pub struct TrackerItem {
    pub request: ControlMessage,

    pub created_at: Timestamp,

    /// `True` if the response is `ok`.
    pub success: bool,

    pub response: Option<ControlMessage>,
}

impl TrackerItem {
    pub fn new(request: ControlMessage) -> Self {
        Self {
            request,
            created_at: now(),
            success: false,
            response: None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ResponseResult {
    pub result: String,
    pub details: String,
}

#[derive(Clone)]
pub struct ControlMessageTracker {
    log: Logger,

    /// Message UUID --> Item
    items: HashMap<String, TrackerItem>,
}

impl ControlMessageTracker {

    pub fn new(task_uuid: String) -> Self {
        Self {
            log: create_logger(&format!("control_tracker_{}", task_uuid)),
            items: HashMap::new(),
        }
    }

    pub fn send_request(
        &mut self,
        msg: ControlMessage,
        addr: &Recipient<ControlMessage>,
    ) {
        debug!(self.log, "[CMD REQ] {:?}", msg);

        let item = TrackerItem::new(msg.clone());

        if let Some(_) = self.items.insert(msg.uuid.clone(), item) {
            panic!("Tried to send a command message multiple times.");
        }

        if msg.cmd == "task_answer" {
            dismiss_task_question(msg.dest_id.clone());
        }

        addr.do_send(msg);
    }

    pub fn handle_response(
        &mut self,
        msg: ControlMessage
    ) -> Result<TrackerItem, &'static str> {

        debug!(self.log, "[CMD RESP] {:?}", msg);

        match self.items.remove(&msg.uuid) {
            Some(mut item) => {
                let result: ResponseResult =
                    serde_json::from_value(msg.data.clone()).unwrap();

                item.success = (result.result == "ok");
                item.response = Some(msg.clone());
                send_control_msg(msg);
                Ok(item)
            },
            _ => {
                Err("Unknown control message [UUID]")
            }
        }
    }

    pub fn clear_unresponded(&mut self) {

    }
}

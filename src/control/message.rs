use actix::prelude::*;
use serde_derive::{Deserialize, Serialize};
use serde;
use serde_json::json;
use std::fmt;
use uuid::Uuid;

use crate::transport::message::*;

#[derive(Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Request,
    Response,
    Unknown,
}

impl Type {
    pub fn from_str(s: &str) -> Self {
        match s {
            "request" => Type::Request,
            "response" => Type::Response,
            _ => Type::Unknown,
        }
    }

    pub fn as_str(&self) -> String {
        match self{
            Type::Request => "request".to_string(),
            Type::Response => "response".to_string(),
            Type::Unknown => "unknown".to_string(),
        }
    }
}

impl fmt::Debug for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", Type::as_str(self))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ControlMessage {
    pub uuid: String,

    #[serde(rename = "type")]
    pub type_: Type,

    /// App ID or Task UUID or whatever.
    pub dest_id: String,

    pub orig_id: String,

    pub cmd: String,

    pub data: serde_json::Value,
}

impl Message for ControlMessage {
    type Result = ();
}

impl ControlMessage {

    pub fn dest(&self) -> &str {
        match self.type_ {
            Type::Request => &self.dest_id,
            Type::Response => &self.orig_id,
            _ => panic!("Unknown control message destination."),
        }
    }

    pub fn request(
        dest_id: &str,
        orig_id: &str,
        cmd: &str,
    ) -> Self {
        Self {
            uuid: Uuid::new_v4().to_string(),
            type_: Type::Request,
            dest_id: dest_id.into(),
            orig_id: orig_id.into(),
            cmd: cmd.into(),
            data: serde_json::Value::default(),
        }
    }

    pub fn request_with_data<D: serde::Serialize>(
        dest_id: &str,
        orig_id: &str,
        cmd: &str,
        data: D
    ) -> Self {
        Self {
            uuid: Uuid::new_v4().to_string(),
            type_: Type::Request,
            dest_id: dest_id.into(),
            orig_id: orig_id.into(),
            cmd: cmd.into(),
            data: json!(data),
        }
    }

    pub fn response<D: serde::Serialize>(mut self, data: D) -> Self {
        self.type_ = Type::Response;
        self.data = json!(data);
        self
    }
}

#[macro_export]
macro_rules! handler_impl_control_message {
    ($x:ty) => {
        impl Handler<ControlMessage> for $x {
            type Result = ();

            fn handle(
                &mut self,
                msg: ControlMessage,
                ctx: &mut Self::Context
            ) -> Self::Result {
                self.handle_control_message(msg, ctx);
            }
        }
    }
}

#[derive(Clone)]
pub struct StopTask {
    pub task_uuid: String,
}

impl Message for StopTask {
    type Result = ();
}

#[macro_export]
macro_rules! handler_impl_stop_task {
    ($x:ty) => {
        impl Handler<StopTask> for $x {
            type Result = ();

            fn handle(
                &mut self,
                msg: StopTask,
                ctx: &mut Self::Context
            ) -> Self::Result {
                info!(self.log, "Stopped [TASK UUID] {}", msg.task_uuid);
                self.handle_stop_task(msg, ctx);
            }
        }
    }
}

/// Remove task from the system.
#[derive(Clone)]
pub struct CloseTask {
    pub task_uuid: String,
}

impl Message for CloseTask {
    type Result = ();
}

#[macro_export]
macro_rules! handler_impl_close_task {
    ($x:ty) => {
        impl Handler<CloseTask> for $x {
            type Result = ();

            fn handle(
                &mut self,
                msg: CloseTask,
                ctx: &mut Self::Context
            ) -> Self::Result {
                info!(self.log, "Closed [TASK UUID] {}", msg.task_uuid);
                self.handle_close_task(msg, ctx);
            }
        }
    }
}

#[derive(Clone)]
pub struct RestartTask {
    pub task_uuid: String,
}

impl Message for RestartTask {
    type Result = ();
}

#[macro_export]
macro_rules! handler_impl_restart_task {
    ($x:ty) => {
        impl Handler<RestartTask> for $x {
            type Result = ();

            fn handle(
                &mut self,
                msg: RestartTask,
                ctx: &mut Self::Context
            ) -> Self::Result {
                info!(self.log, "Restart [TASK UUID] {}", msg.task_uuid);
                self.handle_restart_task(msg, ctx);
            }
        }
    }
}

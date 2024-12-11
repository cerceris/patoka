use serde_derive::{Deserialize, Serialize};
use serde_json;
use std::fmt;

use crate::{
    transport::message::*,
    worker::plugin::{WorkerPlugin},
};

#[derive(Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Dest {
    Controller,
    Client,
    Worker,
    ExternalIn,
    ExternalOut,
    Unknown,
}

impl Dest {
    pub fn from_str(s: &str) -> Self {
        match s {
            "controller" => Dest::Controller,
            "client" => Dest::Client,
            "worker" => Dest::Worker,
            "external_in" => Dest::ExternalIn,
            "external_out" => Dest::ExternalOut,
            _ => Dest::Unknown,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self{
            Dest::Controller => "controller",
            Dest::Client => "client",
            Dest::Worker => "worker",
            Dest::ExternalIn => "external_in",
            Dest::ExternalOut => "external_out",
            _ => "unknown",
        }
    }
}

impl fmt::Debug for Dest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", Dest::as_str(self))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WorkerMessagePayload {
    pub dest: Dest,
    pub worker_id: String,
    pub task_uuid: String,
    #[serde(default)]
    pub plugin: String,
    pub data: serde_json::Value,
}

impl WorkerMessagePayload {
    pub fn header(&self) -> String {
        format!(
            "[{}:{}:{}:{}]",
            self.dest.as_str(),
            self.worker_id,
            self.task_uuid,
            self.plugin
        )
    }

    pub fn new() -> Self {
        Self {
            dest: Dest::Unknown,
            worker_id: String::new(),
            task_uuid: String::new(),
            plugin: WorkerPlugin::as_str(WorkerPlugin::Basic).to_string(),
            data: serde_json::to_value({}).unwrap(),
        }
    }
}

pub type WorkerMessage = GenMessage<WorkerMessagePayload>;

impl WorkerMessage {
    pub fn result<T: serde::de::DeserializeOwned>(&self) -> Option<T> {
        if let Some(r) = self.payload.data.get("task_result") {
            Some(serde_json::from_value(r.clone()).unwrap())
        } else {
            None
        }
    }

    pub fn error(&self) -> Option<serde_json::Value> {
        if let Some(e) = self.payload.data.get("error") {
            Some(e.clone())
        } else {
            None
        }
    }

    pub fn question(&self) -> Option<serde_json::Value> {
        if let Some(e) = self.payload.data.get("task_question") {
            Some(e.clone())
        } else {
            None
        }
    }
}

#[macro_export]
macro_rules! handler_impl_worker_message {
    ($x:ty) => {
        impl Handler<WorkerMessage> for $x {
            type Result = ();

            fn handle(
                &mut self,
                msg: WorkerMessage,
                ctx: &mut Self::Context
            ) -> Self::Result {
                self.handle_worker_message(msg, ctx);
            }
        }
    }
}

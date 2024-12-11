use serde_derive::{Deserialize, Serialize};
use serde_json;
use serde_json::json;
use std::fmt;

use crate::{
    control::message::*,
    worker::worker_message::*,
    transport::message::*,
};

#[derive(Clone, PartialEq)]
pub enum Subject {
    /// Worker has started.
    Started,

    /// Worker is ready to execute a new task.
    Ready,

    /// Worker has prepared the proper plugin.
    PluginReady,

    /// Error occured.
    Error,

    /// Message is being sent periodically to a worker.
    HeartbeatRequest,

    /// A response from the worker indicates it is alive.
    HeartbeatResponse,

    ControlRequest,

    ControlResponse,

    Custom(String),
}

impl Subject {
    pub fn from_str(s: &str) -> Self {
        match s {
            "started" => Subject::Started,
            "ready" => Subject::Ready,
            "plugin_ready" => Subject::PluginReady,
            "error" => Subject::Error,
            "heartbeat_request" => Subject::HeartbeatRequest,
            "heartbeat_response" => Subject::HeartbeatResponse,
            "control_request" => Subject::ControlRequest,
            "control_response" => Subject::ControlResponse,
            _ => Subject::Custom(s.to_string()),
        }
    }

    pub fn as_str(&self) -> String {
        match self{
            Subject::Started => "started".to_string(),
            Subject::Ready => "ready".to_string(),
            Subject::PluginReady => "plugin_ready".to_string(),
            Subject::Error => "error".to_string(),
            Subject::HeartbeatRequest => "heartbeat_request".to_string(),
            Subject::HeartbeatResponse => "heartbeat_response".to_string(),
            Subject::ControlRequest => "control_request".to_string(),
            Subject::ControlResponse => "control_response".to_string(),
            Subject::Custom(s) => s.clone(),
        }
    }
}

impl fmt::Debug for Subject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", Subject::as_str(self))
    }
}

/// Message type used for Controller <-> Worker communication.
#[derive(Debug)]
pub struct ControllerMessage {
    pub identity: Identity,
    pub worker_id: String,
    pub dest: Dest,
    pub subject: Subject,
    pub details: serde_json::Value,
}

#[derive(Deserialize, Serialize)]
pub struct ControllerMessageBody {
    pub subject: String,
    pub details: serde_json::Value,
}

impl ControllerMessage {
    pub fn new(worker_id: String, dest: Dest, subject: Subject) -> Self {
        Self {
            identity: new_identity(),
            worker_id,
            dest,
            subject,
            details: serde_json::to_value({}).unwrap(),
        }
    }

    pub fn from(wm: WorkerMessage) -> Result<Self, serde_json::Error> {
        let body = serde_json::from_value::<ControllerMessageBody>(
            wm.payload.data
        )?;

        Ok(Self {
            identity: wm.identity,
            worker_id: wm.payload.worker_id,
            dest: wm.payload.dest,
            subject: Subject::from_str(&body.subject),
            details: body.details,
        })
    }

    pub fn with_identity(
        worker_id: String,
        dest: Dest,
        subject: Subject,
        identity: Identity,
    ) -> Self {
        Self {
            identity,
            worker_id,
            dest,
            subject,
            details: serde_json::to_value({}).unwrap(),
        }
    }

    pub fn with_details(
        worker_id: String,
        dest: Dest,
        subject: Subject,
        details: serde_json::Value,
    ) -> Self {
        Self {
            identity: new_identity(),
            worker_id,
            dest,
            subject,
            details,
        }
    }
}

impl Clone for ControllerMessage {
    fn clone(&self) -> Self {
        Self {
            identity: Identity::from(&self.identity as &[u8]),
            worker_id: self.worker_id.clone(),
            dest: self.dest,
            subject: self.subject.clone(),
            details: self.details.clone(),
        }
    }
}

impl Into<WorkerMessage> for ControllerMessage {
    fn into(self) -> WorkerMessage {
        let data = json!({
            "subject": Subject::as_str(&self.subject),
            "details": self.details,
        });

        let payload = WorkerMessagePayload {
            dest: self.dest,
            worker_id: self.worker_id,
            task_uuid: String::new(),
            plugin: String::new(),
            data,
        };

        WorkerMessage::with_identity(payload, self.identity)
    }
}

pub fn create_control_request(
    worker_id: String,
    msg: ControlMessage
) -> ControllerMessage {
    ControllerMessage::with_details(
        worker_id,
        Dest::Worker,
        Subject::ControlRequest,
        json!(msg),
    )
}

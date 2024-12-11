use serde_derive::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    center::{connector, message},
    core::timestamp::*,
    transport::message::*,
    worker::task::TaskStatus,
};

#[derive(Clone)]
pub struct TaskState<StateInfo> {
    task_uuid: String,

    pub status: TaskStatus,

    started_at: Timestamp,

    pub info: StateInfo,
}

impl<StateInfo> TaskState<StateInfo>
where
    StateInfo: Default + serde::Serialize
{
    pub fn new() -> Self {
        Self {
            task_uuid: String::new(),
            status: TaskStatus::Unknown,
            started_at: now(),
            info: StateInfo::default(),
        }
    }

    pub fn started(&mut self, task_uuid: String) {
        self.task_uuid = task_uuid;
        self.status = TaskStatus::Running;
        self.started_at = now();
    }

    pub fn send_report(&self) {
        let report = TaskStatusReport {
            task_uuid: self.task_uuid.clone(),
            status: self.status,
            started_at: self.started_at.clone(),
            info: json!(self.info),
        };

        let c_msg = message::create(
            message::Dest::Center,
            message::Subject::TaskStatusReport,
            self.task_uuid.clone(),
            "status_report".to_string(),
            report,
        );

        connector::start().do_send(RawMessage::from(c_msg));
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskStatusReport {
    pub task_uuid: String,

    pub status: TaskStatus,

    pub started_at: Timestamp,

    pub info: serde_json::Value,
}

impl TaskStatusReport {
    pub fn new() -> Self {
        Self {
            task_uuid: String::new(),
            status: TaskStatus::Unknown,
            started_at: now(),
            info: serde_json::Value::default(),
        }
    }
}

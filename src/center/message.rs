use serde_derive::{Deserialize, Serialize};
use serde_json;
use std::fmt;

use crate::{
    transport::message::*,
    core::timestamp::{Timestamp, now},
};

#[derive(Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Dest {
    /// App --> Center
    Center,

    /// Center --> App
    App,

    Unknown,
}

impl Dest {
    pub fn from_str(s: &str) -> Self {
        match s {
            "center" => Dest::Center,
            "app" => Dest::App,
            _ => Dest::Unknown,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self{
            Dest::Center => "center",
            Dest::App => "app",
            _ => "unknown",
        }
    }
}

impl fmt::Debug for Dest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", Dest::as_str(self))
    }
}

#[derive(Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Subject {
    AppStatusReport,
    TaskStatusReport,
    TaskStatusUpdate,
    TaskResult,
    TaskQuestion,
    Control,
    Unknown,

    // TODO: Implement `Custom(String)` with a custom (de)serializer.
}

impl Subject {
    pub fn from_str(s: &str) -> Self {
        match s {
            "app_status_report" => Subject::AppStatusReport,
            "task_status_report" => Subject::TaskStatusReport,
            "task_status_update" => Subject::TaskStatusUpdate,
            "task_result" => Subject::TaskResult,
            "task_question" => Subject::TaskQuestion,
            "control" => Subject::Control,
            _ => Subject::Unknown,
        }
    }

    pub fn as_str(&self) -> String {
        match self{
            Subject::AppStatusReport => "app_status_report".to_string(),
            Subject::TaskStatusReport => "task_status_report".to_string(),
            Subject::TaskStatusUpdate => "task_status_update".to_string(),
            Subject::TaskResult => "task_result".to_string(),
            Subject::TaskQuestion => "task_question".to_string(),
            Subject::Control => "control".to_string(),
            Subject::Unknown => "unknown".to_string(),
        }
    }
}

impl fmt::Debug for Subject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", Subject::as_str(self))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CenterMessagePayload {
    pub dest: Dest,
    pub subject: Subject,

    /// Sender entity identifier: task UUID, Application ID, whatever.
    pub entity_id: String,

    /// The actual payload consists of short `message` and `data`.
    pub message: String,
    pub data: serde_json::Value,

    pub ts: Timestamp,
}

impl CenterMessagePayload {
    pub fn header(&self) -> String {
        format!(
            "[{}:{}:{}:{}:{}]",
            self.dest.as_str(),
            self.subject.as_str(),
            self.entity_id,
            self.message,
            self.ts,
        )
    }

    pub fn new() -> Self {
        Self {
            dest: Dest::Unknown,
            subject: Subject::Unknown,
            entity_id: String::new(),
            message: String::new(),
            data: serde_json::to_value({}).unwrap(),
            ts: now(),
        }
    }

    pub fn create<D: serde::Serialize>(
        dest: Dest,
        subject: Subject,
        entity_id: String,
        message: String,
        data: D
    ) -> Self {
        Self {
            dest,
            subject,
            entity_id,
            message,
            data: serde_json::to_value(data).unwrap(),
            ts: now(),
        }
    }

}

pub type CenterMessage = GenMessage<CenterMessagePayload>;

pub fn create<D: serde::Serialize>(
    dest: Dest,
    subject: Subject,
    entity_id: String,
    message: String,
    data: D
) -> CenterMessage {
    CenterMessage::new(
        CenterMessagePayload::create(
            dest,
            subject,
            entity_id,
            message,
            data,
        )
    )
}

pub fn create_with_identity<D: serde::Serialize>(
    dest: Dest,
    subject: Subject,
    entity_id: String,
    message: String,
    data: D,
    identity: Identity,
) -> CenterMessage {
    CenterMessage::with_identity(
        CenterMessagePayload::create(
            dest,
            subject,
            entity_id,
            message,
            data,
        ),
        identity,
    )
}

pub fn create_no_data(
    dest: Dest,
    subject: Subject,
    entity_id: String,
    message: String,
) -> CenterMessage {
    CenterMessage::new(
        CenterMessagePayload::create(
            dest,
            subject,
            entity_id,
            message,
            "",
        )
    )
}

pub fn create_no_data_with_identity(
    dest: Dest,
    subject: Subject,
    entity_id: String,
    message: String,
    identity: Identity,
) -> CenterMessage {
    CenterMessage::with_identity(
        CenterMessagePayload::create(
            dest,
            subject,
            entity_id,
            message,
            "",
        ),
        identity,
    )
}

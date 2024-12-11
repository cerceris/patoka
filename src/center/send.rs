use serde;
use serde_json::json;

use crate::{
    center::{connector, message},
    control::message::*,
    transport::message::RawMessage,
    worker::{
        task::{GenTaskDefinition, TaskStatus},
        tracker::{self, TaskUpdateTag},
    },
};

pub fn send_center_task_started<P: serde::Serialize>(
    task_uuid: &str,
    task_definition: &P,
    name: &str,
)
{
    let c_msg = message::create(
        message::Dest::Center,
        message::Subject::TaskStatusUpdate,
        task_uuid.to_string(),
        "started".to_string(),
        json!(task_definition),
    );

    tracker::send(
        task_uuid.into(),
        TaskStatus::Running,
        c_msg,
        TaskUpdateTag::Started,
        name.into(),
    );
}

pub fn send_center_task_updated<P: serde::Serialize>(
    task_uuid: &str,
    task_definition: &P,
    name: &str,
)
{
    let c_msg = message::create(
        message::Dest::Center,
        message::Subject::TaskStatusUpdate,
        task_uuid.to_string(),
        "updated".to_string(),
        json!(task_definition),
    );

    tracker::send(
        task_uuid.into(),
        TaskStatus::Running,
        c_msg,
        TaskUpdateTag::Updated,
        name.into()
    );
}

pub fn send_center_task_finished(
    task_uuid: &str,
    status: TaskStatus,
    name: &str,
) {
    let msg = if status == TaskStatus::FinishedSuccess {
        "finished_success"
    } else {
        "finished_failure"
    };

    let c_msg = message::create_no_data(
        message::Dest::Center,
        message::Subject::TaskStatusUpdate,
        task_uuid.to_string(),
        msg.to_string(),
    );

    tracker::send(
        task_uuid.into(),
        status,
        c_msg,
        TaskUpdateTag::Finished,
        name.into()
    );
}

pub fn send_center_task_result<D: serde::Serialize>(
    task_uuid: &str,
    data: &D
) {
    let c_msg = message::create(
        message::Dest::Center,
        message::Subject::TaskResult,
        task_uuid.to_string(),
        "task_result".to_string(),
        json!(data),
    );

    connector::start().do_send(RawMessage::from(c_msg));
}

pub fn send_center_task_question<D: serde::Serialize>(
    task_uuid: &str,
    data: &D,
    name: &str,
) {
    let c_msg = message::create(
        message::Dest::Center,
        message::Subject::TaskQuestion,
        task_uuid.to_string(),
        "task_question".to_string(),
        json!(data),
    );

    tracker::send(
        task_uuid.into(),
        TaskStatus::Running,
        c_msg,
        TaskUpdateTag::Question,
        name.into(),
    );
}

pub fn send_center_task_closed(task_uuid: &str) {
    let c_msg = message::create_no_data(
        message::Dest::Center,
        message::Subject::TaskStatusUpdate,
        task_uuid.to_string(),
        "closed".to_string(),
    );

    connector::start().do_send(RawMessage::from(c_msg));
}

pub fn send_control_msg(msg: ControlMessage) {
    let c_msg = message::create(
        message::Dest::Center,
        message::Subject::Control,
        msg.dest().into(),
        "".into(),
        json!(msg),
    );

    connector::start().do_send(RawMessage::from(c_msg));
}

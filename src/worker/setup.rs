use actix::prelude::*;
use std::marker::PhantomData;

use crate::{
    control::{message::*, registry},
    worker::{
        controller::{self, WorkerController},
        task::ControllerAddr,
        task_reader,
        tracker::{self, TaskUpdate},
        worker_message::WorkerMessage,
    },
};

pub fn setup(
    task_uuid: &str,
    control_addr: Option<Recipient<ControlMessage>>,
    task_update_addr: Option<Recipient<TaskUpdate>>,
) {
    if let Some(a) = control_addr {
        registry::register(task_uuid.into(), a);
    }

    if let Some(a) = task_update_addr {
        tracker::register_task_update_recipient(task_uuid.into(), a);
    }
}

pub fn setup_with_controller(
    task_uuid: &str,
    control_addr: Option<Recipient<ControlMessage>>,
    task_update_addr: Option<Recipient<TaskUpdate>>,
    worker_message_addr: Recipient<WorkerMessage>,
    controller_addr: &ControllerAddr,
    msg: WorkerMessage,
    task_name: String,
) {
    setup(task_uuid, control_addr, task_update_addr);

    // Initiate execution of the task.
    match controller_addr {
        ControllerAddr::Controller(addr) => {
            controller::start_task(
                addr,
                msg,
                worker_message_addr,
                task_name
            );
        },
        ControllerAddr::Reader(addr) => {
            task_reader::register_task(
                addr,
                worker_message_addr,
                task_name,
            );
        },
        _ => {
            panic!("Unexpected ControllerAddr::None");
        },
    }
}


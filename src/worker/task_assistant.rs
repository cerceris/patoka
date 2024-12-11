use actix::prelude::*;
use slog::Logger;
use std::{
    collections::HashMap,
    time::Duration,
};

use crate::{
    core::logger::create_logger,
    worker::{
        tracker::{self, TaskUpdate},
        task::TaskStatus,
        task_tree::self,
    },
};

pub struct TaskAssistantItem {
    task_uuid: String,
    restart_delay: usize,
}

impl TaskAssistantItem {
    pub fn new(task_uuid: String, restart_delay: usize) -> Self {
        Self {
            task_uuid,
            restart_delay,
        }
    }
}

pub struct TaskAssistant {
    log: Logger,

    /// Task UUID --> TaskAssistantItem
    tasks: HashMap<String, TaskAssistantItem>,
}

impl TaskAssistant {
    fn handle_task_recovery(&mut self, msg: TaskRecovery) {
        let item = TaskAssistantItem::new(
            msg.task_uuid.clone(),
            msg.restart_delay,
        );

        if let Some(_) = self.tasks.insert(msg.task_uuid.clone(), item) {
            panic!("Task has already been registered in Task Assistant!");
        } else {
            debug!(self.log, "Registered [TASK UUID] {}", msg.task_uuid);
        }
    }

    fn handle_task_update(
        &mut self,
        msg: TaskUpdate,
        ctx: &mut <Self as Actor>::Context
    ) {

        if !self.tasks.contains_key(&msg.task_uuid) {
            trace!(
                self.log,
                "Received update for unregistered [TASK UUID] {}.",
                msg.task_uuid,
            );

            return;
        }

        match msg.status {
            TaskStatus::FinishedSuccess => {
                debug!(
                    self.log,
                    "Finished SUCCESS [TASK UUID] {}. Removing task.",
                    msg.task_uuid,
                );

                self.tasks.remove(&msg.task_uuid);
            },
            TaskStatus::FinishedFailure => {
                let item = self.tasks.get(&msg.task_uuid).unwrap();

                debug!(
                    self.log,
                    "Finished FAILURE [TASK UUID] {}. Restarting task in {} \
                        ms.",
                    msg.task_uuid,
                    item.restart_delay,
                );

                let restart_delay = item.restart_delay as u64;
                let task_uuid = msg.task_uuid.clone();

                self.tasks.remove(&msg.task_uuid);

                ctx.run_later(
                    Duration::from_millis(restart_delay),
                    |_, _| task_tree::restart_task(task_uuid),
                );
            },
            _ => {
            },
        }
    }
}

impl Default for TaskAssistant {
    fn default() -> Self {
        Self {
            log: create_logger("task_assistant"),
            tasks: HashMap::new(),
        }
    }
}

impl Actor for TaskAssistant {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Task Assistant started.");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Task Assistant stopped.");
    }
}

pub struct TaskRecovery {
    pub task_uuid: String,
    pub restart_delay: usize,
}

impl Message for TaskRecovery {
    type Result = ();
}

impl Handler<TaskRecovery> for TaskAssistant {
    type Result = ();

    fn handle(
        &mut self,
        msg: TaskRecovery,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        self.handle_task_recovery(msg);
    }
}

handler_impl_task_update!(TaskAssistant);

impl Supervised for TaskAssistant {}

impl SystemService for TaskAssistant {
    fn service_started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Task Assistant system service started.")
    }
}

pub fn register(task_uuid: String, restart_delay: usize) {
    start().do_send(TaskRecovery { task_uuid, restart_delay });
}

pub fn start() -> Addr<TaskAssistant> {
    TaskAssistant::from_registry()
}

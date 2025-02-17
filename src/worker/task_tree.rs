use actix::prelude::*;
use slog::Logger;
use std::collections::{HashMap, HashSet};

use crate::{
    center::{
        connector::{self, CenterConnector},
        message,
    },
    control::{
        message::{CloseTask, ControlMessage, RestartTask, StopTask},
        registry,
    },
    core::{
        app_state::{self, *},
        logger::create_logger,
    },
    transport::message::RawMessage,
    worker::{
        processor::{self, TaskWrapperItem, TaskWrapperItemMessage},
        tracker::{self, TaskUpdate},
        task::*,
    },
};

struct TaskTreeItem {
    pub ctx: TaskExecutionContext,

    /// { Task UUID }
    pub child_tasks: HashSet<String>,

    /// To replay.
    pub task: TaskWrapperItem,

    pub task_status: TaskStatus,
}

impl TaskTreeItem {
    pub fn new(
        ctx: TaskExecutionContext,
        task: TaskWrapperItem,
    ) -> Self {
        Self {
            ctx,
            child_tasks: HashSet::new(),
            task,
            task_status: TaskStatus::Running,
        }
    }

    pub fn task_finished(&self) -> bool {
        self.task_status == TaskStatus::FinishedSuccess
            || self.task_status == TaskStatus::FinishedFailure
    }
}

pub struct TaskTree {
    log: Logger,

    center_connector_addr: Addr<CenterConnector>,

    app_state_addr: Addr<AppState>,

    /// Task UUID --> TaskTreeItem
    tasks: HashMap<String, TaskTreeItem>,

    tasks_to_close: HashSet<String>,

    tasks_to_restart: HashSet<String>,
}

impl TaskTree {
    fn handle_stop_task(
        &mut self,
        msg: StopTask,
        _ctx: &mut <Self as Actor>::Context
    ) {
        self.stop_task(msg.task_uuid);
    }

    fn handle_restart_task(
        &mut self,
        msg: RestartTask,
        _ctx: &mut <Self as Actor>::Context
    ) {
        self.restart_task(msg.task_uuid);
    }

    fn handle_task_update(
        &mut self,
        msg: TaskUpdate,
        _ctx: &mut <Self as Actor>::Context
    ) {
        match msg.status {
            TaskStatus::FinishedSuccess | TaskStatus::FinishedFailure => {
                debug!(self.log, "Finished [TASK UUID] {}.", msg.task_uuid);

                if let Some(item) = self.tasks.get_mut(&msg.task_uuid) {
                    item.task_status = msg.status;
                } else {
                    warn!(
                        self.log,
                        "Received TaskUpdate for unknown [TASK UUID] {}",
                        msg.task_uuid,
                    );
                }

                // Send a "task finished" message to the center.
                let c_msg = message::create_no_data(
                    message::Dest::Center,
                    message::Subject::TaskStatusUpdate,
                    msg.task_uuid.clone(),
                    "finished".to_string(),
                );

                self.center_connector_addr.do_send(
                    RawMessage::from(c_msg)
                );

                if self.tasks_to_close.contains(&msg.task_uuid) {
                    self.close_task(msg.task_uuid);
                }
            },
            _ => {
            },
        }
    }

    fn process_new_task(&mut self, msg: NewTask) {
        let ctx = msg.ctx;
        let task_uuid = ctx.task_uuid.clone();
        let parent_task_uuid = ctx.parent_task_uuid.clone();

        debug!(
            self.log,
            "New task [TASK UUID] {} [PARENT TASK UUID] {}.",
            task_uuid,
            parent_task_uuid,
        );

        let item = TaskTreeItem::new(ctx, msg.task);
        self.tasks.insert(task_uuid.clone(), item);

        if parent_task_uuid != "" {
            if let Some(parent_item) = self.tasks.get_mut(&parent_task_uuid) {
                parent_item.child_tasks.insert(task_uuid);
            } else {
                panic!("Could not get the parent task!");
            }
        }
    }

    fn handle_control_message(
        &mut self,
        msg: ControlMessage,
        ctx: &mut <Self as Actor>::Context,
    ) {
        debug!(self.log, "[CONTROL] {:?}", msg);

        match msg.cmd.as_ref() {
            "stop_task" => {
                self.stop_task(msg.data.as_str().unwrap().to_string());
            },
            "close_task" => {
                self.close_task(msg.data.as_str().unwrap().to_string());
            },
            "restart_task" => {
                self.restart_task(msg.data.as_str().unwrap().to_string());
            },
            _ => {
                warn!(self.log, "Unknown [CMD] {}", msg.cmd);
            }
        }
    }

    fn stop_task(&self, task_uuid: String) {
        if let Some(item) = self.tasks.get(&task_uuid) {
            for child_task_uuid in item.child_tasks.clone() {
                self.stop_task(child_task_uuid.clone());
            }

            if item.task_finished() {
                debug!(self.log, "[TASK UUID] {} is finished.", task_uuid);
            } else {
                debug!(self.log, "Stop [TASK UUID] {}", task_uuid);

                let msg = StopTask { task_uuid };

                if let ControllerAddr::Controller(ref a) =
                    item.ctx.controller_addr
                {
                    a.do_send(msg.clone())
                }

                item.ctx.stop_task_addr.do_send(msg);
            }
        } else {
            warn!(self.log, "Tried to stop unknown [TASK UUID] {}", task_uuid);
        }
    }

    fn close_task(&mut self, task_uuid: String) {
        // Ensure the task is finished, then close, and then sometimes restart.
        let mut remove = false;
        if let Some(item) = self.tasks.get(&task_uuid) {
            if item.task_finished() {
                debug!(self.log, "Close [TASK UUID] {}", task_uuid);

                remove = true;
                let msg = CloseTask { task_uuid: task_uuid.clone() };

                if let ControllerAddr::Controller(ref a) =
                    item.ctx.controller_addr
                {
                    a.do_send(msg.clone());
                }

                tracker::start().do_send(msg);
            } else {
                // First stop the task.
                self.tasks_to_close.insert(task_uuid.clone());
                self.stop_task(task_uuid.clone());
            }

            for child_task_uuid in item.child_tasks.clone() {
                self.close_task(child_task_uuid);
            }

        } else {
            warn!(
                self.log,
                "Tried to close unknown [TASK UUID] {}",
                task_uuid,
            );
        }

        if !remove {
            return;
        }

        let item = self.tasks.remove(&task_uuid);
        self.tasks_to_close.remove(&task_uuid);

        if self.tasks_to_restart.contains(&task_uuid) {
            match item {
                Some(mut i) => {
                    debug!(
                        self.log,
                        "Send message to Processor to restart [TASK UUID] {}",
                        task_uuid
                    );

                    i.task.update_task_uuid();
                    processor::start().do_send(TaskWrapperItemMessage(i.task));
                },
                _ => {
                    error!(
                        self.log,
                        "Unable to restart unknown [TASK UUID] {}",
                        task_uuid,
                    );
                }
            }

            self.tasks_to_restart.remove(&task_uuid);
        }
    }

    fn restart_task(&mut self, task_uuid: String) {
        if self.tasks.contains_key(&task_uuid) {
            debug!(self.log, "Restart [TASK UUID] {}", task_uuid);

            self.tasks_to_restart.insert(task_uuid.clone());
            self.close_task(task_uuid);
        } else {
            warn!(
                self.log,
                "Tried to restart unknown [TASK UUID] {}",
                task_uuid,
            );
        }
    }
}

impl Default for TaskTree {
    fn default() -> Self {
        TaskTree {
            log: create_logger("task_tree"),
            center_connector_addr: connector::start(),
            app_state_addr: app_state::start(),
            tasks: HashMap::new(),
            tasks_to_close: HashSet::new(),
            tasks_to_restart: HashSet::new(),
        }
    }
}

impl Actor for TaskTree {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(self.log, "Task Tree started.");

        ctx.set_mailbox_capacity(1000000);

        registry::register(
            "task_tree".to_string(),
            ctx.address().recipient(),
        );
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Task Tree stopped.");
    }
}

pub struct NewTask {
    pub ctx: TaskExecutionContext,
    pub task: TaskWrapperItem,
}

impl Message for NewTask {
    type Result = ();
}

impl Handler<NewTask> for TaskTree {
    type Result = ();

    fn handle(
        &mut self,
        msg: NewTask,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        self.process_new_task(msg);
    }
}

handler_impl_control_message!(TaskTree);
handler_impl_task_update!(TaskTree);
handler_impl_stop_task!(TaskTree);
handler_impl_restart_task!(TaskTree);

pub fn restart_task(task_uuid: String) {
    start().do_send(RestartTask { task_uuid });
}

impl Supervised for TaskTree {}

impl SystemService for TaskTree {
    fn service_started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Task Tree system service started.")
    }
}

pub fn start() -> Addr<TaskTree> {
    TaskTree::from_registry()
}

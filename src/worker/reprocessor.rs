use actix::prelude::*;
use slog::Logger;
use std::collections::HashMap;

use crate::{
    core::{
        logger::create_logger,
        monitor::*,
    },
    worker::processor::{self,  *},
};

type Tasks = Vec<TaskWrapperItem>;

pub struct TaskReprocessor {
    log: Logger,
    task_processor: Addr<TaskProcessor>,

    /// Tasks to reprocess.
    tasks: Tasks,

    /// Worker ID --> [ Task ].
    tasks_linked_with_worker: HashMap<String, Tasks>,

    /// Periodically generate status report.
    report_status_timer: ReportStatusTimer,
}

impl TaskReprocessor {
    fn reprocess_tasks(&self, tasks: Tasks) {
        for task in tasks {
            self.reprocess_task(task);
        }
    }

    fn reprocess_task(&self, task: TaskWrapperItem) {
        debug!(self.log, "Reprocessing [TASK UUID] {}.", task.uuid());
        self.task_processor.do_send(TaskWrapperItemMessage(task));
    }
}

impl Default for TaskReprocessor {
    fn default() -> Self {
        TaskReprocessor {
            log: create_logger("task_reprocessor"),
            task_processor: processor::start(),
            tasks: vec![],
            tasks_linked_with_worker: HashMap::new(),
            report_status_timer: ReportStatusTimer::new_s(5),
        }
    }
}

impl Actor for TaskReprocessor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(self.log, "Task Reprocessor started.");
        self.report_status_timer.reset::<Self>(ctx);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Task Reprocessor stopped.");
    }
}

impl Supervised for TaskReprocessor {}

impl SystemService for TaskReprocessor {
    fn service_started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Task Reprocessor system service started.")
    }
}

pub struct ReprocessTask {
    pub task: TaskWrapperItem,
}

impl Message for ReprocessTask {
    type Result = ();
}

impl Handler<ReprocessTask> for TaskReprocessor {
    type Result = ();

    fn handle(
        &mut self,
        msg: ReprocessTask,
        _ctx: &mut Self::Context
    ) -> Self::Result {

        debug!(self.log, "Task to reprocess [TASK UUID] {}.", msg.task.uuid());

        if msg.task.worker_id() == "" {
            self.tasks.push(msg.task);
        } else {
            if let Some(tasks) = self.tasks_linked_with_worker
                .get_mut(msg.task.worker_id())
            {
                tasks.push(msg.task);
                return;
            }

            let worker_id = msg.task.worker_id().to_string();
            self.tasks_linked_with_worker.insert(worker_id, vec![msg.task]);
        }
    }
}

pub struct WorkerReady {
    pub worker_id: String,
}

impl Message for WorkerReady {
    type Result = ();
}

impl Handler<WorkerReady> for TaskReprocessor {
    type Result = ();

    fn handle(
        &mut self,
        msg: WorkerReady,
        _ctx: &mut Self::Context
    ) -> Self::Result {

        debug!(self.log, "[WORKER ID] {} is ready.", msg.worker_id);

        // Tasks linked with the worker have a higher priority.
        if let Some(tasks) = self.tasks_linked_with_worker
            .remove(&msg.worker_id)
        {
            self.reprocess_tasks(tasks);
        } else {
            while let Some(task) = self.tasks.pop() {
                self.reprocess_task(task);
            }
        }
    }
}

impl Handler<ReportStatusMessage> for TaskReprocessor {
    type Result = ();

    fn handle(
        &mut self,
        _msg: ReportStatusMessage,
        ctx: &mut Self::Context
    ) -> Self::Result {
        let number_of_tasks_to_reprocess = self.tasks.len();
        /*info!(
            self.log,
            "[STATUS] Number of tasks to reprocess: {}.",
            number_of_tasks_to_reprocess,
        );*/

        self.report_status_timer.reset::<Self>(ctx);
    }
}

pub fn start() -> Addr<TaskReprocessor> {
    let addr = TaskReprocessor::from_registry();
    addr
}

use actix::prelude::*;
use lazy_static::lazy_static;
use slog::Logger;
use std::collections::HashMap;
use std::sync::Mutex;

use crate::{
    center::message,
    core::{
        app_state::{self, *},
        arbiter_pool,
        logger::create_logger,
        monitor::*,
    },
    transport::message::RawMessage,
    worker::{
        controller_pool::{ControllerPool},
        plugin::WorkerPlugin,
        reprocessor::{self, ReprocessTask},
        task::*,
        task_reader,
        task_tree::{self, NewTask},
        tracker::{TaskUpdate},
    },
};

lazy_static! {
    pub static ref CONTROLLER_POOL: Mutex<ControllerPool>
        = Mutex::new(ControllerPool::new(1));
}

pub type TaskWrapperItem = Box<dyn TaskWrapper>;

pub struct TaskWrapperItemMessage(pub TaskWrapperItem);

impl Message for TaskWrapperItemMessage {
    type Result = ();
}

fn reprocess_task(task: TaskWrapperItem) {
    let task_reprocessor = reprocessor::start();
    task_reprocessor.do_send(ReprocessTask { task });
}

pub struct TaskProcessor {
    log: Logger,

    /// Periodically generate status report.
    report_status_timer: ReportStatusTimer,
}

impl TaskProcessor {
    fn process_task(
        &mut self,
        mut task: TaskWrapperItem,
        ctx: &mut <TaskProcessor as Actor>::Context
    ) {
        debug!(self.log, "New task arrived [TASK UUID] {}.", task.uuid());

        let mut arbiter_addr = arbiter_pool::next();
        let arbiter_addr_clone = arbiter_addr.clone();

        let task_uuid = task.uuid().to_owned();

        let task_clone = task.clone_box();

        let mut has_reader = false;

        let reader_addr = match task_reader::get_reader(task.name()) {
            Some(addr) => {
                has_reader = true;
                ControllerAddr::Reader(addr)
            },
            _ => ControllerAddr::None,
        };

        if /*task.plugin() == WorkerPlugin::None ||*/ has_reader {
            // The task works without controller.
            let task_exec_ctx = task.execute_in_arbiter(
                &arbiter_addr,
                reader_addr,
            );

            task_tree::start().do_send(
                NewTask { ctx: task_exec_ctx, task: task_clone }
            );

            return;
        }

        async move {
            let mut controller_pool = CONTROLLER_POOL.lock().unwrap();
            controller_pool.next(&arbiter_addr_clone, &task_uuid).await

        }.into_actor(self)
            .then(move |controller_details, act, _| {
                if controller_details.is_none() {
                    warn!(
                        act.log,
                        "Unable to find a suitable controller for [TASK UUID] \
                            {}.",
                        task.uuid(),
                    );

                    reprocess_task(task);

                } else {
                    let (controller_addr, controller_id, created) =
                        controller_details.unwrap();

                    if created {
                        // Run controller and master in different arbiters.
                        arbiter_addr = arbiter_pool::next();
                    }

                    task.update_worker_id(controller_id.to_string());

                    let task_exec_ctx = task.execute_in_arbiter(
                        &arbiter_addr,
                        ControllerAddr::Controller(controller_addr),
                    );

                    task_tree::start().do_send(
                        NewTask { ctx: task_exec_ctx, task: task_clone }
                    );
                }

                async {}.into_actor(act)
            })
            .wait(ctx);
    }
}

impl Default for TaskProcessor {
    fn default() -> Self {
        TaskProcessor {
            log: create_logger("task_processor"),
            report_status_timer: ReportStatusTimer::new_s(5),
        }
    }
}

impl Actor for TaskProcessor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(self.log, "Task Processor started.");

        self.report_status_timer.reset::<Self>(ctx);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Task Processor stopped.");
    }
}

impl Supervised for TaskProcessor {}

impl SystemService for TaskProcessor {
    fn service_started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Task Processor system service started.")
    }
}

impl Handler<TaskWrapperItemMessage> for TaskProcessor {
    type Result = ();

    fn handle(
        &mut self,
        msg: TaskWrapperItemMessage,
        ctx: &mut Self::Context
    ) -> Self::Result {
        self.process_task(msg.0, ctx);
    }
}

impl Handler<ReportStatusMessage> for TaskProcessor {
    type Result = ();

    fn handle(
        &mut self,
        _msg: ReportStatusMessage,
        ctx: &mut Self::Context
    ) -> Self::Result {

        self.report_status_timer.reset::<Self>(ctx);
    }
}

pub fn start() -> Addr<TaskProcessor> {
    let addr = TaskProcessor::from_registry();
    addr
}

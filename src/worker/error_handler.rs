use actix::prelude::*;
use serde_derive::Deserialize;
use serde_json;
use slog::Logger;

use crate::{
    control::{
        message::StopTask,
    },
    core::{
        env,
        logger::create_logger,
    },
    worker::{
        controller::WorkerController,
        task::{ControllerAddr, TaskStatus},
        task_assistant::self,
        worker_message::WorkerMessage,
    },
};

#[derive(Clone, Debug, Deserialize)]
pub struct TaskErrorHandlerParams {
    /// 0 by default.
    #[serde(default)]
    max_errors_then_failure: usize,

    /// Delay before task restart, ms.
    #[serde(default)]
    restart_delay: usize,
}

impl TaskErrorHandlerParams {
    pub fn new() -> Self {
        Self {
            max_errors_then_failure: 0,
            restart_delay: 0,
        }
    }
}

#[derive(Clone)]
pub struct TaskErrorHandler {
    log: Logger,
    task_uuid: String,
    controller_addr: ControllerAddr,
    params: TaskErrorHandlerParams,
    failure: bool,
    error_counter: usize,
}

impl TaskErrorHandler {
    pub fn new(
        task_uuid: String,
        controller_addr: ControllerAddr,
        config_name: &str,
    ) -> Self {
        let logger_name = format!("error_handler_{}", task_uuid);
        let log = create_logger(&logger_name);

        let params =
            match env::load_error::<TaskErrorHandlerParams>(config_name) {
                Some(p) => p,
                None => TaskErrorHandlerParams::new(),
            };

        debug!(log, "Params: {:?}", params);

        task_assistant::register(
            task_uuid.clone(),
            params.restart_delay,
        );

        Self {
            log,
            task_uuid,
            controller_addr,
            params,
            failure: false,
            error_counter: 0,
        }
    }

    pub fn failure(&self) -> bool {
        self.failure
    }

    pub fn task_finished_status(&self) -> TaskStatus {
        if self.failure {
            TaskStatus::FinishedFailure
        } else {
            TaskStatus::FinishedSuccess
        }
    }

    /// Return `true` if `data` contains an error.
    pub fn check<C: ActorContext>(
        &mut self,
        msg: &WorkerMessage,
        ctx: &mut C,
    ) -> bool {
        if let Some(e) = msg.error() {
            self.error_counter += 1;

            debug!(
                self.log,
                "Error [TASK UUID] {} [ERROR COUNTER] {} [PARAMS] {:?}",
                self.task_uuid,
                self.error_counter,
                self.params,
            );

            if self.error_counter > self.params.max_errors_then_failure {
                info!(
                    self.log,
                    "Terminate with FAILURE [TASK UUID] {}",
                    self.task_uuid,
                );

                self.failure = true;

                if let ControllerAddr::Controller(addr) =
                    &self.controller_addr
                {
                    addr.do_send(
                        StopTask { task_uuid: self.task_uuid.clone() }
                    );
                }

                ctx.stop();
            }

            true
        } else {
            // Reset.
            self.error_counter = 0;

            false
        }
    }
}

use actix::prelude::*;

use crate::worker::controller::{WorkerController, ReserveForTask};

pub struct ControllerPool {
    controllers: Vec<Addr<WorkerController>>,
    controller_ids: Vec<String>,
    capacity: usize,
    next_to_use: usize,
}

impl ControllerPool {
    pub fn new(capacity: usize) -> Self {
        ControllerPool {
            controllers: vec![],
            controller_ids: vec![],
            capacity,
            next_to_use: 0,
        }
    }

    pub async fn next(
        &mut self,
        arbiter: &ArbiterHandle,
        task_uuid: &str,
    ) -> Option<(Addr<WorkerController>, String, bool)> {
        let mut created = false;

        if self.controllers.len() < self.capacity {
            let controller_id = self.next_to_use.to_string();
            self.controller_ids.push(controller_id.clone());

            let wc = WorkerController::new(controller_id);
            let controller_address = WorkerController::start_in_arbiter(
                arbiter,
                move |_| {
                    wc
                }
            );

            self.controllers.push(controller_address);

            created = true;
        }

        // Try to find a controller that is ready to accept the task.
        let orig_next_to_use = self.next_to_use;

        loop {
            let addr = &self.controllers[self.next_to_use];

            let reserve_result = self.try_to_reserve_for_task(
                addr,
                task_uuid.to_string(),
            ).await;

            self.next_to_use += 1;
            if self.next_to_use >= self.controllers.len() {
                self.next_to_use = 0;
            }

            if reserve_result {
                let id = self.controller_ids[self.next_to_use].to_owned();
                return Some((addr.clone(), id, created));
            }

            if self.next_to_use == orig_next_to_use {
                // We've made a full cycle over the controllers.
                // Result is negative.
                break;
            }
        }

        None
    }

    async fn try_to_reserve_for_task(
        &self,
        controller_addr: &Addr<WorkerController>,
        task_uuid: String,
    ) -> bool {
        let res = controller_addr.send(ReserveForTask { task_uuid }).await;

        match res {
            Ok(r) => { r },
            _ => { false },
        }
    }

}

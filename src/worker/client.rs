use actix::prelude::*;

use crate::{
    control::message::StopTask,
    worker::{
        controller::{WorkerController},
        task::{ControllerAddr, GenTaskDefinition},
        worker_message::{WorkerMessage},
    },
};

#[derive(Clone)]
pub struct ClientContext<T> {
    pub task_uuid: String,
    pub worker_id: String,
    pub controller_addr: ControllerAddr,
    pub task_definition: T,
}

impl<T> ClientContext<T> {
    pub fn send_worker_message(&self, msg: WorkerMessage) {
        if let ControllerAddr::Controller(addr) = &self.controller_addr {
            addr.do_send(msg);
        }
    }
}

pub type GenClientContext<P> = ClientContext<GenTaskDefinition<P>>;

pub trait WorkerClient: Actor + Handler<StopTask> + Clone {
    type TaskDefinition;

    fn new(ctx: ClientContext<Self::TaskDefinition>) -> Self;

    fn start_in_arbiter_(
        arbiter: &ArbiterHandle,
        ctx: ClientContext<Self::TaskDefinition>,
    ) -> Addr<Self>
    where
        Self: Actor<Context=Context<Self>>,
        Self::TaskDefinition: Send + Sync,
    {
        Self::start_in_arbiter(
            arbiter,
            move |_| {
                Self::new(ctx)
            }
        )
    }

    fn handle_stop_task(&mut self, _msg: StopTask, ctx: &mut Self::Context) {
        ctx.stop();
    }
}


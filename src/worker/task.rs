use actix::prelude::*;
use serde;
use serde_derive::{Deserialize, Serialize};
use serde_json;
use serde_json::json;
use uuid::Uuid;

use crate::{
    center::send::*,
    control::message::StopTask,
    worker::{
        client::*,
        controller::{WorkerController},
        plugin::{WorkerPlugin},
        task_reader::TaskReader,
        tracker,
        worker_message::{WorkerMessage, Dest, WorkerMessagePayload},
    },
};

#[derive(Clone)]
pub enum ControllerAddr {
    Controller(Addr<WorkerController>),
    Reader(Addr<TaskReader>),
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Unknown,
    Running,
    Suspended,
    FinishedSuccess,
    FinishedFailure,
}

pub struct TaskExecutionContext {
    pub task_uuid: String,
    pub parent_task_uuid: String,
    pub stop_task_addr: Recipient<StopTask>,
    pub controller_addr: ControllerAddr,
}

impl TaskExecutionContext {
    pub fn send_worker_message(&self, msg: WorkerMessage) {
        if let ControllerAddr::Controller(addr) = &self.controller_addr {
            addr.do_send(msg);
        }
    }
}

pub trait TaskWrapper: Send + Sync {
    fn execute_in_arbiter(
        &self,
        arbiter: &ArbiterHandle,
        controller_addr: ControllerAddr,
    ) -> TaskExecutionContext;

    fn uuid(&self) -> &str;

    fn parent_uuid(&self) -> &str;

    fn worker_id(&self) -> &str;

    fn update_worker_id(&mut self, worker_id: String);

    /// Used when the task is restarted.
    fn update_task_uuid(&mut self);

    fn clone_box(&self) -> Box<dyn TaskWrapper>;

    fn plugin(&self) -> WorkerPlugin;

    fn name(&self) -> &str;
}

pub trait TaskDefinition {

    fn update_task_uuid(&mut self, task_uuid: String);

    fn update_worker_id(&mut self, task_uuid: String);

    fn parent_task_uuid(&self) -> &str;

    fn plugin(&self) -> WorkerPlugin;

    fn name(&self) -> &str;
}

#[derive(Clone, Serialize, Deserialize)]
pub struct GenTaskDefinition<P> {

    pub executor_path: String,

    /// Parameters to pass to the executor.
    pub params: P,

    pub task_uuid: String,

    pub name: String,

    /// Empty string for the master task.
    pub parent_task_uuid: String,

    /// Optional: if the task is linked with a particular worker.
    pub worker_id: String,

    /// Worker plugin that must be active to execute the task.
    pub plugin: WorkerPlugin,
}

impl<P> TaskDefinition for GenTaskDefinition<P> {
    fn update_task_uuid(&mut self, task_uuid: String) {
        self.task_uuid = task_uuid;
    }

    fn update_worker_id(&mut self, worker_id: String) {
        self.worker_id = worker_id;
    }

    fn parent_task_uuid(&self) -> &str { &self.parent_task_uuid }

    fn plugin(&self) -> WorkerPlugin { self.plugin }

    fn name(&self) -> &str { &self.name }
}

impl<P> GenTaskDefinition<P>
where
    P: serde::Serialize + Clone
{
    pub fn new(
        plugin: WorkerPlugin,
        executor_path: &str,
        params: P,
        name: &str,
    ) -> Self {
        Self {
            executor_path: executor_path.to_string(),
            params,
            task_uuid: String::new(),
            name: name.to_string(),
            parent_task_uuid: String::new(),
            worker_id: String::new(),
            plugin,
        }
    }

    pub fn subtask(
        plugin: WorkerPlugin,
        executor_path: &str,
        params: P,
        parent_task_uuid: String,
        name: &str,
    ) -> Self {
        Self {
            executor_path: executor_path.to_string(),
            params,
            task_uuid: String::new(),
            name: name.to_string(),
            parent_task_uuid,
            worker_id: String::new(),
            plugin,
        }
    }

    pub fn new_none_plugin(params: P, name: &str) -> Self {
        Self::new(WorkerPlugin::None, "", params, name)
    }

    pub fn subtask_none_plugin(
        params: P,
        parent_task_uuid: String,
        name: &str,
    ) -> Self {
        Self::subtask(WorkerPlugin::None, "", params, parent_task_uuid, name)
    }

    pub fn make_message(&self) -> WorkerMessage {
        let params = serde_json::to_value(self.params.clone()).unwrap();
        let data = json!({
            "task": {
                "executor_path": self.executor_path,
                "params": params,
                "name": self.name,
            }
        });

        let dest = Dest::Worker;

        let payload = WorkerMessagePayload {
            dest,
            worker_id: self.worker_id.clone(),
            task_uuid: self.task_uuid.clone(),
            plugin: WorkerPlugin::as_str(self.plugin).to_string(),
            data,
        };

        WorkerMessage::new(payload)
    }

    pub fn make_message_with_data(
        &self,
        data: serde_json::Value
    ) -> WorkerMessage {
        let dest = Dest::Worker;

        let payload = WorkerMessagePayload {
            dest,
            worker_id: self.worker_id.clone(),
            task_uuid: self.task_uuid.clone(),
            plugin: WorkerPlugin::as_str(self.plugin).to_string(),
            data,
        };

        WorkerMessage::new(payload)
    }
}

#[derive(Clone, Default)]
pub struct WorkerTask<C>
where
    C: WorkerClient,
    C: Actor<Context=Context<C>>,
{
    pub task_uuid: String,
    pub worker_id: String,
    pub task_definition: C::TaskDefinition,
}

impl<C: WorkerClient + Send + Sync> WorkerTask<C>
where
    C::TaskDefinition: TaskDefinition,
    C: Actor<Context=Context<C>>,
{
    pub fn new(mut task_definition: C::TaskDefinition) -> Self {
        let task_uuid = Uuid::new_v4().to_string();
        task_definition.update_task_uuid(task_uuid.clone());
        Self {
            task_uuid,
            worker_id: String::new(),
            task_definition,
        }
    }

    pub fn new_with_uuid(
        mut task_definition: C::TaskDefinition,
        task_uuid: String
    ) -> Self {
        task_definition.update_task_uuid(task_uuid.clone());
        Self {
            task_uuid,
            worker_id: String::new(),
            task_definition,
        }
    }
}

impl<C> TaskWrapper for WorkerTask<C>
where
    C: WorkerClient,
    C: Actor<Context=Context<C>>,
    Self: Send + Sync,
    C::TaskDefinition: Clone + TaskDefinition + Send + Sync +
        serde::Serialize,
{
    fn execute_in_arbiter(
        &self,
        arbiter: &ArbiterHandle,
        controller_addr: ControllerAddr,
    ) -> TaskExecutionContext {
        let controller_addr_clone = controller_addr.clone();

        let parent_task_uuid =
            self.task_definition.parent_task_uuid().to_string();

        if !parent_task_uuid.is_empty() {
            tracker::subscribe_no_addr(
                self.task_uuid.clone(),
                parent_task_uuid.clone(),
                self.task_definition.name().into(),
                false,
            );
        }

        let client_ctx = ClientContext {
            task_uuid: self.task_uuid.clone(),
            worker_id: self.worker_id.clone(),
            controller_addr,
            task_definition: self.task_definition.clone(),
        };
        let client_addr = C::start_in_arbiter_(arbiter, client_ctx);

        send_center_task_started(
            &self.task_uuid,
            &self.task_definition,
            self.task_definition.name(),
        );

        TaskExecutionContext {
            task_uuid: self.uuid().to_string(),
            parent_task_uuid,
            stop_task_addr: client_addr.recipient::<StopTask>(),
            controller_addr: controller_addr_clone,
        }
    }

    fn uuid(&self) -> &str { &self.task_uuid }

    fn parent_uuid(&self) -> &str { &self.task_definition.parent_task_uuid() }

    fn worker_id(&self) -> &str { &self.worker_id }

    fn update_worker_id(&mut self, worker_id: String) {
        self.task_definition.update_worker_id(worker_id.clone());
        self.worker_id = worker_id;
    }

    fn update_task_uuid(&mut self) {
        self.task_uuid = Uuid::new_v4().to_string();
        self.task_definition.update_task_uuid(self.task_uuid.clone());
    }

    fn clone_box(&self) -> Box<dyn TaskWrapper> { Box::new((*self).clone()) }

    fn plugin(&self) -> WorkerPlugin { self.task_definition.plugin() }

    fn name(&self) -> &str { self.task_definition.name() }
}


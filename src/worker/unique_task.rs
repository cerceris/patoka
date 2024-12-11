use std::collections::{HashMap, HashSet};

use crate::{
    worker::{
        task::{TaskStatus},
        tracker::{TaskUpdate, TaskUpdateTag},
    },
};

#[derive(Clone)]
pub struct UniqueTask {
    name: String,
    uuid: Option<String>,
}

impl UniqueTask {

    pub fn new(name: String) -> Self {
        Self {
            name,
            uuid: None,
        }
    }

    pub fn update(&mut self, msg: &TaskUpdate) -> Option<TaskUpdateTag> {
        if msg.name != self.name {
            return None;
        }

        match msg.tag  {
            TaskUpdateTag::Started => {
                self.must_not_running();
                self.uuid = Some(msg.task_uuid.clone());
            },
            TaskUpdateTag::Updated => {
                self.must_running();
            },
            TaskUpdateTag::Finished => {
                self.must_running();
                self.uuid = None;
            },
            _ => {
                // Ignore
            }
        }

        Some(msg.tag)
    }

    pub fn must_not_running(&self) {
        if let Some(uuid) = &self.uuid {
            panic!(
                "[NAME] {} is already running [TASK UUID] {}",
                self.name,
                uuid,
            );
        }
    }

    pub fn must_running(&self) {
        if self.uuid.is_none() {
            panic!(
                "[NAME] {} is not running but expected to be running",
                self.name,
            );
        }
    }
}

/// Group of similar tasks.
#[derive(Clone)]
pub struct UniqueTaskGroup {
    /// Group name.
    pub name: String,

    /// Task Name --> UniqueTask
    pub tasks: HashMap<String, UniqueTask>,
}

impl UniqueTaskGroup {

    pub fn new(name: String) -> Self {
        Self {
            name,
            tasks: HashMap::new(),
        }
    }

    pub fn add(&mut self, task_name: String) -> bool {
        if self.tasks.contains_key(&task_name) {
            return false;
        }

        self.tasks.insert(task_name.clone(), UniqueTask::new(task_name));
        true
    }

    pub fn remove(&mut self, task_name: &str) -> bool {
        !self.tasks.remove(task_name).is_none()
    }

    pub fn update(&mut self, msg: &TaskUpdate) -> Option<TaskUpdateTag> {
        for t in self.tasks.values_mut() {
            let tag = t.update(msg);
            if tag.is_some() {
                return tag;
            }
        }

        None
    }

    pub fn must_not_running(&self, task_name: &str) {
        if let Some(t) = self.tasks.get(task_name) {
            t.must_not_running();
        }
    }

    pub fn must_running(&self, task_name: &str) {
        if let Some(t) = self.tasks.get(task_name) {
            t.must_running();
        } else {
            panic!("Task {} must running.", task_name);
        }
    }
}

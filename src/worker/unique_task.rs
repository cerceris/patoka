use std::collections::{HashMap, HashSet};

use crate::{
    worker::{
        task::{TaskStatus},
        tracker::{self, TaskUpdate, TaskUpdateTag},
    },
};

#[derive(Clone)]
pub struct UniqueTask {
    name: String,
    uuid: Option<String>,
    parent_uuid: Option<String>,
}

impl UniqueTask {

    pub fn new(name: String) -> Self {
        Self {
            name,
            uuid: None,
            parent_uuid: None,
        }
    }

    pub fn new_with_parent(
        name: String,
        parent_uuid: Option<String>
    ) -> Self {
        Self {
            name,
            uuid: None,
            parent_uuid,
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

    pub parent_uuid: Option<String>,
}

impl UniqueTaskGroup {

    pub fn new(name: String) -> Self {
        Self {
            name,
            tasks: HashMap::new(),
            parent_uuid: None,
        }
    }

    pub fn new_with_parent(name: String, parent_uuid: String) -> Self {
        Self {
            name,
            tasks: HashMap::new(),
            parent_uuid: Some(parent_uuid),
        }
    }

    pub fn add(&mut self, task_name: String) -> bool {
        if self.tasks.contains_key(&task_name) {
            return false;
        }

        self.tasks.insert(
            task_name.clone(),
            UniqueTask::new_with_parent(
                task_name.clone(),
                self.parent_uuid.clone()
            ),
        );

        if let Some(p) = &self.parent_uuid {
            tracker::subscribe_by_name(task_name, p.clone());
        }

        true
    }

    pub fn remove(&mut self, task_name: &str) -> bool {
        let res = self.tasks.remove(task_name).is_some();
        if res {
            if let Some(p) = &self.parent_uuid {
                tracker::unsubscribe_by_name(task_name.into(), p.clone());
            }
        }
        res
    }

    pub fn update(&mut self, msg: &TaskUpdate) -> Option<TaskUpdateTag> {
        let mut tag = None;
        for t in self.tasks.values_mut() {
            tag = t.update(msg);
            if tag.is_some() {
                break;
            }
        }

        if let Some(TaskUpdateTag::Finished) = tag {
            self.remove(&msg.name);

            if let Some(p) = &self.parent_uuid {
                tracker::unsubscribe_by_name(msg.name.clone(), p.clone());
            }
        }

        tag
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

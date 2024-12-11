use actix::prelude::*;
use config::Value;
use lazy_static::lazy_static;
use regex::Regex;
use serde_json::json;
use serde_derive::{Deserialize};
use slog::Logger;
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File,  OpenOptions},
    io::prelude::*,
    sync::{Mutex, RwLock}
};

use crate::{
    core::{
        arbiter_pool,
        env,
        logger::create_logger,
    },
    worker::worker_message::*,
};

lazy_static! {
    static ref TASK_WRITERS: Mutex<TaskWriters> =
        Mutex::new(TaskWriters::new());

    static ref WRITERS_SETTINGS: RwLock<WritersSettings> =
        RwLock::new(WritersSettings::load());
}

struct TaskWriter {
    task_name: String,
    settings: WriterSettings,
    file_path: String,
    log: Logger,
}

impl TaskWriter {
    fn new(task_name: String, settings: WriterSettings) -> Self {
        let file_path = format!("data/tasks/{}", task_name);

        Self {
            log: create_logger(&format!("task_writer_{}", task_name)),
            task_name,
            settings,
            file_path,
        }
    }

    fn should_be_written(&self, msg: &WorkerMessage) -> bool {
        if let Some(_) = msg.result::<serde_json::Value>() {
            return self.settings.message_types.contains("task_result");
        }

        if let Some(_) = msg.question() {
            return self.settings.message_types.contains("task_question");
        }

        if let Some(_) = msg.error() {
            return self.settings.message_types.contains("error");
        }

        false
    }
}

impl Actor for TaskWriter {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(self.log, "Started.");

        // Create the output folder if needed.
        fs::create_dir_all("data/tasks").unwrap();

        // Create / truncate the output file.
        let mut file = OpenOptions::new()
            .read(false)
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.file_path).unwrap();
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Stopped.");
        remove_writer(&self.task_name);
    }
}

impl Handler<WorkerMessage> for TaskWriter {

    type Result = ();

    fn handle(
        &mut self,
        msg: WorkerMessage,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        if !self.should_be_written(&msg) {
            debug!(self.log, "Skip WORKER MESSAGE {:?}", msg);
            return;
        }

        debug!(self.log, "Write WORKER MESSAGE {:?}", msg);

        let data = json!(msg).to_string();

        let mut file = OpenOptions::new()
            .read(false)
            .append(true)
            .create(true)
            .open(&self.file_path).unwrap();

        file.write(data.as_bytes()).unwrap();
        file.write(b"\n").unwrap();
    }
}

struct TaskWriters {
    /// Task Name --> TaskWriter
    writers: HashMap<String, Addr<TaskWriter>>,

    log: Logger,
}

impl TaskWriters {
    fn new() -> Self {
        Self {
            writers: HashMap::new(),
            log: create_logger("task_writers"),
        }
    }

    fn get_writer(
        &mut self,
        task_name: &str
    ) -> Option<Recipient<WorkerMessage>> {
        if let Some(w) = self.writers.get(task_name) {
            info!(self.log, "Got task writer for [TASK NAME] {}", task_name);

            return Some(w.clone().recipient());
        }

        let settings = WRITERS_SETTINGS.read().unwrap();

        if let Some(s) = settings.get(task_name) {
            let w = self.create_writer(task_name.into(), s);
            return Some(w.clone().recipient());
        }

        info!(
            self.log,
            "No task writer specified for [TASK NAME] {}",
            task_name,
        );

        None
    }

    fn create_writer(
        &mut self,
        task_name: String,
        settings: WriterSettings,
    ) -> Addr<TaskWriter> {
        info!(
            self.log,
            "Create task writer for [TASK NAME] {} with settings {:?}",
            task_name,
            settings,
        );

        let arbiter_addr = arbiter_pool::next();

        let task_name_clone = task_name.clone();

        let task_writer_addr = TaskWriter::start_in_arbiter(
            &arbiter_addr,
            move |_| {
                TaskWriter::new(task_name_clone, settings)
            }
        );

        self.writers.insert(task_name, task_writer_addr.clone());
        task_writer_addr
    }

    fn remove_writer(&mut self, task_name: &str) {
        if let Some(_) = self.writers.remove(task_name) {
            info!(
                self.log,
                "Removed task writer for [TASK NAME] {}",
                task_name
            );
        } else {
            warn!(
                self.log,
                "Tried to remove task writer for unknown [TASK NAME] {}",
                task_name
            );
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct WriterSettings {
    message_types: HashSet<String>,
}

struct WritersSettings {
    /// Task Name Pattern --> Settings
    settings: HashMap<String, WriterSettings>,
}

impl WritersSettings {
    fn load() -> WritersSettings {
        let settings: HashMap<String, WriterSettings> =
            match env::load_opt("task_writers") {
                Some(v) => v,
                None => HashMap::new(),
            };

        //println!("Writers settings: {:?}", settings);

        Self {
            settings
        }
    }

    fn get(&self, task_name: &str) -> Option<WriterSettings> {
        for (task_name_pattern, settings) in &self.settings {
            let re = Regex::new(task_name_pattern).unwrap();

            if re.is_match(task_name) {
                return Some(settings.clone());
            }
        }

        None
    }
}

pub fn get_writer(task_name: &str) -> Option<Recipient<WorkerMessage>> {
    let mut task_writers = TASK_WRITERS.lock().unwrap();
    task_writers.get_writer(task_name)
}

/// Called by TaskWriter on stop.
fn remove_writer(task_name: &str) {
    let mut task_writers = TASK_WRITERS.lock().unwrap();
    task_writers.remove_writer(task_name);
}

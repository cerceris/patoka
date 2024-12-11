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
    io::{BufReader},
    sync::{Mutex, RwLock},
    time::Duration,
    thread,time,
};

use crate::{
    core::{
        arbiter_pool,
        env,
        logger::create_logger,
    },
    worker::{
        worker_message::*,
    },
};

lazy_static! {
    static ref TASK_READERS: Mutex<TaskReaders> =
        Mutex::new(TaskReaders::new());

    static ref READERS_SETTINGS: RwLock<ReadersSettings> =
        RwLock::new(ReadersSettings::load());
}

pub struct TaskReader {
    task_name: String,
    settings: ReaderSettings,
    client_addr: Option<Recipient<WorkerMessage>>,
    log: Logger,
}

impl TaskReader {
    fn new(task_name: String, settings: ReaderSettings) -> Self {
        Self {
            log: create_logger(&format!("task_reader_{}", task_name)),
            task_name,
            settings,
            client_addr: None,
        }
    }

    fn send_all(&mut self, ctx: &mut Context<Self>) {
        if self.client_addr.is_none() {
            panic!(
                "Client address is not provided for [TASK NAME] {}",
                self.task_name,
            );
        }

        let client_addr = self.client_addr.clone().unwrap();

        let file_path = format!("data/tasks/{}", self.task_name);

        let file = match File::open(&file_path) {
            Ok(f) => f,
            Err(e) => {
                error!(self.log, "Failed to open file {}", file_path);
                return;
            }
        };

        let reader = BufReader::new(file);

        let deserializer = serde_json::Deserializer::from_reader(reader);
        let iterator = deserializer.into_iter::<WorkerMessage>();

        // Send all messages to the task.
        let mut msg_counter = 0;
        for item in iterator {
            match item {
                Ok(wm) => {
                    if !self.should_be_sent(&wm) {
                        debug!(self.log, "Skip WORKER MESSAGE {:?}", wm);
                        continue;
                    }

                    debug!(self.log, "Send WORKER MESSAGE {:?}", wm);
                    client_addr.do_send(wm);
                    msg_counter += 1;
                },
                Err(e) => {
                    error!(
                        self.log,
                        "Encountered invalid worker message {:?}",
                        e,
                    );
                },
            }
        }

        if self.settings.loop_interval > 0 {
            info!(
                self.log,
                "Sent {} messages. Will read input file and send again in \
                    {} ms.",
                msg_counter,
                self.settings.loop_interval,
            );

            TimerFunc::new(
                Duration::from_millis(self.settings.loop_interval),
                Self::send_all
            ).spawn(ctx);
        } else {
            info!(
                self.log,
                "All {} messages have been sent to the task. Stopping the \
                    reader.",
                msg_counter
            );

            ctx.stop();
        }
    }

    fn should_be_sent(&self, msg: &WorkerMessage) -> bool {
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

impl Actor for TaskReader {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(self.log, "Started.");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Stopped.");
        remove_reader(&self.task_name);
    }
}

struct RegisterTask {
    pub task_name: String,
    pub client: Recipient<WorkerMessage>,
}

impl Message for RegisterTask {
    type Result = ();
}

impl Handler<RegisterTask> for TaskReader {

    type Result = ();

    fn handle(
        &mut self,
        msg: RegisterTask,
        ctx: &mut Self::Context
    ) -> Self::Result {

        if self.task_name != msg.task_name {
            panic!(
                "Tried to register unexpected [TASK NAME] {}",
                msg.task_name
            );
        }

        debug!(self.log, "Register [TASK NAME] {}", msg.task_name);

        self.client_addr = Some(msg.client);

        if self.settings.delay > 0 {
            TimerFunc::new(
                Duration::from_millis(self.settings.delay),
                Self::send_all
            ).spawn(ctx);
        } else {
            self.send_all(ctx);
        }
    }
}

struct TaskReaders {
    /// Task Name --> TaskReader
    readers: HashMap<String, Addr<TaskReader>>,

    log: Logger,
}

impl TaskReaders {
    fn new() -> Self {
        Self {
            readers: HashMap::new(),
            log: create_logger("task_readers"),
        }
    }

    fn get_reader(
        &mut self,
        task_name: &str
    ) -> Option<Addr<TaskReader>> {
        if let Some(r) = self.readers.get(task_name) {
            info!(self.log, "Got task reader for [TASK NAME] {}", task_name);

            return Some(r.clone());
        }

        let settings = READERS_SETTINGS.read().unwrap();

        if let Some(s) = settings.get(task_name) {
            let r = self.create_reader(task_name.into(), s);
            return Some(r.clone());
        }

        info!(
            self.log,
            "No task reader specified for [TASK NAME] {}",
            task_name,
        );

        None
    }

    fn create_reader(
        &mut self,
        task_name: String,
        settings: ReaderSettings,
    ) -> Addr<TaskReader> {
        info!(
            self.log,
            "Create task reader for [TASK NAME] {} with settings {:?}",
            task_name,
            settings,
        );

        let arbiter_addr = arbiter_pool::next();

        let task_name_clone = task_name.clone();

        let task_reader_addr = TaskReader::start_in_arbiter(
            &arbiter_addr,
            move |_| {
                TaskReader::new(task_name_clone, settings)
            }
        );

        self.readers.insert(task_name, task_reader_addr.clone());
        task_reader_addr
    }

    fn remove_reader(&mut self, task_name: &str) {
        if let Some(_) = self.readers.remove(task_name) {
            info!(
                self.log,
                "Removed task reader for [TASK NAME] {}",
                task_name
            );
        } else {
            warn!(
                self.log,
                "Tried to remove task reader for unknown [TASK NAME] {}",
                task_name
            );
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ReaderSettings {
    message_types: HashSet<String>,

    #[serde(default)]
    delay: u64,

    #[serde(default)]
    #[serde(rename = "loop")]
    loop_interval: u64,
}

struct ReadersSettings {
    /// Task Name Pattern --> Settings
    settings: HashMap<String, ReaderSettings>,
}

impl ReadersSettings {
    fn load() -> ReadersSettings {
        let settings: HashMap<String, ReaderSettings> =
            match env::load_opt("task_readers") {
                Some(v) => v,
                None => HashMap::new(),
            };

        //println!("Readers settings: {:?}", settings);

        Self {
            settings
        }
    }

    fn get(&self, task_name: &str) -> Option<ReaderSettings> {
        for (task_name_pattern, settings) in &self.settings {
            let re = Regex::new(task_name_pattern).unwrap();

            if re.is_match(task_name) {
                return Some(settings.clone());
            }
        }

        None
    }
}

pub fn register_task(
    reader_addr: &Addr<TaskReader>,
    client: Recipient<WorkerMessage>,
    task_name: String,
) {
    reader_addr.do_send(RegisterTask {
        client,
        task_name,
    });
}

pub fn get_reader(task_name: &str) -> Option<Addr<TaskReader>> {
    let mut task_readers = TASK_READERS.lock().unwrap();
    task_readers.get_reader(task_name)
}

/// Called by TaskReader on stop.
fn remove_reader(task_name: &str) {
    let mut task_readers = TASK_READERS.lock().unwrap();
    task_readers.remove_reader(task_name);
}

use actix::prelude::*;
use serde_json::json;
use slog::Logger;
use std::{
    collections::{HashMap, HashSet},
    mem,
    process::{Command, Child},
};

use crate::{
    control::{registry, message::*},
    core::{
        env::{self, *},
        logger::create_logger,
        monitor::*,
        timer::Timer,
    },
    worker::{
        controller_message::*,
        dispatcher::{self, TaskDispatcher},
        worker_message::*,
        plugin::*,
        state::*,
        task_writer,
    },
    transport::message::*,
};

struct ActiveClient {
    pub addr: Recipient<WorkerMessage>,
    pub task_writer: Option<Recipient<WorkerMessage>>,
}

pub struct WorkerController {
    /// Worker/Controller identifier.
    id: String,

    /// The logger instance.
    log: Logger,

    /// Dispatcher address.
    dispatcher_addr: Addr<TaskDispatcher>,

    /// Task UUID --> Client
    /// Route responses to clients.
    active_clients: HashMap<String, ActiveClient>,

    /// Worker process handle.
    worker_process: Option<Child>,

    /// The worker ZMQ identity. Used to route messages.
    identity: Identity,

    /// Current worker state.
    state: WorkerState,

    /// Delayed messages with `dest` Worker. Accumulated while the worker is
    /// not ready yet.
    delayed_worker_messages: Vec<WorkerMessage>,

    /// Delayed messages with `dest` Client. Accumulated while the client is
    /// not registered yet.
    delayed_client_messages: Vec<WorkerMessage>,

    /// The controller would handle only the tasks for those it has been
    /// reserved.
    reserved_tasks: HashSet<String>,

    /// Used to send `HeartbeatRequest` messages periodically.
    heartbeat_interval_timer: Timer<HeartbeatIntervalMessage>,

    /// Used to trigger an event when no `HeartbeatResponse` received in
    /// a specified amount of time.
    heartbeat_timeout_timer: Timer<HeartbeatTimeoutMessage>,

    /// Own address.
    own_addr: Option<Addr<WorkerController>>,

    /// Periodically generate status report.
    report_status_timer: ReportStatusTimer,

    /// `True` when the controller does not start the worker process but
    /// instead communicates with a process managed from outside.
    external_worker: bool,

    /// No heartbeats, the state is not checked and considered always ready.
    /// The identity is updated on every message from the worker.
    simple_protocol: bool,
}

impl WorkerController {
    pub fn new(id: String) -> Self {
        let logger_name = format!("worker_controller_{}", id);
        let log = create_logger(&logger_name);
        let state = WorkerState::new(id.clone(), log.clone());

        let external_worker =
            if let Some(v) = env::get_opt_var("general.external_worker") {
                v == "true"
            } else {
                false
            };

        let simple_protocol =
            if let Some(v) = env::get_opt_var("general.simple_protocol") {
                v == "true"
            } else {
                false
            };

        WorkerController {
            id,
            log,
            dispatcher_addr: dispatcher::start(),
            active_clients: HashMap::new(),
            worker_process: None,
            identity: new_identity(),
            state,
            delayed_worker_messages: vec![],
            delayed_client_messages: vec![],
            reserved_tasks: HashSet::new(),
            heartbeat_interval_timer: Timer::new_s(2),
            heartbeat_timeout_timer: Timer::new_s(10),
            own_addr: None,
            report_status_timer: ReportStatusTimer::new_s(5),
            external_worker,
            simple_protocol,
        }
    }

    fn create_worker_process(&mut self) {
        let main_path = env::full_path(
            "$PATOKA_X_DIR/build/src/main.js",
            "$PATOKA_X_DIR",
            &PATOKA_X_DIR,
        );

        let router_port = env::get_var("general.router_port");
        let args = [
            main_path,
            format!("--worker_id={}", self.id),
            format!(
                "--controller={}",
                "tcp://127.0.0.1:".to_string() + &router_port
            ),
        ];

        info!(self.log, "Creating worker process: node {:?}", args);

        let patoka_node_path = env::full_path(
            "$PATOKA_X_DIR/node_modules",
            "$PATOKA_X_DIR",
            &PATOKA_X_DIR,
        );
        let node_path_env = match std::env::var("NODE_PATH") {
            Ok(path) => {
                patoka_node_path + ";" + &path
            },
            Err(_) => {
                patoka_node_path
            },
        };
        self.worker_process =
            match Command::new("node").args(&args)
                .env("NODE_PATH", node_path_env)
                .spawn()
            {
                Ok(child) => {
                    self.state.starting();
                    Some(child)
                },
                Err(e) => {
                    self.state.error();
                    error!(self.log, "Failed to create worker process: {}", e);
                    None
                }
            };
    }

    fn recover_worker_process(&mut self) {
        if let Some(ref mut wp) = self.worker_process {
            if let Err(e) = wp.kill() {
                warn!(self.log, "Worker process killed with [ERROR] {}.", e);
            } else {
                debug!(self.log, "Worker process killed.");
            }

            match wp.wait() {
                Ok(exit_status) => {
                    debug!(self.log, "Exit status: {}.", exit_status);
                },
                Err(e) => {
                    warn!(self.log, "Exit status with [ERROR] {}.", e);
                },
            }
        }

        self.create_worker_process();
    }

    fn handle_controller_message(&mut self, msg: WorkerMessage) {
        let controller_msg = ControllerMessage::from(msg);
        match controller_msg {
            Ok(controller_msg) => {
                match controller_msg.subject {
                    Subject::Started => {
                        self.handle_started_message(controller_msg);
                    },
                    Subject::Ready => {
                        self.handle_ready_message();
                    },
                    Subject::PluginReady => {
                        self.handle_plugin_ready_message(controller_msg);
                    },
                    Subject::Error => {
                        self.handle_error_message(controller_msg);
                    },
                    Subject::HeartbeatResponse => {
                        self.handle_heartbeat_response(controller_msg);
                    },
                    Subject::ControlResponse => {
                        self.handle_control_response(controller_msg);
                    }
                    _ => {
                        warn!(
                            self.log,
                            "Ignore controller message with unexpected \
                                [SUBJECT] {:?}",
                            controller_msg.subject,
                        );
                    },
                }
            },
            Err(e) => {
                warn!(self.log, "Invalid controller message format: {}", e);
            }
        }
    }

    fn handle_started_message(&mut self, msg: ControllerMessage) {
        debug!(self.log, "Worker process has started.");
        self.identity = msg.identity;

        // Start heartbeat timers.
        if !self.external_worker {
            self.handle_worker_alive_status();
        }

        self.handle_ready_message();
    }

    fn handle_ready_message(&mut self) {
        trace!(self.log, "Worker process is ready.");
        self.state.ready();
        self.send_delayed_messages();
    }

    fn handle_plugin_ready_message(&mut self, msg: ControllerMessage) {
        if let Some(plugin_name) = msg.details.get("name") {
            debug!(self.log, "Worker plugin has been set up.");
            let plugin = WorkerPlugin::from_str(plugin_name.as_str().unwrap());
            self.state.plugin(plugin);
            self.state.ready();
            self.send_delayed_messages();
        } else {
            warn!(
                self.log,
                "Invalid plugin_ready message. No details.name found."
            );
        }
    }

    fn handle_error_message(&mut self, msg: ControllerMessage) {
        if let Some(message) = msg.details.get("message") {
            warn!(
                self.log,
                "Received error message from worker: {}",
                message.as_str().unwrap()
            );
        } else {
            warn!(
                self.log,
                "Invalid error message received from the worker. \
                    No details.message found."
            );
        }
    }

    fn handle_heartbeat_response(&mut self, msg: ControllerMessage) {
        if self.external_worker {
            self.identity = msg.identity;

            if self.state.is_initial() {
                // Stop all running tasks in the worker.
                let cm = ControllerMessage::new(
                    self.id.clone(),
                    Dest::Worker,
                    Subject::Custom("stop_all".into()),
                );

                self.send_urgent_message_to_worker(cm.into());
                self.state.busy();
            }
        } else {
            self.handle_worker_alive_status();
        }
    }

    fn handle_worker_alive_status(&self) {
        if let Some(ref own_addr) = self.own_addr {
            own_addr.do_send(HeartbeatResponseReceivedMessage::default());
        } else {
            panic!("Controller own address is not set.");
        }
    }

    fn handle_control_response(&mut self, msg: ControllerMessage) {
        match serde_json::from_value(msg.details.clone()) {
            Ok(m) => {
                debug!(self.log, "[CMD RESP] {:?}", m);

                registry::send(m);
            },
            Err(_) => {
                error!(
                    self.log,
                    "Invalid control message format: {:?}",
                    msg.details,
                );
            }
        }
    }

    fn send_delayed_messages(&mut self) {
        debug!(
            self.log,
            "There are {} delayed worker messages; {} delayed client \
                messages to send.",
            self.delayed_worker_messages.len(),
            self.delayed_client_messages.len()
        );

        let messages = mem::take(&mut self.delayed_worker_messages);
        for msg in messages {
            self.send_regular_message_to_worker(msg);
        }

        let messages = mem::take(&mut self.delayed_client_messages);
        for msg in messages {
            self.send_message_to_client(msg);
        }
    }

    /// Send a regular (usually from a client) message to the worker.
    fn send_regular_message_to_worker(&mut self, msg: WorkerMessage) {
        // Check whether we know who is the task client.
        if !self.active_clients.contains_key(&msg.payload.task_uuid) {
            debug!(self.log,
                "A client for [TASK UUID] {} has not registered yet. Put the \
                    message to the delayed messages queue.",
                msg.payload.task_uuid,
            );
            self.put_message_to_delayed_queue(msg);
            return;
        }

        // Are the worker ready?
        if !self.simple_protocol && !self.state.is_ready() {
            debug!(
                self.log,
                "Worker process is not ready yet. Put the message to \
                    the delayed messages queue."
            );
            self.put_message_to_delayed_queue(msg);
            return;
        }

        // Check the plugin.
        if !self.simple_protocol {
            let desired_plugin = WorkerPlugin::from_str(&msg.payload.plugin);
            if !self.state.is_plugin(desired_plugin) {
                debug!(
                    self.log,
                    "Worker plugin will be changed. Put the message to \
                        the delayed messages queue."
                );
                self.put_message_to_delayed_queue(msg);
                self.setup_worker_plugin(desired_plugin);
                return;
            }
        }

        // Now the message can be sent.
        self.send_message_to_worker(msg);

        if !self.simple_protocol {
            self.state.busy();
        }
    }

    /// Send an urgent (e.g. control) message to the worker.
    fn send_urgent_message_to_worker(&mut self, msg: WorkerMessage) {
        debug!(self.log, "[URGENT] {:?}", msg);

        self.send_message_to_worker(msg);
    }

    fn send_message_to_worker(&mut self, mut msg: WorkerMessage) {
        msg.identity = Identity::from(&self.identity as &[u8]);
        self.dispatcher_addr.do_send(msg);
    }

    fn put_message_to_delayed_queue(&mut self, msg: WorkerMessage) {
        self.delayed_worker_messages.push(msg);
    }

    fn is_reserved_for_task(&self, task_uuid: &str) -> bool {
        self.reserved_tasks.contains(task_uuid)
    }

    fn reserve_for_task(&mut self, task_uuid: &str) {
        self.reserved_tasks.insert(task_uuid.to_string());
    }

    /// Forward `message` to the respective client.
    fn send_message_to_client(&mut self, msg: WorkerMessage) {
        if let Some(c) = self.active_clients.get(&msg.payload.task_uuid) {
            self.identity = clone_identity(&msg.identity);
            if let Some(addr) = &c.task_writer {
                addr.do_send(msg.clone());
            }

            c.addr.do_send(msg);
        } else {
            warn!(
                self.log,
                "Could not forward a message to a client because \
                    no client is associated with [TASK UUID] {}",
                msg.payload.task_uuid
            );
            self.delayed_client_messages.push(msg);
        }
    }

    fn setup_worker_plugin(&mut self, plugin: WorkerPlugin) {
        debug!(self.log, "Setup worker plugin {:?}", plugin);
        let msg = setup_plugin_message(plugin, &self.id);
        self.send_urgent_message_to_worker(msg);
        self.state.busy();
    }

    fn handle_stop_task(
        &mut self,
        msg: StopTask,
        ctx: &mut <Self as Actor>::Context,
    ) {
        let cm = ControlMessage::request(
            &msg.task_uuid,
            &msg.task_uuid,
            "stop_task"
        );

        self.send_urgent_message_to_worker(
            create_control_request(self.id.to_string(), cm).into()
        );
    }

    fn handle_close_task(
        &mut self,
        msg: CloseTask,
        ctx: &mut <Self as Actor>::Context,
    ) {
        self.active_clients.remove(&msg.task_uuid);
    }
}

impl Actor for WorkerController {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(self.log, "Started.");

        self.own_addr = Some(ctx.address());

        // Register itself on the Dispatcher.
        self.dispatcher_addr.do_send(dispatcher::RegisterController {
            controller_id: self.id.clone(),
            controller_addr: ctx.address(),
        });

        // Create worker process that is managed by the controller.
        if self.external_worker {
            info!(self.log, "Will be using an external worker.");
        } else {
            self.create_worker_process();
        }

        self.report_status_timer.reset::<Self>(ctx);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Stopped.");
    }
}

impl Handler<WorkerMessage> for WorkerController {

    type Result = ();

    fn handle(
        &mut self,
        msg: WorkerMessage,
        _ctx: &mut Self::Context
    ) -> Self::Result {

        //trace!(self.log, "Received message: {}",  msg.payload.header());

        match msg.payload.dest {
            Dest::Controller => {
                // A message for itself.
                self.handle_controller_message(msg);
            },
            Dest::Client => {
                // A message from the worker to a client.
                self.send_message_to_client(msg);
            },
            Dest::Worker => {
                if !self.is_reserved_for_task(&msg.payload.task_uuid) {
                    warn!(self.log,
                        "Rejecting unexpected [TASK UUID] {}",
                        msg.payload.task_uuid
                    );
                } else {
                    // A message from a client to the worker.
                    self.send_regular_message_to_worker(msg);
                }
            }
            _ => {
                // Unknown dest.
            }
        }
    }
}

struct RegisterClient {
    pub task_uuid: String,
    pub task_name: String,
    pub client: Recipient<WorkerMessage>,
}

impl Message for RegisterClient {
    type Result = ();
}

impl Handler<RegisterClient> for WorkerController {
    type Result = ();

    fn handle(
        &mut self,
        msg: RegisterClient,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        info!(self.log, "Register a client for [TASK UUID] {}", msg.task_uuid);

        let active_client = ActiveClient {
            addr: msg.client,
            task_writer: task_writer::get_writer(&msg.task_name),
        };

        self.active_clients.insert(msg.task_uuid, active_client);
        self.send_delayed_messages();
    }
}

/// Reserve the controller to process the given task.
/// It is possible for controller to process more than one task simultaneously.
/// The capability to do so is determined by the controller's `state`.
pub struct ReserveForTask {
    pub task_uuid: String,
}

impl Message for ReserveForTask {
    type Result = bool;
}

impl Handler<ReserveForTask> for WorkerController {
    type Result = bool;

    fn handle(
        &mut self,
        msg: ReserveForTask,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        if !self.state.is_ready() && !self.state.is_starting()
            && !(self.external_worker && self.state.is_initial()) {
            debug!(
                self.log,
                "Unable to reserve the controller for [TASK UUID] {} [STATE] \
                    {:?}",
                msg.task_uuid,
                self.state.current_state(),
            );
            false
        } else {
            debug!(
                self.log,
                "Reserved the controller for [TASK UUID] {}",
                msg.task_uuid
            );
            self.reserve_for_task(&msg.task_uuid);
            true
        }
    }
}

#[derive(Clone, Default, Message)]
#[rtype(result = "()")]
pub struct HeartbeatIntervalMessage {
}

impl Handler<HeartbeatIntervalMessage> for WorkerController {
    type Result = ();

    fn handle(
        &mut self,
        _msg: HeartbeatIntervalMessage,
        ctx: &mut Self::Context
    ) -> Self::Result {
        let heartbeat_request = ControllerMessage::with_identity(
            self.id.clone(),
            Dest::Worker,
            Subject::HeartbeatRequest,
            clone_identity(&self.identity),
        );
        self.send_message_to_worker(heartbeat_request.into());

        self.heartbeat_interval_timer.reset::<Self>(ctx);
    }
}

#[derive(Clone, Default, Message)]
#[rtype(result = "()")]
pub struct HeartbeatTimeoutMessage {
}

impl Handler<HeartbeatTimeoutMessage> for WorkerController {
    type Result = ();

    fn handle(
        &mut self,
        _msg: HeartbeatTimeoutMessage,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        warn!(
            self.log,
            "Worker is not responding on heartbeat requests. Will try to \
                recover the worker process."
        );
        self.state.error();
        self.recover_worker_process();
    }
}

#[derive(Clone, Default, Message)]
#[rtype(result = "()")]
pub struct HeartbeatResponseReceivedMessage {
}

impl Handler<HeartbeatResponseReceivedMessage> for WorkerController {
    type Result = ();

    fn handle(
        &mut self,
        _msg: HeartbeatResponseReceivedMessage,
        ctx: &mut Self::Context
    ) -> Self::Result {
        self.heartbeat_interval_timer.reset::<Self>(ctx);
        self.heartbeat_timeout_timer.reset::<Self>(ctx);
    }
}

impl Handler<ReportStatusMessage> for WorkerController {
    type Result = ();

    fn handle(
        &mut self,
        _msg: ReportStatusMessage,
        ctx: &mut Self::Context
    ) -> Self::Result {
        let number_of_active_clients = self.active_clients.len();
        /*info!(
            self.log,
            "[STATUS] Number of active clients: {}.",
            number_of_active_clients,
        );*/

        self.report_status_timer.reset::<Self>(ctx);
    }
}

impl Handler<ControlMessage> for WorkerController {
    type Result = ();

    fn handle(
        &mut self,
        msg: ControlMessage,
        ctx: &mut Self::Context
    ) -> Self::Result {

        match msg.type_ {
            Type::Response =>  {
            },
            Type::Request => {
                self.send_urgent_message_to_worker(
                    create_control_request(self.id.to_string(), msg).into()
                );
            },
            _ => {
                warn!(self.log, "Unsupported control message type.");
            }
        };
    }
}

handler_impl_stop_task!(WorkerController);
handler_impl_close_task!(WorkerController);

pub fn start_task(
    controller_addr: &Addr<WorkerController>,
    msg: WorkerMessage,
    client: Recipient<WorkerMessage>,
    task_name: String,
) {
    controller_addr.do_send(RegisterClient {
        task_uuid: msg.payload.task_uuid.clone(),
        client,
        task_name,
    });

    controller_addr.do_send(msg);
}

use actix::prelude::*;
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use crate::{
    center::{
        connector,
        message::CenterMessage,
        send::*,
    },
    control::{
        message::{CloseTask, ControlMessage},
        registry,
    },
    core::{
        app_state,
        logger::create_logger,
        monitor::*,
    },
    transport::message::RawMessage,
    worker::{
        task::{TaskStatus},
        task_assistant::self,
        task_tree::{self, TaskTree},
    },
};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum TaskUpdateTag {
    Unknown = 0,
    Started = 1,
    Updated = 2,
    Finished = 3,
    Question = 4,
}

#[derive(Clone, Debug)]
pub struct TaskUpdate {
    pub task_uuid: String,

    /// Task Name
    pub name: String,

    pub status: TaskStatus,
    pub center_msg: Option<RawMessage>,

    /// 0 = unknown; 1 = started; 2 = updated (current state); 3 = finished;
    /// 4 = task question.
    pub tag: TaskUpdateTag,
}

impl TaskUpdate {
    pub fn new(
        task_uuid: String,
        status: TaskStatus,
        tag: TaskUpdateTag,
        name: String,
    ) -> Self {
        Self {
            task_uuid,
            status,
            center_msg: None,
            tag,
            name,
        }
    }

    pub fn with_center_msg(
        task_uuid: String,
        status: TaskStatus,
        center_msg: CenterMessage,
        tag: TaskUpdateTag,
        name: String,
    ) -> Self {
        Self {
            task_uuid,
            status,
            center_msg: Some(RawMessage::from(center_msg)),
            tag,
            name,
        }
    }

    pub fn str_short(&self) -> String {
        format!(
            "TASK UPDATE [TASK UUID] {} [NAME] {} [STATUS] {:?} [TAG] {:?}",
            self.task_uuid,
            self.name,
            self.status,
            self.tag,
        )
    }
}

impl Message for TaskUpdate {
    type Result = ();
}

#[macro_export]
macro_rules! handler_impl_task_update {
    ($x:ty) => {
        impl Handler<TaskUpdate> for $x {
            type Result = ();

            fn handle(
                &mut self,
                msg: TaskUpdate,
                ctx: &mut Self::Context
            ) -> Self::Result {
                self.handle_task_update(msg, ctx);
            }
        }
    }
}

type TaskSubscriber = Recipient<TaskUpdate>;

/// UUID --> TaskSubscriber
type TaskSubscribers = HashMap<String, TaskSubscriber>;

pub struct TaskSubscription {
    /// True to subscribe, False to unsubscribe.
    subscribe: bool,

    task_uuid: String,

    subscriber_uuid: String,

    /// To subscribe by task name.
    name: String,

    /// Subscribe/unsubscribe by name.
    by_name: bool,

    /// If None, a subscription is possible for the already registered
    /// recipient `subscriber_uuid`.
    subscriber: Option<TaskSubscriber>,
}

impl TaskSubscription {
    pub fn subscribe(
        task_uuid: String,
        subscriber_uuid: String,
        subscriber: TaskSubscriber,
    ) -> Self {
        Self {
            subscribe: true,
            task_uuid,
            subscriber_uuid,
            name: String::new(),
            by_name: false,
            subscriber: Some(subscriber),
        }
    }

    pub fn unsubscribe(task_uuid: String, subscriber_uuid: String) -> Self {
        Self {
            subscribe: false,
            task_uuid,
            subscriber_uuid,
            name: String::new(),
            by_name: false,
            subscriber: None,
        }
    }

    pub fn subscribe_no_addr(
        task_uuid: String,
        subscriber_uuid: String,
        name: String,
        by_name: bool,
    ) -> Self {
        Self {
            subscribe: true,
            task_uuid,
            subscriber_uuid,
            name,
            by_name,
            subscriber: None,
        }
    }

    pub fn unsubscribe_by_name(name: String, subscriber_uuid: String) -> Self {
        Self {
            subscribe: false,
            task_uuid: String::new(),
            subscriber_uuid,
            name,
            by_name: true,
            subscriber: None,
        }
    }
}

impl Message for TaskSubscription {
    type Result = ();
}

struct RegisterTaskUpdateRecipient {
    /// True to register, False to unregister.
    register: bool,

    id: String,

    addr: Option<TaskSubscriber>,
}

impl RegisterTaskUpdateRecipient {
    pub fn register(id: String, addr: TaskSubscriber) -> Self {
        Self {
            register: true,
            id,
            addr: Some(addr),
        }
    }

    pub fn unregister(id: String) -> Self {
        Self {
            register: false,
            id,
            addr: None,
        }
    }
}

impl Message for RegisterTaskUpdateRecipient {
    type Result = ();
}

struct TrackerItem {
    task_uuid: String,
    subscribers: TaskSubscribers,

    /// Tag --> Message
    center_messages: HashMap<TaskUpdateTag, RawMessage>,
}

impl TrackerItem {
    pub fn new(task_uuid: String) -> Self {
        Self {
            task_uuid,
            subscribers: TaskSubscribers::new(),
            center_messages: HashMap::new(),
        }
    }

    pub fn debug_info(&self) -> String {
        format!("[TRACKER ITEM] [TASK UUID] {} [SUBSCRIBERS] {} \
            [CENTER MESSAGES] {}",
            self.task_uuid,
            self.subscribers.len(),
            self.center_messages.len()
        )
    }
}

pub struct TaskTracker {
    log: Logger,

    /// Task UUID --> Item
    items: HashMap<String, TrackerItem>,

    /// Periodically generate status report.
    report_status_timer: ReportStatusTimer,

    task_tree_addr: Addr<TaskTree>,

    /// ID --> Recipient
    task_update_recipients: HashMap<String, TaskSubscriber>,

    /// Task Name --> Subscribers
    subscribers_by_name: HashMap<String, TaskSubscribers>,
}

impl TaskTracker {
    fn subscribe(&mut self, msg: TaskSubscription) {
        let subscriber = self.get_recipient(&msg);

        if msg.by_name {
            if msg.name.is_empty() {
                panic!("Tried to subscribe by name but the name is empty.");
            }

            if let Some(s) = self.subscribers_by_name.get_mut(&msg.name) {
                s.insert(msg.subscriber_uuid.clone(), subscriber);
            } else {
                let mut s = HashMap::new();
                s.insert(msg.subscriber_uuid.clone(), subscriber);
                self.subscribers_by_name.insert(msg.name.clone(), s);
            }

            debug!(
                self.log,
                "Subscribed [SUBSCRIBER UUID] {} to [NAME] {}",
                msg.subscriber_uuid,
                msg.name,
            );

            return;
        }

        // Check if the subscriber is already subscribed to the task by name.
        if let Some(s) = self.subscribers_by_name.get(&msg.name) {
            if s.contains_key(&msg.subscriber_uuid) {
                debug!(
                    self.log,
                    "[SUBSCRIBER UUID] {} is already subscribed to \
                        [TASK UUID] {} by [NAME] {}",
                    msg.subscriber_uuid,
                    msg.task_uuid,
                    msg.name,
                );

                return;
            }
        }

        if let Some(item) = self.items.get_mut(&msg.task_uuid) {
            item.subscribers.insert(msg.subscriber_uuid.clone(), subscriber);
        } else {
            debug!(self.log, "Create item [TASK UUID] {}", msg.task_uuid);

            let mut item = TrackerItem::new(msg.task_uuid.clone());
            item.subscribers.insert(msg.subscriber_uuid.clone(), subscriber);
            self.items.insert(msg.task_uuid.clone(), item);
        }

        debug!(
            self.log,
            "Subscribed [SUBSCRIBER UUID] {} to [TASK UUID] {}",
            msg.subscriber_uuid,
            msg.task_uuid,
        );
    }

    fn unsubscribe(&mut self, msg: TaskSubscription) {
        if msg.by_name {
            if msg.name.is_empty() {
                panic!("Tried to unsubscribe by name but the name is empty.");
            }

            if let Some(s) = self.subscribers_by_name.get_mut(&msg.name) {
                s.remove(&msg.subscriber_uuid);
            } else {
                panic!(
                    "Tried to unsubscribe from unknown [NAME] {}",
                    msg.name,
                );
            }

            debug!(
                self.log,
                "Unsubscribed [SUBSCRIBER UUID] {} from [NAME] {}",
                msg.subscriber_uuid,
                msg.name,
            );

            return;
        }

        if let Some(item) = self.items.get_mut(&msg.task_uuid) {
            item.subscribers.remove(&msg.subscriber_uuid);
        } else {

        }

        debug!(
            self.log,
            "Unsubscribed [SUBSCRIBER UUID] {} from [TASK UUID] {}",
            msg.subscriber_uuid,
            msg.task_uuid,
        );
    }

    fn handle_control_msg(&self, msg: ControlMessage) {
        debug!(self.log, "[CONTROL] {:?}", msg);

        match msg.cmd.as_ref() {
            "send_center_messages" => {
                self.cmd_send_center_messages(msg);
            },
            _ => {
                warn!(self.log, "Unknown [CMD] {}", msg.cmd)
            }
        }
    }

    fn cmd_send_center_messages(&self, msg: ControlMessage) {
        let task_uuid = &msg.orig_id;

        if let Some(item) = self.items.get(task_uuid) {
            debug!(
                self.log,
                "[CMD SEND CENTER MESSAGES] [TASK UUID] {} [MESSAGES] {}",
                task_uuid,
                item.center_messages.len(),
            );

            let connector_addr = connector::start();

            // When we have 3 (finished), 4 (task question) is not expected.
            let tag_orger = vec![
                TaskUpdateTag::Started,
                TaskUpdateTag::Updated,
                TaskUpdateTag::Finished,
                TaskUpdateTag::Question,
            ];

            for tag in tag_orger {
                if let Some(c_msg) = item.center_messages.get(&tag) {
                    connector_addr.do_send(c_msg.clone());
                }
            }

        } else {
            warn!(
                self.log,
                "[CMD SEND CENTER MESSAGES] Unknown [TASK UUID] {}",
                task_uuid,
            );
        }
    }

    fn handle_task_update(
        &mut self,
        msg: TaskUpdate,
        _ctx: &mut <Self as Actor>::Context
    ) {
        //debug!(self.log, "Received task update {:?}", msg);

        let msg_short = TaskUpdate::new(
            msg.task_uuid.clone(),
            msg.status,
            msg.tag,
            msg.name.clone(),
        );

        if !self.items.contains_key(&msg.task_uuid) {
            debug!(
                self.log,
                "Create item [TASK UUID] {}",
                msg.task_uuid
            );

            let item = TrackerItem::new(msg.task_uuid.clone());
            self.items.insert(msg.task_uuid.clone(), item);
        }

        // Forward the update message to all the task subscribers.
        let item = self.items.get_mut(&msg.task_uuid).unwrap();

        for s in item.subscribers.values() {
            //if let Err(e) = s.do_send(msg_short.clone()) {
            if let Err(e) = s.try_send(msg_short.clone()) {
                panic!(
                    "Failed to send task status update to subscriber \
                        [ERROR] {}",
                    e
                );
            }
        }

        if let(Some(c_msg)) = msg.center_msg {
            connector::start().do_send(c_msg.clone());
            item.center_messages.insert(msg.tag, c_msg);
        }

        // Subscribers by name.
        if let Some(subscribers) = self.subscribers_by_name.get(&msg.name) {
            for s in subscribers.values() {
                s.do_send(msg_short.clone());
            }
        } else {
            debug!(
                self.log,
                "No subscribers by name [NAME] {}",
                msg.name,
            );
        }

        // Always send to the task tree.
        self.task_tree_addr.do_send(msg_short.clone());

        // Always send to the task assistant.
        task_assistant::start().do_send(msg_short.clone());

        // Always send to the app state.
        app_state::start().do_send(msg_short.clone());

        debug!(self.log, "{}", item.debug_info());

        if msg_short.status == TaskStatus::FinishedSuccess ||
            msg_short.status == TaskStatus::FinishedFailure
        {
            // Remove the task's subscriptions to other tasks and the other
            // tasks' subscriptions to the task.
            self.task_update_recipients.remove(&msg_short.task_uuid);

            for item in self.items.values_mut() {
                item.subscribers.remove(&msg_short.task_uuid);
            }

            for subscribers in self.subscribers_by_name.values_mut() {
                subscribers.remove(&msg_short.task_uuid);
            }

            // The item is removed when the task is closed.
        }
    }

    fn handle_close_task(
        &mut self,
        msg: CloseTask,
        ctx: &mut <Self as Actor>::Context,
    ) {
        self.items.remove(&msg.task_uuid);
        send_center_task_closed(&msg.task_uuid);
        app_state::start().do_send(msg);
    }

    fn register_task_update_recipient(
        &mut self,
        id: String,
        addr: TaskSubscriber
    ) {
        if let Some(v) = self.task_update_recipients.insert(id.clone(), addr) {
            panic!(
                "Tried to register task update recipient multiple times \
                    [ID] {}.",
                id,
            );
        } else {
            debug!(self.log, "Registered task update recipient [ID] {}.", id);
        }
    }

    fn unregister_task_update_recipient(&mut self, id: &str) {
        if let Some(v) = self.task_update_recipients.remove(id) {
            debug!(
                self.log,
                "Unregistered task update recipient [ID] {}.",
                id,
            );
        }
    }

    fn get_recipient(&self, msg: &TaskSubscription) -> TaskSubscriber {
        match msg.subscriber {
            Some(ref s) => s.clone(),
            None => {
                if let Some(s) = self.task_update_recipients.get(
                    &msg.subscriber_uuid
                ) {
                    s.clone()
                } else {
                    panic!(
                        "Unknown task update recipient [ID] {}.",
                        msg.subscriber_uuid,
                    );
                }
            }
        }
    }
}

impl Default for TaskTracker {
    fn default() -> Self {
        TaskTracker {
            log: create_logger("task_tracker"),
            items: HashMap::new(),
            report_status_timer: ReportStatusTimer::new_s(5),
            task_tree_addr: task_tree::start(),
            task_update_recipients: HashMap::new(),
            subscribers_by_name: HashMap::new(),
        }
    }
}

impl Actor for TaskTracker {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(self.log, "Task Tracker started.");

        ctx.set_mailbox_capacity(1000000);

        registry::register(
            "task_tracker".to_string(),
            ctx.address().recipient::<ControlMessage>(),
        );

        self.report_status_timer.reset::<Self>(ctx);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Task Tracker stopped.");
    }
}

impl Handler<TaskSubscription> for TaskTracker {
    type Result = ();

    fn handle(
        &mut self,
        msg: TaskSubscription,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        if msg.subscribe {
            self.subscribe(msg);
        } else {
            self.unsubscribe(msg);
        }
    }
}

impl Handler<RegisterTaskUpdateRecipient> for TaskTracker {
    type Result = ();

    fn handle(
        &mut self,
        msg: RegisterTaskUpdateRecipient,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        if msg.register {
            self.register_task_update_recipient(msg.id, msg.addr.unwrap());
        } else {
            self.unregister_task_update_recipient(&msg.id);
        }
    }
}

handler_impl_task_update!(TaskTracker);
handler_impl_close_task!(TaskTracker);

impl Supervised for TaskTracker {}

impl SystemService for TaskTracker {
    fn service_started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Task Tracker system service started.")
    }
}

impl Handler<ReportStatusMessage> for TaskTracker {
    type Result = ();

    fn handle(
        &mut self,
        _msg: ReportStatusMessage,
        ctx: &mut Self::Context
    ) -> Self::Result {
        let number_of_tracking_tasks = self.items.len();

        /*info!(
            self.log,
            "[STATUS] Number of tracking tasks: {}.",
            number_of_tracking_tasks,
        );*/

        self.report_status_timer.reset::<Self>(ctx);
    }
}

impl Handler<ControlMessage> for TaskTracker {
    type Result = ();

    fn handle(
        &mut self,
        msg: ControlMessage,
        ctx: &mut Self::Context
    ) -> Self::Result {
        self.handle_control_msg(msg);
    }
}

struct DismissTaskQuestion {
    pub task_uuid: String,
}

impl Message for DismissTaskQuestion {
    type Result = ();
}

impl Handler<DismissTaskQuestion> for TaskTracker {
    type Result = ();

    fn handle(
        &mut self,
        msg: DismissTaskQuestion,
        ctx: &mut Self::Context
    ) -> Self::Result {
        debug!(
            self.log,
            "Dismiss task question [TASK UUID] {}",
            msg.task_uuid
        );

        if let Some(item) = self.items.get_mut(&msg.task_uuid) {
            match item.center_messages.remove(&TaskUpdateTag::Question) {
                None => {
                    warn!(
                        self.log,
                        "No active task question [TASK UUID] {}",
                        msg.task_uuid
                    );
                },
                _ => {
                    debug!(
                        self.log,
                        "Dismissed task question [TASK UUID] {}",
                        msg.task_uuid
                    );
                },
            }
        } else {
            warn!(
                self.log,
                "Attempted to dismiss question for unknown [TASK UUID] {}",
                msg.task_uuid
            );
        }
    }
}

pub fn send(
    task_uuid: String,
    status: TaskStatus,
    center_msg: CenterMessage,
    tag: TaskUpdateTag,
    name: String,
) {
    start().do_send(TaskUpdate::with_center_msg(
        task_uuid,
        status,
        center_msg,
        tag,
        name,
    ));
}

pub fn dismiss_task_question(task_uuid: String) {
    start().do_send::<DismissTaskQuestion>(DismissTaskQuestion { task_uuid });
}

pub fn register_task_update_recipient(
    id: String,
    addr: TaskSubscriber,
) {
    start().do_send(RegisterTaskUpdateRecipient::register(id, addr));
}

pub fn unregister_task_update_recipient(id: String) {
    start().do_send(RegisterTaskUpdateRecipient::unregister(id));
}

pub fn subscribe(
    task_uuid: String,
    subscriber_uuid: String,
    subscriber: TaskSubscriber
) {
    start().do_send(
        TaskSubscription::subscribe(task_uuid, subscriber_uuid, subscriber)
    );
}

pub fn subscribe_no_addr(
    task_uuid: String,
    subscriber_uuid: String,
    name: String,
    by_name: bool,
) {
    start().do_send(
        TaskSubscription::subscribe_no_addr(
            task_uuid,
            subscriber_uuid,
            name,
            by_name,
        )
    );
}

pub fn subscribe_by_name(name: String, subscriber_uuid: String) {
    start().do_send(
        TaskSubscription::subscribe_no_addr(
            String::new(),
            subscriber_uuid,
            name,
            true,
        )
    );
}

pub fn unsubscribe(task_uuid: String, subscriber_uuid: String) {
    start().do_send(
        TaskSubscription::unsubscribe(task_uuid, subscriber_uuid)
    );
}

pub fn unsubscribe_by_name(name: String, subscriber_uuid: String) {
    start().do_send(
        TaskSubscription::unsubscribe_by_name(name, subscriber_uuid)
    );
}

pub fn start() -> Addr<TaskTracker> {
    TaskTracker::from_registry()
}

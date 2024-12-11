use actix::prelude::*;
use serde_derive::{Deserialize, Serialize};
use slog::Logger;
use std::collections::HashSet;
use uuid::Uuid;

use crate::{
    center::{
        connector::{self, CenterConnector},
        message,
    },
    control::message::*,
    core::{
        env,
        logger::create_logger,
        monitor::*,
        timestamp::*,
    },
    handler_impl_task_update,
    transport::message::RawMessage,
    worker::tracker::*,
};

#[derive(Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AppStatus {
    Running,
    Idle,
    Error,
    Unknown,
}

pub struct AppState {
    log: Logger,

    /// Application ID.
    app_id: String,

    /// Application name.
    app_name: String,

    /// UI URL
    url: String,

    status: AppStatus,

    started_at: Timestamp,

    /// { Task UUID }
    /// Tasks in all states including Finished.
    /// Task is removed from the list when Closed.
    active_task_uuids: HashSet<String>,

    /// Periodically generate status report.
    report_status_timer: ReportStatusTimer,

    center_connector_addr: Addr<CenterConnector>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AppStatusReport {
    pub app_id: String,

    pub app_name: String,

    pub url: String,

    pub status: AppStatus,

    pub started_at: Timestamp,

    pub active_task_uuids: HashSet<String>,
}

impl AppStatusReport {
    pub fn status_as_str(&self) -> &'static str {
        match self.status {
            AppStatus::Running => "running",
            AppStatus::Idle => "idle",
            AppStatus::Error => "error",
            _  => "unknown",
        }
    }

    pub fn status_from_str(s: &str) -> AppStatus {
        match s {
            "running" => AppStatus::Running,
            "idle" => AppStatus::Idle,
            "error" => AppStatus::Error,
            _ => AppStatus::Unknown,
        }
    }

    pub fn compare_attributes(&self, report: &Self) -> bool {
        self.app_name == report.app_name && self.url == report.url
    }
}

impl AppState {
    fn generate_status_report(&self) {
        //debug!(self.log, "Generate status report.");

        let report = AppStatusReport {
            app_id: self.app_id.clone(),
            app_name: self.app_name.clone(),
            url: self.url.clone(),
            status: self.status,
            started_at: self.started_at.clone(),
            active_task_uuids: self.active_task_uuids.clone(),
        };

        let c_msg = message::create(
            message::Dest::Center,
            message::Subject::AppStatusReport,
            self.app_id.clone(),
            "status_report".to_string(),
            report,
        );

        self.center_connector_addr.do_send(RawMessage::from(c_msg));
    }

    fn determine_status(&mut self) {
        if self.active_task_uuids.len() > 0 {
            self.status = AppStatus::Running;
        } else {
            self.status = AppStatus::Idle;
        }
    }

    fn handle_task_update(
        &mut self,
        msg: TaskUpdate,
        ctx: &mut <Self as Actor>::Context
    ) {
        if msg.tag != TaskUpdateTag::Started {
            return;
        }

        self.active_task_uuids.insert(msg.task_uuid.clone());

        info!(
            self.log,
            "New [TASK UUID] {} [NAME] {}. Number of active tasks: {}",
            msg.task_uuid,
            msg.name,
            self.active_task_uuids.len(),
        );

        self.determine_status();
        self.generate_status_report();
        self.report_status_timer.reset::<Self>(ctx);
    }

    fn handle_close_task(
        &mut self,
        msg: CloseTask,
        ctx: &mut <Self as Actor>::Context,
    ) {
        self.active_task_uuids.remove(&msg.task_uuid);

        info!(
            self.log,
            "Closed [TASK UUID] {}. Number of active tasks: {}",
            msg.task_uuid,
            self.active_task_uuids.len(),
        );

        self.determine_status();
        self.generate_status_report();
        self.report_status_timer.reset::<Self>(ctx);
    }
}

impl Default for AppState {
    fn default() -> Self {
        let app_id = match env::get_opt_var("general.id") {
            Some(id) => id,
            None => {
                // Generate "random" ID.
                "app-".to_owned() + &Uuid::new_v4().to_string()
            },
        };

        let app_name = if let Some(name) = env::get_opt_var("general.name") {
            name
        } else {
            String::new()
        };

        let url = if let Some(url) = env::get_opt_var("general.url") {
            url
        } else {
            String::new()
        };

        Self {
            log: create_logger("app_state"),
            app_id,
            app_name,
            url,
            status: AppStatus::Idle,
            started_at: now(),
            active_task_uuids: HashSet::new(),
            report_status_timer: ReportStatusTimer::new_s(3),
            center_connector_addr: connector::start(),
        }
    }
}

impl Actor for AppState {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(self.log, "Application State started.");

        self.generate_status_report();
        self.report_status_timer.reset::<Self>(ctx);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Application State stopped.");
    }
}

impl Supervised for AppState {}

impl SystemService for AppState {
    fn service_started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Application State system service started.")
    }
}

impl Handler<ReportStatusMessage> for AppState {
    type Result = ();

    fn handle(
        &mut self,
        _msg: ReportStatusMessage,
        ctx: &mut Self::Context
    ) -> Self::Result {
        self.generate_status_report();
        self.report_status_timer.reset::<Self>(ctx);
    }
}

pub fn start() -> Addr<AppState> {
    AppState::from_registry()
}

handler_impl_task_update!(AppState);
handler_impl_close_task!(AppState);

use serde_derive::{Deserialize, Serialize};
use serde_json;
use serde_json::json;
use std::collections::{HashMap};
use std::fmt;

use crate::core::env::{self, *};
use crate::core::proxy;
use crate::core::user_agent;
use crate::worker::worker_message::{WorkerMessage, Dest, WorkerMessagePayload};

#[derive(Clone, PartialEq, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerPlugin {
    Basic,
    HeadlessBrowser,
    None,
}

impl WorkerPlugin {
    pub fn as_str(ws: WorkerPlugin) -> &'static str {
        match ws {
            WorkerPlugin::Basic => "basic",
            WorkerPlugin::HeadlessBrowser => "headless_browser",
            WorkerPlugin::None => "none",
        }
    }

    pub fn from_str(s: &str) -> WorkerPlugin {
        match s {
            "basic" => WorkerPlugin::Basic,
            "headless_browser" => WorkerPlugin::HeadlessBrowser,
            _ => WorkerPlugin::None,
        }
    }
}

impl fmt::Debug for WorkerPlugin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", WorkerPlugin::as_str(*self))
    }
}

impl Default for WorkerPlugin {
    fn default() -> Self {
        WorkerPlugin::None
    }
}

#[derive(Serialize, Deserialize)]
pub struct PluginSettings {
    pub name: String,
    pub path: String,
    pub params: HashMap<String, String>,
}

impl PluginSettings {
    pub fn new(
        name: String,
        path: String,
        params: HashMap<String, String>,
    ) -> Self {
        Self {
            name,
            path,
            params,
        }
    }

    pub fn empty() -> Self {
        PluginSettings::new("".into(), "".into(), HashMap::new())
    }
}

fn plugin_settings(plugin: WorkerPlugin) -> PluginSettings {
    match plugin {
        WorkerPlugin::Basic => {
            PluginSettings::new(
                WorkerPlugin::as_str(plugin).to_string(),
                env::full_path(
                    "$PATOKA_X_DIR/build/src/plugin/basic_plugin.js",
                    "$PATOKA_X_DIR",
                    &PATOKA_X_DIR,
                ),
                HashMap::new(),
            )
        },
        WorkerPlugin::HeadlessBrowser => {
            PluginSettings::new(
                WorkerPlugin::as_str(plugin).to_string(),
                env::full_path(
                    "$PATOKA_X_DIR/build/src/plugin/headless_browser_plugin.js",
                    "$PATOKA_X_DIR",
                    &PATOKA_X_DIR,
                ),
                params_headless_browser(),
            )
        },
        WorkerPlugin::None => {
            PluginSettings::empty()
        },
    }
}

pub fn setup_plugin_message(
    plugin: WorkerPlugin,
    worker_id: &str,
) -> WorkerMessage {
    let settings = plugin_settings(plugin);
    let data = json!({
        "plugin": serde_json::to_value(settings).unwrap(),
    });

    let dest = Dest::Worker;

    let payload = WorkerMessagePayload {
        dest,
        worker_id: worker_id.to_string(),
        task_uuid: String::new(),
        plugin: WorkerPlugin::as_str(plugin).to_string(),
        data,
    };

    WorkerMessage::new(payload)
}

fn params_headless_browser() -> HashMap<String, String> {
    let mut params = HashMap::new();

    // User-Agent header
    params.insert("user_agent".to_string(), user_agent::random_ua());

    // Proxy
    if let Some(proxy) = proxy::next() {
        let proxy_server = proxy.type_ + "://" + &proxy.address;
        params.insert("proxy_server".to_string(), proxy_server);
    }

    // DevTools
    if let Some(v) = env::get_opt_var("plugin.headless_browser.dev_tools") {
        if v == "yes" {
            params.insert("dev_tools".to_string(), "yes".to_string());
        }
    }

    params
}

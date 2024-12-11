use config::{Config, File, ConfigError, Source};
use lazy_static::lazy_static;
use serde;
use serde_json::json;
use std::{env, sync::RwLock};

lazy_static! {
    pub static ref PATOKA_ROOT_DIR: String = make_dir_path("PATOKA_ROOT_DIR");
    pub static ref PATOKA_X_DIR: String = make_dir_path("PATOKA_X_DIR");

    static ref CONFIG: RwLock<Config> = RwLock::new(Config::default());
}

pub fn full_path_curr_dir(relative_path: &str) -> String {
    let mut current_dir = env::current_dir().unwrap();
    current_dir.push(relative_path);
    current_dir.to_str().unwrap().to_string()
}

pub fn full_path(
    relative_path: &str,
    env_var_name: &str,
    patoka_dir: &str,
) -> String {
    if relative_path.starts_with(env_var_name) {
        relative_path.replace(env_var_name, patoka_dir)
    } else {
        let mut current_dir = env::current_dir().unwrap();
        current_dir.push(relative_path);
        current_dir.to_str().unwrap().to_string()
    }
}

/// Get a mandatory variable value.
pub fn get_var(key: &str) -> String {
    let config = CONFIG.read().unwrap();
    config.get_string(key).unwrap()
}

/// Get an optional variable value.
pub fn get_opt_var(key: &str) -> Option<String> {
    let config = CONFIG.read().unwrap();
    match config.get_string(key) {
        Ok(v) => Some(v),
        Err(_) => None,
    }
}

pub fn get_config() -> &'static RwLock<Config> {
    &CONFIG
}

pub fn load(config_file: &str) -> Result<(), ConfigError> {
    let mut config = CONFIG.write().unwrap();
    if let Err(e) = config.merge(File::with_name(config_file)) {
        println!(
            "Failed to load configuration from file {}: {}",
            config_file,
            e
        );
        return Err(e);
    }

    /*if let Ok(c) = config.collect() {
        println!("Configuration: {:#?}", c);
    }*/

    Ok(())
}

pub fn load_params<P: serde::de::DeserializeOwned>(group_name: &str) -> P {
    let config_file_key = group_name.to_string() + ".config";
    if let Some(v) = get_opt_var(&config_file_key) {
        if let Err(e) = load(&v) {
            panic!("{}", e);
        }
    }
    let params_key = group_name.to_string() + ".params";
    let config = get_config().read().unwrap();
    match config.get::<P>(&params_key) {
        Ok(params) => { return params },
        Err(e) => panic!("Failed to load parameters: {}", e),
    }
}

/// Optional error handling parameters.
pub fn load_error<P: serde::de::DeserializeOwned>(
    group_name: &str
) -> Option<P> {
    let params_key = group_name.to_string() + ".error";
    let config = get_config().read().unwrap();
    match config.get::<P>(&params_key) {
        Ok(params) => { Some(params) },
        Err(e) => None,
    }
}

pub fn load_opt<P: serde::de::DeserializeOwned>(
    group_name: &str
) -> Option<P> {
    let config = get_config().read().unwrap();
    match config.get::<P>(&group_name) {
        Ok(v) => { Some(v) },
        Err(e) => None,
    }
}

fn make_dir_path(env_var_name: &str) -> String {
    match env::var(env_var_name) {
        Ok(v) => {
            if v.ends_with("/") { v } else { v + "/" }
        },
        Err(e) => panic!("{}: {}", env_var_name, e),
    }
}

pub fn set_key_value(
    v: &mut serde_json::Value,
    key: String,
    value: serde_json::Value,
) {
    if let Some(o) = v.as_object_mut() {
        o.insert(key, value);
    } else {
        *v = json!({ key : value });
    }
}

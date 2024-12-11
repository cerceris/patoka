use lazy_static::lazy_static;
use serde_derive::{Deserialize};
use std::{error::Error, fs::File, sync::RwLock};

use crate::{
    core::env::{self, *},
    utils::csv,
};

lazy_static! {
    static ref PROXIES: RwLock<Proxies> = RwLock::new(load());
    static ref NO_PROXY: bool = no_proxy();
}

pub fn no_proxy() -> bool {
    if let Some(v) = env::get_opt_var("proxy.disabled") {
        if v == "true" {
            return true;
        }
    }
    false
}

pub fn next() -> Option<Proxy> {
    if *NO_PROXY {
        return None;
    }

    let mut proxies = PROXIES.write().unwrap();
    let idx = proxies.next_to_use;
    proxies.next_to_use += 1;
    if proxies.next_to_use >= proxies.proxies.len() {
        proxies.next_to_use = 0;
    }
    Some(proxies.proxies[idx].clone())
}

#[derive(Debug, Clone, Deserialize)]
pub struct Proxy {
    /// "socks5" or "http"
    pub type_: String,

    /// <host>:<port>
    pub address: String,
}

#[derive(Debug, Default)]
pub struct Proxies {
    pub proxies: Vec<Proxy>,
    pub next_to_use: usize,
}

impl Proxies {

    pub fn new(proxies: Vec<Proxy>) -> Self {
        Self {
            proxies,
            next_to_use: 0,
        }
    }
}

fn load() -> Proxies {
    if *NO_PROXY {
        return Proxies::default();
    }

    let proxies_file = match env::get_opt_var("proxy.list") {
        Some(f) => f,
        None => "$PATOKA_ROOT_DIR/cfg/proxies.csv".to_string(),
    };

    let path = env::full_path(
        &proxies_file,
        "$PATOKA_ROOT_DIR",
        &PATOKA_ROOT_DIR
    );

    match load_from_file(&path) {
        Ok(proxies) => {
            if proxies.proxies.len() < 1 {
                panic!(
                    "No proxies have been loaded from file {}",
                    path
                );
            }
            proxies
        },
        Err(e) => {
            panic!("Failed to load proxies: {}", e);
        }
    }
}

fn load_from_file(path: &str) -> Result<Proxies, Box<dyn Error>> {
    let proxies = csv::load_from_file::<Proxy>(path)?;

    Ok(Proxies::new(proxies))
}

#[cfg(test)]
mod tests {
    #[test]
    fn next() {
        let proxy = super::next().unwrap();
        assert!(proxy.type_ == "http" || proxy.type_ == "socks5");
        assert!(!proxy.address.is_empty());
    }
}

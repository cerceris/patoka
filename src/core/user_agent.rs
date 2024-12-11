use lazy_static::lazy_static;
use rand::{thread_rng, Rng};
use std::{
    fs::File,
    io::BufReader,
    sync::RwLock,
};
use xml::reader::{EventReader, XmlEvent};

use crate::core::env::{self, *};

lazy_static! {
    static ref UAS: RwLock<UserAgents> = RwLock::new(load());
}

pub fn random_ua() -> String {
    let uas = UAS.read().unwrap();
    let mut rng = thread_rng();
    let idx: usize = rng.gen_range(0..uas.uas.len());
    uas.uas[idx].to_string()
}

#[derive(Debug, Default)]
pub struct UserAgents {
    pub uas: Vec<String>
}

fn load() -> UserAgents {
    let user_agents_file = match env::get_opt_var("general.user_agents") {
        Some(f) => f,
        None => "$PATOKA_ROOT_DIR/cfg/useragents.xml".to_string(),
    };
    let path = env::full_path(
        &user_agents_file,
        "$PATOKA_ROOT_DIR",
        &PATOKA_ROOT_DIR
    );
    let file = File::open(&path).expect(
        &format!("Failed to open file with user agents {}", &path)
    );
    let file = BufReader::new(file);
    let parser = EventReader::new(file);
    let mut uas = UserAgents::default();
    for e in parser {
        match e {
            Ok(XmlEvent::StartElement { name, attributes, .. }) => {
                if name.local_name != "useragent" {
                    continue;
                }

                let mut ua = String::new();
                let mut valid = false;
                for a in attributes {
                    if a.name.local_name == "valid" && a.value == "yes" {
                        valid = true;
                    } else if a.name.local_name == "useragent" {
                        ua = a.value;
                    }
                }

                if valid && !ua.is_empty() {
                    uas.uas.push(ua);
                }
            },
            _ => {},
        }
    }

    if uas.uas.len() < 1 {
        panic!(
            "No user agents have been loaded from file {}",
            path
        );
    }

    uas
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random() {
        let ua = random_ua();
        assert!(ua.contains("Firefox") || ua.contains("Chrome"));
    }
}

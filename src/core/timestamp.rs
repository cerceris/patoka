use chrono::prelude::*;

pub type Timestamp = DateTime<Utc>;

pub fn now() -> Timestamp {
    Utc::now()
}

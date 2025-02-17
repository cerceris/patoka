use chrono::prelude::*;

pub type Timestamp = DateTime<Utc>;

pub fn now() -> Timestamp {
    Utc::now()
}

pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

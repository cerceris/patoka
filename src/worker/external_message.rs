use serde_derive::{Deserialize, Serialize};
use serde_json;

use crate::transport::message::*;
use crate::worker::worker_message::*;

pub enum ExternalSubject {

}

#[derive(Serialize, Deserialize, Clone)]
pub struct ExternalMessagePayload {
    pub dest: Dest,
    pub data: serde_json::Value,
}

pub type ExternalMessage = GenMessage<ExternalMessagePayload>;

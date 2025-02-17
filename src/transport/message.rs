use actix::prelude::*;
use serde_derive::{Deserialize, Serialize};
use serde_json;
use zmq;

use crate::core::timestamp;

pub type Identity = zmq::Message;

#[derive(Debug)]
pub struct RawMessage {
    pub identity: Identity,
    pub body: String,
}

impl RawMessage {
    pub fn new(identity: Identity, body: &str) -> Self {
        Self {
            identity,
            body: body.to_string()
        }
    }

    pub fn dummy() -> Self {
        Self {
            identity: new_identity(),
            body: String::new(),
        }
    }

    pub fn with_body(body: &str) -> Self {
        Self {
            identity: new_identity(),
            body: body.to_string(),
        }
    }

    pub fn to<P>(
        rwm: RawMessage
    ) -> Result<GenMessage<P>, serde_json::Error>
    where
        P: serde::de::DeserializeOwned
    {
        let payload: P = serde_json::from_str(&rwm.body)?;
        Ok(GenMessage::with_identity(payload, rwm.identity))
    }

    pub fn from<P>(wm: GenMessage<P>) -> Self
    where
        P: serde::Serialize
    {
        let body = serde_json::to_string(&wm.payload).unwrap();
        Self {
            identity: wm.identity,
            body
        }
    }
}

impl Clone for RawMessage {
    fn clone(&self) -> Self {
        Self {
            identity: clone_identity(&self.identity),
            body: self.body.clone(),
        }
    }
}

impl Message for RawMessage {
    type Result = ();
}

pub fn new_identity() -> Identity {
    Identity::new()
}

pub fn clone_identity(identity: &Identity) -> Identity {
    Identity::from(identity as &[u8])
}

pub fn is_empty(identity: &Identity) -> bool {
    identity.len() < 1
}

#[derive(Message, Debug, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct GenMessage<P> {
    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    #[serde(default = "new_identity")]
    pub identity: Identity,
    pub payload: P,
    #[serde(skip)]
    pub created_at: i64,
}

impl<P> GenMessage<P> {
    pub fn new(payload: P) -> Self {
        Self {
            identity: new_identity(),
            payload,
            created_at: timestamp::now().timestamp_millis(),
        }
    }

    pub fn with_identity(payload: P, identity: Identity) -> Self {
        Self {
            identity,
            payload,
            created_at: timestamp::now().timestamp_millis(),
        }
    }
}

impl<P> Clone for GenMessage<P>
where
    P: Clone
{
    fn clone(&self) -> Self {
        Self {
            identity: clone_identity(&self.identity),
            payload: self.payload.clone(),
            created_at: self.created_at,
        }
    }
}


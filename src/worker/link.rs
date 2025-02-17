use actix::prelude::*;
use lazy_static::lazy_static;
use paste::paste;
pub use std::{
    collections::HashMap,
    sync::RwLock
};

pub trait Link {

    type M: Message + Send;

    fn get_recipient(uuid: &str) -> Recipient<Self::M>
    where
        <<Self as Link>::M as Message>::Result: Send;

    fn register_recipient(uuid: &str, addr: Recipient<Self::M>)
    where
        <<Self as Link>::M as Message>::Result: Send;

    fn unregister_recipient(uuid: &str);
}

#[macro_export]
macro_rules! define_link {
    ($M:ty, $name:ident) => {
        lazy_static! {
            pub static ref $name: RwLock<HashMap<String, Recipient<$M>>> =
                RwLock::new(HashMap::new());
        }

        paste::paste! { lazy_static! {
            pub static ref [<$name _delayed>]:
                RwLock<HashMap<String, Vec<$M>>> = RwLock::new(HashMap::new());
        }}

        impl Link for $M {
            type M = $M;

            fn get_recipient(uuid: &str) -> Recipient<Self::M>
            where
                <<Self as Link>::M as Message>::Result: Send
            {
                let recipients = $name.read().unwrap();
                recipients.get(uuid).unwrap().clone()
            }

            fn register_recipient(uuid: &str, addr: Recipient<Self::M>)
            where
                <<Self as Link>::M as Message>::Result: Send
            {
                let mut recipients = $name.write().unwrap();
                recipients.insert(uuid.into(), addr.clone());
                <$M>::send_delayed(uuid, addr);
            }

            fn unregister_recipient(uuid: &str) {
                let mut recipients = $name.write().unwrap();
                recipients.remove(uuid);
            }
        }

        paste::paste! { impl $M {
            pub fn send_when_ready(uuid: &str, msg: $M) {
                let recipients = $name.read().unwrap();
                if let Some(recipient) = recipients.get(uuid) {
                    recipient.do_send(msg);
                } else {
                    let mut delayed = [<$name _delayed>].write().unwrap();
                    if let Some(msgs) = delayed.get_mut(uuid) {
                        msgs.push(msg);
                    } else {
                        delayed.insert(uuid.into(), vec![msg]);
                    }
                }
            }

            pub fn send_delayed(uuid: &str, addr: Recipient<$M>) {
                let mut delayed = [<$name _delayed>].write().unwrap();
                if let Some(msgs) = delayed.remove(uuid) {
                    for msg in msgs {
                        addr.do_send(msg);
                    }
                }
            }
        }}
    }
}

pub struct RegisterRecipientMessage<M: actix::Message + Send>
where
    <M as actix::Message>::Result: Send,
{
    pub task_uuid: String,
    pub addr: Option<Recipient<M>>
}

impl<M> Message for RegisterRecipientMessage<M>
where
    M: Message + Send,
    <M as actix::Message>::Result: Send,
{
    type Result = ();
}

pub struct SenderRecipientLink<M: actix::Message + Send >
where
    <M as actix::Message>::Result: Send,
{
    /// UUID --> Sender
    pub senders: HashMap<String, Recipient<RegisterRecipientMessage<M>>>,
}

impl<M> SenderRecipientLink<M>
where
    M: actix::Message<Result = ()> + Send,
{
    pub fn new() -> Self {
        Self {
            senders: HashMap::new(),
        }
    }

    pub fn register_sender(
        &mut self,
        uuid: String,
        addr: Recipient<RegisterRecipientMessage<M>>
    ) {
        self.senders.insert(uuid, addr);
    }

    pub fn unregister_sender(&mut self, uuid: &str) {
        self.senders.remove(uuid);
    }

    pub fn register_recipient_on_sender(
        &self,
        sender_uuid: &str,
        recipient_uuid: &str,
        recipient_addr: Recipient<M>,
    ) -> bool {
        if let Some(sender) = self.senders.get(sender_uuid) {
            sender.do_send(RegisterRecipientMessage {
                task_uuid: recipient_uuid.into(),
                addr: Some(recipient_addr),
            });
            true
        } else {
            false
        }
    }

    pub fn unregister_recipient_on_sender(
        &self,
        sender_uuid: &str,
        recipient_uuid: &str
    ) -> bool {
        if let Some(sender) = self.senders.get(sender_uuid) {
            sender.do_send(RegisterRecipientMessage {
                task_uuid: recipient_uuid.into(),
                addr: None,
            });
            true
        } else {
            false
        }
    }
}

#[macro_export]
macro_rules! define_sender_recipient_link {
    ($M:ty, $name:ident) => {
        lazy_static! {
            static ref $name: RwLock<SenderRecipientLink<$M>> =
                RwLock::new(SenderRecipientLink::new());
        }

        impl $M {
            pub fn register_sender(
                uuid: String,
                addr: Recipient<RegisterRecipientMessage<$M>>
            ) {
                let mut link = $name.write().unwrap();
                link.register_sender(uuid, addr);
            }

            pub fn unregister_sender(uuid: &str) {
                let mut link = $name.write().unwrap();
                link.unregister_sender(uuid);
            }

            pub fn register_recipient_on_sender(
                sender_uuid: &str,
                recipient_uuid: &str,
                recipient_addr: Recipient<$M>,
            ) -> bool {
                let link = $name.read().unwrap();
                link.register_recipient_on_sender(
                    sender_uuid,
                    recipient_uuid,
                    recipient_addr
                )
            }

            pub fn unregister_recipient_on_sender(
                sender_uuid: &str,
                recipient_uuid: &str
            ) -> bool {
                let link = $name.read().unwrap();
                link.unregister_recipient_on_sender(
                    sender_uuid,
                    recipient_uuid
                )
            }
        }
    }
}

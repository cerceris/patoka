use actix::prelude::*;
use std::collections::HashMap;

use crate::worker::link::RegisterRecipientMessage;

#[derive(Clone)]
pub struct RecipientGroup<M: Message + Send>
where
    <M as Message>::Result: Send,
{
    pub recipients: HashMap<String, Recipient<M>>,
    next_idx: usize,
}

impl<M: Message + Send + Clone> RecipientGroup<M>
where
    <M as Message>::Result: Send,
{
    pub fn new() -> Self {
        RecipientGroup {
            recipients: HashMap::new(),
            next_idx: 0,
        }
    }

    pub fn register_recipient(&mut self, uuid: String, addr: Recipient<M>) {
        self.recipients.insert(uuid, addr);
    }

    pub fn unregister_recipient(&mut self, uuid: &str) {
        self.recipients.remove(uuid);
    }

    pub fn handle_register_recipient_message(
        &mut self,
        msg: RegisterRecipientMessage<M>,
    ) {
        match msg.addr {
            Some(addr) => self.register_recipient(msg.task_uuid, addr),
            None => self.unregister_recipient(&msg.task_uuid),
        }
    }

    pub fn send_all(&self, msg: M) {
        for recipient in self.recipients.values() {
            recipient.do_send(msg.clone());
        }
    }

    pub fn send_rr(&mut self, msg: M) {
        if self.recipients.is_empty() {
            return;
        }

        let recipient = self.recipients.values().nth(self.next_idx).unwrap();
        recipient.do_send(msg);

        self.next_idx += 1;
        if self.next_idx >= self.recipients.len() {
            self.next_idx = 0;
        }
    }
}

/// An actor that has a recipient group has to handle certain messages.
#[macro_export]
macro_rules! handler_recipient_group {
    ($actor:ident, $M:ty, $name:ident) => {
        impl Handler<RegisterRecipientMessage<$M>> for $actor
        where
            $M: actix::Message<Result = ()> + Send,
        {
            type Result = ();

            fn handle(
                &mut self,
                msg: RegisterRecipientMessage<$M>,
                _ctx: &mut Self::Context,
            ) -> Self::Result {
                self.$name.handle_register_recipient_message(msg);
            }
        }
    };
}

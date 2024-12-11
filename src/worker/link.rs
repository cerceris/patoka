use actix::prelude::*;
use lazy_static::lazy_static;
use std::{
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
                recipients.insert(uuid.into(), addr);
            }

            fn unregister_recipient(uuid: &str) {
                let mut recipients = $name.write().unwrap();
                recipients.remove(uuid);
            }
        }
    }
}

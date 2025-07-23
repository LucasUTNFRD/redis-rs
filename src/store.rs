use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::{
    cmd::Command,
    data_structures::{list::Lists, strings::Strings},
};

struct StorageActor {
    string_store: Strings,
    list_store: Lists,
    cmd_rx: UnboundedReceiver<Command>,
}

impl StorageActor {
    pub fn new(cmd_rx: UnboundedReceiver<Command>) -> Self {
        Self {
            string_store: Strings::default(),
            list_store: Lists::default(),
            cmd_rx,
        }
    }

    async fn run(mut self) {
        while let Some(cmd) = self.cmd_rx.recv().await {
            use Command::*;
            match cmd {
                Command::LLEN { key } => {
                    let result = self.list_store.get_list_len(&key);
                    todo!()
                }
                Command::SET { key, val, px } => {
                    let result = self.string_store.set(key, val, px);
                    todo!()
                }
                Command::GET { key } => {
                    let result = self.string_store.get(&key);
                    todo!()
                }
                Command::LPUSH { key, elements } => {
                    let result = self.list_store.lpush(key, elements);
                    todo!()
                }
                Command::RPUSH { key, elements } => {
                    let result = self.list_store.rpush(key, elements);
                    todo!()
                }
                Command::LRANGE { key, start, stop } => {
                    let result = self.list_store.lrange(&key, start, stop);
                }
                Command::LPOP { key, count } => {
                    let result = self.list_store.left_pop(&key, count);
                    todo!()
                }
                Command::BLPOP { keys, timeout } => {
                    unimplemented!()
                }
                _ => panic!("Unknow command"),
            }
        }
    }
}

#[derive(Clone)]
pub struct StorageHandle {
    cmd_tx: UnboundedSender<Command>,
}

impl Default for StorageHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageHandle {
    pub fn new() -> Self {
        let (cmd_tx, cmd_rx) = unbounded_channel();
        let storage_actor = StorageActor::new(cmd_rx);
        tokio::spawn(storage_actor.run());
        Self { cmd_tx }
    }
}

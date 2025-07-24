use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot,
};

use crate::{
    cmd::Command,
    data_structures::{list::Lists, strings::Strings},
    resp::RespDataType,
};

struct StorageActor {
    string_store: Strings,
    list_store: Lists,
    cmd_rx: UnboundedReceiver<StorageCommand>,
}

impl StorageActor {
    pub fn new(cmd_rx: UnboundedReceiver<StorageCommand>) -> Self {
        Self {
            string_store: Strings::default(),
            list_store: Lists::default(),
            cmd_rx,
        }
    }

    async fn run(mut self) {
        while let Some((cmd, response_tx)) = self.cmd_rx.recv().await {
            match cmd {
                Command::SET { key, val, px } => {
                    let response = self.string_store.set(key, val, px);
                    let _ = response_tx.send(response);
                }
                Command::GET { key } => {
                    let response = self.string_store.get(&key);
                    let _ = response_tx.send(response);
                }
                Command::LLEN { key } => {
                    let response = self.list_store.get_list_len(&key);
                    let _ = response_tx.send(response);
                }
                Command::LPUSH { key, elements } => {
                    let response = self.list_store.lpush(key.clone(), elements); // Clone key for pending check
                    let _ = response_tx.send(response);
                }
                Command::RPUSH { key, elements } => {
                    let response = self.list_store.rpush(key.clone(), elements); // Clone key for pending check
                    let _ = response_tx.send(response);
                }
                Command::LRANGE { key, start, stop } => {
                    let response = self.list_store.lrange(&key, start, stop);
                    let _ = response_tx.send(response);
                }
                Command::LPOP { key, count } => {
                    let response = self.list_store.left_pop(&key, count);
                    let _ = response_tx.send(response);
                }
                Command::BLPOP {
                    keys: _,
                    timeout: _,
                } => {
                    unimplemented!()
                }
                Command::INCR { key } => {
                    let response = self.string_store.increment(key);
                    let _ = response_tx.send(response);
                }
                // Command::MULTI => {
                //     let _ = response_tx.send(RespDataType::SimpleString("OK".into()));
                // }
                _ => {
                    let _ = response_tx
                        .send(RespDataType::SimpleError("Unsupported command".to_string()));
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct StorageHandle {
    cmd_tx: UnboundedSender<StorageCommand>,
}

impl Default for StorageHandle {
    fn default() -> Self {
        Self::new()
    }
}

type StorageCommand = (Command, oneshot::Sender<RespDataType>);

impl StorageHandle {
    pub fn new() -> Self {
        let (cmd_tx, cmd_rx) = unbounded_channel();
        let storage_actor = StorageActor::new(cmd_rx);
        tokio::spawn(storage_actor.run());
        Self { cmd_tx }
    }

    pub async fn send(&self, cmd: Command) -> RespDataType {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx.send((cmd, resp_tx)).expect("Actor task failed");
        resp_rx.await.expect("Actor response failed")
    }
}

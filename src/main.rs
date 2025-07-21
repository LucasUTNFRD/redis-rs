#![allow(unused_imports)]

use core::str;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Ok, Result};
use bytes::BytesMut;
use codecrafters_redis::resp::{RespCodec, RespDataType};
use futures::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .context("Failed to bind from addr")?;

    let redis_state = KvStore::default();

    loop {
        let (mut socket, peer_addr) = listener
            .accept()
            .await
            .context("Failed to accept incoming connection")?; // Use anyhow::Context for errors

        println!("Accepted new connection from: {}", peer_addr);

        let state = redis_state.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(&mut socket, &state).await {
                eprintln!("Errror handling connection from {}: {:?}", peer_addr, e);
            }
        });
    }
}

#[derive(Debug, Default, Clone)]
struct KvStore {
    inner: Arc<RwLock<HashMap<String, Entry>>>,
    instant: Duration,
}

#[derive(Debug, Default)]
struct Entry {
    pub val: String,
    expires_at: Option<Instant>,
}

impl Entry {
    pub fn new(val: String, expiry: Option<Duration>) -> Self {
        Self {
            val,
            expires_at: expiry.map(|expiry| Instant::now() + expiry),
        }
    }

    pub fn is_expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|expiry| now > expiry)
    }
}

impl KvStore {
    pub async fn set(&self, key: String, value: String, expiry: Option<Duration>) -> RespDataType {
        let mut store = self.inner.write().await;

        store.insert(key, Entry::new(value, expiry));
        RespDataType::SimpleString("OK".into())
    }

    pub async fn get(&self, key: &str) -> RespDataType {
        let store = self.inner.read().await;
        match store.get(key) {
            Some(entry) if !entry.is_expired(Instant::now()) => {
                RespDataType::BulkString(entry.val.clone())
            }
            Some(entry) => RespDataType::NullBulkString,
            None => RespDataType::NullBulkString,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Command {
    Ping,
    Echo(String),
    Set {
        key: String,
        val: String,
        px: Option<Duration>, // in milliseconds
    },
    Get {
        key: String,
    },
}

impl TryFrom<RespDataType> for Command {
    type Error = anyhow::Error;
    fn try_from(resp: RespDataType) -> std::result::Result<Self, Self::Error> {
        match resp {
            RespDataType::Array(parts) => {
                if parts.is_empty() {
                    bail!("Empty command array");
                }

                let cmd = match &parts[0] {
                    RespDataType::BulkString(cmd) | RespDataType::SimpleString(cmd) => {
                        cmd.to_uppercase()
                    }
                    _ => bail!("Command must be a string type"),
                };

                match cmd.as_str() {
                    "PING" => {
                        if parts.len() > 1 {
                            bail!("PING command takes no arguments");
                        }
                        Ok(Command::Ping)
                    }
                    "ECHO" => {
                        if parts.len() != 2 {
                            bail!("ECHO command requires exactly 1 argument");
                        }
                        match &parts[1] {
                            RespDataType::BulkString(msg) => Ok(Command::Echo(msg.clone())),
                            _ => bail!("ECHO message must be a bulk string"),
                        }
                    }
                    "GET" => {
                        if parts.len() != 2 {
                            bail!("GET command requires exactly 1 argument");
                        }
                        match &parts[1] {
                            RespDataType::BulkString(key) => Ok(Command::Get { key: key.clone() }),
                            _ => bail!("GET key must be a bulk string"),
                        }
                    }
                    "SET" => {
                        if parts.len() < 3 || parts.len() > 5 {
                            bail!("SET command requires 2 or 4 arguments (key, value, [PX, milliseconds])");
                        }

                        let key = match &parts[1] {
                            RespDataType::BulkString(key) => key.clone(),
                            _ => bail!("SET key must be a bulk string"),
                        };

                        let val = match &parts[2] {
                            RespDataType::BulkString(val) => val.clone(),
                            _ => bail!("SET value must be a bulk string"),
                        };

                        let px = if parts.len() > 3 {
                            match (&parts[3], parts.get(4)) {
                                (
                                    RespDataType::BulkString(opt),
                                    Some(RespDataType::BulkString(ms)),
                                ) => {
                                    if opt.to_uppercase() == "PX" {
                                        let milliseconds = ms
                                            .parse::<u64>()
                                            .context("PX value must be a valid number")?;
                                        Some(Duration::from_millis(milliseconds))
                                    } else {
                                        bail!("Only PX option is supported for SET");
                                    }
                                }
                                _ => bail!("Invalid SET options format"),
                            }
                        } else {
                            None
                        };

                        Ok(Command::Set { key, val, px })
                    }
                    _ => bail!("Unknown command: {}", cmd),
                }
            }
            _ => bail!("Command must be an array of RESP types"),
        }
    }
}

async fn handle_connection(conn: &mut TcpStream, redis_state: &KvStore) -> Result<()> {
    let mut framed = Framed::new(conn, RespCodec);

    while let Some(resp_result) = framed.next().await {
        let resp_data = resp_result.context("Decoding failed")?;
        match Command::try_from(resp_data)? {
            Command::Ping => {
                let response = RespDataType::SimpleString("PONG".to_string());
                framed.send(response).await?;
            }
            Command::Echo(msg) => {
                let response = RespDataType::BulkString(msg);
                framed.send(response).await?;
            }
            Command::Set { key, val, px } => {
                let response = redis_state.set(key, val, px).await;
                framed.send(response).await?;
            }
            Command::Get { key } => {
                let response = redis_state.get(&key).await;
                framed.send(response).await?;
            }
        }
    }

    Ok(())
}

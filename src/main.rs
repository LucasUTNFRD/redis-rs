#![allow(unused_imports)]

use std::{collections::HashMap, sync::Arc};

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
    inner: Arc<RwLock<HashMap<String, String>>>,
}

impl KvStore {
    pub async fn set(&self, key: String, value: String) -> RespDataType {
        let mut store = self.inner.write().await;
        store.insert(key, value);
        RespDataType::SimpleString("OK".into())
    }

    pub async fn get(&self, key: String) -> RespDataType {
        let store = self.inner.read().await;
        match store.get(&key) {
            Some(val) => RespDataType::BulkString(val.clone()),
            None => RespDataType::NullBulkString,
        }
    }
}

async fn handle_connection(conn: &mut TcpStream, redis_state: &KvStore) -> Result<()> {
    let mut framed = Framed::new(conn, RespCodec);

    loop {
        tokio::select! {
            resp_result = framed.next() => {
                match resp_result {
                    Some(resp) => {
                        let resp_data= resp.context("Decoding failed")?;
                        match resp_data {
                            RespDataType::Array(ref parts) if !parts.is_empty() => {
                                match &parts[0] {
                                    RespDataType::BulkString(cmd) | RespDataType::SimpleString(cmd) => {
                                        match cmd.to_uppercase().as_str() {
                                            "PING" => {
                                                let response = RespDataType::SimpleString("PONG".to_string());
                                                framed.send(response).await?;
                                            }
                                            "ECHO" => {
                                                if parts.len() > 1 {
                                                    if let RespDataType::BulkString(msg) = &parts[1] {
                                                        let response = RespDataType::BulkString(msg.clone());
                                                        framed.send(response).await?;
                                                    }
                                                }
                                            }
                                            "GET" => {
                                                if parts.len() == 2 {
                                                      if let RespDataType::BulkString(key) | RespDataType::SimpleString(key) = &parts[1] {
                                                            let value = redis_state.get(key.clone()).await;
                                                            framed.send(value).await?;
                                                        } else {
                                                            let response = RespDataType::SimpleError("ERR invalid key type".to_string());
                                                            framed.send(response).await?;
                                                        }
                                            } else {
                                                        let response = RespDataType::SimpleError("ERR wrong number of arguments for 'get' command".to_string());
                                                        framed.send(response).await?;
                                                }
                                            }
                                            "SET" => {
                                                 if parts.len() == 3 {
                                                         if let (RespDataType::BulkString(key) | RespDataType::SimpleString(key),
                                                                 RespDataType::BulkString(value) | RespDataType::SimpleString(value)) = (&parts[1], &parts[2]) {
                                                             let response = redis_state.set(key.clone(), value.clone()).await;
                                                             framed.send(response).await?;
                                                        } else {
                                                            let response = RespDataType::SimpleError("ERR invalid argument types".to_string());
                                                            framed.send(response).await?;
                                                        }
                                                 } else {
                                                     let response = RespDataType::SimpleError("ERR wrong number of arguments for 'set' command".to_string());
                                                     framed.send(response).await?;
                                                 }
                                            }
                                            _ => {
                                                let response = RespDataType::SimpleError("ERR unknown command".to_string());
                                                framed.send(response).await?;
                                            }
                                        }
                                    }
                                    _ => {
                                        let response = RespDataType::SimpleError("ERR invalid command format".to_string());
                                        framed.send(response).await?;
                                    }
                                }
                            }
                            _ => {
                                let response = RespDataType::SimpleError("ERR commands must be arrays".to_string());
                                framed.send(response).await?;
                            }
                        }
                    }
                    None => {
                        println!("Client disconnected.");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

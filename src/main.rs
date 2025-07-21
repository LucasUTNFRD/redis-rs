#![allow(unused_imports)]

use codecrafters_redis::cmd::Command;
use codecrafters_redis::db::KvStore;
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
            Command::RPush { key, elements } => {
                let response = redis_state.rpush(key, elements).await;
                framed.send(response).await?;
            }
        }
    }

    Ok(())
}

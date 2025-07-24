#![allow(unused_imports)]

use codecrafters_redis::{cmd::Command, storage::StorageHandle};
use core::str;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use bytes::BytesMut;
use codecrafters_redis::resp::{RespCodec, RespDataType};
use futures::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{oneshot, RwLock},
};
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .context("Failed to bind from addr")?;

    let storage_handle = StorageHandle::new();

    loop {
        let (mut socket, peer_addr) = listener.accept().await?;

        println!("Accepted new connection from: {}", peer_addr);

        let storage = storage_handle.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(&mut socket, storage).await {
                eprintln!("Errror handling connection from {}: {:?}", peer_addr, e);
            }
        });
    }
}

async fn handle_connection(conn: &mut TcpStream, storage: StorageHandle) -> Result<()> {
    let mut framed = Framed::new(conn, RespCodec);

    let cmd_pipeline: Option<Vec<Command>> = None;
    let multi_flag = false;

    while let Some(resp_result) = framed.next().await {
        let resp_data = resp_result.context("Decoding failed")?;
        let cmd = Command::try_from(resp_data);
        match cmd {
            Ok(cmd) => {
                let response = match cmd {
                    Command::PING => RespDataType::SimpleString("PONG".to_string()),
                    Command::ECHO(msg) => RespDataType::BulkString(msg),
                    Command::MULTI => RespDataType::SimpleString("OK".into()),
                    _ => storage.send(cmd).await,
                };
                framed.send(response).await?;
            }
            Err(e) => {
                eprintln!("Command error: {}", e);
                let _ = framed.send(RespDataType::SimpleError(e.to_string())).await;
            }
        };
    }

    Ok(())
}

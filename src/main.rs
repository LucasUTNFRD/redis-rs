#![allow(unused_imports)]

use codecrafters_redis::{cmd::Command, storage::StorageHandle};
use std::collections::VecDeque;

use anyhow::{Context, Result};
use codecrafters_redis::resp::{RespCodec, RespDataType};
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
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

    let mut transaction_queue: Option<VecDeque<Command>> = None;

    while let Some(resp_result) = framed.next().await {
        let resp_data = resp_result.context("Decoding failed")?;
        let cmd = Command::try_from(resp_data);
        match cmd {
            Ok(cmd) => {
                if let Some(ref mut queued_cmds) = transaction_queue {
                    let response = match cmd {
                        Command::EXEC if queued_cmds.is_empty() => {
                            transaction_queue = None;
                            RespDataType::Array(vec![])
                        }
                        Command::DISCARD => {
                            transaction_queue = None;
                            RespDataType::SimpleString("OK".into())
                        }
                        Command::EXEC => execute_transaction(queued_cmds, &storage).await,
                        _ => {
                            queued_cmds.push_back(cmd);
                            RespDataType::SimpleString("QUEUED".into())
                        }
                    };
                    framed.send(response).await?;
                } else {
                    let response = match cmd {
                        Command::PING => RespDataType::SimpleString("PONG".to_string()),
                        Command::ECHO(msg) => RespDataType::BulkString(msg),
                        Command::MULTI => {
                            transaction_queue = Some(VecDeque::new());
                            RespDataType::SimpleString("OK".into())
                        }
                        Command::EXEC => RespDataType::SimpleError("ERR EXEC without MULTI".into()),
                        Command::DISCARD => {
                            RespDataType::SimpleError("ERR DISCARD without MULTI".into())
                        }
                        _ => storage.send(cmd).await,
                    };
                    framed.send(response).await?;
                }
            }
            Err(e) => {
                eprintln!("Command error: {}", e);
                let _ = framed.send(RespDataType::SimpleError(e.to_string())).await;
            }
        };
    }

    Ok(())
}

async fn execute_transaction(
    queued_cmds: &mut VecDeque<Command>,
    storage: &StorageHandle,
) -> RespDataType {
    let mut results = Vec::with_capacity(queued_cmds.len());

    while let Some(cmd) = queued_cmds.pop_front() {
        let result = match cmd {
            Command::PING => RespDataType::SimpleString("PONG".to_string()),
            Command::ECHO(msg) => RespDataType::BulkString(msg),
            Command::EXEC | Command::MULTI => panic!("MULTIPLE or EXEC could not be queued"),
            // Note: MULTI/EXEC/DISCARD inside transactions should be handled specially
            // For now, treat them as storage commands
            _ => storage.send(cmd).await,
        };

        results.push(result);
    }

    RespDataType::Array(results)
}

#![allow(unused_imports)]

use anyhow::{bail, Context, Ok, Result};
use bytes::BytesMut;
use codecrafters_redis::resp::{RespDataType, RespDecoder};
use futures::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .context("Failed to bind from addr")?;

    loop {
        let (mut socket, peer_addr) = listener
            .accept()
            .await
            .context("Failed to accept incoming connection")?; // Use anyhow::Context for errors

        println!("Accepted new connection from: {}", peer_addr);

        tokio::spawn(async move {
            if let Err(e) = handle_connection(&mut socket).await {
                eprintln!("Errror handling connection from {}: {:?}", peer_addr, e);
            }
        });
    }
}

async fn handle_connection(conn: &mut TcpStream) -> Result<()> {
    let mut framed = Framed::new(conn, RespDecoder);

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

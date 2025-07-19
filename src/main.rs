#![allow(unused_imports)]
// use std::{io::Write, net::TcpListener};

use anyhow::{Context, Ok, Result};
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const PONG_RESPONSE: &[u8] = b"+PONG\r\n";

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

        // tokio::spawn(async move {
        if let Err(e) = handle_connection(&mut socket).await {
            eprintln!("Errror handling connection from {}: {:?}", peer_addr, e);
        }
        // });
    }
}

async fn handle_connection(conn: &mut TcpStream) -> Result<()> {
    let mut buf = BytesMut::with_capacity(512);
    loop {
        let bytes_read = conn
            .read_buf(&mut buf)
            .await
            .context("failed to read from stream")?;

        if bytes_read == 0 {
            println!("Client disconnected.");
            break;
        }

        conn.write_all(PONG_RESPONSE)
            .await
            .context("Failed to write PONG response to TCP stream")?;

        if buf.is_empty() {
            buf.clear(); // Clear the buffer if all data has been processed
        }
    }

    Ok(())
}

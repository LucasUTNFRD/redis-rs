#![allow(unused_imports)]

use anyhow::Result;
use codecrafters_redis::server::RedisServer;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Logs from your program will appear here!");

    let server = RedisServer::new("127.0.0.1:6379").await?;
    server.run().await
}

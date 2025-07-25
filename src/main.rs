#![allow(unused_imports)]

use anyhow::Result;
use codecrafters_redis::{config::ServerConfig, server::RedisServer};

#[tokio::main]
async fn main() -> Result<()> {
    let config = ServerConfig::from_cli();
    let server = RedisServer::new(config).await?;
    server.run().await
}

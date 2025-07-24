#![allow(unused_imports)]

use anyhow::Result;
use clap::{Arg, Command};
use codecrafters_redis::server::RedisServer;

#[tokio::main]
async fn main() -> Result<()> {
    let matches = Command::new("codecrafters-redis")
        .arg(
            Arg::new("port")
                .long("port")
                .value_name("PORT")
                .help("Port to bind the Redis server to")
                .default_value("6379"),
        )
        .get_matches();

    let port = matches
        .get_one::<String>("port")
        .expect("default always present");

    let addr = format!("127.0.0.1:{}", port);

    let server = RedisServer::new(&addr).await?;
    server.run().await
}

#![allow(unused_imports)]

use std::net::{SocketAddr, SocketAddrV4, TcpStream};

use anyhow::{bail, Result};
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
        .arg(
            Arg::new("replicaof")
                .long("replicaof")
                .value_name("MASTER_HOST MASTER_PORT")
                .help("Make this server a replica of the specified master")
                .num_args(1),
        )
        .get_matches();

    let port = matches
        .get_one::<String>("port")
        .expect("default always present");

    let addr = format!("127.0.0.1:{}", port);

    // Parse replicaof argument if provided
    let replica_of = matches.get_one::<String>("replicaof").map(|s| {
        let parts: Vec<&str> = s.split_whitespace().collect();
        if parts.len() != 2 {
            panic!("Invalid replicaof format. Expected: '<host> <port>'");
        }
        let host = parts[0].to_string();
        let port = parts[1];

        if port.parse::<u16>().is_err() {
            eprintln!("Invalid port in --replicaof");
        }

        (host, port)
    });

    let server = RedisServer::new(&addr).await?;
    server.run().await
}

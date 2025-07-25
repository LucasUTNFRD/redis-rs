use clap::{Arg, Command};

pub struct ServerConfig {
    pub bind_addr: String,
    pub port: u16,
    pub replica_of: Option<String>,
    // pub replication_id: String,
    // pub replication_offset: u64,
}

impl ServerConfig {
    pub fn from_cli() -> Self {
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
                panic!("Invalid port in --replicaof");
            }

            (host, port)
        });

        Self {
            bind_addr: addr,
            port: port.parse().expect("default port should be valid"),
            replica_of: replica_of.map(|(host, port)| format!("{}:{}", host, port)),
        }
    }
}

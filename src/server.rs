use crate::config::ServerConfig;
use crate::resp::{RespCodec, RespDataType};
use crate::{cmd::Command, storage::StorageHandle};
use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, RwLock};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

/// Represents a Redis server that handles client connections
pub struct RedisServer {
    listener: TcpListener,
    storage: StorageHandle,
    server_info: Arc<RwLock<ServerInfo>>,
}

impl RedisServer {
    /// Creates a new Redis server bound to the specified address
    pub async fn new(config: ServerConfig) -> Result<Self> {
        let listener = TcpListener::bind(&config.bind_addr)
            .await
            .context("Failed to bind to address")?;

        let storage = StorageHandle::new();
        let server_info = Arc::new(RwLock::new(ServerInfo::from(config)));

        Ok(Self {
            listener,
            storage,
            server_info,
        })
    }

    async fn send_handshake(&self, addr: &str) -> Result<()> {
        let stream = TcpStream::connect(addr).await?;
        let mut framed = Framed::new(stream, RespCodec);

        // #1 send ping command
        let ping = RespDataType::Array(vec![RespDataType::BulkString("PING".into())]);
        framed.send(ping).await?;

        // TODO : remove .unwrap()
        // assert is pong
        let ping_response = framed.next().await.unwrap().unwrap();
        println!("recv:{ping_response:?}");
        Ok(())
    }

    /// Starts the server and begins accepting connections
    pub async fn run(self) -> Result<()> {
        {
            println!(
                "Redis server started on {} with role {:#?}",
                self.listener.local_addr()?,
                self.server_info.read().unwrap().role
            );
        }

        let info = self.server_info.read().unwrap();
        if let ServerRole::Slave { addr } = &info.role {
            self.send_handshake(addr).await?
        }

        loop {
            let (socket, peer_addr) = self.listener.accept().await?;
            println!("Accepted new connection from: {}", peer_addr);

            let storage = self.storage.clone();
            // server_info could not be shared and be asked via cmd
            let server_info = self.server_info.clone();

            tokio::spawn(async move {
                let mut connection = Connection::new(socket, storage, server_info);
                if let Err(e) = connection.handle().await {
                    eprintln!("Error handling connection from {}: {:?}", peer_addr, e);
                }
            });
        }
    }
}

pub struct ServerInfo {
    pub role: ServerRole,
    // The number of connected replicas
    pub connected_slaves: usize,
    //The replication ID of the master (we'll get to this in later stages)
    pub master_replid: String,
    // The replication offset of the master (we'll get to this in later stages)
    pub master_repl_offset: usize,
}
impl ServerInfo {
    pub fn is_slave(&self) -> bool {
        matches!(self.role, ServerRole::Slave { addr: _ })
    }
}

impl fmt::Display for ServerInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.role {
            ServerRole::Master => write!(f, "role:master")?,
            ServerRole::Slave { .. } => write!(f, "role:slave")?,
        }
        // Add other replication info fields
        writeln!(f, "connected_slaves:{}", self.connected_slaves)?;
        writeln!(f, "master_replid:{}", self.master_replid)?;
        writeln!(f, "master_repl_offset:{}", self.master_repl_offset)?;
        Ok(())
    }
}

const DEFAULT_MASTER_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

impl From<ServerConfig> for ServerInfo {
    fn from(cfg: ServerConfig) -> Self {
        Self {
            role: cfg
                .replica_of
                .map_or(ServerRole::Master, |addr| ServerRole::Slave { addr }),
            connected_slaves: 0,
            master_replid: DEFAULT_MASTER_ID.to_string(),
            master_repl_offset: 0,
        }
    }
}

#[derive(Debug)]
enum ServerRole {
    Master,
    Slave { addr: String },
}

/// Represents an individual client connection
pub struct Connection {
    framed: Framed<TcpStream, RespCodec>,
    storage: StorageHandle,
    transaction_queue: Option<VecDeque<Command>>,
    server_info: Arc<RwLock<ServerInfo>>,
}

impl Connection {
    /// Creates a new connection with the given socket and storage handle
    pub fn new(
        socket: TcpStream,
        storage: StorageHandle,
        server_info: Arc<RwLock<ServerInfo>>,
    ) -> Self {
        let framed = Framed::new(socket, RespCodec);

        Self {
            framed,
            storage,
            transaction_queue: None,
            server_info,
        }
    }

    /// Handles the connection lifecycle, processing commands until the connection closes
    pub async fn handle(&mut self) -> Result<()> {
        while let Some(resp_result) = self.framed.next().await {
            let resp_data = resp_result.context("Decoding failed")?;
            let cmd = Command::try_from(resp_data);

            match cmd {
                Ok(cmd) => {
                    println!("Recv {cmd:?}");
                    let response = self.process_command(cmd).await;
                    self.framed.send(response).await?;
                }
                Err(e) => {
                    eprintln!("Command error: {}", e);
                    let _ = self
                        .framed
                        .send(RespDataType::SimpleError(e.to_string()))
                        .await;
                }
            }
        }

        Ok(())
    }

    /// Processes a single command and returns the appropriate response
    async fn process_command(&mut self, cmd: Command) -> RespDataType {
        if self.transaction_queue.is_some() {
            self.handle_transaction_command(cmd).await
        } else {
            self.handle_regular_command(cmd).await
        }
    }

    /// Handles commands when in transaction mode
    async fn handle_transaction_command(&mut self, cmd: Command) -> RespDataType {
        match cmd {
            Command::EXEC => {
                if let Some(mut queued_cmds) = self.transaction_queue.take() {
                    if queued_cmds.is_empty() {
                        RespDataType::Array(vec![])
                    } else {
                        self.execute_transaction(&mut queued_cmds).await
                    }
                } else {
                    RespDataType::SimpleError("ERR EXEC without MULTI".into())
                }
            }
            Command::DISCARD => {
                self.transaction_queue = None;
                RespDataType::SimpleString("OK".into())
            }
            _ => {
                if let Some(ref mut queued_cmds) = self.transaction_queue {
                    queued_cmds.push_back(cmd);
                }
                RespDataType::SimpleString("QUEUED".into())
            }
        }
    }

    /// Handles commands when not in transaction mode
    async fn handle_regular_command(&mut self, cmd: Command) -> RespDataType {
        match cmd {
            Command::PING => RespDataType::SimpleString("PONG".to_string()),
            Command::ECHO(msg) => RespDataType::BulkString(msg),
            Command::MULTI => {
                self.transaction_queue = Some(VecDeque::new());
                RespDataType::SimpleString("OK".into())
            }
            Command::EXEC => RespDataType::SimpleError("ERR EXEC without MULTI".into()),
            Command::DISCARD => RespDataType::SimpleError("ERR DISCARD without MULTI".into()),
            Command::INFO { section: _ } => self.retrieve_info(),
            _ => self.storage.send(cmd).await,
        }
    }

    /// retrieves a BulkString like
    /// $ redis-cli INFO replication
    /// # Replication
    /// role:master
    /// connected_slaves:0
    /// master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb
    /// master_repl_offset:0
    /// second_repl_offset:-1
    /// repl_backlog_active:0
    /// repl_backlog_size:1048576
    /// repl_backlog_first_byte_offset:0
    /// repl_backlog_histlen:
    fn retrieve_info(&self) -> RespDataType {
        let server_info = self.server_info.read().unwrap();
        RespDataType::BulkString(server_info.to_string())
    }

    /// Executes a transaction by processing all queued commands
    async fn execute_transaction(&self, queued_cmds: &mut VecDeque<Command>) -> RespDataType {
        let mut results = Vec::with_capacity(queued_cmds.len());

        while let Some(cmd) = queued_cmds.pop_front() {
            let result = match cmd {
                Command::PING => RespDataType::SimpleString("PONG".to_string()),
                Command::ECHO(msg) => RespDataType::BulkString(msg),
                Command::EXEC | Command::MULTI => {
                    panic!("MULTI or EXEC should not be queued in a transaction")
                }
                _ => self.storage.send(cmd).await,
            };

            results.push(result);
        }

        RespDataType::Array(results)
    }
}

use crate::config::ServerConfig;
use crate::resp::{RespCodec, RespDataType};
use crate::{cmd::Command, storage::StorageHandle};
use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use log::{debug, info, warn};
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

    /// Performs the complete replication handshake with a Redis master
    /// 
    /// This implements the Redis replication protocol handshake sequence:
    /// 1. PING - Test connectivity
    /// 2. REPLCONF listening-port <port> - Inform master of our listening port
    /// 3. REPLCONF capa psync2 - Announce PSYNC2 capability  
    /// 4. PSYNC ? -1 - Request full synchronization
    async fn perform_replication_handshake(&self, addr: &str) -> Result<()> {
        let stream = TcpStream::connect(addr).await
            .context("Failed to connect to master")?;
        let mut framed = Framed::new(stream, RespCodec);

        info!("Starting replication handshake with master at {}", addr);

        // Step 1: Send PING to test connectivity
        let ping = RespDataType::Array(vec![RespDataType::BulkString("PING".into())]);
        framed.send(ping).await
            .context("Failed to send PING to master")?;

        let response = framed.next().await
            .context("No response from master for PING")?
            .context("Failed to decode PING response")?;
        debug!("Received PING response: {:?}", response);

        // Step 2: Send REPLCONF commands
        self.send_replconf(&mut framed, "listening-port", "6380").await
            .context("Failed to send listening-port REPLCONF")?;
            
        self.send_replconf(&mut framed, "capa", "psync2").await
            .context("Failed to send capa REPLCONF")?;

        // Step 3: Send PSYNC for full synchronization
        self.send_psync(&mut framed).await
            .context("Failed to send PSYNC")?;

        info!("Replication handshake completed successfully");
        Ok(())
    }

    /// Sends a REPLCONF command with the specified key-value pair
    /// 
    /// REPLCONF is used during replication handshake to exchange configuration
    /// information between master and replica.
    async fn send_replconf(
        &self, 
        framed: &mut Framed<TcpStream, RespCodec>, 
        key: &str, 
        value: &str
    ) -> Result<()> {
        let replconf = RespDataType::Array(vec![
            RespDataType::BulkString("REPLCONF".to_string()),
            RespDataType::BulkString(key.to_string()),
            RespDataType::BulkString(value.to_string()),
        ]);

        framed.send(replconf).await
            .context("Failed to send REPLCONF command")?;

        let response = framed.next().await
            .context("No response from master for REPLCONF")?
            .context("Failed to decode REPLCONF response")?;
            
        debug!("Received REPLCONF {} response: {:?}", key, response);
        Ok(())
    }

    /// Sends a PSYNC command to request synchronization with the master
    /// 
    /// PSYNC ? -1 requests a full synchronization since we don't have any
    /// previous replication state (? for unknown replication ID, -1 for unknown offset).
    async fn send_psync(&self, framed: &mut Framed<TcpStream, RespCodec>) -> Result<()> {
        let psync = RespDataType::Array(vec![
            RespDataType::BulkString("PSYNC".to_string()),
            RespDataType::BulkString("?".to_string()),
            RespDataType::BulkString("-1".to_string()),
        ]);

        framed.send(psync).await
            .context("Failed to send PSYNC command")?;

        let response = framed.next().await
            .context("No response from master for PSYNC")?
            .context("Failed to decode PSYNC response")?;
            
        info!("Connected to master, PSYNC response: {:?}", response);
        Ok(())
    }

    async fn send_handshake(&self, addr: &str) -> Result<()> {
        self.perform_replication_handshake(addr).await
    }

    /// Starts the server and begins accepting connections
    pub async fn run(self) -> Result<()> {
        {
            info!(
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
            info!("Accepted new connection from: {}", peer_addr);

            let storage = self.storage.clone();
            // server_info could not be shared and be asked via cmd
            let server_info = self.server_info.clone();

            tokio::spawn(async move {
                let mut connection = Connection::new(socket, storage, server_info);
                if let Err(e) = connection.handle().await {
                    warn!("Error handling connection from {}: {:?}", peer_addr, e);
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
pub enum ServerRole {
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
                    debug!("Recv {cmd:?}");
                    let response = self.process_command(cmd).await;
                    self.framed.send(response).await?;
                }
                Err(e) => {
                    warn!("Command error: {}", e);
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
            Command::REPLCONF => RespDataType::SimpleString("OK".into()),
            Command::PSYNC {
                replication_id,
                offset: _,
            } => {
                let current_offset = 0;
                let my_id = DEFAULT_MASTER_ID;
                RespDataType::SimpleString(format!("FULLRESYNC {} {}", my_id, current_offset))
            }
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

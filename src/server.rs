use std::{
    collections::HashMap,
    fmt,
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{bail, Context, Result};
use futures::{SinkExt, StreamExt};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_util::codec::Framed;

use crate::{
    cmd::Command,
    config::ServerConfig,
    resp::{RespCodec, RespDataType},
    storage::StorageHandle,
};

pub struct RedisServer {
    listener: TcpListener,
    storage: StorageHandle,
    info: ServerInfo,
    role_state: RoleState,

    cmd_tx: mpsc::Sender<ServerCommand>,
    cmd_rx: mpsc::Receiver<ServerCommand>,
}

// Role-specific"idk"e
enum RoleState {
    Master(MasterState),
    Replica(ReplicaState),
}

// Master-specific fields
struct MasterState {
    pub master_replid: String,
    pub master_repl_offset: u64,
    pub replica_connections: HashMap<u64, ReplicaConnection>,
}

// Supporting types
struct ReplicaConnection {
    sender: mpsc::UnboundedSender<ReplicaCommand>,
    // last_ack_offset: u64,
}

impl ReplicaConnection {}

impl Default for MasterState {
    fn default() -> Self {
        Self {
            master_replid: DEFAULT_MASTER_ID.to_string(),
            master_repl_offset: 0,
            replica_connections: HashMap::new(),
        }
    }
}

// Replica-specific fields
#[derive(Default)]
struct ReplicaState {
    master_connection: Option<Framed<TcpStream, RespCodec>>,
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
                .map_or(ServerRole::Master, |master_addr| ServerRole::Slave {
                    master_addr,
                }),
            connected_slaves: 0,
            master_replid: DEFAULT_MASTER_ID.to_string(),
            master_repl_offset: 0,
        }
    }
}

impl ServerInfo {
    pub fn get_role(&self) -> ServerRole {
        self.role.clone()
    }
}

#[derive(Debug, Clone)]
pub enum ServerRole {
    Master,
    Slave { master_addr: String },
}

#[derive(Debug)]
pub enum ServerCommand {
    Storage {
        cmd: Command,
        resp: oneshot::Sender<RespDataType>,
    },
    Replication {
        cmd: Command,
        replica_id: u64,
        resp: oneshot::Sender<RespDataType>,
    },
    RegisterReplica {
        id: u64,
        sender: mpsc::UnboundedSender<ReplicaCommand>,
    },
    Misc {
        cmd: Command,
        resp: oneshot::Sender<RespDataType>,
    },
    Transaction {
        queued_cmds: Vec<Command>,
        resp: oneshot::Sender<RespDataType>,
    },
}

impl ServerCommand {
    pub fn is_write(&self) -> bool {
        if let Self::Storage { cmd, resp: _ } = self {
            let cond = match cmd {
                // Write operations that modify data
                Command::SET { .. } => true,
                Command::RPUSH { .. } => true,
                Command::LPUSH { .. } => true,
                Command::LPOP { .. } => true,
                Command::BLPOP { .. } => true,
                Command::INCR { .. } => true,
                _ => false,
            };
            return cond;
        };
        false
    }
}

impl RedisServer {
    /// Creates a new Redis server bound to the specified address
    pub async fn new(config: ServerConfig) -> Result<Self> {
        let listener = TcpListener::bind(&config.bind_addr)
            .await
            .context("Failed to bind to address")?;

        let storage = StorageHandle::new();

        let info = ServerInfo::from(config);

        let role_state = match info.get_role() {
            ServerRole::Slave { master_addr: _ } => RoleState::Replica(ReplicaState::default()),
            ServerRole::Master => RoleState::Master(MasterState::default()),
        };

        let (cmd_tx, cmd_rx) = mpsc::channel(1024);

        Ok(Self {
            listener,
            storage,
            info,
            role_state,
            cmd_tx,
            cmd_rx,
        })
    }

    fn is_master(&self) -> bool {
        matches!(self.role_state, RoleState::Master(_))
    }

    /// Performs the complete replication handshake with a Redis master
    ///
    /// This implements the Redis replication protocol handshake sequence:
    /// 1. PING - Test connectivity
    /// 2. REPLCONF listening-port <port> - Inform master of our listening port
    /// 3. REPLCONF capa psync2 - Announce PSYNC2 capability
    /// 4. PSYNC ? -1 - Request full synchronization
    async fn handshake_master(&self, addr: &str) -> Result<Framed<TcpStream, RespCodec>> {
        let stream = TcpStream::connect(addr)
            .await
            .context("Failed to connect to master")?;
        let mut framed = Framed::new(stream, RespCodec);

        // Step 1: Send PING to test connectivity
        let ping = RespDataType::Array(vec![RespDataType::BulkString("PING".into())]);

        framed
            .send(ping)
            .await
            .context("Failed to send PING to master")?;

        let _response = framed
            .next()
            .await
            .context("No response from master for PING")?
            .context("Failed to decode PING response")?;

        // Step 2: Send REPLCONF commands
        self.send_replconf(&mut framed, "listening-port", "6380")
            .await
            .context("Failed to send listening-port REPLCONF")?;

        self.send_replconf(&mut framed, "capa", "psync2")
            .await
            .context("Failed to send capa REPLCONF")?;

        // Step 3: Send PSYNC for full synchronization
        self.send_psync(&mut framed)
            .await
            .context("Failed to send PSYNC")?;

        Ok(framed)
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

        framed
            .send(psync)
            .await
            .context("Failed to send PSYNC command")?;

        let _response = framed
            .next()
            .await
            .context("No response from master for PSYNC")?
            .context("Failed to decode PSYNC response")?;

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
        value: &str,
    ) -> Result<()> {
        let replconf = RespDataType::Array(vec![
            RespDataType::BulkString("REPLCONF".to_string()),
            RespDataType::BulkString(key.to_string()),
            RespDataType::BulkString(value.to_string()),
        ]);

        framed
            .send(replconf)
            .await
            .context("Failed to send REPLCONF command")?;

        let _response = framed
            .next()
            .await
            .context("No response from master forREPLCONF")?
            .context("Failed to decode REPLCONF response")?;

        Ok(())
    }

    fn register_master(&mut self, master_connection: Framed<TcpStream, RespCodec>) {
        if let RoleState::Replica(ref mut state) = self.role_state {
            state.master_connection = Some(master_connection);
        }
    }

    pub async fn run_event_loop(mut self) -> Result<()> {
        if let ServerRole::Slave { master_addr } = &self.info.role {
            let connection = self.handshake_master(master_addr).await?;
            self.register_master(connection);
            // logic for initializing a heartbeat sender
        }

        loop {
            tokio::select! {
                Ok((socket,remote_addr)) = self.listener.accept() => {
                    let cmd_tx= self.cmd_tx.clone();
                    tokio::spawn(async move {
                        let mut connection = Connection::new(socket,remote_addr,cmd_tx);
                        if let Err(e) = connection.handle().await {
                            eprintln!("Error handling connection from {}: {:?}", remote_addr, e);
                        }
                    });
                }
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => {
                            println!("should handle cmd {cmd:?}");
                            if let Err(e) = self.handle_command(cmd).await{
                                eprintln!("Command error: {:?}",e);
                            }
                        }
                        None => {
                            return Ok(())
                        }
                    }
                }


            }
        }
    }

    /// Upon receiving PSYNC ? -1, the Redis master initiates a full resynchronization.
    /// The master records no state for the replica beyond its connection.
    /// The master generates and sends a full RDB file snapshot of its dataset to the replica
    fn initiate_full_resync(&mut self, replica_id: u64, resp: oneshot::Sender<RespDataType>) {
        if let RoleState::Master(ref state) = self.role_state {
            let cmd_result = RespDataType::SimpleString(format!(
                "FULLRESYNC {} {}",
                state.master_replid, state.master_repl_offset
            ));

            let _ = resp.send(cmd_result);

            state
                .replica_connections
                .get(&replica_id)
                .map(|replica| replica.sender.send(ReplicaCommand::SendRdb));
        }
    }

    async fn handle_command(&mut self, cmd: ServerCommand) -> Result<()> {
        match cmd {
            ServerCommand::Transaction { queued_cmds, resp } => {
                // Execute all commands atomically
                let mut results = Vec::with_capacity(queued_cmds.len());

                // Process each command in the transaction
                for cmd in queued_cmds {
                    match cmd {
                        // Storage commands
                        Command::GET { .. }
                        | Command::SET { .. }
                        | Command::RPUSH { .. }
                        | Command::LRANGE { .. }
                        | Command::LPUSH { .. }
                        | Command::LLEN { .. }
                        | Command::LPOP { .. }
                        | Command::INCR { .. }
                        | Command::BLPOP { .. } => {
                            let result = self.storage.send(cmd).await;
                            results.push(result);
                        }

                        // Simple commands
                        Command::PING => {
                            results.push(RespDataType::SimpleString("PONG".to_string()));
                        }
                        Command::ECHO(msg) => {
                            results.push(RespDataType::BulkString(msg));
                        }
                        Command::INFO { section: _ } => {
                            results.push(RespDataType::BulkString(self.info.to_string()));
                        }

                        // Transaction commands shouldn't be in the queue
                        Command::MULTI | Command::EXEC | Command::DISCARD => {
                            results.push(RespDataType::SimpleError(
                                "ERR transaction control commands cannot be nested".into(),
                            ));
                        }

                        // Commands not supported in transactions
                        Command::REPLCONF { .. } | Command::PSYNC { .. } => {
                            results.push(RespDataType::SimpleError(
                                "ERR replication commands not allowed in transaction".into(),
                            ));
                        }
                    }
                }

                // Send all results as a single array
                let _ = resp.send(RespDataType::Array(results));
            }
            // Other command handling remains unchanged
            ServerCommand::Storage { cmd, resp } => {
                if let RoleState::Master(ref state) = self.role_state {
                    state.replica_connections.values().for_each(|replica| {
                        let _ = replica
                            .sender
                            .send(ReplicaCommand::CommandProgation(cmd.clone()));
                    });
                }

                let cmd_result = self.storage.send(cmd).await;
                let _ = resp.send(cmd_result);
            }
            ServerCommand::Replication {
                cmd,
                replica_id,
                resp,
            } => match cmd {
                Command::PSYNC {
                    replication_id,
                    offset,
                } => {
                    let full_resync_needed = replication_id == "?" && offset == -1;
                    if self.is_master() && full_resync_needed {
                        self.initiate_full_resync(replica_id, resp);
                    }
                }
                Command::REPLCONF => {
                    let cmd_result = RespDataType::SimpleString("OK".into());
                    let _ = resp.send(cmd_result);
                }
                // Command::PING => {
                //     let cmd_result = RespDataType::SimpleString("PONG".to_string());
                //     let _ = resp.send(cmd_result);
                // }
                cmd => bail!("Command:{cmd:?} was not replication related"),
            },
            ServerCommand::RegisterReplica { id, sender } => {
                if let RoleState::Master(ref mut state) = self.role_state {
                    state.replica_connections.insert(
                        id,
                        ReplicaConnection {
                            sender: sender.clone(),
                        },
                    );
                } else {
                    panic!("A replica tried to modify Master state");
                };
            }
            ServerCommand::Misc { cmd, resp } => {
                let cmd_result = match cmd {
                    // Simple commands
                    Command::PING => RespDataType::SimpleString("PONG".to_string()),
                    Command::ECHO(msg) => RespDataType::BulkString(msg),
                    Command::INFO { section: _ } => RespDataType::BulkString(self.info.to_string()),
                    _ => panic!("Non misc cmd"),
                };
                let _ = resp.send(cmd_result);
            }
        }
        Ok(())
    }
}

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

/// Connections handle I/O only
pub struct Connection {
    framed: Framed<TcpStream, RespCodec>,
    remote_addr: SocketAddr,
    server_cmd_tx: mpsc::Sender<ServerCommand>,

    //Replica connection state
    replica_rx: Option<mpsc::UnboundedReceiver<ReplicaCommand>>,
    replica_id: Option<u64>,

    execution_mode: ExecutionMode,
}

pub enum ExecutionMode {
    Transaction { cmd_queue: Vec<Command> },
    Normal,
}

pub enum ReplicaCommand {
    SendRdb,
    CommandProgation(Command),
}

impl Connection {
    pub fn new(
        socket: TcpStream,
        remote_addr: SocketAddr,
        server_cmd_tx: mpsc::Sender<ServerCommand>,
    ) -> Self {
        let framed = Framed::new(socket, RespCodec);
        Self {
            framed,
            remote_addr,
            server_cmd_tx,
            replica_rx: None,
            replica_id: None,
            execution_mode: ExecutionMode::Normal,
        }
    }

    pub async fn handle(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                // Handle incoming frames from the client/replica
                resp_result = self.framed.next() => {
                    match resp_result {
                        Some(Ok(resp_data)) => {
                            let cmd = Command::try_from(resp_data);

                            match cmd {
                                Ok(cmd) => {
                                if let Err(e) = self.handle_cmd(cmd).await {
                                    eprintln!("Failed to send command to server: {}", e);
                                    break;
                                }
                            }
                                Err(e) => {
                                    // Send error response back to client
                                    if let Err(send_err) = self
                                        .framed
                                        .send(RespDataType::SimpleError(e.to_string()))
                                        .await
                                    {
                                        eprintln!("Failed to send error response: {}", send_err);
                                        break;
                                    }
                                }
                            }
                        }
                        Some(Err(e)) => {
                            eprintln!("Error decoding frame from {}: {:?}", self.remote_addr, e);
                            break;
                        }
                        None => {
                            // Connection closed by client
                            println!("Connection from {} closed", self.remote_addr);
                            break;
                        }
                    }
                }

                // Handle replica commands (only if this is a replica connection)
                Some(replica_cmd) = async {
                    match &mut self.replica_rx {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await, // Never resolves if no replica_rx
                    }
                } => {
                    // Handle replica-specific commands sent from the server
                    match replica_cmd {
                        ReplicaCommand::SendRdb => {
                            self.send_rdb_file().await?;
                        }
                        ReplicaCommand::CommandProgation(cmd) => {
                            // Broadcasted cmd from MASTER to REPLICA
                            println!("Broadcasted to {}",self.remote_addr);
                            let resp_enconded = RespDataType::from(cmd);
                            self.framed.send(resp_enconded).await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn register_replica(&mut self) -> Result<()> {
        let (replica_tx, replica_rx) = mpsc::unbounded_channel();
        self.replica_rx = Some(replica_rx);
        let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
        self.replica_id = Some(connection_id);
        self.server_cmd_tx
            .send(ServerCommand::RegisterReplica {
                id: connection_id,
                sender: replica_tx,
            })
            .await?;
        Ok(())
    }

    async fn handle_cmd(&mut self, cmd: Command) -> Result<()> {
        println!("received {cmd:?}");
        // Handle transaction control commands first
        match &cmd {
            Command::MULTI => {
                // Check if we're already in a transaction
                match self.execution_mode {
                    ExecutionMode::Transaction { .. } => {
                        self.framed
                            .send(RespDataType::SimpleError(
                                "ERR MULTI calls can not be nested".into(),
                            ))
                            .await?;
                        return Ok(());
                    }
                    ExecutionMode::Normal => {
                        // Start a new transaction
                        self.execution_mode = ExecutionMode::Transaction {
                            cmd_queue: Vec::new(),
                        };
                        self.framed
                            .send(RespDataType::SimpleString("OK".into()))
                            .await?;
                        return Ok(());
                    }
                }
            }
            Command::EXEC => {
                match std::mem::replace(&mut self.execution_mode, ExecutionMode::Normal) {
                    ExecutionMode::Transaction { cmd_queue } => {
                        if cmd_queue.is_empty() {
                            self.framed.send(RespDataType::Array(vec![])).await?;
                            return Ok(());
                        }

                        // Send all queued commands as a single transaction
                        let (resp_tx, resp_rx) = oneshot::channel();
                        let server_cmd = ServerCommand::Transaction {
                            queued_cmds: cmd_queue, // Just send the raw commands
                            resp: resp_tx,
                        };

                        self.server_cmd_tx.send(server_cmd).await?;
                        let resp = resp_rx.await.expect("Transaction response channel closed");
                        self.framed.send(resp).await?;
                        return Ok(());
                    }
                    ExecutionMode::Normal => {
                        self.framed
                            .send(RespDataType::SimpleError("ERR EXEC without MULTI".into()))
                            .await?;
                        return Ok(());
                    }
                }
            }
            Command::DISCARD => {
                match std::mem::replace(&mut self.execution_mode, ExecutionMode::Normal) {
                    ExecutionMode::Transaction { .. } => {
                        self.framed
                            .send(RespDataType::SimpleString("OK".into()))
                            .await?;
                        return Ok(());
                    }
                    ExecutionMode::Normal => {
                        self.framed
                            .send(RespDataType::SimpleError(
                                "ERR DISCARD without MULTI".into(),
                            ))
                            .await?;
                        return Ok(());
                    }
                }
            }
            _ => {}
        }

        // Process command based on current execution mode
        match &mut self.execution_mode {
            ExecutionMode::Transaction { cmd_queue } => {
                // Queue the command and send a QUEUED response
                cmd_queue.push(cmd);
                self.framed
                    .send(RespDataType::SimpleString("QUEUED".into()))
                    .await?;
            }
            ExecutionMode::Normal => {
                // Normal command execution flow
                let (resp_tx, resp_rx) = oneshot::channel();

                let server_cmd = match cmd {
                    // Storage related commands
                    Command::GET { .. }
                    | Command::SET { .. }
                    | Command::RPUSH { .. }
                    | Command::LRANGE { .. }
                    | Command::LPUSH { .. }
                    | Command::LLEN { .. }
                    | Command::LPOP { .. }
                    | Command::INCR { .. }
                    | Command::BLPOP { .. } => ServerCommand::Storage { cmd, resp: resp_tx },

                    // MISC Related commands
                    Command::INFO { .. } | Command::PING | Command::ECHO(_) => {
                        ServerCommand::Misc { cmd, resp: resp_tx }
                    }

                    // Replication related commands
                    Command::REPLCONF { .. } | Command::PSYNC { .. } => {
                        // Register as a replica if not already
                        if self.replica_rx.is_none() {
                            self.register_replica().await?;
                        }
                        ServerCommand::Replication {
                            cmd,
                            replica_id: self.replica_id.context("replica state uninitialized")?,
                            resp: resp_tx,
                        }
                    }

                    // Transaction commands should be handled above
                    Command::MULTI | Command::EXEC | Command::DISCARD => {
                        unreachable!("Transaction commands should be handled above")
                    }
                };

                self.server_cmd_tx.send(server_cmd).await?;
                let resp = resp_rx.await?;
                self.framed.send(resp).await?;
            }
        }

        Ok(())
    }

    /// Sends the RDB file after PSYNC response
    async fn send_rdb_file(&mut self) -> Result<()> {
        // Empty RDB file as hex bytes
        let empty_rdb = include_bytes!("../empty.rdb");

        // Get the underlying TCP stream
        let stream = self.framed.get_mut(); // is this bypass ok?

        // Send RDB file in the format: $<length>\r\n<binary_contents>
        let rdb_response = format!("${}\r\n", empty_rdb.len());
        stream.write_all(rdb_response.as_bytes()).await?;
        stream.write_all(empty_rdb).await?;
        stream.flush().await?;

        Ok(())
    }
}

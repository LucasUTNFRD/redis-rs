use crate::resp::{RespCodec, RespDataType};
use crate::{cmd::Command, storage::StorageHandle};
use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use std::collections::VecDeque;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

/// Represents a Redis server that handles client connections
pub struct RedisServer {
    listener: TcpListener,
    storage: StorageHandle,
}

impl RedisServer {
    /// Creates a new Redis server bound to the specified address
    pub async fn new(addr: &str) -> Result<Self> {
        let listener = TcpListener::bind(addr)
            .await
            .context("Failed to bind to address")?;

        let storage = StorageHandle::new();

        Ok(Self { listener, storage })
    }

    /// Starts the server and begins accepting connections
    pub async fn run(self) -> Result<()> {
        println!("Redis server started on {}", self.listener.local_addr()?);

        loop {
            let (socket, peer_addr) = self.listener.accept().await?;
            println!("Accepted new connection from: {}", peer_addr);

            let storage = self.storage.clone();

            tokio::spawn(async move {
                let mut connection = Connection::new(socket, storage);
                if let Err(e) = connection.handle().await {
                    eprintln!("Error handling connection from {}: {:?}", peer_addr, e);
                }
            });
        }
    }
}

/// Represents an individual client connection
pub struct Connection {
    framed: Framed<TcpStream, RespCodec>,
    storage: StorageHandle,
    transaction_queue: Option<VecDeque<Command>>,
}

impl Connection {
    /// Creates a new connection with the given socket and storage handle
    pub fn new(socket: TcpStream, storage: StorageHandle) -> Self {
        let framed = Framed::new(socket, RespCodec);

        Self {
            framed,
            storage,
            transaction_queue: None,
        }
    }

    /// Handles the connection lifecycle, processing commands until the connection closes
    pub async fn handle(&mut self) -> Result<()> {
        while let Some(resp_result) = self.framed.next().await {
            let resp_data = resp_result.context("Decoding failed")?;
            let cmd = Command::try_from(resp_data);

            match cmd {
                Ok(cmd) => {
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
            _ => self.storage.send(cmd).await,
        }
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

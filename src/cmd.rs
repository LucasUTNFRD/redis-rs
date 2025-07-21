use std::time::Duration;

use anyhow::{bail, Context};

use crate::resp::RespDataType;

#[derive(Debug, Clone)]
pub enum Command {
    Ping,
    Echo(String),
    Set {
        key: String,
        val: String,
        px: Option<Duration>, // in milliseconds
    },
    Get {
        key: String,
    },
}

impl TryFrom<RespDataType> for Command {
    type Error = anyhow::Error;
    fn try_from(resp: RespDataType) -> std::result::Result<Self, Self::Error> {
        match resp {
            RespDataType::Array(parts) => {
                if parts.is_empty() {
                    bail!("Empty command array");
                }

                let cmd = match &parts[0] {
                    RespDataType::BulkString(cmd) | RespDataType::SimpleString(cmd) => {
                        cmd.to_uppercase()
                    }
                    _ => bail!("Command must be a string type"),
                };

                match cmd.as_str() {
                    "PING" => {
                        if parts.len() > 1 {
                            bail!("PING command takes no arguments");
                        }
                        Ok(Command::Ping)
                    }
                    "ECHO" => {
                        if parts.len() != 2 {
                            bail!("ECHO command requires exactly 1 argument");
                        }
                        match &parts[1] {
                            RespDataType::BulkString(msg) => Ok(Command::Echo(msg.clone())),
                            _ => bail!("ECHO message must be a bulk string"),
                        }
                    }
                    "GET" => {
                        if parts.len() != 2 {
                            bail!("GET command requires exactly 1 argument");
                        }
                        match &parts[1] {
                            RespDataType::BulkString(key) => Ok(Command::Get { key: key.clone() }),
                            _ => bail!("GET key must be a bulk string"),
                        }
                    }
                    "SET" => {
                        if parts.len() < 3 || parts.len() > 5 {
                            bail!("SET command requires 2 or 4 arguments (key, value, [PX, milliseconds])");
                        }

                        let key = match &parts[1] {
                            RespDataType::BulkString(key) => key.clone(),
                            _ => bail!("SET key must be a bulk string"),
                        };

                        let val = match &parts[2] {
                            RespDataType::BulkString(val) => val.clone(),
                            _ => bail!("SET value must be a bulk string"),
                        };

                        let px = if parts.len() > 3 {
                            match (&parts[3], parts.get(4)) {
                                (
                                    RespDataType::BulkString(opt),
                                    Some(RespDataType::BulkString(ms)),
                                ) => {
                                    if opt.to_uppercase() == "PX" {
                                        let milliseconds = ms
                                            .parse::<u64>()
                                            .context("PX value must be a valid number")?;
                                        Some(Duration::from_millis(milliseconds))
                                    } else {
                                        bail!("Only PX option is supported for SET");
                                    }
                                }
                                _ => bail!("Invalid SET options format"),
                            }
                        } else {
                            None
                        };

                        Ok(Command::Set { key, val, px })
                    }
                    _ => bail!("Unknown command: {}", cmd),
                }
            }
            _ => bail!("Command must be an array of RESP types"),
        }
    }
}

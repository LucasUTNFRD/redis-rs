use std::time::Duration;

use anyhow::{bail, Context};

use crate::resp::RespDataType;

#[derive(Debug, Clone)]
pub enum Command {
    PING,
    ECHO(String),
    SET {
        key: String,
        val: String,
        px: Option<Duration>, // in milliseconds
    },
    GET {
        key: String,
    },
    RPUSH {
        key: String,
        elements: Vec<String>,
    },
    /// The LRANGE command is used to list the elements in a list given a start index and end index. The index of the first element 0. The end index is inclusive, which means that the element at the end index will be included in the response.
    LRANGE {
        key: String,
        start: i64,
        stop: i64,
    },
    LPUSH {
        key: String,
        elements: Vec<String>,
    },
    LLEN {
        key: String,
    },
    LPOP {
        key: String,
        count: Option<i64>,
    },
    // NOT implemented
    BLPOP {
        keys: Vec<String>,
        timeout: Duration,
    },
    INCR {
        key: String,
    },
    MULTI,
    EXEC,
    DISCARD,
    INFO {
        section: Option<Section>,
    },
    REPLCONF,
    PSYNC {
        replication_id: String,
        offset: i64,
    },
}

#[derive(Debug, Clone)]
pub enum Section {
    Replication,
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
                        Ok(Command::PING)
                    }
                    "ECHO" => {
                        if parts.len() != 2 {
                            bail!("ECHO command requires exactly 1 argument");
                        }
                        match &parts[1] {
                            RespDataType::BulkString(msg) => Ok(Command::ECHO(msg.clone())),
                            _ => bail!("ECHO message must be a bulk string"),
                        }
                    }
                    "GET" => {
                        if parts.len() != 2 {
                            bail!("GET command requires exactly 1 argument");
                        }
                        match &parts[1] {
                            RespDataType::BulkString(key) => Ok(Command::GET { key: key.clone() }),
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

                        Ok(Command::SET { key, val, px })
                    }
                    "RPUSH" => {
                        if parts.len() < 3 {
                            bail!("RPush command requires 3 or more arguments RPUSH key element [element ...]");
                        }

                        let key = match &parts[1] {
                            RespDataType::BulkString(key) => key.clone(),
                            _ => bail!("RPUSH key must be a bulk string"),
                        };

                        let elements = parts[2..]
                            .iter()
                            .map(|p| match p {
                                RespDataType::BulkString(s) => Ok(s.clone()),
                                _ => bail!("RPUSH values must be bulk strings"),
                            })
                            .collect::<Result<Vec<String>, anyhow::Error>>()?;

                        Ok(Command::RPUSH { key, elements })
                    }
                    "LRANGE" => {
                        // LRANGE key start stop
                        if parts.len() != 4 {
                            bail!("LRANGE LRANGE key start stop");
                        }
                        match (&parts[1], &parts[2], &parts[3]) {
                            (
                                RespDataType::BulkString(key),
                                RespDataType::BulkString(start),
                                RespDataType::BulkString(stop),
                            ) => Ok(Command::LRANGE{
                                key: key.clone(),
                                start: start.parse().context("Failed to parse Start ")?,
                                stop: stop.parse().context("Failed to parse Stop")?,
                            }),
                            (part_1, part_2, part_3) => bail!("LRANGE params must be a bulk string, got ({part_1:#?},{part_2:#?},{part_3:#?})"),

                        }
                    }
                    "LPUSH" => {
                        if parts.len() < 3 {
                            bail!("LPush command requires 3 or more arguments RPUSH key element [element ...]");
                        }

                        let key = match &parts[1] {
                            RespDataType::BulkString(key) => key.clone(),
                            _ => bail!("LPUSH key must be a bulk string"),
                        };

                        let elements = parts[2..]
                            .iter()
                            .map(|p| match p {
                                RespDataType::BulkString(s) => Ok(s.clone()),
                                _ => bail!("LPUSH values must be bulk strings"),
                            })
                            .collect::<Result<Vec<String>, anyhow::Error>>()?;

                        Ok(Command::LPUSH { key, elements })
                    }
                    "LLEN" => {
                        if parts.len() != 2 {
                            bail!("LLEN command requires exactly 1 argument");
                        }
                        match &parts[1] {
                            RespDataType::BulkString(key) => Ok(Command::LLEN { key: key.clone() }),
                            _ => bail!("LLEN key must be a bulk string"),
                        }
                    }
                    "LPOP" => {
                        if parts.len() < 2 || parts.len() > 3 {
                            bail!("LPOP command requires 1 or 2 arguments (key, [count])");
                        }

                        let key = match &parts[1] {
                            RespDataType::BulkString(key) => key.clone(),
                            _ => bail!("LPOP key must be a bulk string"),
                        };

                        let count = if let Some(RespDataType::BulkString(s)) = parts.get(2) {
                            Some(
                                s.parse::<i64>()
                                    .context("LPOP count mas be a valid integer")?,
                            )
                        } else {
                            None
                        };

                        Ok(Command::LPOP { key, count })
                    }

                    "BLPOP" => {
                        if parts.len() < 2 {
                            bail!("BLPOP requires at least one key and a timeout");
                        }

                        // All elements except the last are keys
                        let keys = parts[1..parts.len() - 1]
                            .iter()
                            .map(|p| match p {
                                RespDataType::BulkString(key) => Ok(key.clone()),
                                _ => bail!("BLPOP keys must be bulk strings"),
                            })
                            .collect::<Result<Vec<String>, anyhow::Error>>()?;

                        if keys.is_empty() {
                            bail!("BLPOP requires at least one key");
                        }

                        let timeout = match &parts[parts.len() - 1] {
                            RespDataType::BulkString(timeout_str) => timeout_str
                                .parse::<u64>()
                                .context("Timeout must be a valid unsigned integer")?,
                            _ => bail!("Timeout must be a bulk string"),
                        };

                        let timeout = Duration::from_secs(timeout);

                        Ok(Command::BLPOP { keys, timeout })
                    }
                    "INCR" => {
                        if parts.len() != 2 {
                            bail!("INCR command requires exactly 1 argument");
                        }
                        match &parts[1] {
                            RespDataType::BulkString(key) => Ok(Command::INCR { key: key.clone() }),
                            _ => bail!("GET key must be a bulk string"),
                        }
                    }
                    "MULTI" => {
                        if parts.len() > 1 {
                            bail!("MULTI command takes no arguments");
                        }
                        Ok(Command::MULTI)
                    }

                    "EXEC" => {
                        if parts.len() > 1 {
                            bail!("EXEC command takes no arguments");
                        }
                        Ok(Command::EXEC)
                    }

                    "DISCARD" => {
                        if parts.len() > 1 {
                            bail!("DISCARD command takes no arguments");
                        }
                        Ok(Command::DISCARD)
                    }
                    "REPLCONF" => {
                        if let Some(args) = parts.get(1..) {
                            println!("{args:?}");
                        }
                        Ok(Command::REPLCONF)
                    }

                    "INFO" => match parts.get(2) {
                        Some(RespDataType::BulkString(param)) => match param.as_str() {
                            "replication" => Ok(Command::INFO {
                                section: Some(Section::Replication),
                            }),
                            _ => bail!("ERR unsupported INFO section"),
                        },
                        Some(_) => bail!("ERR expected BulkString for section"),
                        None => Ok(Command::INFO { section: None }),
                    },
                    "PSYNC" => {
                        if parts.len() != 3 {
                            bail!("expected 3 parameters in psync");
                        }
                        match (&parts[1], &parts[2]) {
                            (
                                RespDataType::BulkString(replica_id),
                                RespDataType::BulkString(master_offset),
                            ) => Ok(Command::PSYNC {
                                replication_id: replica_id.clone(),
                                offset: master_offset
                                    .parse()
                                    .expect("Failed to take offset as i64"),
                            }),
                            _ => bail!("lazy to handle this"),
                        }
                    }
                    _ => bail!("Unknown command: {}", cmd),
                }
            }
            _ => bail!("Command must be an array of RESP types"),
        }
    }
}

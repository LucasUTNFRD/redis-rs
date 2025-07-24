use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::resp::RespDataType;

#[derive(Default)]
pub struct Strings {
    inner: HashMap<String, Value>,
}

struct Value {
    data: String,
    expires_at: Option<Instant>,
}

impl Value {
    pub fn new(data: String, expiry: Option<Duration>) -> Self {
        Self {
            data,
            expires_at: expiry.map(|expiry| Instant::now() + expiry),
        }
    }
    pub fn is_expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|expiry| now > expiry)
    }
}

impl Strings {
    pub fn set(&mut self, key: String, value: String, expiry: Option<Duration>) -> RespDataType {
        self.inner.insert(key, Value::new(value, expiry));
        RespDataType::SimpleString("OK".into())
    }

    pub fn increment(&mut self, key: String) -> RespDataType {
        match self.inner.get_mut(&key) {
            Some(entry) if !entry.is_expired(Instant::now()) => {
                // Try to parse the current value as an integer
                match entry.data.parse::<i64>() {
                    Ok(current_value) => {
                        let new_value = current_value + 1;
                        entry.data = new_value.to_string();
                        RespDataType::Integer(new_value)
                    }
                    Err(_) => {
                        // Key exists but value is not a valid integer
                        // This will be handled in later stages
                        RespDataType::SimpleError(
                            "ERR value is not an integer or out of range".into(),
                        )
                    }
                }
            }
            Some(_) => {
                // Key exists but is expired - remove it and treat as non-existent
                self.inner.remove(&key);
                // This will be handled in later stages (key doesn't exist)
                RespDataType::SimpleError("Key expired - later stage".into())
            }
            None => {
                let default_value = Value::new(1.to_string(), None);
                self.inner.insert(key, default_value);
                RespDataType::Integer(1)
            }
        }
    }

    pub fn get(&mut self, key: &str) -> RespDataType {
        match self.inner.get(key) {
            Some(entry) if !entry.is_expired(Instant::now()) => {
                RespDataType::BulkString(entry.data.clone())
            }
            Some(_) => {
                if let Some(entry) = self.inner.get(key) {
                    if entry.is_expired(Instant::now()) {
                        self.inner.remove(key);
                    }
                }
                RespDataType::NullBulkString
            }
            None => RespDataType::NullBulkString,
        }
    }
}

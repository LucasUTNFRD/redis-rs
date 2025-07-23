use std::{
    collections::HashMap,
    sync::RwLock,
    time::{Duration, Instant},
};

use crate::resp::RespDataType;

#[derive(Default)]
pub struct Strings {
    inner: RwLock<HashMap<String, Value>>,
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
    pub fn set(&self, key: String, value: String, expiry: Option<Duration>) -> RespDataType {
        let mut store = self.inner.write().unwrap();

        store.insert(key, Value::new(value, expiry));
        RespDataType::SimpleString("OK".into())
    }

    pub fn get(&self, key: &str) -> RespDataType {
        let store = self.inner.read().unwrap();
        match store.get(key) {
            Some(entry) if !entry.is_expired(Instant::now()) => {
                RespDataType::BulkString(entry.data.clone())
            }
            Some(_) => {
                drop(store);
                let mut store = self.inner.write().unwrap();
                //double-check in case that the entry expired while get rwlock
                if let Some(entry) = store.get(key) {
                    if entry.is_expired(Instant::now()) {
                        store.remove(key);
                    }
                }
                RespDataType::NullBulkString
            }
            None => RespDataType::NullBulkString,
        }
    }
}

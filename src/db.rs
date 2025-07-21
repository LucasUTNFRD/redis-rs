use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::RwLock;

use crate::resp::RespDataType;

#[derive(Debug, Default, Clone)]
pub struct KvStore {
    inner: Arc<RwLock<HashMap<String, Entry>>>,
    instant: Duration,
}

#[derive(Debug, Default)]
struct Entry {
    pub val: String,
    expires_at: Option<Instant>,
}

impl Entry {
    pub fn new(val: String, expiry: Option<Duration>) -> Self {
        Self {
            val,
            expires_at: expiry.map(|expiry| Instant::now() + expiry),
        }
    }

    pub fn is_expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|expiry| now > expiry)
    }
}

impl KvStore {
    pub async fn set(&self, key: String, value: String, expiry: Option<Duration>) -> RespDataType {
        let mut store = self.inner.write().await;

        store.insert(key, Entry::new(value, expiry));
        RespDataType::SimpleString("OK".into())
    }

    pub async fn get(&self, key: &str) -> RespDataType {
        let store = self.inner.read().await;
        match store.get(key) {
            Some(entry) if !entry.is_expired(Instant::now()) => {
                RespDataType::BulkString(entry.val.clone())
            }
            Some(_) => RespDataType::NullBulkString,
            None => RespDataType::NullBulkString,
        }
    }
}

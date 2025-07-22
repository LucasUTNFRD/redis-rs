use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use std::sync::RwLock;

use crate::resp::RespDataType;

#[derive(Debug, Default, Clone)]
pub struct KvStore {
    // Most Redis operations are simple HashMap gets/sets. Critical sections are very short. No Critical sections are very short
    inner: Arc<RwLock<HashMap<String, Entry>>>,
    instant: Duration,
}

#[derive(Debug)]
struct Entry {
    pub val: Value,
    expires_at: Option<Instant>,
}

#[derive(Debug)]
enum Value {
    List(VecDeque<String>),
    String(String),
}

impl Entry {
    pub fn new(val: String, expiry: Option<Duration>) -> Self {
        Self {
            val: Value::String(val),
            expires_at: expiry.map(|expiry| Instant::now() + expiry),
        }
    }

    pub fn is_expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|expiry| now > expiry)
    }

    pub fn new_list(elems: VecDeque<String>) -> Self {
        Self {
            val: Value::List(elems),
            expires_at: None,
        }
    }
}

// note:
// Redis handles expired keys through a combination of passive expiration and active expiration.
// Both mechanisms primarily operate within Redis's single-threaded main event loop,

// TODO::
// Implement an Active Expiration mechanism.

impl KvStore {
    pub fn set(&self, key: String, value: String, expiry: Option<Duration>) -> RespDataType {
        let mut store = self.inner.write().unwrap();

        store.insert(key, Entry::new(value, expiry));
        RespDataType::SimpleString("OK".into())
    }

    pub fn lrange(&self, key: String, start: usize, stop: usize) -> RespDataType {
        let store = self.inner.read().unwrap();
        if let Some(entry) = store.get(&key) {
            match &entry.val {
                Value::List(list) => {
                    if start >= list.len() {
                        return RespDataType::Array(vec![]);
                    }

                    let stop = if stop > list.len() {
                        list.len() - 1
                    } else {
                        stop
                    } + 1;

                    if start > stop {
                        return RespDataType::Array(vec![]);
                    }

                    let elems = list
                        .range(start..stop)
                        .cloned()
                        .map(RespDataType::BulkString)
                        .collect::<Vec<RespDataType>>();

                    RespDataType::Array(elems)
                }

                _ => RespDataType::SimpleError(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            }
        } else {
            // if list does not exist an empty array is returned
            RespDataType::Array(vec![])
        }
    }
    // insert all the specified values at the tail of the list stored at key.
    // If key does not exist, it is created as empty list before performing the push operation.
    // When key holds a value that is not a list, an error is returned.
    // It is possible to push multiple elements using a single command call just specifying multiple arguments at the end of the command.
    // Elements are inserted one after the other to the tail of the list, from the leftmost element to the rightmost element.
    // So for instance the command RPUSH mylist a b c will result into a list containing a as first element, b as second element and c as third element.
    pub fn rpush(&self, key: String, values: Vec<String>) -> RespDataType {
        let mut store = self.inner.write().unwrap();
        let entry = store
            .entry(key)
            .or_insert_with(|| Entry::new_list(VecDeque::new()));

        match &mut entry.val {
            Value::List(list) => {
                list.extend(values);
                RespDataType::Integer(list.len() as i64)
            }
            _ => RespDataType::SimpleError(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
        }
    }

    pub fn get(&self, key: &str) -> RespDataType {
        let store = self.inner.read().unwrap();
        match store.get(key) {
            Some(entry) if !entry.is_expired(Instant::now()) => match &entry.val {
                Value::String(s) => RespDataType::BulkString(s.clone()),
                Value::List(_) => RespDataType::SimpleError(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
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

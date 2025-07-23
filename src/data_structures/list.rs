use std::collections::{HashMap, VecDeque};

use crate::resp::RespDataType;

/// A thread-safe Redis-like list data structure implementation.
///
/// `Lists` provides operations for managing named lists of strings, similar to Redis lists.
/// Each list is identified by a string key and supports operations like push, pop, and range queries.
/// All operations are thread-safe through the use of `RwLock`.
#[derive(Default)]
pub struct Lists {
    /// Internal storage mapping list names to their contents.
    /// Uses `VecDeque` for efficient operations at both ends of the list.
    inner: HashMap<String, BlockingList>,
}

#[derive(Default)]
struct BlockingList {
    inner: VecDeque<String>,
}

impl Lists {
    /// Returns the length of the list stored at the specified key.
    ///
    /// # Arguments
    ///
    /// * `key` - The name of the list to query
    ///
    /// # Returns
    ///
    /// * `RespDataType::Integer` - The length of the list, or 0 if the key doesn't exist
    ///
    pub fn get_list_len(&self, key: &str) -> RespDataType {
        let len = self
            .inner
            .get(key)
            .map(|list| list.inner.len() as i64)
            .unwrap_or(0);

        RespDataType::Integer(len)
    }

    /// Prepends one or more values to the head of the list stored at key.
    ///
    /// If the key does not exist, it is created as an empty list before performing the push operation.
    /// Values are inserted in reverse order, so the last value becomes the first element.
    ///
    /// # Arguments
    ///
    /// * `key` - The name of the list
    /// * `values` - Vector of string values to prepend to the list
    ///
    /// # Returns
    ///
    /// * `RespDataType::Integer` - The length of the list after the push operation
    ///
    pub fn lpush(&mut self, key: String, values: Vec<String>) -> RespDataType {
        let list = self.inner.entry(key).or_default();

        for v in values {
            list.inner.push_front(v);
        }

        RespDataType::Integer(list.inner.len() as i64)
    }

    /// Removes and returns elements from the head of the list stored at key.
    ///
    /// # Arguments
    ///
    /// * `key` - The name of the list
    /// * `count` - Optional number of elements to pop. If `None`, pops a single element.
    ///   If `Some(n)`, pops up to `n` elements (or all remaining if fewer exist).
    ///
    /// # Returns
    ///
    /// * When `count` is `None`:
    ///   - `RespDataType::BulkString` - The popped element
    ///   - `RespDataType::NullBulkString` - If the key doesn't exist or list is empty
    /// * When `count` is `Some(n)`:
    ///   - `RespDataType::Array` - Array of popped elements (may be empty)
    ///
    pub fn left_pop(&mut self, key: &str, count: Option<i64>) -> RespDataType {
        let list = match self.inner.get_mut(key) {
            Some(list) if !list.inner.is_empty() => list,
            Some(_) => return RespDataType::NullBulkString,
            None => return RespDataType::NullBulkString,
        };

        match count {
            Some(n) => {
                let n = n.max(0) as usize;
                let elements = list
                    .inner
                    .drain(..n.min(list.inner.len()))
                    .map(RespDataType::BulkString)
                    .collect();
                RespDataType::Array(elements)
            }
            None => {
                // safety: list has been checked that is not emtpy
                let val = list.inner.pop_front().unwrap();
                RespDataType::BulkString(val)
            }
        }
    }

    /// Appends one or more values to the tail of the list stored at key.
    ///
    /// If the key does not exist, it is created as an empty list before performing the push operation.
    /// Values are appended in the order they appear in the input vector.
    ///
    /// # Arguments
    ///
    /// * `key` - The name of the list
    /// * `values` - Vector of string values to append to the list
    ///
    /// # Returns
    ///
    /// * `RespDataType::Integer` - The length of the list after the push operation
    ///
    pub fn rpush(&mut self, key: String, values: Vec<String>) -> RespDataType {
        let list = self.inner.entry(key).or_default();
        list.inner.extend(values);
        RespDataType::Integer(list.inner.len() as i64)
    }

    // Returns the specified elements of the list stored at key.
    ///
    /// The indices `start` and `stop` are zero-based, where 0 is the first element,
    /// 1 is the second element, and so on. Negative indices can be used to designate
    /// elements starting from the tail of the list, where -1 is the last element,
    /// -2 is the penultimate, and so on.
    ///
    /// # Arguments
    ///
    /// * `key` - The name of the list
    /// * `start` - Start index (inclusive). Can be negative.
    /// * `stop` - Stop index (inclusive). Can be negative.
    ///
    /// # Returns
    ///
    /// * `RespDataType::Array` - Array containing the elements in the specified range.
    ///   Returns an empty array if the key doesn't exist, the list is empty,
    ///   or the range is invalid (start > stop).
    ///
    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> RespDataType {
        let Some(list) = self.inner.get(key) else {
            return RespDataType::Array(vec![]);
        };

        let len = list.inner.len() as i64;
        if len == 0 {
            return RespDataType::Array(vec![]);
        }

        // Convert negative indices to positive
        let start_idx = normalize_index(start, len);
        let stop_idx = normalize_index(stop, len);

        // Check if range is valid
        if start_idx > stop_idx {
            return RespDataType::Array(vec![]);
        }

        let elements: Vec<RespDataType> = list
            .inner
            .range(start_idx..=stop_idx)
            .cloned()
            .map(RespDataType::BulkString)
            .collect();

        RespDataType::Array(elements)
    }
}

/// Converts a potentially negative index to a valid positive index within bounds.
///
/// Negative indices are converted by adding them to the length (e.g., -1 becomes len-1).
/// The result is clamped to the valid range [0, len-1].
///
/// # Arguments
///
/// * `index` - The index to normalize (can be negative)
/// * `len` - The length of the collection
///
/// # Returns
///
/// * `usize` - A valid index within [0, len-1]
///
fn normalize_index(index: i64, len: i64) -> usize {
    let normalized = if index < 0 { len + index } else { index };
    normalized.clamp(0, len - 1) as usize
}

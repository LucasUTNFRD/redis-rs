use std::{
    io::{Error, ErrorKind},
    str::from_utf8,
};

use anyhow::{bail, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

pub struct RespCodec;

impl RespCodec {}

#[derive(Debug, PartialEq, Eq)]
pub enum RespDataType {
    BulkString(String),
    NullBulkString,
    SimpleError(String),
    Array(Vec<RespDataType>),
    SimpleString(String),
    Integer(i64),
}

const SIMPLE_STRING_BYTE: u8 = b'+';
const ARRAY_BYTE: u8 = b'*';
const BULK_STRING_BYTE: u8 = b'$';
const ERROR_BYTE: u8 = b'-';
const INTEGER_BYTE: u8 = b':';
const CRLF: &[u8] = b"\r\n";

pub enum RespError {}

impl Decoder for RespCodec {
    type Item = RespDataType;
    type Error = std::io::Error;

    /// * `Ok(Some(Vec<RespType>))` if a complete command (array of bulk strings) was successfully decoded.
    /// * `Ok(None)` if more data is needed to complete the command.
    /// * `Err(std::io::Error)` if an error occurred during decoding.
    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        match src[0] {
            SIMPLE_STRING_BYTE => parse_simple_string(src),
            ARRAY_BYTE => parse_array(src),
            BULK_STRING_BYTE => parse_bulk_string(src),
            INTEGER_BYTE => parse_integer(src),

            _ => Err(Error::new(ErrorKind::InvalidData, "Unknown RESP type byte")),
        }
    }
}

// :[< + | - >]<value>\r\n
//     The colon (:) as the first byte.
//     An optional plus (+) or minus (-) as the sign.
//     One or more decimal digits (0..9) as the integer's unsigned, base-10 value.
//     The CRLF terminator.
fn parse_integer(src: &mut BytesMut) -> Result<Option<RespDataType>, std::io::Error> {
    if let Some(crlf_pos) = find_crlf(src) {
        if crlf_pos == 1 {
            return Err(Error::new(ErrorKind::InvalidData, "Empty integer"));
        }

        let integer_str = from_utf8(&src[1..crlf_pos])
            .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid UTF-8 in integer string"))?;

        let integer: i64 = integer_str
            .parse()
            .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid integer format"))?;

        src.advance(crlf_pos + CRLF.len());
        Ok(Some(RespDataType::Integer(integer)))
    } else {
        Ok(None)
    }
}

fn find_crlf(src: &BytesMut) -> Option<usize> {
    src.windows(2).position(|window| window == CRLF)
}

fn parse_simple_string(src: &mut BytesMut) -> Result<Option<RespDataType>, std::io::Error> {
    if let Some(crlf_pos) = find_crlf(src) {
        // A simple string like "+\r\n" should be an error because it has no content.
        // The CRLF starts immediately after the type byte (index 1).
        if crlf_pos == 1 {
            return Err(Error::new(ErrorKind::InvalidData, "Empty simple string"));
        }

        let content = from_utf8(&src[1..crlf_pos])
            .map_err(|_| {
                Error::new(
                    ErrorKind::InvalidData,
                    "Invalid UTF-8 in bulk string length",
                )
            })?
            .to_string();
        src.advance(crlf_pos + CRLF.len()); // Skip the content and CRLF
        Ok(Some(RespDataType::SimpleString(content)))
    } else {
        Ok(None)
    }
}

fn parse_simple_errors(src: &mut BytesMut) -> Result<Option<RespDataType>, std::io::Error> {
    if let Some(crlf_pos) = find_crlf(src) {
        // A simple string like "+\r\n" should be an error because it has no content.
        // The CRLF starts immediately after the type byte (index 1).
        if crlf_pos == 1 {
            return Err(Error::new(ErrorKind::InvalidData, "Empty simple string"));
        }

        let content = from_utf8(&src[1..crlf_pos])
            .map_err(|_| {
                Error::new(
                    ErrorKind::InvalidData,
                    "Invalid UTF-8 in bulk string length",
                )
            })?
            .to_string();
        src.advance(crlf_pos + CRLF.len()); // Skip the content and CRLF
        Ok(Some(RespDataType::SimpleError(content)))
    } else {
        Ok(None) // Need more data
    }
}

// A bulk string represents a single binary string. The string can be of any size, but by default, Redis limits it to 512 MB (see the proto-max-bulk-len configuration directive).
// RESP encodes bulk strings in the following way:
//$<length>\r\n<data>\r\n
// The dollar sign ($) as the first byte.
// One or more decimal digits (0..9) as the string's length, in bytes, as an unsigned, base-10 value.
// The CRLF terminator.
// The data.
// A final CRLF.
fn parse_bulk_string(src: &mut BytesMut) -> Result<Option<RespDataType>, std::io::Error> {
    // read string length
    if let Some(crlf_pos) = find_crlf(src) {
        if crlf_pos == 1 {
            return Err(Error::new(ErrorKind::InvalidData, "Empty simple string"));
        }

        let length_str = from_utf8(&src[1..crlf_pos]).map_err(|_| {
            Error::new(
                ErrorKind::InvalidData,
                "Invalid UTF-8 in bulk string length",
            )
        })?;

        // Parse the length
        let length: isize = length_str
            .parse()
            .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid bulk string length format"))?;

        if length == -1 {
            return Ok(Some(RespDataType::NullBulkString));
        }

        let data_len = length as usize;
        if src.len() < (crlf_pos + CRLF.len()) + data_len + CRLF.len() {
            return Ok(None);
        }
        src.advance(crlf_pos + CRLF.len());

        let content = from_utf8(&src[0..data_len])
            .map_err(|_| {
                Error::new(
                    ErrorKind::InvalidData,
                    "Invalid UTF-8 in bulk string length",
                )
            })?
            .to_string();

        src.advance(data_len + 2);
        Ok(Some(RespDataType::BulkString(content)))
    } else {
        Ok(None)
    }
}

// Clients send commands to the Redis server as RESP arrays. Similarly, some Redis commands that return collections of elements use arrays as their replies. An example is the LRANGE command that returns elements of a list.
//
// RESP Arrays' encoding uses the following format:
//
// *<number-of-elements>\r\n<element-1>...<element-n>
//
//     An asterisk (*) as the first byte.
//     One or more decimal digits (0..9) as the number of elements in the array as an unsigned, base-10 value.
//     The CRLF terminator.
//     An additional RESP type for every element of the array.
fn parse_array(src: &mut BytesMut) -> Result<Option<RespDataType>, std::io::Error> {
    if let Some(crlf_pos) = find_crlf(src) {
        if crlf_pos == 1 {
            return Err(Error::new(ErrorKind::InvalidData, "Empty simple string"));
        }

        let num_elements_str = from_utf8(&src[1..crlf_pos]).map_err(|_| {
            Error::new(
                ErrorKind::InvalidData,
                "Invalid UTF-8 in bulk string length",
            )
        })?;

        // Parse the length
        let num_elements: isize = num_elements_str
            .parse()
            .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid bulk string length format"))?;

        if num_elements == -1 {
            todo!("implement null array data type");
        }

        let num_elements = num_elements as usize;
        let mut array = Vec::with_capacity(num_elements);

        // advance from  *<number-of-elements>\r\n<element-1>...<element-n> to  <element-1>...<element-n>
        src.advance(crlf_pos + 2);
        for _ in 0..num_elements {
            if src.is_empty() {
                return Ok(None);
            }

            let first_byte = match src.first() {
                Some(&byte) => byte,
                None => return Ok(None),
            };

            match first_byte {
                SIMPLE_STRING_BYTE => {
                    if let Some(simple_str) = parse_simple_string(src)? {
                        array.push(simple_str);
                    } else {
                        return Ok(None);
                    }
                }
                ARRAY_BYTE => {
                    if let Some(simple_str) = parse_array(src)? {
                        array.push(simple_str);
                    } else {
                        return Ok(None);
                    }
                }
                BULK_STRING_BYTE => {
                    if let Some(simple_str) = parse_bulk_string(src)? {
                        array.push(simple_str);
                    } else {
                        return Ok(None);
                    }
                }
                _ => return Err(Error::new(ErrorKind::InvalidData, "Invalid RESP data type")),
            }
        }

        Ok(Some(RespDataType::Array(array)))
        // todo!()
    } else {
        Ok(None)
    }
}

impl Encoder<RespDataType> for RespCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: RespDataType, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        dst.put_slice(&item.as_bytes());
        Ok(())
    }
}

impl RespDataType {
    pub fn get_str(&self) -> anyhow::Result<String> {
        match self {
            RespDataType::BulkString(s) | RespDataType::SimpleString(s) => Ok(s.clone()),
            _ => bail!("Expected string type"),
        }
    }
    pub fn as_bytes(&self) -> Bytes {
        match self {
            RespDataType::SimpleString(s) => {
                let len = 1 + s.len() + CRLF.len(); // '+'s + data + \r\n
                let mut buf = BytesMut::with_capacity(len);
                buf.put_u8(SIMPLE_STRING_BYTE);
                buf.put_slice(s.as_bytes());
                buf.put_slice(CRLF);
                buf.freeze()
            }
            RespDataType::SimpleError(s) => {
                let len = 1 + s.len() + CRLF.len(); // '-'s + data + \r\n
                let mut buf = BytesMut::with_capacity(len);
                buf.put_u8(ERROR_BYTE);
                buf.put_slice(s.as_bytes());
                buf.put_slice(CRLF);
                buf.freeze()
            }
            RespDataType::BulkString(s) => {
                let len_bytes = s.len().to_string(); // length prefix
                let len = 1 + len_bytes.len() + CRLF.len()   // '$' + len + \r\n
                        + s.len() + CRLF.len(); // data + \r\n
                let mut buf = BytesMut::with_capacity(len);
                buf.put_u8(BULK_STRING_BYTE);
                buf.put_slice(len_bytes.as_bytes());
                buf.put_slice(CRLF);
                buf.put_slice(s.as_bytes());
                buf.put_slice(CRLF);
                buf.freeze()
            }
            RespDataType::Array(arr) => {
                let len_str = arr.len().to_string();
                // Compute the length of the prefix: *<len>\r\n
                let mut total_len = 1 + len_str.len() + CRLF.len();

                // Compute the total size ahead of time if desired
                let elems_bytes: Vec<Bytes> = arr.iter().map(|elem| elem.as_bytes()).collect();
                for b in &elems_bytes {
                    total_len += b.len();
                }

                let mut buf = BytesMut::with_capacity(total_len);
                buf.put_u8(ARRAY_BYTE);
                buf.put_slice(len_str.as_bytes());
                buf.put_slice(CRLF);

                for b in elems_bytes {
                    buf.put_slice(&b);
                }

                buf.freeze()
            }
            RespDataType::NullBulkString => {
                let mut buf = BytesMut::with_capacity(1 + 1 + CRLF.len());
                buf.put_u8(BULK_STRING_BYTE);
                buf.put_slice(b"-1");
                buf.put_slice(CRLF);
                buf.freeze()
            }
            Self::Integer(int) => {
                let int_str = int.to_string();
                let len = 1 + int_str.len() + CRLF.len();
                let mut buf = BytesMut::with_capacity(len);
                buf.put_u8(INTEGER_BYTE);
                buf.put_slice(int_str.as_bytes());
                buf.put_slice(CRLF);
                buf.freeze()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use std::io::ErrorKind;

    // Helper function to create BytesMut from a string
    fn bytes_from_str(s: &str) -> BytesMut {
        BytesMut::from(s)
    }

    #[test]
    fn test_parse_simple_string_success() {
        let mut buf = bytes_from_str("+OK\r\n");
        let result = parse_simple_string(&mut buf).unwrap();
        assert!(result.is_some());
        if let Some(RespDataType::SimpleString(s)) = result {
            assert_eq!(s, "OK");
        } else {
            panic!("Expected SimpleString");
        }
        // Ensure buffer is consumed
        assert!(buf.is_empty());

        let mut buf = bytes_from_str("+Hello World\r\nRemaining Data");
        let result = parse_simple_string(&mut buf).unwrap();
        assert!(result.is_some());
        if let Some(RespDataType::SimpleString(s)) = result {
            assert_eq!(s, "Hello World");
        } else {
            panic!("Expected SimpleString");
        }
        // Ensure only the simple string and CRLF are consumed
        assert_eq!(buf.to_vec(), b"Remaining Data");
    }

    #[test]
    fn test_parse_simple_string_not_enough_data() {
        let mut buf = bytes_from_str("+OK");
        let result = parse_simple_string(&mut buf).unwrap();
        assert!(result.is_none());
        // Buffer should not be consumed
        assert_eq!(buf.to_vec(), b"+OK");
    }

    #[test]
    fn test_parse_simple_string_empty_string_error() {
        let mut buf = bytes_from_str("+\r\n");
        let result = parse_simple_string(&mut buf);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn test_parse_simple_errors_success() {
        let mut buf = bytes_from_str("-Error message\r\n");
        let result = parse_simple_errors(&mut buf).unwrap();
        assert!(result.is_some());
        // The current implementation of parse_simple_errors returns SimpleString,
        // which matches the provided code. If it were to return SimpleError,
        // this test would need adjustment.
        if let Some(RespDataType::SimpleError(e)) = result {
            assert_eq!(e, "Error message");
        } else {
            panic!("Expected SimpleString (as per current parse_simple_errors implementation)");
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_simple_errors_not_enough_data() {
        let mut buf = bytes_from_str("-Error");
        let result = parse_simple_errors(&mut buf).unwrap();
        assert!(result.is_none());
        assert_eq!(buf.to_vec(), b"-Error");
    }

    #[test]
    fn test_parse_simple_errors_empty_string_error() {
        let mut buf = bytes_from_str("-\r\n");
        let result = parse_simple_errors(&mut buf);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut buf = bytes_from_str("$3\r\nhey\r\n");
        let result = parse_bulk_string(&mut buf).unwrap();
        if let Some(RespDataType::BulkString(s)) = result {
            assert_eq!(s, "hey");
        } else {
            panic!("Expected BulkString");
        }
        // Ensure buffer is consumed
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_array() {
        let mut buf = bytes_from_str("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n");
        let result = parse_array(&mut buf).unwrap();
        if let Some(RespDataType::Array(array)) = result {
            assert_eq!(array[0], RespDataType::BulkString("ECHO".to_string()));
            assert_eq!(array[1], RespDataType::BulkString("hey".to_string()));
        } else {
            panic!("Expected array");
        }
        // Ensure buffer is consumed
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_array_pt_2() {
        let mut buf = bytes_from_str("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n");
        let result = parse_array(&mut buf).unwrap();
        if let Some(RespDataType::Array(array)) = result {
            assert_eq!(array[0], RespDataType::BulkString("ECHO".to_string()));
            assert_eq!(array[1], RespDataType::BulkString("hey".to_string()));
        } else {
            panic!("Expected array");
        }
        // Ensure buffer is consumed
        assert!(buf.is_empty());
    }
    #[test]
    fn test_parse_intger() {
        let mut buf = bytes_from_str("$-1\r\n");
        let result = parse_integer(&mut buf).unwrap();
        if let Some(RespDataType::Integer(int)) = result {
            assert_eq!(int, -1);
        } else {
            panic!("Expected array");
        }
        // Ensure buffer is consumed
        assert!(buf.is_empty());
    }

    #[test]
    fn test_encoded_bulk_str() {
        let expected_bytes = bytes_from_str("$4\r\nECHO\r\n");
        let resp_data_type = RespDataType::BulkString("ECHO".to_string());

        assert_eq!(resp_data_type.as_bytes(), expected_bytes)
    }

    #[test]
    fn test_encoded_array() {
        let expected_bytes = bytes_from_str("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n");
        let resp_data_type = RespDataType::Array(vec![
            RespDataType::BulkString("ECHO".to_string()),
            RespDataType::BulkString("hey".to_string()),
        ]);

        assert_eq!(resp_data_type.as_bytes(), expected_bytes)
    }

    #[test]
    fn test_enconde_empty_array() {
        let expected_bytes = bytes_from_str("*0\r\n");
        let resp_data_type = RespDataType::Array(vec![]);

        assert_eq!(resp_data_type.as_bytes(), expected_bytes)
    }

    #[test]
    fn test_encoded_null_bulk_str() {
        let expected_bytes = bytes_from_str("$-1\r\n");
        let resp_data_type = RespDataType::NullBulkString;

        assert_eq!(resp_data_type.as_bytes(), expected_bytes)
    }

    #[test]
    fn test_encoded_integer() {
        let expected_bytes = bytes_from_str(":-1\r\n");
        let resp_data_type = RespDataType::Integer(-1);

        assert_eq!(resp_data_type.as_bytes(), expected_bytes)
    }
}

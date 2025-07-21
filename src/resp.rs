use std::{
    io::{Error, ErrorKind},
    str::from_utf8,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

pub struct RespDecoder;

impl RespDecoder {}

#[derive(Debug, PartialEq, Eq)]
pub enum RespDataType {
    BulkString(String),
    SimpleError(String),
    Array(Vec<RespDataType>),
    SimpleString(String),
}

const SIMPLE_STRING_BYTE: u8 = b'+';
const ARRAY_BYTE: u8 = b'*';
const BULK_STRING_BYTE: u8 = b'$';
const ERROR_BYTE: u8 = b'-';

pub enum RespError {}

// TODO:
//
//     We suggest that you implement a proper Redis protocol parser in this stage. It'll come in handy in later stages.
//     Redis command names are case-insensitive, so ECHO, echo and EcHo are all valid commands.
//     The tester will send a random string as an argument to the ECHO command, so you won't be able to hardcode the response to pass this stage.
//     The exact bytes your program will receive won't be just ECHO hey, you'll receive something like this: *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n. That's ["ECHO", "hey"] encoded using the Redis protocol.
//     You can read more about how "commands" are handled in the Redis protocol here.
//
//
// notes:
// Clients send commands to a Redis server as an array of bulk strings.
// The first (and sometimes also the second) bulk string in the array is the command's name.
// Subsequent elements of the array are the arguments for the command.

const CRLF: &[u8] = b"\r\n";

impl Decoder for RespDecoder {
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
            SIMPLE_STRING_BYTE => {
                if let Some(simple_string) = parse_simple_string(src)? {
                    Ok(Some(simple_string))
                } else {
                    Ok(None)
                }
            }
            ARRAY_BYTE => {
                if let Some(array) = parse_array(src)? {
                    Ok(Some(array))
                } else {
                    Ok(None)
                }
            }
            BULK_STRING_BYTE => {
                if let Some(bulk_str) = parse_bulk_str(src)? {
                    Ok(Some(bulk_str))
                } else {
                    Ok(None)
                }
            }
            _ => Err(Error::new(ErrorKind::InvalidData, "Unknown RESP type byte")),
        }
    }
}

fn find_crlf(src: &BytesMut) -> Option<usize> {
    src.windows(2).position(|window| window == CRLF)
}

fn parse_simple_string(src: &mut BytesMut) -> Result<Option<RespDataType>, std::io::Error> {
    dbg!(&src);

    if let Some(crlf_pos) = find_crlf(src) {
        // A simple string like "+\r\n" should be an error because it has no content.
        // The CRLF starts immediately after the type byte (index 1).
        if crlf_pos == 1 {
            return Err(Error::new(ErrorKind::InvalidData, "Empty simple string"));
        }

        let content = String::from_utf8_lossy(&src[1..crlf_pos]).to_string();
        dbg!(&content);
        src.advance(crlf_pos + 2); // Skip the content and CRLF
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

        let content = String::from_utf8_lossy(&src[1..crlf_pos]).to_string();
        src.advance(crlf_pos + 2); // Skip the content and CRLF
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

fn parse_bulk_str(src: &mut BytesMut) -> Result<Option<RespDataType>, std::io::Error> {
    dbg!(&src);
    // read string length
    if let Some(crlf_pos) = find_crlf(src) {
        if crlf_pos == 1 {
            dbg!(&src, crlf_pos);
            return Err(Error::new(ErrorKind::InvalidData, "Empty simple string"));
        }

        dbg!(&src, crlf_pos);
        let length_str = from_utf8(&src[1..crlf_pos]).map_err(|_| {
            Error::new(
                ErrorKind::InvalidData,
                "Invalid UTF-8 in bulk string length",
            )
        })?;
        dbg!(&length_str);

        // TODO : Use a custom error

        // Parse the length
        let length: isize = length_str
            .parse()
            .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid bulk string length format"))?;

        if length == -1 {
            todo!("implement null bulk string data type");
        }

        let data_len = length as usize;
        if src.len() < (crlf_pos + CRLF.len()) + data_len + CRLF.len() {
            dbg!(src.len());
            return Ok(None);
        }
        src.advance(crlf_pos + 2);
        let content = String::from_utf8_lossy(&src[0..data_len]).to_string();
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
            dbg!(&src, crlf_pos);
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
        let mut i = num_elements;
        while i > 0 {
            dbg!(&src[0]);
            dbg!(&array);
            match src[0] {
                SIMPLE_STRING_BYTE => {
                    if let Some(simple_str) = parse_simple_string(src)? {
                        array.push(simple_str);
                        dbg!(&src);
                    } else {
                        todo!("handle this correctly")
                    }
                }
                ARRAY_BYTE => {
                    if let Some(simple_str) = parse_array(src)? {
                        array.push(simple_str);
                        dbg!(&src);
                    } else {
                        todo!("handle this correctly")
                    }
                }
                BULK_STRING_BYTE => {
                    if let Some(simple_str) = parse_bulk_str(src)? {
                        array.push(simple_str);
                        dbg!(&src);
                    } else {
                        todo!("handle this correctly")
                    }
                }
                _ => panic!(),
            }
            i -= 1
        }

        Ok(Some(RespDataType::Array(array)))
        // todo!()
    } else {
        Ok(None)
    }
}

impl Encoder<RespDataType> for RespDecoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: RespDataType, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        dst.put_slice(&item.as_bytes());
        Ok(())
    }
}

impl RespDataType {
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
        // assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut buf = bytes_from_str("$3\r\nhey\r\n");
        let result = parse_bulk_str(&mut buf).unwrap();
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
    fn test_encoded_bulk_str() {
        let expected_bytes = bytes_from_str("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n");
        let resp_data_type = RespDataType::Array(vec![
            RespDataType::BulkString("ECHO".to_string()),
            RespDataType::BulkString("hey".to_string()),
        ]);

        assert_eq!(resp_data_type.as_bytes(), expected_bytes)
    }
    #[test]
    fn test_encoded_array() {
        let expected_bytes = bytes_from_str("$4\r\nECHO\r\n");
        let resp_data_type = RespDataType::BulkString("ECHO".to_string());

        assert_eq!(resp_data_type.as_bytes(), expected_bytes)
    }
}

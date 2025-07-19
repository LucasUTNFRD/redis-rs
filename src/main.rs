#![allow(unused_imports)]
use std::{io::Write, net::TcpListener};

const PONG_RESPONSE: &[u8] = b"+PONG\r\n";

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                stream.write_all(PONG_RESPONSE).unwrap();
                // println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

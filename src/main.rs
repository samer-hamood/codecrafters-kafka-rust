#![allow(unused_imports)]
use std::{cmp::Ordering, net::TcpListener};
use std::io::Write;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                let (message_size_bytes, correlation_id_bytes) = response(0, 7);
                let _message_size_bytes_sent = _stream.write(&message_size_bytes).unwrap();
                // println!("Sent {:#?} byte(s) for message ID", message_id_bytes_sent);
                let _correlation_id_bytes_sent = _stream.write(&correlation_id_bytes);
                // println!("Sent {:#?} byte(s) for correlation ID", correlation_id_bytes_sent);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn response(message_size: i32, correlation_id: i32) -> ([u8; 4], [u8; 4]) {
    // Convert to bytes in big-endian order
    let message_size_bytes = message_size.to_be_bytes();
    let correlation_id_bytes = correlation_id.to_be_bytes();
    (message_size_bytes, correlation_id_bytes)
}

mod test {
    use crate::response;

    #[test]
    fn response_is_converted_to_bytes_in_big_endian() {
        let message_id = 0;
        let correlation_id = 7;
        let (message_id, correlation_id) = response(message_id, correlation_id);
        assert_eq!(message_id, [0x00, 0x00, 0x00, 0x00]); 
        assert_eq!(correlation_id, [0x00, 0x00, 0x00, 0x07]); 
    }
}

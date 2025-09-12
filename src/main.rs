#![allow(unused_imports)]
use std::i32;
use std::net::TcpStream;
use std::process::exit;
use std::{cmp::Ordering, net::TcpListener};
use std::io::{Read, Write};

use crate::api_version_request::ApiVersionsRequest;
use crate::api_version_v4_response::{ApiVersionsV4Response, UNSUPPORTED_VERSION};

mod api_version_request;
mod api_version_v4_response;

// Header Bytes
const MESSAGE_SIZE: usize = 4;
const REQUEST_API_KEY: usize = 2;
const REQUEST_API_VERSION: usize = 2;
const CORRELATION_ID: usize = 4;
const HEADER_SIZE: usize = MESSAGE_SIZE + REQUEST_API_KEY + REQUEST_API_VERSION + CORRELATION_ID;

const SUPPORTED_API_VERSIONS: [i16; 5] = [0, 1, 2, 3, 4];

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("\nAccepted new connection");

                let mut buf = [0u8; 1024];
                process_bytes_from_stream(&mut _stream, &mut buf);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn process_bytes_from_stream(_stream: &mut TcpStream, buf: &mut [u8]) -> usize {
    let mut total_bytes_read = 0;
    println!("Buffer length: {}", buf.len());
    loop {
        match _stream.read(&mut buf[total_bytes_read..]) {
            Ok(0) => {
                println!("Connection closed by peer");
                break
            },
            Ok(n) => {
                println!("Read {} byte(s)", n);
                total_bytes_read += n;
                if total_bytes_read >= HEADER_SIZE {
                    let api_versions_request = ApiVersionsRequest::parse(buf);
                    println!("ApiVersions request: {:#?}", api_versions_request);

                    let api_versions_response = 
                        ApiVersionsV4Response::new(
                            0, 
                            api_versions_request.correlation_id, 
                            check_supported_version(api_versions_request.request_api_version)
                        );

                    let response_bytes_sent = write_bytes_to_stream(_stream, &api_versions_response.to_bytes());
                    println!("Sent {:#?} byte(s) for response", response_bytes_sent);
                    
                    // let correlation_id = parse_correlation_id(&buf, 8, HEADER_SIZE);
                    // println!("Correlation ID: {correlation_id}");
                    // let (message_size_bytes, correlation_id_bytes) = convert_to_bytes(8, correlation_id);
                    // let message_size_bytes_sent = write_bytes_to_stream(_stream, &message_size_bytes);
                    // println!("Sent {:#?} byte(s) for message size", message_size_bytes_sent);
                    // let correlation_id_bytes_sent = write_bytes_to_stream(_stream, &correlation_id_bytes);
                    // println!("Sent {:#?} byte(s) for correlation ID", correlation_id_bytes_sent);
                }
            },
            Err(e) => {
                println!("Failed to read: {}", e);
                break
            },
        }
    }
    println!("Total bytes read: {}", total_bytes_read);
    total_bytes_read
}

fn check_supported_version(version: i16) -> i16 {
    let version_is_supported = SUPPORTED_API_VERSIONS.iter().any(|&supported_version| supported_version == version);
    if version_is_supported {
        0
    } else {
        UNSUPPORTED_VERSION
    }
}

#[allow(dead_code)]
fn parse_correlation_id(bytes: &[u8], offset: usize, size: usize) -> i32 {
    let correlation_id = i32::from_be_bytes(bytes[offset..size].try_into().unwrap());
    correlation_id
}

#[allow(dead_code)]
fn convert_to_bytes(message_size: i32, correlation_id: i32) -> ([u8; 4], [u8; 4]) {
    // Convert to bytes in big-endian order
    let message_size_bytes = message_size.to_be_bytes();
    let correlation_id_bytes = correlation_id.to_be_bytes();
    (message_size_bytes, correlation_id_bytes)
}

#[allow(dead_code)]
fn convert_to_bytes2(message_size: i32, correlation_id: i32) -> [u8; 8] {
    // Convert to bytes in big-endian order
    let message_size_bytes = message_size.to_be_bytes();
    let correlation_id_bytes = correlation_id.to_be_bytes();
    let mut bytes = [0u8; 8];
    let mut index = 0;
    for i in 0..message_size_bytes.len() {
        bytes[index] = message_size_bytes[i];
        index += 1;
    }
    for j in 0..correlation_id_bytes.len() {
        bytes[index] = correlation_id_bytes[j];
        index += 1;
    }
    bytes
}

fn write_bytes_to_stream(_stream: &mut TcpStream, bytes: &[u8]) -> usize {
    println!("Writing the following bytes to stream: {:X?}", bytes);
    match _stream.write(&bytes) {
        Ok(n) => {
            println!("Wrote {:#?} byte(s) successfully", n);
            n 
        },
        Err(e) => {
            println!("Write failed: {}", e);
            0
        }
    }
}

#[allow(dead_code)]
fn write_all_bytes_to_stream(_stream: &mut TcpStream, bytes: &[u8]) {
    match _stream.write_all(&bytes) {
        Ok(_) => println!("Wrote {:#?} byte(s) successfully", bytes.len()),
        Err(e) => {
            println!("Write failed: {}", e);
        }
    }
}

mod test {
    use super::*;

    #[test]
    fn converts_message_size_and_correlation_id_to_big_endian_bytes() {
        let message_size = 0;
        let correlation_id = 7;
        let (message_size, correlation_id) = convert_to_bytes(message_size, correlation_id);
        assert_eq!(message_size, [0x00, 0x00, 0x00, 0x00]); 
        assert_eq!(correlation_id, [0x00, 0x00, 0x00, 0x07]); 
    }

    #[test]
    fn converts_message_size_and_correlation_id_to_big_endian_bytes_2() {
        let message_size = 0;
        let correlation_id = 7;
        let bytes = convert_to_bytes2(message_size, correlation_id);
        assert_eq!(bytes, [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07]); 
    }

    #[test]
    fn parses_correlation_id_from_request_bytes() {
        // 00 00 00 23  // message_size:        35
        // 00 12        // request_api_key:     18
        // 00 04        // request_api_version: 4
        // 6f 7f c6 61  // correlation_id:      1870644833
        let bytes: &[u8] = &[
            0x00, 0x00, 0x00, 0x23,
            0x00, 0x12,
            0x00, 0x04,
            0x6f, 0x7f, 0xc6, 0x61,
        ];
        // assert_eq!(HEADER_SIZE, 12);
        let correlation_id = parse_correlation_id(bytes, 8, HEADER_SIZE);
        assert_eq!(correlation_id, 1870644833)
    }
}


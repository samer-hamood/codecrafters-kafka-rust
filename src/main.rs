#![allow(unused_imports)]
use std::i32;
use std::net::TcpStream;
use std::process::exit;
use std::{cmp::Ordering, net::TcpListener};
use std::io::{Read, Write};

use crate::serializable::Serializable;
use crate::fetch::fetch_v16_request::{FetchV16Request};
use crate::fetch::fetch_v16_response::{FetchV16Response};
use crate::tag_section::{TagSection};
use crate::api_keys::{FETCH, API_VERSIONS};
use crate::api_version_request::{ApiVersionsRequest};
use crate::api_version_v4_response::{ApiKey, ApiVersionsV4Response, SUPPORTED_VERSION, UNSUPPORTED_VERSION};

mod size;
mod serializable;
mod headers;
mod fetch;
mod compact_array;
mod tag_section;
mod api_keys;
mod api_version_request;
mod api_version_v4_response;

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
    let header_size = FetchV16Request::header_size();
    loop {
        match _stream.read(&mut buf[total_bytes_read..]) {
            Ok(0) => {
                println!("Connection closed by peer");
                break
            },
            Ok(n) => {
                println!("Read {} byte(s)", n);
                total_bytes_read += n;
                if total_bytes_read >= header_size {
                // if total_bytes_read >= ApiVersionsRequest::header_size() {
                    // let api_versions_request = ApiVersionsRequest::parse(buf);
                    // println!("{:#?}", api_versions_request);
                    //
                    // let api_versions_response = 
                    //     ApiVersionsV4Response::new(
                    //         api_versions_request.header.correlation_id, 
                    //         check_supported_version(api_versions_request.header.request_api_version),
                    //         vec![
                    //             ApiKey::new(API_VERSIONS, 0, 4, TagSection::empty()),
                    //             ApiKey::new(FETCH, 0, 16, TagSection::empty()),
                    //         ],
                    //         0,
                    //         TagSection::empty(),
                    //     );
                    //
                    // let response_bytes_sent = write_bytes_to_stream(_stream, &api_versions_response.to_be_bytes());

                    let request = FetchV16Request::parse(buf);
                    println!("{:#?}", request);
                    let response = 
                        FetchV16Response::new(
                            request.header.correlation_id, 
                            0,
                            // check_supported_version(request.header.request_api_version),
                            0,
                            0,
                            Vec::new(),
                            TagSection::empty(),
                        );

                    let response_bytes_sent = write_bytes_to_stream(_stream, &response.to_be_bytes());
                    
                    println!("Sent {:#?} byte(s) for response", response_bytes_sent);
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
    if SUPPORTED_API_VERSIONS.contains(&version) {
        SUPPORTED_VERSION
    } else {
        UNSUPPORTED_VERSION
    }
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


mod test {
    use super::*;
    use parameterized::parameterized;

    #[parameterized(
        version = {
            0, 1, 2, 3, 4
        }
    )]
    fn checks_supported_version(version: i16) {
        assert_eq!(SUPPORTED_VERSION, check_supported_version(version)); 
    }
    
    #[test]
    fn checks_unsupported_version() {
        assert_eq!(UNSUPPORTED_VERSION, check_supported_version(6)); 
    }
}


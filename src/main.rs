#![allow(unused_imports)]
use std::i32;
use std::net::TcpStream;
use std::process::exit;
use std::{cmp::Ordering, net::TcpListener};
use std::io::{Read, Write};

use crate::compact_array::CompactArray;
use crate::compact_records::CompactRecords;
use crate::fetch::partition::ResponsePartition;
use crate::fetch::topic::{self, ResponseTopic};
use crate::size::Size;
use crate::byte_parsable::ByteParsable;
use crate::headers::request_header_v2::{self, RequestHeaderV2};
use crate::serializable::Serializable;
use crate::tagged_fields_section::TaggedFieldsSection;
use crate::error_codes::{NONE, UNKNOWN_TOPIC_ID, UNSUPPORTED_VERSION};
use crate::fetch::fetch_request_v16::{FetchRequestV16};
use crate::fetch::fetch_response_v16::{FetchResponseV16};
use crate::api_keys::{FETCH, API_VERSIONS};
use crate::api_versions::api_versions_request_v4::{ApiVersionsRequestV4};
use crate::api_versions::api_versions_response_v4::{ApiKey, ApiVersionsResponseV4};

mod compact_records;
mod byte_parsable;
mod compact_string;
mod nullable_string;
mod size;
mod serializable;
mod headers;
mod compact_array;
mod tagged_fields_section;
mod error_codes;
mod fetch;
mod api_keys;
mod api_versions;

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
    let header_size = RequestHeaderV2::min_size();
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
                    let request_header = RequestHeaderV2::parse(buf, 0);

                    let response_bytes =
                        if request_header.request_api_key == API_VERSIONS {
                            println!("Handling {} request...", "ApiVersions");
                            ApiVersionsResponseV4::new(
                                request_header.correlation_id, 
                                check_supported_version(request_header.request_api_version),
                                vec![
                                ApiKey::new(API_VERSIONS, 0, 4, TaggedFieldsSection::empty()),
                                ApiKey::new(FETCH, 0, 16, TaggedFieldsSection::empty()),
                                ],
                                0,
                                TaggedFieldsSection::empty(),
                            ).to_be_bytes()
                        } else if request_header.request_api_key == FETCH {
                            println!("Handling {} request...", "Fetch");
                            let fetch_request = FetchRequestV16::parse(buf, request_header.size());
                            let mut topics = Vec::new();
                            for _ in 0..fetch_request.topics.len() {
                                topics.push(
                                    ResponseTopic::new(
                                        fetch_request.topics[0].topic_id, 
                                        vec![
                                        ResponsePartition::new(0, UNKNOWN_TOPIC_ID, 0, 0, 0, CompactArray::empty(), 0, CompactRecords::empty(), TaggedFieldsSection::empty()),
                                        ],
                                        TaggedFieldsSection::empty(),
                                    )
                                );
                            }
                            FetchResponseV16::new(
                                request_header.correlation_id, 
                                0,
                                NONE,
                                0,
                                CompactArray::new(topics),
                                TaggedFieldsSection::empty(),
                            ).to_be_bytes()
                        } else {
                            Vec::new()
                        };

                    let response_bytes_sent = write_bytes_to_stream(_stream, &response_bytes);

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
        NONE
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
        assert_eq!(NONE, check_supported_version(version)); 
    }
    
    #[test]
    fn checks_unsupported_version() {
        assert_eq!(UNSUPPORTED_VERSION, check_supported_version(6)); 
    }
}


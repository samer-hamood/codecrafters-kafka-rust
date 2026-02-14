#![allow(unused_imports)]
use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::net::TcpStream;
use std::thread;

use crate::api_keys::{API_VERSIONS, DESCRIBE_TOPIC_PARTITIONS, FETCH, PRODUCE};
use crate::api_response::ApiResponse;
use crate::api_versions::api_versions_api::ApiVersionsApi;
use crate::api_versions::api_versions_response_v4::{ApiKey, ApiVersionsResponseV4};
use crate::byte_parsable::ByteParsable;
use crate::describe_topic_partitions::describe_topic_partitions_api::DescribeTopicPartitionsApi;
use crate::describe_topic_partitions::describe_topic_partitions_request_v0::{
    self, topic_name, DescribeTopicPartitionsRequestV0,
};
use crate::describe_topic_partitions::describe_topic_partitions_response_v0::{
    DescribeTopicPartitionsResponseV0, Partition, Topic,
};
use crate::fetch::fetch_api::FetchApi;
use crate::fetch::fetch_request_v16::FetchRequestV16;
use crate::fetch::fetch_response_v16::FetchResponseV16;
use crate::fetch::partition::{ResponsePartition, Transaction};
use crate::fetch::topic::ResponseTopic;
use crate::headers::request_header_v2::RequestHeaderV2;
use crate::partial_parsable::PartialParsable;
use crate::records::metadata_record::{MetadataRecord, TOPIC};
use crate::records::record_batch::{RecordBatch, RecordValue, SearchItem};
use crate::records::topic_record::TopicRecord;
use crate::serializable::Serializable;
use crate::size::Size;
use crate::tagged_fields_section::TaggedFieldsSection;
use crate::types::compact_array::CompactArray;
use crate::types::compact_records::CompactRecords;
use crate::types::compact_string::CompactString;
use crate::utils::config::load_config;
use crate::utils::logging::init_logging;
use crate::utils::uuid::all_zeroes_uuid;
use tracing::{debug, info, trace};
use uuid::Uuid;

mod api_keys;
mod api_response;
mod api_versions;
mod byte_parsable;
mod describe_topic_partitions;
mod error_codes;
mod fetch;
mod headers;
mod macros;
mod partial_parsable;
mod produce;
mod records;
mod serializable;
mod size;
mod tagged_fields_section;
mod types;
mod utils;

fn main() {
    let config = load_config();
    init_logging(&config.log.level);
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        // Uses 1:1 model of thread implementation (1 thread: 1 OS thread), so probably won't scale
        thread::spawn(move || match stream {
            Ok(mut _stream) => {
                println!("\nAccepted new connection");

                let mut buf = [0u8; 1024];
                process_bytes_from_stream(&mut _stream, &mut buf);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        });
    }
}

fn process_bytes_from_stream(stream: &mut TcpStream, buf: &mut [u8]) -> usize {
    debug!("Buffer length: {}", buf.len());
    let mut total_bytes_read = 0;
    let header_size = RequestHeaderV2::min_size();
    loop {
        match stream.read(&mut buf[total_bytes_read..]) {
            Ok(0) => {
                println!("Connection closed by peer");
                break;
            }
            Ok(n) => {
                debug!("Read {n} byte(s)");
                let request_start_offset = total_bytes_read;
                total_bytes_read += n;
                if total_bytes_read >= header_size {
                    let request_header = RequestHeaderV2::parse(buf, request_start_offset);
                    let response_bytes = match request_header.request_api_key {
                        API_VERSIONS => ApiVersionsApi::respond(request_header).to_be_bytes(),
                        FETCH => FetchApi::respond(request_header, buf, request_start_offset)
                            .to_be_bytes(),
                        DESCRIBE_TOPIC_PARTITIONS => DescribeTopicPartitionsApi::respond(
                            request_header,
                            buf,
                            request_start_offset,
                        )
                        .to_be_bytes(),
                        _ => Vec::new(),
                    };

                    debug!("Response size: {} byte(s)", &response_bytes.len());

                    let response_bytes_sent = write_bytes_to_stream(stream, &response_bytes);

                    debug!("Sent {response_bytes_sent} byte(s) for response");
                }
            }
            Err(e) => {
                println!("Failed to read: {}", e);
                break;
            }
        }
    }
    debug!("Total bytes read: {}", total_bytes_read);
    total_bytes_read
}

pub fn get_record_batches_from_metadata_log() -> Vec<RecordBatch> {
    get_record_batches_from_log_file("__cluster_metadata-0")
}

pub fn get_record_batches_from_log_file(directory: &str) -> Vec<RecordBatch> {
    let log_file_path = format!("/tmp/kraft-combined-logs/{directory}/00000000000000000000.log");
    RecordBatch::from_file(&log_file_path)
}

fn write_bytes_to_stream(_stream: &mut TcpStream, bytes: &[u8]) -> usize {
    trace!("Writing the following bytes to stream: {:X?}", bytes);
    match _stream.write(bytes) {
        Ok(n) => {
            trace!("Wrote {n} byte(s) successfully");
            n
        }
        Err(e) => {
            println!("Write failed: {}", e);
            0
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    #[ignore = ""]
    fn parses_metadata_log_file() {
        // let metadata_file_path =
        //     "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
        //
        // let metadata_log_bytes: [u8; 259] = [
        //     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x4f, 0x00, 0x00,
        //     0x00, 0x01, 0x02, 0xb0, 0x69, 0x45, 0x7c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        //     0x00, 0x01, 0x91, 0xe0, 0x5a, 0xf8, 0x18, 0x00, 0x00, 0x01, 0x91, 0xe0, 0x5a, 0xf8,
        //     0x18, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        //     0xff, 0x00, 0x00, 0x00, 0x01, 0x3a, 0x00, 0x00, 0x00, 0x01, 0x2e, 0x01, 0x0c, 0x00,
        //     0x11, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x2e, 0x76, 0x65, 0x72, 0x73,
        //     0x69, 0x6f, 0x6e, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        //     0x02, 0x00, 0x00, 0x00, 0x9c, 0x00, 0x00, 0x00, 0x01, 0x02, 0x50, 0xe6, 0x84, 0xbd,
        //     0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01, 0x91, 0xe0, 0x5b, 0x2d, 0x15,
        //     0x00, 0x00, 0x01, 0x91, 0xe0, 0x5b, 0x2d, 0x15, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        //     0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x02, 0x40, 0x00,
        //     0x00, 0x00, 0x01, 0x34, 0x01, 0x02, 0x00, 0x06, 0x6d, 0x61, 0x6e, 0x67, 0x6f, 0x71,
        //     0xa5, 0x9a, 0x51, 0x89, 0x68, 0x4f, 0x8b, 0x93, 0x7e, 0xe0, 0xd0, 0x10, 0x0d, 0x85,
        //     0x6a, 0x00, 0x00, 0x90, 0x01, 0x00, 0x00, 0x02, 0x01, 0x82, 0x01, 0x01, 0x03, 0x01,
        //     0x00, 0x00, 0x00, 0x00, 0x71, 0xa5, 0x9a, 0x51, 0x89, 0x68, 0x4f, 0x8b, 0x93, 0x7e,
        //     0xe0, 0xd0, 0x10, 0x0d, 0x85, 0x6a, 0x02, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00,
        //     0x00, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        //     0x00, 0x00, 0x02, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00,
        //     0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
        // ];

        // let path = std::path::Path::new(metadata_file_path);
        //
        // if let Some(parent) = path.parent()  {
        //     let _ = fs::create_dir_all(parent);
        // }

        // println!("Writing file {metadata_file_path} ...");
        // let file = fs::File::create(metadata_file_path);

        // let _ = file.expect("REASON").write(&metadata_log_bytes);
        // let res = fs::write(metadata_file_path, metadata_log_bytes);

        // let err = file.unwrap_err();

        // println!("Error: {}", err);
        // assert!(res.is_ok());
        // assert!(res.is_err());
        // assert_eq!(get_file_size(metadata_file_path), metadata_log_bytes.len());
        // let record_batches = parse_metadata_log_file(metadata_file_path);
        // for record_batch in record_batches {
        //     for record in &record_batch.records {
        //         let offset: usize = 0;
        //         let metadata_record = MetadataRecord::parse(&record.value, offset);
        //         // metadata_record.
        //         let topic_record = TopicRecord::parse(&record.value, 0, metadata_record);
        //     }
        // }
        // assert_eq!(record_batches.len(), 1);
    }
}

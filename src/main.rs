#![allow(unused_imports)]
use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::exit;
use std::{cmp::Ordering, net::TcpListener};

use uuid::Uuid;

use crate::api_keys::{API_VERSIONS, FETCH};
use crate::api_versions::api_versions_request_v4::ApiVersionsRequestV4;
use crate::api_versions::api_versions_response_v4::{ApiKey, ApiVersionsResponseV4};
use crate::byte_parsable::ByteParsable;
use crate::compact_array::CompactArray;
use crate::compact_records::CompactRecords;
use crate::fetch::fetch_request_v16::FetchRequestV16;
use crate::fetch::fetch_response_v16::FetchResponseV16;
use crate::fetch::partition::{ResponsePartition, Transaction};
use crate::fetch::topic::{self, ResponseTopic};
use crate::headers::request_header_v2::{self, RequestHeaderV2};
use crate::partial_parsable::PartialParsable;
use crate::records::metadata_record::{self, MetadataRecord};
use crate::records::partition_record::PartitionRecord;
use crate::records::record_batch::{self, RecordBatch};
use crate::records::topic_record::TopicRecord;
use crate::serializable::Serializable;
use crate::size::Size;
use crate::tagged_fields_section::TaggedFieldsSection;

mod api_keys;
mod api_versions;
mod byte_parsable;
mod compact_array;
mod compact_records;
mod compact_string;
mod error_codes;
mod fetch;
mod headers;
mod nullable_string;
mod partial_parsable;
mod records;
mod serializable;
mod size;
mod tagged_fields_section;
mod types;

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
                break;
            }
            Ok(n) => {
                println!("Read {} byte(s)", n);
                total_bytes_read += n;
                if total_bytes_read >= header_size {
                    let request_header = RequestHeaderV2::parse(buf, 0);

                    let response_bytes = if request_header.request_api_key == API_VERSIONS {
                        println!("Handling ApiVersions request...");
                        let throttle_time_ms = 0;
                        ApiVersionsResponseV4::new(
                            request_header.correlation_id,
                            check_supported_version(request_header.request_api_version),
                            vec![
                                ApiKey::new(
                                    API_VERSIONS,
                                    api_versions::MIN_VERSION,
                                    api_versions::MAX_VERSION,
                                    TaggedFieldsSection::empty(),
                                ),
                                ApiKey::new(
                                    FETCH,
                                    fetch::MIN_VERSION,
                                    fetch::MAX_VERSION,
                                    TaggedFieldsSection::empty(),
                                ),
                            ]
                            .into(),
                            throttle_time_ms,
                            TaggedFieldsSection::empty(),
                        )
                        .to_be_bytes()
                    } else if request_header.request_api_key == FETCH {
                        println!("Handling Fetch request...");
                        let fetch_request = FetchRequestV16::parse(buf, request_header.size());
                        let mut topics = Vec::new();
                        for _ in 0..fetch_request.topics.len() {
                            let partition_index = 0;
                            let high_watermark = 0;
                            let last_stable_offset = 0;
                            let log_start_offset = 0;
                            let aborted_transactions: CompactArray<Transaction> =
                                CompactArray::empty();
                            let preferred_read_replica = 0;
                            let records = CompactRecords::empty();
                            let topic_id: Uuid = fetch_request.topics[0].topic_id;
                            topics.push(ResponseTopic::new(
                                topic_id,
                                vec![ResponsePartition {
                                    partition_index,
                                    error_code: check_topic_exists(&topic_id),
                                    high_watermark,
                                    last_stable_offset,
                                    log_start_offset,
                                    aborted_transactions,
                                    preferred_read_replica,
                                    records,
                                    _tagged_fields: TaggedFieldsSection::empty(),
                                }]
                                .into(),
                                TaggedFieldsSection::empty(),
                            ));
                        }
                        let throttle_time_ms = 0;
                        let session_id = 0;
                        let responses = CompactArray::new(topics);
                        FetchResponseV16::new(
                            request_header.correlation_id,
                            throttle_time_ms,
                            error_codes::NONE,
                            session_id,
                            responses,
                            TaggedFieldsSection::empty(),
                        )
                        .to_be_bytes()
                    } else {
                        Vec::new()
                    };

                    let response_bytes_sent = write_bytes_to_stream(_stream, &response_bytes);

                    println!("Sent {:#?} byte(s) for response", response_bytes_sent);
                }
            }
            Err(e) => {
                println!("Failed to read: {}", e);
                break;
            }
        }
    }
    println!("Total bytes read: {}", total_bytes_read);
    total_bytes_read
}

fn check_topic_exists(topic_id: &Uuid) -> i16 {
    let topic_exists = metadata_file_contains(topic_id);
    if topic_exists {
        error_codes::NONE
    } else {
        error_codes::UNKNOWN_TOPIC_ID
    }
}

fn metadata_file_contains(topic_id: &Uuid) -> bool {
    let metadata_file_path =
        "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
    let mut metadata_file = File::open(metadata_file_path)
        .unwrap_or_else(|_| panic!("Metadata log file not found: {metadata_file_path}"));

    // Parse file
    let file_byte_count: usize = get_file_size(metadata_file_path);
    let mut buf = vec![0; file_byte_count];
    let _ = metadata_file.read(&mut buf);
    let mut offset = 0;
    while offset < file_byte_count {
        let record_batch = RecordBatch::parse(&buf, offset);
        offset += record_batch.size();
        if record_batch_contains_topic(&record_batch, topic_id) {
            return true;
        }
    }

    false
}

#[allow(dead_code)]
fn parse_metadata_log_file(path: &str) -> Vec<RecordBatch> {
    let mut metadata_file =
        File::open(path).unwrap_or_else(|_| panic!("Metadata log file not found: {path}"));
    let file_byte_count: usize = get_file_size(path);
    let mut buf = vec![0; file_byte_count];
    let _ = metadata_file.read(&mut buf);

    let mut record_batches = Vec::new();

    let mut offset = 0;
    while offset < file_byte_count {
        let record_batch = RecordBatch::parse(&buf, offset);
        offset += record_batch.size();
        // println!("{:?}\n", &record_batch);
        record_batches.push(record_batch);
    }
    // println!("Offset after parsing: {offset}");
    record_batches
}

fn get_file_size(path: &str) -> usize {
    fs::metadata(path)
        .expect("Unable to read metadata for file")
        .len() as usize
}

fn record_batch_contains_topic(record_batch: &RecordBatch, topic_id: &Uuid) -> bool {
    for record in &record_batch.records {
        let mut offset: usize = 0;
        let metadata_record = MetadataRecord::parse(&record.value, offset);
        offset += metadata_record.size();
        if metadata_record._type == 2 {
            let topic_record = TopicRecord::parse(&record.value, offset, metadata_record);
            if &topic_record.topic_uuid == topic_id {
                return true;
            }
        } else if metadata_record._type == 3 {
            let partition_record = PartitionRecord::parse(&record.value, offset, metadata_record);
            if &partition_record.topic_uuid == topic_id {
                return true;
            }
        }
    }
    false
}

fn check_supported_version(version: i16) -> i16 {
    if SUPPORTED_API_VERSIONS.contains(&version) {
        error_codes::NONE
    } else {
        error_codes::UNSUPPORTED_VERSION
    }
}

fn write_bytes_to_stream(_stream: &mut TcpStream, bytes: &[u8]) -> usize {
    println!("Writing the following bytes to stream: {:X?}", bytes);
    match _stream.write(bytes) {
        Ok(n) => {
            println!("Wrote {:#?} byte(s) successfully", n);
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
    use parameterized::parameterized;

    #[parameterized(
        version = {
            0, 1, 2, 3, 4
        }
    )]
    fn checks_supported_version(version: i16) {
        assert_eq!(error_codes::NONE, check_supported_version(version));
    }

    #[test]
    fn checks_unsupported_version() {
        assert_eq!(error_codes::UNSUPPORTED_VERSION, check_supported_version(6));
    }

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

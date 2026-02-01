#![allow(unused_imports)]
use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::net::TcpStream;
use std::thread;

use crate::api_keys::{API_VERSIONS, DESCRIBE_TOPIC_PARTITIONS, FETCH};
use crate::api_versions::api_versions_response_v4::{ApiKey, ApiVersionsResponseV4};
use crate::byte_parsable::ByteParsable;
use crate::describe_topic_partitions::describe_topic_partitions_request_v0::DescribeTopicPartitionsRequestV0;
use crate::describe_topic_partitions::describe_topic_partitions_response_v0::{
    DescribeTopicPartitionsResponseV0, Partition, Topic,
};
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
mod api_versions;
mod byte_parsable;
mod describe_topic_partitions;
mod error_codes;
mod fetch;
mod headers;
mod macros;
mod partial_parsable;
mod records;
mod serializable;
mod size;
mod tagged_fields_section;
mod types;
mod utils;

const SUPPORTED_API_VERSIONS: [i16; 5] = [0, 1, 2, 3, 4];

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

fn process_bytes_from_stream(_stream: &mut TcpStream, buf: &mut [u8]) -> usize {
    debug!("Buffer length: {}", buf.len());
    let mut total_bytes_read = 0;
    let header_size = RequestHeaderV2::min_size();
    loop {
        match _stream.read(&mut buf[total_bytes_read..]) {
            Ok(0) => {
                println!("Connection closed by peer");
                break;
            }
            Ok(n) => {
                debug!("Read {n} byte(s)");
                total_bytes_read += n;
                if total_bytes_read >= header_size {
                    let request_header = RequestHeaderV2::parse(buf, 0);
                    let response_bytes = match request_header.request_api_key {
                        API_VERSIONS => respond_to_api_versions_request(request_header),
                        FETCH => respond_to_fetch_request(request_header, buf),
                        DESCRIBE_TOPIC_PARTITIONS => {
                            respond_to_describe_topic_partitions_request(request_header, buf)
                        }
                        _ => Vec::new(),
                    };

                    debug!("Response size: {} byte(s)", &response_bytes.len());

                    let response_bytes_sent = write_bytes_to_stream(_stream, &response_bytes);

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

fn respond_to_describe_topic_partitions_request(
    request_header: RequestHeaderV2,
    buf: &[u8],
) -> Vec<u8> {
    debug!("Handling DescribeTopicPartitions request...");
    let describe_topic_partitions_request =
        DescribeTopicPartitionsRequestV0::parse(buf, request_header.size());
    let throttle_time_ms = 0;
    let is_internal = false;
    let topic_authorized_operation = 0;
    let record_batches = get_record_batches_from_metadata_log();
    let topics = describe_topic_partitions_request
        .topics
        .iter()
        .map(|request_topic| {
            let record_values = get_record_values(&record_batches, &request_topic.name);
            let (topic_id, error_code) = get_topic_id_and_error_code(&record_values);
            let partitions = record_values
                .iter()
                .filter_map(|record_value| record_value.to_partition_record())
                .map(Partition::from_partition_record)
                .collect::<Vec<Partition>>()
                .into();
            Topic::new(
                error_code,
                request_topic.name.clone(),
                topic_id,
                is_internal,
                partitions,
                topic_authorized_operation,
                TaggedFieldsSection::empty(),
            )
        })
        .collect::<Vec<Topic>>()
        .into();
    let next_cursor: i8 = -1;
    let response = DescribeTopicPartitionsResponseV0::new(
        request_header.correlation_id,
        throttle_time_ms,
        topics,
        next_cursor,
        TaggedFieldsSection::empty(),
    );
    response.to_be_bytes()
}

fn respond_to_api_versions_request(request_header: RequestHeaderV2) -> Vec<u8> {
    debug!("Handling ApiVersions request...");
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
            ApiKey::new(
                DESCRIBE_TOPIC_PARTITIONS,
                describe_topic_partitions::MIN_VERSION,
                describe_topic_partitions::MAX_VERSION,
                TaggedFieldsSection::empty(),
            ),
        ]
        .into(),
        throttle_time_ms,
        TaggedFieldsSection::empty(),
    )
    .to_be_bytes()
}

fn respond_to_fetch_request(request_header: RequestHeaderV2, buf: &[u8]) -> Vec<u8> {
    debug!("Handling Fetch request...");
    let throttle_time_ms = 0;
    let session_id = 0;
    let mut topics = Vec::new();
    let fetch_request = FetchRequestV16::parse(buf, request_header.size());
    for _ in 0..fetch_request.topics.len() {
        let partition_index = 0;
        let high_watermark = 0;
        let last_stable_offset = 0;
        let log_start_offset = 0;
        let aborted_transactions: CompactArray<Transaction> = CompactArray::empty();
        let preferred_read_replica = 0;
        let topic_id: Uuid = fetch_request.topics[0].topic_id;
        let metadata_record_batches = get_record_batches_from_metadata_log();
        let records =
            if let Some(topic_record) = get_topic_record(&topic_id, &metadata_record_batches) {
                let topic_name = topic_record.topic_name.to_string();
                let data_record_batches =
                    get_record_batches_from_data_log(&topic_name, partition_index);
                CompactRecords::from_record_batches(&data_record_batches)
            } else {
                CompactRecords::null()
            };
        topics.push(ResponseTopic::new(
            topic_id,
            vec![ResponsePartition {
                partition_index,
                error_code: check_topic_exists(&topic_id, metadata_record_batches),
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
}

fn get_record_values(
    record_batches: &[RecordBatch],
    topic_name: &CompactString,
) -> Vec<RecordValue> {
    record_batches
        .iter()
        .flat_map(|record_batch| {
            record_batch.parse_record_values(SearchItem::TopicName(topic_name.clone()), false)
        })
        .collect()
}

fn get_topic_id_and_error_code(record_values: &[RecordValue]) -> (Uuid, i16) {
    if record_values.is_empty() {
        (all_zeroes_uuid(), error_codes::UNKNOWN_TOPIC_OR_PARTITION)
    } else if let RecordValue::Topic(record) = &record_values[0] {
        (record.topic_uuid, error_codes::NONE)
    // Should always be TopicRecord but could get topic_uuid from PartitionRecord
    } else if let RecordValue::Partition(record) = &record_values[0] {
        (record.topic_uuid, error_codes::NONE)
    } else {
        (all_zeroes_uuid(), error_codes::UNKNOWN_TOPIC_OR_PARTITION)
    }
}

fn get_topic_record(topic_id: &Uuid, record_batches: &[RecordBatch]) -> Option<TopicRecord> {
    record_batches
        .iter()
        .flat_map(|record_batch| {
            record_batch.parse_record_values(SearchItem::TopicId(*topic_id), true)
        })
        .filter_map(|record_value| record_value.to_topic_record())
        .next()
}

fn check_topic_exists(topic_id: &Uuid, record_batches: Vec<RecordBatch>) -> i16 {
    let topic_exists = metadata_file_contains(topic_id, record_batches);
    if topic_exists {
        error_codes::NONE
    } else {
        error_codes::UNKNOWN_TOPIC_ID
    }
}

fn metadata_file_contains(topic_id: &Uuid, record_batches: Vec<RecordBatch>) -> bool {
    record_batches
        .iter()
        .any(|record_batch| record_batch_contains_topic(record_batch, topic_id))
}

fn get_record_batches_from_metadata_log() -> Vec<RecordBatch> {
    get_record_batches_from_log_file("__cluster_metadata-0")
}

fn get_record_batches_from_data_log(topic_name: &str, partition_index: i32) -> Vec<RecordBatch> {
    get_record_batches_from_log_file(format!("{topic_name}-{partition_index}").as_str())
}

fn get_record_batches_from_log_file(directory: &str) -> Vec<RecordBatch> {
    let log_file_path = format!("/tmp/kraft-combined-logs/{directory}/00000000000000000000.log");
    let mut metadata_file = File::open(&log_file_path)
        .unwrap_or_else(|_| panic!("Log file not found: {log_file_path}"));

    let mut record_batches = Vec::new();

    // Parse file
    let file_byte_count: usize = get_file_size(&log_file_path);
    let mut buf = vec![0; file_byte_count];
    let _ = metadata_file.read(&mut buf);
    let mut offset = 0;
    while offset < file_byte_count {
        let record_batch = RecordBatch::parse(&buf, offset);
        offset += record_batch.size();
        record_batches.push(record_batch);
    }

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

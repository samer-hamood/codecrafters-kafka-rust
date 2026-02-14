use uuid::Uuid;

use crate::{
    api_response::{self, ApiResponse},
    byte_parsable::ByteParsable,
    error_codes,
    fetch::{
        fetch_request_v16::FetchRequestV16,
        fetch_response_v16::FetchResponseV16,
        partition::{ResponsePartition, Transaction},
        topic::{RequestTopic, ResponseTopic},
    },
    get_record_batches_from_log_file, get_record_batches_from_metadata_log,
    headers::{request_header_v2::RequestHeaderV2, response_header_v1::ResponseHeaderV1},
    records::{
        record_batch::{RecordBatch, SearchItem},
        topic_record::TopicRecord,
    },
    serializable::Serializable,
    size::Size,
    tagged_fields_section::TaggedFieldsSection,
    types::{compact_array::CompactArray, compact_records::CompactRecords},
};

pub struct FetchApi;

impl FetchApi {
    pub fn respond(
        request_header: RequestHeaderV2,
        buf: &[u8],
        offset: usize,
    ) -> ApiResponse<ResponseHeaderV1, FetchResponseV16> {
        let fetch_request = FetchRequestV16::parse(buf, offset + request_header.size());
        let throttle_time_ms = 0;
        let session_id = 0;
        let responses = Self::responses(fetch_request.topics);
        let response = FetchResponseV16::new(
            throttle_time_ms,
            error_codes::NONE,
            session_id,
            responses,
            TaggedFieldsSection::empty(),
        );
        api_response::v1(request_header.correlation_id, response)
    }

    fn responses(topics: CompactArray<RequestTopic>) -> CompactArray<ResponseTopic> {
        topics
            .into_iter()
            .map(Self::response_topic)
            .collect::<Vec<ResponseTopic>>()
            .into()
    }

    fn response_topic(topic: RequestTopic) -> ResponseTopic {
        let partition_index = 0;
        let topic_id = topic.topic_id;
        let metadata_record_batches = get_record_batches_from_metadata_log();
        let error_code = Self::check_topic_exists(topic_id, &metadata_record_batches);
        let high_watermark = 0;
        let last_stable_offset = 0;
        let log_start_offset = 0;
        let aborted_transactions = CompactArray::empty();
        let preferred_read_replica = 0;
        let records =
            Self::get_records_from_data_log(topic_id, &metadata_record_batches, partition_index);
        ResponseTopic::new(
            topic_id,
            [ResponsePartition {
                partition_index,
                error_code,
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
        )
    }

    fn check_topic_exists(topic_id: Uuid, record_batches: &[RecordBatch]) -> i16 {
        let topic_exists = Self::metadata_file_contains(topic_id, record_batches);
        if topic_exists {
            error_codes::NONE
        } else {
            error_codes::UNKNOWN_TOPIC_ID
        }
    }

    fn metadata_file_contains(topic_id: Uuid, record_batches: &[RecordBatch]) -> bool {
        record_batches.iter().any(|record_batch| {
            !record_batch
                .parse_record_values(SearchItem::TopicId(topic_id), true)
                .is_empty()
        })
    }

    fn get_records_from_data_log(
        topic_id: Uuid,
        metadata_record_batches: &[RecordBatch],
        partition_index: i32,
    ) -> CompactRecords {
        if let Some(topic_record) = Self::get_topic_record(topic_id, metadata_record_batches) {
            let topic_name = topic_record.topic_name.to_string();
            let data_record_batches =
                Self::get_record_batches_from_data_log(&topic_name, partition_index);
            CompactRecords::from_record_batches(&data_record_batches)
        } else {
            CompactRecords::null()
        }
    }

    fn get_topic_record(topic_id: Uuid, record_batches: &[RecordBatch]) -> Option<TopicRecord> {
        record_batches
            .iter()
            .flat_map(|record_batch| {
                record_batch.parse_record_values(SearchItem::TopicId(topic_id), true)
            })
            .filter_map(|record_value| record_value.to_topic_record())
            .next()
    }

    fn get_record_batches_from_data_log(
        topic_name: &str,
        partition_index: i32,
    ) -> Vec<RecordBatch> {
        get_record_batches_from_log_file(format!("{topic_name}-{partition_index}").as_str())
    }
}

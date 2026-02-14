use uuid::Uuid;

use crate::{
    api_response::{self, ApiResponse},
    byte_parsable::ByteParsable,
    describe_topic_partitions::{
        describe_topic_partitions_request_v0::{
            DescribeTopicPartitionsRequestV0, Topic as RequestTopic,
        },
        describe_topic_partitions_response_v0::{
            DescribeTopicPartitionsResponseV0, Partition, Topic as ResponseTopic,
        },
    },
    error_codes, get_record_batches_from_metadata_log,
    headers::{request_header_v2::RequestHeaderV2, response_header_v1::ResponseHeaderV1},
    records::record_batch::{RecordBatch, RecordValue, SearchItem},
    size::Size,
    tagged_fields_section::TaggedFieldsSection,
    types::{compact_array::CompactArray, compact_string::CompactString},
    utils::uuid::all_zeroes_uuid,
};

pub struct DescribeTopicPartitionsApi;

impl DescribeTopicPartitionsApi {
    pub fn respond(
        request_header: RequestHeaderV2,
        buf: &[u8],
        offset: usize,
    ) -> ApiResponse<ResponseHeaderV1, DescribeTopicPartitionsResponseV0> {
        let describe_topic_partitions_request =
            DescribeTopicPartitionsRequestV0::parse(buf, offset + request_header.size());
        let throttle_time_ms = 0;
        let topics = Self::topics(describe_topic_partitions_request.topics);
        let next_cursor: i8 = -1;
        let response = DescribeTopicPartitionsResponseV0::new(
            throttle_time_ms,
            topics,
            next_cursor,
            TaggedFieldsSection::empty(),
        );
        api_response::v1(request_header.correlation_id, response)
    }

    fn topics(request_topics: CompactArray<RequestTopic>) -> CompactArray<ResponseTopic> {
        request_topics
            .into_iter()
            .map(Self::topic)
            .collect::<Vec<ResponseTopic>>()
            .into()
    }

    fn topic(topic: RequestTopic) -> ResponseTopic {
        let record_values = Self::get_record_values(&topic.name);
        let (error_code, topic_id) = Self::get_error_code_and_topic_id(&record_values);
        let is_internal = false;
        let partitions = Self::partitions(record_values);
        let topic_authorized_operation = 0;
        ResponseTopic::new(
            error_code,
            topic.name.into_compact_nullable_string(),
            topic_id,
            is_internal,
            partitions,
            topic_authorized_operation,
            TaggedFieldsSection::empty(),
        )
    }

    fn partitions(record_values: Vec<RecordValue>) -> CompactArray<Partition> {
        record_values
            .into_iter()
            .filter_map(|record_value| record_value.into_partition_record())
            .map(Partition::from_partition_record)
            .collect::<Vec<Partition>>()
            .into()
    }

    fn get_record_values(topic_name: &CompactString) -> Vec<RecordValue> {
        let record_batches = get_record_batches_from_metadata_log();
        record_batches
            .iter()
            .flat_map(|record_batch| {
                record_batch.parse_record_values(SearchItem::TopicName(topic_name.clone()), false)
            })
            .collect()
    }

    fn get_error_code_and_topic_id(record_values: &[RecordValue]) -> (i16, Uuid) {
        if record_values.is_empty() {
            (error_codes::UNKNOWN_TOPIC_OR_PARTITION, all_zeroes_uuid())
        } else if let RecordValue::Topic(record) = &record_values[0] {
            (error_codes::NONE, record.topic_uuid)
        // Should always be TopicRecord but could get topic_uuid from PartitionRecord
        } else if let RecordValue::Partition(record) = &record_values[0] {
            (error_codes::NONE, record.topic_uuid)
        } else {
            (error_codes::UNKNOWN_TOPIC_OR_PARTITION, all_zeroes_uuid())
        }
    }
}

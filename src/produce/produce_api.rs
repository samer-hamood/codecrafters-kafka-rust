use crate::{
    api_response::{self, ApiResponse},
    byte_parsable::ByteParsable,
    error_codes, get_record_values_by_topic_name_from_metadata_log,
    headers::{request_header_v2::RequestHeaderV2, response_header_v1::ResponseHeaderV1},
    produce::{
        produce_request_v11::{Partition, ProduceRequestV11, Topic},
        produce_response_v11::{PartitionResponse, ProduceResponseV11, Response},
    },
    records::{partition_record, record_batch::RecordValue},
    size::Size,
    tagged_fields_section::TaggedFieldsSection,
    types::{compact_array::CompactArray, compact_nullable_string::CompactNullableString},
};

pub struct ProduceApi;

impl ProduceApi {
    pub fn respond(
        request_header: RequestHeaderV2,
        buf: &[u8],
        offset: usize,
    ) -> ApiResponse<ResponseHeaderV1, ProduceResponseV11> {
        let produce_request = ProduceRequestV11::parse(buf, offset + request_header.size());
        let responses = Self::responses(produce_request.topic_data);
        let throttle_time_ms = 0;
        let response =
            ProduceResponseV11::new(responses, throttle_time_ms, TaggedFieldsSection::empty());
        api_response::v1(request_header.correlation_id, response)
    }

    fn responses(topic_data: CompactArray<Topic>) -> CompactArray<Response> {
        topic_data
            .into_iter()
            .map(Self::response)
            .collect::<Vec<Response>>()
            .into()
    }

    fn response(topic: Topic) -> Response {
        let record_values = get_record_values_by_topic_name_from_metadata_log(&topic.name);
        Response::new(
            topic.name,
            Self::partition_responses(topic.partition_data, record_values),
            TaggedFieldsSection::empty(),
        )
    }

    fn partition_responses(
        partition_data: CompactArray<Partition>,
        record_values: Vec<RecordValue>,
    ) -> CompactArray<PartitionResponse> {
        partition_data
            .into_iter()
            .map(|partition| Self::partition_response(partition, &record_values))
            .collect::<Vec<PartitionResponse>>()
            .into()
    }

    fn partition_response(
        partition: Partition,
        record_values: &[RecordValue],
    ) -> PartitionResponse {
        let (error_code, base_offset, log_start_offset) =
            if Self::partition_exists(record_values, partition.index) {
                (error_codes::NONE, 0, 0)
            } else {
                (error_codes::UNKNOWN_TOPIC_OR_PARTITION, -1i64, -1i64)
            };
        let log_append_time_ms = -1i64;
        let record_errors = CompactArray::empty();
        let error_message = CompactNullableString::null();
        PartitionResponse::new(
            partition.index,
            error_code,
            base_offset,
            log_append_time_ms,
            log_start_offset,
            record_errors,
            error_message,
            TaggedFieldsSection::empty(),
        )
    }

    fn partition_exists(record_values: &[RecordValue], partition_id: i32) -> bool {
        let mut record_values = record_values.iter();

        // Assumes the partition records come after the topic record
        let Some(topic_record) = record_values
            .by_ref()
            .find_map(|record_value| record_value.as_topic_record())
        else {
            return false;
        };

        for record_value in record_values {
            if let RecordValue::Partition(partition_record) = record_value {
                partition_record
                    .directories_array
                    .iter()
                    .for_each(|d| println!("Directory id: {d}"));
                if partition_record.partition_id == partition_id
                    && partition_record.topic_uuid == topic_record.topic_uuid
                {
                    return true;
                }
            }
        }

        false
    }
}

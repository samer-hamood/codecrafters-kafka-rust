use crate::{
    api_response::{self, ApiResponse},
    byte_parsable::ByteParsable,
    error_codes::UNKNOWN_TOPIC_OR_PARTITION,
    headers::{request_header_v2::RequestHeaderV2, response_header_v1::ResponseHeaderV1},
    produce::{
        produce_request_v11::{Partition, ProduceRequestV11, Topic},
        produce_response_v11::{PartitionResponse, ProduceResponseV11, Response},
    },
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
        Response::new(
            topic.name,
            Self::partition_responses(topic.partition_data),
            TaggedFieldsSection::empty(),
        )
    }

    fn partition_responses(
        partition_data: CompactArray<Partition>,
    ) -> CompactArray<PartitionResponse> {
        partition_data
            .into_iter()
            .map(Self::partition_response)
            .collect::<Vec<PartitionResponse>>()
            .into()
    }

    fn partition_response(partition: Partition) -> PartitionResponse {
        let base_offset = -1i64;
        let log_append_time_ms = -1i64;
        let log_start_offset = -1i64;
        let record_errors = CompactArray::empty();
        let error_message = CompactNullableString::null();
        PartitionResponse::new(
            partition.index,
            UNKNOWN_TOPIC_OR_PARTITION,
            base_offset,
            log_append_time_ms,
            log_start_offset,
            record_errors,
            error_message,
            TaggedFieldsSection::empty(),
        )
    }
}

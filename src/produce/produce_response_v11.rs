use crate::{
    byte_parsable::ByteParsable,
    headers::response_header_v1::ResponseHeaderV1,
    serializable::Serializable,
    size::Size,
    tagged_fields_section::TaggedFieldsSection,
    types::{
        compact_array::CompactArray, compact_nullable_string::CompactNullableString,
        compact_string::CompactString,
    },
};

// https://kafka.apache.org/41/design/protocol/#The_Messages_Produce

/// Produce Response (Version: 11) => [responses] throttle_time_ms _tagged_fields
///   responses => name [partition_responses] _tagged_fields
///     name => COMPACT_STRING
///     partition_responses => index error_code base_offset log_append_time_ms log_start_offset [record_errors] error_message _tagged_fields
///       index => INT32
///       error_code => INT16
///       base_offset => INT64
///       log_append_time_ms => INT64
///       log_start_offset => INT64
///       record_errors => batch_index batch_index_error_message _tagged_fields
///         batch_index => INT32
///         batch_index_error_message => COMPACT_NULLABLE_STRING
///       error_message => COMPACT_NULLABLE_STRING
///   throttle_time_ms => INT32
#[derive(Debug, Clone)]
pub struct ProduceResponseV11 {
    responses: CompactArray<Response>,
    throttle_time_ms: i32,
    _tagged_fields: TaggedFieldsSection,
}

impl ProduceResponseV11 {
    pub fn new(
        responses: CompactArray<Response>,
        throttle_time_ms: i32,
        _tagged_fields: TaggedFieldsSection,
    ) -> Self {
        Self {
            responses,
            throttle_time_ms,
            _tagged_fields,
        }
    }
}

impl Size for ProduceResponseV11 {
    fn size(&self) -> usize {
        self.responses.size() + self.throttle_time_ms.size() + self._tagged_fields.size()
    }
}

impl Serializable for ProduceResponseV11 {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.responses.to_be_bytes());
        bytes.extend_from_slice(&self.throttle_time_ms.to_be_bytes());
        bytes.extend_from_slice(&self._tagged_fields.to_be_bytes());
        bytes
    }
}

#[derive(Debug, Clone)]
pub struct Response {
    name: CompactString,
    partition_responses: CompactArray<PartitionResponse>,
    _tagged_fields: TaggedFieldsSection,
}

impl Response {
    pub fn new(
        name: CompactString,
        partition_responses: CompactArray<PartitionResponse>,
        _tagged_fields: TaggedFieldsSection,
    ) -> Self {
        Self {
            name,
            partition_responses,
            _tagged_fields,
        }
    }
}

impl Size for Response {
    fn size(&self) -> usize {
        self.name.size() + self.partition_responses.size() + self._tagged_fields.size()
    }
}

impl ByteParsable<Response> for Response {
    fn parse(_bytes: &[u8], _offset: usize) -> Self {
        todo!()
    }
}

impl Serializable for Response {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.name.to_be_bytes());
        bytes.extend_from_slice(&self.partition_responses.to_be_bytes());
        bytes.extend_from_slice(&self._tagged_fields.to_be_bytes());
        bytes
    }
}

#[derive(Debug, Clone)]
pub struct PartitionResponse {
    index: i32,
    error_code: i16,
    base_offset: i64,
    log_append_time_ms: i64,
    log_start_offset: i64,
    record_errors: CompactArray<RecordError>,
    error_message: CompactNullableString,
    _tagged_fields: TaggedFieldsSection,
}

impl PartitionResponse {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        index: i32,
        error_code: i16,
        base_offset: i64,
        log_append_time_ms: i64,
        log_start_offset: i64,
        record_errors: CompactArray<RecordError>,
        error_message: CompactNullableString,
        _tagged_fields: TaggedFieldsSection,
    ) -> Self {
        Self {
            index,
            error_code,
            base_offset,
            log_append_time_ms,
            log_start_offset,
            record_errors,
            error_message,
            _tagged_fields,
        }
    }
}

impl Size for PartitionResponse {
    fn size(&self) -> usize {
        self.index.size()
            + self.error_code.size()
            + self.base_offset.size()
            + self.log_append_time_ms.size()
            + self.log_start_offset.size()
            + self.record_errors.size()
            + self.error_message.size()
            + self._tagged_fields.size()
    }
}

impl ByteParsable<PartitionResponse> for PartitionResponse {
    fn parse(_bytes: &[u8], _offset: usize) -> PartitionResponse {
        todo!()
    }
}

impl Serializable for PartitionResponse {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.index.to_be_bytes());
        bytes.extend_from_slice(&self.error_code.to_be_bytes());
        bytes.extend_from_slice(&self.base_offset.to_be_bytes());
        bytes.extend_from_slice(&self.log_append_time_ms.to_be_bytes());
        bytes.extend_from_slice(&self.log_start_offset.to_be_bytes());
        bytes.extend_from_slice(&self.record_errors.to_be_bytes());
        bytes.extend_from_slice(&self.error_message.to_be_bytes());
        bytes.extend_from_slice(&self._tagged_fields.to_be_bytes());
        bytes
    }
}

#[derive(Debug, Clone)]
pub struct RecordError {
    batch_index: i32,
    batch_index_error_message: CompactNullableString,
    _tagged_fields: TaggedFieldsSection,
}

impl Size for RecordError {
    fn size(&self) -> usize {
        self.batch_index.size() + self.batch_index_error_message.size() + self._tagged_fields.size()
    }
}

impl ByteParsable<RecordError> for RecordError {
    fn parse(_bytes: &[u8], _offset: usize) -> Self {
        todo!()
    }
}

impl Serializable for RecordError {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.batch_index.to_be_bytes());
        bytes.extend_from_slice(&self.batch_index_error_message.to_be_bytes());
        bytes.extend_from_slice(&self._tagged_fields.to_be_bytes());
        bytes
    }
}

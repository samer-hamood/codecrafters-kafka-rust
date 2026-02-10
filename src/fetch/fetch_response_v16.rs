use crate::fetch::topic::ResponseTopic;
use crate::headers::response_header_v1::ResponseHeaderV1;
use crate::serializable::{BoxedSerializable, Serializable};
use crate::size::Size;
use crate::tagged_fields_section::TaggedFieldsSection;
use crate::types::compact_array::CompactArray;

/// Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] _tagged_fields
///   throttle_time_ms => INT32
///   error_code => INT16
///   session_id => INT32
///   responses => topic_id [partitions] _tagged_fields
///     topic_id => UUID
///     partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records _tagged_fields
///       partition_index => INT32
///       error_code => INT16
///       high_watermark => INT64
///       last_stable_offset => INT64
///       log_start_offset => INT64
///       aborted_transactions => producer_id first_offset _tagged_fields
///         producer_id => INT64 first_offset => INT64
///       preferred_read_replica => INT32
///       records => COMPACT_RECORDS
#[derive(Debug, Clone)]
pub struct FetchResponseV16 {
    throttle_time_ms: i32,
    error_code: i16,
    session_id: i32,
    responses: CompactArray<ResponseTopic>,
    _tagged_fields: TaggedFieldsSection,
}

impl FetchResponseV16 {
    pub fn new(
        throttle_time_ms: i32,
        error_code: i16,
        session_id: i32,
        responses: CompactArray<ResponseTopic>,
        _tagged_fields: TaggedFieldsSection,
    ) -> FetchResponseV16 {
        FetchResponseV16 {
            throttle_time_ms,
            error_code,
            session_id,
            responses,
            _tagged_fields,
        }
    }
}

impl Size for FetchResponseV16 {
    fn size(&self) -> usize {
        self.throttle_time_ms.size()
            + self.error_code.size()
            + self.session_id.size()
            + self.responses.size()
            + self._tagged_fields.size()
    }
}

impl Serializable for FetchResponseV16 {
    fn serializable_fields(&self) -> Vec<BoxedSerializable> {
        let mut fields: Vec<BoxedSerializable> = Vec::with_capacity(7);
        let message_size = self.size() as i32;
        fields.push(Box::new(message_size));
        fields.push(Box::new(self.header.clone()));
        fields.push(Box::new(self.throttle_time_ms));
        fields.push(Box::new(self.error_code));
        fields.push(Box::new(self.session_id));
        fields.push(Box::new(self.responses.clone()));
        fields.push(Box::new(self._tagged_fields.clone()));
        fields
    }
}

#[cfg(test)]
mod test {
    use crate::api_response::{self, ApiResponse};

    use super::*;

    #[test]
    fn computes_message_size() {
        let expected_size = (4 + 1) + 4 + 2 + 4 + (1 + 0) + 1;

        let correlation_id = 1519289319; // 4 + 1 (tag buffer) bytes
        let response = FetchResponseV16::new(
            0,                            // 4 bytes
            0,                            // 2 bytes
            0,                            // 4 bytes
            CompactArray::empty(),        // 1 byte
            TaggedFieldsSection::empty(), // 1 byte
        );
        let api_response = api_response::v1(correlation_id, response);

        assert_eq!(expected_size, api_response.message_size);
    }

    #[test]
    fn converts_to_bytes() {
        // 00 00 00 11  // message_size:                17
        // 00 00 00 07  // correlation_id:              7
        // 00 00 00 00  // throttle_time_ms:            0
        // 00 00        // error_code:                  0
        // 04           // responses (array length):    4
        // 00           // tag buffer                   0
        let expected_bytes: &[u8] = &[
            // message_size
            0x00, 0x00, 0x00, 0x11, // header: correlation_id + tag buffer (4 + 1 bytes)
            0x00, 0x00, 0x00, 0x00, 0x00, // throttle_time_ms (4 bytes)
            0x00, 0x00, 0x00, 0x00, // error_code (2 bytes)
            0x00, 0x00, // session_id (4 bytes)
            0x00, 0x00, 0x00, 0x00, // responses: array length (1 byte)
            0x01, // tag buffer (1 byte)
            0x00,
        ];

        let correlation_id = 0; // 4 + 1 (tag buffer) bytes
        let response = FetchResponseV16::new(
            0,                            // 4 bytes
            0,                            // 2 bytes
            0,                            // 4 bytes
            CompactArray::empty(),        // 1 byte
            TaggedFieldsSection::empty(), // 1 byte
        );

        let api_response = api_response::v1(correlation_id, response);

        assert_eq!(expected_bytes, api_response.to_be_bytes());
    }
}

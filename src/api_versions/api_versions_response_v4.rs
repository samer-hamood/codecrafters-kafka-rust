use std::array;

use crate::byte_parsable::ByteParsable;
use crate::error_codes::NONE;
use crate::serializable::{BoxedSerializable, Serializable};
use crate::size::Size;
use crate::tagged_fields_section::TaggedFieldsSection;
use crate::types::compact_array::CompactArray;

#[derive(Debug)]
pub struct ApiVersionsResponseV4 {
    pub error_code: i16,
    pub api_keys: CompactArray<ApiKey>,
    pub throttle_time_ms: i32,
    pub _tagged_fields: TaggedFieldsSection,
}

impl ApiVersionsResponseV4 {
    pub fn new(
        error_code: i16,
        api_keys: CompactArray<ApiKey>,
        throttle_time_ms: i32,
        _tagged_fields: TaggedFieldsSection,
    ) -> Self {
        Self {
            error_code,
            api_keys,
            throttle_time_ms,
            _tagged_fields,
        }
    }
}

impl Size for ApiVersionsResponseV4 {
    fn size(&self) -> usize {
        self.error_code.size()
            + self.api_keys.size()
            + self.throttle_time_ms.size()
            + self._tagged_fields.size()
    }
}

impl Serializable for ApiVersionsResponseV4 {
    fn serializable_fields(&self) -> Vec<BoxedSerializable> {
        let mut fields: Vec<BoxedSerializable> = Vec::with_capacity(6);
        let message_size = self.size() as i32;
        fields.push(Box::new(message_size));
        fields.push(Box::new(self.correlation_id));
        fields.push(Box::new(self.error_code));
        fields.push(Box::new(self.api_keys.clone()));
        fields.push(Box::new(self.throttle_time_ms));
        fields.push(Box::new(self._tagged_fields.clone()));
        fields
    }
}

#[derive(Debug, Clone)]
pub struct ApiKey {
    api_key: i16,
    min_version: i16,
    max_version: i16,
    _tagged_fields: TaggedFieldsSection,
}

impl ApiKey {
    pub fn new(
        api_key: i16,
        min_version: i16,
        max_version: i16,
        _tagged_fields: TaggedFieldsSection,
    ) -> ApiKey {
        ApiKey {
            api_key,
            min_version,
            max_version,
            _tagged_fields,
        }
    }
}

impl Size for ApiKey {
    fn size(&self) -> usize {
        self.api_key.size()
            + self.min_version.size()
            + self.max_version.size()
            + self._tagged_fields.size()
    }
}

impl ByteParsable<ApiKey> for ApiKey {
    fn parse(_bytes: &[u8], _offset: usize) -> ApiKey {
        todo!()
    }
}

impl Serializable for ApiKey {
    fn serializable_fields(&self) -> Vec<BoxedSerializable> {
        vec![
            Box::new(self.api_key),
            Box::new(self.min_version),
            Box::new(self.max_version),
            Box::new(self._tagged_fields.clone()),
        ]
    }
}

#[cfg(test)]
mod test {
    use crate::{
        api_response::{self, v0, ApiResponse},
        error_codes,
        headers::response_header_v0::ResponseHeaderV0,
    };

    use super::*;

    fn api_versions_response() -> ApiResponse<ResponseHeaderV0, ApiVersionsResponseV4> {
        let correlation_id = 7; // 4 bytes
        let api_keys: CompactArray<ApiKey> = vec![
            ApiKey::new(1, 0, 17, TaggedFieldsSection::empty()), // 7 bytes
            ApiKey::new(18, 0, 4, TaggedFieldsSection::empty()), // 7 bytes
            ApiKey::new(75, 0, 0, TaggedFieldsSection::empty()), // 7 bytes
        ]
        .into();
        let throttle_time_ms = 0;
        let response = ApiVersionsResponseV4::new(
            error_codes::NONE,            // 2 bytes
            api_keys,                     // 1 + 21 bytes
            throttle_time_ms,             // 4 bytes
            TaggedFieldsSection::empty(), // 1 bytes
        );
        api_response::v0(correlation_id, response)
    }

    #[test]
    fn calculates_message_size() {
        let expected_size = 33;

        let response = api_versions_response();

        assert_eq!(expected_size, response.message_size);
    }

    #[test]
    fn converts_to_bytes() {
        // 00 00 00 21  // message_size:     33
        // 00 00 00 07  // correlation_id:   7
        // 00 00        // error_code:       0
        // 04           // array length:     4
        // 00 01        // api_key:          1
        // 00 00        // min_version:      0
        // 00 11        // max_version:      17
        // 00           // tag buffer        0
        // 00 12        // api_key:          18
        // 00 00        // min_version:      0
        // 00 04        // max_version:      4
        // 00           // tag buffer        0
        // 00 4b        // api_key:          75
        // 00 00        // min_version:      0
        // 00 00        // max_version:      0
        // 00           // tag buffer        0
        // 00 00 00 00  // throttle_time_ms: 0
        // 00           // tag buffer        0
        let expected_bytes: &[u8] = &[
            // message_size
            0x00, 0x00, 0x00, 0x21, // correlation_id
            0x00, 0x00, 0x00, 0x07, // error_code
            0x00, 0x00, // Api Versions/Keys array
            0x04, // array length
            0x00, 0x01, 0x00, 0x00, 0x00, 0x11,
            0x00, // api_key (2 bytes) + min_version (2 bytes)+ max_version (2 bytes) + tag
            // buffer (1 byte)
            0x00, 0x12, 0x00, 0x00, 0x00, 0x04,
            0x00, // api_key (2 bytes) + min_version (2 bytes)+ max_version (2 bytes) + tag
            // buffer (1 byte)
            0x00, 0x4b, 0x00, 0x00, 0x00, 0x00,
            0x00, // api_key (2 bytes) + min_version (2 bytes)+ max_version (2 bytes) + tag
            // buffer (1 byte)
            // throttle_time_ms (4 bytes) + tag buffer (1 byte)
            0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        let response = api_versions_response();

        assert_eq!(expected_bytes, response.to_be_bytes());
    }
}

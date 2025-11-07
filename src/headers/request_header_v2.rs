use std::{i32, usize};

use crate::byte_parsable::ByteParsable;
use crate::nullable_string::{self, NullableString};
use crate::size::Size;
use crate::tagged_fields_section::{self, TaggedFieldsSection, EMPTY};

// Header Bytes
const MESSAGE_SIZE: usize = 4;
const REQUEST_API_KEY: usize = 2;
const REQUEST_API_VERSION: usize = 2;
const CORRELATION_ID: usize = 4;

/// Request Header v2 => request_api_key request_api_version correlation_id client_id _tagged_fields
///   request_api_key => INT16
///   request_api_version => INT16
///   correlation_id => INT32
///   client_id => NULLABLE_STRING
#[allow(dead_code)]
#[derive(Debug)]
pub struct RequestHeaderV2 {
    pub message_size: i32,
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    pub client_id: NullableString,
    pub _tagged_fields: TaggedFieldsSection,
}

impl RequestHeaderV2 {
    pub fn min_size() -> usize {
        size_of::<i32>()
            + size_of::<i16>()
            + size_of::<i16>()
            + size_of::<i32>()
            + nullable_string::LENGTH
            + tagged_fields_section::EMPTY
    }
}

impl Size for RequestHeaderV2 {
    fn size(&self) -> usize {
        self.message_size.size()
            + self.request_api_key.size()
            + self.request_api_version.size()
            + self.correlation_id.size()
            + self.client_id.size()
            + self._tagged_fields.size()
    }
}

impl ByteParsable<RequestHeaderV2> for RequestHeaderV2 {
    fn parse(bytes: &[u8], offset: usize) -> RequestHeaderV2 {
        let mut offset = offset;
        // let message_size = i32::from_be_bytes(bytes[offset..MESSAGE_SIZE].try_into().unwrap());
        // offset += MESSAGE_SIZE;
        // let request_api_key = i16::from_be_bytes(bytes[offset..offset + REQUEST_API_KEY].try_into().unwrap());
        // offset += REQUEST_API_KEY;
        // let request_api_version = i16::from_be_bytes(bytes[offset..offset + REQUEST_API_VERSION].try_into().unwrap());
        // offset += REQUEST_API_VERSION;
        // let correlation_id = i32::from_be_bytes(bytes[offset..offset + CORRELATION_ID].try_into().unwrap());
        // offset += CORRELATION_ID;
        let message_size = i32::parse(bytes, offset);
        offset += message_size.size();
        let request_api_key = i16::parse(bytes, offset);
        offset += request_api_key.size();
        let request_api_version = i16::parse(bytes, offset);
        offset += request_api_key.size();
        let correlation_id = i32::parse(bytes, offset);
        offset += correlation_id.size();
        let client_id = NullableString::parse(bytes, offset);
        offset += client_id.size();
        let _tagged_fields = TaggedFieldsSection::parse(bytes, offset);

        RequestHeaderV2 {
            message_size,
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
            _tagged_fields,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn calculates_size_from_field_types() {
        let header_size = MESSAGE_SIZE
            + REQUEST_API_KEY
            + REQUEST_API_VERSION
            + CORRELATION_ID
            + nullable_string::LENGTH
            + tagged_fields_section::EMPTY;
        assert_eq!(header_size, RequestHeaderV2::min_size());
    }

    #[test]
    fn parses_request_header_from_request_bytes() {
        let _bytes: &[u8] = &[
            0x00, 0x00, 0x00, 0x60, // message_size
            0x00, 0x01, // request_api_key
            0x00, 0x10, // request_api_version: 16
            0x1b, 0x84, 0x59, 0x19, // correlation_id
            0x00, 0x09, // client_id (length): 9
            0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69, // client_id (content)
            0x00, // _tagged_fields
        ];

        // 00 00 00 23                  // message_size:        35
        // 00 12                        // request_api_key:     18
        // 67 4a                        // request_api_version: 26442
        // 4f 74 d2 8b                  // correlation_id:      1333056139
        // 00 09                        // client_id (length):  9
        // 6b 61 66 6b 61 2d 63 6c 69   // client_id (content):
        // 00                           // _tagged_fields:      0
        let bytes: &[u8] = &[
            0x00, 0x00, 0x00, 0x23, 0x00, 0x12, 0x67, 0x4a, 0x4f, 0x74, 0xd2, 0x8b,
            // 0xff, 0xff, // -1
            0x00, 0x09, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69, 0x00,
        ];
        let request_header = RequestHeaderV2::parse(bytes, 0);
        assert_eq!(request_header.message_size, 35);
        assert_eq!(request_header.request_api_key, 18);
        assert_eq!(request_header.request_api_version, 26442);
        assert_eq!(request_header.correlation_id, 1333056139);
        // assert_eq!(request_header.client_id, NullableString::null());
        assert_eq!(request_header.client_id, NullableString::from("kafka-cli"));
        assert_eq!(request_header._tagged_fields, TaggedFieldsSection::empty());
    }
}

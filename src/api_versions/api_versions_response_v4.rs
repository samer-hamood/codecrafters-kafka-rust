use std::{array, i32};

use crate::byte_parsable::ByteParsable;
use crate::size::Size;
use crate::serializable::{BoxedSerializable, Serializable};
use crate::tagged_fields_section::TaggedFieldsSection;
use crate::compact_array::CompactArray;                        
use crate::error_codes::NONE;

#[allow(dead_code)]
#[derive(Debug)]
pub struct ApiVersionsResponseV4 {
    pub correlation_id: i32,
    pub error_code: i16,
    pub api_keys: CompactArray<ApiKey>,
    pub throttle_time_ms: i32,
    pub _tagged_fields: TaggedFieldsSection,
}

impl ApiVersionsResponseV4 {

    pub fn new(correlation_id: i32, error_code: i16, api_keys: Vec<ApiKey>, throttle_time_ms: i32, _tagged_fields: TaggedFieldsSection) -> ApiVersionsResponseV4 {
        ApiVersionsResponseV4 {
            correlation_id: correlation_id,
            error_code: error_code,                              
            api_keys: CompactArray::new(api_keys),
            throttle_time_ms: throttle_time_ms,
            _tagged_fields: _tagged_fields,
        }
    }

}

impl Size for ApiVersionsResponseV4 {

    fn size(&self) -> usize {
        size_of::<i32>() + size_of::<i16>() + size_of::<i32>() + self.api_keys.size() + self._tagged_fields.size()
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

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ApiKey {
    api_key: i16,
    min_version: i16,
    max_version: i16,
    _tagged_fields: TaggedFieldsSection,
}

#[allow(dead_code)]
impl ApiKey {

    pub fn new(api_key: i16, min_version: i16, max_version: i16, _tagged_fields: TaggedFieldsSection) -> ApiKey {
        ApiKey {
            api_key: api_key,
            min_version: min_version,
            max_version: max_version,
            _tagged_fields: _tagged_fields,
        }
    }

}

impl Size for ApiKey {

    fn size(&self) -> usize {
        3 * size_of::<i16>() + self._tagged_fields.size()
    }

}


impl ByteParsable<ApiKey> for ApiKey {

   fn parse(_bytes: &[u8], _offset: usize) -> ApiKey {
      todo!() 
   } 

}

impl Serializable for ApiKey {

    fn serializable_fields(&self) -> Vec<BoxedSerializable> {
        let mut fields: Vec<BoxedSerializable> = Vec::with_capacity(4);
        fields.push(Box::new(self.api_key));
        fields.push(Box::new(self.min_version));
        fields.push(Box::new(self.max_version));
        fields.push(Box::new(self._tagged_fields.clone()));
        fields
    }

}

mod test {
    use super::*;

    #[test]
    fn calculates_message_size() {
        let expected_size = 33;

        let api_version_response = 
            ApiVersionsResponseV4::new(
                7,                                              // 4 bytes 
                NONE,                                           // 2 bytes
                vec![
                    ApiKey::new(1, 0, 17, TaggedFieldsSection::empty()), // 7 bytes
                    ApiKey::new(18, 0, 4, TaggedFieldsSection::empty()), // 7 bytes
                    ApiKey::new(75, 0, 0, TaggedFieldsSection::empty()), // 7 bytes
                ], 
                0,                                              // 4 bytes 
                TaggedFieldsSection::empty(),                            // 1 bytes
            );

        assert_eq!(expected_size, api_version_response.size());
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
            0x00, 0x00, 0x00, 0x21,
            // correlation_id
            0x00, 0x00, 0x00, 0x07,
            // error_code
            0x00, 0x00,
            // Api Versions/Keys array
            0x04, // array length 
            0x00, 0x01, 0x00, 0x00, 0x00, 0x11, 0x00, // api_key (2 bytes) + min_version (2 bytes)+ max_version (2 bytes) + tag
                                                      // buffer (1 byte)
            0x00, 0x12, 0x00, 0x00, 0x00, 0x04, 0x00, // api_key (2 bytes) + min_version (2 bytes)+ max_version (2 bytes) + tag
                                                      // buffer (1 byte)
            0x00, 0x4b, 0x00, 0x00, 0x00, 0x00, 0x00, // api_key (2 bytes) + min_version (2 bytes)+ max_version (2 bytes) + tag
                                                      // buffer (1 byte)
            // throttle_time_ms (4 bytes) + tag buffer (1 byte)
            0x00, 0x00, 0x00, 0x00, 0x00
        ];

        let api_version_response = 
            ApiVersionsResponseV4::new(
                7,                                                  // 4 bytes 
                NONE,                                               // 2 bytes
                vec![
                    ApiKey::new(1, 0, 17, TaggedFieldsSection::empty()),     // 7 bytes
                    ApiKey::new(18, 0, 4, TaggedFieldsSection::empty()),     // 7 bytes
                    ApiKey::new(75, 0, 0, TaggedFieldsSection::empty()),     // 7 bytes
                ], 
                0,                                                  // 4 bytes 
                TaggedFieldsSection::empty(),                                // 1 bytes
            );


        assert_eq!(expected_bytes, api_version_response.to_be_bytes());
    }
}


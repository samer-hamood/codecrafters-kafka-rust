use std::array;

// Error Codes 
pub const SUPPORTED_VERSION: i16 = 0;
pub const UNSUPPORTED_VERSION: i16 = 35;

#[derive(Debug)]
pub struct ApiVersionsV4Response {
    pub correlation_id: i32,
    pub error_code: i16,
    pub api_keys: Vec<ApiKey>,
    pub throttle_time_ms: i32,
    pub _tagged_fields: TagSection,
}

impl ApiVersionsV4Response {

    pub fn new(correlation_id: i32, error_code: i16, api_keys: Vec<ApiKey>, throttle_time_ms: i32, _tagged_fields: TagSection) -> ApiVersionsV4Response {
        ApiVersionsV4Response {
            correlation_id: correlation_id,
            error_code: error_code,
            api_keys: api_keys,
            throttle_time_ms: throttle_time_ms,
            _tagged_fields: _tagged_fields,
        }
    }

    fn message_size(&self) -> i32 {
        let array_length_size = 1;  
        (size_of::<i32>() + size_of::<i16>() + array_length_size + self.api_keys.len() * ApiKey::len() + size_of::<i32>() + TagSection::len()).try_into().unwrap()
    }

    pub fn to_be_bytes(&self) -> Vec<u8> {
        // Convert to bytes in big-endian order
        let message_size = self.message_size();
        let message_size_bytes = message_size.to_be_bytes();
        let correlation_id_bytes = self.correlation_id.to_be_bytes();
        let error_code_bytes = self.error_code.to_be_bytes();
        let api_key_bytes = self.api_keys_bytes();
        let throttle_time_ms_bytes = self.throttle_time_ms.to_be_bytes();
        let tagged_fields_bytes = self._tagged_fields.to_be_bytes();
        let mut bytes = Vec::new();
        for i in 0..message_size_bytes.len() {
            bytes.push(message_size_bytes[i]);
        }
        for i in 0..correlation_id_bytes.len() {
            bytes.push(correlation_id_bytes[i]);
        }
        for i in 0..error_code_bytes.len() {
            bytes.push(error_code_bytes[i]);
        }
        for i in 0..api_key_bytes.len() {
            bytes.push(api_key_bytes[i]);
        }
        for i in 0..throttle_time_ms_bytes.len() {
            bytes.push(throttle_time_ms_bytes[i]);
        }
        for i in 0..tagged_fields_bytes.len() {
            bytes.push(tagged_fields_bytes[i]);
        }
        bytes
    }

    fn api_keys_bytes(&self) -> Vec<u8> {
        let array_length_bytes = self.array_length().to_be_bytes();
        let mut bytes: Vec<u8> = Vec::new();
        for i in 0..array_length_bytes.len() {
            bytes.push(array_length_bytes[i]);
        }
        for i in 0..self.api_keys.len() {
            let api_key_bytes = self.api_keys[i].to_be_bytes();
            for j in 0..api_key_bytes.len() {
                bytes.push(api_key_bytes[j]);
            }
        }
        bytes
    }
    
    fn array_length(&self) -> u8 {
        (1 + self.api_keys.len()).try_into().unwrap()
    }
}

#[derive(Debug)]
pub struct ApiKey {
    api_key: i16,
    min_version: i16,
    max_version: i16,
    _tagged_fields: TagSection,
}

impl ApiKey {

    pub fn new(api_key: i16, min_version: i16, max_version: i16, _tagged_fields: TagSection) -> ApiKey {
        ApiKey {
            api_key: api_key,
            min_version: min_version,
            max_version: max_version,
            _tagged_fields: _tagged_fields,
        }
    }

    pub fn len() -> usize {
        3 * size_of::<i16>() + TagSection::len()
    }

    pub fn to_be_bytes(&self) -> [u8; 7] {
        // Convert to bytes in big-endian order
        let api_key_bytes = self.api_key.to_be_bytes();
        let min_version_bytes = self.min_version.to_be_bytes();
        let max_version_bytes = self.max_version.to_be_bytes();
        let tagged_field_bytes = self._tagged_fields.to_be_bytes();
        let mut bytes = [0u8; 7];
        let mut index = 0;
        for i in 0..api_key_bytes.len() {
            bytes[index] = api_key_bytes[i];
            index += 1;
        }
        for j in 0..min_version_bytes.len() {
            bytes[index] = min_version_bytes[j];
            index += 1;
        }
        for k in 0..max_version_bytes.len() {
            bytes[index] = max_version_bytes[k];
            index += 1;
        }
        for l in 0..tagged_field_bytes.len() {
            bytes[index] = tagged_field_bytes[l];
            index += 1;
        }
        bytes
    }
}

#[derive(Debug)]
pub struct TagSection {
    number_of_tagged_fields: u8,
}

impl TagSection {

    pub fn empty() -> TagSection {
        TagSection {
            number_of_tagged_fields: 0,
        }
    }

    pub fn len() -> usize {
        1
    }

    pub fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        if self.number_of_tagged_fields == 0 {
            bytes.push(0);
        }
        bytes
    }

}

mod test {
    use super::*;

    #[test]
    fn calculates_message_size() {
        let expected_size = 33;

        let api_version_response = 
            ApiVersionsV4Response::new(
                7,                                              // 4 bytes 
                SUPPORTED_VERSION,                              // 2 bytes
                vec![
                    ApiKey::new(1, 0, 17, TagSection::empty()), // 7 bytes
                    ApiKey::new(18, 0, 4, TagSection::empty()), // 7 bytes
                    ApiKey::new(75, 0, 0, TagSection::empty()), // 7 bytes
                ], 
                0,                                              // 4 bytes 
                TagSection::empty(),                            // 1 bytes
            );

        assert_eq!(expected_size, api_version_response.message_size());
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
            ApiVersionsV4Response::new(
                7,                                                  // 4 bytes 
                SUPPORTED_VERSION,                                  // 2 bytes
                vec![
                    ApiKey::new(1, 0, 17, TagSection::empty()),     // 7 bytes
                    ApiKey::new(18, 0, 4, TagSection::empty()),     // 7 bytes
                    ApiKey::new(75, 0, 0, TagSection::empty()),     // 7 bytes
                ], 
                0,                                                  // 4 bytes 
                TagSection::empty(),                                // 1 bytes
            );


        assert_eq!(expected_bytes, api_version_response.to_be_bytes());
    }
}


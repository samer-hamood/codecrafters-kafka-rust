
// Header Bytes
const MESSAGE_SIZE: usize = 4;
const CORRELATION_ID: usize = 4;
// Body Bytes
const ERROR_CODE: usize = 2;

const SIZE: usize = MESSAGE_SIZE + CORRELATION_ID + ERROR_CODE;

// Error Codes
pub const UNSUPPORTED_VERSION: i16 = 35;

#[derive(Debug)]
pub struct ApiVersionsV4Response {
    pub message_size: i32,
    pub correlation_id: i32,
    pub error_code: i16,
}

impl ApiVersionsV4Response {

    pub fn new(message_size: i32, correlation_id: i32, error_code: i16) -> ApiVersionsV4Response {
        ApiVersionsV4Response {
            message_size: message_size,
            correlation_id: correlation_id,
            error_code: error_code,
        }
    }

    #[allow(dead_code)]
    pub fn header_size() -> usize {
        size_of::<i32>() + size_of::<i32>() + size_of::<i16>()
    }

    pub fn to_bytes(&self) -> [u8; SIZE] {
        // Convert to bytes in big-endian order
        let message_size_bytes = self.message_size.to_be_bytes();
        let correlation_id_bytes = self.correlation_id.to_be_bytes();
        let error_code_bytes = self.error_code.to_be_bytes();
        let mut bytes = [0u8; SIZE];
        let mut index = 0;
        for i in 0..message_size_bytes.len() {
            bytes[index] = message_size_bytes[i];
            index += 1;
        }
        for j in 0..correlation_id_bytes.len() {
            bytes[index] = correlation_id_bytes[j];
            index += 1;
        }
        for k in 0..error_code_bytes.len() {
            bytes[index] = error_code_bytes[k];
            index += 1;
        }
        bytes
    }
}

mod test {
    use super::*;

    #[test]
    fn calculates_header_size_from_field_types() {
        assert_eq!(SIZE, ApiVersionsV4Response::header_size());
    }

    #[test]
    fn converts_to_bytes() {
        // 00 00 00 00  // message_size:   0 (any value works)
        // 4f 74 d2 8b  // correlation_id: 1333056139
        // 00 23        // error_code:     35
        let expected_bytes: &[u8] = &[
            0x00, 0x00, 0x00, 0x00,
            0x4f, 0x74, 0xd2, 0x8b,
            0x00, 0x23,
        ];

        let api_version_response = ApiVersionsV4Response::new(0, 1333056139, UNSUPPORTED_VERSION);

        let actual_bytes = api_version_response.to_bytes();

        assert_eq!(expected_bytes, actual_bytes);
    }
}


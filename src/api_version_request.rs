
// Header Bytes
const MESSAGE_SIZE: usize = 4;
const REQUEST_API_KEY: usize = 2;
const REQUEST_API_VERSION: usize = 2;
const CORRELATION_ID: usize = 4;

#[allow(dead_code)]
#[derive(Debug)]
pub struct ApiVersionsRequest {
    pub message_size: i32,
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
}

impl ApiVersionsRequest {

    pub fn header_size() -> usize {
        size_of::<i32>() + size_of::<i16>() + size_of::<i16>() + size_of::<i32>()
    }

    pub fn parse(bytes: &[u8]) -> ApiVersionsRequest {
        let mut offset = 0;
        let message_size = i32::from_be_bytes(bytes[offset..MESSAGE_SIZE].try_into().unwrap());
        offset += MESSAGE_SIZE;
        let request_api_key = i16::from_be_bytes(bytes[offset..offset + REQUEST_API_KEY].try_into().unwrap());
        offset += REQUEST_API_KEY;
        let request_api_version = i16::from_be_bytes(bytes[offset..offset + REQUEST_API_VERSION].try_into().unwrap());
        offset += REQUEST_API_VERSION;
        let correlation_id = i32::from_be_bytes(bytes[offset..offset + CORRELATION_ID].try_into().unwrap());
        ApiVersionsRequest {
            message_size,
            request_api_key,
            request_api_version,
            correlation_id,
        }
    }
}

mod test {
    use super::*;

    #[allow(dead_code)]
    const HEADER_SIZE: usize = MESSAGE_SIZE + REQUEST_API_KEY + REQUEST_API_VERSION + CORRELATION_ID;

    #[test]
    fn calculates_header_size_from_field_types() {
        assert_eq!(HEADER_SIZE, ApiVersionsRequest::header_size());
    }
        
    #[test]
    fn parses_api_versions_request_from_request_bytes() {
        // 00 00 00 23  // message_size:        35
        // 00 12        // request_api_key:     18
        // 67 4a        // request_api_version: 26442
        // 4f 74 d2 8b  // correlation_id:      1333056139
        let bytes: &[u8] = &[
            0x00, 0x00, 0x00, 0x23,
            0x00, 0x12,
            0x67, 0x4a,
            0x4f, 0x74, 0xd2, 0x8b,
        ];
        let api_version_request = ApiVersionsRequest::parse(bytes);
        assert_eq!(api_version_request.message_size, 35);
        assert_eq!(api_version_request.request_api_key, 18);
        assert_eq!(api_version_request.request_api_version, 26442);
        assert_eq!(api_version_request.correlation_id, 1333056139);
    }
}


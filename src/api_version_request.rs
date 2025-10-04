use crate::headers::request_header_v1::RequestHeaderV1;

#[allow(dead_code)]
#[derive(Debug)]
pub struct ApiVersionsRequest {
    pub header: RequestHeaderV1,
}

#[allow(dead_code)]
impl ApiVersionsRequest {

    pub fn header_size() -> usize {
        RequestHeaderV1::size()
    }

    pub fn parse(bytes: &[u8]) -> ApiVersionsRequest {
        ApiVersionsRequest {
            header: RequestHeaderV1::parse(bytes)
        }
    }

}


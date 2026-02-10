use crate::{
    headers::{response_header_v0::ResponseHeaderV0, response_header_v1::ResponseHeaderV1},
    serializable::Serializable,
    size::Size,
};

#[derive(Debug, Clone)]
pub struct ApiResponse<H: Serializable, R: Serializable> {
    pub message_size: i32,
    response_header: H,
    response: R,
}

impl<H: Serializable, R: Serializable> ApiResponse<H, R> {
    pub fn new(response_header: H, response: R) -> ApiResponse<H, R> {
        let message_size = (response_header.size() + response.size()) as i32;
        ApiResponse {
            message_size,
            response_header,
            response,
        }
    }
}

// TODO: Remove as (currently) not needed
impl<H: Serializable, R: Serializable> Size for ApiResponse<H, R> {
    fn size(&self) -> usize {
        self.message_size.size() + self.response_header.size() + self.response.size()
    }
}

impl<H: Serializable, R: Serializable> Serializable for ApiResponse<H, R> {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.message_size.to_be_bytes());
        bytes.extend_from_slice(&self.response_header.to_be_bytes());
        bytes.extend_from_slice(&self.response.to_be_bytes());
        bytes
    }
}

pub fn v0<R: Serializable>(correlation_id: i32, response: R) -> ApiResponse<ResponseHeaderV0, R> {
    let response_header = ResponseHeaderV0::new(correlation_id);
    ApiResponse::new(response_header, response)
}

pub fn v1<R: Serializable>(correlation_id: i32, response: R) -> ApiResponse<ResponseHeaderV1, R> {
    let response_header = ResponseHeaderV1::new(correlation_id);
    ApiResponse::new(response_header, response)
}

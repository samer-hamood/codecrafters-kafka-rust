
use crate::tag_section::TagSection;
use crate::headers::request_header_v1::RequestHeaderV1;
use crate::size::Size;

#[allow(dead_code)]
#[derive(Debug)]
pub struct FetchRequestV16 {
    pub header: RequestHeaderV1,
    _tagged_fields: TagSection,
}

impl FetchRequestV16 {



}

impl Size for FetchRequestV16 {
    fn size(&self) -> i32 {
        <usize as TryInto<i32>>::try_into(RequestHeaderV1::size()).unwrap() + self._tagged_fields.size()
    }
}


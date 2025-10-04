
use crate::tag_section::TagSection;
use crate::headers::request_header_v1::RequestHeaderV1;
use crate::size::Size;

#[derive(Debug)]
pub struct FetchV16Request {
    pub header: RequestHeaderV1,
    _tagged_fields: TagSection,
}

impl FetchV16Request {

    pub fn header_size() -> usize {
        RequestHeaderV1::size()
    }

    pub fn parse(bytes: &[u8]) -> FetchV16Request {
        FetchV16Request {
            header: RequestHeaderV1::parse(bytes),
            _tagged_fields: TagSection::empty(),
        }
    }

}

impl Size for FetchV16Request {
    fn size(&self) -> i32 {
        <usize as TryInto<i32>>::try_into(RequestHeaderV1::size()).unwrap() + self._tagged_fields.size()
    }
}


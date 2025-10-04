use std::i32;

use crate::serializable::Serializable;
use crate::tag_section::TagSection;
use crate::size::Size;

#[derive(Debug)]
pub struct ResponseHeaderV1 {
    pub correlation_id: i32,
    _tagged_fields: TagSection,
}

impl ResponseHeaderV1 {

    pub fn new(correlation_id: i32) -> ResponseHeaderV1 {
        ResponseHeaderV1 {
            correlation_id: correlation_id,
            _tagged_fields: TagSection::empty(),
        }
    }

}

impl Serializable for ResponseHeaderV1 {

    fn to_be_bytes(&self) -> Vec<u8> {
        // Convert to bytes in big-endian order
        let correlation_id_bytes = self.correlation_id.to_be_bytes();
        let tagged_fields_bytes = self._tagged_fields.to_be_bytes();
        let mut bytes = Vec::new();
        for i in 0..correlation_id_bytes.len() {
            bytes.push(correlation_id_bytes[i]);
        }
        for i in 0..tagged_fields_bytes.len() {
            bytes.push(tagged_fields_bytes[i]);
        }
        bytes
    }

}

impl Size for ResponseHeaderV1 {

    fn size(&self) -> i32 {
        <usize as TryInto<i32>>::try_into(size_of::<i32>()).unwrap() + self._tagged_fields.size()
    }

}


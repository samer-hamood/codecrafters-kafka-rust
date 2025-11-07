use std::i32;

use crate::serializable::{BoxedSerializable, Serializable};
use crate::size::Size;
use crate::tagged_fields_section::TaggedFieldsSection;

#[derive(Debug, Clone)]
pub struct ResponseHeaderV1 {
    pub correlation_id: i32,
    _tagged_fields: TaggedFieldsSection,
}

impl ResponseHeaderV1 {
    pub fn new(correlation_id: i32) -> ResponseHeaderV1 {
        ResponseHeaderV1 {
            correlation_id: correlation_id,
            _tagged_fields: TaggedFieldsSection::empty(),
        }
    }
}

impl Size for ResponseHeaderV1 {
    fn size(&self) -> usize {
        self.correlation_id.size() + self._tagged_fields.size()
    }
}

impl Serializable for ResponseHeaderV1 {
    fn serializable_fields(&self) -> Vec<BoxedSerializable> {
        let mut fields: Vec<BoxedSerializable> = Vec::with_capacity(2);
        fields.push(Box::new(self.correlation_id));
        fields.push(Box::new(self._tagged_fields.clone()));
        fields
    }
}

use crate::serializable::Serializable;
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
            correlation_id,
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
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.correlation_id.to_be_bytes());
        bytes.extend_from_slice(&self._tagged_fields.to_be_bytes());
        bytes
    }
}

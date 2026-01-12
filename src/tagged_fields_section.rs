use crate::{
    byte_parsable::ByteParsable, serializable::Serializable, size::Size,
    types::unsigned_varint::UnsignedVarint,
};

pub const EMPTY: usize = 1;

#[derive(Debug, Clone)]
pub struct TaggedFieldsSection {
    number_of_tagged_fields: UnsignedVarint,
}

impl TaggedFieldsSection {
    pub fn empty() -> Self {
        Self {
            number_of_tagged_fields: UnsignedVarint::new(0),
            // TODO: add field for tagged fields
        }
    }
}

impl PartialEq for TaggedFieldsSection {
    fn eq(&self, other: &Self) -> bool {
        self.number_of_tagged_fields.value == other.number_of_tagged_fields.value
        // TODO: compare tagged fields
    }
}

impl Size for TaggedFieldsSection {
    fn size(&self) -> usize {
        self.number_of_tagged_fields.size()
        // TODO: add sizes for tagged fields also
    }
}

impl Serializable for TaggedFieldsSection {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.number_of_tagged_fields.to_be_bytes());
        // TODO: serialize tagged fields also
        bytes
    }
}

impl ByteParsable<TaggedFieldsSection> for TaggedFieldsSection {
    fn parse(bytes: &[u8], offset: usize) -> TaggedFieldsSection {
        let number_of_tagged_fields = UnsignedVarint::parse(bytes, offset);
        // TODO: parse tagged fields also
        TaggedFieldsSection {
            number_of_tagged_fields,
        }
    }
}

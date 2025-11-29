use crate::{byte_parsable::ByteParsable, serializable::Serializable, size::Size};

pub const EMPTY: usize = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct TaggedFieldsSection {
    number_of_tagged_fields: u8,
}

impl TaggedFieldsSection {
    pub fn empty() -> TaggedFieldsSection {
        TaggedFieldsSection {
            number_of_tagged_fields: 0,
        }
    }
}

impl Size for TaggedFieldsSection {
    fn size(&self) -> usize {
        if self.number_of_tagged_fields == 0 {
            1
        } else {
            panic!("Calculating size of non-empty Tagsection not implemented")
        }
    }
}

impl Serializable for TaggedFieldsSection {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        if self.number_of_tagged_fields == 0 {
            bytes.push(0);
        }
        bytes
    }
}

impl ByteParsable<TaggedFieldsSection> for TaggedFieldsSection {
    fn parse(_bytes: &[u8], _offset: usize) -> TaggedFieldsSection {
        // let mut offset = offset;
        // TOOD: parse length to determine what to return: empty or not
        TaggedFieldsSection::empty()
    }
}

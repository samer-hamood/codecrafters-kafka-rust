use crate::serializable::{BoxedSerializable, Serializable};
use crate::size::Size;
use crate::tagged_fields_section::TaggedFieldsSection;

#[derive(Debug, Clone)]
pub struct CompactRecords {
    length: u8, // uvarint
                // attributes: u8,
                // timestamp_delta: i64, // zigzag varint
                // offset_delta: i32,    // zigzag varint
                // key: Option<Vec<u8>>, // CompactBytes
                // value: Option<Vec<u8>>,
                // headers: Vec<Header>, // CompactArray
                // _tag_fields: TagSection,
}

impl CompactRecords {
    pub fn empty() -> Self {
        Self { length: 0 }
    }
}

impl Size for CompactRecords {
    fn size(&self) -> usize {
        size_of::<u8>() + self.length as usize
    }
}

impl Serializable for CompactRecords {
    fn serializable_fields(&self) -> Vec<BoxedSerializable> {
        vec![Box::new(self.length)]
    }
}

// struct Header {
//     key: String,          // CompactString
//     value: Option<Vec<u8>>,
// }

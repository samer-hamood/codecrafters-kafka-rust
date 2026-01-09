use crate::serializable::{BoxedSerializable, Serializable};
use crate::size::Size;
use crate::tagged_fields_section::TaggedFieldsSection;
use crate::types::compact_nullable_bytes::CompactNullableBytes;

#[derive(Debug, Clone)]
pub struct CompactRecords {
    records: CompactNullableBytes,
}

impl CompactRecords {
    pub fn null() -> Self {
        Self {
            records: CompactNullableBytes::null(),
        }
    }
}

impl Size for CompactRecords {
    fn size(&self) -> usize {
        self.records.size()
    }
}

impl Serializable for CompactRecords {
    fn to_be_bytes(&self) -> Vec<u8> {
        self.records.to_be_bytes()
    }
}

use crate::serializable::{BoxedSerializable, Serializable};
use crate::size::Size;
use crate::tagged_fields_section::TaggedFieldsSection;
use crate::types::compact_nullable_bytes::CompactNullableBytes;
use crate::types::unsigned_varint::UnsignedVarint;

#[derive(Debug, Clone)]
pub struct CompactRecords {
    records: CompactNullableBytes,
}

impl CompactRecords {
    #[allow(dead_code)]
    pub fn null() -> Self {
        Self {
            records: CompactNullableBytes::null(),
        }
    }

    fn new(length: UnsignedVarint, bytes: Option<Vec<u8>>) -> Self {
        Self {
            records: CompactNullableBytes { length, bytes },
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

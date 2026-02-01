use crate::records::record_batch::RecordBatch;
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
    pub fn null() -> Self {
        Self {
            records: CompactNullableBytes::null(),
        }
    }

    pub fn from_record_batches(record_batches: &[RecordBatch]) -> Self {
        let record_batches_bytes = record_batches
            .iter()
            .flat_map(|record_batch| record_batch._parsed_bytes.clone())
            .collect();
        let unsigned_varint_value = record_batches
            .iter()
            .map(|record_batch| record_batch.expected_length() as u32)
            .sum::<u32>()
            + 1;
        let length = UnsignedVarint::new(unsigned_varint_value);
        let bytes = if record_batches.is_empty() {
            None
        } else {
            Some(record_batches_bytes)
        };
        Self::new(length, bytes)
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

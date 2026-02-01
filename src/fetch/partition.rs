use std::io::Cursor;

use crate::byte_parsable::ByteParsable;
use crate::fetch::partition;
use crate::serializable::{BoxedSerializable, Serializable};
use crate::size::{self, Size};
use crate::tagged_fields_section::TaggedFieldsSection;
use crate::types::compact_array::CompactArray;
use crate::types::compact_records::CompactRecords;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct RequestPartition {
    pub partition: i32,
    pub current_leader_epoch: i32,
    pub fetch_offset: i64,
    pub last_fetched_epoch: i32,
    pub log_start_offset: i64,
    pub partition_max_bytes: i32,
    pub _tagged_fields: TaggedFieldsSection,
}

impl Size for RequestPartition {
    fn size(&self) -> usize {
        self.partition.size()
            + self.current_leader_epoch.size()
            + self.fetch_offset.size()
            + self.last_fetched_epoch.size()
            + self.log_start_offset.size()
            + self.partition_max_bytes.size()
            + self._tagged_fields.size()
    }
}

impl ByteParsable<RequestPartition> for RequestPartition {
    fn parse(bytes: &[u8], offset: usize) -> Self {
        let mut offset = offset;
        let partition = i32::parse(bytes, offset);
        offset += partition.size();
        let current_leader_epoch = i32::parse(bytes, offset);
        offset += current_leader_epoch.size();
        let fetch_offset = i64::parse(bytes, offset);
        offset += fetch_offset.size();
        let last_fetched_epoch = i32::parse(bytes, offset);
        offset += last_fetched_epoch.size();
        let log_start_offset = i64::parse(bytes, offset);
        offset += log_start_offset.size();
        let partition_max_bytes = i32::parse(bytes, offset);
        offset += partition_max_bytes.size();
        let _tagged_fields = TaggedFieldsSection::parse(bytes, offset);
        Self {
            partition,
            current_leader_epoch,
            fetch_offset,
            last_fetched_epoch,
            log_start_offset,
            partition_max_bytes,
            _tagged_fields,
        }
    }
}

impl Serializable for RequestPartition {
    fn to_be_bytes(&self) -> Vec<u8> {
        todo!()
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ResponsePartition {
    pub partition_index: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub log_start_offset: i64,
    pub aborted_transactions: CompactArray<Transaction>,
    pub preferred_read_replica: i32,
    pub records: CompactRecords,
    pub _tagged_fields: TaggedFieldsSection,
}

impl Size for ResponsePartition {
    fn size(&self) -> usize {
        self.partition_index.size()
            + self.error_code.size()
            + self.high_watermark.size()
            + self.last_stable_offset.size()
            + self.log_start_offset.size()
            + self.aborted_transactions.size()
            + self.preferred_read_replica.size()
            + self.records.size()
            + self._tagged_fields.size()
    }
}

impl Serializable for ResponsePartition {
    fn serializable_fields(&self) -> Vec<BoxedSerializable> {
        vec![
            Box::new(self.partition_index),
            Box::new(self.error_code),
            Box::new(self.high_watermark),
            Box::new(self.last_stable_offset),
            Box::new(self.log_start_offset),
            Box::new(self.aborted_transactions.clone()),
            Box::new(self.preferred_read_replica),
            Box::new(self.records.clone()),
            Box::new(self._tagged_fields.clone()),
        ]
    }
}

impl ByteParsable<ResponsePartition> for ResponsePartition {
    fn parse(_bytes: &[u8], _offset: usize) -> ResponsePartition {
        todo!()
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Transaction {
    producer_id: i64,
    first_offset: i64,
    _tagged_fields: TaggedFieldsSection,
}

impl Size for Transaction {
    fn size(&self) -> usize {
        self.producer_id.size() + self.first_offset.size() + self._tagged_fields.size()
    }
}

impl ByteParsable<Transaction> for Transaction {
    fn parse(_bytes: &[u8], _offset: usize) -> Transaction {
        todo!()
    }
}

impl Serializable for Transaction {
    fn to_be_bytes(&self) -> Vec<u8> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use crate::error_codes::UNKNOWN_TOPIC_ID;

    use super::*;

    #[test]
    fn computes_message_size() {
        let expected_size = 4 + 2 + 8 + 8 + 8 + (1 + 0) + 4 + 1 + 1;

        let partition = ResponsePartition {
            partition_index: 0,                           // 4 bytes
            error_code: UNKNOWN_TOPIC_ID,                 // 2 bytes
            high_watermark: 0,                            // 8 bytes
            last_stable_offset: 0,                        // 8 bytes
            log_start_offset: 0,                          // 8 bytes
            aborted_transactions: CompactArray::empty(),  // (1 + 0) byte
            preferred_read_replica: 0,                    // 4 bytes
            records: CompactRecords::null(),              // 1 byte
            _tagged_fields: TaggedFieldsSection::empty(), // 1 bytes
        };

        assert_eq!(expected_size, partition.size());
    }
}

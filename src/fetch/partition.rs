use std::io::Cursor;

use crate::byte_parsable::ByteParsable;
use crate::compact_array::CompactArray;
use crate::compact_records::CompactRecords;
use crate::fetch::partition;
use crate::serializable::{BoxedSerializable, Serializable};
use crate::size::{self, Size};
use crate::tagged_fields_section::TaggedFieldsSection;

#[allow(dead_code)]
const PARTITION: usize = size_of::<i32>();
#[allow(dead_code)]
const CURRENT_LEADER_EPOCH: usize = size_of::<i32>();
#[allow(dead_code)]
const FETCH_OFFSET: usize = size_of::<i64>();
#[allow(dead_code)]
const LAST_FETCHED_EPOCH : usize = size_of::<i32>();
#[allow(dead_code)]
const LOG_START_OFFSET: usize = size_of::<i64>();
#[allow(dead_code)]
const PARTITION_MAX_BYTES: usize = size_of::<i32>();

#[allow(dead_code)]
pub trait Partition: Serializable + Size {}

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
        let partition = i32::from_be_bytes(bytes[offset..offset + PARTITION].try_into().unwrap());
        offset += PARTITION;
        let current_leader_epoch = i32::from_be_bytes(bytes[offset..offset + CURRENT_LEADER_EPOCH].try_into().unwrap());
        offset += CURRENT_LEADER_EPOCH;
        let fetch_offset = i64::from_be_bytes(bytes[offset..offset + FETCH_OFFSET].try_into().unwrap());
        offset += FETCH_OFFSET;
        let last_fetched_epoch = i32::from_be_bytes(bytes[offset..offset + LAST_FETCHED_EPOCH].try_into().unwrap());
        offset += LAST_FETCHED_EPOCH;
        let log_start_offset = i64::from_be_bytes(bytes[offset..offset + LOG_START_OFFSET].try_into().unwrap());
        offset += LOG_START_OFFSET;
        let partition_max_bytes = i32::from_be_bytes(bytes[offset..offset + PARTITION_MAX_BYTES].try_into().unwrap());
        offset += PARTITION_MAX_BYTES;
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
    partition_index: i32,
    error_code: i16,
    high_watermark: i64,
    last_stable_offset: i64,
    log_start_offset: i64,
    aborted_transactions: CompactArray<Transaction>,
    preferred_read_replica: i32,
    records: CompactRecords,
    _tagged_fields: TaggedFieldsSection,
}

impl ResponsePartition {
    pub fn new(
        partition_index: i32,
        error_code: i16,
        high_watermark: i64,
        last_stable_offset: i64,
        log_start_offset: i64,
        aborted_transactions: CompactArray<Transaction>,
        preferred_read_replica: i32,
        records: CompactRecords,
        _tagged_fields: TaggedFieldsSection,
    ) -> Self {
        Self {
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            aborted_transactions,
            preferred_read_replica,
            records,
            _tagged_fields,
        }
    }
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
        let mut fields: Vec<BoxedSerializable> = Vec::with_capacity(9);
        fields.push(Box::new(self.partition_index));
        fields.push(Box::new(self.error_code));
        fields.push(Box::new(self.high_watermark));
        fields.push(Box::new(self.last_stable_offset));
        fields.push(Box::new(self.log_start_offset));
        fields.push(Box::new(self.aborted_transactions.clone()));
        fields.push(Box::new(self.preferred_read_replica));
        fields.push(Box::new(self.records.clone()));
        fields.push(Box::new(self._tagged_fields.clone()));
        fields
    }
}

impl ByteParsable<ResponsePartition> for ResponsePartition {
    fn parse(_bytes: &[u8], _offset: usize) -> ResponsePartition {
        todo!()
    }
}

impl Partition for ResponsePartition {}

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

        let partition = ResponsePartition::new(
            0,                            // 4 bytes
            UNKNOWN_TOPIC_ID,             // 2 bytes
            0,                            // 8 bytes
            0,                            // 8 bytes
            0,                            // 8 bytes
            CompactArray::empty(),        // (1 + 0) byte
            0,                            // 4 bytes
            CompactRecords::empty(),      // 1 bytes
            TaggedFieldsSection::empty(), // 1 bytes
        );

        assert_eq!(expected_size, partition.size());
    }
}

use uuid::Uuid;

use crate::partial_parsable::PartialParsable;
use crate::records::metadata_record::{MetadataRecord, PARTITION, TOPIC};
use crate::records::partition_record::PartitionRecord;
use crate::records::topic_record::TopicRecord;
use crate::types::compact_string::CompactString;
use crate::types::signed_varint::SignedVarint;
use crate::types::unsigned_varint::UnsignedVarint;
use crate::types::varlong::Varlong;
use crate::{byte_parsable::ByteParsable, size::Size};

#[derive(Debug)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records_count: i32,
    pub records: Vec<Record>,
    // Store bytes parsed
    pub _parsed_bytes: Vec<u8>,
}

impl RecordBatch {
    pub fn expected_length(&self) -> usize {
        self.base_offset.size() + self.batch_length.size() + self.batch_length as usize
    }

    pub fn parse_record_values(
        &self,
        search_item: SearchItem,
        topic_record_only: bool,
    ) -> Vec<RecordValue> {
        let mut record_values = Vec::new();
        for record in &self.records {
            let mut offset: usize = 0;
            let metadata_record = MetadataRecord::parse(&record.value, offset);
            offset += metadata_record.size();
            match metadata_record._type {
                TOPIC => {
                    let topic_record = TopicRecord::parse(&record.value, offset, metadata_record);
                    if search_item.found_in(&topic_record) {
                        record_values.push(RecordValue::Topic(topic_record));
                    } else {
                        break;
                    }
                    if topic_record_only {
                        break;
                    }
                }
                // Based on topic records always existing and before a partition record if one is in
                // the batch
                PARTITION => {
                    let partition_record =
                        PartitionRecord::parse(&record.value, offset, metadata_record);
                    record_values.push(RecordValue::Partition(partition_record));
                }
                _ => {}
            }
        }
        record_values
    }
}

impl Size for RecordBatch {
    fn size(&self) -> usize {
        self.base_offset.size()
            + self.batch_length.size()
            + self.partition_leader_epoch.size()
            + self.magic.size()
            + self.crc.size()
            + self.attributes.size()
            + self.last_offset_delta.size()
            + self.base_timestamp.size()
            + self.max_timestamp.size()
            + self.producer_id.size()
            + self.producer_epoch.size()
            + self.base_sequence.size()
            + self.records_count.size()
            + self.records.size()
    }
}

impl ByteParsable<RecordBatch> for RecordBatch {
    fn parse(bytes: &[u8], offset: usize) -> Self {
        let initial_offset: usize = offset;
        let mut offset: usize = offset;
        let base_offset = i64::parse(bytes, offset);
        offset += base_offset.size();
        let batch_length = i32::parse(bytes, offset);
        offset += batch_length.size();
        let partition_leader_epoch = i32::parse(bytes, offset);
        offset += partition_leader_epoch.size();
        let magic = i8::parse(bytes, offset);
        offset += magic.size();
        let crc = u32::parse(bytes, offset);
        offset += crc.size();
        let attributes = i16::parse(bytes, offset);
        offset += attributes.size();
        let last_offset_delta = i32::parse(bytes, offset);
        offset += last_offset_delta.size();
        let base_timestamp = i64::parse(bytes, offset);
        offset += base_timestamp.size();
        let max_timestamp = i64::parse(bytes, offset);
        offset += max_timestamp.size();
        let producer_id = i64::parse(bytes, offset);
        offset += producer_id.size();
        let producer_epoch = i16::parse(bytes, offset);
        offset += producer_epoch.size();
        let base_sequence = i32::parse(bytes, offset);
        offset += base_sequence.size();
        let records_count = i32::parse(bytes, offset);
        offset += records_count.size();
        let mut records = Vec::with_capacity(records_count as usize);
        for _ in 0..records_count {
            let record = Record::parse(bytes, offset);
            offset += record.size();
            records.push(record);
        }
        let parsed_bytes_count = offset - initial_offset;
        let expected_batch_size = base_offset.size() + batch_length.size() + batch_length as usize;
        assert_eq!(parsed_bytes_count, expected_batch_size);
        let _parsed_bytes = bytes[initial_offset..offset].to_vec();
        Self {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records_count,
            records,
            _parsed_bytes,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Record {
    pub length: SignedVarint,
    pub attributes: i8,
    pub timestamp_delta: Varlong,
    pub offset_delta: SignedVarint,
    pub key_length: SignedVarint,
    pub key: Option<Vec<u8>>,
    pub value_length: SignedVarint,
    pub value: Vec<u8>,
    pub headers_count: SignedVarint,
    pub headers: Option<Vec<Header>>,
}

impl Size for Record {
    fn size(&self) -> usize {
        self.length.size()
            + self.attributes.size()
            + self.timestamp_delta.size()
            + self.offset_delta.size()
            + self.key_length.size()
            + self.key.size()
            + self.value_length.size()
            + self.value.size()
            + self.headers_count.size()
            + self.headers.size()
    }
}

impl ByteParsable<Record> for Record {
    fn parse(bytes: &[u8], offset: usize) -> Self {
        let initial_offset: usize = offset;
        let mut offset: usize = offset;
        let length = SignedVarint::parse(bytes, offset);
        offset += length.size();
        let attributes = i8::parse(bytes, offset);
        offset += attributes.size();
        let timestamp_delta = Varlong::parse(bytes, offset);
        offset += timestamp_delta.size();
        let offset_delta = SignedVarint::parse(bytes, offset);
        offset += offset_delta.size();
        let key_length = SignedVarint::parse(bytes, offset);
        offset += key_length.size();
        let key = if key_length.value == -1 {
            None
        } else {
            Some(bytes[offset..offset + key_length.value as usize].to_vec())
        };
        offset += key.size();
        let value_length = SignedVarint::parse(bytes, offset);
        offset += value_length.size();
        let value = bytes[offset..offset + value_length.value as usize].to_vec();
        offset += value.len();
        let headers_count = SignedVarint::parse(bytes, offset);
        offset += headers_count.size();
        let headers = if headers_count.value == 0 {
            None
        } else {
            let mut headers = Vec::new();
            for _ in 0..headers_count.value {
                let header = Header::parse(bytes, offset);
                offset += header.size();
                headers.push(header);
            }
            Some(headers)
        };
        assert_eq!(
            offset - initial_offset,
            length.size() + length.value as usize
        );
        Self {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key_length,
            key,
            value_length,
            value,
            headers_count,
            headers,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Header {
    header_key_length: SignedVarint,
    header_key: String,
    header_value_length: SignedVarint,
    value: Vec<u8>,
}

impl Size for Header {
    fn size(&self) -> usize {
        self.header_key_length.size()
            + self.header_key.size()
            + self.header_value_length.size()
            + self.value.size()
    }
}

impl ByteParsable<Header> for Header {
    fn parse(bytes: &[u8], offset: usize) -> Self {
        let mut offset: usize = offset;
        let header_key_length = SignedVarint::parse(bytes, offset);
        offset += header_key_length.size();
        let header_key =
            match String::from_utf8(bytes[offset..header_key_length.value as usize].to_vec()) {
                Ok(s) => s,
                Err(e) => panic!("Invalid UTF-8: {}", e),
            };
        offset += header_key_length.size();
        let header_value_length = SignedVarint::parse(bytes, offset);
        offset += header_value_length.size();
        let value = bytes[offset..offset + header_value_length.value as usize].to_vec();
        Self {
            header_key_length,
            header_key,
            header_value_length,
            value,
        }
    }
}

pub enum SearchItem {
    TopicId(Uuid),
    TopicName(CompactString),
}

impl SearchItem {
    fn found_in(&self, topic_record: &TopicRecord) -> bool {
        match self {
            Self::TopicId(id) => id == &topic_record.topic_uuid,
            Self::TopicName(name) => name == &topic_record.topic_name,
        }
    }
}

pub enum RecordValue {
    Topic(TopicRecord),
    Partition(PartitionRecord),
}

impl RecordValue {
    pub fn to_topic_record(&self) -> Option<TopicRecord> {
        if let RecordValue::Topic(record) = self {
            Some(record.clone())
        } else {
            None
        }
    }

    pub fn to_partition_record(&self) -> Option<PartitionRecord> {
        if let RecordValue::Partition(record) = self {
            Some(record.clone())
        } else {
            None
        }
    }

    pub fn into_partition_record(self) -> Option<PartitionRecord> {
        if let RecordValue::Partition(record) = self {
            Some(record)
        } else {
            None
        }
    }
}

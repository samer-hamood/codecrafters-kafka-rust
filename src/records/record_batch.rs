use crate::types::signed_varint::SignedVarint;
use crate::types::unsigned_varint::UnsignedVarint;
use crate::types::varlong::Varlong;
use crate::{byte_parsable::ByteParsable, compact_array::CompactArray, size::Size};

#[allow(dead_code)]
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
            + self
                .records
                .iter()
                .map(|record| record.size())
                .sum::<usize>()
    }
}

impl ByteParsable<RecordBatch> for RecordBatch {
    fn parse(bytes: &[u8], offset: usize) -> Self {
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
        }
    }
}

#[allow(dead_code)]
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
    pub headers_count: UnsignedVarint,
    pub headers: Option<Vec<Header>>,
}

impl Size for Record {
    fn size(&self) -> usize {
        self.length.byte_count
            + self.attributes.size()
            + self.timestamp_delta.size()
            + self.offset_delta.size()
            + self.key_length.size()
            + self.key.as_ref().map(|v| v.len()).unwrap_or(0)
            + self.value_length.size()
            + self.value.len()
            + self.headers_count.size()
            + self.headers.as_ref().map(|v| v.len()).unwrap_or(0)
    }
}

impl ByteParsable<Record> for Record {
    fn parse(bytes: &[u8], offset: usize) -> Self {
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
        offset += key.as_ref().map_or(0, |v| v.len());
        let value_length = SignedVarint::parse(bytes, offset);
        offset += value_length.size();
        let value = bytes[offset..offset + value_length.value as usize].to_vec();
        offset += value.len();
        let headers_count = UnsignedVarint::parse(bytes, offset);
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

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct Header {}

impl Size for Header {
    fn size(&self) -> usize {
        0
    }
}

impl ByteParsable<Header> for Header {
    fn parse(_bytes: &[u8], _offset: usize) -> Self {
        Self {}
    }
}

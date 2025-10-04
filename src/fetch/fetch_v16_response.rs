use std::i32;

use crate::compact_array::CompactArray;
use crate::serializable::Serializable;
use crate::tag_section::TagSection;
use crate::headers::response_header_v1::ResponseHeaderV1;
use crate::size::Size;

/// Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] _tagged_fields 
///   throttle_time_ms => INT32
///   error_code => INT16
///   session_id => INT32
///   responses => topic_id [partitions] _tagged_fields 
///     topic_id => UUID
///     partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records _tagged_fields 
///       partition_index => INT32
///       error_code => INT16
///       high_watermark => INT64
///       last_stable_offset => INT64
///       log_start_offset => INT64
///       aborted_transactions => producer_id first_offset _tagged_fields 
///         producer_id => INT64
///         first_offset => INT64
///       preferred_read_replica => INT32
///       records => COMPACT_RECORDS
#[derive(Debug)]
pub struct FetchV16Response {
    header: ResponseHeaderV1,
    throttle_time_ms: i32,
    error_code: i16,
    session_id: i16,
    responses: CompactArray<Topic>,
    _tagged_fields: TagSection
}

impl FetchV16Response {

    pub fn new(correlation_id: i32, throttle_time_ms: i32, error_code: i16, session_id: i16, responses: Vec<Topic>, _tagged_fields: TagSection) -> FetchV16Response {
        FetchV16Response {
            header: ResponseHeaderV1::new(correlation_id),
            throttle_time_ms: throttle_time_ms,
            error_code: error_code,                              
            session_id: session_id,
            responses: CompactArray::new(responses),
            _tagged_fields: _tagged_fields,
        }
    }

}

impl Serializable for FetchV16Response {

    fn to_be_bytes(&self) -> Vec<u8> {
        // Convert to bytes in big-endian order
        let message_size = self.size();
        let message_size_bytes = message_size.to_be_bytes();
        let header_bytes = self.header.to_be_bytes();
        let throttle_time_ms_bytes = self.throttle_time_ms.to_be_bytes();
        let error_code_bytes = self.error_code.to_be_bytes();
        let session_id_bytes = self.session_id.to_be_bytes();
        let responses_bytes = self.responses.to_be_bytes();
        let tagged_fields_bytes = self._tagged_fields.to_be_bytes();
        let mut bytes = Vec::new();
        for i in 0..message_size_bytes.len() {
            bytes.push(message_size_bytes[i]);
        }
        for i in 0..header_bytes.len() {
            bytes.push(header_bytes[i]);
        }
        for i in 0..throttle_time_ms_bytes.len() {
            bytes.push(throttle_time_ms_bytes[i]);
        }
        for i in 0..error_code_bytes.len() {
            bytes.push(error_code_bytes[i]);
        }
        for i in 0..session_id_bytes.len() {
            bytes.push(session_id_bytes[i]);
        }
        for i in 0..responses_bytes.len() {
            bytes.push(responses_bytes[i]);
        }
        for i in 0..tagged_fields_bytes.len() {
            bytes.push(tagged_fields_bytes[i]);
        }
        bytes
    }

}

impl Size for FetchV16Response {
    
    fn size(&self) -> i32 {
        self.header.size() + 
            <usize as TryInto<i32>>::try_into(
                size_of::<i32>() + size_of::<i16>() + size_of::<i16>()
            )
            .unwrap() + self.responses.size() + self._tagged_fields.size()
    }

}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Topic {
    topic_id: String, // UUID
    partitions: CompactArray<Partition>,
    _tagged_fields: TagSection
}

impl Serializable for Topic {

    fn to_be_bytes(&self) -> Vec<u8> {
        // TODO: Implement serialization
        let bytes = Vec::new();
        bytes
    }

}

impl Size for Topic {

    fn size(&self) -> i32 {
        let topic_id_size = 0; // TODO: Figure out actual size of topic_id UUID  
        topic_id_size + self.partitions.size() + self._tagged_fields.size()
    }

}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Partition {
    partition_index: i32,
    error_code: i16,
    high_watermark: i64,
    last_stable_offset: i64,
    log_start_offset: i64,
    aborted_transactions: CompactArray<Transaction>,
    preferred_read_replica: i32,
    // records: COMPACT_RECORDS,
    _tagged_fields: TagSection
}

impl Serializable for Partition {

    fn to_be_bytes(&self) -> Vec<u8> {
        // TODO: Implement serialization
        let bytes = Vec::new();
        bytes
    }

}

impl Size for Partition {

    fn size(&self) -> i32 {
        let records = 0;
        <usize as TryInto<i32>>::try_into(
           2 * size_of::<i32>() + 3 * size_of::<i64>() + size_of::<i16>()
        ).unwrap() + self.aborted_transactions.size() + records + self._tagged_fields.size()
    }

}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Transaction {
    producer_id: i64,
    first_offset: i64,
    _tagged_fields: TagSection
}

impl Serializable for Transaction {

    fn to_be_bytes(&self) -> Vec<u8> {
        // TODO: Implement serialization
        let bytes = Vec::new();
        bytes
    }

}

impl Size for Transaction {

    fn size(&self) -> i32 {
        <usize as TryInto<i32>>::try_into(2 * size_of::<i64>()).unwrap() + self._tagged_fields.size()
    }

}


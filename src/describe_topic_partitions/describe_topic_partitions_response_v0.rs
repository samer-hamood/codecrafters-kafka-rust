use uuid::Uuid;

use crate::{
    byte_parsable::ByteParsable,
    headers::{self, response_header_v1::ResponseHeaderV1},
    serializable::{BoxedSerializable, Serializable},
    size::Size,
    tagged_fields_section::TaggedFieldsSection,
    types::{compact_array::CompactArray, compact_string::CompactString},
};

// https://kafka.apache.org/41/design/protocol/#The_Messages_DescribeTopicPartitions

/// DescribeTopicPartitions Response (Version: 0) => throttle_time_ms [topics] next_cursor _tagged_fields
///   throttle_time_ms => INT32
///   topics => error_code name topic_id is_internal [partitions] topic_authorized_operations _tagged_fields
///     error_code => INT16
///     name => COMPACT_NULLABLE_STRING
///     topic_id => UUID
///     is_internal => BOOLEAN
///     partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [eligible_leader_replicas] [last_known_elr] [offline_replicas] _tagged_fields
///       error_code => INT16
///       partition_index => INT32
///       leader_id => INT32
///       leader_epoch => INT32
///       replica_nodes => INT32
///       isr_nodes => INT32
///       eligible_leader_replicas => INT32
///       last_known_elr => INT32
///       offline_replicas => INT32
///     topic_authorized_operations => INT32
///   next_cursor => topic_name partition_index _tagged_fields
///     topic_name => COMPACT_STRING
///     partition_index => INT32
#[derive(Debug)]
pub struct DescribeTopicPartitionsResponseV0 {
    header: ResponseHeaderV1,
    pub throttle_time_ms: i32,
    pub topics: CompactArray<Topic>,
    pub next_cursor: i8, // Although, this type is not what is in the protocol spec (see above)
    pub _tagged_fields: TaggedFieldsSection,
}

impl DescribeTopicPartitionsResponseV0 {
    pub fn new(
        correlation_id: i32,
        throttle_time_ms: i32,
        topics: CompactArray<Topic>,
        next_cursor: i8,
        _tagged_fields: TaggedFieldsSection,
    ) -> Self {
        Self {
            header: ResponseHeaderV1::new(correlation_id),
            throttle_time_ms,
            topics,
            next_cursor,
            _tagged_fields,
        }
    }
}

impl Size for DescribeTopicPartitionsResponseV0 {
    fn size(&self) -> usize {
        self.header.size()
            + self.throttle_time_ms.size()
            + self.topics.size()
            + self.next_cursor.size()
            + self._tagged_fields.size()
    }
}

impl Serializable for DescribeTopicPartitionsResponseV0 {
    fn to_be_bytes(&self) -> Vec<u8> {
        let message_size = self.size() as i32;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&message_size.to_be_bytes());
        bytes.extend_from_slice(&self.header.to_be_bytes());
        bytes.extend_from_slice(&self.throttle_time_ms.to_be_bytes());
        bytes.extend_from_slice(&self.topics.to_be_bytes());
        bytes.extend_from_slice(&self.next_cursor.to_be_bytes());
        bytes.extend_from_slice(&self._tagged_fields.to_be_bytes());
        bytes
    }
}

impl std::fmt::Display for DescribeTopicPartitionsResponseV0 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("DescribeTopicPartitionsResponseV0");

        debug_struct.field("header", &self.header);
        debug_struct.field("throttle_time_ms", &self.throttle_time_ms);
        debug_struct.field("topics", &self.topics);
        debug_struct.field("next_cursor", &self.next_cursor);
        debug_struct.field("_tagged_fields", &self._tagged_fields);

        debug_struct.finish()
    }
}

#[derive(Debug, Clone)]
pub struct Topic {
    pub error_code: i16,
    pub name: CompactString,
    pub topic_id: Uuid,
    pub is_internal: bool,
    pub partitions: CompactArray<Partition>,
    pub topic_authorized_operations: i32,
    pub _tagged_fields: TaggedFieldsSection,
}

impl Topic {
    pub fn new(
        error_code: i16,
        name: CompactString,
        topic_id: Uuid,
        is_internal: bool,
        partitions: CompactArray<Partition>,
        topic_authorized_operations: i32,
        _tagged_fields: TaggedFieldsSection,
    ) -> Self {
        Self {
            error_code,
            name,
            topic_id,
            is_internal,
            partitions,
            topic_authorized_operations,
            _tagged_fields,
        }
    }
}

impl Size for Topic {
    fn size(&self) -> usize {
        self.error_code.size()
            + self.name.size()
            + self.topic_id.size()
            + self.is_internal.size()
            + self.partitions.size()
            + self.topic_authorized_operations.size()
            + self._tagged_fields.size()
    }
}

impl ByteParsable<Self> for Topic {
    fn parse(_bytes: &[u8], _offset: usize) -> Self {
        todo!()
    }
}

impl Serializable for Topic {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.error_code.to_be_bytes());
        bytes.extend_from_slice(&self.name.to_be_bytes());
        bytes.extend_from_slice(&self.topic_id.to_be_bytes());
        bytes.extend_from_slice(&self.is_internal.to_be_bytes());
        bytes.extend_from_slice(&self.partitions.to_be_bytes());
        bytes.extend_from_slice(&self.topic_authorized_operations.to_be_bytes());
        bytes.extend_from_slice(&self._tagged_fields.to_be_bytes());
        bytes
    }
}

#[derive(Debug, Clone)]
pub struct Partition {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
}
    pub replica_nodes: CompactArray<i32>,
    pub isr_nodes: CompactArray<i32>,
    pub eligible_leader_replicas: CompactArray<i32>,
    pub last_known_elr: CompactArray<i32>,
    pub offline_replicas: CompactArray<i32>,

impl Size for Partition {}

impl ByteParsable<Self> for Partition {
    fn parse(_bytes: &[u8], _offset: usize) -> Self {
        todo!()
    }
}

impl Serializable for Partition {}

use uuid::Uuid;

use crate::types::compact_array::CompactArray;
use crate::{
    byte_parsable::ByteParsable, partial_parsable::PartialParsable,
    records::metadata_record::MetadataRecord, size::Size, types::unsigned_varint::UnsignedVarint,
};

#[allow(dead_code)]
#[derive(Debug)]
pub struct PartitionRecord {
    #[allow(dead_code)]
    pub frame_version: i8,
    #[allow(dead_code)]
    pub _type: i8,
    #[allow(dead_code)]
    pub version: i8,
    #[allow(dead_code)]
    pub partition_id: i32,
    #[allow(dead_code)]
    pub topic_uuid: Uuid,
    #[allow(dead_code)]
    pub leader: u32,
    #[allow(dead_code)]
    pub leader_epoch: u32,
    #[allow(dead_code)]
    pub partition_epoch: u32,
    #[allow(dead_code)]
    pub replica_array: CompactArray<i32>,
    pub in_sync_replica_array: CompactArray<i32>,
    pub removing_replica_array: CompactArray<i32>,
    pub adding_replica_array: CompactArray<i32>,
    pub directories_array: CompactArray<Uuid>, // Array of UUIDs
    #[allow(dead_code)]
    pub tagged_fields_count: UnsignedVarint,
}

impl PartialParsable<Self, MetadataRecord> for PartitionRecord {
    fn parse(bytes: &[u8], offset: usize, metadata_record: MetadataRecord) -> Self {
        let mut offset = offset;
        let partition_id = i32::parse(bytes, offset);
        offset += partition_id.size();
        let topic_uuid = Uuid::parse(bytes, offset);
        offset += topic_uuid.size();
        let replica_array = CompactArray::parse(bytes, offset);
        offset += replica_array.size();
        let in_sync_replica_array = CompactArray::parse(bytes, offset);
        offset += in_sync_replica_array.size();
        let leader = u32::parse(bytes, offset);
        let removing_replica_array = CompactArray::parse(bytes, offset);
        offset += removing_replica_array.size();
        let adding_replica_array = CompactArray::parse(bytes, offset);
        offset += adding_replica_array.size();
        offset += leader.size();
        let leader_epoch = u32::parse(bytes, offset);
        offset += leader_epoch.size();
        let partition_epoch = u32::parse(bytes, offset);
        offset += partition_epoch.size();
        let directories_array = CompactArray::parse(bytes, offset);
        offset += directories_array.size();
        let tagged_fields_count = UnsignedVarint::parse(bytes, offset);
        Self {
            frame_version: metadata_record.frame_version,
            _type: metadata_record._type,
            version: metadata_record.version,
            partition_id,
            topic_uuid,
            replica_array,
            in_sync_replica_array,
            removing_replica_array,
            adding_replica_array,
            leader,
            leader_epoch,
            partition_epoch,
            directories_array,
            tagged_fields_count,
        }
    }
}

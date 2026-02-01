use uuid::Uuid;

use crate::types::compact_string::CompactString;
use crate::{
    byte_parsable::ByteParsable, partial_parsable::PartialParsable,
    records::metadata_record::MetadataRecord, size::Size, types::unsigned_varint::UnsignedVarint,
};

#[derive(Debug)]
pub struct TopicRecord {
    #[allow(dead_code)]
    pub frame_version: i8,
    #[allow(dead_code)]
    pub _type: i8,
    #[allow(dead_code)]
    pub version: i8,
    #[allow(dead_code)]
    pub topic_name: CompactString,
    pub topic_uuid: Uuid,
    #[allow(dead_code)]
    pub tagged_fields_count: UnsignedVarint,
}

impl PartialParsable<Self, MetadataRecord> for TopicRecord {
    fn parse(bytes: &[u8], offset: usize, metadata_record: MetadataRecord) -> Self {
        let mut offset = offset;
        let topic_name = CompactString::parse(bytes, offset);
        offset += topic_name.size();
        let topic_uuid = Uuid::parse(bytes, offset);
        offset += topic_uuid.size();
        let tagged_fields_count = UnsignedVarint::parse(bytes, offset);
        Self {
            frame_version: metadata_record.frame_version,
            _type: metadata_record._type,
            version: metadata_record.version,
            topic_name,
            topic_uuid,
            tagged_fields_count,
        }
    }
}

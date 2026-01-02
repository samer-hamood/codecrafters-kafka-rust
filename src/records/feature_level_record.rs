use crate::{
    byte_parsable::ByteParsable, compact_string::CompactString, partial_parsable::PartialParsable,
    records::metadata_record::MetadataRecord, size::Size, types::unsigned_varint::UnsignedVarint,
};

#[allow(dead_code)]
#[derive(Debug)]
pub struct FeatureLevelRecord {
    #[allow(dead_code)]
    pub frame_version: i8,
    #[allow(dead_code)]
    pub _type: i8,
    #[allow(dead_code)]
    pub version: i8,
    #[allow(dead_code)]
    pub name_length: UnsignedVarint,
    #[allow(dead_code)]
    pub name: CompactString,
    pub feature_level: i16,
    #[allow(dead_code)]
    pub tagged_fields_count: UnsignedVarint,
}

impl PartialParsable<Self, MetadataRecord> for FeatureLevelRecord {
    fn parse(bytes: &[u8], offset: usize, metadata_record: MetadataRecord) -> Self {
        let mut offset = offset;
        let name = CompactString::parse(bytes, offset);
        let name_length = name.length.clone();
        offset += name.size();
        let feature_level = i16::parse(bytes, offset);
        offset += feature_level.size();
        let tagged_fields_count = UnsignedVarint::parse(bytes, offset);
        Self {
            frame_version: metadata_record.frame_version,
            _type: metadata_record._type,
            version: metadata_record.version,
            name_length,
            name,
            feature_level,
            tagged_fields_count,
        }
    }
}

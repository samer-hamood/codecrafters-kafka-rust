use crate::{
    byte_parsable::ByteParsable,
    serializable::Serializable,
    size::Size,
    tagged_fields_section::TaggedFieldsSection,
    types::{
        compact_array::CompactArray, compact_nullable_string::CompactNullableString,
        compact_records::CompactRecords, compact_string::CompactString,
    },
};

// https://kafka.apache.org/41/design/protocol/#The_Messages_Produce

/// Produce Request (Version: 11) => transactional_id acks timeout_ms [topic_data] _tagged_fields
///   transactional_id => COMPACT_NULLABLE_STRING
///   acks => INT16
///   timeout_ms => INT32
///   topic_data => name [partition_data] _tagged_fields
///     name => COMPACT_STRING
///     partition_data => index records _tagged_fields
///       index => INT32
///       records => COMPACT_RECORDS
#[derive(Debug, Clone)]
pub struct ProduceRequestV11 {
    transactional_id: CompactNullableString,
    acks: i16,
    timeout_ms: i32,
    pub topic_data: CompactArray<Topic>,
    _tagged_fields: TaggedFieldsSection,
}

impl Size for ProduceRequestV11 {
    fn size(&self) -> usize {
        self.transactional_id.size()
            + self.acks.size()
            + self.timeout_ms.size()
            + self.topic_data.size()
            + self._tagged_fields.size()
    }
}

impl ByteParsable<ProduceRequestV11> for ProduceRequestV11 {
    fn parse(bytes: &[u8], offset: usize) -> ProduceRequestV11 {
        let mut offset = offset;
        let transactional_id = CompactNullableString::parse(bytes, offset);
        offset += transactional_id.size();
        let acks = i16::parse(bytes, offset);
        offset += acks.size();
        let timeout_ms = i32::parse(bytes, offset);
        offset += timeout_ms.size();
        let topic_data = CompactArray::<Topic>::parse(bytes, offset);
        offset += topic_data.size();
        let _tagged_fields = TaggedFieldsSection::parse(bytes, offset);
        Self {
            transactional_id,
            acks,
            timeout_ms,
            topic_data,
            _tagged_fields,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Topic {
    pub name: CompactString,
    pub partition_data: CompactArray<Partition>,
    _tagged_fields: TaggedFieldsSection,
}

impl Size for Topic {
    fn size(&self) -> usize {
        self.name.size() + self.partition_data.size() + self._tagged_fields.size()
    }
}

impl ByteParsable<Topic> for Topic {
    fn parse(bytes: &[u8], offset: usize) -> Topic {
        let mut offset = offset;
        let name = CompactString::parse(bytes, offset);
        offset += name.size();
        let partition_data = CompactArray::<Partition>::parse(bytes, offset);
        offset += partition_data.size();
        let _tagged_fields = TaggedFieldsSection::parse(bytes, offset);
        Self {
            name,
            partition_data,
            _tagged_fields,
        }
    }
}

impl Serializable for Topic {}

#[derive(Debug, Clone)]
pub struct Partition {
    pub index: i32,
    records: CompactRecords,
    _tagged_fields: TaggedFieldsSection,
}

impl Size for Partition {
    fn size(&self) -> usize {
        self.index.size() + self.records.size() + self._tagged_fields.size()
    }
}

impl ByteParsable<Partition> for Partition {
    fn parse(bytes: &[u8], offset: usize) -> Self {
        let mut offset = offset;
        let index = i32::parse(bytes, offset);
        offset += index.size();
        let records = CompactRecords::parse(bytes, offset);
        offset += records.size();
        let _tagged_fields = TaggedFieldsSection::parse(bytes, offset);
        Self {
            index,
            records,
            _tagged_fields,
        }
    }
}

impl Serializable for Partition {}

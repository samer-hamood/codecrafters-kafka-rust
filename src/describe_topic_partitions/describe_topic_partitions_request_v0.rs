use crate::{
    byte_parsable::ByteParsable,
    serializable::Serializable,
    size::Size,
    tagged_fields_section::TaggedFieldsSection,
    types::{compact_array::CompactArray, compact_string::CompactString},
};

// https://kafka.apache.org/41/design/protocol/#The_Messages_DescribeTopicPartitions

/// DescribeTopicPartitions Request (Version: 0) => [topics] response_partition_limit cursor _tagged_fields
///   topics => name _tagged_fields
///     name => COMPACT_STRING
///   response_partition_limit => INT32
///   cursor => topic_name partition_index _tagged_fields
///     topic_name => COMPACT_STRING
///     partition_index => INT32
#[derive(Debug, Clone)]
pub struct DescribeTopicPartitionsRequestV0 {
    pub topics: CompactArray<Topic>,
    pub response_partition_limit: i32,
    pub cursor: Cursor,
    pub _tagged_fields: TaggedFieldsSection,
}

impl Size for DescribeTopicPartitionsRequestV0 {
    fn size(&self) -> usize {
        self.topics.size()
            + self.response_partition_limit.size()
            + self.cursor.size()
            + self._tagged_fields.size()
    }
}

impl ByteParsable<DescribeTopicPartitionsRequestV0> for DescribeTopicPartitionsRequestV0 {
    fn parse(bytes: &[u8], offset: usize) -> DescribeTopicPartitionsRequestV0 {
        let mut offset: usize = offset;
        let topics = CompactArray::<Topic>::parse(bytes, offset);
        offset += topics.size();
        let response_partition_limit = i32::parse(bytes, offset);
        offset += response_partition_limit.size();
        let cursor = Cursor::parse(bytes, offset);
        offset += cursor.size();
        let _tagged_fields = TaggedFieldsSection::parse(bytes, offset);
        Self {
            topics,
            response_partition_limit,
            cursor,
            _tagged_fields,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Topic {
    pub name: CompactString,
    pub _tagged_fields: TaggedFieldsSection,
}

impl Size for Topic {
    fn size(&self) -> usize {
        self.name.size() + self._tagged_fields.size()
    }
}

impl ByteParsable<Topic> for Topic {
    fn parse(bytes: &[u8], offset: usize) -> Self {
        let mut offset: usize = offset;
        let name = CompactString::parse(bytes, offset);
        offset += name.size();
        let _tagged_fields = TaggedFieldsSection::parse(bytes, offset);
        Self {
            name,
            _tagged_fields,
        }
    }
}

impl Serializable for Topic {}

#[derive(Debug, Clone)]
pub struct Cursor {
    pub topic_name: CompactString,
    pub partition_index: i32,
    pub _tagged_fields: TaggedFieldsSection,
}

impl Size for Cursor {
    fn size(&self) -> usize {
        self.topic_name.size() + self.partition_index.size() + self._tagged_fields.size()
    }
}

impl ByteParsable<Cursor> for Cursor {
    fn parse(bytes: &[u8], offset: usize) -> Self {
        let mut offset: usize = offset;
        let topic_name = CompactString::parse(bytes, offset);
        offset += topic_name.size();
        let partition_index = i32::parse(bytes, offset);
        offset += partition_index.size();
        let _tagged_fields = TaggedFieldsSection::parse(bytes, offset);
        Self {
            topic_name,
            partition_index,
            _tagged_fields,
        }
    }
}

use std::any::Any;

use crate::byte_parsable::ByteParsable;
use crate::fetch::partition::{Partition, RequestPartition};
use crate::fetch::topic::{self, ForgottenTopicsDatum, RequestTopic};
use crate::headers::request_header_v2::RequestHeaderV2;
use crate::size::Size;
use crate::tagged_fields_section::TaggedFieldsSection;
use crate::types::compact_array::CompactArray;
use crate::types::compact_string::CompactString;

/// Fetch Request (Version: 16) => max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id _tagged_fields
///   max_wait_ms => INT32
///   min_bytes => INT32
///   max_bytes => INT32
///   isolation_level => INT8
///   session_id => INT32
///   session_epoch => INT32
///   topics => topic_id [partitions] _tagged_fields
///     topic_id => UUID
///     partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes _tagged_fields
///       partition => INT32
///       current_leader_epoch => INT32
///       fetch_offset => INT64
///       last_fetched_epoch => INT32
///       log_start_offset => INT64
///       partition_max_bytes => INT32
///   forgotten_topics_data => topic_id [partitions] _tagged_fields
///     topic_id => UUID
///     partitions => INT32
///   rack_id => COMPACT_STRING
#[derive(Debug)]
pub struct FetchRequestV16 {
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub session_id: i32,
    pub session_epoch: i32,
    pub topics: CompactArray<RequestTopic>,
    pub forgotten_topics_data: CompactArray<ForgottenTopicsDatum>,
    pub rack_id: CompactString,
    pub _tagged_fields: TaggedFieldsSection,
}

impl Size for FetchRequestV16 {
    fn size(&self) -> usize {
        self.max_wait_ms.size()
            + self.min_bytes.size()
            + self.max_bytes.size()
            + self.isolation_level.size()
            + self.session_id.size()
            + self.session_epoch.size()
            + self.topics.size()
            + self.forgotten_topics_data.size()
            + self.rack_id.size()
            + self._tagged_fields.size()
    }
}

impl ByteParsable<FetchRequestV16> for FetchRequestV16 {
    fn parse(bytes: &[u8], offset: usize) -> Self {
        let mut offset: usize = offset;
        let max_wait_ms = i32::parse(bytes, offset);
        offset += max_wait_ms.size();
        let min_bytes = i32::parse(bytes, offset);
        offset += min_bytes.size();
        let max_bytes = i32::parse(bytes, offset);
        offset += max_bytes.size();
        let isolation_level = i8::parse(bytes, offset);
        offset += isolation_level.size();
        let session_id = i32::parse(bytes, offset);
        offset += session_id.size();
        let session_epoch = i32::parse(bytes, offset);
        offset += session_epoch.size();
        let topics = CompactArray::<RequestTopic>::parse(bytes, offset);
        offset += topics.size();
        let forgotten_topics_data = CompactArray::<ForgottenTopicsDatum>::parse(bytes, offset);
        offset += forgotten_topics_data.size();
        let rack_id = CompactString::parse(bytes, offset);
        offset += rack_id.size();
        let _tagged_fields = TaggedFieldsSection::parse(bytes, offset);

        Self {
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data,
            rack_id,
            _tagged_fields,
        }
    }
}

#[cfg(test)]
mod test {
    use uuid::Uuid;

    use crate::{
        byte_parsable::ByteParsable,
        fetch::fetch_request_v16::{self, FetchRequestV16},
        tagged_fields_section::TaggedFieldsSection,
    };

    #[test]
    fn parses_fetch_request() {
        let bytes: &[u8] = &[
            0x00, 0x00, 0x00, 0x60, // message_size
            0x00, 0x01, // request_api_key
            0x00, 0x10, // request_api_version: 16
            0x1b, 0x84, 0x59, 0x19, // correlation_id
            0x00, 0x09, // client_id (length): 9
            0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69, // client_id (content)
            0x00, // _tagged_fields
            0x00, 0x00, 0x01, 0xf4, // max_wait_ms: 500
            0x00, 0x00, 0x00, 0x01, // min_bytes: 1
            0x03, 0x20, 0x00, 0x00, // max_bytes: 52428800
            0x00, // isolation_level: 0
            0x00, 0x00, 0x00, 0x00, // session_id: 0
            0x00, 0x00, 0x00, 0x00, // session_epoch: 0
            0x02, // topics (length: 1 + N): 2
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x60, 0x70, // UUID (16 bytes): 24688
            0x02, // partitions (length: 1 + N): 2
            0x00, 0x00, 0x00, 0x00, // partition (id): 0
            0xff, 0xff, 0xff, 0xff, // current_leader_epoch: -1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // fetch_offset: 0
            0xff, 0xff, 0xff, 0xff, // last_fetched_epoch: -1
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // log_start_offset: -1
            0x00, 0x10, 0x00, 0x00, // partition_max_bytes: 1048576
            0x00, // _tagged_fields
            0x00, // _tagged_fields
            0x01, // forgotten_topics_data (length: 1 + N): 1
            0x01, // rack_id (length): 1
            0x00, // _tagged_fields
        ];

        let fetch_request = FetchRequestV16::parse(bytes, 24);

        assert_eq!(500, fetch_request.max_wait_ms);
        assert_eq!(1, fetch_request.min_bytes);
        assert_eq!(52428800, fetch_request.max_bytes);
        assert_eq!(0, fetch_request.isolation_level);
        assert_eq!(Uuid::from_u128(24688), fetch_request.topics[0].topic_id);
        assert_eq!(0, fetch_request.topics[0].partitions[0].partition);
        assert_eq!(
            -1,
            fetch_request.topics[0].partitions[0].current_leader_epoch
        );
        assert_eq!(0, fetch_request.topics[0].partitions[0].fetch_offset);
        assert_eq!(-1, fetch_request.topics[0].partitions[0].last_fetched_epoch);
        assert_eq!(-1, fetch_request.topics[0].partitions[0].log_start_offset);
        assert_eq!(
            1048576,
            fetch_request.topics[0].partitions[0].partition_max_bytes
        );
        assert_eq!(
            TaggedFieldsSection::empty(),
            fetch_request.topics[0].partitions[0]._tagged_fields
        );
        assert_eq!(
            TaggedFieldsSection::empty(),
            fetch_request.topics[0]._tagged_fields
        );
        assert_eq!(TaggedFieldsSection::empty(), fetch_request._tagged_fields);
    }
}

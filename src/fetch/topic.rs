use std::{u128, usize};

use uuid::Uuid;

use super::partition::{RequestPartition, ResponsePartition};
use crate::byte_parsable::ByteParsable;
use crate::compact_array::{CompactArray, CompactArrayElementI32};
use crate::tagged_fields_section::{self, TaggedFieldsSection};
use crate::serializable::{BoxedSerializable, Serializable};
use crate::size::Size;

#[allow(dead_code)]
const UUID: usize = size_of::<Uuid>();

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct RequestTopic {
    pub topic_id: Uuid, // v4 128 bits (16 bytes) UUID
    pub partitions: CompactArray<RequestPartition>,
    pub _tagged_fields: TaggedFieldsSection
}

impl Size for RequestTopic {

    fn size(&self) -> usize {
        // size_of::<Uuid>() + 
        self.topic_id.size() +
            self.partitions.size() + 
            self._tagged_fields.size()
    }

}


impl ByteParsable<RequestTopic> for RequestTopic {

    fn parse(bytes: &[u8], offset: usize) -> Self {
        let mut offset = offset;
        // let topic_id = Uuid::from_bytes(bytes[offset..offset + UUID].try_into().unwrap());
        let topic_id = Uuid::parse(bytes, offset);
        offset += topic_id.size(); 
        let partitions = CompactArray::<RequestPartition>::parse(bytes, offset);
        offset += partitions.size();
        let _tagged_fields = TaggedFieldsSection::parse(bytes, offset);
        Self {
            topic_id,
            partitions,
            _tagged_fields,
        }
    }

}

impl Serializable for RequestTopic {

    fn to_be_bytes(&self) -> Vec<u8> {
        todo!() 
    }

}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ResponseTopic {
    topic_id: Uuid, // v4 128 bits (16 bytes) UUID
    partitions: CompactArray<ResponsePartition>,
    _tagged_fields: TaggedFieldsSection
}

impl ResponseTopic {

    pub fn new(topic_id: Uuid, partitions: Vec<ResponsePartition>, _tagged_fields: TaggedFieldsSection) -> Self {
        Self {
            topic_id,
            partitions: CompactArray::new(partitions),
            _tagged_fields,
        }
    }

}

impl Size for ResponseTopic {

    fn size(&self) -> usize {
        // size_of::<Uuid>() + 
        self.topic_id.size() + self.partitions.size() + self._tagged_fields.size()
    }

}

impl Serializable for ResponseTopic {

    fn serializable_fields(&self) -> Vec<BoxedSerializable> {
        let mut fields: Vec<BoxedSerializable> = Vec::with_capacity(3);
        fields.push(Box::new(self.topic_id));
        fields.push(Box::new(self.partitions.clone()));
        fields.push(Box::new(self._tagged_fields.clone()));
        fields
    }

}

impl Serializable for Uuid {

    fn to_be_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

}

impl ByteParsable<ResponseTopic> for ResponseTopic {

    fn parse(_bytes: &[u8], _offset: usize) -> ResponseTopic {
       todo!() 
    }

}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ForgottenTopicsDatum {
    topic_id: Uuid, // v4 128 bits (16 bytes) UUID
    partitions: CompactArray<CompactArrayElementI32>,
    _tagged_fields: TaggedFieldsSection
}

impl Size for ForgottenTopicsDatum {

    fn size(&self) -> usize {
        // size_of::<Uuid>() 
        self.topic_id.size() + self.partitions.size() + self._tagged_fields.size()
    }

}

impl ByteParsable<ForgottenTopicsDatum> for ForgottenTopicsDatum {

    fn parse(bytes: &[u8], offset: usize) -> ForgottenTopicsDatum {
        let mut offset = offset;
        // let topic_id = Uuid::from_bytes(bytes[offset..offset + UUID].try_into().unwrap());
        // offset += UUID; 
        let topic_id = Uuid::parse(bytes, offset);
        offset += topic_id.size(); 
        let partitions = CompactArray::<CompactArrayElementI32>::parse(bytes, offset);
        offset += partitions.size();
        let _tagged_fields = TaggedFieldsSection::parse(bytes, offset);
        Self {
            topic_id,
            partitions,
            _tagged_fields,
        }
    }

}

impl Serializable for ForgottenTopicsDatum {

    fn to_be_bytes(&self) -> Vec<u8> {
        todo!()
    }

}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn computes_message_size() {
        let expected_size = 16 + (1 + 0) + 1;

        let topic = 
            ResponseTopic::new(
                Uuid::new_v4(),                 // 16 bytes
                Vec::new(),                     // 1 byte
                TaggedFieldsSection::empty(),   // 1 byte
            );

        assert_eq!(expected_size, topic.size());
    }

}


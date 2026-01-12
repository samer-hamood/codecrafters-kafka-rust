use crate::byte_parsable::ByteParsable;
use crate::size::Size;
use crate::types::unsigned_varint::UnsignedVarint;

// https://kafka.apache.org/27/protocol.html#protocol_types

#[allow(dead_code)]
#[derive(Debug)]
pub struct CompactString {
    pub length: UnsignedVarint,
    pub bytes: Option<Vec<u8>>,
}

impl Size for CompactString {
    fn size(&self) -> usize {
        self.length.size() + self.bytes.size()
    }
}

impl ByteParsable<CompactString> for CompactString {
    fn parse(bytes: &[u8], offset: usize) -> CompactString {
        let mut offset = offset;
        let length = UnsignedVarint::parse(bytes, offset);
        offset += length.size();

        let bytes = match length.value {
            0 => None,
            1 => Some(Vec::new()),
            _ => Some(bytes[offset..offset + (length.value - 1) as usize].into()),
        };

        Self { length, bytes }
    }
}

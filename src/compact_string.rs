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

#[allow(dead_code)]
impl CompactString {
    fn number_of_bytes(&self) -> usize {
        self.bytes.as_ref().map(|v| v.len()).unwrap_or(0)
    }
}

impl Size for CompactString {
    fn size(&self) -> usize {
        self.length.byte_count + self.number_of_bytes()
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

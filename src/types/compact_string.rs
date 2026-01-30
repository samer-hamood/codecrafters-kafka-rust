use std::fmt::{Debug, Display};

use crate::byte_parsable::ByteParsable;
use crate::serializable::Serializable;
use crate::size::Size;
use crate::types::unsigned_varint::UnsignedVarint;

// https://kafka.apache.org/27/protocol.html#protocol_types

#[derive(Clone, Debug)]
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

impl Serializable for CompactString {
    fn to_be_bytes(&self) -> Vec<u8> {
        match &self.bytes {
            Some(b) => {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&self.length.to_be_bytes());
                bytes.extend_from_slice(&b.to_vec());
                bytes
            }
            None => Vec::new(),
        }
    }
}

impl Display for CompactString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(bytes) = &self.bytes {
            match str::from_utf8(bytes) {
                Ok(s) => write!(f, "{}", s),
                Err(e) => panic!("Invalid UTF-8: {}", e),
            }
        } else {
            panic!("No bytes to display CompactString")
        }
    }
}

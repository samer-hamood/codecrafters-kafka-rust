use crate::byte_parsable::ByteParsable;
use crate::serializable::Serializable;
use crate::size::Size;

// https://kafka.apache.org/27/protocol.html#protocol_types

pub const LENGTH: usize = 2;

/// Represents a sequence of characters or null.
/// For non-null strings, first the length N is given as an INT16.
/// Then N bytes follow which are the UTF-8 encoding of the character sequence.
/// A null value is encoded with length of -1 and there are no following bytes.
#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub struct NullableString {
    bytes: Option<Vec<u8>>,
}

#[allow(dead_code)]
impl NullableString {
    pub fn null() -> Self {
        Self { bytes: None }
    }

    pub fn from(string: &str) -> Self {
        Self {
            bytes: Some(string.as_bytes().to_vec()),
        }
    }
}

impl Size for NullableString {
    fn size(&self) -> usize {
        size_of::<i16>() + self.bytes.as_ref().map_or(0, |string| string.len())
    }
}

impl ByteParsable<NullableString> for NullableString {
    fn parse(bytes: &[u8], offset: usize) -> Self {
        let mut offset = offset;
        let length = i16::parse(bytes, offset);
        let bytes = if length == -1 {
            None
        } else {
            offset += length.size();
            Some(bytes[offset..offset + length as usize].to_vec())
        };
        Self { bytes }
    }
}

use std::usize;

use crate::byte_parsable::ByteParsable;
use crate::size::Size;
use crate::serializable::Serializable;

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
        Self { bytes: Some(string.as_bytes().to_vec()) }
    } 

}

impl Size for NullableString {

    fn size(&self) -> usize {
        size_of::<i16>() + self.bytes.as_ref().map_or(0, |v| v.len())
    }

}

impl ByteParsable<NullableString> for NullableString {

    fn parse(bytes: &[u8], offset: usize) -> Self {
        let mut offset = offset;
        let length = i16::from_be_bytes(bytes[offset..offset + LENGTH].try_into().unwrap());
        return if length == -1 {
            NullableString::null()
        } else {
            offset += LENGTH;
            let utf8_bytes = bytes[offset..offset + (length as usize)].try_into().unwrap();
            Self { bytes: Some(utf8_bytes) }
        };
    }

}


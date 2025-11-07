use std::usize;

use crate::byte_parsable::ByteParsable;
use crate::serializable::Serializable;
use crate::size::Size;

#[allow(dead_code)]
const LENGTH: usize = 1;

#[allow(dead_code)]
#[derive(Debug)]
pub struct CompactString {
    bytes: Vec<u8>,
}

#[allow(dead_code)]
impl CompactString {
    pub fn new(string: &str) -> Self {
        Self {
            bytes: string.as_bytes().to_vec(),
        }
    }

    fn emtpy() -> Self {
        Self { bytes: Vec::new() }
    }

    pub fn len(&self) -> u8 {
        // pub fn len(&self) -> i32 {
        (1 + self.bytes.len()).try_into().unwrap()
    }
}

impl Size for CompactString {
    fn size(&self) -> usize {
        // TODO: Compute unsigned varint size of length: encode_unsigned_varint(1 + self.elements.len()).len()
        let length_size = size_of::<u8>();
        length_size + self.bytes.len()
    }
}

impl Serializable for CompactString {
    fn to_be_bytes(&self) -> Vec<u8> {
        let array_length_bytes = self.len().to_be_bytes();
        let mut bytes: Vec<u8> = Vec::with_capacity(array_length_bytes.len() + self.bytes.len());
        bytes.extend_from_slice(&self.len().to_be_bytes());
        bytes.extend_from_slice(&self.bytes);
        bytes
    }
}

impl ByteParsable<CompactString> for CompactString {
    fn parse(bytes: &[u8], offset: usize) -> CompactString {
        let mut offset = offset;
        let length = u8::from_be_bytes(bytes[offset..offset + LENGTH].try_into().unwrap());
        offset += LENGTH;

        let bytes = if length == 0 {
            Vec::new()
        } else if length == 1 {
            Vec::new()
        } else {
            bytes[offset..offset + (length - 1) as usize].to_vec()
        };

        Self { bytes }
    }
}

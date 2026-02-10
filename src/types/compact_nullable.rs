use crate::{byte_parsable::ByteParsable, size::Size, types::unsigned_varint::UnsignedVarint};

pub trait CompactNullable<CN: ByteParsable<CN>> {
    fn parse_length_and_bytes(bytes: &[u8], offset: usize) -> (UnsignedVarint, Option<Vec<u8>>) {
        let mut offset = offset;
        let length = UnsignedVarint::parse(bytes, offset);
        offset += length.size();
        let bytes = match length.value {
            0 => None,
            1 => Some(Vec::new()),
            _ => Some(bytes[offset..offset + (length.value - 1) as usize].into()),
        };
        (length, bytes)
    }
}

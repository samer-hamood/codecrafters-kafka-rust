use std::any::type_name;

use crate::serializable::Serializable;
use crate::types::variable_integer::serialize;
use crate::{
    byte_parsable::ByteParsable,
    size::Size,
    types::{variable_integer::parse, zig_zag_decoder::ZigZagDecoder},
};

#[derive(Clone, Debug)]
pub struct UnsignedVarint {
    pub value: u32,
    pub byte_count: usize,
}

impl Size for UnsignedVarint {
    fn size(&self) -> usize {
        self.byte_count
    }
}

impl ByteParsable<UnsignedVarint> for UnsignedVarint {
    fn parse(bytes: &[u8], offset: usize) -> Self {
        let (value, byte_count) = parse(bytes, offset);
        let value = value.try_into().unwrap_or_else(|_| {
            panic!(
                "Invalid return type: expected {} but was out of range {}",
                type_name::<u32>(),
                value
            )
        });
        Self { value, byte_count }
    }
}

impl Serializable for UnsignedVarint {
    fn to_be_bytes(&self) -> Vec<u8> {
        serialize(self.value)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use parameterized::parameterized;
    use rstest::rstest;

    #[rstest]
    #[case(&[0x00], 0)]
    #[case(&[0x96, 0x01], 150)]
    fn parses_varint_encoded_bytes(#[case] bytes: &[u8], #[case] expected: u32) {
        // 150, encoded as `9601`
        // 10010110 00000001        // Original inputs.
        // 0010110  0000001         // Drop continuation bits.
        // 0000001  0010110         // Convert to big-endian.
        // 00000010010110           // Concatenate.
        // 128 + 16 + 4 + 2 = 150   // Interpret as an unsigned 64-bit integer.

        let varint = UnsignedVarint::parse(bytes, 0);

        assert_eq!(varint.value, expected);
        assert_eq!(varint.byte_count, bytes.len());
    }
}

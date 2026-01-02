use crate::{
    byte_parsable::ByteParsable,
    size::Size,
    types::{variable_integer::parse, zig_zag_decoder::ZigZagDecoder},
};

#[derive(Clone, Debug)]
pub struct SignedVarint {
    pub value: i32,
    pub byte_count: usize,
}

impl ZigZagDecoder for SignedVarint {
    type Int = i32;
}

impl Size for SignedVarint {
    fn size(&self) -> usize {
        self.byte_count
    }
}

impl ByteParsable<SignedVarint> for SignedVarint {
    fn parse(bytes: &[u8], offset: usize) -> Self {
        let (value, byte_count) = parse(bytes, offset);
        let value = SignedVarint::zig_zag_decode(value);
        Self { value, byte_count }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use parameterized::parameterized;

    #[test]
    fn parses_varint_encoded_bytes() {
        // 150, encoded as `9601`
        // 10010110 00000001        // Original inputs.
        // 0010110  0000001         // Drop continuation bits.
        // 0000001  0010110         // Convert to big-endian.
        // 00000010010110           // Concatenate.
        // 128 + 16 + 4 + 2 = 150   // Interpret as an unsigned 64-bit integer.
        let varint_encoded_bytes: [u8; 2] = [0x96, 0x01];
        let expected_parsed_value = SignedVarint::zig_zag_decode(150u64);

        let varint = SignedVarint::parse(&varint_encoded_bytes, 0);

        assert_eq!(varint.value, expected_parsed_value);
        assert_eq!(varint.byte_count, varint_encoded_bytes.len());
    }

    #[parameterized(
        input = {
            0, 1, 2, 3, 0xfffffffe, 0xffffffff, 0x3c, 0x30,
        },
        expected = {
            0, -1, 1, -2, 0x7fffffff, -0x80000000, 30, 24,
        }
    )]
    fn zig_zag_decodes(input: u64, expected: i32) {
        let res = SignedVarint::zig_zag_decode(input);

        assert_eq!(res, expected);
    }

    #[test]
    #[should_panic = "Invalid return type: expected i32"]
    fn panics_on_zig_zag_decoding_overflow() {
        let input: u64 = 0xffffffffff;
        let _ = SignedVarint::zig_zag_decode(input);
    }
}

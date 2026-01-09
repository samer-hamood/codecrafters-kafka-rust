use itertools::join;
use tracing::{debug, field, trace};
use tracing_subscriber::field::debug;

use crate::{lazy_debug, lazy_trace};

pub fn parse(varint_encoded_bytes: &[u8], offset: usize) -> (u64, usize) {
    // https://protobuf.dev/programming-guides/encoding/#varints
    let mut value = 0u64;
    let mut i = offset;
    let mut shift = 0;
    let mut continuation_bit_set = true;
    let mut byte_count: usize = 0;
    // varint_encoded_bytes should come in little-endian order
    lazy_debug!(
        "bytes: {}\n",
        join(
            varint_encoded_bytes
                .iter()
                .map(|byte| format!("{:08b}", byte)),
            " "
        )
    );
    while continuation_bit_set {
        debug!("bytes[{i}]: {:08b}", varint_encoded_bytes[i]);
        let continuation_bit = (varint_encoded_bytes[i] >> 7) & 0x01;
        continuation_bit_set = continuation_bit == 1;
        trace!(
            "continuation bit: {continuation_bit}, continuation_bit_set: {continuation_bit_set}"
        );
        let byte_with_8th_bit_cleared = varint_encoded_bytes[i] & 0x7F;
        assert!(get_bit_value(byte_with_8th_bit_cleared, 7) == 0);
        debug!("Drop continuation bit: {:07b}", byte_with_8th_bit_cleared);
        // Concatenate bytes in opposite order (big-endian)
        value |= (byte_with_8th_bit_cleared << shift) as u64;
        trace!("concatenated value: {:b}", value);
        byte_count += 1;
        if continuation_bit_set {
            i += 1;
            shift += 7;
        }
        debug!("");
    }
    debug!(
        "Concatenated: {:0width$b} ({})",
        value,
        value,
        width = byte_count * 7
    );
    (value, byte_count)
}

pub fn serialize(number: u32) -> Vec<u8> {
    // https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=120722234#KIP482:TheKafkaProtocolshouldSupportOptionalTaggedFields-UnsignedVarints
    // 1. Break up number into groups of seven bits
    // 2. Set high bit (bit 8) of the group if it's NOT the last one and clear bit if it is the last group

    debug!("Number: {} ({:b})", number, number);

    let mut bytes = Vec::new();

    const GROUP: u8 = 7u8;
    let mut byte_index = 0u8;
    // let mut serialized_number = 0u128;
    let mut remaining_bits = number;
    let mut continuation_bit_needed = true;
    while continuation_bit_needed {
        // let mut current_byte = remaining_bits & 0x00_00_00_00_00_00_00_7F;
        let mut byte = (remaining_bits & 0x00_00_00_7F) as u8; // takes lowest seven bits
        assert!(byte < 255);
        debug!(
            "Lowest 7 bits from remaining (before continuation bit added): {:07b}",
            byte
        );

        let shift = GROUP + byte_index;
        let byte_shifted = remaining_bits >> shift;
        trace!(
            "Remaining bits shifted by {} ({} + {}): {:08b}",
            shift,
            GROUP,
            byte_index,
            byte_shifted
        );
        continuation_bit_needed = byte_shifted != 0;
        if continuation_bit_needed {
            byte |= 0x80; // sets eighth bit to 1 while the rest are unchanged
            byte_index += 1;
            remaining_bits = number >> (GROUP * byte_index);
        } else {
            byte &= 0x7F; // sets eighth bit to 0 while the rest are unchanged
        }
        debug!("Add continuation bit: {:08b}", byte);

        bytes.push(byte);
    }

    lazy_debug!(
        "Serialized: {}\n",
        join(bytes.iter().map(|byte| format!("{:08b}", byte)), " ")
    );
    bytes
}


#[cfg(test)]
mod test {
    use super::*;
    use rstest::rstest;
    use serial_test::serial;

    #[test_log::test]
    fn parses_varint_encoded_bytes() {
        // 150, encoded as `9601`
        // 10010110 00000001        // Original inputs.
        // 0010110  0000001         // Drop continuation bits.
        // 0000001  0010110         // Convert to big-endian.
        // 00000010010110           // Concatenate.
        // 128 + 16 + 4 + 2 = 150   // Interpret as an unsigned 64-bit integer.
        let varint_encoded_bytes: [u8; 2] = [0x96, 0x01];
        let expected_parsed_value = 150u64;

        let (value, byte_count) = parse(&varint_encoded_bytes, 0);

        assert_eq!(value, expected_parsed_value);
        assert_eq!(byte_count, varint_encoded_bytes.len());
    }

    #[test_log::test]
    #[rstest]
    #[case(0, &[0x00])]
    #[case(300, &[0xAC, 0x02])]
    #[serial]
    fn serializes_to_varint_encoded_bytes(#[case] number: u32, #[case] expected: &[u8]) {
        let serialized = serialize(number);
        assert_eq!(serialized, expected);
    }
}

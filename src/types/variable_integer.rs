pub fn parse(varint_encoded_bytes: &[u8], offset: usize) -> (u64, usize) {
    // https://protobuf.dev/programming-guides/encoding/#varints
    let mut value = 0u64;
    let mut i = offset;
    let mut shift = 0;
    let mut continuation_bit_found = true;
    let mut byte_count: usize = 0;
    while continuation_bit_found {
        // println!("bytes[{i}]: {:08b}", varint_encoded_bytes[i]);
        continuation_bit_found = ((varint_encoded_bytes[i] >> 7) & 0x01) == 1;
        // println!("continuation bit found? {continuation_bit_found}");
        // println!(
        //     "continuation bit value: {}",
        //     ((varint_encoded_bytes[i] >> 7) & 0x01)
        // );
        // println!(
        //     "bytes[{i}] without continuation bit: {:08b}",
        //     (varint_encoded_bytes[i] & 0x7F)
        // );
        value |= ((varint_encoded_bytes[i] & 0x7F) << shift) as u64;
        // println!("value: {:b}\n", value);
        byte_count += 1;
        if continuation_bit_found {
            i += 1;
            shift += 7;
        }
    }
    // println!("value: {value}");
    // println!("value in binary: {:b}", value);
    (value, byte_count)
}

// pub fn serialize(number: u32) -> Int {
pub fn serialize(number: u32) -> Vec<u8> {
    // https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=120722234#KIP482:TheKafkaProtocolshouldSupportOptionalTaggedFields-UnsignedVarints
    // 1. Break up number into groups of seven bits
    // 2. Set high bit (bit 8) of the group if it's NOT the last one and clear bit if it is the last group
    println!("number to serialize: {:08b}", number);

    const GROUP: u8 = 7u8;

    let mut bytes = Vec::new();

    let mut byte_count = 0u8;
    // let mut serialized_number = 0u128;
    let mut continuation_bit_needed = true;
    while continuation_bit_needed {
        let remaining_bits = number >> (GROUP * byte_count);
        println!("remaining_bits: {:08b}", remaining_bits);

        // let mut current_byte = remaining_bits & 0x00_00_00_00_00_00_00_7F;
        let mut current_byte = (remaining_bits & 0x00_00_00_7F) as u8; // takes lowest seven bits
        println!(
            "current byte before continuation bit added: {:08b}",
            current_byte
        );

        continuation_bit_needed = (remaining_bits >> (GROUP + byte_count + 1)) != 0;

        if continuation_bit_needed {
            current_byte |= 0x80; // sets eighth bit to 1 while the rest are unchanged
            byte_count += 1;
        } else {
            current_byte &= 0x7F; // sets eighth bit to 0 while the rest are unchanged
        }
        println!(
            "current byte after continuation bit added: {:08b}",
            current_byte
        );

        bytes.push(current_byte);
    }

    bytes
    // if serialized_number <= u8::MAX as u64 {
    //     Int::U8(serialized_number as u8)
    // } else if serialized_number <= u16::MAX as u64 {
    //     Int::U16(serialized_number as u16)
    // } else if serialized_number <= u32::MAX as u64 {
    //     Int::U32(serialized_number as u32)
    // } else if serialized_number <= u64::MAX {
    //     Int::U64(serialized_number)
    // }
}

// pub enum Int {
//     U8(u8),
//     U16(u16),
//     U32(u32),
//     U64(u64),
// }

#[cfg(test)]
mod test {
    use super::*;
    use rstest::rstest;

    #[test]
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

    #[rstest]
    #[case(0, &[0x00])]
    #[case(300, &[0xAC, 0x02])]
    fn serializes_to_varint_encoded_bytes(#[case] number: u32, #[case] expected: &[u8]) {
        let serialized = serialize(number);
        assert_eq!(serialized, expected);
    }
}

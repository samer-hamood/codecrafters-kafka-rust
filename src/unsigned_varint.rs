

// TODO: Test this
#[allow(dead_code)]
fn encode_unsigned_varint(mut n: u32) -> Vec<u8> {
    let mut buf = Vec::new();
    loop {
        let mut byte = (n & 0x7F) as u8; // take 7 bits
        n >>= 7;
        if n != 0 {
            // still have more bits, set continuation bit
            byte |= 0x80;
        }
        buf.push(byte);
        if n == 0 {
            break;
        }
    }
    buf
}


mod test {
    use super::*;


    #[test]
    fn converts_to_unsigned_varint() {
        let expected_bytes = vec![0x01];
        let actual_bytes = encode_unsigned_varint(1);

        assert_eq!(1, actual_bytes.len());
        assert_eq!(expected_bytes, actual_bytes);
    }


}


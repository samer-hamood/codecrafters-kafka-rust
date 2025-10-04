use std::i32;
use std::slice::Iter;
use crate::serializable::Serializable;
use crate::size::Size;

#[derive(Debug)]
pub struct CompactArray<T: Serializable + Size> {
    elements: Vec<T>,
}

#[allow(dead_code)]
impl <T:Serializable + Size> CompactArray<T> {

    pub fn new(elements: Vec<T>) -> CompactArray<T> {
        CompactArray {
            elements: elements,
        }
    }

    pub fn len(&self) -> u8 {
    // pub fn len(&self) -> i32 {
        (1 + self.elements.len()).try_into().unwrap()
    }

    pub fn iter(&self) -> Iter<'_, T> {
        self.elements.iter()
    }

}

impl <T:Serializable + Size> Serializable for CompactArray<T> {

    fn to_be_bytes(&self) -> Vec<u8> {
        let array_length_bytes = self.len().to_be_bytes();
        let mut bytes: Vec<u8> = Vec::new();
        for i in 0..array_length_bytes.len() {
            bytes.push(array_length_bytes[i]);
        }
        for i in 0..self.elements.len() {
            let elements_bytes = self.elements[i].to_be_bytes();
            for j in 0..elements_bytes.len() {
                bytes.push(elements_bytes[j]);
            }
        }
        bytes
    }
    
}

impl <T:Serializable + Size> Size for CompactArray<T> {

    fn size(&self) -> i32 {
        // TODO: Compute unsigned varint size of length: encode_unsigned_varint(1 + self.elements.len()).len()
        <usize as TryInto<i32>>::try_into(size_of::<u8>()).unwrap() + self.elements.iter().map(|element| element.size()).sum::<i32>()
    }

}

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


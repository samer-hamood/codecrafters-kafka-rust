use crate::size::Size;

#[allow(dead_code)]
pub struct ResponseHeaderV0 {
    pub correlation_id: i32,
}

#[allow(dead_code)]
impl ResponseHeaderV0 {
    fn new(correlation_id: i32) -> ResponseHeaderV0 {
        ResponseHeaderV0 {
            correlation_id: correlation_id,
        }
    }

    pub fn to_be_bytes(&self) -> Vec<u8> {
        // Convert to bytes in big-endian order
        let correlation_id_bytes = self.correlation_id.to_be_bytes();
        let mut bytes = Vec::new();
        for i in 0..correlation_id_bytes.len() {
            bytes.push(correlation_id_bytes[i]);
        }
        bytes
    }
}

impl Size for ResponseHeaderV0 {

    fn size(&self) -> i32 {
        size_of::<i32>().try_into().unwrap()
    }

}


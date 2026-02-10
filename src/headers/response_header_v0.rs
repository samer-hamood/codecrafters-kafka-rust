use crate::{serializable::Serializable, size::Size};

#[derive(Debug, Clone)]
pub struct ResponseHeaderV0 {
    pub correlation_id: i32,
}

impl ResponseHeaderV0 {
    pub fn new(correlation_id: i32) -> Self {
        Self { correlation_id }
    }
}

impl Size for ResponseHeaderV0 {
    fn size(&self) -> usize {
        self.correlation_id.size()
    }
}

impl Serializable for ResponseHeaderV0 {
    fn to_be_bytes(&self) -> Vec<u8> {
        self.correlation_id.to_be_bytes().to_vec()
    }
}

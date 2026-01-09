use crate::{serializable::Serializable, size::Size, types::unsigned_varint::UnsignedVarint};

#[derive(Debug, Clone)]
pub struct CompactNullableBytes {
    pub length: UnsignedVarint,
    pub bytes: Option<Vec<u8>>,
}

impl CompactNullableBytes {
    pub fn null() -> Self {
        CompactNullableBytes {
            length: UnsignedVarint::new(0),
            bytes: None,
        }
    }
}

impl Size for CompactNullableBytes {
    fn size(&self) -> usize {
        self.length.size() + self.bytes.as_ref().map(|v| v.len()).unwrap_or(0)
    }
}

impl Serializable for CompactNullableBytes {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.length.to_be_bytes());
        if let Some(b) = &self.bytes {
            bytes.extend_from_slice(b)
        }
        bytes
    }
}

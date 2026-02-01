use crate::{byte_parsable::ByteParsable, size::Size};

// Record types
pub const TOPIC: i8 = 2;
#[derive(Debug)]
pub struct MetadataRecord {
    pub frame_version: i8,
    pub _type: i8,
    pub version: i8,
}

impl Size for MetadataRecord {
    fn size(&self) -> usize {
        self.frame_version.size() + self._type.size() + self.version.size()
    }
}

impl ByteParsable<Self> for MetadataRecord {
    fn parse(bytes: &[u8], offset: usize) -> Self {
        let mut offset = offset;
        let frame_version = i8::parse(bytes, offset);
        offset += frame_version.size();
        let _type = i8::parse(bytes, offset);
        offset += _type.size();
        let version = i8::parse(bytes, offset);
        Self {
            frame_version,
            _type,
            version,
        }
    }
}

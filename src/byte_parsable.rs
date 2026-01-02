use uuid::Uuid;

use crate::serializable::Serializable;
use crate::size::Size;

pub trait ByteParsable<T> {
    fn parse(bytes: &[u8], offset: usize) -> T;
}

impl ByteParsable<i64> for i64 {
    fn parse(bytes: &[u8], offset: usize) -> i64 {
        i64::from_be_bytes(bytes[offset..offset + size_of::<i64>()].try_into().unwrap())
    }
}

impl ByteParsable<i32> for i32 {
    fn parse(bytes: &[u8], offset: usize) -> i32 {
        i32::from_be_bytes(bytes[offset..offset + size_of::<i32>()].try_into().unwrap())
    }
}

impl ByteParsable<i16> for i16 {
    fn parse(bytes: &[u8], offset: usize) -> i16 {
        i16::from_be_bytes(bytes[offset..offset + size_of::<i16>()].try_into().unwrap())
    }
}

impl ByteParsable<i8> for i8 {
    fn parse(bytes: &[u8], offset: usize) -> i8 {
        i8::from_be_bytes(bytes[offset..offset + size_of::<i8>()].try_into().unwrap())
    }
}

impl ByteParsable<Self> for u16 {
    fn parse(bytes: &[u8], offset: usize) -> Self {
        Self::from_be_bytes(
            bytes[offset..offset + size_of::<Self>()]
                .try_into()
                .unwrap(),
        )
    }
}

impl ByteParsable<u32> for u32 {
    fn parse(bytes: &[u8], offset: usize) -> u32 {
        u32::from_be_bytes(bytes[offset..offset + size_of::<u32>()].try_into().unwrap())
    }
}

impl ByteParsable<Uuid> for Uuid {
    fn parse(bytes: &[u8], offset: usize) -> Uuid {
        Uuid::from_bytes(
            bytes[offset..offset + size_of::<Uuid>()]
                .try_into()
                .unwrap(),
        )
    }
}

use crate::serializable::Serializable;
use crate::size::Size;

#[allow(dead_code)]
pub trait ByteParsable<T> {

    fn parse(bytes: &[u8], offset: usize) -> T;

}

impl ByteParsable<i32> for i32 {

    fn parse(bytes: &[u8], offset: usize) -> i32 {
        i32::from_be_bytes(bytes[offset..offset + size_of::<i32>()].try_into().unwrap())
    }

}

impl ByteParsable<i8> for i8 {

    fn parse(bytes: &[u8], offset: usize) -> i8 {
        i8::from_be_bytes(bytes[offset..offset + size_of::<i8>()].try_into().unwrap())
    }

}


use std::i32;

use uuid::Uuid;

pub trait Size {
    fn size(&self) -> usize;
}

impl Size for i64 {
    fn size(&self) -> usize {
        size_of::<i64>()
    }
}

impl Size for i32 {
    fn size(&self) -> usize {
        size_of::<i32>()
    }
}

impl Size for i16 {
    fn size(&self) -> usize {
        size_of::<i16>()
    }
}

impl Size for i8 {
    fn size(&self) -> usize {
        size_of::<i8>()
    }
}

impl Size for Uuid {
    fn size(&self) -> usize {
        size_of::<Uuid>()
    }
}

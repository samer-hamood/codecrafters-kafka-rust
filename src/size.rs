use std::i32;


pub trait Size {
    fn size(&self) -> usize;
}

impl Size for i32 {

    fn size(&self) -> usize {
        size_of::<i32>()
    }

}

impl Size for i8 {

    fn size(&self) -> usize {
        size_of::<i8>()
    }

}


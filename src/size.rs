use uuid::Uuid;

pub trait Size {
    fn size(&self) -> usize {
        self.sized_fields().iter().map(|field| field.size()).sum()
    }

    fn sized_fields(&self) -> Vec<Box<dyn Size>> {
        vec![]
    }
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

impl Size for u8 {
    fn size(&self) -> usize {
        size_of::<u8>()
    }
}

impl Size for u16 {
    fn size(&self) -> usize {
        size_of::<u16>()
    }
}

impl Size for u32 {
    fn size(&self) -> usize {
        size_of::<u32>()
    }
}

impl Size for bool {
    fn size(&self) -> usize {
        size_of::<bool>()
    }
}

impl Size for Uuid {
    fn size(&self) -> usize {
        size_of::<Uuid>()
    }
}

impl Size for String {
    fn size(&self) -> usize {
        self.len()
    }
}

impl<T: Size> Size for Vec<T> {
    fn size(&self) -> usize {
        self.iter().map(|e| e.size()).sum()
    }
}

impl<T: Size> Size for Option<Vec<T>> {
    fn size(&self) -> usize {
        self.as_ref().map(|v| v.size()).unwrap_or(0)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn size_of_string_equals_number_of_bytes() {
        let string = String::from("Hello, world!");
        assert_eq!(string.size(), string.len());
    }

    #[test]
    fn size_of_vec_equals_sum_of_size_of_each_element() {
        let vec = vec![1, 2, 3, 4];
        let expected_size = vec.len() * 4; // i32 is 4 bytes
        assert_eq!(vec.size(), expected_size);
    }

    #[test]
    fn size_of_bool_equals_one_byte() {
        let boolean = false;
        assert_eq!(boolean.size(), 1);
    }
}

pub type BoxedSerializable = Box<dyn Serializable>;

pub trait Serializable {
    fn to_be_bytes(&self) -> Vec<u8> {
        self.serializable_fields()
            .iter()
            .flat_map(|field| field.to_be_bytes())
            .collect()
    }

    fn serializable_fields(&self) -> Vec<BoxedSerializable> {
        vec![]
    }
}

impl Serializable for i64 {
    fn to_be_bytes(&self) -> Vec<u8> {
        i64::to_be_bytes(*self).to_vec()
    }
}

impl Serializable for i32 {
    fn to_be_bytes(&self) -> Vec<u8> {
        i32::to_be_bytes(*self).to_vec()
    }
}

impl Serializable for i16 {
    fn to_be_bytes(&self) -> Vec<u8> {
        i16::to_be_bytes(*self).to_vec()
    }
}

impl Serializable for u8 {
    fn to_be_bytes(&self) -> Vec<u8> {
        u8::to_be_bytes(*self).to_vec()
    }
}


pub trait Serializable {
    fn to_be_bytes(&self) -> Vec<u8>;
}


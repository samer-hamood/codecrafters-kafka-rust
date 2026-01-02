#[allow(dead_code)]
pub trait PartialParsable<T, U> {
    fn parse(bytes: &[u8], offset: usize, data: U) -> T;
}

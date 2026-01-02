use std::{any::type_name, fmt::Debug};

pub trait ZigZagDecoder {
    type Int: TryFrom<i64>;

    fn zig_zag_decode(n: u64) -> Self::Int
    where
        <<Self as ZigZagDecoder>::Int as TryFrom<i64>>::Error: Debug,
    {
        let n_i64 = n as i64;
        let decoded = (n_i64 >> 1) ^ -(n_i64 & 1);
        decoded.try_into().unwrap_or_else(|_| {
            panic!(
                "Invalid return type: expected {} but was out of range {}",
                type_name::<Self::Int>(),
                decoded
            )
        })
    }
}

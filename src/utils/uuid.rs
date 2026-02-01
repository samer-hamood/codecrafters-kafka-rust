use std::str::FromStr;

use uuid::Uuid;

pub fn all_zeroes_uuid() -> Uuid {
    Uuid::from_str("00000000-0000-0000-0000-000000000000").unwrap()
}

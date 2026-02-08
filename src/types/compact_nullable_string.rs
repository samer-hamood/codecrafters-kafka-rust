use std::fmt::{Debug, Display};

use crate::byte_parsable::ByteParsable;
use crate::serializable::Serializable;
use crate::size::Size;
use crate::types::unsigned_varint::UnsignedVarint;

// https://kafka.apache.org/27/protocol.html#protocol_types

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactNullableString {
    pub length: UnsignedVarint,
    pub bytes: Option<Vec<u8>>,
}

impl CompactNullableString {
    pub fn null() -> Self {
        Self {
            length: UnsignedVarint::new(0),
            bytes: None,
        }
    }
}

impl Size for CompactNullableString {
    fn size(&self) -> usize {
        self.length.size() + self.bytes.size()
    }
}

impl ByteParsable<CompactNullableString> for CompactNullableString {
    fn parse(bytes: &[u8], offset: usize) -> Self {
        let mut offset = offset;
        let length = UnsignedVarint::parse(bytes, offset);
        offset += length.size();
        let bytes = match length.value {
            0 => None,
            1 => Some(Vec::new()),
            _ => Some(bytes[offset..offset + (length.value - 1) as usize].into()),
        };
        Self { length, bytes }
    }
}

impl Serializable for CompactNullableString {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.length.to_be_bytes());
        if let Some(b) = &self.bytes {
            bytes.extend_from_slice(&b.to_vec());
        }
        bytes
    }
}

impl PartialOrd for CompactNullableString {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CompactNullableString {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.bytes.cmp(&other.bytes)
    }
}

impl Display for CompactNullableString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(bytes) = &self.bytes {
            match str::from_utf8(bytes) {
                Ok(s) => write!(f, "{}", s),
                Err(e) => panic!("Invalid UTF-8: {}", e),
            }
        } else {
            panic!("No bytes to display CompactNullableString")
        }
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use super::*;
    use rstest::rstest;

    impl CompactNullableString {
        fn from(s: &str) -> Self {
            let length = UnsignedVarint::new(s.len() as u32);
            let bytes = Some(String::from(s).into_bytes());
            Self { length, bytes }
        }
    }

    #[test]
    fn same_strings_are_equal() {
        let s1 = CompactNullableString::from("Apple");
        let s2 = CompactNullableString::from("Apple");

        assert_eq!(s1, s2);
    }

    #[test]
    fn different_strings_are_not_equal() {
        let s1 = CompactNullableString::from("Apple");
        let s2 = CompactNullableString::from("Banana");

        assert_ne!(s1, s2);
    }

    #[rstest]
    #[case::perfectly_sorted(
        [CompactNullableString::from("Apple"), CompactNullableString::from("Banana"), CompactNullableString::from("Cherry")].to_vec(),
        [CompactNullableString::from("Apple"), CompactNullableString::from("Banana"), CompactNullableString::from("Cherry")].to_vec(),
    )]
    #[case::duplicate_values(
        [CompactNullableString::from("Apple"), CompactNullableString::from("Apple"), CompactNullableString::from("Banana")].to_vec(),
        [CompactNullableString::from("Apple"), CompactNullableString::from("Apple"), CompactNullableString::from("Banana")].to_vec(),
    )]
    #[case::single_element(
        [CompactNullableString::from("Apple")].to_vec(),
        [CompactNullableString::from("Apple")].to_vec(),
    )]
    #[case::mixed_cases(
        [CompactNullableString::from("apple"), CompactNullableString::from("Banana")].to_vec(),
        [CompactNullableString::from("Banana"), CompactNullableString::from("apple")].to_vec(),
    )]
    #[case::numeric_string(
        [CompactNullableString::from("1"), CompactNullableString::from("2"), CompactNullableString::from("10")].to_vec(),
        [CompactNullableString::from("1"), CompactNullableString::from("10"), CompactNullableString::from("2")].to_vec(),
    )]
    #[case::prefixes(
        [CompactNullableString::from("Apple"), CompactNullableString::from("App")].to_vec(),
        [CompactNullableString::from("App"), CompactNullableString::from("Apple")].to_vec(),
    )]
    #[case::completely_reversed(
        [CompactNullableString::from("Zebra"), CompactNullableString::from("Monkey"), CompactNullableString::from("Ant")].to_vec(),
        [CompactNullableString::from("Ant"), CompactNullableString::from("Monkey"), CompactNullableString::from("Zebra")].to_vec(),
    )]
    #[case::single_swap(
        [CompactNullableString::from("Banana"), CompactNullableString::from("Apple"), CompactNullableString::from("Cherry")].to_vec(),
        [CompactNullableString::from("Apple"), CompactNullableString::from("Banana"), CompactNullableString::from("Cherry")].to_vec(),
    )]
    #[case::end_displacement(
        [CompactNullableString::from("Banana"), CompactNullableString::from("Cherry"), CompactNullableString::from("Apple")].to_vec(),
        [CompactNullableString::from("Apple"), CompactNullableString::from("Banana"), CompactNullableString::from("Cherry")].to_vec(),
    )]
    #[case::whitespace(
        [CompactNullableString::from(" Apple"), CompactNullableString::from("Apple")].to_vec(),
        [CompactNullableString::from(" Apple"), CompactNullableString::from("Apple")].to_vec(),
    )]
    #[case::empty_string(
        [CompactNullableString::from(""), CompactNullableString::from("A")].to_vec(),
        [CompactNullableString::from(""), CompactNullableString::from("A")].to_vec(),
    )]
    fn sorts_in_lexicographical_order(
        #[case] unsorted: Vec<CompactNullableString>,
        #[case] expected_sorted: Vec<CompactNullableString>,
    ) {
        let actual_sorted: Vec<CompactNullableString> = unsorted.into_iter().sorted().collect();

        assert_eq!(actual_sorted, expected_sorted);
    }

    #[test]
    #[ignore = "used just to check natural order of strings"]
    fn sorts_strings_in_lexicographical_order() {
        let unsorted = ["apple", "Banana"];
        let expected_sorted = ["Banana", "apple"];
        let actual_sorted: Vec<&str> = unsorted.into_iter().sorted().collect();

        assert_eq!(actual_sorted, expected_sorted);
    }
}

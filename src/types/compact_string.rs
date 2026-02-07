use std::fmt::{Debug, Display};

use crate::byte_parsable::ByteParsable;
use crate::serializable::Serializable;
use crate::size::Size;
use crate::types::unsigned_varint::UnsignedVarint;

// https://kafka.apache.org/27/protocol.html#protocol_types

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactString {
    pub length: UnsignedVarint,
    pub bytes: Option<Vec<u8>>,
}

impl Size for CompactString {
    fn size(&self) -> usize {
        self.length.size() + self.bytes.size()
    }
}

impl ByteParsable<CompactString> for CompactString {
    fn parse(bytes: &[u8], offset: usize) -> CompactString {
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

impl Serializable for CompactString {
    fn to_be_bytes(&self) -> Vec<u8> {
        match &self.bytes {
            Some(b) => {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&self.length.to_be_bytes());
                bytes.extend_from_slice(&b.to_vec());
                bytes
            }
            None => Vec::new(),
        }
    }
}

impl PartialOrd for CompactString {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CompactString {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.bytes.cmp(&other.bytes)
    }
}

impl Display for CompactString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(bytes) = &self.bytes {
            match str::from_utf8(bytes) {
                Ok(s) => write!(f, "{}", s),
                Err(e) => panic!("Invalid UTF-8: {}", e),
            }
        } else {
            panic!("No bytes to display CompactString")
        }
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use super::*;
    use rstest::rstest;

    impl CompactString {
        fn from(s: &str) -> Self {
            let length = UnsignedVarint::new(s.len() as u32);
            let bytes = Some(String::from(s).into_bytes());
            Self { length, bytes }
        }
    }

    #[test]
    fn same_strings_are_equal() {
        let s1 = CompactString::from("Apple");
        let s2 = CompactString::from("Apple");

        assert_eq!(s1, s2);
    }

    #[test]
    fn different_strings_are_not_equal() {
        let s1 = CompactString::from("Apple");
        let s2 = CompactString::from("Banana");

        assert_ne!(s1, s2);
    }

    #[rstest]
    #[case::perfectly_sorted(
        [CompactString::from("Apple"), CompactString::from("Banana"), CompactString::from("Cherry")].to_vec(),
        [CompactString::from("Apple"), CompactString::from("Banana"), CompactString::from("Cherry")].to_vec(),
    )]
    #[case::duplicate_values(
        [CompactString::from("Apple"), CompactString::from("Apple"), CompactString::from("Banana")].to_vec(),
        [CompactString::from("Apple"), CompactString::from("Apple"), CompactString::from("Banana")].to_vec(),
    )]
    #[case::single_element(
        [CompactString::from("Apple")].to_vec(),
        [CompactString::from("Apple")].to_vec(),
    )]
    #[case::mixed_cases(
        [CompactString::from("apple"), CompactString::from("Banana")].to_vec(),
        [CompactString::from("Banana"), CompactString::from("apple")].to_vec(),
    )]
    #[case::numeric_string(
        [CompactString::from("1"), CompactString::from("2"), CompactString::from("10")].to_vec(),
        [CompactString::from("1"), CompactString::from("10"), CompactString::from("2")].to_vec(),
    )]
    #[case::prefixes(
        [CompactString::from("Apple"), CompactString::from("App")].to_vec(),
        [CompactString::from("App"), CompactString::from("Apple")].to_vec(),
    )]
    #[case::completely_reversed(
        [CompactString::from("Zebra"), CompactString::from("Monkey"), CompactString::from("Ant")].to_vec(),
        [CompactString::from("Ant"), CompactString::from("Monkey"), CompactString::from("Zebra")].to_vec(),
    )]
    #[case::single_swap(
        [CompactString::from("Banana"), CompactString::from("Apple"), CompactString::from("Cherry")].to_vec(),
        [CompactString::from("Apple"), CompactString::from("Banana"), CompactString::from("Cherry")].to_vec(),
    )]
    #[case::end_displacement(
        [CompactString::from("Banana"), CompactString::from("Cherry"), CompactString::from("Apple")].to_vec(),
        [CompactString::from("Apple"), CompactString::from("Banana"), CompactString::from("Cherry")].to_vec(),
    )]
    #[case::whitespace(
        [CompactString::from(" Apple"), CompactString::from("Apple")].to_vec(),
        [CompactString::from(" Apple"), CompactString::from("Apple")].to_vec(),
    )]
    #[case::empty_string(
        [CompactString::from(""), CompactString::from("A")].to_vec(),
        [CompactString::from(""), CompactString::from("A")].to_vec(),
    )]
    fn sorts_in_lexicographical_order(
        #[case] unsorted: Vec<CompactString>,
        #[case] expected_sorted: Vec<CompactString>,
    ) {
        let actual_sorted: Vec<CompactString> = unsorted.into_iter().sorted().collect();

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

use crate::byte_parsable::ByteParsable;
use crate::serializable::{BoxedSerializable, Serializable};
use crate::size::Size;
use crate::types::unsigned_varint::UnsignedVarint;
use std::iter;
use std::slice::Iter;

// https://kafka.apache.org/27/protocol.html#protocol_types

#[derive(Debug, Clone)]
pub struct CompactArray<T: Serializable + Size + ByteParsable<T> + Clone> {
    pub length: UnsignedVarint,
    elements: Option<Vec<T>>,
}

#[allow(dead_code)]
impl<T: Serializable + Size + ByteParsable<T> + Clone> CompactArray<T> {
    pub fn new(elements: Vec<T>) -> CompactArray<T> {
        CompactArray {
            length: UnsignedVarint {
                value: (1 + elements.len()).try_into().unwrap(),
                byte_count: 1, // TODO: Compute number of bytes for value
            },
            elements: Some(elements),
        }
    }

    pub fn empty() -> Self {
        CompactArray {
            length: UnsignedVarint {
                value: 1,
                byte_count: 1,
            },
            elements: Some(Vec::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.number_of_elements()
    }

    pub fn iter(&self) -> Iter<'_, T> {
        self.elements.as_deref().unwrap_or(&[]).iter()
    }

    fn number_of_elements(&self) -> usize {
        self.elements.as_ref().map(|v| v.len()).unwrap_or(0)
    }
}

impl<T: Serializable + Size + ByteParsable<T> + Clone> Size for CompactArray<T> {
    fn size(&self) -> usize {
        self.length.size() + self.iter().map(|element| element.size()).sum::<usize>()
    }
}

impl<T: Serializable + Size + ByteParsable<T> + Clone> ByteParsable<CompactArray<T>>
    for CompactArray<T>
{
    fn parse(bytes: &[u8], offset: usize) -> CompactArray<T> {
        let mut offset = offset;
        let length = UnsignedVarint::parse(bytes, offset);
        offset += length.size();
        let elements = match length.value {
            0 => None,
            1 => Some(Vec::new()),
            _ => {
                let mut elements = Vec::new();
                for _ in 0..(length.value - 1) {
                    let element = T::parse(bytes, offset);
                    offset += element.size();
                    elements.push(element);
                }
                Some(elements)
            }
        };
        CompactArray { length, elements }
    }
}

impl<T: Serializable + Size + ByteParsable<T> + Clone + 'static> Serializable for CompactArray<T> {
    fn serializable_fields(&self) -> Vec<BoxedSerializable> {
        iter::once(Box::new(self.length.clone()) as BoxedSerializable)
            .chain(
                self.iter()
                    .map(|x| Box::new(x.clone()) as BoxedSerializable),
            )
            .collect()
    }
}

impl<T: Serializable + Size + ByteParsable<T> + Clone> std::ops::Index<usize> for CompactArray<T> {
    type Output = T;

    fn index(&self, offset: usize) -> &Self::Output {
        &self.elements.as_ref().expect("elements is None")[offset]
    }
}

impl<T: Serializable + Size + ByteParsable<T> + Clone> From<Vec<T>> for CompactArray<T> {
    fn from(val: Vec<T>) -> Self {
        CompactArray::new(val)
    }
}

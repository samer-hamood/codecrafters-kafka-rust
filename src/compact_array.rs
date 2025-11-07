use crate::byte_parsable::ByteParsable;
use crate::serializable::{BoxedSerializable, Serializable};
use crate::size::Size;
use std::iter;
use std::slice::Iter;
use std::{i32, usize};

#[allow(dead_code)]
const LENGTH: usize = 1;

#[derive(Debug, Clone)]
pub struct CompactArray<T: Serializable + Size + ByteParsable<T> + Clone> {
    elements: Vec<T>,
}

#[allow(dead_code)]
impl<T: Serializable + Size + ByteParsable<T> + Clone> CompactArray<T> {
    pub fn new(elements: Vec<T>) -> CompactArray<T> {
        CompactArray { elements }
    }

    pub fn empty() -> Self {
        CompactArray::new(Vec::new())
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn length(&self) -> u8 {
        // pub fn len(&self) -> u32 {
        (1 + self.elements.len()).try_into().unwrap()
    }

    pub fn iter(&self) -> Iter<'_, T> {
        self.elements.iter()
    }
}

impl<T: Serializable + Size + ByteParsable<T> + Clone> Size for CompactArray<T> {
    fn size(&self) -> usize {
        // TODO: Compute unsigned varint size of length: encode_unsigned_varint(1 + self.elements.len()).len()
        size_of::<u8>()
            + self
                .elements
                .iter()
                .map(|element| element.size())
                .sum::<usize>()
    }
}

impl<T: Serializable + Size + ByteParsable<T> + Clone> ByteParsable<CompactArray<T>>
    for CompactArray<T>
{
    fn parse(bytes: &[u8], offset: usize) -> CompactArray<T> {
        let mut offset = offset;
        let length = u8::from_be_bytes(bytes[offset..offset + LENGTH].try_into().unwrap());
        offset += LENGTH;

        let compact_array = if length == 0 {
            CompactArray::<T>::empty()
        } else if length == 1 {
            CompactArray::<T>::empty()
        } else {
            let mut elements = Vec::new();
            for _ in 0..(length - 1) {
                let element = T::parse(bytes, offset);
                offset += element.size();
                elements.push(element);
            }
            CompactArray::<T>::new(elements)
        };
        compact_array
    }
}

impl<T: Serializable + Size + ByteParsable<T> + Clone + 'static> Serializable for CompactArray<T> {
    fn serializable_fields(&self) -> Vec<BoxedSerializable> {
        iter::once(Box::new(self.length()) as BoxedSerializable)
            .chain(
                self.elements
                    .iter()
                    .map(|x| Box::new(x.clone()) as BoxedSerializable),
            )
            .collect()
    }
}

impl<T: Serializable + Size + ByteParsable<T> + Clone> std::ops::Index<usize> for CompactArray<T> {
    type Output = T;

    fn index(&self, offset: usize) -> &Self::Output {
        &self.elements[offset]
    }
}

impl<T: Serializable + Size + ByteParsable<T> + Clone> Into<CompactArray<T>> for Vec<T> {
    fn into(self) -> CompactArray<T> {
        CompactArray::new(self)
    }
}

#[derive(Debug, Clone)]
pub struct CompactArrayElementI32(i32);

impl Size for CompactArrayElementI32 {
    fn size(&self) -> usize {
        size_of::<i32>()
    }
}

impl Serializable for CompactArrayElementI32 {
    fn to_be_bytes(&self) -> Vec<u8> {
        self.0.to_be_bytes().to_vec()
    }
}

impl ByteParsable<CompactArrayElementI32> for CompactArrayElementI32 {
    fn parse(bytes: &[u8], offset: usize) -> Self {
        let partition =
            i32::from_be_bytes(bytes[offset..offset + size_of::<i32>()].try_into().unwrap());
        CompactArrayElementI32(partition)
    }
}

use std::slice::Iter;
use crate::serializable::Serializable;
use crate::size::Size;

#[derive(Debug)]
pub struct CompactArray<T: Serializable + Size> {
    elements: Vec<T>,
}

#[allow(dead_code)]
impl <T:Serializable + Size> CompactArray<T> {

    pub fn new(elements: Vec<T>) -> CompactArray<T> {
        CompactArray {
            elements: elements,
        }
    }

    pub fn len(&self) -> u8 {
        (1 + self.elements.len()).try_into().unwrap()
    }

    pub fn iter(&self) -> Iter<'_, T> {
        self.elements.iter()
    }

}

impl <T:Serializable + Size> Serializable for CompactArray<T> {

    fn to_be_bytes(&self) -> Vec<u8> {
        let array_length_bytes = self.len().to_be_bytes();
        let mut bytes: Vec<u8> = Vec::new();
        for i in 0..array_length_bytes.len() {
            bytes.push(array_length_bytes[i]);
        }
        for i in 0..self.elements.len() {
            let elements_bytes = self.elements[i].to_be_bytes();
            for j in 0..elements_bytes.len() {
                bytes.push(elements_bytes[j]);
            }
        }
        bytes
    }
    
}

impl <T:Serializable + Size> Size for CompactArray<T> {

    fn size(&self) -> i32 {
        1 + self.elements.iter().map(|element| element.size()).sum::<i32>()
    }

}

use crate::size::Size;

#[derive(Debug)]
pub struct TagSection {
    number_of_tagged_fields: u8,
}

impl TagSection {

    pub fn empty() -> TagSection {
        TagSection {
            number_of_tagged_fields: 0,
        }
    }

    pub fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        if self.number_of_tagged_fields == 0 {
            bytes.push(0);
        }
        bytes
    }

}

impl Size for TagSection {

    fn size(&self) -> i32 {
        if self.number_of_tagged_fields == 0 {
            1
        }
        else {
            panic!("Calculating size of non-empty Tagsection not implemented")
        }
    }

}

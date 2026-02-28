use std::{
    fs::{self, File},
    io::Read,
};

pub fn read_file(path: &str) -> Result<Vec<u8>, std::io::Error> {
    let file_byte_count: usize = get_file_size(path);
    let mut buf = Vec::with_capacity(file_byte_count);
    let mut file = File::open(path).unwrap_or_else(|e| panic!("Unable to open file {path}: {e}"));
    file.read_to_end(&mut buf).map(|_| buf)
}

pub fn get_file_size(path: &str) -> usize {
    fs::metadata(path)
        .unwrap_or_else(|e| panic!("Unable to read metadata for file {path}: {e}"))
        .len() as usize
}

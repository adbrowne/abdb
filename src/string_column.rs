use std::io::Read;
use std::io::Write;

use crate::io;
use crate::{TrackedWriter, MAX_ROW_GROUP_SIZE};

fn read_u8_string_entry<R: Read>(reader: &mut std::io::BufReader<R>) -> (u8, u32) {
    let mut buffer = [0u8; 1];
    reader.read_exact(&mut buffer).expect("Failed to read");
    let count = io::read_u32(reader);
    (buffer[0], count)
}

pub fn read_string_column<R: Read>(
    reader: &mut std::io::BufReader<R>,
    item_count: u16,
) -> Vec<std::string::String> {
    let mut result = Vec::with_capacity(MAX_ROW_GROUP_SIZE);
    for (u8_value, repeat_count) in read_u8_string_column(reader, item_count).iter() {
        let value = String::from_utf8(vec![*u8_value]).expect("Failed to convert to string");
        let r = vec![value; *repeat_count as usize];
        result.extend(r);
    }
    result
}

pub fn read_u8_string_column<R: Read>(
    reader: &mut std::io::BufReader<R>,
    item_count: u16,
) -> [(u8, u32); MAX_ROW_GROUP_SIZE] {
    let mut column = [(0u8, 0u32); MAX_ROW_GROUP_SIZE];
    let mut i = 0;
    let mut remaining = item_count as i16;
    while remaining > 0 {
        let (value, count) = read_u8_string_entry(reader);
        column[i] = (value, count);
        remaining -= count as i16;
        i += 1;
    }
    column
}

pub struct StringColumnReader {
    data: [(u8, u32); MAX_ROW_GROUP_SIZE],
    item_count: u16,
    item_index: u16,
    repeat_index: i16
}

impl StringColumnReader {
    pub fn new<R: Read>(reader: &mut std::io::BufReader<R>, item_count: u16)  -> Self {
        StringColumnReader { data: read_u8_string_column(reader, item_count), item_count, item_index: 0, repeat_index: 0 }
    }
}

impl Iterator for StringColumnReader {
    type Item = String;
    
    fn next(&mut self) -> Option<Self::Item> {
        println!("item_index: {}, repeat_index: {}", self.item_index, self.repeat_index);
        if self.item_index < self.item_count as u16 {
            let (value, count) = self.data[self.item_index as usize];
            if self.repeat_index < count as i16 {
                self.repeat_index += 1;
                return Some(std::string::String::from_utf8(vec![value]).expect("Failed to convert to string"));
            } else {
                self.item_index += 1;
                self.repeat_index = 0;
                return self.next();
            }
        } else {
            return None;
        }
    }
}


pub fn write_string_column<'a, I, W: Write>(column: I, writer: &mut TrackedWriter<W>)
where
    I: Iterator<Item = &'a String>,
{
    let mut iter = column.peekable();
    while let Some(value) = iter.next() {
        let mut count = 1;
        while iter.peek() == Some(&value) {
            iter.next();
            count += 1;
        }
        writer
            .write_all(&[
                value.as_bytes()[0],
                (count as u32).to_le_bytes()[0],
                (count as u32).to_le_bytes()[1],
                (count as u32).to_le_bytes()[2],
                (count as u32).to_le_bytes()[3],
            ])
            .expect("Failed to write");
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufReader, Cursor};

    use super::*;

    #[test]
    fn test_read_write_string_column() {
        let input = vec!["a", "a", "b", "b", "b", "c"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        // Write column
        let mut writer = TrackedWriter::new(Vec::new());
        write_string_column(input.iter(), &mut writer);
        let written = writer.into_inner().into_inner().unwrap();

        // Read column back
        let mut reader = BufReader::new(Cursor::new(written));
        let col_reader = StringColumnReader::new(&mut reader, input.len() as u16);
        let output: Vec<String> = col_reader.collect();

        assert_eq!(input, output);
    }
}

use std::io::Read;
use std::io::Write;

use crate::io;
use crate::io::read_u64;
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
    for (u8_value, repeat_count) in read_u8_string_column(reader, item_count).1.iter() {
        let value = String::from_utf8(vec![*u8_value]).expect("Failed to convert to string");
        let r = vec![value; *repeat_count as usize];
        result.extend(r);
    }
    result
}


pub fn read_u8_string_column<R: Read>(
    reader: &mut std::io::BufReader<R>,
    item_count: u16,
) -> (u64, [(u8, u32); MAX_ROW_GROUP_SIZE]) {
    let mut column = [(0u8, 0u32); MAX_ROW_GROUP_SIZE];
    let mut i = 0;
    let mut remaining = item_count as i16;
    let column_entries = read_u64(reader);
    while remaining > 0 {
        let (value, count) = read_u8_string_entry(reader);
        column[i] = (value, count);
        remaining -= count as i16;
        i += 1;
    }
    (column_entries, column)
}

pub struct StringColumnReader {
    data: Vec<(u8, u32)>,
    column_entries: u64,
    item_index: u16,
    repeat_index: i16,
}

pub fn read_u8_string_column_to_vec<R: Read>(
    reader: &mut std::io::BufReader<R>,
    data : &mut Vec<(u8, u32)>
) -> u64 {
    let column_entries = read_u64(reader);

    data.clear();
    // data.reserve(column_entries as usize); // I think not required as we are setting the length in the initilization
    unsafe {
        data.set_len(column_entries as usize);
        // Using data as a raw byte buffer
        let byte_slice = std::slice::from_raw_parts_mut(
            data.as_mut_ptr() as *mut u8,
            std::mem::size_of::<(u8, u32)>() * column_entries as usize
        );
        reader.read_exact(byte_slice).expect("Failed to read column data");
    }

    // No further processing needed as the bytes should now represent the (u8, u32) pairs
    column_entries
}

pub fn write_u8_string_column_from_vec<W: Write>(
    writer: &mut TrackedWriter<W>,
    data: &[(u8, u32)]
) {
    // Write the number of entries
    io::write_u64(writer, data.len() as u64);
    
    // Write the raw bytes of the vector
    unsafe {
        let byte_slice = std::slice::from_raw_parts(
            data.as_ptr() as *const u8,
            std::mem::size_of::<(u8, u32)>() * data.len()
        );
        writer.write_all(byte_slice).expect("Failed to write column data");
    }
}

fn write_vec_to_data_array(input : &Vec<&str>, data: &mut Vec<(u8, u32)>) -> u64 {
    //io::write_u64(writer, column_length);
    // Clone the iterator to avoid consuming it when we count
    data.clear();
    let mut column_length : u64 = 0;
    let mut iter = input.iter().peekable();
    while let Some(value) = iter.next() {
        let mut count = 1;
        while iter.peek() == Some(&value) {
            iter.next();
            count += 1;
        }
        data.push((value.as_bytes()[0], count));
        column_length += 1;
    }
    column_length
}
impl StringColumnReader {
    pub fn new_from_strings(strings : Vec<&str>) -> Self {
        let mut data = Vec::with_capacity(MAX_ROW_GROUP_SIZE*5);
        let column_entries = write_vec_to_data_array(&strings, &mut data);
        StringColumnReader {
            data,
            column_entries,
            item_index: 0,
            repeat_index: 0,
        }
    }

    pub fn empty() -> Self {
        let data = Vec::with_capacity(MAX_ROW_GROUP_SIZE*5);
        StringColumnReader {
            data,
            column_entries: 0,
            item_index: 0,
            repeat_index: 0,
        }
    }

    pub fn new<R: Read>(reader: &mut std::io::BufReader<R>) -> Self {
        let mut data = Vec::with_capacity(MAX_ROW_GROUP_SIZE*5);
        let column_entries = read_u8_string_column_to_vec(reader, &mut data);
        StringColumnReader {
            data,
            column_entries,
            item_index: 0,
            repeat_index: 0,
        }
    }

    pub fn write(&self, writer: &mut TrackedWriter<impl Write>) {
        write_u8_string_column_from_vec(writer, &self.data);
    }

    pub fn read(&mut self, reader: &mut std::io::BufReader<impl Read>) {
        self.column_entries = read_u8_string_column_to_vec(reader, &mut self.data);
        self.item_index = 0;
        self.repeat_index = 0;
    }

    pub fn count_strings(&self) -> u64 {
        self.data.iter().map(|x| x.1 as u64).sum()
    }
}

impl Iterator for StringColumnReader {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        println!(
            "item_index: {}, repeat_index: {}, item_count: {}",
            self.item_index, self.repeat_index, self.column_entries
        );
        if self.item_index < self.column_entries as u16 {
            let (value, count) = self.data[self.item_index as usize];
            if self.repeat_index < count as i16 {
                self.repeat_index += 1;
                return Some(
                    std::string::String::from_utf8(vec![value])
                        .expect("Failed to convert to string"),
                );
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
    I: Iterator<Item = &'a str>,
    I: Clone
{
    let mut iter = column.peekable();
    let column_length = count_column_length(iter.clone());
    
    io::write_u64(writer, column_length);
    // Clone the iterator to avoid consuming it when we count
    while let Some(value) = iter.next() {
        let mut count = 1;
        while iter.peek() == Some(&value) {
            iter.next();
            count += 1;
        }
        io::write_repeated_string(writer, value.as_bytes()[0], count);
    }
}

fn count_column_length<'a, I>(iter: I) -> u64
where
    I: Iterator<Item = &'a str>,
{
    let mut result : u64 = 0;
    let mut iter = iter.peekable();
    while let Some(value) = iter.next() {
        while iter.peek() == Some(&value) {
            iter.next();
        }
        result += 1; // Count each unique run as one entry
    }
    result
}

#[cfg(test)]
mod tests {
    use std::io::{BufReader, Cursor};

    use datafusion::datasource::file_format::write;

    use super::*;

    #[test]
    fn test_read_write_string_column() {
        let input = vec!["a", "a", "b", "b", "b", "c"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        let input_refs: Vec<&str> = input.iter().map(|s| s.as_str()).collect();
        let writer_string_reader = StringColumnReader::new_from_strings(input_refs);
        let mut writer = TrackedWriter::new(Vec::new());
        // Write column
        writer_string_reader.write(&mut writer);
        let written = writer.into_inner().into_inner().unwrap();

        // Read column back
        let mut reader = BufReader::new(Cursor::new(written));
        let col_reader = StringColumnReader::new(&mut reader);
        let output: Vec<String> = col_reader.collect();

        assert_eq!(input, output);
    }
}

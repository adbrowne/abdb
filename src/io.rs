use std::io::{Read, Write};

pub fn read_u16<R: Read>(reader: &mut std::io::BufReader<R>) -> u16 {
    let mut buffer = [0u8; 2];
    reader.read_exact(&mut buffer).expect("Failed to read u16");
    u16::from_le_bytes(buffer)
}

pub fn read_u32<R: Read>(reader: &mut R) -> u32 {
    let mut buffer = [0u8; 4];
    reader.read_exact(&mut buffer).expect("Failed to read");
    u32::from_le_bytes(buffer)
}

pub fn write_u32<W: Write>(writer: &mut W, value: u32) {
    writer
        .write_all(&value.to_le_bytes())
        .expect("Failed to write");
}

pub fn write_u64<W: Write>(writer: &mut W, value: u64) {
    writer
        .write_all(&value.to_le_bytes())
        .expect("Failed to write");
}

pub fn read_u64<R: Read>(reader: &mut R) -> u64 {
    let mut buffer = [0u8; 8];
    reader.read_exact(&mut buffer).expect("Failed to read");
    u64::from_le_bytes(buffer)
}

pub fn read_u8<R: Read>(reader: &mut R) -> u8 {
    let mut buffer = [0u8; 1];
    reader.read_exact(&mut buffer).expect("Failed to read");
    u8::from_le_bytes(buffer)
}

pub fn write_u8<W: Write>(writer: &mut W, value: u8) {
    writer
        .write_all(&value.to_le_bytes())
        .expect("Failed to write");
}

pub fn write_repeated_string<W: Write>(writer: &mut W, value: u8, count: u32) {
    writer.write_all(&[value]).expect("Failed to write");
    write_u32(writer, count);
}

pub fn read_repeated_string<R: Read>(reader: &mut R) -> (u8, u32) {
    let value = read_u8(reader);
    let count = read_u32(reader);
    (value, count)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_write_read_u8(test_value: u8) {
            let mut buffer = Cursor::new(Vec::new());
            let mut writer = std::io::BufWriter::new(&mut buffer);

            write_u8(&mut writer, test_value);
            writer.flush().unwrap();

            let binding = writer.into_inner().unwrap().clone().into_inner();
            buffer = Cursor::new(binding);

            buffer.set_position(0);
            let mut reader = std::io::BufReader::new(buffer);
            let result = read_u8(&mut reader);

            prop_assert_eq!(result, test_value);
        }

        #[test]
        fn test_write_read_u32(test_value: u32) {
            let mut buffer = Cursor::new(Vec::new());
            let mut writer = std::io::BufWriter::new(&mut buffer);

            write_u32(&mut writer, test_value);
            writer.flush().unwrap();

            let binding = writer.into_inner().unwrap().clone().into_inner();
            buffer = Cursor::new(binding);

            buffer.set_position(0);
            let mut reader = std::io::BufReader::new(buffer);
            let result = read_u32(&mut reader);

            prop_assert_eq!(result, test_value);
        }

        #[test]
        fn test_write_read_repeated_string(value: u8, count: u32) {
            let mut buffer = Cursor::new(Vec::new());
            let mut writer = std::io::BufWriter::new(&mut buffer);

            write_repeated_string(&mut writer, value, count);
            writer.flush().unwrap();

            let binding = writer.into_inner().unwrap().clone().into_inner();
            buffer = Cursor::new(binding);

            buffer.set_position(0);
            let mut reader = std::io::BufReader::new(buffer);
            let result = read_repeated_string(&mut reader);

            prop_assert_eq!(result, (value, count));
        }
    }
}

use std::io::{Cursor, Read, Write};

pub fn read_u32<R: Read>(reader: &mut std::io::BufReader<R>) -> u32 {
    let mut buffer = [0u8; 4];
    reader.read_exact(&mut buffer).expect("Failed to read");
    u32::from_le_bytes(buffer)
}

pub fn write_u32<W: Write>(writer: &mut std::io::BufWriter<W>, value: u32) {
    writer.write_all(&value.to_le_bytes()).expect("Failed to write");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_read_u32() {
        let mut buffer = Cursor::new(Vec::new());
        let mut writer = std::io::BufWriter::new(&mut buffer);
        let test_value = 42u32;
        
        write_u32(&mut writer, test_value);
        writer.flush().unwrap();

        let binding = writer.into_inner().unwrap().clone().into_inner();
        buffer = Cursor::new(binding);
        
        buffer.set_position(0);
        let mut reader = std::io::BufReader::new(buffer);
        let result = read_u32(&mut reader);
        
        assert_eq!(result, test_value);
    }
}

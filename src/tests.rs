#[cfg(test)]
use super::*;

#[test]
fn test_compress_f64() {
    assert_eq!(compress_f64(1.23), 123);
    assert_eq!(compress_f64(0.0), 0);
    assert_eq!(compress_f64(0.01), 1);
    assert_eq!(compress_f64(2.345), 235);
}

#[test]
fn test_decompress_f64() {
    assert_eq!(decompress_f64(123), 1.23);
    assert_eq!(decompress_f64(0), 0.0);
    assert_eq!(decompress_f64(1), 0.01);
    assert_eq!(decompress_f64(235), 2.35);
}

#[test]
fn test_query_1() {
    // Add test for query_1 function
}

#[test]
fn test_read_file() {
    // Add test for read_file function
}

#[test]
fn test_save_data() {
    // Add test for save_data function
}





#[test]
fn test_write_and_read_row_group() {
    // Add test for write_and_read_row_group function
    let lineitems: [LineItem; 20] = array::from_fn(|_| LineItem {
        l_returnflag: "A".to_string(),
        l_linestatus: "B".to_string(),
        l_quantity: 1.0,
        l_extendedprice: 2.0,
        l_discount: 3.0,
        l_tax: 4.0,
    });
    let buffer = Vec::new();
    let mut writer = std::io::BufWriter::new(buffer);

    write_row_group(&lineitems[0..9], &mut writer);
    write_row_group(&lineitems[9..20], &mut writer);

    let binding = writer.into_inner().unwrap();
    let mut reader = {
        let buffer: &[u8] = &binding;
        std::io::BufReader::new(buffer)
    }; 

    let read_lineitems1 = read_row_group(&mut reader); 
    let read_lineitems2 = read_row_group(&mut reader); 
    let read_lineitems = read_lineitems1.iter().chain(read_lineitems2.iter()).cloned().collect::<Vec<LineItem>>();

    assert_eq!(lineitems.to_vec(), read_lineitems);
}
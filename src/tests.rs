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

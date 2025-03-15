pub mod io;
pub mod string_column;
use std::{
    cmp::min,
    io::{BufRead, Read, Write},
};
static MAX_ROW_GROUP_SIZE: usize = 8000;
use string_column::{read_u8_string_column, write_string_column};
#[derive(Debug, Default, PartialEq, Clone)]
struct QueryOneState {
    count: u64,
    sum_qty: f64,
    sum_base_price: f64,
    sum_disc_price: f64,
    sum_charge: f64,
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct QueryOneStateColumn {
    count: u64,
    sum_qty: u64,
    sum_base_price: u64,
    sum_discount: u64,
    sum_tax: u64,
}

pub fn query_1_column(path: &str) -> Vec<Option<QueryOneStateColumn>> {
    let file = std::fs::File::open(path).expect("Failed to open file");
    let mut reader = std::io::BufReader::new(file);
    let mut state: Vec<Option<QueryOneStateColumn>> = vec![None; 256 * 256];

    loop {
        if reader.fill_buf().unwrap().is_empty() {
            break;
        }
        update_state_from_row_group(&mut reader, &mut state);
    }
    state
}

fn sum_u16s(data: &U16column, start: usize, count: usize) -> u64 {
    data.data[start..start + count]
        .iter()
        .map(|x| *x as u64)
        .sum::<u64>()
}

fn update_state_from_row_group<R: Read>(
    reader: &mut std::io::BufReader<R>,
    state: &mut Vec<Option<QueryOneStateColumn>>,
) -> () {
    let item_count = read_u16(reader);
    let mut linestatus = read_u8_string_column(reader, item_count).1;
    let mut returnflag = read_u8_string_column(reader, item_count).1;
    let quantity = read_u16_column(reader, item_count);
    let discount = read_u16_column(reader, item_count);
    let tax = read_u16_column(reader, item_count);
    let extendedprice = read_u16_column(reader, item_count);

    let mut last_returnflag_index = 0;
    let mut last_linestatus_index = 0;
    let mut index: usize = 0;
    while index < item_count as usize {
        let last_returnflag = returnflag[last_returnflag_index];
        let last_linestatus = linestatus[last_linestatus_index];
        let run_length = min(last_returnflag.1, last_linestatus.1) as usize;

        let current_state =
            state[get_state_index(last_returnflag.0, last_linestatus.0)].get_or_insert_default();

        current_state.sum_qty += sum_u16s(&quantity, index, run_length) ;
        current_state.count += run_length as u64;
        current_state.sum_base_price += sum_u16s(&extendedprice, index, run_length);
        current_state.sum_discount += sum_u16s(&discount, index, run_length);
        current_state.sum_tax += sum_u16s(&tax, index, run_length);
        returnflag[last_returnflag_index].1 -= run_length as u32;
        linestatus[last_linestatus_index].1 -= run_length as u32;
        if returnflag[last_returnflag_index].1 == 0 {
            last_returnflag_index += 1;
        }
        if linestatus[last_linestatus_index].1 == 0 as u32 {
            last_linestatus_index += 1;
        }
        index += run_length;
    }
}

pub fn print_state_column(state: Vec<Option<QueryOneStateColumn>>) {
    for i in 0..256 {
        for j in 0..256 {
            if let Some(state_column) = &state[i * 256 + j] {
                let l_returnflag = String::from_utf8(vec![i as u8]).unwrap();
                let l_linestatus = String::from_utf8(vec![j as u8]).unwrap();
                let state = QueryOneState {
                    count: state_column.count,
                    sum_qty: state_column.sum_qty as f64 / 100.0,
                    sum_base_price: state_column.sum_base_price as f64 / 100.0,
                    sum_disc_price: state_column.sum_base_price as f64 / 100.0
                        * (1.0 - state_column.sum_discount as f64 / 100.0),
                    sum_charge: state_column.sum_base_price as f64 / 100.0
                        * (1.0 - state_column.sum_discount as f64 / 100.0)
                        * (1.0 + state_column.sum_tax as f64 / 100.0),
                };
                println!("{}, {}, {:?}", l_returnflag, l_linestatus, state);
            }
        }
    }
}

fn read_u16<R: Read>(reader: &mut std::io::BufReader<R>) -> u16 {
    let mut buffer = [0u8; 2];
    reader.read_exact(&mut buffer).expect("Failed to read");
    let item_count = u16::from_le_bytes(buffer);
    item_count
}

pub fn read_u16_column<R: Read>(reader: &mut std::io::BufReader<R>, item_count: u16) -> U16column {
    let mut data = [0u16; MAX_ROW_GROUP_SIZE];
    reader
        .read_exact(bytemuck::cast_slice_mut(&mut data[0..item_count as usize]))
        .expect("Failed to read");
    U16column {
        data,
        size: item_count as usize,
    }
}

pub fn read_f64_column<R: Read>(reader: &mut std::io::BufReader<R>, item_count: u16) -> Vec<f64> {
    read_u16_column(reader, item_count)
        .data
        .iter()
        .map(|x| decompress_f64(*x))
        .collect()
}

fn get_state_index(returnflag: u8, linestatus: u8) -> usize {
    (returnflag as usize) * 256 + (linestatus as usize)
}
pub struct U16column {
    pub data: [u16; MAX_ROW_GROUP_SIZE],
    #[allow(dead_code)]
    size: usize,
}

pub fn decompress_f64(f: u16) -> f64 {
    f as f64 / 100.0
}

#[derive(Debug, PartialEq, Clone)]
pub struct LineItem {
    pub l_returnflag: String,
    pub l_linestatus: String,
    pub l_quantity: f64,
    pub l_extendedprice: f64,
    pub l_discount: f64,
    pub l_tax: f64,
}

pub fn write_batch(writer: &mut TrackedWriter<std::io::BufWriter<std::fs::File>>, batch: &mut Vec<LineItem>) {
    batch.sort_by(|a, b| {
        a.l_returnflag
            .cmp(&b.l_returnflag)
            .then(a.l_linestatus.cmp(&b.l_linestatus))
    });
    write_row_group(&*batch, writer);
}

pub fn write_row_group<W: Write>(lineitems: &[LineItem], writer: &mut TrackedWriter<W>) {
    let item_count = (lineitems.len() as u16).to_le_bytes();
    writer.write_all(&item_count).expect("Failed to write");
    write_string_column(lineitems.iter().map(|x| x.l_linestatus.as_str()), writer);
    write_string_column(lineitems.iter().map(|x| x.l_returnflag.as_str()), writer);
    write_f64_column(lineitems.iter().map(|x| x.l_quantity), writer);
    write_f64_column(lineitems.iter().map(|x| x.l_discount), writer);
    write_f64_column(lineitems.iter().map(|x| x.l_tax), writer);
    write_f64_column(lineitems.iter().map(|x| x.l_extendedprice), writer);
}

fn write_f64_column<I, W: Write>(column: I, writer: &mut TrackedWriter<W>)
where
    I: Iterator<Item = f64>,
{
    let mut buffer = [0u8; 2];
    for value in column {
        let compressed = compress_f64(value);
        buffer.copy_from_slice(&compressed.to_le_bytes());
        writer.write_all(&buffer).expect("Failed to write");
    }
}

pub fn compress_f64(f: f64) -> u16 {
    let f = f * 100.0;
    let f = f.round();
    f as u16
}
pub struct TrackedWriter<W: Write> {
    writer: std::io::BufWriter<W>,
    bytes_written: usize,
}

impl<W: Write> TrackedWriter<W> {
    pub fn new(writer: W) -> Self {
        TrackedWriter {
            writer: std::io::BufWriter::new(writer),
            bytes_written: 0,
        }
    }

    #[allow(dead_code)]
    fn bytes_written(&self) -> usize {
        self.bytes_written
    }
}

impl<W: Write> Write for TrackedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes = self.writer.write(buf)?;
        self.bytes_written += bytes;
        Ok(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}
impl<W: Write> TrackedWriter<W> {
    #[allow(dead_code)]
    pub fn into_inner(self) -> std::io::BufWriter<W> {
        self.writer
    }
}

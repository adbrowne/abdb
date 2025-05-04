pub mod io;
pub mod string_column;
pub mod f64_column;
use std::{
    cmp::min,
    io::{BufRead, Read, Write},
};
static MAX_ROW_GROUP_SIZE: usize = 8000;
use string_column::StringColumnReader;
use f64_column::write_f64_column;
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
    pub count: u64,
    pub sum_qty: u64,
    pub sum_base_price: u64,
    pub sum_discount: u64,
    pub sum_tax: u64,
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

pub fn update_state_from_row_group<R: Read>(
    reader: &mut std::io::BufReader<R>,
    state: &mut Vec<Option<QueryOneStateColumn>>,
) -> () {
    let item_count = read_u16(reader);
    let linestatus_column = StringColumnReader::new(reader);
    let mut linestatus = linestatus_column.compressed_iterator();
    let returnflag_column = StringColumnReader::new(reader);
    let mut returnflag = returnflag_column.compressed_iterator();
    let quantity = read_u16_column(reader, item_count);
    let discount = read_u16_column(reader, item_count);
    let tax = read_u16_column(reader, item_count);
    let extendedprice = read_u16_column(reader, item_count);

    let mut index: usize = 0;
    let mut current_returnflag = None;
    let mut current_returnflag_count = 0;
    let mut current_linestatus = None;
    let mut current_linestatus_count = 0;
    
    while index < item_count as usize {
        // Get new values if we've used up the current ones
        if current_returnflag_count == 0 {
            let (rf_char, rf_count) = returnflag.next().expect("Returnflag ended early");
            current_returnflag = Some(rf_char);
            current_returnflag_count = *rf_count;
        }
        
        if current_linestatus_count == 0 {
            let (ls_char, ls_count) = linestatus.next().expect("Linestatus ended early");
            current_linestatus = Some(ls_char);
            current_linestatus_count = *ls_count;
        }
        
        let run_length = min(current_returnflag_count as usize, current_linestatus_count as usize);
        
        let rf_char = current_returnflag.unwrap();
        let ls_char = current_linestatus.unwrap();
        let current_index = get_state_index(rf_char, ls_char);
        
        let current_state = state[current_index].get_or_insert_with(|| QueryOneStateColumn::default());
        
        // Update the state with this run
        current_state.count += run_length as u64;
        current_state.sum_qty += sum_u16s(&quantity, index, run_length);
        current_state.sum_base_price += sum_u16s(&extendedprice, index, run_length);
        current_state.sum_discount += sum_u16s(&discount, index, run_length);
        current_state.sum_tax += sum_u16s(&tax, index, run_length);

        // Update the remaining counts
        current_returnflag_count -= run_length as u32;
        current_linestatus_count -= run_length as u32;
        
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

pub fn get_state_index(returnflag: &u8, linestatus: &u8) -> usize {
    (*returnflag as usize) * 256 + (*linestatus as usize)
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
    let lineitems_column = StringColumnReader::new_from_strings(lineitems.iter().map(|x| x.l_linestatus.as_str()).collect());
    lineitems_column.write(writer);
    let returnflag_column = StringColumnReader::new_from_strings(lineitems.iter().map(|x| x.l_returnflag.as_str()).collect());
    returnflag_column.write(writer);
    write_f64_column(lineitems.iter().map(|x| x.l_quantity), writer);
    write_f64_column(lineitems.iter().map(|x| x.l_discount), writer);
    write_f64_column(lineitems.iter().map(|x| x.l_tax), writer);
    write_f64_column(lineitems.iter().map(|x| x.l_extendedprice), writer);
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

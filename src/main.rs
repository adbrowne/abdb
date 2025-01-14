use std::{
    array::{self}, cmp::{max, min}, io::{BufRead, Read, Write}, result
};

use clap::{Parser, Subcommand};
use duckdb::{Connection, Row};
mod tests;
#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    WriteLineItems,
    WriteLineItemsColumn,
    RunQuery1,
    RunQuery1Column,
    ReadFile,
}

#[derive(Debug, Default, PartialEq, Clone)]
struct QueryOneState {
    count: u64,
    sum_qty: f64,
    sum_base_price: f64,
    sum_disc_price: f64,
    sum_charge: f64,
}

fn read_file() {
    let file = std::fs::File::open("lineitems_column.bin").expect("Failed to open file");

    let mut reader = std::io::BufReader::new(file);

    loop {
        let mut buffer = [0u8; 8000]; // 1 byte for l_returnflag, 1 byte for l_linestatus, 8 bytes for l_quantity
        match reader.read_exact(&mut buffer) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => panic!("Failed to read from file: {}", e),
        }
    }
}

fn read_row_group<R: Read>(reader: &mut std::io::BufReader<R>) -> Vec<LineItem> {
    let item_count = read_u16(reader);
    let mut lineitems = Vec::new();
    let linestatus = read_string_column(reader, item_count);
    let returnflag = read_string_column(reader, item_count);
    let quantity = read_f64_column(reader, item_count);
    let discount = read_f64_column(reader, item_count);
    let tax = read_f64_column(reader, item_count);
    let extendedprice = read_f64_column(reader, item_count);

    for i in 0..item_count {
        lineitems.push(LineItem {
            l_linestatus: linestatus[i as usize].clone(),
            l_returnflag: returnflag[i as usize].clone(),
            l_quantity: quantity[i as usize],
            l_extendedprice: extendedprice[i as usize],
            l_discount: discount[i as usize],
            l_tax: tax[i as usize],
        });
    }

    lineitems
}

fn read_u16<R: Read>(reader: &mut std::io::BufReader<R>) -> u16 {
    let mut buffer = [0u8; 2];
    reader.read_exact(&mut buffer).expect("Failed to read");
    let item_count = u16::from_le_bytes(buffer);
    item_count
}

fn read_u16_column<R: Read>(reader: &mut std::io::BufReader<R>, item_count: u16) -> U16column {
    let mut data = [0u16; MAX_ROW_GROUP_SIZE];
    reader.read_exact(bytemuck::cast_slice_mut(&mut data[0..item_count as usize])).expect("Failed to read");
    U16column {
        data,
        size: item_count as usize,
    }
}

fn read_f64_column<R: Read>(reader: &mut std::io::BufReader<R>, item_count: u16) -> Vec<f64> {
    read_u16_column(reader, item_count)
        .data
        .iter()
        .map(|x| decompress_f64(*x))
        .collect()
}

fn read_u8_string_column<R: Read>(reader: &mut std::io::BufReader<R>, item_count: u16) -> [(u8,u16); MAX_ROW_GROUP_SIZE] {
    let mut column = [(0u8,0u16); MAX_ROW_GROUP_SIZE];
    let mut i = 0;
    let mut remaining = item_count as i16;
    while remaining > 0 {
        let (value, count ) = read_u8_string_entry(reader);
        column[i] = (value, count);
        remaining -= count as i16;
        i += 1;
    }
    column
}

fn read_u8_string_entry<R: Read>(reader: &mut std::io::BufReader<R>) -> (u8, u16){
    let mut buffer = [0u8; 3]; 
    reader.read_exact(&mut buffer).expect("Failed to read");
    let count = u16::from_le_bytes([buffer[1], buffer[2]]);
    (buffer[0], count)
}

fn read_string_column<R: Read>(
    reader: &mut std::io::BufReader<R>,
    item_count: u16,
) -> Vec<std::string::String> {
    let mut result = Vec::new();
    for (u8_value, repeat_count) in read_u8_string_column(reader, item_count).iter() {
        let value = String::from_utf8(vec![*u8_value]).expect("Failed to convert to string");
        let r = vec![value; *repeat_count as usize];
        result.extend(r);
    }
    result
}
struct TrackedWriter<W: Write> {
    writer: std::io::BufWriter<W>,
    bytes_written: usize,
}

impl<W: Write> TrackedWriter<W> {
    fn new(writer: W) -> Self {
        TrackedWriter {
            writer: std::io::BufWriter::new(writer),
            bytes_written: 0,
        }
    }

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
    fn into_inner(self) -> std::io::BufWriter<W> {
        self.writer
    }
}
fn write_row_group<W: Write>(lineitems: &[LineItem], writer: &mut TrackedWriter<W>) {
    let item_count = (lineitems.len() as u16).to_le_bytes();
    writer.write(&item_count).expect("Failed to write");
    write_string_column(lineitems.iter().map(|x| &x.l_linestatus), writer);
    write_string_column(lineitems.iter().map(|x| &x.l_returnflag), writer);
    write_f64_column(lineitems.iter().map(|x| x.l_quantity), writer);
    write_f64_column(lineitems.iter().map(|x| x.l_discount), writer);
    write_f64_column(lineitems.iter().map(|x| x.l_tax), writer);
    write_f64_column(lineitems.iter().map(|x| x.l_extendedprice), writer);
}

fn write_string_column<'a, I, W: Write>(column: I, writer: &mut TrackedWriter<W>)
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
            .write_all(&[value.as_bytes()[0], (count as u16).to_le_bytes()[0], (count as u16).to_le_bytes()[1]])
            .expect("Failed to write");
    }
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

fn query_1() {
    let file = std::fs::File::open("lineitems.bin").expect("Failed to open file");

    let mut reader = std::io::BufReader::new(file);
    let mut state: [Option<QueryOneState>; 256 * 256] = array::from_fn(|_x| None);

    loop {
        // 1 byte for l_returnflag
        // 1 byte for l_linestatus
        // 1 byte for l_quantity,
        // 1 byte for l_extended_price
        // 1 byte for l_discount
        // 1 byte for l_tax
        let mut buffer = [0u8; 10];
        match reader.read_exact(&mut buffer) {
            Ok(_) => {
                let l_returnflag_u8 = buffer[0];
                let l_linestatus_u8 = buffer[1];
                let l_quantity =
                    decompress_f64(u16::from_le_bytes(buffer[2..4].try_into().unwrap()));
                let l_extended_price =
                    decompress_f64(u16::from_le_bytes(buffer[4..6].try_into().unwrap()));
                let l_discount =
                    decompress_f64(u16::from_le_bytes(buffer[6..8].try_into().unwrap()));
                let l_tax = decompress_f64(u16::from_le_bytes(buffer[8..10].try_into().unwrap()));

                let array_location = (l_returnflag_u8 as usize) * 256 + (l_linestatus_u8 as usize);
                let current_state = state[array_location].get_or_insert_default();
                current_state.sum_qty += l_quantity;
                current_state.sum_base_price += l_extended_price;
                current_state.sum_disc_price += l_extended_price * (1.0 - l_discount);
                current_state.sum_charge += l_extended_price * (1.0 - l_discount) * (1.0 + l_tax);
                current_state.count += 1;
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => panic!("Failed to read from file: {}", e),
        }
    }

    print_state(state);
}

fn print_state(state: [Option<QueryOneState>; 256*256]) {
    for i in 0..256 {
        for j in 0..256 {
            if state[i * 256 + j] != None {
                let l_returnflag = String::from_utf8(vec![i as u8]).unwrap();
                let l_linestatus = String::from_utf8(vec![j as u8]).unwrap();
                println!(
                    "{}, {}, {:?} ",
                    l_returnflag,
                    l_linestatus,
                    state[i * 256 + j]
                );
            }
        }
    }
}

fn print_state_column(state: Vec<Option<QueryOneStateColumn>>) {
    for i in 0..256 {
        for j in 0..256 {
            if let Some(state_column) = &state[i * 256 + j] {
                let l_returnflag = String::from_utf8(vec![i as u8]).unwrap();
                let l_linestatus = String::from_utf8(vec![j as u8]).unwrap();
                let state = QueryOneState {
                    count: state_column.count,
                    sum_qty: state_column.sum_qty as f64 / 100.0,
                    sum_base_price: state_column.sum_base_price as f64 / 100.0,
                    sum_disc_price: state_column.sum_base_price as f64 / 100.0 * (1.0 - state_column.sum_discount as f64 / 100.0),
                    sum_charge: state_column.sum_base_price as f64 / 100.0 * (1.0 - state_column.sum_discount as f64 / 100.0) * (1.0 + state_column.sum_tax as f64 / 100.0),
                };
                println!("{}, {}, {:?}", l_returnflag, l_linestatus, state);
            }
        }
    }
}

fn compress_f64(f: f64) -> u16 {
    let f = f * 100.0;
    let f = f.round();
    f as u16
}

fn decompress_f64(f: u16) -> f64 {
    f as f64 / 100.0
}

#[derive(Debug, PartialEq, Clone)]
struct LineItem {
    l_returnflag: String,
    l_linestatus: String,
    l_quantity: f64,
    l_extendedprice: f64,
    l_discount: f64,
    l_tax: f64,
}

impl<'a> From<&Row<'a>> for LineItem {
    fn from(row: &Row) -> Self {
        LineItem {
            l_returnflag: row.get(0).unwrap(),
            l_linestatus: row.get(1).unwrap(),
            l_quantity: row.get(2).unwrap(),
            l_extendedprice: row.get(3).unwrap(),
            l_discount: row.get(4).unwrap(),
            l_tax: row.get(5).unwrap(),
        }
    }
}

pub struct QueryResult<'a> {
    stmt: duckdb::Statement<'a>,
}

impl<'a> QueryResult<'a> {
    fn new(conn: &'a Connection) -> Result<QueryResult<'a>, duckdb::Error> {
        let stmt = conn.prepare("SELECT l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax FROM lineitem where l_shipdate <= CAST('1998-09-02' AS date)")?;
        Ok(QueryResult { stmt })
    }

    fn iter_records(
        &'a mut self,
    ) -> Result<impl Iterator<Item = Result<LineItem, duckdb::Error>> + 'a, duckdb::Error> {
        Ok(self.stmt.query_map([], |row| Ok(LineItem::from(row)))?)
    }
}

fn save_data() {
    let conn = duckdb::Connection::open("db").unwrap();
    let mut result = QueryResult::new(&conn).unwrap();

    let file = std::fs::File::create("lineitems.bin").expect("Failed to create file");
    let mut writer = std::io::BufWriter::new(file);

    for row_result in result.iter_records().unwrap() {
        let lineitem = row_result.unwrap();
        write_line_item(&mut writer, lineitem);
    }
}

fn write_line_item(writer: &mut std::io::BufWriter<std::fs::File>, lineitem: LineItem) {
    let ls_byte: u8 = lineitem.l_linestatus.as_bytes()[0];
    writer
        .write(&[lineitem.l_returnflag.as_bytes()[0], ls_byte])
        .expect("Failed to write");
    writer
        .write_all(&compress_f64(lineitem.l_quantity).to_le_bytes())
        .expect("Failed to write");
    writer
        .write_all(&compress_f64(lineitem.l_extendedprice).to_le_bytes())
        .expect("Failed to write");
    writer
        .write_all(&compress_f64(lineitem.l_discount).to_le_bytes())
        .expect("Failed to write");
    writer
        .write_all(&compress_f64(lineitem.l_tax).to_le_bytes())
        .expect("Failed to write");
}

fn main() {
    println!("Hello, world!2");
    let cli = Cli::parse();

    // You can check for the existence of subcommands, and if found use their
    // matches just as you would the top level cmd
    match &cli.command {
        Some(Commands::WriteLineItems {}) => {
            save_data();
        }
        Some(Commands::WriteLineItemsColumn {}) => {
            save_data_column();
        }
        Some(Commands::RunQuery1Column {}) => {
            query_1_column();
        }
        Some(Commands::RunQuery1) => {
            query_1();
        }
        Some(Commands::ReadFile) => {
            read_file();
        }
        None => {}
    }

    //query_1();
}

static MAX_ROW_GROUP_SIZE: usize = 8000;
struct U16column {
    data: [u16; MAX_ROW_GROUP_SIZE],
    size: usize,
}
fn get_state_index(returnflag: u8, linestatus: u8) -> usize {
    (returnflag as usize) * 256 + (linestatus as usize)
}

fn update_state_from_row_group<R: Read>(reader: &mut std::io::BufReader<R>, state: &mut Vec<Option<QueryOneStateColumn>>) -> () {
    let item_count = read_u16(reader);
    let mut linestatus = read_u8_string_column(reader, item_count);
    let mut returnflag = read_u8_string_column(reader, item_count);
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

        let current_state = state[get_state_index(last_returnflag.0, last_linestatus.0)].get_or_insert_default();

        current_state.sum_qty += quantity.data[index as usize..(index + run_length) as usize].iter().map(|x| *x as u64).sum::<u64>();
        current_state.sum_base_price += extendedprice.data[index as usize..(index + run_length) as usize].iter().map(|x| *x as u64).sum::<u64>();
        current_state.sum_discount += discount.data[index as usize..(index + run_length) as usize].iter().map(|x| *x as u64).sum::<u64>();
        current_state.sum_tax += tax.data[index as usize..(index + run_length) as usize].iter().map(|x| *x as u64).sum::<u64>();

        returnflag[last_returnflag_index].1 -= run_length as u16;
        linestatus[last_linestatus_index].1 -= run_length as u16;
        if returnflag[last_returnflag_index].1 == 0 as u16 {
            last_returnflag_index += 1;
        } 
        if linestatus[last_linestatus_index].1 == 0 as u16 {
            last_linestatus_index += 1;
        } 
        index += run_length;
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
struct QueryOneStateColumn {
    count: u64,
    sum_qty: u64,
    sum_base_price: u64,
    sum_discount: u64,
    sum_tax: u64,
}

fn query_1_column() {
    let file = std::fs::File::open("lineitems_column.bin").expect("Failed to open file");
    let mut reader = std::io::BufReader::new(file);
    let mut state: Vec<Option<QueryOneStateColumn>> = vec![None; 256*256];

    loop {
        if reader.fill_buf().unwrap().is_empty() {
            println!("End of file");
            break;
        }
        update_state_from_row_group(&mut reader, &mut state);
    }
    print_state_column(state);
}

fn save_data_column() {
    let conn = duckdb::Connection::open("db").unwrap();
    let mut result = QueryResult::new(&conn).unwrap();
    let file = std::fs::File::create("lineitems_column.bin").expect("Failed to create file");
    let mut writer = TrackedWriter::new(std::io::BufWriter::new(file));
    let mut batch = Vec::with_capacity(8000);
    println!("save_data_column");

    for row_result in result.iter_records().unwrap() {
        let lineitem = row_result.unwrap();
        batch.push(lineitem);

        if batch.len() == 8000 {
            batch.sort_by(|a, b| a.l_returnflag.cmp(&b.l_returnflag).then(a.l_linestatus.cmp(&b.l_linestatus)));
            write_row_group(&batch, &mut writer);
            batch.clear();
        }
    }

    if !batch.is_empty() {
        write_row_group(&batch, &mut writer);
    }
}

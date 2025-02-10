use std::{
    array::{self},
    cmp::min,
    io::{BufRead, Read, Write},
};

mod deltaread;

use abdb::*;
use clap::{command, Parser, Subcommand};
use datafusion::{arrow::datatypes::{DataType, Field, Int32Type, Schema}, parquet::schema::types::ColumnPath};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::*;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::
    arrow::array::{Float64Array, StringDictionaryBuilder}
;

use duckdb::{Connection, Row};
use std::sync::Arc;
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
    WriteLineItemsParquet,
    RunQuery1,
    RunQuery1Column,
    RunQuery1Parquet,
    RunQuery1Delta,
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

#[allow(dead_code)]
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
    u16::from_le_bytes(buffer)
}

fn read_u16_column<R: Read>(reader: &mut std::io::BufReader<R>, item_count: u16) -> U16column {
    let mut column = U16column::new();
    let mut buffer = vec![0u8; item_count as usize * 2]; // 2 bytes per u16
    reader
        .read_exact(&mut buffer)
        .expect("Failed to read");
    column.data.extend_from_slice(bytemuck::cast_slice(&buffer));
    
    column
}

fn read_f64_column<R: Read>(reader: &mut std::io::BufReader<R>, item_count: u16) -> Vec<f64> {
    read_u16_column(reader, item_count)
        .data
        .iter()
        .map(|x| decompress_f64(*x))
        .collect()
}

fn read_u8_string_column<R: Read>(
    reader: &mut std::io::BufReader<R>,
    item_count: u16,
    mut column: Vec<(u8, u32)>,
) -> Vec<(u8, u32)> {
    unsafe {
        column.set_len(0);
    }
    let mut remaining = item_count as i32; // Correctly set remaining to item_count
    while remaining > 0 {
        let (value, count) = read_u8_string_entry(reader);
        column.push((value, count));
        remaining -= count as i32;
    }
    column
}

fn read_u8_string_entry<R: Read>(reader: &mut std::io::BufReader<R>) -> (u8, u32) {
    let mut buffer = [0u8; 5];
    reader.read_exact(&mut buffer).expect("Failed to read");
    let count = u32::from_le_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]);
    (buffer[0], count)
}

fn read_string_column<R: Read>(
    reader: &mut std::io::BufReader<R>,
    item_count: u16,
) -> Vec<std::string::String> {
    let mut result = Vec::with_capacity(MAX_ROW_GROUP_SIZE);
    let column = Vec::with_capacity(item_count as usize);
    for (u8_value, repeat_count) in read_u8_string_column(reader, item_count, column).iter() {
        let value = String::from_utf8(vec![*u8_value]).expect("Failed to convert to string");
        let r = vec![value; *repeat_count as usize];
        result.extend(r);
    }
    result
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

fn print_state(state: [Option<QueryOneState>; 256 * 256]) {
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

fn lineitem_from_row(row: &Row) -> LineItem {
    LineItem {
        l_returnflag: row.get(0).unwrap(),
        l_linestatus: row.get(1).unwrap(),
        l_quantity: row.get(2).unwrap(),
        l_extendedprice: row.get(3).unwrap(),
        l_discount: row.get(4).unwrap(),
        l_tax: row.get(5).unwrap(),
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
        Ok(self.stmt.query_map([], |row| Ok(lineitem_from_row(row)))?)
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
        Some(Commands::WriteLineItemsParquet {}) => {
            //save_data_parquet();
            save_data_parquet_with_dictionary();
        }
        Some(Commands::RunQuery1Column {}) => {
            //query_1_column();
            query_1_column();
        }
        Some(Commands::RunQuery1Parquet) => {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(query_1_column_parquet());
        }
        Some(Commands::RunQuery1Delta) => {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(deltaread::query_1_delta(QUERY1_SQL));
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

static MAX_ROW_GROUP_SIZE: usize = 100000;
struct U16column {
    data: Vec<u16>,
    #[allow(dead_code)]
    size: usize,
}

impl U16column {
    fn new() -> U16column {
        U16column {
            data: Vec::with_capacity(MAX_ROW_GROUP_SIZE),
            size: 0,
        }
    }
}

fn get_state_index(returnflag: u8, linestatus: u8) -> usize {
    (returnflag as usize) * 256 + (linestatus as usize)
}

fn update_state_from_row_group<R: Read>(
    reader: &mut std::io::BufReader<R>,
    state: &mut [Option<QueryOneStateColumn>],
){
    let item_count = read_u16(reader);
    let linestatus_data = Vec::with_capacity(item_count as usize);
    let returnflag_data = Vec::with_capacity(item_count as usize);
    let mut linestatus = read_u8_string_column(reader, item_count, linestatus_data);
    let mut returnflag = read_u8_string_column(reader, item_count, returnflag_data);
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

        current_state.sum_qty += quantity.data[index..(index + run_length) as usize]
            .iter()
            .map(|x| *x as u64)
            .sum::<u64>();
        current_state.sum_base_price += extendedprice.data
            [index..(index + run_length) as usize]
            .iter()
            .map(|x| *x as u64)
            .sum::<u64>();
        current_state.sum_discount += discount.data[index..(index + run_length) as usize]
            .iter()
            .map(|x| *x as u64)
            .sum::<u64>();
        current_state.sum_tax += tax.data[index..(index + run_length) as usize]
            .iter()
            .map(|x| *x as u64)
            .sum::<u64>();

        returnflag[last_returnflag_index].1 -= run_length as u32;
        linestatus[last_linestatus_index].1 -= run_length as u32;
        if returnflag[last_returnflag_index].1 == 0 as u32 {
            last_returnflag_index += 1;
        }
        if linestatus[last_linestatus_index].1 == 0 as u32 {
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
    let mut state: Vec<Option<QueryOneStateColumn>> = vec![None; 256 * 256];

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
            batch.sort_by(|a, b| {
                a.l_returnflag
                    .cmp(&b.l_returnflag)
                    .then(a.l_linestatus.cmp(&b.l_linestatus))
            });
            write_row_group(&batch, &mut writer);
            batch.clear();
        }
    }

    if !batch.is_empty() {
        write_row_group(&batch, &mut writer);
    }
}


const QUERY1_SQL : &str = "
        SELECT 
            l_returnflag,
            l_linestatus,
            COUNT(*) as count,
            SUM(l_quantity) as sum_qty,
            SUM(l_extendedprice) as sum_base_price,
            SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
            SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
            AVG(l_quantity) as avg_qty,
            AVG(l_extendedprice) as avg_price,
            AVG(l_discount) as avg_disc
        FROM lineitem
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus";
    
async fn query_1_column_parquet() {
    let ctx = SessionContext::new();

    // Register the parquet file as a table
    ctx.register_parquet(
        "lineitem",
        //"lineitems_with_dictionary.parquet",
        "lineitems_with_dictionary_orig.parquet",
        ParquetReadOptions::default(),
    )
    .await
    .expect("Failed to register parquet file");

    let df = ctx.sql(QUERY1_SQL).await.expect("Failed to execute query");
    let results = df.collect().await.expect("Failed to collect results");

    // Print results
    for batch in results {
        println!("{:?}", batch);
    }
}

fn save_data_parquet_with_dictionary() {
    let conn = duckdb::Connection::open("db").unwrap();
    let mut result = QueryResult::new(&conn).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "l_returnflag",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new(
            "l_linestatus",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
    ]));

    let returnflag_col = ColumnPath::new(vec![String::from("l_returnflag")]);
    let linestatus_col = ColumnPath::new(vec![String::from("l_linestatus")]);
    let writer_properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_dictionary_enabled(true)
        .set_column_dictionary_enabled(returnflag_col, true)
        .set_column_dictionary_enabled(linestatus_col, true)
        .build();

    let file =
        std::fs::File::create("lineitems_with_dictionary.parquet").expect("Failed to create file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(writer_properties))
        .expect("Failed to create writer");

    let mut l_returnflag_builder =
        StringDictionaryBuilder::<datafusion::arrow::datatypes::Int32Type>::new();
    let mut l_linestatus_builder =
        StringDictionaryBuilder::<datafusion::arrow::datatypes::Int32Type>::new();
    let mut l_quantity = Vec::new();
    let mut l_extendedprice = Vec::new();
    let mut l_discount = Vec::new();
    let mut l_tax = Vec::new();

    let batch_size = 2048;

    for row_result in result.iter_records().unwrap() {
        let lineitem = row_result.unwrap();
        l_returnflag_builder.append_value(&lineitem.l_returnflag);
        l_linestatus_builder.append_value(&lineitem.l_linestatus);
        l_quantity.push(lineitem.l_quantity);
        l_extendedprice.push(lineitem.l_extendedprice);
        l_discount.push(lineitem.l_discount);
        l_tax.push(lineitem.l_tax);

        if l_quantity.len() == batch_size {
            write_parquet_batch_with_dictionary(
                schema.clone(),
                &mut writer,
                &mut l_returnflag_builder,
                &mut l_linestatus_builder,
                l_quantity,
                l_extendedprice,
                l_discount,
                l_tax,
            );
            l_quantity = Vec::new();
            l_extendedprice = Vec::new();
            l_discount = Vec::new();
            l_tax = Vec::new();
        }
    }

    write_parquet_batch_with_dictionary(
        schema,
        &mut writer,
        &mut l_returnflag_builder,
        &mut l_linestatus_builder,
        l_quantity,
        l_extendedprice,
        l_discount,
        l_tax,
    );
    writer.close().expect("Failed to close writer");
    println!("Done writing parquet file");
}

fn write_parquet_batch_with_dictionary(
    schema: Arc<Schema>,
    writer: &mut ArrowWriter<std::fs::File>,
    l_returnflag_builder: &mut StringDictionaryBuilder<Int32Type>,
    l_linestatus_builder: &mut StringDictionaryBuilder<Int32Type>,
    l_quantity: Vec<f64>,
    l_extendedprice: Vec<f64>,
    l_discount: Vec<f64>,
    l_tax: Vec<f64>,
) {
    let l_returnflag = l_returnflag_builder.finish();
    let l_linestatus = l_linestatus_builder.finish();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(l_returnflag),
            Arc::new(l_linestatus),
            Arc::new(Float64Array::from(l_quantity)),
            Arc::new(Float64Array::from(l_extendedprice)),
            Arc::new(Float64Array::from(l_discount)),
            Arc::new(Float64Array::from(l_tax)),
        ],
    )
    .expect("Failed to create record batch");

    writer.write(&batch).expect("Failed to write batch");
}

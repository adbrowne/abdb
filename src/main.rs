use std::{
    array::{self},
    io::{Read, Write},
};

mod deltaread;

use abdb::*;
use clap::{command, Parser, Subcommand};
use datafusion::arrow::array::{Float64Array, StringDictionaryBuilder};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::*;
use datafusion::{
    arrow::datatypes::{DataType, Field, Int32Type, Schema},
    parquet::schema::types::ColumnPath,
};

use abdb::f64_column::compress_f64;
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

fn query_1_column() {
    let state = abdb::query_1_column("lineitems_column.bin");
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
            write_batch(&mut writer, &mut batch);
            batch.clear();
        }
    }

    if !batch.is_empty() {
        write_batch(&mut writer, &mut batch);
    }
}

const QUERY1_SQL: &str = "
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
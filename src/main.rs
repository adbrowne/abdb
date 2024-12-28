use std::{
    array::{self}, io::{Read, Write}
};

use clap::{Parser, Subcommand};
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
    let file = std::fs::File::open("lineitems.bin").expect("Failed to open file");

    let mut reader = std::io::BufReader::new(file);

    loop {
        let mut buffer = [0u8; 34]; // 1 byte for l_returnflag, 1 byte for l_linestatus, 8 bytes for l_quantity
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
    let item_count =u16::from_le_bytes(buffer);
    item_count
}

fn read_f64_column<R: Read>(reader: &mut std::io::BufReader<R>, item_count: u16) -> Vec<f64> {
    let mut column = Vec::new();
    for _ in 0..item_count {
        let mut buffer = [0u8; 2];
        reader.read_exact(&mut buffer).expect("Failed to read");
        let value = decompress_f64(u16::from_le_bytes(buffer));
        column.push(value);
    }
    column
}

fn read_string_column<R: Read>(reader: &mut std::io::BufReader<R>, item_count: u16) -> Vec<std::string::String> {
    let mut column = Vec::new();
    for _ in 0..item_count {
        let mut buffer = [0u8; 1];
        reader.read_exact(&mut buffer).expect("Failed to read");
        column.push(String::from_utf8(buffer.to_vec()).expect("Failed to convert to string"));
    }
    column
}

fn write_row_group<W: Write>(lineitems: &[LineItem], writer: &mut std::io::BufWriter<W>) {
    let item_count = (lineitems.len() as u16).to_le_bytes();
    writer.write(&item_count).expect("Failed to write");
    write_string_column(lineitems.iter().map(|x| &x.l_linestatus), writer);
    write_string_column(lineitems.iter().map(|x| &x.l_returnflag), writer);
    write_f64_column(lineitems.iter().map(|x| x.l_quantity), writer);
    write_f64_column(lineitems.iter().map(|x| x.l_discount), writer);
    write_f64_column(lineitems.iter().map(|x| x.l_tax), writer);
    write_f64_column(lineitems.iter().map(|x| x.l_extendedprice), writer);
}

fn write_string_column<'a, I, W: Write>(column: I, writer: &mut std::io::BufWriter<W>)
where
    I: Iterator<Item = &'a String>,
{
    for value in column {
        writer
            .write(&[value.as_bytes()[0]])
            .expect("Failed to write");
    }
}

fn write_f64_column<I, W: Write>(column: I, writer: &mut std::io::BufWriter<W>)
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

fn save_data() {
    let conn = duckdb::Connection::open("db").unwrap();
    let mut stmt = conn.prepare("SELECT l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax FROM lineitem where l_shipdate <= CAST('1998-09-02' AS date)").unwrap();

    let mut rows = stmt.query([]).unwrap().mapped(|row| {
        Ok(LineItem {
            l_returnflag: row.get(0).unwrap(),
            l_linestatus: row.get(1).unwrap(),
            l_quantity: row.get(2).unwrap(),
            l_extendedprice: row.get(3).unwrap(),
            l_discount: row.get(4).unwrap(),
            l_tax: row.get(5).unwrap(),
        })
    });

    let file = std::fs::File::create("lineitems.bin").expect("Failed to create file");
    let mut writer = std::io::BufWriter::new(file);

    /*
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,*/

    while let Some(row_result) = rows.next() {
        let lineitem = row_result.unwrap();
        //let lineitem = row.unwrap();
        write_line_item(&mut writer, lineitem);
    }
    // while let Some(row) = rows.next().unwrap() {
    //     let lineitem = LineItem {
    //         l_returnflag: row.get(0).unwrap(),
    //         l_linestatus: row.get(1).unwrap(),
    //         l_quantity: row.get(2).unwrap(),
    //         l_extendedprice: row.get(3).unwrap(),
    //         l_discount: row.get(4).unwrap(),
    //         l_tax: row.get(5).unwrap(),
    //     };

    //     write_line_item(&mut writer, lineitem);
    // }
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

fn query_1_column() {
    todo!()
}

fn save_data_column() {
    todo!()
}

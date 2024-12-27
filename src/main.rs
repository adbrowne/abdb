use std::{array::{self}, io::{Read, Write}};

use serde::{Deserialize, Serialize};

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    WriteLineItems,
    RunQuery1,
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

fn read_file(){
    let file = std::fs::File::open("lineitems.bin").expect("Failed to open file");
    
    let mut reader = std::io::BufReader::new(file);
    
    loop {
        let mut buffer = [0u8; 34]; // 1 byte for l_returnflag, 1 byte for l_linestatus, 8 bytes for l_quantity
        match reader.read_exact(&mut buffer) {
            Ok(_) => { }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => panic!("Failed to read from file: {}", e),
        }
    }
}

fn query_1() {
    let file = std::fs::File::open("lineitems.bin").expect("Failed to open file");
    
    let mut reader = std::io::BufReader::new(file);
    let mut state : [Option<QueryOneState>; 256*256] = array::from_fn(|_x| None);
    
    loop {
        let mut buffer = [0u8; 34]; // 1 byte for l_returnflag, 1 byte for l_linestatus, 8 bytes for l_quantity
        match reader.read_exact(&mut buffer) {
            Ok(_) => {
                let l_returnflag_u8 = buffer[0];
                let l_linestatus_u8 = buffer[1];
                let l_quantity = f64::from_le_bytes(buffer[2..10].try_into().unwrap());
                let l_extended_price = f64::from_le_bytes(buffer[10..18].try_into().unwrap());
                let l_discount = f64::from_le_bytes(buffer[18..26].try_into().unwrap());
                let l_tax = f64::from_le_bytes(buffer[26..34].try_into().unwrap());

                let array_location = (l_returnflag_u8 as usize) * 256 + (l_linestatus_u8 as usize);
                let current_state =state[array_location].get_or_insert_default();
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
                println!("{}, {}, {:?} ", l_returnflag, l_linestatus, state[i * 256 + j]);
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct LineItem {
    l_returnflag : String,
    l_linestatus : String,
    l_quantity : f64 // TODO: Should be Decimal
}

fn save_data() {
    let conn = duckdb::Connection::open("db").unwrap();
    let mut stmt = conn.prepare("SELECT l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax FROM lineitem where l_shipdate <= CAST('1998-09-02' AS date)").unwrap();
    
    let mut rows = stmt.query([]).unwrap();

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
    while let Some(row) = rows.next().unwrap() {
        let l_returnflag: String = row.get(0).unwrap();
        let l_linestatus: String = row.get(1).unwrap();
        let l_quantity: f64 = row.get(2).unwrap();
        let l_extended_price: f64 = row.get(3).unwrap();
        let l_discount: f64 = row.get(4).unwrap();
        let l_tax: f64 = row.get(5).unwrap();

        let ls_byte: u8 = l_linestatus.as_bytes()[0];
        let returnflag_byte: u8 = l_returnflag.as_bytes()[0];
        writer.write(&[returnflag_byte, ls_byte]).expect("Failed to write");
        writer.write_all(&l_quantity.to_le_bytes()).expect("Failed to write");
        writer.write_all(&l_extended_price.to_le_bytes()).expect("Failed to write");
        writer.write_all(&l_discount.to_le_bytes()).expect("Failed to write");
        writer.write_all(&l_tax.to_le_bytes()).expect("Failed to write");
    }
}

fn main() {
    println!("Hello, world!2");
    let cli = Cli::parse();

    // You can check for the existence of subcommands, and if found use their
    // matches just as you would the top level cmd
    match &cli.command {
        Some(Commands::WriteLineItems    {}) => {
            save_data();
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

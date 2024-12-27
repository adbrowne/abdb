use std::{collections::HashMap, hash::Hash, io::{Read, Write}};

use ahash::AHashMap;
use bincode::de::read;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

fn query_1() {
    /* 
            SELECT
                l_returnflag,
                l_linestatus,
                sum(l_quantity) AS sum_qty,
                sum(l_extendedprice) AS sum_base_price,
                sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                avg(l_quantity) AS avg_qty,
                avg(l_extendedprice) AS avg_price,
                avg(l_discount) AS avg_disc,
                count(*) AS count_order
            FROM
                lineitem
            WHERE
                l_shipdate <= CAST('1998-09-02' AS date)
            GROUP BY
                l_returnflag,
                l_linestatus
            ORDER BY
                l_returnflag,
                l_linestatus; */
    
    let file = std::fs::File::open("lineitems.bin").expect("Failed to open file");
    
    let mut reader = std::io::BufReader::new(file);
    // let mut state: AHashMap<(u8, u8), f64> = AHashMap::new();
    let mut state2 = [0.0; 256 * 256];
    let mut count = 0;
    
    loop {
        let mut buffer = [0u8; 10]; // 1 byte for l_returnflag, 1 byte for l_linestatus, 8 bytes for l_quantity
        match reader.read_exact(&mut buffer) {
            Ok(_) => {
                let l_returnflag_u8 = buffer[0];
                let l_linestatus_u8 = buffer[1];
                // let l_returnflag = String::from_utf8_lossy(&buffer[0..1]).to_string();
                // let l_linestatus = String::from_utf8_lossy(&buffer[1..2]).to_string();
                let l_quantity = f64::from_le_bytes(buffer[2..10].try_into().unwrap());

                let key = (l_returnflag_u8, l_linestatus_u8);
                // let entry = state.entry(key).or_insert(0.0);
                // *entry += l_quantity;
                state2[(l_returnflag_u8 as usize) * 256 + l_linestatus_u8 as usize] += l_quantity;
                count += 1;
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => panic!("Failed to read from file: {}", e),
        }
    }

    println!("Count: {}", count);
    // for x in state.iter() {
    //     let l_returnflag = String::from_utf8(vec![x.0 .0]).unwrap();
    //     let l_linestatus = String::from_utf8(vec![x.0 .1]).unwrap();
    //     println!("{}, {}, {} ", l_returnflag, l_linestatus, x.1);
    // }

    for i in 0..256 {
        for j in 0..256 {
            if state2[i * 256 + j] != 0.0 {
                let l_returnflag = String::from_utf8(vec![i as u8]).unwrap();
                let l_linestatus = String::from_utf8(vec![j as u8]).unwrap();
                println!("{}, {}, {} ", l_returnflag, l_linestatus, state2[i * 256 + j]);
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
    let mut stmt = conn.prepare("SELECT l_returnflag, l_linestatus, l_quantity FROM lineitem").unwrap();
    
    let mut rows = stmt.query([]).unwrap();

    let file = std::fs::File::create("lineitems.bin").expect("Failed to create file");
    let mut writer = std::io::BufWriter::new(file);

    while let Some(row) = rows.next().unwrap() {
        let l_returnflag: String = row.get(0).unwrap();
        let l_linestatus: String = row.get(1).unwrap();
        let l_quantity: f64 = row.get(2).unwrap();

        let ls_byte: u8 = l_linestatus.as_bytes()[0];
        let returnflag_byte: u8 = l_returnflag.as_bytes()[0];
        writer.write(&[returnflag_byte, ls_byte]).expect("Failed to write");
        writer.write_all(&l_quantity.to_le_bytes()).expect("Failed to write");
    }
}

fn main() {
    println!("Hello, world!2");
    //save_data();
    query_1();
}

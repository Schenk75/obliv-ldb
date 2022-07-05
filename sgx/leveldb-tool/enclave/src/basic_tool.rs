#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

#[cfg(feature = "mesalock_sgx")]
use std::string::String;

use rusty_leveldb::{LdbIterator, DB};

use std::io::{self, Write};

pub fn get(db: &mut DB, k: &str) {
    match db.get(k.as_bytes()) {
        Some(v) => {
            if let Ok(s) = String::from_utf8(v.clone()) {
                println!("{} => {}", k, s);
            } else {
                println!("{} => {:?}", k, v);
            }
        }
        None => println!("{} => <not found>", k),
    }
}

pub fn put(db: &mut DB, k: &str, v: &str) {
    db.put(k.as_bytes(), v.as_bytes()).unwrap();
    db.flush().unwrap();
}

pub fn delete(db: &mut DB, k: &str) {
    db.delete(k.as_bytes()).unwrap();
    db.flush().unwrap();
}

pub fn iter(db: &mut DB) {
    let mut it = db.new_iter().unwrap();
    let (mut k, mut v) = (vec![], vec![]);
    let mut out = io::BufWriter::new(io::stdout());
    while it.advance() {
        it.current(&mut k, &mut v);
        out.write_all(&k).unwrap();
        out.write_all(b" => ").unwrap();
        out.write_all(&v).unwrap();
        out.write_all(b"\n").unwrap();
    }
}

pub fn compact(db: &mut DB, from: &str, to: &str) {
    db.compact_range(from.as_bytes(), to.as_bytes()).unwrap();
}
#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

#[cfg(feature = "mesalock_sgx")]
use std::string::String;
use std::io::{self, BufRead};
use std::path::Path;
use std::sgxfs::OpenOptions;

use rusty_leveldb::DB;

fn update_count(w: &str, db: &mut DB) -> Option<()> {
    let mut count: usize = 0;
    if let Some(v) = db.get(w.as_bytes()) {
        let s = String::from_utf8(v).unwrap();
        count = usize::from_str_radix(&s, 10).unwrap();
    }
    count += 1;
    let s = count.to_string();
    db.put(w.as_bytes(), s.as_bytes()).unwrap();
    Some(())
}

pub fn run(mut db: DB, file_path: &str) -> io::Result<()> {
    let f = OpenOptions::new().read(true).open(Path::new(file_path))?;
    for line in io::BufReader::new(f).lines() {
        for word in line.unwrap().split_whitespace() {
            let mut word = word.to_ascii_lowercase();
            word.retain(|c| c.is_ascii_alphanumeric());
            update_count(&word, &mut db);
        }
    }

    Ok(())
}
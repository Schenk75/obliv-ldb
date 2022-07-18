#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

use rand::Rng;
use rand::distributions::Alphanumeric;
use std::iter;
#[cfg(feature = "mesalock_sgx")]
use std::string::String;

use rusty_leveldb::CompressionType;
use rusty_leveldb::Options;
use rusty_leveldb::DB;

use std::untrusted::time::InstantEx;
use std::time::Instant;

const KEY_LEN: usize = 4;
const VAL_LEN: usize = 8;

fn gen_string(len: usize) -> String {
    let mut rng = rand::thread_rng();
    let value: String = iter::repeat(())
        .map(|()| rng.sample(Alphanumeric))
        .take(len)
        .collect();
    value.to_lowercase()
}

fn write(db: &mut DB, n: usize) {
    let write_start = Instant::now();
    for _ in 0..n {
        let (k, v) = (gen_string(KEY_LEN), gen_string(VAL_LEN));

        db.put(k.as_bytes(), v.as_bytes()).unwrap();
    }

    {
        let flush_start = Instant::now();
        db.flush().unwrap();
        println!("write flush duration {:?}", flush_start.elapsed());
    }
    println!("write {} entries duration {:?}", n, write_start.elapsed());
}

fn read(db: &mut DB, n: usize) -> usize {
    let mut succ = 0;
    let start = Instant::now();
    for _ in 0..n {
        let k = gen_string(KEY_LEN);

        if let Some(_) = db.get(k.as_bytes()) {
            succ += 1;
        }
    }
    println!("random read {} entries duration {:?}", n, start.elapsed());
    succ
}

pub fn stress_test() {
    let n = 100_000;
    let m = 10;
    let path = "stresstestdb";
    let mut entries = 0;

    for i in 0..m {
        let key = [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 
            0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 0x09, 0x08,];
        let mut opt = Options::new_disk_db_with(key);
        opt.compression_type = CompressionType::CompressionSnappy;
        let mut db = DB::open(path, opt).unwrap();
        write(&mut db, n);
        entries += n;
        println!("[.] Wrote {} entries ({}/{})", entries, i + 1, m);

        let s = read(&mut db, n);
        println!("[.] Read back {} entries (found {}) ({}/{})", n, s, i + 1, m);
    }
}

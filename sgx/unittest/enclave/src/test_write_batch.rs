#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

use std::iter::Iterator;
use rusty_leveldb::write_batch::*;

pub fn test_write_batch() {
    let mut b = WriteBatch::new();
    let entries = vec![
        ("abc".as_bytes(), "def".as_bytes()),
        ("123".as_bytes(), "456".as_bytes()),
        ("xxx".as_bytes(), "yyy".as_bytes()),
        ("zzz".as_bytes(), "".as_bytes()),
        ("010".as_bytes(), "".as_bytes()),
    ];

    for &(k, v) in entries.iter() {
        if !v.is_empty() {
            b.put(k, v);
        } else {
            b.delete(k)
        }
    }

    println!("{:?}", b.entries);
    assert_eq!(b.byte_size(), 49);
    assert_eq!(b.iter().count(), 5);

    let mut i = 0;

    for (k, v) in b.iter() {
        assert_eq!(k, entries[i].0);

        match v {
            None => assert!(entries[i].1.is_empty()),
            Some(v_) => assert_eq!(v_, entries[i].1),
        }

        i += 1;
    }

    assert_eq!(i, 5);
    assert_eq!(b.encode(1).len(), 49);
}
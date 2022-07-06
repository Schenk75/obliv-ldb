#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

use rusty_leveldb::test_util::*;
use rusty_leveldb::LdbIterator;

pub fn test_test_util_basic() {
    let v = vec![
        ("abc".as_bytes(), "def".as_bytes()),
        ("abd".as_bytes(), "deg".as_bytes()),
    ];
    let mut iter = TestLdbIter::new(v);
    assert_eq!(
        iter.next(),
        Some((Vec::from("abc".as_bytes()), Vec::from("def".as_bytes())))
    );
}

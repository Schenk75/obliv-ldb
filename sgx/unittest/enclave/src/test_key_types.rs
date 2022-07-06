#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

use rusty_leveldb::key_types::*;
use integer_encoding::VarInt;

pub fn test_memtable_lookupkey() {
    let lk1 = LookupKey::new("abcde".as_bytes(), 123);
    let lk2 = LookupKey::new("xyabxy".as_bytes(), 97);

    let lk1_key = lk1.get_key();

    // Assert correct allocation strategy
    assert_eq!(lk1_key.len(), 14);
    assert_eq!(lk1_key.capacity(), 14);

    assert_eq!(lk1.user_key(), "abcde".as_bytes());
    assert_eq!(u32::decode_var(lk1.memtable_key()), (13, 1));
    assert_eq!(
        lk2.internal_key(),
        vec![120, 121, 97, 98, 120, 121, 1, 97, 0, 0, 0, 0, 0, 0].as_slice()
    );
}

pub fn test_build_memtable_key() {
    assert_eq!(
        build_memtable_key(
            "abc".as_bytes(),
            "123".as_bytes(),
            ValueType::TypeValue,
            231
        ),
        vec![11, 97, 98, 99, 1, 231, 0, 0, 0, 0, 0, 0, 3, 49, 50, 51]
    );
    assert_eq!(
        build_memtable_key("".as_bytes(), "123".as_bytes(), ValueType::TypeValue, 231),
        vec![8, 1, 231, 0, 0, 0, 0, 0, 0, 3, 49, 50, 51]
    );
    assert_eq!(
        build_memtable_key(
            "abc".as_bytes(),
            "123".as_bytes(),
            ValueType::TypeDeletion,
            231
        ),
        vec![11, 97, 98, 99, 0, 231, 0, 0, 0, 0, 0, 0, 3, 49, 50, 51]
    );
    assert_eq!(
        build_memtable_key(
            "abc".as_bytes(),
            "".as_bytes(),
            ValueType::TypeDeletion,
            231
        ),
        vec![11, 97, 98, 99, 0, 231, 0, 0, 0, 0, 0, 0, 0]
    );
}
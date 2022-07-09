#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

use rusty_leveldb::{
    memtable::*,
    key_types::*,
    options,
    test_util::{test_iterator_properties, LdbIteratorIter},
    LdbIterator,
    types::current_key_val,
};

fn get_memtable() -> MemTable {
    let mut mt = MemTable::new(options::for_test().cmp);
    let entries = vec![
        (ValueType::TypeValue, 115, "abc", "122"),
        (ValueType::TypeValue, 120, "abc", "123"),
        (ValueType::TypeValue, 121, "abd", "124"),
        (ValueType::TypeDeletion, 122, "abe", "125"),
        (ValueType::TypeValue, 123, "abf", "126"),
    ];

    for e in entries.iter() {
        mt.add(e.1, e.0, e.2.as_bytes(), e.3.as_bytes());
    }
    mt
}

pub fn test_shift_left() {
    let mut v = vec![1, 2, 3, 4, 5];
    shift_left(&mut v, 1);
    assert_eq!(v, vec![2, 3, 4, 5]);

    let mut v = vec![1, 2, 3, 4, 5];
    shift_left(&mut v, 4);
    assert_eq!(v, vec![5]);
}

pub fn test_memtable_parse_tag() {
    let tag = (12345 << 8) | 1;
    assert_eq!(parse_tag(tag), (ValueType::TypeValue, 12345));
}

pub fn test_memtable_add() {
    let mut mt = MemTable::new(options::for_test().cmp);
    mt.add(
        123,
        ValueType::TypeValue,
        "abc".as_bytes(),
        "123".as_bytes(),
    );

    assert_eq!(
        mt.map.iter().next().unwrap().0,
        &[11, 97, 98, 99, 1, 123, 0, 0, 0, 0, 0, 0, 3, 49, 50, 51]
    );
    assert_eq!(
        mt.iter().next().unwrap().0,
        &[97, 98, 99, 1, 123, 0, 0, 0, 0, 0, 0]
    );
}

pub fn test_memtable_add_get() {
    let mt = get_memtable();

    // Smaller sequence number doesn't find entry
    if let Some(v) = mt.get(&LookupKey::new("abc".as_bytes(), 110)).0 {
        println!("{:?}", v);
        panic!("found");
    }

    if let Some(v) = mt.get(&LookupKey::new("abf".as_bytes(), 110)).0 {
        println!("{:?}", v);
        panic!("found");
    }

    // Bigger sequence number falls back to next smaller
    if let Some(v) = mt.get(&LookupKey::new("abc".as_bytes(), 116)).0 {
        assert_eq!(v, "122".as_bytes());
    } else {
        panic!("not found");
    }

    // Exact match works
    if let (Some(v), deleted) = mt.get(&LookupKey::new("abc".as_bytes(), 120)) {
        assert_eq!(v, "123".as_bytes());
        assert!(!deleted);
    } else {
        panic!("not found");
    }

    if let (None, deleted) = mt.get(&LookupKey::new("abe".as_bytes(), 122)) {
        assert!(deleted);
    } else {
        panic!("found deleted");
    }

    if let Some(v) = mt.get(&LookupKey::new("abf".as_bytes(), 129)).0 {
        assert_eq!(v, "126".as_bytes());
    } else {
        panic!("not found");
    }
}

pub fn test_memtable_iterator_init() {
    let mt = get_memtable();
    let mut iter = mt.iter();

    assert!(!iter.valid());
    iter.next();
    assert!(iter.valid());
    assert_eq!(
        current_key_val(&iter).unwrap().0,
        vec![97, 98, 99, 1, 120, 0, 0, 0, 0, 0, 0].as_slice()
    );
    iter.reset();
    assert!(!iter.valid());
}

pub fn test_memtable_iterator_seek() {
    let mt = get_memtable();
    let mut iter = mt.iter();

    assert!(!iter.valid());

    iter.seek(LookupKey::new("abc".as_bytes(), 400).internal_key());
    let (mut gotkey, gotval) = current_key_val(&iter).unwrap();
    truncate_to_userkey(&mut gotkey);
    assert_eq!(
        ("abc".as_bytes(), "123".as_bytes()),
        (gotkey.as_slice(), gotval.as_slice())
    );

    iter.seek(LookupKey::new("xxx".as_bytes(), 400).internal_key());
    assert!(!iter.valid());

    iter.seek(LookupKey::new("abd".as_bytes(), 400).internal_key());
    let (mut gotkey, gotval) = current_key_val(&iter).unwrap();
    truncate_to_userkey(&mut gotkey);
    assert_eq!(
        ("abd".as_bytes(), "124".as_bytes()),
        (gotkey.as_slice(), gotval.as_slice())
    );
}

pub fn test_memtable_iterator_fwd() {
    let mt = get_memtable();
    let mut iter = mt.iter();

    let expected = vec![
        "123".as_bytes(), /* i.e., the abc entry with
                           * higher sequence number comes first */
        "122".as_bytes(),
        "124".as_bytes(),
        // deleted entry:
        "125".as_bytes(),
        "126".as_bytes(),
    ];
    let mut i = 0;

    for (_k, v) in LdbIteratorIter::wrap(&mut iter) {
        assert_eq!(v, expected[i]);
        i += 1;
    }
}

pub fn test_memtable_iterator_reverse() {
    let mt = get_memtable();
    let mut iter = mt.iter();

    // Bigger sequence number comes first
    iter.next();
    assert!(iter.valid());
    assert_eq!(
        current_key_val(&iter).unwrap().0,
        vec![97, 98, 99, 1, 120, 0, 0, 0, 0, 0, 0].as_slice()
    );

    iter.next();
    assert!(iter.valid());
    assert_eq!(
        current_key_val(&iter).unwrap().0,
        vec![97, 98, 99, 1, 115, 0, 0, 0, 0, 0, 0].as_slice()
    );

    iter.next();
    assert!(iter.valid());
    assert_eq!(
        current_key_val(&iter).unwrap().0,
        vec![97, 98, 100, 1, 121, 0, 0, 0, 0, 0, 0].as_slice()
    );

    iter.prev();
    assert!(iter.valid());
    assert_eq!(
        current_key_val(&iter).unwrap().0,
        vec![97, 98, 99, 1, 115, 0, 0, 0, 0, 0, 0].as_slice()
    );

    iter.prev();
    assert!(iter.valid());
    assert_eq!(
        current_key_val(&iter).unwrap().0,
        vec![97, 98, 99, 1, 120, 0, 0, 0, 0, 0, 0].as_slice()
    );

    iter.prev();
    assert!(!iter.valid());
}

pub fn test_memtable_parse_key() {
    let key = vec![11, 1, 2, 3, 1, 123, 0, 0, 0, 0, 0, 0, 3, 4, 5, 6];
    let (keylen, keyoff, tag, vallen, valoff) = parse_memtable_key(&key);
    assert_eq!(keylen, 3);
    assert_eq!(&key[keyoff..keyoff + keylen], vec![1, 2, 3].as_slice());
    assert_eq!(tag, 123 << 8 | 1);
    assert_eq!(vallen, 3);
    assert_eq!(&key[valoff..valoff + vallen], vec![4, 5, 6].as_slice());
}

pub fn test_memtable_iterator_behavior() {
    let mut mt = MemTable::new(options::for_test().cmp);
    let entries = vec![
        (115, "abc", "122"),
        (120, "abd", "123"),
        (121, "abe", "124"),
        (123, "abf", "126"),
    ];

    for e in entries.iter() {
        mt.add(e.0, ValueType::TypeValue, e.1.as_bytes(), e.2.as_bytes());
    }

    test_iterator_properties(mt.iter());
}
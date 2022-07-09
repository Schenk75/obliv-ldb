#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

// use std::cell::RefCell;
// use std::cmp::Ordering;
use std::rc::Rc;
use rusty_leveldb::{
    LdbIterator,
    cmp::{MemtableKeyCmp, Cmp},
    options,
    test_util::{test_iterator_properties, LdbIteratorIter},
    types::current_key_val,
    skipmap::*
};
// use sgx_tunittest::should_panic;

fn make_skipmap() -> SkipMap {
    let mut skm = SkipMap::new(options::for_test().cmp);
    let keys = vec![
        "aba", "abb", "abc", "abd", "abe", "abf", "abg", "abh", "abi", "abj", "abk", "abl",
        "abm", "abn", "abo", "abp", "abq", "abr", "abs", "abt", "abu", "abv", "abw", "abx",
        "aby", "abz",
    ];

    for k in keys {
        skm.insert(k.as_bytes().to_vec(), "def".as_bytes().to_vec());
    }
    skm
}

pub fn test_insert() {
    let skm = make_skipmap();
    assert_eq!(skm.len(), 26);
    skm.map.borrow().dbg_print();
}

// pub fn test_no_dupes() {
//     let mut skm = make_skipmap();
//     // this should panic
//     skm.insert("abc".as_bytes().to_vec(), "def".as_bytes().to_vec());
//     should_panic!(skm.insert("abf".as_bytes().to_vec(), "def".as_bytes().to_vec()));
// }

pub fn test_contains() {
    let skm = make_skipmap();
    assert!(skm.contains(&"aby".as_bytes().to_vec()));
    assert!(skm.contains(&"abc".as_bytes().to_vec()));
    assert!(skm.contains(&"abz".as_bytes().to_vec()));
    assert!(!skm.contains(&"ab{".as_bytes().to_vec()));
    assert!(!skm.contains(&"123".as_bytes().to_vec()));
    assert!(!skm.contains(&"aaa".as_bytes().to_vec()));
    assert!(!skm.contains(&"456".as_bytes().to_vec()));
}

pub fn test_find() {
    let skm = make_skipmap();
    assert_eq!(
        skm.map
            .borrow()
            .get_greater_or_equal(&"abf".as_bytes().to_vec())
            .unwrap()
            .key,
        "abf".as_bytes().to_vec()
    );
    assert!(skm
        .map
        .borrow()
        .get_greater_or_equal(&"ab{".as_bytes().to_vec())
        .is_none());
    assert_eq!(
        skm.map
            .borrow()
            .get_greater_or_equal(&"aaa".as_bytes().to_vec())
            .unwrap()
            .key,
        "aba".as_bytes().to_vec()
    );
    assert_eq!(
        skm.map
            .borrow()
            .get_greater_or_equal(&"ab".as_bytes())
            .unwrap()
            .key
            .as_slice(),
        "aba".as_bytes()
    );
    assert_eq!(
        skm.map
            .borrow()
            .get_greater_or_equal(&"abc".as_bytes())
            .unwrap()
            .key
            .as_slice(),
        "abc".as_bytes()
    );
    assert!(skm
        .map
        .borrow()
        .get_next_smaller(&"ab0".as_bytes())
        .is_none());
    assert_eq!(
        skm.map
            .borrow()
            .get_next_smaller(&"abd".as_bytes())
            .unwrap()
            .key
            .as_slice(),
        "abc".as_bytes()
    );
    assert_eq!(
        skm.map
            .borrow()
            .get_next_smaller(&"ab{".as_bytes())
            .unwrap()
            .key
            .as_slice(),
        "abz".as_bytes()
    );
}

pub fn test_empty_skipmap_find_memtable_cmp() {
    // Regression test: Make sure comparator isn't called with empty key.
    let cmp: Rc<Box<dyn Cmp>> = Rc::new(Box::new(MemtableKeyCmp(options::for_test().cmp)));
    let skm = SkipMap::new(cmp);

    let mut it = skm.iter();
    it.seek("abc".as_bytes());
    assert!(!it.valid());
}

pub fn test_skipmap_iterator_0() {
    let skm = SkipMap::new(options::for_test().cmp);
    let mut i = 0;

    for (_, _) in LdbIteratorIter::wrap(&mut skm.iter()) {
        i += 1;
    }

    assert_eq!(i, 0);
    assert!(!skm.iter().valid());
}

pub fn test_skipmap_iterator_init() {
    let skm = make_skipmap();
    let mut iter = skm.iter();

    assert!(!iter.valid());
    iter.next();
    assert!(iter.valid());
    iter.reset();
    assert!(!iter.valid());

    iter.next();
    assert!(iter.valid());
    iter.prev();
    assert!(!iter.valid());
}

pub fn test_skipmap_iterator() {
    let skm = make_skipmap();
    let mut i = 0;

    for (k, v) in LdbIteratorIter::wrap(&mut skm.iter()) {
        assert!(!k.is_empty());
        assert!(!v.is_empty());
        i += 1;
    }
    assert_eq!(i, 26);
}

pub fn test_skipmap_iterator_seek_valid() {
    let skm = make_skipmap();
    let mut iter = skm.iter();

    iter.next();
    assert!(iter.valid());
    assert_eq!(current_key_val(&iter).unwrap().0, "aba".as_bytes().to_vec());
    iter.seek(&"abz".as_bytes().to_vec());
    assert_eq!(
        current_key_val(&iter).unwrap(),
        ("abz".as_bytes().to_vec(), "def".as_bytes().to_vec())
    );
    // go back to beginning
    iter.seek(&"aba".as_bytes().to_vec());
    assert_eq!(
        current_key_val(&iter).unwrap(),
        ("aba".as_bytes().to_vec(), "def".as_bytes().to_vec())
    );

    iter.seek(&"".as_bytes().to_vec());
    assert!(iter.valid());
    iter.prev();
    assert!(!iter.valid());

    while iter.advance() {}
    assert!(!iter.valid());
    assert!(!iter.prev());
    assert_eq!(current_key_val(&iter), None);
}

pub fn test_skipmap_behavior() {
    let mut skm = SkipMap::new(options::for_test().cmp);
    let keys = vec!["aba", "abb", "abc", "abd"];
    for k in keys {
        skm.insert(k.as_bytes().to_vec(), "def".as_bytes().to_vec());
    }
    test_iterator_properties(skm.iter());
}

pub fn test_skipmap_iterator_prev() {
    let skm = make_skipmap();
    let mut iter = skm.iter();

    iter.next();
    assert!(iter.valid());
    iter.prev();
    assert!(!iter.valid());
    iter.seek(&"abc".as_bytes());
    iter.prev();
    assert_eq!(
        current_key_val(&iter).unwrap(),
        ("abb".as_bytes().to_vec(), "def".as_bytes().to_vec())
    );
}

pub fn test_skipmap_iterator_concurrent_insert() {
    // time_test!();
    // Asserts that the map can be mutated while an iterator exists; this is intentional.
    let mut skm = make_skipmap();
    let mut iter = skm.iter();

    assert!(iter.advance());
    skm.insert("abccc".as_bytes().to_vec(), "defff".as_bytes().to_vec());
    // Assert that value inserted after obtaining iterator is present.
    for (k, _) in LdbIteratorIter::wrap(&mut iter) {
        if k == "abccc".as_bytes() {
            return;
        }
    }
    panic!("abccc not found in map.");
}
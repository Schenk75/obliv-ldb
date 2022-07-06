#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

use std::cmp::Ordering;
use std::rc::Rc;
use rusty_leveldb::{
    cmp::*,
    key_types::LookupKey,
    types
};
// use sgx_tunittest::should_panic;

pub fn test_cmp_defaultcmp_shortest_sep() {
    assert_eq!(
        DefaultCmp.find_shortest_sep("abcd".as_bytes(), "abcf".as_bytes()),
        "abce".as_bytes()
    );
    assert_eq!(
        DefaultCmp.find_shortest_sep("abc".as_bytes(), "acd".as_bytes()),
        "abd".as_bytes()
    );
    assert_eq!(
        DefaultCmp.find_shortest_sep("abcdefghi".as_bytes(), "abcffghi".as_bytes()),
        "abce".as_bytes()
    );
    assert_eq!(
        DefaultCmp.find_shortest_sep("a".as_bytes(), "a".as_bytes()),
        "a".as_bytes()
    );
    assert_eq!(
        DefaultCmp.find_shortest_sep("a".as_bytes(), "b".as_bytes()),
        "a\0".as_bytes()
    );
    assert_eq!(
        DefaultCmp.find_shortest_sep("abc".as_bytes(), "zzz".as_bytes()),
        "b".as_bytes()
    );
    assert_eq!(
        DefaultCmp.find_shortest_sep("yyy".as_bytes(), "z".as_bytes()),
        "yyz".as_bytes()
    );
    assert_eq!(
        DefaultCmp.find_shortest_sep("".as_bytes(), "".as_bytes()),
        "".as_bytes()
    );
}

pub fn test_cmp_defaultcmp_short_succ() {
    assert_eq!(
        DefaultCmp.find_short_succ("abcd".as_bytes()),
        "b".as_bytes()
    );
    assert_eq!(
        DefaultCmp.find_short_succ("zzzz".as_bytes()),
        "{".as_bytes()
    );
    assert_eq!(DefaultCmp.find_short_succ(&[]), &[0xff]);
    assert_eq!(
        DefaultCmp.find_short_succ(&[0xff, 0xff, 0xff]),
        &[0xff, 0xff, 0xff, 0xff]
    );
}

pub fn test_cmp_internalkeycmp_shortest_sep() {
    let cmp = InternalKeyCmp(Rc::new(Box::new(DefaultCmp)));
    assert_eq!(
        cmp.find_shortest_sep(
            LookupKey::new("abcd".as_bytes(), 1).internal_key(),
            LookupKey::new("abcf".as_bytes(), 2).internal_key()
        ),
        LookupKey::new("abce".as_bytes(), 1).internal_key()
    );
    assert_eq!(
        cmp.find_shortest_sep(
            LookupKey::new("abcd".as_bytes(), 1).internal_key(),
            LookupKey::new("abce".as_bytes(), 2).internal_key()
        ),
        LookupKey::new("abcd\0".as_bytes(), 1).internal_key()
    );
    assert_eq!(
        cmp.find_shortest_sep(
            LookupKey::new("abc".as_bytes(), 1).internal_key(),
            LookupKey::new("zzz".as_bytes(), 2).internal_key()
        ),
        LookupKey::new("b".as_bytes(), types::MAX_SEQUENCE_NUMBER).internal_key()
    );
    assert_eq!(
        cmp.find_shortest_sep(
            LookupKey::new("abc".as_bytes(), 1).internal_key(),
            LookupKey::new("acd".as_bytes(), 2).internal_key()
        ),
        LookupKey::new("abd".as_bytes(), 1).internal_key()
    );
    assert_eq!(
        cmp.find_shortest_sep(
            LookupKey::new("abc".as_bytes(), 1).internal_key(),
            LookupKey::new("abe".as_bytes(), 2).internal_key()
        ),
        LookupKey::new("abd".as_bytes(), 1).internal_key()
    );
    assert_eq!(
        cmp.find_shortest_sep(
            LookupKey::new("".as_bytes(), 1).internal_key(),
            LookupKey::new("".as_bytes(), 2).internal_key()
        ),
        LookupKey::new("".as_bytes(), 1).internal_key()
    );
    assert_eq!(
        cmp.find_shortest_sep(
            LookupKey::new("abc".as_bytes(), 2).internal_key(),
            LookupKey::new("abc".as_bytes(), 2).internal_key()
        ),
        LookupKey::new("abc".as_bytes(), 2).internal_key()
    );
}

pub fn test_cmp_internalkeycmp() {
    let cmp = InternalKeyCmp(Rc::new(Box::new(DefaultCmp)));
    // a < b < c
    let a = LookupKey::new("abc".as_bytes(), 2).internal_key().to_vec();
    let b = LookupKey::new("abc".as_bytes(), 1).internal_key().to_vec();
    let c = LookupKey::new("abd".as_bytes(), 3).internal_key().to_vec();
    let d = "xyy".as_bytes();
    let e = "xyz".as_bytes();

    assert_eq!(Ordering::Less, cmp.cmp(&a, &b));
    assert_eq!(Ordering::Equal, cmp.cmp(&a, &a));
    assert_eq!(Ordering::Greater, cmp.cmp(&b, &a));
    assert_eq!(Ordering::Less, cmp.cmp(&a, &c));
    assert_eq!(Ordering::Less, cmp.cmp_inner(d, e));
    assert_eq!(Ordering::Greater, cmp.cmp_inner(e, d));
}

// pub fn test_cmp_memtablekeycmp_panics() {
//     let cmp = MemtableKeyCmp(Rc::new(Box::new(DefaultCmp)));
//     cmp.cmp(&[1, 2, 3], &[4, 5, 6]);
// }
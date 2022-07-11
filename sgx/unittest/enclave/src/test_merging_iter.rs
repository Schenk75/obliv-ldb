#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

use std::rc::Rc;
use rusty_leveldb::{
    merging_iter::*,
    cmp::DefaultCmp,
    test_util::{test_iterator_properties, LdbIteratorIter, TestLdbIter},
    types::{current_key_val, LdbIterator},
    skipmap::SkipMap,
    options,
};

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

fn b(s: &'static str) -> &'static [u8] {
    s.as_bytes()
}

pub fn test_merging_one() {
    let skm = make_skipmap();
    let iter = skm.iter();
    let mut iter2 = skm.iter();

    let mut miter = MergingIter::new(Rc::new(Box::new(DefaultCmp)), vec![Box::new(iter)]);

    loop {
        if let Some((k, v)) = miter.next() {
            if let Some((k2, v2)) = iter2.next() {
                assert_eq!(k, k2);
                assert_eq!(v, v2);
            } else {
                panic!("Expected element from iter2");
            }
        } else {
            break;
        }
    }
}

pub fn test_merging_two() {
    let skm = make_skipmap();
    let iter = skm.iter();
    let iter2 = skm.iter();

    let mut miter = MergingIter::new(
        Rc::new(Box::new(DefaultCmp)),
        vec![Box::new(iter), Box::new(iter2)],
    );

    loop {
        if let Some((k, v)) = miter.next() {
            if let Some((k2, v2)) = miter.next() {
                assert_eq!(k, k2);
                assert_eq!(v, v2);
            } else {
                panic!("Odd number of elements");
            }
        } else {
            break;
        }
    }
}

pub fn test_merging_zero() {
    let mut miter = MergingIter::new(Rc::new(Box::new(DefaultCmp)), vec![]);
    assert_eq!(0, LdbIteratorIter::wrap(&mut miter).count());
}

pub fn test_merging_behavior() {
    let val = "def".as_bytes();
    let iter = TestLdbIter::new(vec![(b("aba"), val), (b("abc"), val)]);
    let iter2 = TestLdbIter::new(vec![(b("abb"), val), (b("abd"), val)]);
    let miter = MergingIter::new(
        Rc::new(Box::new(DefaultCmp)),
        vec![Box::new(iter), Box::new(iter2)],
    );
    test_iterator_properties(miter);
}

pub fn test_merging_forward_backward() {
    let val = "def".as_bytes();
    let iter = TestLdbIter::new(vec![(b("aba"), val), (b("abc"), val), (b("abe"), val)]);
    let iter2 = TestLdbIter::new(vec![(b("abb"), val), (b("abd"), val)]);

    let mut miter = MergingIter::new(
        Rc::new(Box::new(DefaultCmp)),
        vec![Box::new(iter), Box::new(iter2)],
    );

    // miter should return the following sequence: [aba, abb, abc, abd, abe]

    // -> aba
    let first = miter.next();
    // -> abb
    let second = miter.next();
    // -> abc
    let third = miter.next();
    println!("{:?} {:?} {:?}", first, second, third);

    assert!(first != third);
    // abb <-
    assert!(miter.prev());
    assert_eq!(second, current_key_val(&miter));
    // aba <-
    assert!(miter.prev());
    assert_eq!(first, current_key_val(&miter));
    // -> abb
    assert!(miter.advance());
    assert_eq!(second, current_key_val(&miter));
    // -> abc
    assert!(miter.advance());
    assert_eq!(third, current_key_val(&miter));
    // -> abd
    assert!(miter.advance());
    assert_eq!(
        Some((b("abd").to_vec(), val.to_vec())),
        current_key_val(&miter)
    );
}

pub fn test_merging_real() {
    let val = "def".as_bytes();

    let it1 = TestLdbIter::new(vec![(&b("aba"), val), (&b("abc"), val), (&b("abe"), val)]);
    let it2 = TestLdbIter::new(vec![(&b("abb"), val), (&b("abd"), val)]);
    let expected = vec![b("aba"), b("abb"), b("abc"), b("abd"), b("abe")];

    let mut iter = MergingIter::new(
        Rc::new(Box::new(DefaultCmp)),
        vec![Box::new(it1), Box::new(it2)],
    );

    let mut i = 0;
    for (k, _) in LdbIteratorIter::wrap(&mut iter) {
        assert_eq!(k, expected[i]);
        i += 1;
    }
}

pub fn test_merging_seek_reset() {
    let val = "def".as_bytes();

    let it1 = TestLdbIter::new(vec![(b("aba"), val), (b("abc"), val), (b("abe"), val)]);
    let it2 = TestLdbIter::new(vec![(b("abb"), val), (b("abd"), val)]);

    let mut iter = MergingIter::new(
        Rc::new(Box::new(DefaultCmp)),
        vec![Box::new(it1), Box::new(it2)],
    );

    assert!(!iter.valid());
    iter.advance();
    assert!(iter.valid());
    assert!(current_key_val(&iter).is_some());

    iter.seek("abc".as_bytes());
    assert_eq!(
        current_key_val(&iter),
        Some((b("abc").to_vec(), val.to_vec()))
    );
    iter.seek("ab0".as_bytes());
    assert_eq!(
        current_key_val(&iter),
        Some((b("aba").to_vec(), val.to_vec()))
    );
    iter.seek("abx".as_bytes());
    assert_eq!(current_key_val(&iter), None);

    iter.reset();
    assert!(!iter.valid());
    iter.next();
    assert_eq!(
        current_key_val(&iter),
        Some((b("aba").to_vec(), val.to_vec()))
    );
}
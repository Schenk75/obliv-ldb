#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

use rusty_leveldb::cache::*;

fn make_key(a: u8, b: u8, c: u8) -> CacheKey {
    [a, b, c, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
}

pub fn test_blockcache_cache_add_rm() {
    let mut cache = Cache::new(128);

    let h_123 = make_key(1, 2, 3);
    let h_521 = make_key(1, 2, 4);
    let h_372 = make_key(3, 4, 5);
    let h_332 = make_key(6, 3, 1);
    let h_899 = make_key(8, 2, 1);

    cache.insert(&h_123, 123);
    cache.insert(&h_332, 332);
    cache.insert(&h_521, 521);
    cache.insert(&h_372, 372);
    cache.insert(&h_899, 899);

    assert_eq!(cache.count(), 5);

    assert_eq!(cache.get(&h_123), Some(&123));
    assert_eq!(cache.get(&h_372), Some(&372));

    assert_eq!(cache.remove(&h_521), Some(521));
    assert_eq!(cache.get(&h_521), None);
    assert_eq!(cache.remove(&h_521), None);

    assert_eq!(cache.count(), 4);
}

pub fn test_blockcache_cache_capacity() {
    let mut cache = Cache::new(3);

    let h_123 = make_key(1, 2, 3);
    let h_521 = make_key(1, 2, 4);
    let h_372 = make_key(3, 4, 5);
    let h_332 = make_key(6, 3, 1);
    let h_899 = make_key(8, 2, 1);

    cache.insert(&h_123, 123);
    cache.insert(&h_332, 332);
    cache.insert(&h_521, 521);
    cache.insert(&h_372, 372);
    cache.insert(&h_899, 899);

    assert_eq!(cache.count(), 3);

    assert_eq!(cache.get(&h_123), None);
    assert_eq!(cache.get(&h_332), None);
    assert_eq!(cache.get(&h_521), Some(&521));
    assert_eq!(cache.get(&h_372), Some(&372));
    assert_eq!(cache.get(&h_899), Some(&899));
}

pub fn test_blockcache_lru_remove() {
    let mut lru = LRUList::<usize>::new();

    let h_56 = lru.insert(56);
    lru.insert(22);
    lru.insert(223);
    let h_244 = lru.insert(244);
    lru.insert(1111);
    let h_12 = lru.insert(12);

    assert_eq!(lru.count(), 6);
    assert_eq!(244, lru.remove(h_244));
    assert_eq!(lru.count(), 5);
    assert_eq!(12, lru.remove(h_12));
    assert_eq!(lru.count(), 4);
    assert_eq!(56, lru.remove(h_56));
    assert_eq!(lru.count(), 3);
}

pub fn test_blockcache_lru_1() {
    let mut lru = LRUList::<usize>::new();

    lru.insert(56);
    lru.insert(22);
    lru.insert(244);
    lru.insert(12);

    assert_eq!(lru.count(), 4);

    assert_eq!(Some(56), lru.remove_last());
    assert_eq!(Some(22), lru.remove_last());
    assert_eq!(Some(244), lru.remove_last());

    assert_eq!(lru.count(), 1);

    assert_eq!(Some(12), lru.remove_last());

    assert_eq!(lru.count(), 0);

    assert_eq!(None, lru.remove_last());
}

pub fn test_blockcache_lru_reinsert() {
    let mut lru = LRUList::<usize>::new();

    let handle1 = lru.insert(56);
    let handle2 = lru.insert(22);
    let handle3 = lru.insert(244);

    assert_eq!(lru._testing_head_ref().map(|r| (*r)).unwrap(), 244);

    lru.reinsert_front(handle1);

    assert_eq!(lru._testing_head_ref().map(|r| (*r)).unwrap(), 56);

    lru.reinsert_front(handle3);

    assert_eq!(lru._testing_head_ref().map(|r| (*r)).unwrap(), 244);

    lru.reinsert_front(handle2);

    assert_eq!(lru._testing_head_ref().map(|r| (*r)).unwrap(), 22);

    assert_eq!(lru.remove_last(), Some(56));
    assert_eq!(lru.remove_last(), Some(244));
    assert_eq!(lru.remove_last(), Some(22));
}

pub fn test_blockcache_lru_reinsert_2() {
    let mut lru = LRUList::<usize>::new();

    let handles = vec![
        lru.insert(0),
        lru.insert(1),
        lru.insert(2),
        lru.insert(3),
        lru.insert(4),
        lru.insert(5),
        lru.insert(6),
        lru.insert(7),
        lru.insert(8),
    ];

    for i in 0..9 {
        lru.reinsert_front(handles[i]);
        assert_eq!(lru._testing_head_ref().map(|x| *x), Some(i));
    }
}

pub fn test_blockcache_lru_edge_cases() {
    let mut lru = LRUList::<usize>::new();

    let handle = lru.insert(3);

    lru.reinsert_front(handle);
    assert_eq!(lru._testing_head_ref().map(|x| *x), Some(3));
    assert_eq!(lru.remove_last(), Some(3));
    assert_eq!(lru.remove_last(), None);
    assert_eq!(lru.remove_last(), None);
}
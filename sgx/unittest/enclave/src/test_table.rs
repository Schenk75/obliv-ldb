#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

use std::rc::Rc;
use std::convert::AsRef;
use std::path::Path;
use rusty_leveldb::{
    blockhandle::BlockHandle,
    options::{self, CompressionType, Options},
    table_builder::*,
    filter::BloomPolicy,
    key_types::LookupKey,
    test_util::{LdbIteratorIter, test_iterator_properties},
    types::{current_key_val, LdbIterator},
    table_reader::*,
    env::RandomAccess,
    mem_env::MemEnv,
    cache,
    table_cache::*
};
// use sgx_tunittest::should_panic;

pub fn test_footer() {
    let f = Footer::new(BlockHandle::new(44, 4), BlockHandle::new(55, 5));
    let mut buf = [0; 48];
    f.encode(&mut buf[..]);

    let f2 = Footer::decode(&buf);
    assert_eq!(f2.meta_index.offset(), 44);
    assert_eq!(f2.meta_index.size(), 4);
    assert_eq!(f2.index.offset(), 55);
    assert_eq!(f2.index.size(), 5);
}

pub fn test_table_builder() {
    let mut d = Vec::with_capacity(512);
    let mut opt = options::for_test();
    opt.block_restart_interval = 3;
    opt.compression_type = CompressionType::CompressionSnappy;
    let mut b = TableBuilder::new_raw(opt, &mut d);

    let data = vec![
        ("abc", "def"),
        ("abe", "dee"),
        ("bcd", "asa"),
        ("dcc", "a00"),
    ];
    let data2 = vec![
        ("abd", "def"),
        ("abf", "dee"),
        ("ccd", "asa"),
        ("dcd", "a00"),
    ];

    for i in 0..data.len() {
        b.add(&data[i].0.as_bytes(), &data[i].1.as_bytes()).unwrap();
        b.add(&data2[i].0.as_bytes(), &data2[i].1.as_bytes())
            .unwrap();
    }

    let estimate = b.size_estimate();

    assert_eq!(143, estimate);
    assert!(b.filter_block.is_some());

    let actual = b.finish().unwrap();
    assert_eq!(223, actual);
}

// pub fn test_bad_input() {
//     let mut d = Vec::with_capacity(512);
//     let mut opt = options::for_test();
//     opt.block_restart_interval = 3;
//     let mut b = TableBuilder::new_raw(opt, &mut d);

//     // Test two equal consecutive keys
//     let data = vec![
//         ("abc", "def"),
//         ("abc", "dee"),
//         ("bcd", "asa"),
//         ("bsr", "a00"),
//     ];

//     for &(k, v) in data.iter() {
//         if v == "dee" {
//             should_panic!(b.add(k.as_bytes(), v.as_bytes()).unwrap());
//         }
//         b.add(k.as_bytes(), v.as_bytes()).unwrap();
//     }
//     b.finish().unwrap();
// }


fn build_data() -> Vec<(&'static str, &'static str)> {
    vec![
        // block 1
        ("abc", "def"),
        ("abd", "dee"),
        ("bcd", "asa"),
        // block 2
        ("bsr", "a00"),
        ("xyz", "xxx"),
        ("xzz", "yyy"),
        // block 3
        ("zzz", "111"),
    ]
}

// Build a table containing raw keys (no format). It returns (vector, length) for convenience
// reason, a call f(v, v.len()) doesn't work for borrowing reasons.
fn build_table(data: Vec<(&'static str, &'static str)>) -> (Vec<u8>, usize) {
    let mut d = Vec::with_capacity(512);
    let mut opt = options::for_test();
    opt.block_restart_interval = 2;
    opt.block_size = 32;
    opt.compression_type = CompressionType::CompressionSnappy;

    {
        // Uses the standard comparator in opt.
        let mut b = TableBuilder::new_raw(opt, &mut d);

        for &(k, v) in data.iter() {
            b.add(k.as_bytes(), v.as_bytes()).unwrap();
        }

        b.finish().unwrap();
    }

    let size = d.len();
    (d, size)
}

// Build a table containing keys in InternalKey format.
fn build_internal_table() -> (Vec<u8>, usize) {
    let mut d = Vec::with_capacity(512);
    let mut opt = options::for_test();
    opt.block_restart_interval = 1;
    opt.block_size = 32;
    opt.filter_policy = Rc::new(Box::new(BloomPolicy::new(4)));

    let mut i = 1 as u64;
    let data: Vec<(Vec<u8>, &'static str)> = build_data()
        .into_iter()
        .map(|(k, v)| {
            i += 1;
            (LookupKey::new(k.as_bytes(), i).internal_key().to_vec(), v)
        })
        .collect();

    {
        // Uses InternalKeyCmp
        let mut b = TableBuilder::new(opt, &mut d);

        for &(ref k, ref v) in data.iter() {
            b.add(k.as_slice(), v.as_bytes()).unwrap();
        }

        b.finish().unwrap();
    }

    let size = d.len();

    (d, size)
}

fn wrap_buffer(src: Vec<u8>) -> Rc<Box<dyn RandomAccess>> {
    Rc::new(Box::new(src))
}

pub fn test_table_approximate_offset() {
    let (src, size) = build_table(build_data());
    let mut opt = options::for_test();
    opt.block_size = 32;
    let table = Table::new_raw(opt, wrap_buffer(src), size).unwrap();
    let mut iter = table.iter();

    let expected_offsets = vec![0, 0, 0, 44, 44, 44, 89];
    let mut i = 0;
    for (k, _) in LdbIteratorIter::wrap(&mut iter) {
        assert_eq!(expected_offsets[i], table.approx_offset_of(&k));
        i += 1;
    }

    // Key-past-last returns offset of metaindex block.
    assert_eq!(137, table.approx_offset_of("{aa".as_bytes()));
}

pub fn test_table_block_cache_use() {
    let (src, size) = build_table(build_data());
    let mut opt = options::for_test();
    opt.block_size = 32;

    let table = Table::new_raw(opt.clone(), wrap_buffer(src), size).unwrap();
    let mut iter = table.iter();

    // index/metaindex blocks are not cached. That'd be a waste of memory.
    assert_eq!(opt.block_cache.borrow().count(), 0);
    iter.next();
    assert_eq!(opt.block_cache.borrow().count(), 1);
    // This may fail if block parameters or data change. In that case, adapt it.
    iter.next();
    iter.next();
    iter.next();
    iter.next();
    assert_eq!(opt.block_cache.borrow().count(), 2);
}

pub fn test_table_iterator_fwd_bwd() {
    let (src, size) = build_table(build_data());
    let data = build_data();

    let table = Table::new_raw(options::for_test(), wrap_buffer(src), size).unwrap();
    let mut iter = table.iter();
    let mut i = 0;

    while let Some((k, v)) = iter.next() {
        assert_eq!(
            (data[i].0.as_bytes(), data[i].1.as_bytes()),
            (k.as_ref(), v.as_ref())
        );
        i += 1;
    }

    assert_eq!(i, data.len());
    assert!(!iter.valid());

    // Go forward again, to last entry.
    while let Some((key, _)) = iter.next() {
        if key.as_slice() == b"zzz" {
            break;
        }
    }

    assert!(iter.valid());
    // backwards count
    let mut j = 0;

    while iter.prev() {
        if let Some((k, v)) = current_key_val(&iter) {
            j += 1;
            assert_eq!(
                (
                    data[data.len() - 1 - j].0.as_bytes(),
                    data[data.len() - 1 - j].1.as_bytes()
                ),
                (k.as_ref(), v.as_ref())
            );
        } else {
            break;
        }
    }

    // expecting 7 - 1, because the last entry that the iterator stopped on is the last entry
    // in the table; that is, it needs to go back over 6 entries.
    assert_eq!(j, 6);
}

pub fn test_table_iterator_filter() {
    let (src, size) = build_table(build_data());

    let table = Table::new_raw(options::for_test(), wrap_buffer(src), size).unwrap();
    assert!(table.filters.is_some());
    let filter_reader = table.filters.clone().unwrap();
    let mut iter = table.iter();

    loop {
        if let Some((k, _)) = iter.next() {
            assert!(filter_reader.key_may_match(iter.current_block_off, &k));
            assert!(!filter_reader.key_may_match(iter.current_block_off, b"somerandomkey"));
        } else {
            break;
        }
    }
}

pub fn test_table_iterator_state_behavior() {
    let (src, size) = build_table(build_data());

    let table = Table::new_raw(options::for_test(), wrap_buffer(src), size).unwrap();
    let mut iter = table.iter();

    // behavior test

    // See comment on valid()
    assert!(!iter.valid());
    assert!(current_key_val(&iter).is_none());
    assert!(!iter.prev());

    assert!(iter.advance());
    let first = current_key_val(&iter);
    assert!(iter.valid());
    assert!(current_key_val(&iter).is_some());

    assert!(iter.advance());
    assert!(iter.prev());
    assert!(iter.valid());

    iter.reset();
    assert!(!iter.valid());
    assert!(current_key_val(&iter).is_none());
    assert_eq!(first, iter.next());
}

pub fn test_table_iterator_behavior_standard() {
    let mut data = build_data();
    data.truncate(4);
    let (src, size) = build_table(data);
    let table = Table::new_raw(options::for_test(), wrap_buffer(src), size).unwrap();
    test_iterator_properties(table.iter());
}

pub fn test_table_iterator_values() {
    let (src, size) = build_table(build_data());
    let data = build_data();

    let table = Table::new_raw(options::for_test(), wrap_buffer(src), size).unwrap();
    let mut iter = table.iter();
    let mut i = 0;

    iter.next();
    iter.next();

    // Go back to previous entry, check, go forward two entries, repeat
    // Verifies that prev/next works well.
    loop {
        iter.prev();

        if let Some((k, v)) = current_key_val(&iter) {
            assert_eq!(
                (data[i].0.as_bytes(), data[i].1.as_bytes()),
                (k.as_ref(), v.as_ref())
            );
        } else {
            break;
        }

        i += 1;
        if iter.next().is_none() || iter.next().is_none() {
            break;
        }
    }

    // Skipping the last value because the second next() above will break the loop
    assert_eq!(i, 6);
}

pub fn test_table_iterator_seek() {
    let (src, size) = build_table(build_data());

    let table = Table::new_raw(options::for_test(), wrap_buffer(src), size).unwrap();
    let mut iter = table.iter();

    iter.seek(b"bcd");
    assert!(iter.valid());
    assert_eq!(
        current_key_val(&iter),
        Some((b"bcd".to_vec(), b"asa".to_vec()))
    );
    iter.seek(b"abc");
    assert!(iter.valid());
    assert_eq!(
        current_key_val(&iter),
        Some((b"abc".to_vec(), b"def".to_vec()))
    );

    // Seek-past-last invalidates.
    iter.seek("{{{".as_bytes());
    assert!(!iter.valid());
    iter.seek(b"bbb");
    assert!(iter.valid());
}

pub fn test_table_get() {
    let (src, size) = build_table(build_data());

    let table = Table::new_raw(options::for_test(), wrap_buffer(src), size).unwrap();
    let table2 = table.clone();

    let mut _iter = table.iter();
    // Test that all of the table's entries are reachable via get()
    for (k, v) in LdbIteratorIter::wrap(&mut _iter) {
        let r = table2.get(&k);
        assert_eq!(Ok(Some((k, v))), r);
    }

    assert_eq!(table.opt.block_cache.borrow().count(), 3);

    // test that filters work and don't return anything at all.
    assert!(table.get(b"aaa").unwrap().is_none());
    assert!(table.get(b"aaaa").unwrap().is_none());
    assert!(table.get(b"aa").unwrap().is_none());
    assert!(table.get(b"abcd").unwrap().is_none());
    assert!(table.get(b"abb").unwrap().is_none());
    assert!(table.get(b"zzy").unwrap().is_none());
    assert!(table.get(b"zz1").unwrap().is_none());
    assert!(table.get("zz{".as_bytes()).unwrap().is_none());
}

pub fn test_table_internal_keys() {
    let (src, size) = build_internal_table();

    let table = Table::new(options::for_test(), wrap_buffer(src), size).unwrap();
    let filter_reader = table.filters.clone().unwrap();

    // Check that we're actually using internal keys
    let mut _iter = table.iter();
    for (ref k, ref v) in LdbIteratorIter::wrap(&mut _iter) {
        assert_eq!(k.len(), 3 + 8);
        assert_eq!((k.to_vec(), v.to_vec()), table.get(k).unwrap().unwrap());
    }

    assert!(table
        .get(LookupKey::new(b"abc", 1000).internal_key())
        .unwrap()
        .is_some());

    let mut iter = table.iter();

    loop {
        if let Some((k, _)) = iter.next() {
            let lk = LookupKey::new(&k, 123);
            let userkey = lk.user_key();

            assert!(filter_reader.key_may_match(iter.current_block_off, userkey));
            assert!(!filter_reader.key_may_match(iter.current_block_off, b"somerandomkey"));
        } else {
            break;
        }
    }
}

pub fn test_table_reader_checksum() {
    let (mut src, size) = build_table(build_data());

    src[10] += 1;

    let table = Table::new_raw(options::for_test(), wrap_buffer(src), size).unwrap();

    assert!(table.filters.is_some());
    assert_eq!(table.filters.as_ref().unwrap().num(), 1);

    {
        let mut _iter = table.iter();
        let iter = LdbIteratorIter::wrap(&mut _iter);
        // first block is skipped
        assert_eq!(iter.count(), 4);
    }

    {
        let mut _iter = table.iter();
        let iter = LdbIteratorIter::wrap(&mut _iter);

        for (k, _) in iter {
            if k == build_data()[5].0.as_bytes() {
                return;
            }
        }

        panic!("Should have hit 5th record in table!");
    }
}


fn make_key(a: u8, b: u8, c: u8) -> cache::CacheKey {
    [a, b, c, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
}

fn write_table_to(o: Options, p: &Path) {
    let w = o.env.open_writable_file(p).unwrap();
    let mut b = TableBuilder::new_raw(o, w);

    let data = vec![
        ("abc", "def"),
        ("abd", "dee"),
        ("bcd", "asa"),
        ("bsr", "a00"),
    ];

    for &(k, v) in data.iter() {
        b.add(k.as_bytes(), v.as_bytes()).unwrap();
    }
    b.finish().unwrap();
}

pub fn test_table_file_name() {
    assert_eq!(Path::new("abc/000122.ldb"), table_file_name("abc", 122));
    assert_eq!(
        Path::new("abc/1234567.ldb"),
        table_file_name("abc", 1234567)
    );
}

pub fn test_filenum_to_key() {
    assert_eq!(make_key(16, 0, 0), filenum_to_key(0x10));
    assert_eq!(make_key(16, 1, 0), filenum_to_key(0x0110));
    assert_eq!(make_key(1, 2, 3), filenum_to_key(0x030201));
}

pub fn test_table_cache() {
    // Tests that a table can be written to a MemFS file, read back by the table cache and
    // parsed/iterated by the table reader.
    let mut opt = options::for_test();
    opt.env = Rc::new(Box::new(MemEnv::new()));
    let dbname = Path::new("testdb1");
    let tablename = table_file_name(dbname, 123);
    let tblpath = Path::new(&tablename);

    write_table_to(opt.clone(), tblpath);
    assert!(opt.env.exists(tblpath).unwrap());
    assert!(opt.env.size_of(tblpath).unwrap() > 20);

    let mut cache = TableCache::new(dbname, opt.clone(), 10);
    assert!(cache.cache.get(&filenum_to_key(123)).is_none());
    assert_eq!(
        LdbIteratorIter::wrap(&mut cache.get_table(123).unwrap().iter()).count(),
        4
    );
    // Test cached table.
    assert_eq!(
        LdbIteratorIter::wrap(&mut cache.get_table(123).unwrap().iter()).count(),
        4
    );

    assert!(cache.cache.get(&filenum_to_key(123)).is_some());
    assert!(cache.evict(123).is_ok());
    assert!(cache.evict(123).is_err());
    assert!(cache.cache.get(&filenum_to_key(123)).is_none());
}
#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

use rusty_leveldb::{
    block::*, 
    block_builder::BlockBuilder,
    blockhandle::BlockHandle,
    options,
    types::{current_key_val, LdbIterator},
    test_util::{test_iterator_properties, LdbIteratorIter}
};
// use sgx_tunittest::should_panic;

fn get_data() -> Vec<(&'static [u8], &'static [u8])> {
    vec![
        ("key1".as_bytes(), "value1".as_bytes()),
        (
            "loooooooooooooooooooooooooooooooooongerkey1".as_bytes(),
            "shrtvl1".as_bytes(),
        ),
        ("medium length key 1".as_bytes(), "some value 2".as_bytes()),
        ("prefix_key1".as_bytes(), "value".as_bytes()),
        ("prefix_key2".as_bytes(), "value".as_bytes()),
        ("prefix_key3".as_bytes(), "value".as_bytes()),
    ]
}

pub fn test_block_builder_sanity() {
    let mut o = options::for_test();
    o.block_restart_interval = 3;
    let mut builder = BlockBuilder::new(o);
    let d = get_data();

    for &(k, v) in d.iter() {
        builder.add(k, v);
        assert!(builder.get_restart_counter() <= 3);
        assert_eq!(builder.last_key(), k);
    }

    assert_eq!(149, builder.size_estimate());
    assert_eq!(d.len(), builder.entries());

    let block = builder.finish();
    assert_eq!(block.len(), 149);
}

pub fn test_block_builder_reset() {
    let mut o = options::for_test();
    o.block_restart_interval = 3;
    let mut builder = BlockBuilder::new(o);
    let d = get_data();

    for &(k, v) in d.iter() {
        builder.add(k, v);
        assert!(builder.get_restart_counter() <= 3);
        assert_eq!(builder.last_key(), k);
    }

    assert_eq!(d.len(), builder.entries());
    builder.reset();
    assert_eq!(0, builder.entries());
    assert_eq!(4, builder.size_estimate());
}

// pub fn test_block_builder_panics() {
//     let mut d = get_data();
//     // Identical key as d[3].
//     d[4].0 = b"prefix_key1";

//     let mut builder = BlockBuilder::new(options::for_test());
//     for &(k, v) in d.iter() {
//         builder.add(k, v);
//         should_panic!(assert_eq!(k, builder.last_key()));
//     }
// }

pub fn test_block_iterator_properties() {
    let o = options::for_test();
    let mut builder = BlockBuilder::new(o.clone());
    let mut data = get_data();
    data.truncate(4);
    for &(k, v) in data.iter() {
        builder.add(k, v);
    }
    let block_contents = builder.finish();

    let block = Block::new(o.clone(), block_contents).iter();
    test_iterator_properties(block);
}

pub fn test_block_empty() {
    let mut o = options::for_test();
    o.block_restart_interval = 16;
    let builder = BlockBuilder::new(o);

    let blockc = builder.finish();
    assert_eq!(blockc.len(), 8);
    assert_eq!(blockc, vec![0, 0, 0, 0, 1, 0, 0, 0]);

    let block = Block::new(options::for_test(), blockc);

    for _ in LdbIteratorIter::wrap(&mut block.iter()) {
        panic!("expected 0 iterations");
    }
}

pub fn test_block_build_iterate() {
    let data = get_data();
    let mut builder = BlockBuilder::new(options::for_test());

    for &(k, v) in data.iter() {
        builder.add(k, v);
    }

    let block_contents = builder.finish();
    let mut block = Block::new(options::for_test(), block_contents).iter();
    let mut i = 0;

    assert!(!block.valid());

    for (k, v) in LdbIteratorIter::wrap(&mut block) {
        assert_eq!(&k[..], data[i].0);
        assert_eq!(v, data[i].1);
        i += 1;
    }
    assert_eq!(i, data.len());
}

pub fn test_block_iterate_reverse() {
    let mut o = options::for_test();
    o.block_restart_interval = 3;
    let data = get_data();
    let mut builder = BlockBuilder::new(o.clone());

    for &(k, v) in data.iter() {
        builder.add(k, v);
    }

    let block_contents = builder.finish();
    let mut block = Block::new(o.clone(), block_contents).iter();

    assert!(!block.valid());
    assert_eq!(
        block.next(),
        Some(("key1".as_bytes().to_vec(), "value1".as_bytes().to_vec()))
    );
    assert!(block.valid());
    block.next();
    assert!(block.valid());
    block.prev();
    assert!(block.valid());
    assert_eq!(
        current_key_val(&block),
        Some(("key1".as_bytes().to_vec(), "value1".as_bytes().to_vec()))
    );
    block.prev();
    assert!(!block.valid());

    // Verify that prev() from the last entry goes to the prev-to-last entry
    // (essentially, that next() returning None doesn't advance anything)
    while let Some(_) = block.next() {}

    block.prev();
    assert!(block.valid());
    assert_eq!(
        current_key_val(&block),
        Some((
            "prefix_key2".as_bytes().to_vec(),
            "value".as_bytes().to_vec()
        ))
    );
}

pub fn test_block_seek() {
    let mut o = options::for_test();
    o.block_restart_interval = 3;

    let data = get_data();
    let mut builder = BlockBuilder::new(o.clone());

    for &(k, v) in data.iter() {
        builder.add(k, v);
    }

    let block_contents = builder.finish();

    let mut block = Block::new(o.clone(), block_contents).iter();

    block.seek(&"prefix_key2".as_bytes());
    assert!(block.valid());
    assert_eq!(
        current_key_val(&block),
        Some((
            "prefix_key2".as_bytes().to_vec(),
            "value".as_bytes().to_vec()
        ))
    );

    block.seek(&"prefix_key0".as_bytes());
    assert!(block.valid());
    assert_eq!(
        current_key_val(&block),
        Some((
            "prefix_key1".as_bytes().to_vec(),
            "value".as_bytes().to_vec()
        ))
    );

    block.seek(&"key1".as_bytes());
    assert!(block.valid());
    assert_eq!(
        current_key_val(&block),
        Some(("key1".as_bytes().to_vec(), "value1".as_bytes().to_vec()))
    );

    block.seek(&"prefix_key3".as_bytes());
    assert!(block.valid());
    assert_eq!(
        current_key_val(&block),
        Some((
            "prefix_key3".as_bytes().to_vec(),
            "value".as_bytes().to_vec()
        ))
    );

    block.seek(&"prefix_key8".as_bytes());
    assert!(!block.valid());
    assert_eq!(current_key_val(&block), None);
}

pub fn test_block_seek_to_last() {
    let mut o = options::for_test();

    // Test with different number of restarts
    for block_restart_interval in vec![2, 6, 10] {
        o.block_restart_interval = block_restart_interval;

        let data = get_data();
        let mut builder = BlockBuilder::new(o.clone());

        for &(k, v) in data.iter() {
            builder.add(k, v);
        }

        let block_contents = builder.finish();

        let mut block = Block::new(o.clone(), block_contents).iter();

        block.seek_to_last();
        assert!(block.valid());
        assert_eq!(
            current_key_val(&block),
            Some((
                "prefix_key3".as_bytes().to_vec(),
                "value".as_bytes().to_vec()
            ))
        );
    }
}

pub fn test_blockhandle() {
    let bh = BlockHandle::new(890, 777);
    let mut dst = [0 as u8; 128];
    let enc_sz = bh.encode_to(&mut dst[..]);

    let (bh2, dec_sz) = BlockHandle::decode(&dst);

    assert_eq!(enc_sz, dec_sz);
    assert_eq!(bh.size(), bh2.size());
    assert_eq!(bh.offset(), bh2.offset());
}
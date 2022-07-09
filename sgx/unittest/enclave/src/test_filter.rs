#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

use std::rc::Rc;
use rusty_leveldb::{
    filter::*,
    filter_block::*,
    key_types::LookupKey,
};

const _BITS_PER_KEY: u32 = 12;

fn input_data() -> (Vec<u8>, Vec<usize>) {
    let mut concat = vec![];
    let mut offs = vec![];

    for d in [
        "abc123def456".as_bytes(),
        "xxx111xxx222".as_bytes(),
        "ab00cd00ab".as_bytes(),
        "908070605040302010".as_bytes(),
    ]
    .iter()
    {
        offs.push(concat.len());
        concat.extend_from_slice(d);
    }
    (concat, offs)
}

/// Creates a filter using the keys from input_data().
fn create_filter() -> Vec<u8> {
    let fpol = BloomPolicy::new(_BITS_PER_KEY);
    let (data, offs) = input_data();
    let filter = fpol.create_filter(&data, &offs);

    assert_eq!(filter, vec![194, 148, 129, 140, 192, 196, 132, 164, 8]);
    filter
}

/// Creates a filter using the keys from input_data() but converted to InternalKey format.
fn create_internalkey_filter() -> Vec<u8> {
    let fpol = Rc::new(Box::new(InternalFilterPolicy::new(BloomPolicy::new(
        _BITS_PER_KEY,
    ))));
    let (data, offs) = input_data();
    let (mut intdata, mut intoffs) = (vec![], vec![]);

    offset_data_iterate(&data, &offs, |key| {
        let ikey = LookupKey::new(key, 123);
        intoffs.push(intdata.len());
        intdata.extend_from_slice(ikey.internal_key());
    });
    let filter = fpol.create_filter(&intdata, &intoffs);

    filter
}

pub fn test_filter_bloom() {
    let f = create_filter();
    let fp = BloomPolicy::new(_BITS_PER_KEY);
    let (data, offs) = input_data();

    offset_data_iterate(&data, &offs, |key| {
        assert!(fp.key_may_match(key, &f));
    });
}

pub fn test_filter_internal_keys_identical() {
    assert_eq!(create_filter(), create_internalkey_filter());
}

pub fn test_filter_bloom_hash() {
    let d1 = vec![0x62];
    let d2 = vec![0xc3, 0x97];
    let d3 = vec![0xe2, 0x99, 0xa5];
    let d4 = vec![0xe1, 0x80, 0xb9, 0x32];

    let fp = BloomPolicy::new_unwrapped(_BITS_PER_KEY);

    assert_eq!(fp.bloom_hash(&d1), 0xef1345c4);
    assert_eq!(fp.bloom_hash(&d2), 0x5b663814);
    assert_eq!(fp.bloom_hash(&d3), 0x323c078f);
    assert_eq!(fp.bloom_hash(&d4), 0xed21633a);
}


fn get_keys() -> Vec<&'static [u8]> {
    vec![
        "abcd".as_bytes(),
        "efgh".as_bytes(),
        "ijkl".as_bytes(),
        "mnopqrstuvwxyz".as_bytes(),
    ]
}

fn produce_filter_block() -> Vec<u8> {
    let keys = get_keys();
    let mut bld = FilterBlockBuilder::new(Rc::new(Box::new(BloomPolicy::new(32))));

    bld.start_block(0);

    for k in keys.iter() {
        bld.add_key(k);
    }

    // second block
    bld.start_block(5000);

    for k in keys.iter() {
        bld.add_key(k);
    }

    bld.finish()
}

pub fn test_filter_index() {
    assert_eq!(get_filter_index(3777, FILTER_BASE_LOG2), 1);
    assert_eq!(get_filter_index(10000, FILTER_BASE_LOG2), 4);
}

pub fn test_filter_block_builder() {
    let result = produce_filter_block();
    // 2 blocks of 4 filters of 4 bytes plus 1B for `k`; plus three filter offsets (because of
    //   the block offsets of 0 and 5000); plus footer
    assert_eq!(result.len(), 2 * (get_keys().len() * 4 + 1) + (3 * 4) + 5);
    assert_eq!(
        result,
        vec![
            234, 195, 25, 155, 61, 141, 173, 140, 221, 28, 222, 92, 220, 112, 234, 227, 22,
            234, 195, 25, 155, 61, 141, 173, 140, 221, 28, 222, 92, 220, 112, 234, 227, 22, 0,
            0, 0, 0, 17, 0, 0, 0, 17, 0, 0, 0, 34, 0, 0, 0, 11,
        ]
    );
}

pub fn test_filter_block_build_read() {
    let result = produce_filter_block();
    let reader = FilterBlockReader::new_owned(Rc::new(Box::new(BloomPolicy::new(32))), result);

    assert_eq!(
        reader.offset_of(get_filter_index(5121, FILTER_BASE_LOG2)),
        17
    ); // third block in third filter

    let unknown_keys = vec![
        "xsb".as_bytes(),
        "9sad".as_bytes(),
        "assssaaaass".as_bytes(),
    ];

    for block_offset in vec![0, 1024, 5000, 6025].into_iter() {
        for key in get_keys().iter() {
            let _fault = format!("{} {:?} ", block_offset, key);
            assert!(reader.key_may_match(block_offset, key));
        }
        for key in unknown_keys.iter() {
            assert!(!reader.key_may_match(block_offset, key));
        }
    }
}
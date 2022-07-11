#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

use std::path::Path;
use std::rc::Rc;
use std::cmp::Ordering;
use rusty_leveldb::{
    version::*,
    version_edit::*,
    version_set::*,
    cmp::{DefaultCmp, InternalKeyCmp, Cmp},
    env::Env,
    key_types::{ValueType, LookupKey},
    options::{self, Options},
    table_builder::TableBuilder,
    table_cache::{table_file_name, TableCache},
    types::{share, FileMetaData, FileNum, MAX_SEQUENCE_NUMBER, NUM_LEVELS},
    Result,
    merging_iter::MergingIter,
    test_util::{test_iterator_properties, LdbIteratorIter},
    LdbIterator,
    log::LogWriter,
};


fn new_file(
    num: u64,
    smallest: &[u8],
    smallestix: u64,
    largest: &[u8],
    largestix: u64,
) -> FileMetaHandle {
    share(FileMetaData {
        allowed_seeks: 10,
        size: 163840,
        num,
        smallest: LookupKey::new(smallest, smallestix).internal_key().to_vec(),
        largest: LookupKey::new(largest, largestix).internal_key().to_vec(),
    })
}

/// write_table creates a table with the given number and contents (must be sorted!) in the
/// memenv. The sequence numbers given to keys start with startseq.
fn write_table(
    me: &Box<dyn Env>,
    contents: &[(&[u8], &[u8], ValueType)],
    startseq: u64,
    num: FileNum,
) -> FileMetaHandle {
    let dst = me
        .open_writable_file(Path::new(&table_file_name("db", num)))
        .unwrap();
    let mut seq = startseq;
    let keys: Vec<Vec<u8>> = contents
        .iter()
        .map(|&(k, _, typ)| {
            seq += 1;
            LookupKey::new_full(k, seq - 1, typ).internal_key().to_vec()
        })
        .collect();

    let mut tbl = TableBuilder::new(options::for_test(), dst);
    for i in 0..contents.len() {
        tbl.add(&keys[i], contents[i].1).unwrap();
        seq += 1;
    }
    let f = new_file(
        num,
        contents[0].0,
        startseq,
        contents[contents.len() - 1].0,
        startseq + (contents.len() - 1) as u64,
    );
    f.borrow_mut().size = tbl.finish().unwrap();
    f
}

fn make_version() -> (Version, Options) {
    let opts = options::for_test();
    let env = opts.env.clone();

    // The different levels overlap in a sophisticated manner to be able to test compactions
    // and so on.
    // The sequence numbers are in "natural order", i.e. highest levels have lowest sequence
    // numbers.

    // Level 0 (overlapping)
    let f2: &[(&[u8], &[u8], ValueType)] = &[
        ("aac".as_bytes(), "val1".as_bytes(), ValueType::TypeDeletion),
        ("aax".as_bytes(), "val2".as_bytes(), ValueType::TypeValue),
        ("aba".as_bytes(), "val3".as_bytes(), ValueType::TypeValue),
        ("bab".as_bytes(), "val4".as_bytes(), ValueType::TypeValue),
        ("bba".as_bytes(), "val5".as_bytes(), ValueType::TypeValue),
    ];
    let t2 = write_table(&env, f2, 26, 2);
    let f1: &[(&[u8], &[u8], ValueType)] = &[
        ("aaa".as_bytes(), "val1".as_bytes(), ValueType::TypeValue),
        ("aab".as_bytes(), "val2".as_bytes(), ValueType::TypeValue),
        ("aac".as_bytes(), "val3".as_bytes(), ValueType::TypeValue),
        ("aba".as_bytes(), "val4".as_bytes(), ValueType::TypeValue),
    ];
    let t1 = write_table(&env, f1, 22, 1);
    // Level 1
    let f3: &[(&[u8], &[u8], ValueType)] = &[
        ("aaa".as_bytes(), "val0".as_bytes(), ValueType::TypeValue),
        ("cab".as_bytes(), "val2".as_bytes(), ValueType::TypeValue),
        ("cba".as_bytes(), "val3".as_bytes(), ValueType::TypeValue),
    ];
    let t3 = write_table(&env, f3, 19, 3);
    let f4: &[(&[u8], &[u8], ValueType)] = &[
        ("daa".as_bytes(), "val1".as_bytes(), ValueType::TypeValue),
        ("dab".as_bytes(), "val2".as_bytes(), ValueType::TypeValue),
        ("dba".as_bytes(), "val3".as_bytes(), ValueType::TypeValue),
    ];
    let t4 = write_table(&env, f4, 16, 4);
    let f5: &[(&[u8], &[u8], ValueType)] = &[
        ("eaa".as_bytes(), "val1".as_bytes(), ValueType::TypeValue),
        ("eab".as_bytes(), "val2".as_bytes(), ValueType::TypeValue),
        ("fab".as_bytes(), "val3".as_bytes(), ValueType::TypeValue),
    ];
    let t5 = write_table(&env, f5, 13, 5);
    // Level 2
    let f6: &[(&[u8], &[u8], ValueType)] = &[
        ("cab".as_bytes(), "val1".as_bytes(), ValueType::TypeValue),
        ("fab".as_bytes(), "val2".as_bytes(), ValueType::TypeValue),
        ("fba".as_bytes(), "val3".as_bytes(), ValueType::TypeValue),
    ];
    let t6 = write_table(&env, f6, 10, 6);
    let f7: &[(&[u8], &[u8], ValueType)] = &[
        ("gaa".as_bytes(), "val1".as_bytes(), ValueType::TypeValue),
        ("gab".as_bytes(), "val2".as_bytes(), ValueType::TypeValue),
        ("gba".as_bytes(), "val3".as_bytes(), ValueType::TypeValue),
        ("gca".as_bytes(), "val4".as_bytes(), ValueType::TypeDeletion),
        ("gda".as_bytes(), "val5".as_bytes(), ValueType::TypeValue),
    ];
    let t7 = write_table(&env, f7, 5, 7);
    // Level 3 (2 * 2 entries, for iterator behavior).
    let f8: &[(&[u8], &[u8], ValueType)] = &[
        ("haa".as_bytes(), "val1".as_bytes(), ValueType::TypeValue),
        ("hba".as_bytes(), "val2".as_bytes(), ValueType::TypeValue),
    ];
    let t8 = write_table(&env, f8, 3, 8);
    let f9: &[(&[u8], &[u8], ValueType)] = &[
        ("iaa".as_bytes(), "val1".as_bytes(), ValueType::TypeValue),
        ("iba".as_bytes(), "val2".as_bytes(), ValueType::TypeValue),
    ];
    let t9 = write_table(&env, f9, 1, 9);

    let cache = TableCache::new("db", opts.clone(), 100);
    let mut v = Version::new(share(cache), Rc::new(Box::new(DefaultCmp)));
    v.files[0] = vec![t1, t2];
    v.files[1] = vec![t3, t4, t5];
    v.files[2] = vec![t6, t7];
    v.files[3] = vec![t8, t9];
    (v, opts)
}


pub fn test_version_concat_iter() {
    let v = make_version().0;

    let expected_entries = vec![0, 9, 8, 4];
    for l in 1..4 {
        let mut iter = v.new_concat_iter(l);
        let iter = LdbIteratorIter::wrap(&mut iter);
        assert_eq!(iter.count(), expected_entries[l]);
    }
}

pub fn test_version_concat_iter_properties() {
    let v = make_version().0;
    let iter = v.new_concat_iter(3);
    test_iterator_properties(iter);
}

pub fn test_version_max_next_level_overlapping() {
    let v = make_version().0;
    assert_eq!(218, v.max_next_level_overlapping_bytes());
}

pub fn test_version_all_iters() {
    let v = make_version().0;
    let iters = v.new_iters().unwrap();
    let mut opt = options::for_test();
    opt.cmp = Rc::new(Box::new(InternalKeyCmp(Rc::new(Box::new(DefaultCmp)))));

    let mut miter = MergingIter::new(opt.cmp.clone(), iters);
    assert_eq!(LdbIteratorIter::wrap(&mut miter).count(), 30);

    // Check that all elements are in order.
    let init = LookupKey::new("000".as_bytes(), MAX_SEQUENCE_NUMBER);
    let cmp = InternalKeyCmp(Rc::new(Box::new(DefaultCmp)));
    LdbIteratorIter::wrap(&mut miter).fold(init.internal_key().to_vec(), |b, (k, _)| {
        assert!(cmp.cmp(&b, &k) == Ordering::Less);
        k
    });
}

pub fn test_version_summary() {
    let v = make_version().0;
    let expected = "level 0: 2 files, 483 bytes ([(1, 232), (2, 251)]); level 1: 3 files, 651 \
                    bytes ([(3, 218), (4, 216), (5, 217)]); level 2: 2 files, 468 bytes ([(6, \
                    218), (7, 250)]); level 3: 2 files, 400 bytes ([(8, 200), (9, 200)]); ";
    assert_eq!(expected, &v.level_summary());
}

pub fn test_version_get_simple() {
    let v = make_version().0;
    let cases: &[(&[u8], u64, Result<Option<Vec<u8>>>)] = &[
        ("aaa".as_bytes(), 1, Ok(None)),
        ("aaa".as_bytes(), 100, Ok(Some("val1".as_bytes().to_vec()))),
        ("aaa".as_bytes(), 21, Ok(Some("val0".as_bytes().to_vec()))),
        ("aab".as_bytes(), 0, Ok(None)),
        ("aab".as_bytes(), 100, Ok(Some("val2".as_bytes().to_vec()))),
        ("aac".as_bytes(), 100, Ok(None)),
        ("aac".as_bytes(), 25, Ok(Some("val3".as_bytes().to_vec()))),
        ("aba".as_bytes(), 100, Ok(Some("val3".as_bytes().to_vec()))),
        ("aba".as_bytes(), 25, Ok(Some("val4".as_bytes().to_vec()))),
        ("daa".as_bytes(), 100, Ok(Some("val1".as_bytes().to_vec()))),
        ("dab".as_bytes(), 1, Ok(None)),
        ("dac".as_bytes(), 100, Ok(None)),
        ("gba".as_bytes(), 100, Ok(Some("val3".as_bytes().to_vec()))),
        // deleted key
        ("gca".as_bytes(), 100, Ok(None)),
        ("gbb".as_bytes(), 100, Ok(None)),
    ];

    for ref c in cases {
        match v.get(LookupKey::new(c.0, c.1).internal_key()) {
            Ok(Some((val, _))) => assert_eq!(c.2.as_ref().unwrap().as_ref().unwrap(), &val),
            Ok(None) => assert!(c.2.as_ref().unwrap().as_ref().is_none()),
            Err(_) => assert!(c.2.is_err()),
        }
    }
}

pub fn test_version_get_overlapping_basic() {
    let v = make_version().0;

    // Overlapped by tables 1 and 2.
    let ol = v.get_overlapping(LookupKey::new(b"aay", 50).internal_key());
    // Check that sorting order is newest-first in L0.
    assert_eq!(2, ol[0][0].borrow().num);
    // Check that table from L1 matches.
    assert_eq!(3, ol[1][0].borrow().num);

    let ol = v.get_overlapping(LookupKey::new(b"cb", 50).internal_key());
    assert_eq!(3, ol[1][0].borrow().num);
    assert_eq!(6, ol[2][0].borrow().num);

    let ol = v.get_overlapping(LookupKey::new(b"x", 50).internal_key());
    for i in 0..NUM_LEVELS {
        assert!(ol[i].is_empty());
    }
}

pub fn test_version_overlap_in_level() {
    let v = make_version().0;

    for &(level, (k1, k2), want) in &[
        (0, ("000".as_bytes(), "003".as_bytes()), false),
        (0, ("aa0".as_bytes(), "abx".as_bytes()), true),
        (1, ("012".as_bytes(), "013".as_bytes()), false),
        (1, ("abc".as_bytes(), "def".as_bytes()), true),
        (2, ("xxx".as_bytes(), "xyz".as_bytes()), false),
        (2, ("gac".as_bytes(), "gaz".as_bytes()), true),
    ] {
        if want {
            assert!(v.overlap_in_level(level, k1, k2));
        } else {
            assert!(!v.overlap_in_level(level, k1, k2));
        }
    }
}

pub fn test_version_pick_memtable_output_level() {
    let v = make_version().0;

    for c in [
        ("000".as_bytes(), "abc".as_bytes(), 0),
        ("gab".as_bytes(), "hhh".as_bytes(), 1),
        ("000".as_bytes(), "111".as_bytes(), 2),
    ]
    .iter()
    {
        assert_eq!(c.2, v.pick_memtable_output_level(c.0, c.1));
    }
}

pub fn test_version_overlapping_inputs() {
    let v = make_version().0;

    // time_test!("overlapping-inputs");
    {
        // Range is expanded in overlapping level-0 files.
        let from = LookupKey::new("aab".as_bytes(), MAX_SEQUENCE_NUMBER);
        let to = LookupKey::new("aae".as_bytes(), 0);
        let r = v.overlapping_inputs(0, from.internal_key(), to.internal_key());
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].borrow().num, 1);
        assert_eq!(r[1].borrow().num, 2);
    }
    {
        let from = LookupKey::new("cab".as_bytes(), MAX_SEQUENCE_NUMBER);
        let to = LookupKey::new("cbx".as_bytes(), 0);
        // expect one file.
        let r = v.overlapping_inputs(1, from.internal_key(), to.internal_key());
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].borrow().num, 3);
    }
    {
        let from = LookupKey::new("cab".as_bytes(), MAX_SEQUENCE_NUMBER);
        let to = LookupKey::new("ebx".as_bytes(), 0);
        let r = v.overlapping_inputs(1, from.internal_key(), to.internal_key());
        // Assert that correct number of files and correct files were returned.
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].borrow().num, 3);
        assert_eq!(r[1].borrow().num, 4);
        assert_eq!(r[2].borrow().num, 5);
    }
    {
        let from = LookupKey::new("hhh".as_bytes(), MAX_SEQUENCE_NUMBER);
        let to = LookupKey::new("ijk".as_bytes(), 0);
        let r = v.overlapping_inputs(2, from.internal_key(), to.internal_key());
        assert_eq!(r.len(), 0);
        let r = v.overlapping_inputs(1, from.internal_key(), to.internal_key());
        assert_eq!(r.len(), 0);
    }
}

pub fn test_version_record_read_sample() {
    let mut v = make_version().0;
    let k = LookupKey::new("aab".as_bytes(), MAX_SEQUENCE_NUMBER);
    let only_in_one = LookupKey::new("cax".as_bytes(), MAX_SEQUENCE_NUMBER);

    assert!(!v.record_read_sample(k.internal_key()));
    assert!(!v.record_read_sample(only_in_one.internal_key()));

    for fs in v.files.iter() {
        for f in fs {
            f.borrow_mut().allowed_seeks = 0;
        }
    }
    assert!(v.record_read_sample(k.internal_key()));
}

pub fn test_version_key_ordering() {
    // time_test!();
    let fmh = new_file(1, &[1, 0, 0], 0, &[2, 0, 0], 1);
    let cmp = InternalKeyCmp(Rc::new(Box::new(DefaultCmp)));

    // Keys before file.
    for k in &[&[0][..], &[1], &[1, 0], &[0, 9, 9, 9]] {
        assert!(key_is_before_file(&cmp, k, &fmh));
        assert!(!key_is_after_file(&cmp, k, &fmh));
    }
    // Keys in file.
    for k in &[
        &[1, 0, 0][..],
        &[1, 0, 1],
        &[1, 2, 3, 4],
        &[1, 9, 9],
        &[2, 0, 0],
    ] {
        assert!(!key_is_before_file(&cmp, k, &fmh));
        assert!(!key_is_after_file(&cmp, k, &fmh));
    }
    // Keys after file.
    for k in &[&[2, 0, 1][..], &[9, 9, 9], &[9, 9, 9, 9]] {
        assert!(!key_is_before_file(&cmp, k, &fmh));
        assert!(key_is_after_file(&cmp, k, &fmh));
    }
}

pub fn test_version_file_overlaps() {
    // time_test!();

    let files_disjoint = [
        new_file(1, &[2, 0, 0], 0, &[3, 0, 0], 1),
        new_file(2, &[3, 0, 1], 0, &[4, 0, 0], 1),
        new_file(3, &[4, 0, 1], 0, &[5, 0, 0], 1),
    ];
    let files_joint = [
        new_file(1, &[2, 0, 0], 0, &[3, 0, 0], 1),
        new_file(2, &[2, 5, 0], 0, &[4, 0, 0], 1),
        new_file(3, &[3, 5, 1], 0, &[5, 0, 0], 1),
    ];
    let cmp = InternalKeyCmp(Rc::new(Box::new(DefaultCmp)));

    assert!(some_file_overlaps_range(
        &cmp,
        &files_joint,
        &[2, 5, 0],
        &[3, 1, 0]
    ));
    assert!(some_file_overlaps_range(
        &cmp,
        &files_joint,
        &[2, 5, 0],
        &[7, 0, 0]
    ));
    assert!(some_file_overlaps_range(
        &cmp,
        &files_joint,
        &[0, 0],
        &[2, 0, 0]
    ));
    assert!(some_file_overlaps_range(
        &cmp,
        &files_joint,
        &[0, 0],
        &[7, 0, 0]
    ));
    assert!(!some_file_overlaps_range(
        &cmp,
        &files_joint,
        &[0, 0],
        &[0, 5]
    ));
    assert!(!some_file_overlaps_range(
        &cmp,
        &files_joint,
        &[6, 0],
        &[7, 5]
    ));

    assert!(some_file_overlaps_range_disjoint(
        &cmp,
        &files_disjoint,
        &[2, 0, 1],
        &[2, 5, 0]
    ));
    assert!(some_file_overlaps_range_disjoint(
        &cmp,
        &files_disjoint,
        &[3, 0, 1],
        &[4, 9, 0]
    ));
    assert!(some_file_overlaps_range_disjoint(
        &cmp,
        &files_disjoint,
        &[2, 0, 1],
        &[6, 5, 0]
    ));
    assert!(some_file_overlaps_range_disjoint(
        &cmp,
        &files_disjoint,
        &[0, 0, 1],
        &[2, 5, 0]
    ));
    assert!(some_file_overlaps_range_disjoint(
        &cmp,
        &files_disjoint,
        &[0, 0, 1],
        &[6, 5, 0]
    ));
    assert!(!some_file_overlaps_range_disjoint(
        &cmp,
        &files_disjoint,
        &[0, 0, 1],
        &[0, 1]
    ));
    assert!(!some_file_overlaps_range_disjoint(
        &cmp,
        &files_disjoint,
        &[6, 0, 1],
        &[7, 0, 1]
    ));
}

pub fn test_version_edit_encode_decode() {
    let mut ve = VersionEdit::new();

    ve.set_comparator_name(DefaultCmp.id());
    ve.set_log_num(123);
    ve.set_next_file(456);
    ve.set_compact_pointer(0, &[0, 1, 2]);
    ve.set_compact_pointer(1, &[3, 4, 5]);
    ve.set_compact_pointer(2, &[6, 7, 8]);
    ve.add_file(
        0,
        FileMetaData {
            allowed_seeks: 12345,
            num: 901,
            size: 234,
            smallest: vec![5, 6, 7],
            largest: vec![8, 9, 0],
        },
    );
    ve.delete_file(1, 132);

    let encoded = ve.encode();

    let decoded = VersionEdit::decode_from(encoded.as_ref()).unwrap();

    assert_eq!(decoded.comparator, Some(DefaultCmp.id().to_string()));
    assert_eq!(decoded.log_number, Some(123));
    assert_eq!(decoded.next_file_number, Some(456));
    assert_eq!(decoded.compaction_ptrs.len(), 3);
    assert_eq!(
        decoded.compaction_ptrs[0],
        CompactionPointer {
            level: 0,
            key: vec![0, 1, 2],
        }
    );
    assert_eq!(
        decoded.compaction_ptrs[1],
        CompactionPointer {
            level: 1,
            key: vec![3, 4, 5],
        }
    );
    assert_eq!(
        decoded.compaction_ptrs[2],
        CompactionPointer {
            level: 2,
            key: vec![6, 7, 8],
        }
    );
    assert_eq!(decoded.new_files.len(), 1);
    assert_eq!(
        decoded.new_files[0],
        (
            0,
            FileMetaData {
                allowed_seeks: 0,
                num: 901,
                size: 234,
                smallest: vec![5, 6, 7],
                largest: vec![8, 9, 0],
            }
        )
    );
    assert_eq!(decoded.deleted.len(), 1);
    assert!(decoded.deleted.contains(&(1, 132)));
}


fn example_files() -> Vec<FileMetaHandle> {
    let mut f1 = FileMetaData::default();
    f1.num = 1;
    f1.size = 10;
    f1.smallest = "f".as_bytes().to_vec();
    f1.largest = "g".as_bytes().to_vec();
    let mut f2 = FileMetaData::default();
    f2.num = 2;
    f2.size = 20;
    f2.smallest = "e".as_bytes().to_vec();
    f2.largest = "f".as_bytes().to_vec();
    let mut f3 = FileMetaData::default();
    f3.num = 3;
    f3.size = 30;
    f3.smallest = "a".as_bytes().to_vec();
    f3.largest = "b".as_bytes().to_vec();
    let mut f4 = FileMetaData::default();
    f4.num = 4;
    f4.size = 40;
    f4.smallest = "q".as_bytes().to_vec();
    f4.largest = "z".as_bytes().to_vec();
    vec![f1, f2, f3, f4].into_iter().map(share).collect()
}

pub fn test_version_set_merge_iters() {
    let v1 = vec![2, 4, 6, 8, 10];
    let v2 = vec![1, 3, 5, 7];
    assert_eq!(
        vec![1, 2, 3, 4, 5, 6, 7, 8, 10],
        merge_iters(v1.into_iter(), v2.into_iter(), |a, b| a.cmp(&b))
    );
}

pub fn test_version_set_total_size() {
    assert_eq!(100, total_size(example_files().iter()));
}

pub fn test_version_set_get_range() {
    let cmp = DefaultCmp;
    let fs = example_files();
    assert_eq!(
        ("a".as_bytes().to_vec(), "z".as_bytes().to_vec()),
        get_range(&cmp, fs.iter())
    );
}

pub fn test_version_set_builder() {
    let (v, opt) = make_version();
    let v = share(v);

    let mut fmd = FileMetaData::default();
    fmd.num = 21;
    fmd.size = 123;
    fmd.smallest = LookupKey::new("klm".as_bytes(), 777)
        .internal_key()
        .to_vec();
    fmd.largest = LookupKey::new("kop".as_bytes(), 700)
        .internal_key()
        .to_vec();

    let mut ve = VersionEdit::new();
    ve.add_file(1, fmd);
    // This deletion should be undone by apply().
    ve.delete_file(1, 21);
    ve.delete_file(0, 2);
    ve.set_compact_pointer(2, LookupKey::new("xxx".as_bytes(), 123).internal_key());

    let mut b = Builder::new();
    let mut ptrs: [Vec<u8>; NUM_LEVELS] = Default::default();
    b.apply(&ve, &mut ptrs);

    assert_eq!(
        &[120 as u8, 120, 120, 1, 123, 0, 0, 0, 0, 0, 0],
        ptrs[2].as_slice()
    );
    assert_eq!(2, b.deleted[0][0]);
    assert_eq!(1, b.added[1].len());

    let mut v2 = Version::new(
        share(TableCache::new("db", opt.clone(), 100)),
        opt.cmp.clone(),
    );
    b.save_to(&InternalKeyCmp(opt.cmp.clone()), &v, &mut v2);
    // Second file in L0 was removed.
    assert_eq!(1, v2.files[0].len());
    // File was added to L1.
    assert_eq!(4, v2.files[1].len());
    assert_eq!(21, v2.files[1][3].borrow().num);
}

pub fn test_version_set_log_and_apply() {
    let (_, opt) = make_version();
    let mut vs = VersionSet::new(
        "db",
        opt.clone(),
        share(TableCache::new("db", opt.clone(), 100)),
    );

    assert_eq!(2, vs.new_file_number());
    // Simulate NewDB
    {
        let mut ve = VersionEdit::new();
        ve.set_comparator_name("leveldb.BytewiseComparator");
        ve.set_log_num(10);
        ve.set_next_file(20);
        ve.set_last_seq(30);

        // Write first manifest to be recovered from.
        let manifest = manifest_file_name("db", 19);
        let mffile = opt.env.open_writable_file(Path::new(&manifest)).unwrap();
        let mut lw = LogWriter::new(mffile);
        lw.add_record(&ve.encode()).unwrap();
        lw.flush().unwrap();
        set_current_file(&opt.env.as_ref(), "db", 19).unwrap();
    }

    // Recover from new state.
    {
        vs.recover().unwrap();
        assert_eq!(10, vs.log_num);
        assert_eq!(21, vs.next_file_num);
        assert_eq!(30, vs.last_seq);
        assert_eq!(0, vs.current.as_ref().unwrap().borrow().files[0].len());
        assert_eq!(0, vs.current.as_ref().unwrap().borrow().files[1].len());
        assert_eq!(35, vs.write_snapshot().unwrap());
    }

    // Simulate compaction by adding a file.
    {
        let mut ve = VersionEdit::new();
        ve.set_log_num(11);
        let mut fmd = FileMetaData::default();
        fmd.num = 21;
        fmd.size = 123;
        fmd.smallest = LookupKey::new("abc".as_bytes(), 777)
            .internal_key()
            .to_vec();
        fmd.largest = LookupKey::new("def".as_bytes(), 700)
            .internal_key()
            .to_vec();
        ve.add_file(1, fmd);
        vs.log_and_apply(ve).unwrap();

        assert!(opt.env.exists(Path::new("db/CURRENT")).unwrap());
        assert!(opt.env.exists(Path::new("db/MANIFEST-000019")).unwrap());
        // next_file_num and last_seq are untouched by log_and_apply
        assert_eq!(21, vs.new_file_number());
        assert_eq!(22, vs.next_file_num);
        assert_eq!(30, vs.last_seq);
        // the following fields are touched by log_and_apply.
        assert_eq!(11, vs.log_num);

        // The previous "compaction" should have added one file to the first level in the
        // current version.
        assert_eq!(0, vs.current.as_ref().unwrap().borrow().files[0].len());
        assert_eq!(1, vs.current.as_ref().unwrap().borrow().files[1].len());
        assert_eq!(63, vs.write_snapshot().unwrap());
    }
}

pub fn test_version_set_utils() {
    let (v, opt) = make_version();
    let mut vs = VersionSet::new("db", opt.clone(), share(TableCache::new("db", opt, 100)));
    vs.add_version(v);
    // live_files()
    assert_eq!(9, vs.live_files().len());
    assert!(vs.live_files().contains(&3));

    let v = vs.current();
    let v = v.borrow();
    // num_level_bytes()
    assert_eq!(483, v.num_level_bytes(0));
    assert_eq!(651, v.num_level_bytes(1));
    assert_eq!(468, v.num_level_bytes(2));
    // num_level_files()
    assert_eq!(2, v.num_level_files(0));
    assert_eq!(3, v.num_level_files(1));
    assert_eq!(2, v.num_level_files(2));
    // new_file_number()
    assert_eq!(2, vs.new_file_number());
    assert_eq!(3, vs.new_file_number());
}

pub fn test_version_set_pick_compaction() {
    let (mut v, opt) = make_version();
    let mut vs = VersionSet::new("db", opt.clone(), share(TableCache::new("db", opt, 100)));

    v.compaction_score = Some(2.0);
    v.compaction_level = Some(0);
    vs.add_version(v);

    // Size compaction
    {
        let c = vs.pick_compaction().unwrap();
        assert_eq!(2, c.inputs[0].len());
        assert_eq!(1, c.inputs[1].len());
        assert_eq!(0, c.level);
        assert!(c.input_version.is_some());
    }
    // Seek compaction
    {
        let current = vs.current();
        current.borrow_mut().compaction_score = None;
        current.borrow_mut().compaction_level = None;
        current.borrow_mut().file_to_compact_lvl = 1;

        let fmd = current.borrow().files[1][0].clone();
        current.borrow_mut().file_to_compact = Some(fmd);

        let c = vs.pick_compaction().unwrap();
        assert_eq!(3, c.inputs[0].len()); // inputs on l+0 are expanded.
        assert_eq!(1, c.inputs[1].len());
        assert_eq!(1, c.level);
        assert!(c.input_version.is_some());
    }
}

/// iterator_properties tests that it contains len elements and that they are ordered in
/// ascending order by cmp.
fn iterator_properties<It: LdbIterator>(mut it: It, len: usize, cmp: Rc<Box<dyn Cmp>>) {
    let mut wr = LdbIteratorIter::wrap(&mut it);
    let first = wr.next().unwrap();
    let mut count = 1;
    wr.fold(first, |(a, _), (b, c)| {
        assert_eq!(Ordering::Less, cmp.cmp(&a, &b));
        count += 1;
        (b, c)
    });
    assert_eq!(len, count);
}

pub fn test_version_set_compaction() {
    let (v, opt) = make_version();
    let mut vs = VersionSet::new("db", opt.clone(), share(TableCache::new("db", opt, 100)));
    // time_test!();
    vs.add_version(v);

    {
        // approximate_offset()
        let v = vs.current();
        assert_eq!(
            0,
            vs.approximate_offset(&v, LookupKey::new("aaa".as_bytes(), 9000).internal_key())
        );
        assert_eq!(
            232,
            vs.approximate_offset(&v, LookupKey::new("bab".as_bytes(), 9000).internal_key())
        );
        assert_eq!(
            1134,
            vs.approximate_offset(&v, LookupKey::new("fab".as_bytes(), 9000).internal_key())
        );
    }
    // The following tests reuse the same version set and verify that various compactions work
    // like they should.
    {
        // time_test!("compaction tests");
        // compact level 0 with a partial range.
        let from = LookupKey::new("000".as_bytes(), 1000);
        let to = LookupKey::new("ab".as_bytes(), 1010);
        let c = vs
            .compact_range(0, from.internal_key(), to.internal_key())
            .unwrap();
        assert_eq!(2, c.inputs[0].len());
        assert_eq!(1, c.inputs[1].len());
        assert_eq!(1, c.grandparents.unwrap().len());

        // compact level 0, but entire range of keys in version.
        let from = LookupKey::new("000".as_bytes(), 1000);
        let to = LookupKey::new("zzz".as_bytes(), 1010);
        let c = vs
            .compact_range(0, from.internal_key(), to.internal_key())
            .unwrap();
        assert_eq!(2, c.inputs[0].len());
        assert_eq!(1, c.inputs[1].len());
        assert_eq!(1, c.grandparents.as_ref().unwrap().len());
        iterator_properties(
            vs.make_input_iterator(&c),
            12,
            Rc::new(Box::new(vs.cmp.clone())),
        );

        // Expand input range on higher level.
        let from = LookupKey::new("dab".as_bytes(), 1000);
        let to = LookupKey::new("eab".as_bytes(), 1010);
        let c = vs
            .compact_range(1, from.internal_key(), to.internal_key())
            .unwrap();
        assert_eq!(3, c.inputs[0].len());
        assert_eq!(1, c.inputs[1].len());
        assert_eq!(0, c.grandparents.as_ref().unwrap().len());
        iterator_properties(
            vs.make_input_iterator(&c),
            12,
            Rc::new(Box::new(vs.cmp.clone())),
        );

        // is_trivial_move
        let from = LookupKey::new("fab".as_bytes(), 1000);
        let to = LookupKey::new("fba".as_bytes(), 1010);
        let mut c = vs
            .compact_range(2, from.internal_key(), to.internal_key())
            .unwrap();
        // pretend it's not manual
        c.manual = false;
        assert!(c.is_trivial_move());

        // should_stop_before
        let from = LookupKey::new("000".as_bytes(), 1000);
        let to = LookupKey::new("zzz".as_bytes(), 1010);
        let mid = LookupKey::new("abc".as_bytes(), 1010);
        let mut c = vs
            .compact_range(0, from.internal_key(), to.internal_key())
            .unwrap();
        assert!(!c.should_stop_before(from.internal_key()));
        assert!(!c.should_stop_before(mid.internal_key()));
        assert!(!c.should_stop_before(to.internal_key()));

        // is_base_level_for
        let from = LookupKey::new("000".as_bytes(), 1000);
        let to = LookupKey::new("zzz".as_bytes(), 1010);
        let mut c = vs
            .compact_range(0, from.internal_key(), to.internal_key())
            .unwrap();
        assert!(c.is_base_level_for("aaa".as_bytes()));
        assert!(!c.is_base_level_for("hac".as_bytes()));

        // input/add_input_deletions
        let from = LookupKey::new("000".as_bytes(), 1000);
        let to = LookupKey::new("zzz".as_bytes(), 1010);
        let mut c = vs
            .compact_range(0, from.internal_key(), to.internal_key())
            .unwrap();
        for inp in &[(0, 0, 1), (0, 1, 2), (1, 0, 3)] {
            let f = &c.inputs[inp.0][inp.1];
            assert_eq!(inp.2, f.borrow().num);
        }
        c.add_input_deletions();
        assert_eq!(23, c.edit().encode().len())
    }
}
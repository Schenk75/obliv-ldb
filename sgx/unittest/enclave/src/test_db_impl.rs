#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

use std::{
    rc::Rc,
    path::Path,
    mem,
    collections::{HashMap, HashSet},
    iter::FromIterator
};
use rusty_leveldb::{
    db_impl::*,
    version::{Version, FileMetaHandle},
    version_edit::VersionEdit,
    version_set::{manifest_file_name, set_current_file, Compaction},
    options::{self, Options},
    types::{share, FileMetaData, FileNum, NUM_LEVELS, current_key_val, Direction, LdbIterator},
    key_types::{ValueType, LookupKey},
    cmp::DefaultCmp,
    env::Env,
    mem_env::MemEnv,
    table_builder::TableBuilder,
    table_cache::{TableCache, table_file_name},
    log::LogWriter,
    error::{Status, StatusCode},
    test_util::LdbIteratorIter,
    memtable::MemTable,
};

#[macro_export]
macro_rules! log {
    ($l:expr) => ($l.as_ref().map(|l| l.borrow_mut().0.write("\n".as_bytes()).is_ok()));
    ($l:expr, $fmt:expr) => (
        $l.as_ref().map(|l| l.borrow_mut().0.write(concat!($fmt, "\n").as_bytes()).is_ok()));
    ($l:expr, $fmt:expr, $($arg:tt)*) => (
        $l.as_ref().map(
            |l| l.borrow_mut().0.write_fmt(format_args!(concat!($fmt, "\n"), $($arg)*)).is_ok()));
}

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

/// build_db creates a database filled with the tables created by make_version().
fn build_db() -> (DB, Options) {
    let name = "db";
    let (v, mut opt) = make_version();
    opt.reuse_logs = false;
    opt.reuse_manifest = false;
    let mut ve = VersionEdit::new();
    ve.set_comparator_name(opt.cmp.id());
    ve.set_log_num(0);
    // 9 files + 1 manifest we write below.
    ve.set_next_file(11);
    // 30 entries in these tables.
    ve.set_last_seq(30);

    for l in 0..NUM_LEVELS {
        for f in &v.files[l] {
            ve.add_file(l, f.borrow().clone());
        }
    }

    let manifest = manifest_file_name(name, 10);
    let manifest_file = opt.env.open_writable_file(Path::new(&manifest)).unwrap();
    let mut lw = LogWriter::new(manifest_file);
    lw.add_record(&ve.encode()).unwrap();
    lw.flush().unwrap();
    set_current_file(&opt.env, name, 10).unwrap();

    (DB::open(name, opt.clone()).unwrap(), opt)
}

/// set_file_to_compact ensures that the specified table file will be compacted next.
fn set_file_to_compact(db: &mut DB, num: FileNum) {
    let v = db.current();
    let mut v = v.borrow_mut();

    let mut ftc = None;
    for l in 0..NUM_LEVELS {
        for f in &v.files[l] {
            if f.borrow().num == num {
                ftc = Some((f.clone(), l));
            }
        }
    }
    if let Some((f, l)) = ftc {
        v.file_to_compact = Some(f);
        v.file_to_compact_lvl = l;
    } else {
        panic!("file number not found");
    }
}

fn build_memtable() -> MemTable {
    let mut mt = MemTable::new(options::for_test().cmp);
    let mut i = 1;
    for k in ["abc", "def", "ghi", "jkl", "mno", "aabc", "test123"].iter() {
        mt.add(
            i,
            ValueType::TypeValue,
            k.as_bytes(),
            "looooongval".as_bytes(),
        );
        i += 1;
    }
    mt
}

pub fn test_db_impl_open_info_log() {
    let e = MemEnv::new();
    {
        let l = Some(share(open_info_log(&e, "abc")));
        assert!(e.exists(Path::new("abc/LOG")).unwrap());
        log!(l, "hello {}", "world");
        assert_eq!(12, e.size_of(Path::new("abc/LOG")).unwrap());
    }
    {
        let l = Some(share(open_info_log(&e, "abc")));
        assert!(e.exists(Path::new("abc/LOG.old")).unwrap());
        assert!(e.exists(Path::new("abc/LOG")).unwrap());
        assert_eq!(12, e.size_of(Path::new("abc/LOG.old")).unwrap());
        assert_eq!(0, e.size_of(Path::new("abc/LOG")).unwrap());
        log!(l, "something else");
        log!(l, "and another {}", 1);

        let mut s = String::new();
        let mut r = e.open_sequential_file(Path::new("abc/LOG")).unwrap();
        r.read_to_string(&mut s).unwrap();
        assert_eq!("something else\nand another 1\n", &s);
    }
}

pub fn test_db_impl_init() {
    // A sanity check for recovery and basic persistence.
    let opt = options::for_test();
    let env = opt.env.clone();

    // Several test cases with different options follow. The printlns can eventually be
    // removed.

    {
        let mut opt = opt.clone();
        opt.reuse_manifest = false;
        let _ = DB::open("otherdb", opt.clone()).unwrap();

        println!(
            "children after: {:?}",
            env.children(Path::new("otherdb/")).unwrap()
        );
        assert!(env.exists(Path::new("otherdb/CURRENT")).unwrap());
        // Database is initialized and initial manifest reused.
        assert!(!env.exists(Path::new("otherdb/MANIFEST-000001")).unwrap());
        assert!(env.exists(Path::new("otherdb/MANIFEST-000002")).unwrap());
        assert!(env.exists(Path::new("otherdb/000003.log")).unwrap());
    }

    {
        let mut opt = opt.clone();
        opt.reuse_manifest = true;
        let mut db = DB::open("db", opt.clone()).unwrap();

        println!(
            "children after: {:?}",
            env.children(Path::new("db/")).unwrap()
        );
        assert!(env.exists(Path::new("db/CURRENT")).unwrap());
        // Database is initialized and initial manifest reused.
        assert!(env.exists(Path::new("db/MANIFEST-000001")).unwrap());
        assert!(env.exists(Path::new("db/LOCK")).unwrap());
        assert!(env.exists(Path::new("db/000003.log")).unwrap());

        db.put("abc".as_bytes(), "def".as_bytes()).unwrap();
        db.put("abd".as_bytes(), "def".as_bytes()).unwrap();
    }

    {
        println!(
            "children before: {:?}",
            env.children(Path::new("db/")).unwrap()
        );
        let mut opt = opt.clone();
        opt.reuse_manifest = false;
        opt.reuse_logs = false;
        let mut db = DB::open("db", opt.clone()).unwrap();

        println!(
            "children after: {:?}",
            env.children(Path::new("db/")).unwrap()
        );
        // Obsolete manifest is deleted.
        assert!(!env.exists(Path::new("db/MANIFEST-000001")).unwrap());
        // New manifest is created.
        assert!(env.exists(Path::new("db/MANIFEST-000002")).unwrap());
        // Obsolete log file is deleted.
        assert!(!env.exists(Path::new("db/000003.log")).unwrap());
        // New L0 table has been added.
        assert!(env.exists(Path::new("db/000003.ldb")).unwrap());
        assert!(env.exists(Path::new("db/000004.log")).unwrap());
        // Check that entry exists and is correct. Phew, long call chain!
        let current = db.current();
        log!(opt.log, "files: {:?}", current.borrow().files);
        assert_eq!(
            "def".as_bytes(),
            current
                .borrow_mut()
                .get(LookupKey::new("abc".as_bytes(), 1).internal_key())
                .unwrap()
                .unwrap()
                .0
                .as_slice()
        );
        db.put("abe".as_bytes(), "def".as_bytes()).unwrap();
    }

    {
        println!(
            "children before: {:?}",
            env.children(Path::new("db/")).unwrap()
        );
        // reuse_manifest above causes the old manifest to be deleted as obsolete, but no new
        // manifest is written. CURRENT becomes stale.
        let mut opt = opt.clone();
        opt.reuse_logs = true;
        let db = DB::open("db", opt).unwrap();

        println!(
            "children after: {:?}",
            env.children(Path::new("db/")).unwrap()
        );
        assert!(!env.exists(Path::new("db/MANIFEST-000001")).unwrap());
        assert!(env.exists(Path::new("db/MANIFEST-000002")).unwrap());
        assert!(!env.exists(Path::new("db/MANIFEST-000005")).unwrap());
        assert!(env.exists(Path::new("db/000004.log")).unwrap());
        // 000004 should be reused, no new log file should be created.
        assert!(!env.exists(Path::new("db/000006.log")).unwrap());
        // Log is reused, so memtable should contain last written entry from above.
        assert_eq!(1, db.mem.len());
        assert_eq!(
            "def".as_bytes(),
            db.mem
                .get(&LookupKey::new("abe".as_bytes(), 3))
                .0
                .unwrap()
                .as_slice()
        );
    }
}

pub fn test_db_impl_compact_range() {
    let (mut db, opt) = build_db();
    let env = &opt.env;

    println!(
        "children before: {:?}",
        env.children(Path::new("db/")).unwrap()
    );
    db.compact_range(b"aaa", b"dba").unwrap();
    println!(
        "children after: {:?}",
        env.children(Path::new("db/")).unwrap()
    );

    assert_eq!(250, opt.env.size_of(Path::new("db/000007.ldb")).unwrap());
    assert_eq!(200, opt.env.size_of(Path::new("db/000008.ldb")).unwrap());
    assert_eq!(200, opt.env.size_of(Path::new("db/000009.ldb")).unwrap());
    assert_eq!(435, opt.env.size_of(Path::new("db/000015.ldb")).unwrap());

    assert!(!opt.env.exists(Path::new("db/000001.ldb")).unwrap());
    assert!(!opt.env.exists(Path::new("db/000002.ldb")).unwrap());
    assert!(!opt.env.exists(Path::new("db/000004.ldb")).unwrap());
    assert!(!opt.env.exists(Path::new("db/000005.ldb")).unwrap());
    assert!(!opt.env.exists(Path::new("db/000006.ldb")).unwrap());
    assert!(!opt.env.exists(Path::new("db/000013.ldb")).unwrap());
    assert!(!opt.env.exists(Path::new("db/000014.ldb")).unwrap());

    assert_eq!(b"val1".to_vec(), db.get(b"aaa").unwrap());
    assert_eq!(b"val2".to_vec(), db.get(b"cab").unwrap());
    assert_eq!(b"val3".to_vec(), db.get(b"aba").unwrap());
    assert_eq!(b"val3".to_vec(), db.get(b"fab").unwrap());
}

pub fn test_db_impl_compact_range_memtable() {
    let (mut db, opt) = build_db();
    let env = &opt.env;

    db.put(b"xxx", b"123").unwrap();

    println!(
        "children before: {:?}",
        env.children(Path::new("db/")).unwrap()
    );
    db.compact_range(b"aaa", b"dba").unwrap();
    println!(
        "children after: {:?}",
        env.children(Path::new("db/")).unwrap()
    );

    assert_eq!(250, opt.env.size_of(Path::new("db/000007.ldb")).unwrap());
    assert_eq!(200, opt.env.size_of(Path::new("db/000008.ldb")).unwrap());
    assert_eq!(200, opt.env.size_of(Path::new("db/000009.ldb")).unwrap());
    assert_eq!(182, opt.env.size_of(Path::new("db/000014.ldb")).unwrap());
    assert_eq!(435, opt.env.size_of(Path::new("db/000017.ldb")).unwrap());

    assert!(!opt.env.exists(Path::new("db/000001.ldb")).unwrap());
    assert!(!opt.env.exists(Path::new("db/000002.ldb")).unwrap());
    assert!(!opt.env.exists(Path::new("db/000003.ldb")).unwrap());
    assert!(!opt.env.exists(Path::new("db/000004.ldb")).unwrap());
    assert!(!opt.env.exists(Path::new("db/000005.ldb")).unwrap());
    assert!(!opt.env.exists(Path::new("db/000006.ldb")).unwrap());
    assert!(!opt.env.exists(Path::new("db/000015.ldb")).unwrap());
    assert!(!opt.env.exists(Path::new("db/000016.ldb")).unwrap());

    assert_eq!(b"val1".to_vec(), db.get(b"aaa").unwrap());
    assert_eq!(b"val2".to_vec(), db.get(b"cab").unwrap());
    assert_eq!(b"val3".to_vec(), db.get(b"aba").unwrap());
    assert_eq!(b"val3".to_vec(), db.get(b"fab").unwrap());
    assert_eq!(b"123".to_vec(), db.get(b"xxx").unwrap());
}

#[allow(unused_variables)]
pub fn test_db_impl_locking() {
    let opt = options::for_test();
    let db = DB::open("db", opt.clone()).unwrap();
    let want_err = Status::new(
        StatusCode::LockError,
        "database lock is held by another instance",
    );
    assert_eq!(want_err, DB::open("db", opt.clone()).err().unwrap());
}

pub fn test_db_impl_build_table() {
    let mut opt = options::for_test();
    opt.block_size = 128;
    let mt = build_memtable();

    let f = build_table("db", &opt, mt.iter(), 123).unwrap();
    let path = Path::new("db/000123.ldb");

    assert_eq!(
        LookupKey::new("aabc".as_bytes(), 6).internal_key(),
        f.smallest.as_slice()
    );
    assert_eq!(
        LookupKey::new("test123".as_bytes(), 7).internal_key(),
        f.largest.as_slice()
    );
    assert_eq!(379, f.size);
    assert_eq!(123, f.num);
    assert!(opt.env.exists(path).unwrap());

    {
        // Read table back in.
        let mut tc = TableCache::new("db", opt.clone(), 100);
        let tbl = tc.get_table(123).unwrap();
        assert_eq!(mt.len(), LdbIteratorIter::wrap(&mut tbl.iter()).count());
    }

    {
        // Corrupt table; make sure it doesn't load fully.
        let mut buf = vec![];
        opt.env
            .open_sequential_file(path)
            .unwrap()
            .read_to_end(&mut buf)
            .unwrap();
        buf[150] += 1;
        opt.env
            .open_writable_file(path)
            .unwrap()
            .write_all(&buf)
            .unwrap();

        let mut tc = TableCache::new("db", opt.clone(), 100);
        let tbl = tc.get_table(123).unwrap();
        // The last two entries are skipped due to the corruption above.
        assert_eq!(
            5,
            LdbIteratorIter::wrap(&mut tbl.iter())
                .map(|v| println!("{:?}", v))
                .count()
        );
    }
}

#[allow(unused_variables)]
pub fn test_db_impl_build_db_sanity() {
    let db = build_db().0;
    let env = &db.opt.env;
    let name = &db.name;

    assert!(env.exists(Path::new(&log_file_name(name, 12))).unwrap());
}

pub fn test_db_impl_get_from_table_with_snapshot() {
    let mut db = build_db().0;

    assert_eq!(30, db.vset.borrow().last_seq);

    // seq = 31
    db.put("xyy".as_bytes(), "123".as_bytes()).unwrap();
    let old_ss = db.get_snapshot();
    // seq = 32
    db.put("xyz".as_bytes(), "123".as_bytes()).unwrap();
    db.flush().unwrap();
    assert!(db.get_at(&old_ss, "xyy".as_bytes()).unwrap().is_some());
    assert!(db.get_at(&old_ss, "xyz".as_bytes()).unwrap().is_none());

    // memtable get
    assert_eq!(
        "123".as_bytes(),
        db.get("xyz".as_bytes()).unwrap().as_slice()
    );
    assert!(db.get_internal(31, "xyy".as_bytes()).unwrap().is_some());
    assert!(db.get_internal(32, "xyy".as_bytes()).unwrap().is_some());

    assert!(db.get_internal(31, "xyz".as_bytes()).unwrap().is_none());
    assert!(db.get_internal(32, "xyz".as_bytes()).unwrap().is_some());

    // table get
    assert_eq!(
        "val2".as_bytes(),
        db.get("eab".as_bytes()).unwrap().as_slice()
    );
    assert!(db.get_internal(3, "eab".as_bytes()).unwrap().is_none());
    assert!(db.get_internal(32, "eab".as_bytes()).unwrap().is_some());

    {
        let ss = db.get_snapshot();
        assert_eq!(
            "val2".as_bytes(),
            db.get_at(&ss, "eab".as_bytes())
                .unwrap()
                .unwrap()
                .as_slice()
        );
    }

    // from table.
    assert_eq!(
        "val2".as_bytes(),
        db.get("cab".as_bytes()).unwrap().as_slice()
    );
}

pub fn test_db_impl_delete() {
    let mut db = build_db().0;

    db.put(b"xyy", b"123").unwrap();
    db.put(b"xyz", b"123").unwrap();

    assert!(db.get(b"xyy").is_some());
    assert!(db.get(b"gaa").is_some());

    // Delete one memtable entry and one table entry.
    db.delete(b"xyy").unwrap();
    db.delete(b"gaa").unwrap();

    assert!(db.get(b"xyy").is_none());
    assert!(db.get(b"gaa").is_none());
    assert!(db.get(b"xyz").is_some());
}

pub fn test_db_impl_compact_single_file() {
    let mut db = build_db().0;
    set_file_to_compact(&mut db, 4);
    db.maybe_do_compaction().unwrap();

    let env = &db.opt.env;
    let name = &db.name;
    assert!(!env.exists(Path::new(&table_file_name(name, 3))).unwrap());
    assert!(!env.exists(Path::new(&table_file_name(name, 4))).unwrap());
    assert!(!env.exists(Path::new(&table_file_name(name, 5))).unwrap());
    assert!(env.exists(Path::new(&table_file_name(name, 13))).unwrap());
}

pub fn test_db_impl_compaction_trivial_move() {
    let mut db = DB::open("db", options::for_test()).unwrap();

    db.put("abc".as_bytes(), "xyz".as_bytes()).unwrap();
    db.put("ab3".as_bytes(), "xyz".as_bytes()).unwrap();
    db.put("ab0".as_bytes(), "xyz".as_bytes()).unwrap();
    db.put("abz".as_bytes(), "xyz".as_bytes()).unwrap();
    assert_eq!(4, db.mem.len());
    let mut imm = MemTable::new(db.opt.cmp.clone());
    mem::swap(&mut imm, &mut db.mem);
    db.imm = Some(imm);
    db.compact_memtable().unwrap();

    println!(
        "children after: {:?}",
        db.opt.env.children(Path::new("db/")).unwrap()
    );
    assert!(db.opt.env.exists(Path::new("db/000004.ldb")).unwrap());

    {
        let v = db.current();
        let mut v = v.borrow_mut();
        v.file_to_compact = Some(v.files[2][0].clone());
        v.file_to_compact_lvl = 2;
    }

    db.maybe_do_compaction().unwrap();

    {
        let v = db.current();
        let v = v.borrow_mut();
        assert_eq!(1, v.files[3].len());
    }
}

pub fn test_db_impl_memtable_compaction() {
    let mut opt = options::for_test();
    opt.write_buffer_size = 25;
    let mut db = DB::new("db", opt);

    // Fill up memtable.
    db.mem = build_memtable();

    // Trigger memtable compaction.
    db.make_room_for_write(true).unwrap();
    assert_eq!(0, db.mem.len());
    assert!(db.opt.env.exists(Path::new("db/000002.log")).unwrap());
    assert!(db.opt.env.exists(Path::new("db/000003.ldb")).unwrap());
    assert_eq!(351, db.opt.env.size_of(Path::new("db/000003.ldb")).unwrap());
    assert_eq!(
        7,
        LdbIteratorIter::wrap(&mut db.cache.borrow_mut().get_table(3).unwrap().iter()).count()
    );
}

pub fn test_db_impl_compaction() {
    let mut db = build_db().0;
    let v = db.current();
    v.borrow_mut().compaction_score = Some(2.0);
    v.borrow_mut().compaction_level = Some(1);

    db.maybe_do_compaction().unwrap();

    assert!(!db.opt.env.exists(Path::new("db/000003.ldb")).unwrap());
    assert!(db.opt.env.exists(Path::new("db/000013.ldb")).unwrap());
    assert_eq!(345, db.opt.env.size_of(Path::new("db/000013.ldb")).unwrap());

    // New current version.
    let v = db.current();
    assert_eq!(0, v.borrow().files[1].len());
    assert_eq!(2, v.borrow().files[2].len());
}

pub fn test_db_impl_compaction_trivial() {
    let (mut v, opt) = make_version();

    let to_compact = v.files[2][0].clone();
    v.file_to_compact = Some(to_compact);
    v.file_to_compact_lvl = 2;

    let mut db = DB::new("db", opt.clone());
    db.vset.borrow_mut().add_version(v);
    db.vset.borrow_mut().next_file_num = 10;

    db.maybe_do_compaction().unwrap();

    assert!(opt.env.exists(Path::new("db/000006.ldb")).unwrap());
    assert!(!opt.env.exists(Path::new("db/000010.ldb")).unwrap());
    assert_eq!(218, opt.env.size_of(Path::new("db/000006.ldb")).unwrap());

    let v = db.current();
    assert_eq!(1, v.borrow().files[2].len());
    assert_eq!(3, v.borrow().files[3].len());
}

pub fn test_db_impl_compaction_state_cleanup() {
    let env: Box<dyn Env> = Box::new(MemEnv::new());
    let name = "db";

    let stuff = "abcdefghijkl".as_bytes();
    env.open_writable_file(Path::new("db/000001.ldb"))
        .unwrap()
        .write_all(stuff)
        .unwrap();
    let mut fmd = FileMetaData::default();
    fmd.num = 1;

    let mut cs = CompactionState::new(Compaction::new(&options::for_test(), 2, None), 12);
    cs.outputs = vec![fmd];
    cs.cleanup(&env, name);

    assert!(!env.exists(Path::new("db/000001.ldb")).unwrap());
}

pub fn test_db_impl_open_close_reopen() {
    let opt;
    {
        let mut db = build_db().0;
        opt = db.opt.clone();
        db.put(b"xx1", b"111").unwrap();
        db.put(b"xx2", b"112").unwrap();
        db.put(b"xx3", b"113").unwrap();
        db.put(b"xx4", b"114").unwrap();
        db.put(b"xx5", b"115").unwrap();
        db.delete(b"xx2").unwrap();
    }

    {
        let mut db = DB::open("db", opt.clone()).unwrap();
        db.delete(b"xx5").unwrap();
    }

    {
        let mut db = DB::open("db", opt.clone()).unwrap();

        assert_eq!(None, db.get(b"xx5"));

        let ss = db.get_snapshot();
        db.put(b"xx4", b"222").unwrap();
        let ss2 = db.get_snapshot();

        assert_eq!(Some(b"113".to_vec()), db.get_at(&ss, b"xx3").unwrap());
        assert_eq!(None, db.get_at(&ss, b"xx2").unwrap());
        assert_eq!(None, db.get_at(&ss, b"xx5").unwrap());

        assert_eq!(Some(b"114".to_vec()), db.get_at(&ss, b"xx4").unwrap());
        assert_eq!(Some(b"222".to_vec()), db.get_at(&ss2, b"xx4").unwrap());
    }

    {
        let mut db = DB::open("db", opt).unwrap();

        let ss = db.get_snapshot();
        assert_eq!(Some(b"113".to_vec()), db.get_at(&ss, b"xx3").unwrap());
        assert_eq!(Some(b"222".to_vec()), db.get_at(&ss, b"xx4").unwrap());
        assert_eq!(None, db.get_at(&ss, b"xx2").unwrap());
    }
}


pub fn db_iter_basic_test() {
    let mut db = build_db().0;
    let mut iter = db.new_iter().unwrap();

    // keys and values come from make_version(); they are each the latest entry.
    let keys: &[&[u8]] = &[
        b"aaa", b"aab", b"aax", b"aba", b"bab", b"bba", b"cab", b"cba",
    ];
    let vals: &[&[u8]] = &[
        b"val1", b"val2", b"val2", b"val3", b"val4", b"val5", b"val2", b"val3",
    ];

    for (k, v) in keys.iter().zip(vals.iter()) {
        assert!(iter.advance());
        assert_eq!((k.to_vec(), v.to_vec()), current_key_val(&iter).unwrap());
    }
}

pub fn db_iter_reset() {
    let mut db = build_db().0;
    let mut iter = db.new_iter().unwrap();

    assert!(iter.advance());
    assert!(iter.valid());
    iter.reset();
    assert!(!iter.valid());
    assert!(iter.advance());
    assert!(iter.valid());
}

pub fn db_iter_test_fwd_backwd() {
    let mut db = build_db().0;
    let mut iter = db.new_iter().unwrap();

    // keys and values come from make_version(); they are each the latest entry.
    let keys: &[&[u8]] = &[
        b"aaa", b"aab", b"aax", b"aba", b"bab", b"bba", b"cab", b"cba",
    ];
    let vals: &[&[u8]] = &[
        b"val1", b"val2", b"val2", b"val3", b"val4", b"val5", b"val2", b"val3",
    ];

    // This specifies the direction that the iterator should move to. Based on this, an index
    // into keys/vals is incremented/decremented so that we get a nice test checking iterator
    // move correctness.
    let dirs: &[Direction] = &[
        Direction::Forward,
        Direction::Forward,
        Direction::Forward,
        Direction::Reverse,
        Direction::Reverse,
        Direction::Reverse,
        Direction::Forward,
        Direction::Forward,
        Direction::Reverse,
        Direction::Forward,
        Direction::Forward,
        Direction::Forward,
        Direction::Forward,
    ];
    let mut i = 0;
    iter.advance();
    for d in dirs {
        assert_eq!(
            (keys[i].to_vec(), vals[i].to_vec()),
            current_key_val(&iter).unwrap()
        );
        match *d {
            Direction::Forward => {
                assert!(iter.advance());
                i += 1;
            }
            Direction::Reverse => {
                assert!(iter.prev());
                i -= 1;
            }
        }
    }
}

pub fn db_iter_test_seek() {
    let mut db = build_db().0;
    let mut iter = db.new_iter().unwrap();

    // gca is the deleted entry.
    let keys: &[&[u8]] = &[b"aab", b"aaa", b"cab", b"eaa", b"aaa", b"iba", b"fba"];
    let vals: &[&[u8]] = &[
        b"val2", b"val1", b"val2", b"val1", b"val1", b"val2", b"val3",
    ];

    for (k, v) in keys.iter().zip(vals.iter()) {
        println!("{:?}", String::from_utf8(k.to_vec()).unwrap());
        iter.seek(k);
        assert_eq!((k.to_vec(), v.to_vec()), current_key_val(&iter).unwrap());
    }

    // seek past last.
    iter.seek(b"xxx");
    assert!(!iter.valid());
    iter.seek(b"aab");
    assert!(iter.valid());

    // Seek skips over deleted entry.
    iter.seek(b"gca");
    assert!(iter.valid());
    assert_eq!(
        (b"gda".to_vec(), b"val5".to_vec()),
        current_key_val(&iter).unwrap()
    );
}

pub fn db_iter_deleted_entry_not_returned() {
    let mut db = build_db().0;
    let mut iter = db.new_iter().unwrap();
    let must_not_appear = b"gca";

    for (k, _) in LdbIteratorIter::wrap(&mut iter) {
        assert!(k.as_slice() != must_not_appear);
    }
}

pub fn db_iter_deleted_entry_not_returned_memtable() {
    let mut db = build_db().0;

    db.put(b"xyz", b"123").unwrap();
    db.delete(b"xyz").unwrap();

    let mut iter = db.new_iter().unwrap();
    let must_not_appear = b"xyz";

    for (k, _) in LdbIteratorIter::wrap(&mut iter) {
        assert!(k.as_slice() != must_not_appear);
    }
}

pub fn db_iter_repeated_open_close() {
    let opt;
    {
        let (mut db, opt_) = build_db();
        opt = opt_;

        db.put(b"xx1", b"111").unwrap();
        db.put(b"xx2", b"112").unwrap();
        db.put(b"xx3", b"113").unwrap();
        db.put(b"xx4", b"114").unwrap();
        db.delete(b"xx2").unwrap();
    }

    {
        let mut db = DB::open("db", opt.clone()).unwrap();
        db.put(b"xx4", b"222").unwrap();
    }

    {
        let mut db = DB::open("db", opt).unwrap();

        let ss = db.get_snapshot();
        // xx5 should not be visible.
        db.put(b"xx5", b"223").unwrap();

        let expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::from_iter(
            vec![
                (b"xx1".to_vec(), b"111".to_vec()),
                (b"xx4".to_vec(), b"222".to_vec()),
                (b"aaa".to_vec(), b"val1".to_vec()),
                (b"cab".to_vec(), b"val2".to_vec()),
            ]
            .into_iter(),
        );
        let non_existing: HashSet<Vec<u8>> = HashSet::from_iter(
            vec![b"gca".to_vec(), b"xx2".to_vec(), b"xx5".to_vec()].into_iter(),
        );

        let mut iter = db.new_iter_at(ss.clone()).unwrap();
        for (k, v) in LdbIteratorIter::wrap(&mut iter) {
            if let Some(ev) = expected.get(&k) {
                assert_eq!(ev, &v);
            }
            assert!(!non_existing.contains(&k));
        }
    }
}
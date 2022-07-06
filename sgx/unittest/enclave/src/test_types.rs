#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

use rusty_leveldb::types::*;

pub fn test_types_parse_file_name() {
    for c in &[
        ("CURRENT", (0, FileType::Current)),
        ("LOCK", (0, FileType::DBLock)),
        ("LOG", (0, FileType::InfoLog)),
        ("LOG.old", (0, FileType::InfoLog)),
        ("MANIFEST-01234", (1234, FileType::Descriptor)),
        ("001122.sst", (1122, FileType::Table)),
        ("001122.ldb", (1122, FileType::Table)),
        ("001122.dbtmp", (1122, FileType::Temp)),
    ] {
        assert_eq!(parse_file_name(c.0).unwrap(), c.1);
    }
    assert!(parse_file_name("xyz.LOCK").is_err());
    assert!(parse_file_name("01a.sst").is_err());
    assert!(parse_file_name("0011.abc").is_err());
    assert!(parse_file_name("MANIFEST-trolol").is_err());
}
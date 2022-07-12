//! rusty-leveldb is a reimplementation of LevelDB in pure rust. It depends only on a few crates,
//! and is very close to the original, implementation-wise. The external API is relatively small
//! and should be easy to use.
//!
//! ```
//! use rusty_leveldb::{DB, DBIterator, LdbIterator, Options};
//!
//! let opt = rusty_leveldb::in_memory();
//! let mut db = DB::open("mydatabase", opt).unwrap();
//!
//! db.put(b"Hello", b"World").unwrap();
//! assert_eq!(b"World", db.get(b"Hello").unwrap().as_slice());
//!
//! let mut iter = db.new_iter().unwrap();
//! // Note: For efficiency reasons, it's recommended to use advance() and current() instead of
//! // next() when iterating over many elements.
//! assert_eq!((b"Hello".to_vec(), b"World".to_vec()), iter.next().unwrap());
//!
//! db.delete(b"Hello").unwrap();
//! db.flush().unwrap();
//! ```
//!
#![cfg_attr(all(feature = "mesalock_sgx", not(target_env = "sgx")), no_std)]
#![cfg_attr(all(target_env = "sgx", target_vendor = "mesalock"), feature(rustc_private))]

#![allow(dead_code)]

#[cfg(all(feature = "mesalock_sgx", not(target_env = "sgx")))]
#[macro_use]
extern crate sgx_tstd as std;

#[macro_use]
extern crate cfg_if;
cfg_if! {
    if #[cfg(feature = "mesalock_sgx")]  {
        extern crate sgx_libc as libc;
        extern crate sgx_trts;
        extern crate sgx_types;
        extern crate protected_fs;
        extern crate sgx_tunittest;
    } else {
        extern crate libc;
    }
}

extern crate crc;
extern crate integer_encoding;
extern crate rand;
extern crate snap;

// #[cfg(test)]
// #[macro_use]
// extern crate time_test;

pub mod block;
pub mod block_builder;
pub mod blockhandle;
pub mod cache;
pub mod cmp;
pub mod disk_env;
pub mod env;
mod env_common;
pub mod error;
pub mod filter;
pub mod filter_block;
#[macro_use]
pub mod infolog;
pub mod key_types;
pub mod log;
pub mod mem_env;
pub mod memtable;
pub mod merging_iter;
pub mod options;
pub mod skipmap;
pub mod snapshot;
pub mod table_block;
pub mod table_builder;
pub mod table_cache;
pub mod table_reader;
pub mod test_util;
pub mod types;
pub mod version;
pub mod version_edit;
pub mod version_set;
pub mod write_batch;

pub mod db_impl;
pub mod db_iter;

pub use cmp::{Cmp, DefaultCmp};
pub use db_impl::DB;
pub use db_iter::DBIterator;
pub use disk_env::PosixDiskEnv;
pub use env::Env;
pub use error::{Result, Status, StatusCode};
pub use filter::{BloomPolicy, FilterPolicy};
pub use mem_env::MemEnv;
pub use options::{in_memory, CompressionType, Options};
pub use skipmap::SkipMap;
pub use types::LdbIterator;
pub use write_batch::WriteBatch;

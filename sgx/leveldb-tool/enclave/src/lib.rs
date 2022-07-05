// Copyright (C) 2017-2018 Baidu, Inc. All Rights Reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
//  * Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
//  * Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in
//    the documentation and/or other materials provided with the
//    distribution.
//  * Neither the name of Baidu, Inc., nor the names of its
//    contributors may be used to endorse or promote products derived
//    from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#![crate_name = "leveldbtool"]
#![crate_type = "staticlib"]
#![cfg_attr(not(target_env = "sgx"), no_std)]
#![cfg_attr(target_env = "sgx", feature(rustc_private))]

extern crate sgx_types;
#[cfg(not(target_env = "sgx"))]
#[macro_use]
extern crate sgx_tstd as std;
extern crate sgx_tunittest;

extern crate rand;
extern crate rusty_leveldb;

// use sgx_tunittest::*;
use sgx_types::*;
// use std::io::{self, Write};
use std::slice;
// use std::string::String;
// use std::vec::Vec;
use basic_tool::*;
use rusty_leveldb::{Options, DB};

mod basic_tool;

#[repr(u8)]
pub enum Operation {
    Get = 0,
    Put = 1,
    Delete = 2,
    Iter = 3,
    Compact = 4,
}

impl From<u8> for Operation {
    fn from(n: u8) -> Self {
        match n {
            0 => Self::Get,
            1 => Self::Put,
            2 => Self::Delete,
            3 => Self::Iter,
            4 => Self::Compact,
            _ => panic!("invalid operation: {}", n),
        }
    }
}

#[no_mangle]
pub extern "C" fn basic_operation(
    op: u8,
    key: *const u8, key_len: usize,
    val: *const u8, val_len: usize
) -> sgx_status_t {
    let key_slice = unsafe { slice::from_raw_parts(key, key_len) };
    let key = std::str::from_utf8(key_slice).unwrap();

    let val_slice = unsafe { slice::from_raw_parts(val, val_len) };
    let val = std::str::from_utf8(val_slice).unwrap();

    let op = Operation::from(op);

    let dbkey = [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 
               0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 0x09, 0x08,];
    let mut opt = Options::new_disk_db_with(dbkey);
    opt.reuse_logs = false;
    opt.reuse_manifest = false;
    let mut db = DB::open("tooldb", opt).unwrap();

    // rsgx_unit_tests!(
    //     write_a_lot::test_write_a_lot,
    //     //|| should_panic!(),
    // );

    #[allow(unreachable_patterns)]
    match op {
        Operation::Get => get(&mut db, key),
        Operation::Put => put(&mut db, key, val),
        Operation::Delete => delete(&mut db, key),
        Operation::Iter => iter(&mut db),
        Operation::Compact => compact(&mut db, key, val),
        _ => unimplemented!()
    }

    sgx_status_t::SGX_SUCCESS
}

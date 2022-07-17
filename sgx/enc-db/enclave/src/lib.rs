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

#![crate_name = "encdb"]
#![crate_type = "staticlib"]
#![cfg_attr(not(target_env = "sgx"), no_std)]
#![cfg_attr(target_env = "sgx", feature(rustc_private))]

extern crate sgx_types;
extern crate sgx_tcrypto;
extern crate sgx_tseal;
#[cfg(not(target_env = "sgx"))]
#[macro_use]
extern crate sgx_tstd as std;

extern crate rand;
extern crate rusty_leveldb;
extern crate protected_fs;

use sgx_tcrypto::SgxEccHandle;
use sgx_types::*;

use std::slice;
use std::path::{Path, PathBuf};
use std::vec::Vec;
use std::string::String;
use std::untrusted::path::PathEx;
use rusty_leveldb::{Options, DB};

use user_enc::*;
use utils::*;

mod user_enc;
mod utils;

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
pub extern "C" fn enc_operation(
    op: u8,
    key: *const u8, key_len: usize,
    val: *const u8, val_len: usize
) -> sgx_status_t {
    // let pubkey_path = "ecc.pub";
    // let privkey_path = "ecc.priv";
    // let (pubkey, privkey) = gen_ecc_key_pair(pubkey_path, privkey_path);
    let aes_key_path = "aes.key";
    let (aes_key, iv) = gen_aes_key(aes_key_path);

    let key_slice = unsafe { slice::from_raw_parts(key, key_len) };
    let _key = std::str::from_utf8(key_slice).unwrap();
    let val_slice = unsafe { slice::from_raw_parts(val, val_len) };
    let _val = std::str::from_utf8(val_slice).unwrap();

    let mut enc_key_vec: Vec<u8> = vec![0; key_len + SGX_AESGCM_MAC_SIZE];
    let mut enc_val_vec: Vec<u8> = vec![0; val_len + SGX_AESGCM_MAC_SIZE];
    let enc_key = &mut enc_key_vec[..];
    let enc_val = &mut enc_val_vec[..];
    if key_len > 0 {
        encrypt(&aes_key, &iv, key_slice, enc_key);
    }
    if val_len > 0 {
        encrypt(&aes_key, &iv, val_slice, enc_val);
    }

    let op = Operation::from(op);

    let dbkey = [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 
               0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 0x09, 0x08,];
    let mut opt = Options::new_disk_db_with(dbkey);
    opt.reuse_logs = false;
    opt.reuse_manifest = false;
    let mut db = DB::open("encdb", opt).unwrap();

    #[allow(unreachable_patterns)]
    match op {
        Operation::Get => {
            if let Some(v) = get(&mut db, enc_key) {
                let input = v.as_slice();
                let mut plaintext_vec: Vec<u8> = vec![0; input.len() - SGX_AESGCM_MAC_SIZE];
                let plaintext = &mut plaintext_vec[..];
                decrypt(&aes_key, &iv, input, plaintext);
                println!("{} => {}", _key, String::from_utf8_lossy(plaintext));
            } else {
                println!("{} => <not found>", _key);
            }
        },
        Operation::Put => put(&mut db, enc_key, enc_val),
        Operation::Delete => delete(&mut db, enc_key),
        Operation::Iter => iter(&mut db),
        Operation::Compact => compact(&mut db, enc_key, enc_val),
        _ => unimplemented!()
    }

    sgx_status_t::SGX_SUCCESS
}

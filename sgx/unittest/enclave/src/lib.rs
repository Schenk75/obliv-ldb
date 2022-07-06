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

#![crate_name = "unittest"]
#![crate_type = "staticlib"]
#![cfg_attr(not(target_env = "sgx"), no_std)]
#![cfg_attr(target_env = "sgx", feature(rustc_private))]

extern crate sgx_types;
#[cfg(not(target_env = "sgx"))]
#[macro_use]
extern crate sgx_tstd as std;
extern crate sgx_tunittest;

extern crate rand;
extern crate integer_encoding;
extern crate rusty_leveldb;

use sgx_tunittest::*;
use sgx_types::*;
use std::io::{self, Write};
use std::slice;
use std::string::String;
use std::vec::Vec;

mod test_env;
mod test_types;
mod test_key_types;
mod test_cmp;
mod test_test_util;
mod test_block;
mod test_cache;


#[no_mangle]
pub extern "C" fn test_something(some_string: *const u8, some_len: usize) -> sgx_status_t {
    let str_slice = unsafe { slice::from_raw_parts(some_string, some_len) };
    let _ = io::stdout().write(str_slice);

    rsgx_unit_tests!(
        // test_env::test_mem_fs_memfile_read,
        // test_env::test_mem_fs_memfile_write,
        // test_env::test_mem_fs_memfile_readat,
        // test_env::test_mem_fs_open_read_write,
        // test_env::test_mem_fs_open_read_write_append_truncate,
        // test_env::test_mem_fs_metadata_operations,
        // test_env::test_mem_fs_children,
        // test_env::test_mem_fs_lock,
        // test_env::test_memenv_all,
        // test_env::test_files,
        // test_env::test_locking,
        // test_env::test_dirs,

        // test_types::test_types_parse_file_name,

        // test_key_types::test_memtable_lookupkey,
        // test_key_types::test_build_memtable_key,

        // test_cmp::test_cmp_defaultcmp_shortest_sep,
        // test_cmp::test_cmp_defaultcmp_short_succ,
        // test_cmp::test_cmp_internalkeycmp_shortest_sep,
        // test_cmp::test_cmp_internalkeycmp,
        // // test_cmp::test_cmp_memtablekeycmp_panics,

        // test_test_util::test_test_util_basic,

        // test_block::test_block_builder_sanity,
        // test_block::test_block_builder_reset,
        // // test_block::test_block_builder_panics,
        // test_block::test_block_iterator_properties,
        // test_block::test_block_empty,
        // test_block::test_block_build_iterate,
        // test_block::test_block_iterate_reverse,
        // test_block::test_block_seek,
        // test_block::test_block_seek_to_last,
        // test_block::test_blockhandle,

        test_cache::test_blockcache_cache_add_rm,
        test_cache::test_blockcache_cache_capacity,
        test_cache::test_blockcache_lru_remove,
        test_cache::test_blockcache_lru_1,
        test_cache::test_blockcache_lru_reinsert,
        test_cache::test_blockcache_lru_reinsert_2,
        test_cache::test_blockcache_lru_edge_cases,
    );
    sgx_status_t::SGX_SUCCESS
}

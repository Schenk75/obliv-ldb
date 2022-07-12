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
extern crate crc;

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
mod test_skipmap;
mod test_memtable;
mod test_filter;
mod test_table;
mod test_merging_iter;
mod test_log;
mod test_version;
mod test_snapshot;
mod test_write_batch;
mod test_db_impl;


#[no_mangle]
pub extern "C" fn test_something(some_string: *const u8, some_len: usize) -> sgx_status_t {
    let str_slice = unsafe { slice::from_raw_parts(some_string, some_len) };
    let _ = io::stdout().write(str_slice);

    rsgx_unit_tests!(
        test_env::test_mem_fs_memfile_read,
        test_env::test_mem_fs_memfile_write,
        test_env::test_mem_fs_memfile_readat,
        test_env::test_mem_fs_open_read_write,
        test_env::test_mem_fs_open_read_write_append_truncate,
        test_env::test_mem_fs_metadata_operations,
        test_env::test_mem_fs_children,
        test_env::test_mem_fs_lock,
        test_env::test_memenv_all,
        test_env::test_files,
        test_env::test_locking,
        test_env::test_dirs, 
    );

    rsgx_unit_tests!(
        test_types::test_types_parse_file_name,
    );

    rsgx_unit_tests!(
        test_key_types::test_memtable_lookupkey,
        test_key_types::test_build_memtable_key,
    );

    rsgx_unit_tests!(
        test_cmp::test_cmp_defaultcmp_shortest_sep,
        test_cmp::test_cmp_defaultcmp_short_succ,
        test_cmp::test_cmp_internalkeycmp_shortest_sep,
        test_cmp::test_cmp_internalkeycmp,
        // test_cmp::test_cmp_memtablekeycmp_panics,
    );

    rsgx_unit_tests!(
        test_test_util::test_test_util_basic,
    );

    rsgx_unit_tests!(
        test_block::test_block_builder_sanity,
        test_block::test_block_builder_reset,
        // test_block::test_block_builder_panics,
        test_block::test_block_iterator_properties,
        test_block::test_block_empty,
        test_block::test_block_build_iterate,
        test_block::test_block_iterate_reverse,
        test_block::test_block_seek,
        test_block::test_block_seek_to_last,
        test_block::test_blockhandle,
    );

    rsgx_unit_tests!(
        test_cache::test_blockcache_cache_add_rm,
        test_cache::test_blockcache_cache_capacity,
        test_cache::test_blockcache_lru_remove,
        test_cache::test_blockcache_lru_1,
        test_cache::test_blockcache_lru_reinsert,
        test_cache::test_blockcache_lru_reinsert_2,
        test_cache::test_blockcache_lru_edge_cases,
    );

    rsgx_unit_tests!(
        test_skipmap::test_insert,
        // test_skipmap::test_no_dupes,
        test_skipmap::test_contains,
        test_skipmap::test_find,
        test_skipmap::test_empty_skipmap_find_memtable_cmp,
        test_skipmap::test_skipmap_iterator_0,
        test_skipmap::test_skipmap_iterator_init,
        test_skipmap::test_skipmap_iterator,
        test_skipmap::test_skipmap_iterator_seek_valid,
        test_skipmap::test_skipmap_behavior,
        test_skipmap::test_skipmap_iterator_prev,
        test_skipmap::test_skipmap_iterator_concurrent_insert,
    );

    rsgx_unit_tests!(
        test_memtable::test_shift_left,
        test_memtable::test_memtable_parse_tag,
        test_memtable::test_memtable_add,
        test_memtable::test_memtable_add_get,
        test_memtable::test_memtable_iterator_init,
        test_memtable::test_memtable_iterator_seek,
        test_memtable::test_memtable_iterator_fwd,
        test_memtable::test_memtable_iterator_reverse,
        test_memtable::test_memtable_parse_key,
        test_memtable::test_memtable_iterator_behavior,
    );

    rsgx_unit_tests!(
        test_filter::test_filter_bloom,
        test_filter::test_filter_internal_keys_identical,
        test_filter::test_filter_bloom_hash,
        test_filter::test_filter_index,
        test_filter::test_filter_block_builder,
        test_filter::test_filter_block_build_read,
    );

    rsgx_unit_tests!(
        test_table::test_footer,
        test_table::test_table_builder,
        // test_table::test_bad_input,
        test_table::test_table_approximate_offset,
        test_table::test_table_block_cache_use,
        test_table::test_table_iterator_fwd_bwd,
        test_table::test_table_iterator_filter,
        test_table::test_table_iterator_state_behavior,
        test_table::test_table_iterator_behavior_standard,
        test_table::test_table_iterator_values,
        test_table::test_table_iterator_seek,
        test_table::test_table_get,
        test_table::test_table_internal_keys,
        test_table::test_table_reader_checksum,
        test_table::test_table_file_name,
        test_table::test_filenum_to_key,
        test_table::test_table_cache,
    );

    rsgx_unit_tests!(
        test_merging_iter::test_merging_one,
        test_merging_iter::test_merging_two,
        test_merging_iter::test_merging_zero,
        test_merging_iter::test_merging_behavior,
        test_merging_iter::test_merging_forward_backward,
        test_merging_iter::test_merging_real,
        test_merging_iter::test_merging_seek_reset,
    );

    rsgx_unit_tests!(
        test_log::test_crc_mask_crc,
        test_log::test_crc_sanity,
        test_log::test_writer,
        test_log::test_writer_append,
        test_log::test_reader,
    );

    rsgx_unit_tests!(
        test_version::test_version_concat_iter,
        test_version::test_version_concat_iter_properties,
        test_version::test_version_max_next_level_overlapping,
        test_version::test_version_all_iters,
        test_version::test_version_summary,
        test_version::test_version_get_simple,
        test_version::test_version_get_overlapping_basic,
        test_version::test_version_overlap_in_level,
        test_version::test_version_pick_memtable_output_level,
        test_version::test_version_overlapping_inputs,
        test_version::test_version_record_read_sample,
        test_version::test_version_key_ordering,
        test_version::test_version_file_overlaps,
        test_version::test_version_edit_encode_decode,
        test_version::test_version_set_merge_iters,
        test_version::test_version_set_total_size,
        test_version::test_version_set_get_range,
        test_version::test_version_set_builder,
        test_version::test_version_set_log_and_apply,
        test_version::test_version_set_utils,
        test_version::test_version_set_pick_compaction,
        test_version::test_version_set_compaction,
    );

    rsgx_unit_tests!(
        test_snapshot::test_snapshot_list,
    );

    rsgx_unit_tests!(
        test_write_batch::test_write_batch,
    );

    rsgx_unit_tests!(
        test_db_impl::test_db_impl_open_info_log,
        test_db_impl::test_db_impl_init,
        test_db_impl::test_db_impl_compact_range,
        test_db_impl::test_db_impl_compact_range_memtable,
        test_db_impl::test_db_impl_locking,
        test_db_impl::test_db_impl_build_table,
        test_db_impl::test_db_impl_build_db_sanity,
        test_db_impl::test_db_impl_get_from_table_with_snapshot,
        test_db_impl::test_db_impl_delete,
        test_db_impl::test_db_impl_compact_single_file,
        test_db_impl::test_db_impl_compaction_trivial_move,
        test_db_impl::test_db_impl_memtable_compaction,
        test_db_impl::test_db_impl_compaction,
        test_db_impl::test_db_impl_compaction_trivial,
        test_db_impl::test_db_impl_compaction_state_cleanup,
        test_db_impl::test_db_impl_open_close_reopen,
        test_db_impl::db_iter_basic_test,
        test_db_impl::db_iter_reset,
        test_db_impl::db_iter_test_fwd_backwd,
        test_db_impl::db_iter_test_seek,
        test_db_impl::db_iter_deleted_entry_not_returned,
        test_db_impl::db_iter_deleted_entry_not_returned_memtable,
        test_db_impl::db_iter_repeated_open_close
    );

    sgx_status_t::SGX_SUCCESS
}

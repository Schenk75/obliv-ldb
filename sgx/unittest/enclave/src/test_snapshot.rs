#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

use rusty_leveldb::snapshot::*;

#[allow(unused_variables)]
pub fn test_snapshot_list() {
    let mut l = SnapshotList::new();

    {
        assert!(l.empty());
        let a = l.new_snapshot(1);

        {
            let b = l.new_snapshot(2);

            {
                let c = l.new_snapshot(3);

                assert!(!l.empty());
                assert_eq!(l.oldest(), 1);
                assert_eq!(l.newest(), 3);
            }

            assert_eq!(l.newest(), 2);
            assert_eq!(l.oldest(), 1);
        }

        assert_eq!(l.oldest(), 1);
    }
    assert_eq!(l.oldest(), 0);
}
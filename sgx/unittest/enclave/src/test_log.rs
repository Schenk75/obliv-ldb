#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

use std::io::Cursor;
use crc::crc32;
use rusty_leveldb::{
    log::*,
    error::{err, StatusCode}
};

pub fn test_crc_mask_crc() {
    let crc = crc32::checksum_castagnoli("abcde".as_bytes());
    assert_eq!(crc, unmask_crc(mask_crc(crc)));
    assert!(crc != mask_crc(crc));
}

pub fn test_crc_sanity() {
    assert_eq!(0x8a9136aa, crc32::checksum_castagnoli(&[0 as u8; 32]));
    assert_eq!(0x62a8ab43, crc32::checksum_castagnoli(&[0xff as u8; 32]));
}

pub fn test_writer() {
    let data = &[
        "hello world. My first log entry.",
        "and my second",
        "and my third",
    ];
    let mut lw = LogWriter::new(Vec::new());
    let total_len = data.iter().fold(0, |l, d| l + d.len());

    for d in data {
        let _ = lw.add_record(d.as_bytes());
    }

    assert_eq!(lw.current_block_offset, total_len + 3 * HEADER_SIZE);
}

pub fn test_writer_append() {
    let data = &[
        "hello world. My first log entry.",
        "and my second",
        "and my third",
    ];

    let mut dst = Vec::new();
    dst.resize(1024, 0 as u8);

    {
        let mut lw = LogWriter::new(Cursor::new(dst.as_mut_slice()));
        for d in data {
            let _ = lw.add_record(d.as_bytes());
        }
    }

    let old = dst.clone();

    // Ensure that new_with_off positions the writer correctly. Some ugly mucking about with
    // cursors and stuff is required.
    {
        let offset = data[0].len() + HEADER_SIZE;
        let mut lw =
            LogWriter::new_with_off(Cursor::new(&mut dst.as_mut_slice()[offset..]), offset);
        for d in &data[1..] {
            let _ = lw.add_record(d.as_bytes());
        }
    }
    assert_eq!(old, dst);
}

pub fn test_reader() {
    let data = vec![
        "abcdefghi".as_bytes().to_vec(),    // fits one block of 17
        "123456789012".as_bytes().to_vec(), // spans two blocks of 17
        "0101010101010101010101".as_bytes().to_vec(),
    ]; // spans three blocks of 17
    let mut lw = LogWriter::new(Vec::new());
    lw.block_size = HEADER_SIZE + 10;

    for e in data.iter() {
        assert!(lw.add_record(e).is_ok());
    }

    assert_eq!(lw.dst.len(), 93);
    // Corrupt first record.
    lw.dst[2] += 1;

    let mut lr = LogReader::new(lw.dst.as_slice(), true);
    lr.blocksize = HEADER_SIZE + 10;
    let mut dst = Vec::with_capacity(128);

    // First record is corrupted.
    assert_eq!(
        err(StatusCode::Corruption, "Invalid Checksum"),
        lr.read(&mut dst)
    );

    let mut i = 1;
    loop {
        let r = lr.read(&mut dst);

        if !r.is_ok() {
            panic!("{}", r.unwrap_err());
        } else if r.unwrap() == 0 {
            break;
        }

        assert_eq!(dst, data[i]);
        i += 1;
    }
    assert_eq!(i, data.len());
}
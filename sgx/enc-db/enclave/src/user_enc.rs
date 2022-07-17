#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

use rusty_leveldb::{LdbIterator, DB};
use std::io::{self, Write};
use sgx_tcrypto::*;
use sgx_types::*;

pub fn get(db: &mut DB, k: &[u8]) -> Option<Vec<u8>> {
    db.get(k)
}

pub fn put(db: &mut DB, k: &[u8], v: &[u8]) {
    db.put(k, v).unwrap();
    db.flush().unwrap();
}

pub fn delete(db: &mut DB, k: &[u8]) {
    db.delete(k).unwrap();
    db.flush().unwrap();
}

pub fn iter(db: &mut DB) {
    let mut it = db.new_iter().unwrap();
    let (mut k, mut v) = (vec![], vec![]);
    let mut out = io::BufWriter::new(io::stdout());
    while it.advance() {
        it.current(&mut k, &mut v);
        out.write_all(&k).unwrap();
        out.write_all(b" => ").unwrap();
        out.write_all(&v).unwrap();
        out.write_all(b"\n").unwrap();
    }
}

pub fn compact(db: &mut DB, from: &[u8], to: &[u8]) {
    db.compact_range(from, to).unwrap();
}

pub fn encrypt(
    key: &sgx_aes_gcm_128bit_key_t, 
    iv: &[u8; SGX_AESGCM_IV_SIZE], 
    plaintext: &[u8], 
    result: &mut [u8]) 
{
    let aad_array: [u8; 0] = [0; 0];
    let text_len = plaintext.len();
    let mut ciphertext_vec: Vec<u8> = vec![0; text_len];
    let mut mac_array: [u8; SGX_AESGCM_MAC_SIZE] = [0; SGX_AESGCM_MAC_SIZE];
    let ciphertext_slice = &mut ciphertext_vec[..];
    let _ = rsgx_rijndael128GCM_encrypt(
        key,
        plaintext,
        iv,
        &aad_array,
        ciphertext_slice,
        &mut mac_array);
    result[..SGX_AESGCM_MAC_SIZE].copy_from_slice(&mac_array);
    result[SGX_AESGCM_MAC_SIZE..].copy_from_slice(&ciphertext_slice);
}

pub fn decrypt(
    key: &sgx_aes_gcm_128bit_key_t, 
    iv: &[u8; SGX_AESGCM_IV_SIZE], 
    input: &[u8], 
    plaintext: &mut [u8])
{
    let aad_array: [u8; 0] = [0; 0];
    let mut mac: [u8; SGX_AESGCM_MAC_SIZE] = [0; SGX_AESGCM_MAC_SIZE];
    mac[..].copy_from_slice(&input[..SGX_AESGCM_MAC_SIZE]);
    let ciphertext = &input[SGX_AESGCM_MAC_SIZE..];
    let _ = rsgx_rijndael128GCM_decrypt(
        key,
        &ciphertext,
        iv,
        &aad_array,
        &mac,
        plaintext);
}
#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

use protected_fs;
use sgx_tcrypto::SgxEccHandle;
use sgx_tseal::SgxSealedData;
use sgx_types::*;
use std::io::{Write, Read};
use std::path::Path;
use std::untrusted::path::PathEx;
use rand::Rng;

pub const LOG_SIZE: size_t = 1024;

#[allow(dead_code)]
pub fn gen_ecc_key_pair(pubkey_path: &str, privkey_path: &str) -> (sgx_ec256_public_t, sgx_ec256_private_t) {
    println!("[+] Generate and Store Ecc Key Pair");
    let pubkey_path = Path::new(pubkey_path);
    let privkey_path = Path::new(privkey_path);
    // If key file exists, just read pubkey and privkey from file
    if pubkey_path.exists() && privkey_path.exists() {
        return (load_pub_key(pubkey_path), load_priv_key(privkey_path))
    }

    // Generate key pair
    let ecc_handle = SgxEccHandle::new();
    ecc_handle.open().unwrap();
    let (prv_k, pub_k) = ecc_handle.create_key_pair().unwrap();
    let _ = ecc_handle.close();

    // Store public key
    {
        let mut pubkey_file = protected_fs::OpenOptions::default()
            .write(true)
            .open(pubkey_path)
            .unwrap();
        let mut pub_encoded: [u8; SGX_ECP256_KEY_SIZE*2] = [0; SGX_ECP256_KEY_SIZE*2];
        pub_encoded[..SGX_ECP256_KEY_SIZE].copy_from_slice(&pub_k.gx);
        pub_encoded[SGX_ECP256_KEY_SIZE..].copy_from_slice(&pub_k.gy);
        // println!("[.] pub_encoded: {:?}", pub_encoded);
        let _ = pubkey_file.write(&pub_encoded);
    }
    
    // We only need to seal private key
    {
        let mut privkey_file = protected_fs::OpenOptions::default()
            .write(true)
            .open(privkey_path)
            .unwrap();
        let priv_encoded = prv_k.r;
        // println!("[.] priv_encoded: {:?}", priv_encoded);
        let aad: [u8; 0] = [0_u8; 0];
        let result = SgxSealedData::<[u8]>::seal_data(&aad, &priv_encoded);
        let sealed_priv = match result {
            Ok(x) => x,
            Err(ret) => {
                panic!("[-] Err seal data: {}", ret.as_str());
            },
        };
        let sealed_priv_log: [u8; LOG_SIZE] = [0; LOG_SIZE];
        let sealed_priv_log_size = sealed_priv_log.len() as u32;
        let opt = unsafe {
            sealed_priv.to_raw_sealed_data_t(sealed_priv_log.as_ptr() as *mut sgx_sealed_data_t, sealed_priv_log_size)
        };
        if opt.is_none() {
            println!("[-] Err to_raw_sealed_data_t")
        }
        // println!("[.] sealed_priv: {:?}", sealed_priv_log);
        let _ = privkey_file.write(&sealed_priv_log);
    }

    (pub_k, prv_k)
}

// Load and unseal private key
#[allow(dead_code)]
fn load_priv_key(file_name: &Path) -> sgx_ec256_private_t {
    println!("[+] Load and Unseal Private Key");
    let mut sealed_log: [u8; LOG_SIZE] = [0; LOG_SIZE];
    let sealed_log_size = sealed_log.len() as u32;
    let mut privkey_file = protected_fs::OpenOptions::default()
        .read(true)
        .open(file_name)
        .unwrap();
    let mut buf = Vec::new(); 
    match privkey_file.read_to_end(&mut buf) {
        Ok(_size) => {
            // println!("read bytes {}", _size);
            ()
        },
        Err(_) => panic!("Error read file")
    }
    let buf = buf.as_slice();
    sealed_log[..].copy_from_slice(buf);

    // Unseal private key
    let opt = unsafe {
        SgxSealedData::<[u8]>::from_raw_sealed_data_t(
            sealed_log.as_ptr() as *mut sgx_sealed_data_t, 
            sealed_log_size)
    };
    let sealed_data = match opt {
        Some(x) => x,
        None => {
            panic!("[-] unwrap sealed data fail");
        },
    };
    let result = sealed_data.unseal_data();
    let unsealed_data = match result {
        Ok(x) => x,
        Err(_ret) => {
            panic!("[-] unseal data fail");
        },
    };
    let priv_key_slice = unsealed_data.get_decrypt_txt();
    // println!("[.] Unsealed Private Key from File: {:?}", priv_key_slice);

    let mut r: [u8; SGX_ECP256_KEY_SIZE] = [0; SGX_ECP256_KEY_SIZE];
    r[..].copy_from_slice(priv_key_slice);
    let ecc_priv_key = sgx_ec256_private_t { r };
    ecc_priv_key
}

#[allow(dead_code)]
fn load_pub_key(file_name: &Path) -> sgx_ec256_public_t {
    println!("[+] Load Public Key");
    
    let mut pubkey_file = protected_fs::OpenOptions::default()
            .read(true)
            .open(file_name)
            .unwrap();
    let mut buf = Vec::new(); 
    match pubkey_file.read_to_end(&mut buf) {
        Ok(_size) => {
            // println!("read bytes {}", _size)
            ()
        },
        Err(_) => panic!("Error read file")
    }
    let buf = buf.as_slice();

    let mut gx: [u8; SGX_ECP256_KEY_SIZE] = [0; SGX_ECP256_KEY_SIZE];
    let mut gy: [u8; SGX_ECP256_KEY_SIZE] = [0; SGX_ECP256_KEY_SIZE];
    gx[..].copy_from_slice(&buf[..SGX_ECP256_KEY_SIZE]);
    gy[..].copy_from_slice(&buf[SGX_ECP256_KEY_SIZE..]);
    // println!("[.] Read Public Key from File: \n\tgx:{:?}\n\tgy:{:?}", gx, gy);
    let ecc_pub_key = sgx_ec256_public_t { gx, gy };
    ecc_pub_key
}

pub fn gen_aes_key(key_path: &str) -> (sgx_aes_gcm_128bit_key_t, [u8; SGX_AESGCM_IV_SIZE]) {
    println!("[+] Generate and Store AES-GCM Key");
    let key_path = Path::new(key_path);
    if key_path.exists() {
        return load_aes_key(key_path);
    }
    let mut rng = rand::thread_rng();
    let key: sgx_aes_gcm_128bit_key_t = rng.gen();
    let iv: [u8; SGX_AESGCM_IV_SIZE] = rng.gen();

    // Save to file
    let mut key_file = protected_fs::OpenOptions::default()
        .write(true)
        .open(key_path)
        .unwrap();
    let mut aes_key_encoded: [u8; SGX_AESGCM_IV_SIZE + SGX_AESGCM_KEY_SIZE] = [0; SGX_AESGCM_IV_SIZE + SGX_AESGCM_KEY_SIZE];
    aes_key_encoded[..SGX_AESGCM_KEY_SIZE].copy_from_slice(&key);
    aes_key_encoded[SGX_AESGCM_KEY_SIZE..].copy_from_slice(&iv);
    // println!("[.] aes_key_encoded: {:?}", aes_key_encoded);

    let aad: [u8; 0] = [0_u8; 0];
    let result = SgxSealedData::<[u8]>::seal_data(&aad, &aes_key_encoded);
    let sealed_key = match result {
        Ok(x) => x,
        Err(ret) => {
            panic!("[-] Err seal data: {}", ret.as_str());
        },
    };
    let sealed_key_log: [u8; LOG_SIZE] = [0; LOG_SIZE];
    let sealed_key_log_size = sealed_key_log.len() as u32;
    let opt = unsafe {
        sealed_key.to_raw_sealed_data_t(sealed_key_log.as_ptr() as *mut sgx_sealed_data_t, sealed_key_log_size)
    };
    if opt.is_none() {
        println!("[-] Err to_raw_sealed_data_t")
    }
    // println!("[.] sealed_key: {:?}", sealed_key_log);
    let _ = key_file.write(&sealed_key_log);

    (key, iv)
}

// Load and unseal aes-gcm key
fn load_aes_key(file_name: &Path) -> (sgx_aes_gcm_128bit_key_t, [u8; SGX_AESGCM_IV_SIZE]) {
    println!("[+] Load and Unseal AES-GCM Key");
    let mut sealed_log: [u8; LOG_SIZE] = [0; LOG_SIZE];
    let sealed_log_size = sealed_log.len() as u32;
    let mut key_file = protected_fs::OpenOptions::default()
        .read(true)
        .open(file_name)
        .unwrap();
    let mut buf = Vec::new(); 
    match key_file.read_to_end(&mut buf) {
        Ok(_size) => {
            // println!("read bytes {}", _size);
            ()
        },
        Err(_) => panic!("Error read file")
    }
    let buf = buf.as_slice();
    sealed_log[..].copy_from_slice(buf);

    // Unseal private key
    let opt = unsafe {
        SgxSealedData::<[u8]>::from_raw_sealed_data_t(
            sealed_log.as_ptr() as *mut sgx_sealed_data_t, 
            sealed_log_size)
    };
    let sealed_data = match opt {
        Some(x) => x,
        None => {
            panic!("[-] unwrap sealed data fail");
        },
    };
    let result = sealed_data.unseal_data();
    let unsealed_data = match result {
        Ok(x) => x,
        Err(_ret) => {
            panic!("[-] unseal data fail");
        },
    };
    let key_slice = unsealed_data.get_decrypt_txt();
    // println!("[.] Unsealed AES-GCM Key from File: {:?}", key_slice);

    let mut key: sgx_aes_gcm_128bit_key_t = [0; SGX_AESGCM_KEY_SIZE];
    let mut iv: [u8; SGX_AESGCM_IV_SIZE] = [0; SGX_AESGCM_IV_SIZE];
    key[..].copy_from_slice(&key_slice[..SGX_AESGCM_KEY_SIZE]);
    iv[..].copy_from_slice(&key_slice[SGX_AESGCM_KEY_SIZE..]);

    (key, iv)
}


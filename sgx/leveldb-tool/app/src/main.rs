extern crate sgx_types;
extern crate sgx_urts;
extern crate dirs;

use sgx_types::*;
use sgx_urts::SgxEnclave;

use std::io::{Read, Write};
use std::fs;
use std::path;
use std::env::args;
use std::iter::FromIterator;

static ENCLAVE_FILE: &'static str = "enclave.signed.so";
static ENCLAVE_TOKEN: &'static str = "enclave.token";

#[repr(u8)]
pub enum Operation {
    Get = 0,
    Put = 1,
    Delete = 2,
    Iter = 3,
    Compact = 4,
}

extern {
    fn basic_operation(
        eid: sgx_enclave_id_t, 
        retval: *mut sgx_status_t,
        operation: u8,
        key: *const u8, 
        key_len: usize,
        val: *const u8,
        val_len: usize,
    ) -> sgx_status_t;
}

fn init_enclave() -> SgxResult<SgxEnclave> {
    let mut launch_token: sgx_launch_token_t = [0; 1024];
    let mut launch_token_updated: i32 = 0;
    // Step 1: try to retrieve the launch token saved by last transaction
    //         if there is no token, then create a new one.
    //
    // try to get the token saved in $HOME */
    let mut home_dir = path::PathBuf::new();
    let use_token = match dirs::home_dir() {
        Some(path) => {
            println!("[+] Home dir is {}", path.display());
            home_dir = path;
            true
        },
        None => {
            println!("[-] Cannot get home dir");
            false
        }
    };

    let token_file: path::PathBuf = home_dir.join(ENCLAVE_TOKEN);;
    if use_token == true {
        match fs::File::open(&token_file) {
            Err(_) => {
                println!("[-] Open token file {} error! Will create one.", token_file.as_path().to_str().unwrap());
            },
            Ok(mut f) => {
                println!("[+] Open token file success! ");
                match f.read(&mut launch_token) {
                    Ok(1024) => {
                        println!("[+] Token file valid!");
                    },
                    _ => println!("[+] Token file invalid, will create new token file"),
                }
            }
        }
    }

    // Step 2: call sgx_create_enclave to initialize an enclave instance
    // Debug Support: set 2nd parameter to 1
    let debug = 1;
    let mut misc_attr = sgx_misc_attribute_t {secs_attr: sgx_attributes_t { flags:0, xfrm:0}, misc_select:0};
    let enclave = SgxEnclave::create(
        ENCLAVE_FILE,
        debug,
        &mut launch_token,
        &mut launch_token_updated,
        &mut misc_attr)?;

    // Step 3: save the launch token if it is updated
    if use_token == true && launch_token_updated != 0 {
        // reopen the file with write capablity
        match fs::File::create(&token_file) {
            Ok(mut f) => {
                match f.write_all(&launch_token) {
                    Ok(()) => println!("[+] Saved updated launch token!"),
                    Err(_) => println!("[-] Failed to save updated launch token!"),
                }
            },
            Err(_) => {
                println!("[-] Failed to save updated enclave token, but doesn't matter");
            },
        }
    }

    Ok(enclave)
}

fn main() {
    let enclave = match init_enclave() {
        Ok(r) => {
            println!("[+] Init Enclave Successful {}!", r.geteid());
            r
        },
        Err(x) => {
            println!("[-] Init Enclave Failed {}!", x.as_str());
            return;
        },
    };

    let mut retval = sgx_status_t::SGX_SUCCESS;

    let args = Vec::from_iter(args());
    if args.len() < 2 {
        panic!(
            "Usage: {} [get|put/set|delete|iter|compact] [key|from] [val|to]",
            args[0]
        );
    }
    
    let op: Operation;
    let mut key = String::new();
    let mut val = String::new();

    match args[1].as_str() {
        "get" => {
            if args.len() < 3 {
                panic!("Usage: {} get key", args[0]);
            }
            key = (&args[2]).to_string();
            op = Operation::Get;
        },
        "put" | "set" => {
            if args.len() < 4 {
                panic!("Usage: {} put key val", args[0]);
            }
            key = (&args[2]).to_string();
            val = (&args[3]).to_string();
            op = Operation::Put;
        },
        "delete" => {
            if args.len() < 3 {
                panic!("Usage: {} delete key", args[0]);
            }
            key = (&args[2]).to_string();
            op = Operation::Delete;
        },
        "iter" => {
            op = Operation::Iter;
        },
        "compact" => {
            if args.len() < 4 {
                panic!("Usage: {} compact from to", args[0]);
            }
            key = (&args[2]).to_string();
            val = (&args[3]).to_string();
            op = Operation::Compact;
        },
        _ => unimplemented!(),
    }


    let result = unsafe {
        basic_operation(
            enclave.geteid(),
            &mut retval,
            op as u8,
            key.as_ptr() as * const u8,
            key.len(),
            val.as_ptr() as * const u8,
            val.len())
    };

    match result {
        sgx_status_t::SGX_SUCCESS => {},
        _ => {
            println!("[-] ECALL Enclave Failed {}!", result.as_str());
            return;
        }
    }

    println!("[+] basic_operation success...");

    enclave.destroy();
}
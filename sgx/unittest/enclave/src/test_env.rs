#[cfg(feature = "mesalock_sgx")]
use std::prelude::v1::*;

use std::path::{Path, PathBuf};
use std::convert::AsRef;
use std::iter::FromIterator;
use std::sync::{Arc, SgxMutex as Mutex};
use std::io::{Read, Write};
use rusty_leveldb::{
    env::{self, *},
    mem_env::*,
    disk_env::*
};

fn new_memfile(v: Vec<u8>) -> MemFile {
    MemFile(Arc::new(Mutex::new(v)))
}

fn s2p(x: &str) -> PathBuf {
    Path::new(x).to_owned()
}

pub fn test_mem_fs_memfile_read() {
    let f = new_memfile(vec![1, 2, 3, 4, 5, 6, 7, 8]);
    let mut buf: [u8; 1] = [0];
    let mut reader = MemFileReader(f, 0);

    for i in [1, 2, 3, 4, 5, 6, 7, 8].iter() {
        assert_eq!(reader.read(&mut buf).unwrap(), 1);
        assert_eq!(buf, [*i]);
    }
}

pub fn test_mem_fs_memfile_write() {
    let f = new_memfile(vec![]);
    let mut w1 = MemFileWriter::new(f.clone(), false);
    assert_eq!(w1.write(&[1, 2, 3]).unwrap(), 3);

    let mut w2 = MemFileWriter::new(f, true);
    assert_eq!(w1.write(&[1, 7, 8, 9]).unwrap(), 4);
    assert_eq!(w2.write(&[4, 5, 6]).unwrap(), 3);

    assert_eq!(
        (w1.0).0.lock().unwrap().as_ref() as &Vec<u8>,
        &[1, 2, 3, 4, 5, 6, 9]
    );
}

pub fn test_mem_fs_memfile_readat() {
    let f = new_memfile(vec![1, 2, 3, 4, 5]);

    let mut buf = [0; 3];
    assert_eq!(f.read_at(2, &mut buf).unwrap(), 3);
    assert_eq!(buf, [3, 4, 5]);

    assert_eq!(f.read_at(0, &mut buf[0..1]).unwrap(), 1);
    assert_eq!(buf, [1, 4, 5]);

    assert_eq!(f.read_at(5, &mut buf).unwrap(), 0);
    assert_eq!(buf, [1, 4, 5]);

    let mut buf2 = [0; 6];
    assert_eq!(f.read_at(0, &mut buf2[0..5]).unwrap(), 5);
    assert_eq!(buf2, [1, 2, 3, 4, 5, 0]);
    assert_eq!(f.read_at(0, &mut buf2[0..6]).unwrap(), 5);
    assert_eq!(buf2, [1, 2, 3, 4, 5, 0]);
}

pub fn test_mem_fs_open_read_write() {
    let fs = MemFS::new();
    let path = Path::new("/a/b/hello.txt");

    {
        let mut w = fs.open_w(&path, false, false).unwrap();
        write!(w, "Hello").unwrap();
        // Append.
        let mut w2 = fs.open_w(&path, true, false).unwrap();
        write!(w2, "World").unwrap();
    }
    {
        let mut r = MemFileReader::new(fs.open(&path, false).unwrap(), 0);
        let mut s = String::new();
        assert_eq!(r.read_to_string(&mut s).unwrap(), 10);
        assert_eq!(s, "HelloWorld");

        let mut r2 = MemFileReader::new(fs.open(&path, false).unwrap(), 2);
        s.clear();
        assert_eq!(r2.read_to_string(&mut s).unwrap(), 8);
        assert_eq!(s, "lloWorld");
    }
    assert_eq!(fs.size_of_(&path).unwrap(), 10);
    assert!(fs.exists_(&path).unwrap());
    assert!(!fs.exists_(&Path::new("/non/existing/path")).unwrap());
}

pub fn test_mem_fs_open_read_write_append_truncate() {
    let fs = MemFS::new();
    let path = Path::new("/a/b/hello.txt");

    {
        let mut w0 = fs.open_w(&path, false, true).unwrap();
        write!(w0, "Garbage").unwrap();

        // Truncate.
        let mut w = fs.open_w(&path, false, true).unwrap();
        write!(w, "Xyz").unwrap();
        // Write to the beginning.
        let mut w2 = fs.open_w(&path, false, false).unwrap();
        write!(w2, "OverwritingEverythingWithGarbage").unwrap();
        // Overwrite the overwritten stuff.
        write!(w, "Xyz").unwrap();
        assert!(w.flush().is_ok());
    }
    {
        let mut r = MemFileReader::new(fs.open(&path, false).unwrap(), 0);
        let mut s = String::new();
        assert_eq!(r.read_to_string(&mut s).unwrap(), 32);
        assert_eq!(s, "OveXyzitingEverythingWithGarbage");
    }
    assert!(fs.exists_(&path).unwrap());
    assert_eq!(fs.size_of_(&path).unwrap(), 32);
    assert!(!fs.exists_(&Path::new("/non/existing/path")).unwrap());
}

pub fn test_mem_fs_metadata_operations() {
    let fs = MemFS::new();
    let path = Path::new("/a/b/hello.file");
    let newpath = Path::new("/a/b/hello2.file");
    let nonexist = Path::new("/blah");

    // Make file/remove file.
    {
        let mut w = fs.open_w(&path, false, false).unwrap();
        write!(w, "Hello").unwrap();
    }
    assert!(fs.exists_(&path).unwrap());
    assert_eq!(fs.size_of_(&path).unwrap(), 5);
    fs.delete_(&path).unwrap();
    assert!(!fs.exists_(&path).unwrap());
    assert!(fs.delete_(&nonexist).is_err());

    // rename_ file.
    {
        let mut w = fs.open_w(&path, false, false).unwrap();
        write!(w, "Hello").unwrap();
    }
    assert!(fs.exists_(&path).unwrap());
    assert!(!fs.exists_(&newpath).unwrap());
    assert_eq!(fs.size_of_(&path).unwrap(), 5);
    assert!(fs.size_of_(&newpath).is_err());

    fs.rename_(&path, &newpath).unwrap();

    assert!(!fs.exists_(&path).unwrap());
    assert!(fs.exists_(&newpath).unwrap());
    assert_eq!(fs.size_of_(&newpath).unwrap(), 5);
    assert!(fs.size_of_(&path).is_err());

    assert!(fs.rename_(&nonexist, &path).is_err());
}

pub fn test_mem_fs_children() {
    let fs = MemFS::new();
    let (path1, path2, path3) = (
        Path::new("/a/1.txt"),
        Path::new("/a/2.txt"),
        Path::new("/b/1.txt"),
    );

    for p in &[&path1, &path2, &path3] {
        fs.open_w(*p, false, false).unwrap();
    }
    let children = fs.children_of(&Path::new("/a")).unwrap();
    assert!(
        (children == vec![s2p("1.txt"), s2p("2.txt")])
            || (children == vec![s2p("2.txt"), s2p("1.txt")])
    );
    let children = fs.children_of(&Path::new("/a/")).unwrap();
    assert!(
        (children == vec![s2p("1.txt"), s2p("2.txt")])
            || (children == vec![s2p("2.txt"), s2p("1.txt")])
    );
}

pub fn test_mem_fs_lock() {
    let fs = MemFS::new();
    let p = Path::new("/a/lock");

    {
        let mut f = fs.open_w(p, true, true).unwrap();
        f.write("abcdef".as_bytes()).expect("write failed");
    }

    // Locking on new file.
    let lock = fs.lock_(p).unwrap();
    assert!(fs.lock_(p).is_err());

    // Unlock of locked file is ok.
    assert!(fs.unlock_(lock).is_ok());

    // Lock of unlocked file is ok.
    let lock = fs.lock_(p).unwrap();
    assert!(fs.lock_(p).is_err());
    assert!(fs.unlock_(lock).is_ok());

    // Rogue operation.
    assert!(fs
        .unlock_(env::FileLock {
            id: "/a/lock".to_string(),
        })
        .is_err());

    // Non-existent files.
    let p2 = Path::new("/a/lock2");
    assert!(fs.lock_(p2).is_ok());
    assert!(fs
        .unlock_(env::FileLock {
            id: "/a/lock2".to_string(),
        })
        .is_ok());
}

pub fn test_memenv_all() {
    let me = MemEnv::new();
    let (p1, p2, p3) = (Path::new("/a/b"), Path::new("/a/c"), Path::new("/a/d"));
    let nonexist = Path::new("/x/y");
    me.open_writable_file(p2).unwrap();
    me.open_appendable_file(p3).unwrap();
    me.open_sequential_file(p2).unwrap();
    me.open_random_access_file(p3).unwrap();

    assert!(me.exists(p2).unwrap());
    assert_eq!(me.children(Path::new("/a/")).unwrap().len(), 2);
    assert_eq!(me.size_of(p2).unwrap(), 0);

    me.delete(p2).unwrap();
    assert!(me.mkdir(p3).is_err());
    me.mkdir(p1).unwrap();
    me.rmdir(p3).unwrap();
    assert!(me.rmdir(nonexist).is_err());

    me.open_writable_file(p1).unwrap();
    me.rename(p1, p3).unwrap();
    assert!(!me.exists(p1).unwrap());
    assert!(me.rename(nonexist, p1).is_err());

    me.unlock(me.lock(p3).unwrap()).unwrap();
    assert!(me.lock(nonexist).is_ok());

    me.new_logger(p1).unwrap();
    assert!(me.micros() > 0);
}

pub fn test_files() {
    let n = "testfile.xyz".to_string();
    let name = n.as_ref();
    let key = [0; 16];
    let env = PosixDiskEnv::new_with(key);

    // exists, size_of, delete
    assert!(env.open_appendable_file(name).is_ok());
    assert!(env.exists(name).unwrap_or(false));
    assert_eq!(env.size_of(name).unwrap_or(1), 0);
    assert!(env.delete(name).is_ok());

    assert!(env.open_writable_file(name).is_ok());
    assert!(env.exists(name).unwrap_or(false));
    assert_eq!(env.size_of(name).unwrap_or(1), 0);
    assert!(env.delete(name).is_ok());

    {
        {
            // write
            let mut f = env.open_writable_file(name).unwrap();
            let _ = f.write("123xyz".as_bytes());
        }
        assert_eq!(6, env.size_of(name).unwrap_or(0));

        // rename
        let newname = Path::new("testfile2.xyz");
        assert!(env.rename(name, newname).is_ok());
        assert_eq!(false, env.size_of(newname).is_err());
        assert!(!env.exists(name).unwrap());
        // rename back so that the remaining tests can use the file.
        assert!(env.rename(newname, name).is_ok());
    }

    assert!(env.open_sequential_file(name).is_ok());
    assert!(env.open_random_access_file(name).is_ok());

    assert!(env.delete(name).is_ok());
}

pub fn test_locking() {
    let key = [0; 16];
    let env = PosixDiskEnv::new_with(key);
    let n = "testfile.123".to_string();
    let name = n.as_ref();

    {
        {
            let mut f = env.open_writable_file(name).unwrap();
            let _ = f.write("123xyz".as_bytes());
        }
        assert_eq!(env.size_of(name).unwrap_or(0), 6);
    }

    {
        let r = env.lock(name);
        assert!(r.is_ok());
        env.unlock(r.unwrap()).unwrap();
    }

    {
        let r = env.lock(name);
        assert!(r.is_ok());
        let s = env.lock(name);
        assert!(s.is_err());
        env.unlock(r.unwrap()).unwrap();
    }

    assert!(env.delete(name).is_ok());
}

pub fn test_dirs() {
    let d = "subdir/";
    let dirname = d.as_ref();
    let key = [0; 16];
    let env = PosixDiskEnv::new_with(key);

    assert!(env.mkdir(dirname).is_ok());
    assert!(env
        .open_writable_file(
            String::from_iter(vec![d.to_string(), "f1.txt".to_string()].into_iter()).as_ref()
        )
        .is_ok());
    assert_eq!(env.children(dirname).unwrap().len(), 1);
    assert!(env.rmdir(dirname).is_ok());
}
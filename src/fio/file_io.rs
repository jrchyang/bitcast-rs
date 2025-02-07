use log::error;
use parking_lot::RwLock;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::{fs::File, os::unix::fs::FileExt, sync::Arc};

use crate::error::{Errors, Result};
use crate::fio::IOManager;

/// 标准文件 IO
pub struct FileIO {
    fd: Arc<RwLock<File>>,
}

impl FileIO {
    pub fn new(file_name: PathBuf) -> Result<Self> {
        match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(file_name)
        {
            Ok(file) => {
                return Ok(FileIO {
                    fd: Arc::new(RwLock::new(file)),
                })
            }
            Err(e) => {
                error!("failed to open data file: {}", e);
                Err(Errors::FailedToOpenDataFile)
            }
        }
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let read_guard = self.fd.read();
        match read_guard.read_at(buf, offset) {
            Ok(n) => return Ok(n),
            Err(e) => {
                error!("read from data file error: {}", e);
                return Err(Errors::FailedToReadFromDataFile);
            }
        };
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        let mut write_gurad = self.fd.write();
        match write_gurad.write(buf) {
            Ok(n) => return Ok(n),
            Err(e) => {
                error!("write to data file error {}", e);
                return Err(Errors::FailedToWriteToDataFile);
            }
        };
    }

    fn sync(&self) -> Result<()> {
        let read_guard = self.fd.read();
        if let Err(e) = read_guard.sync_all() {
            error!("failed to sync data file: {}", e);
            return Err(Errors::FailedToSyncDataFile);
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use crate::fio::IOManager;

    use super::FileIO;

    #[test]
    fn test_file_io_write() {
        let path = PathBuf::from("/tmp/a.data");
        let file_res = FileIO::new(path.clone());
        assert!(file_res.is_ok());
        let fio = file_res.unwrap();

        let res1 = fio.write("key-a".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(5, res1.unwrap());

        let res2 = fio.write("key-b".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(5, res2.unwrap());

        let res3 = fs::remove_file(path.clone());
        assert!(res3.is_ok());
    }

    #[test]
    fn test_file_io_read() {
        let path = PathBuf::from("/tmp/b.data");
        let file_res = FileIO::new(path.clone());
        assert!(file_res.is_ok());
        let fio = file_res.unwrap();

        let res1 = fio.write("key-a".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(5, res1.unwrap());

        let res2 = fio.write("key-b".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(5, res2.unwrap());

        let mut buf = [0; 5];
        let res3 = fio.read(&mut buf, 0);
        assert!(res3.is_ok());
        assert_eq!(5, res3.unwrap());
        assert_eq!("key-a".as_bytes(), buf);

        let res3 = fs::remove_file(path.clone());
        assert!(res3.is_ok());
    }

    #[test]
    fn test_file_io_sync() {
        let path = PathBuf::from("/tmp/c.data");
        let file_res = FileIO::new(path.clone());
        assert!(file_res.is_ok());
        let fio = file_res.unwrap();

        let res1 = fio.write("key-a".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(5, res1.unwrap());

        let res2 = fio.write("key-b".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(5, res2.unwrap());

        let res3 = fio.sync();
        assert!(res3.is_ok());

        let res4 = fs::remove_file(path.clone());
        assert!(res4.is_ok());
    }
}

use parking_lot::RwLock;
use std::path::PathBuf;
use std::sync::Arc;

use crate::error::Result;
use crate::fio;

use super::log_record::LogRecord;

/// 数据文件
pub struct DataFile {
    file_id: Arc<RwLock<u32>>,
    write_offset: Arc<RwLock<u64>>,
    io_manager: Box<dyn fio::IOManager>,
}

impl DataFile {
    pub fn new(dir_path: PathBuf, file_id: u32) -> Result<(DataFile)> {
        todo!()
    }

    pub fn get_write_offset(&self) -> u64 {
        let read_guard = self.write_offset.read();
        *read_guard
    }

    pub fn get_file_id(&self) -> u32 {
        let read_guard = self.file_id.read();
        *read_guard
    }

    pub fn read_log_record(&self, offset: u64) -> Result<LogRecord> {
        todo!()
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        todo!()
    }

    pub fn sync(&self) -> Result<()> {
        todo!()
    }
}

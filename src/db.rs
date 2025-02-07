use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{
    data::{
        data_file::DataFile,
        log_record::{LogRecord, LogRecordPos, LogRecordType},
    },
    error::{Errors, Result},
    index,
    options::Options,
};

/// Bitcask 存储引擎实例结构体
pub struct Engine {
    options: Arc<Options>,
    active_file: Arc<RwLock<DataFile>>,
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    index: Box<dyn index::Indexer>,
}

impl Engine {
    /// 存储 key/value 数据，key 不能为空
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        // 判断 key 的有效性
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 构造 LogRecord
        let mut record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            record_type: LogRecordType::NORMAL,
        };

        // 追加写到活跃数据文件中
        let log_record_pos = self.append_log_record(&mut record)?;

        // 更新内存索引
        let ok = self.index.put(key.to_vec(), log_record_pos);
        if !ok {
            return Err(Errors::FailedToUpdataMemIndex);
        }

        Ok(())
    }

    // 根据 key 获取对应的数据
    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        // 判断 key 的有效性
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 从内存索引中获取 key 对应的数据信息
        let pos = self.index.get(key.to_vec());
        // 如果 key 不存在则返回
        if pos.is_none() {
            return Err(Errors::KeyNotFound);
        }

        let log_record_pos = pos.unwrap();
        let active_file = self.active_file.read();
        let older_files = self.older_files.read();

        let log_record = match active_file.get_file_id() == log_record_pos.file_id {
            true => active_file.read_log_record(log_record_pos.offset)?,
            false => {
                let data_file = older_files.get(&log_record_pos.file_id);
                if data_file.is_none() {
                    // 找不到对应的数据文件返回错误
                    return Err(Errors::DataFileNotFound);
                }
                data_file.unwrap().read_log_record(log_record_pos.offset)?
            }
        };

        // 判断 LogRecord 类型
        if log_record.record_type == LogRecordType::DELETED {
            return Err(Errors::KeyNotFound);
        }

        // 数据有效返回 value
        Ok(log_record.value.into())
    }

    // 追加写数据到当前活跃文件中
    fn append_log_record(&self, log_record: &mut LogRecord) -> Result<LogRecordPos> {
        let dir_path = self.options.dir_path.clone();

        // 编码输入数据
        let enc_record = log_record.encode();
        let record_len = enc_record.len() as u64;

        // 获取当前活跃的文件
        let mut active_file = self.active_file.write();

        // 判断当前活跃文件是否达到了阈值
        if active_file.get_write_offset() + record_len > self.options.data_file_size {
            // 将当前活跃文件进行持久化
            active_file.sync()?;

            let current_fid = active_file.get_file_id();
            // 旧的数据文件存储到 map 中
            let mut older_files = self.older_files.write();
            let old_file = DataFile::new(dir_path.clone(), current_fid)?;
            older_files.insert(current_fid, old_file);

            // 打开新的数据文件
            let new_file = DataFile::new(dir_path.clone(), current_fid + 1)?;
            *active_file = new_file;
        }

        // 追加写数据到当前活跃文件中
        let write_offset = active_file.get_write_offset();
        active_file.write(&enc_record)?;

        // 根据配置项决定是否持久化
        if self.options.sync_write {
            active_file.sync()?;
        }

        Ok(LogRecordPos {
            file_id: active_file.get_file_id(),
            offset: write_offset,
        })
    }
}

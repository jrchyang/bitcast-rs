use std::{collections::BTreeMap, sync::Arc};
use parking_lot::RwLock;

use crate::data::log_record::LogRecordPos;

use crate::index::Indexer;

/// BTree 索引，主要封装了标准库中的 BTreeMap 结构
pub struct BTree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl BTree {
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexer for BTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let mut write_guard  = self.tree.write();
        write_guard.insert(key, pos);
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let read_gurad = self.tree.read();
        read_gurad.get(&key).copied()
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let mut write_guard = self.tree.write();
        let res = write_guard.remove(&key);
        res.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_put() {
        let bt = BTree::new();
        let res = bt.put("".as_bytes().to_vec(), LogRecordPos { file_id: 1, offset : 0 });
        assert_eq!(res, true);
    }

    #[test]
    fn test_btree_get() {
        let bt = BTree::new();
        let res1 = bt.put("hello".as_bytes().to_vec(), LogRecordPos { file_id: 1, offset: 10 });
        assert_eq!(res1, true);
        let res2 = bt.put("foo".as_bytes().to_vec(), LogRecordPos { file_id: 2, offset: 20 });
        assert_eq!(res2, true);

        let pos1 = bt.get("hello".as_bytes().to_vec());
        assert_eq!(pos1.is_some(), true);
        assert_eq!(pos1.unwrap().file_id, 1);
        let pos2 = bt.get("world".as_bytes().to_vec());
        assert_eq!(pos2.is_none(), true);
    }

    #[test]
    fn test_btree_del() {
        let bt = BTree::new();
        let res1 = bt.put("hello".as_bytes().to_vec(), LogRecordPos { file_id: 1, offset: 10 });
        assert_eq!(res1, true);
        let res2 = bt.put("foo".as_bytes().to_vec(), LogRecordPos { file_id: 2, offset: 20 });
        assert_eq!(res2, true);

        let res3 = bt.delete("hello".as_bytes().to_vec());
        assert_eq!(res3, true);
        let res4 = bt.delete("hello".as_bytes().to_vec());
        assert_eq!(res4, false);
    }
}

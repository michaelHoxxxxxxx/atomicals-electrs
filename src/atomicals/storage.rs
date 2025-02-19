use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bitcoin::{TxOut, Txid};
use electrs_rocksdb::{DB, IteratorMode, Options, WriteBatch};
use serde::{Deserialize, Serialize};

use super::protocol::{AtomicalId, AtomicalType};
use super::state::AtomicalOutput;

const CF_STATE: &str = "state";
const CF_OUTPUTS: &str = "outputs";
const CF_METADATA: &str = "metadata";
const CF_INDEXES: &str = "indexes";

/// Atomicals 存储
pub struct AtomicalsStorage {
    /// RocksDB 实例
    db: Arc<DB>,
}

impl AtomicalsStorage {
    /// 创建新的存储实例
    pub fn new(data_dir: &Path) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        
        // 创建列族
        let cf_opts = Options::default();
        let cf_descriptors = vec![
            electrs_rocksdb::ColumnFamilyDescriptor::new(CF_STATE, cf_opts.clone()),
            electrs_rocksdb::ColumnFamilyDescriptor::new(CF_OUTPUTS, cf_opts.clone()),
            electrs_rocksdb::ColumnFamilyDescriptor::new(CF_METADATA, cf_opts.clone()),
            electrs_rocksdb::ColumnFamilyDescriptor::new(CF_INDEXES, cf_opts),
        ];
        
        // 打开数据库
        let db = electrs_rocksdb::DB::open_cf_descriptors(&opts, data_dir, cf_descriptors)?;
        
        Ok(Self { db: Arc::new(db) })
    }

    /// 存储状态
    pub fn store_state(&self, state: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_STATE)
            .ok_or_else(|| anyhow!("Column family not found: {}", CF_STATE))?;
        
        self.db.put_cf(&cf, "current_state", state)?;
        Ok(())
    }

    /// 加载状态
    pub fn load_state(&self) -> Result<Option<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_STATE)
            .ok_or_else(|| anyhow!("Column family not found: {}", CF_STATE))?;
        
        Ok(self.db.get_cf(&cf, "current_state")?)
    }

    /// 存储输出
    pub fn store_output(&self, atomical_id: &AtomicalId, output: &AtomicalOutput) -> Result<()> {
        let cf = self.db.cf_handle(CF_OUTPUTS)
            .ok_or_else(|| anyhow!("Column family not found: {}", CF_OUTPUTS))?;
        
        let key = format!("{}:{}", atomical_id.txid, atomical_id.vout);
        let value = serde_json::to_vec(output)?;
        
        self.db.put_cf(&cf, key.as_bytes(), value)?;
        Ok(())
    }

    /// 获取输出
    pub fn get_output(&self, atomical_id: &AtomicalId) -> Result<Option<AtomicalOutput>> {
        let cf = self.db.cf_handle(CF_OUTPUTS)
            .ok_or_else(|| anyhow!("Column family not found: {}", CF_OUTPUTS))?;
        
        let key = format!("{}:{}", atomical_id.txid, atomical_id.vout);
        
        if let Some(value) = self.db.get_cf(&cf, key.as_bytes())? {
            let output: AtomicalOutput = serde_json::from_slice(&value)?;
            Ok(Some(output))
        } else {
            Ok(None)
        }
    }

    /// 存储元数据
    pub fn store_metadata(&self, atomical_id: &AtomicalId, metadata: &serde_json::Value) -> Result<()> {
        let cf = self.db.cf_handle(CF_METADATA)
            .ok_or_else(|| anyhow!("Column family not found: {}", CF_METADATA))?;
        
        let key = format!("{}:{}", atomical_id.txid, atomical_id.vout);
        let value = serde_json::to_vec(metadata)?;
        
        self.db.put_cf(&cf, key.as_bytes(), value)?;
        Ok(())
    }

    /// 获取元数据
    pub fn get_metadata(&self, atomical_id: &AtomicalId) -> Result<Option<serde_json::Value>> {
        let cf = self.db.cf_handle(CF_METADATA)
            .ok_or_else(|| anyhow!("Column family not found: {}", CF_METADATA))?;
        
        let key = format!("{}:{}", atomical_id.txid, atomical_id.vout);
        
        if let Some(value) = self.db.get_cf(&cf, key.as_bytes())? {
            let metadata: serde_json::Value = serde_json::from_slice(&value)?;
            Ok(Some(metadata))
        } else {
            Ok(None)
        }
    }

    /// 存储索引
    pub fn store_index(&self, key: &str, value: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_INDEXES)
            .ok_or_else(|| anyhow!("Column family not found: {}", CF_INDEXES))?;
        
        self.db.put_cf(&cf, key.as_bytes(), value)?;
        Ok(())
    }

    /// 加载所有索引
    pub fn load_all_indexes(&self) -> Result<Vec<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_INDEXES)
            .ok_or_else(|| anyhow!("Column family not found: {}", CF_INDEXES))?;
        
        let mut indexes = Vec::new();
        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);
        
        for item in iter {
            let (_, value) = item?;
            indexes.push(value.to_vec());
        }
        
        Ok(indexes)
    }

    /// 压缩数据库
    pub fn compact(&self) -> Result<()> {
        for cf_name in &[CF_STATE, CF_OUTPUTS, CF_METADATA, CF_INDEXES] {
            let cf = self.db.cf_handle(cf_name)
                .ok_or_else(|| anyhow!("Column family not found: {}", cf_name))?;
            
            self.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_storage() -> (AtomicalsStorage, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let storage = AtomicalsStorage::new(temp_dir.path()).unwrap();
        (storage, temp_dir)
    }

    fn create_test_atomical_id() -> AtomicalId {
        AtomicalId {
            txid: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".parse().unwrap(),
            vout: 0,
        }
    }

    fn create_test_output(atomical_id: &AtomicalId) -> AtomicalOutput {
        AtomicalOutput {
            txid: atomical_id.txid,
            vout: atomical_id.vout,
            output: TxOut::default(),
            metadata: None,
            height: 100,
            timestamp: 1234567890,
            atomical_type: AtomicalType::NFT,
            sealed: false,
        }
    }

    #[test]
    fn test_storage_creation() -> Result<()> {
        let (storage, _temp_dir) = create_test_storage();
        
        // 验证列族是否创建成功
        for cf_name in &[CF_STATE, CF_OUTPUTS, CF_METADATA, CF_INDEXES] {
            assert!(storage.db.cf_handle(cf_name).is_some());
        }
        
        Ok(())
    }

    #[test]
    fn test_state_operations() -> Result<()> {
        let (storage, _temp_dir) = create_test_storage();
        
        // 测试存储和加载状态
        let test_state = b"test state data";
        storage.store_state(test_state)?;
        
        let loaded_state = storage.load_state()?;
        assert!(loaded_state.is_some());
        assert_eq!(loaded_state.unwrap(), test_state);
        
        Ok(())
    }

    #[test]
    fn test_output_operations() -> Result<()> {
        let (storage, _temp_dir) = create_test_storage();
        let atomical_id = create_test_atomical_id();
        let output = create_test_output(&atomical_id);
        
        // 测试存储输出
        storage.store_output(&atomical_id, &output)?;
        
        // 测试获取输出
        let loaded_output = storage.get_output(&atomical_id)?;
        assert!(loaded_output.is_some());
        let loaded_output = loaded_output.unwrap();
        assert_eq!(loaded_output.txid, output.txid);
        assert_eq!(loaded_output.vout, output.vout);
        assert_eq!(loaded_output.height, output.height);
        assert_eq!(loaded_output.timestamp, output.timestamp);
        
        Ok(())
    }

    #[test]
    fn test_metadata_operations() -> Result<()> {
        let (storage, _temp_dir) = create_test_storage();
        let atomical_id = create_test_atomical_id();
        
        // 测试存储元数据
        let metadata = serde_json::json!({
            "name": "Test NFT",
            "description": "Test Description",
            "attributes": {
                "rarity": "legendary",
                "level": 100
            }
        });
        
        storage.store_metadata(&atomical_id, &metadata)?;
        
        // 测试获取元数据
        let loaded_metadata = storage.get_metadata(&atomical_id)?;
        assert!(loaded_metadata.is_some());
        assert_eq!(loaded_metadata.unwrap(), metadata);
        
        Ok(())
    }

    #[test]
    fn test_index_operations() -> Result<()> {
        let (storage, _temp_dir) = create_test_storage();
        
        // 测试存储索引
        let test_indexes = vec![
            ("index1", b"value1"),
            ("index2", b"value2"),
            ("index3", b"value3"),
        ];
        
        for (key, value) in &test_indexes {
            storage.store_index(key, value)?;
        }
        
        // 测试加载所有索引
        let loaded_indexes = storage.load_all_indexes()?;
        assert_eq!(loaded_indexes.len(), test_indexes.len());
        
        for (i, value) in test_indexes.iter().enumerate() {
            assert!(loaded_indexes.contains(&value.1.to_vec()));
        }
        
        Ok(())
    }

    #[test]
    fn test_compaction() -> Result<()> {
        let (storage, _temp_dir) = create_test_storage();
        
        // 添加一些测试数据
        let atomical_id = create_test_atomical_id();
        let output = create_test_output(&atomical_id);
        storage.store_output(&atomical_id, &output)?;
        
        let metadata = serde_json::json!({"test": "data"});
        storage.store_metadata(&atomical_id, &metadata)?;
        
        // 执行压缩
        storage.compact()?;
        
        // 验证数据仍然可以访问
        assert!(storage.get_output(&atomical_id)?.is_some());
        assert!(storage.get_metadata(&atomical_id)?.is_some());
        
        Ok(())
    }

    #[test]
    fn test_concurrent_operations() -> Result<()> {
        use std::thread;
        
        let (storage, _temp_dir) = create_test_storage();
        let storage = Arc::new(storage);
        
        let mut handles = vec![];
        
        // 创建多个线程同时操作存储
        for i in 0..10 {
            let storage = Arc::clone(&storage);
            let handle = thread::spawn(move || -> Result<()> {
                let atomical_id = AtomicalId {
                    txid: format!("{:064x}", i).parse().unwrap(),
                    vout: 0,
                };
                
                let output = AtomicalOutput {
                    txid: atomical_id.txid,
                    vout: atomical_id.vout,
                    output: TxOut::default(),
                    metadata: None,
                    height: 100,
                    timestamp: 1234567890,
                    atomical_type: AtomicalType::NFT,
                    sealed: false,
                };
                
                storage.store_output(&atomical_id, &output)?;
                
                let metadata = serde_json::json!({
                    "thread": i,
                    "timestamp": std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                });
                
                storage.store_metadata(&atomical_id, &metadata)?;
                
                Ok(())
            });
            
            handles.push(handle);
        }
        
        // 等待所有线程完成
        for handle in handles {
            handle.join().unwrap()?;
        }
        
        Ok(())
    }
}

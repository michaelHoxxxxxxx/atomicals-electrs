use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bitcoin::Txid;
use electrs_rocksdb::{DB, IteratorMode, Options, WriteBatch};
use serde::{Deserialize, Serialize};

use super::protocol::AtomicalId;
use super::state::{AtomicalOutput, OwnerInfo};

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
        
        self.db.put_cf(&cf, key, value)?;
        Ok(())
    }

    /// 获取输出
    pub fn get_output(&self, atomical_id: &AtomicalId) -> Result<Option<AtomicalOutput>> {
        let cf = self.db.cf_handle(CF_OUTPUTS)
            .ok_or_else(|| anyhow!("Column family not found: {}", CF_OUTPUTS))?;
        
        let key = format!("{}:{}", atomical_id.txid, atomical_id.vout);
        
        if let Some(value) = self.db.get_cf(&cf, key)? {
            Ok(Some(serde_json::from_slice(&value)?))
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
        
        self.db.put_cf(&cf, key, value)?;
        Ok(())
    }

    /// 获取元数据
    pub fn get_metadata(&self, atomical_id: &AtomicalId) -> Result<Option<serde_json::Value>> {
        let cf = self.db.cf_handle(CF_METADATA)
            .ok_or_else(|| anyhow!("Column family not found: {}", CF_METADATA))?;
        
        let key = format!("{}:{}", atomical_id.txid, atomical_id.vout);
        
        if let Some(value) = self.db.get_cf(&cf, key)? {
            Ok(Some(serde_json::from_slice(&value)?))
        } else {
            Ok(None)
        }
    }

    /// 存储索引
    pub fn store_index(&self, key: &str, value: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_INDEXES)
            .ok_or_else(|| anyhow!("Column family not found: {}", CF_INDEXES))?;
        
        self.db.put_cf(&cf, key, value)?;
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
        // 压缩所有列族
        for cf_name in [CF_STATE, CF_OUTPUTS, CF_METADATA, CF_INDEXES] {
            if let Some(cf) = self.db.cf_handle(cf_name) {
                self.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::str::FromStr;
    use bitcoin::OutPoint;
    use serde_json::json;

    fn create_test_storage() -> (AtomicalsStorage, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let storage = AtomicalsStorage::new(temp_dir.path()).unwrap();
        (storage, temp_dir)
    }

    fn create_test_atomical_id() -> AtomicalId {
        let txid = bitcoin::Txid::from_str(
            "1234567890123456789012345678901234567890123456789012345678901234"
        ).unwrap();
        AtomicalId { txid, vout: 0 }
    }

    fn create_test_output(atomical_id: &AtomicalId) -> AtomicalOutput {
        AtomicalOutput {
            owner: OwnerInfo {
                script_pubkey: vec![1, 2, 3],
                value: 1000,
            },
            atomical_id: atomical_id.clone(),
            metadata: Some(json!({
                "name": "Test NFT",
                "description": "Test Description"
            })),
            location: OutPoint::new(atomical_id.txid, 0),
            spent: false,
            height: 100,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    #[test]
    fn test_storage_creation() -> Result<()> {
        let temp_dir = TempDir::new()?;
        
        // 测试正常创建
        let storage = AtomicalsStorage::new(temp_dir.path())?;
        assert!(storage.db.cf_handle(CF_STATE).is_some());
        assert!(storage.db.cf_handle(CF_OUTPUTS).is_some());
        assert!(storage.db.cf_handle(CF_METADATA).is_some());
        assert!(storage.db.cf_handle(CF_INDEXES).is_some());

        // 测试重复打开
        let storage2 = AtomicalsStorage::new(temp_dir.path())?;
        assert!(storage2.db.cf_handle(CF_STATE).is_some());

        Ok(())
    }

    #[test]
    fn test_state_operations() -> Result<()> {
        let (storage, _temp_dir) = create_test_storage();

        // 测试存储和加载状态
        let test_state = b"test_state_data";
        storage.store_state(test_state)?;
        assert_eq!(storage.load_state()?.unwrap(), test_state);

        // 测试更新状态
        let new_state = b"updated_state_data";
        storage.store_state(new_state)?;
        assert_eq!(storage.load_state()?.unwrap(), new_state);

        // 测试大状态数据
        let large_state = vec![0xFF; 1000];
        storage.store_state(&large_state)?;
        assert_eq!(storage.load_state()?.unwrap(), large_state);

        Ok(())
    }

    #[test]
    fn test_output_operations() -> Result<()> {
        let (storage, _temp_dir) = create_test_storage();
        let atomical_id = create_test_atomical_id();
        let output = create_test_output(&atomical_id);

        // 测试存储和获取输出
        storage.store_output(&atomical_id, &output)?;
        let retrieved = storage.get_output(&atomical_id)?.unwrap();
        assert_eq!(retrieved.owner.value, output.owner.value);
        assert_eq!(retrieved.owner.script_pubkey, output.owner.script_pubkey);
        assert_eq!(retrieved.height, output.height);
        assert_eq!(retrieved.timestamp, output.timestamp);

        // 测试不存在的输出
        let non_existent_id = AtomicalId {
            txid: bitcoin::Txid::from_str(
                "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
            ).unwrap(),
            vout: 0,
        };
        assert!(storage.get_output(&non_existent_id)?.is_none());

        // 测试更新输出
        let mut updated_output = output.clone();
        updated_output.owner.value = 2000;
        storage.store_output(&atomical_id, &updated_output)?;
        let retrieved = storage.get_output(&atomical_id)?.unwrap();
        assert_eq!(retrieved.owner.value, 2000);

        Ok(())
    }

    #[test]
    fn test_metadata_operations() -> Result<()> {
        let (storage, _temp_dir) = create_test_storage();
        let atomical_id = create_test_atomical_id();

        // 测试简单元数据
        let metadata = json!({
            "name": "Test NFT",
            "description": "Test Description"
        });
        storage.store_metadata(&atomical_id, &metadata)?;
        let retrieved = storage.get_metadata(&atomical_id)?.unwrap();
        assert_eq!(retrieved["name"], "Test NFT");
        assert_eq!(retrieved["description"], "Test Description");

        // 测试复杂元数据
        let complex_metadata = json!({
            "name": "Complex NFT",
            "description": "Complex Description",
            "attributes": [
                {
                    "trait_type": "Color",
                    "value": "Blue"
                },
                {
                    "trait_type": "Size",
                    "value": "Large"
                }
            ],
            "nested": {
                "field1": "value1",
                "field2": 42,
                "field3": true,
                "field4": null,
                "field5": ["item1", "item2"]
            }
        });
        storage.store_metadata(&atomical_id, &complex_metadata)?;
        let retrieved = storage.get_metadata(&atomical_id)?.unwrap();
        assert_eq!(retrieved["name"], "Complex NFT");
        assert_eq!(retrieved["attributes"][0]["trait_type"], "Color");
        assert_eq!(retrieved["nested"]["field2"], 42);

        // 测试更新元数据
        let updated_metadata = json!({
            "name": "Updated NFT",
            "version": 2
        });
        storage.store_metadata(&atomical_id, &updated_metadata)?;
        let retrieved = storage.get_metadata(&atomical_id)?.unwrap();
        assert_eq!(retrieved["name"], "Updated NFT");
        assert_eq!(retrieved["version"], 2);

        // 测试不存在的元数据
        let non_existent_id = AtomicalId {
            txid: bitcoin::Txid::from_str(
                "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
            ).unwrap(),
            vout: 0,
        };
        assert!(storage.get_metadata(&non_existent_id)?.is_none());

        Ok(())
    }

    #[test]
    fn test_index_operations() -> Result<()> {
        let (storage, _temp_dir) = create_test_storage();

        // 测试存储和加载索引
        let test_indexes = vec![
            ("index1", vec![1, 2, 3]),
            ("index2", vec![4, 5, 6]),
            ("index3", vec![7, 8, 9]),
        ];

        // 存储索引
        for (key, value) in &test_indexes {
            storage.store_index(key, value)?;
        }

        // 加载所有索引
        let loaded_indexes = storage.load_all_indexes()?;
        assert_eq!(loaded_indexes.len(), test_indexes.len());

        // 验证每个索引的内容
        for (_, value) in &test_indexes {
            assert!(loaded_indexes.contains(value));
        }

        // 测试更新索引
        storage.store_index("index1", &vec![10, 11, 12])?;
        let updated_indexes = storage.load_all_indexes()?;
        assert!(updated_indexes.contains(&vec![10, 11, 12]));

        Ok(())
    }

    #[test]
    fn test_compaction() -> Result<()> {
        let (storage, _temp_dir) = create_test_storage();
        let atomical_id = create_test_atomical_id();

        // 存储一些数据
        storage.store_state(b"test_state")?;
        storage.store_output(&atomical_id, &create_test_output(&atomical_id))?;
        storage.store_metadata(&atomical_id, &json!({"name": "Test NFT"}))?;
        storage.store_index("test_index", &vec![1, 2, 3])?;

        // 执行压缩
        storage.compact()?;

        // 验证数据完整性
        assert_eq!(storage.load_state()?.unwrap(), b"test_state");
        assert!(storage.get_output(&atomical_id)?.is_some());
        assert!(storage.get_metadata(&atomical_id)?.is_some());
        assert!(!storage.load_all_indexes()?.is_empty());

        Ok(())
    }

    #[test]
    fn test_concurrent_operations() -> Result<()> {
        use std::sync::Arc;
        use std::thread;

        let (storage, _temp_dir) = create_test_storage();
        let storage = Arc::new(storage);
        let atomical_id = create_test_atomical_id();

        // 创建多个线程同时操作存储
        let mut handles = vec![];
        for i in 0..10 {
            let storage = storage.clone();
            let atomical_id = atomical_id.clone();
            
            let handle = thread::spawn(move || -> Result<()> {
                // 存储状态
                storage.store_state(format!("state_{}", i).as_bytes())?;

                // 存储输出
                let mut output = create_test_output(&atomical_id);
                output.owner.value = 1000 + i;
                storage.store_output(&atomical_id, &output)?;

                // 存储元数据
                let metadata = json!({
                    "name": format!("NFT_{}", i),
                    "value": i
                });
                storage.store_metadata(&atomical_id, &metadata)?;

                // 存储索引
                storage.store_index(&format!("index_{}", i), &vec![i as u8])?;

                Ok(())
            });
            
            handles.push(handle);
        }

        // 等待所有线程完成
        for handle in handles {
            handle.join().unwrap()?;
        }

        // 验证数据
        assert!(storage.load_state()?.is_some());
        assert!(storage.get_output(&atomical_id)?.is_some());
        assert!(storage.get_metadata(&atomical_id)?.is_some());
        assert!(!storage.load_all_indexes()?.is_empty());

        Ok(())
    }
}

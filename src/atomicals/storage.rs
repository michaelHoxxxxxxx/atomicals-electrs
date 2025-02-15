use std::path::Path;
use anyhow::{anyhow, Result};
use rocksdb::{DB, ColumnFamilyDescriptor, Options};
use serde::{Serialize, Deserialize};
use std::time::{SystemTime, UNIX_EPOCH};

use super::protocol::AtomicalId;
use super::state::{AtomicalOutput, OwnerInfo};

const CF_STATE: &str = "state";
const CF_OUTPUTS: &str = "outputs";
const CF_METADATA: &str = "metadata";
const CF_INDEXES: &str = "indexes";

/// Atomicals 存储
pub struct AtomicalsStorage {
    /// RocksDB 实例
    db: DB,
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
            ColumnFamilyDescriptor::new(CF_STATE, cf_opts.clone()),
            ColumnFamilyDescriptor::new(CF_OUTPUTS, cf_opts.clone()),
            ColumnFamilyDescriptor::new(CF_METADATA, cf_opts.clone()),
            ColumnFamilyDescriptor::new(CF_INDEXES, cf_opts),
        ];
        
        // 打开数据库
        let db = DB::open_cf_descriptors(&opts, data_dir, cf_descriptors)?;
        
        Ok(Self { db })
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
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        
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

    #[test]
    fn test_storage() -> Result<()> {
        let (storage, _temp_dir) = create_test_storage();
        let atomical_id = create_test_atomical_id();

        // 测试状态存储
        storage.store_state(b"test_state")?;
        assert_eq!(storage.load_state()?.unwrap(), b"test_state");

        // 测试输出存储
        let output = AtomicalOutput {
            owner: OwnerInfo {
                script_pubkey: vec![],
                value: 1000,
            },
            atomical_id: atomical_id.clone(),
            metadata: None,
        };
        storage.store_output(&atomical_id, &output)?;
        assert_eq!(
            storage.get_output(&atomical_id)?.unwrap().owner.value,
            output.owner.value
        );

        // 测试元数据存储
        let metadata = serde_json::json!({
            "name": "Test NFT",
            "description": "Test Description",
        });
        storage.store_metadata(&atomical_id, &metadata)?;
        assert_eq!(
            storage.get_metadata(&atomical_id)?.unwrap()["name"],
            "Test NFT"
        );

        Ok(())
    }
}

use std::collections::HashMap;
use anyhow::{anyhow, Result};
use bitcoin::{OutPoint, Address};
use serde::{Serialize, Deserialize};
use dashmap::DashMap;
use std::sync::Arc;

use super::protocol::{AtomicalId, AtomicalType};
use super::state::{AtomicalOutput, OwnerInfo};
use super::storage::AtomicalsStorage;

/// 索引类型
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum IndexType {
    /// 地址索引
    Address,
    /// 类型索引
    Type,
    /// 元数据索引
    Metadata,
    /// 时间索引
    Timestamp,
}

/// 索引项
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexEntry {
    /// 索引类型
    pub index_type: IndexType,
    /// 索引键
    pub key: String,
    /// Atomical ID
    pub atomical_id: AtomicalId,
    /// 更新时间戳
    pub timestamp: u64,
}

/// 索引统计信息
#[derive(Debug, Serialize, Deserialize)]
pub struct IndexStats {
    /// 每种索引类型的数量
    pub counts: HashMap<IndexType, u64>,
    /// 总索引数量
    pub total_count: u64,
    /// 索引大小（字节）
    pub size_bytes: u64,
}

/// Atomicals 索引器
pub struct AtomicalsIndexer {
    /// 存储引擎
    storage: Arc<AtomicalsStorage>,
    /// 内存缓存
    cache: DashMap<String, Vec<IndexEntry>>,
}

impl AtomicalsIndexer {
    /// 创建新的索引器实例
    pub fn new(storage: AtomicalsStorage) -> Self {
        Self {
            storage: Arc::new(storage),
            cache: DashMap::new(),
        }
    }

    /// 添加索引项
    pub fn add_index(&self, entry: IndexEntry) -> Result<()> {
        let key = self.create_index_key(&entry);
        
        // 更新内存缓存
        self.cache
            .entry(key.clone())
            .or_default()
            .push(entry.clone());
        
        // 持久化到存储
        self.storage.store_index(&key, &entry)?;
        
        Ok(())
    }

    /// 批量添加索引项
    pub fn add_indexes(&self, entries: &[IndexEntry]) -> Result<()> {
        for entry in entries {
            self.add_index(entry.clone())?;
        }
        Ok(())
    }

    /// 根据地址查询 Atomicals
    pub fn find_by_address(&self, address: &Address) -> Result<Vec<AtomicalOutput>> {
        let key = format!("address:{}", address);
        let entries = self.cache
            .get(&key)
            .map(|e| e.clone())
            .unwrap_or_default();
        
        let mut outputs = Vec::new();
        for entry in entries {
            if let Some(output) = self.storage.get_output(&entry.atomical_id)? {
                outputs.push(output);
            }
        }
        
        Ok(outputs)
    }

    /// 根据类型查询 Atomicals
    pub fn find_by_type(&self, atomical_type: AtomicalType) -> Result<Vec<AtomicalOutput>> {
        let key = format!("type:{:?}", atomical_type);
        let entries = self.cache
            .get(&key)
            .map(|e| e.clone())
            .unwrap_or_default();
        
        let mut outputs = Vec::new();
        for entry in entries {
            if let Some(output) = self.storage.get_output(&entry.atomical_id)? {
                outputs.push(output);
            }
        }
        
        Ok(outputs)
    }

    /// 根据元数据查询 Atomicals
    pub fn find_by_metadata(&self, key: &str, value: &str) -> Result<Vec<AtomicalOutput>> {
        let index_key = format!("metadata:{}:{}", key, value);
        let entries = self.cache
            .get(&index_key)
            .map(|e| e.clone())
            .unwrap_or_default();
        
        let mut outputs = Vec::new();
        for entry in entries {
            if let Some(output) = self.storage.get_output(&entry.atomical_id)? {
                outputs.push(output);
            }
        }
        
        Ok(outputs)
    }

    /// 根据时间范围查询 Atomicals
    pub fn find_by_time_range(
        &self,
        start_time: u64,
        end_time: u64,
    ) -> Result<Vec<AtomicalOutput>> {
        let mut outputs = Vec::new();
        
        // 遍历所有时间索引
        for entry in self.cache.iter() {
            let entries = entry.value();
            for entry in entries {
                if entry.index_type == IndexType::Timestamp 
                    && entry.timestamp >= start_time 
                    && entry.timestamp <= end_time {
                    if let Some(output) = self.storage.get_output(&entry.atomical_id)? {
                        outputs.push(output);
                    }
                }
            }
        }
        
        Ok(outputs)
    }

    /// 获取索引统计信息
    pub fn get_stats(&self) -> IndexStats {
        let mut counts = HashMap::new();
        let mut total_count = 0;
        let mut size_bytes = 0;
        
        // 统计每种索引类型的数量
        for entry in self.cache.iter() {
            let entries = entry.value();
            for entry in entries {
                *counts.entry(entry.index_type).or_default() += 1;
                total_count += 1;
                size_bytes += std::mem::size_of_val(entry) as u64;
            }
        }
        
        IndexStats {
            counts,
            total_count,
            size_bytes,
        }
    }

    /// 创建索引键
    fn create_index_key(&self, entry: &IndexEntry) -> String {
        match entry.index_type {
            IndexType::Address => format!("address:{}", entry.key),
            IndexType::Type => format!("type:{}", entry.key),
            IndexType::Metadata => format!("metadata:{}", entry.key),
            IndexType::Timestamp => format!("timestamp:{}", entry.timestamp),
        }
    }

    /// 重建索引
    pub fn rebuild_indexes(&self) -> Result<()> {
        // 清空内存缓存
        self.cache.clear();
        
        // 从存储中加载所有索引
        self.storage.load_all_indexes()?
            .into_iter()
            .try_for_each(|entry| self.add_index(entry))?;
        
        Ok(())
    }

    /// 压缩索引
    pub fn compact(&self) -> Result<()> {
        // 压缩存储
        self.storage.compact()?;
        
        // 清理内存缓存中的无效项
        self.cache.retain(|_, entries| {
            entries.retain(|entry| {
                self.storage.get_output(&entry.atomical_id).unwrap().is_some()
            });
            !entries.is_empty()
        });
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use tempfile::TempDir;

    fn create_test_storage() -> AtomicalsStorage {
        let temp_dir = TempDir::new().unwrap();
        AtomicalsStorage::new(temp_dir.path()).unwrap()
    }

    fn create_test_atomical_id() -> AtomicalId {
        let txid = bitcoin::Txid::from_str(
            "1234567890123456789012345678901234567890123456789012345678901234"
        ).unwrap();
        AtomicalId { txid, vout: 0 }
    }

    fn create_test_index_entry() -> IndexEntry {
        IndexEntry {
            index_type: IndexType::Address,
            key: "bc1qtest".to_string(),
            atomical_id: create_test_atomical_id(),
            timestamp: 1644900000,
        }
    }

    #[test]
    fn test_indexer() -> Result<()> {
        let storage = create_test_storage();
        let indexer = AtomicalsIndexer::new(storage);
        
        // 测试添加索引
        let entry = create_test_index_entry();
        indexer.add_index(entry.clone())?;
        
        // 测试查询
        let address = Address::from_str("bc1qtest").unwrap();
        let outputs = indexer.find_by_address(&address)?;
        assert!(outputs.is_empty()); // 因为我们没有添加对应的输出
        
        // 测试统计
        let stats = indexer.get_stats();
        assert_eq!(stats.total_count, 1);
        assert_eq!(*stats.counts.get(&IndexType::Address).unwrap(), 1);
        
        Ok(())
    }
}

use std::collections::HashMap;
use anyhow::{anyhow, Result};
use bitcoin::{OutPoint, Address, TxOut};
use serde::{Serialize, Deserialize};
use dashmap::DashMap;
use std::sync::Arc;
use bincode;

use super::protocol::{AtomicalId, AtomicalType};
use super::state::AtomicalOutput;
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
        
        // 序列化并持久化到存储
        let serialized = bincode::serialize(&entry)?;
        self.storage.store_index(&key, &serialized[..])?;
        
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
    use bitcoin::hashes::hex::FromHex;
    use bitcoin::Network;
    use bitcoin::secp256k1::rand::{self, Rng};
    use serde_json::json;
    use std::str::FromStr;

    fn create_test_storage() -> AtomicalsStorage {
        AtomicalsStorage::new_test().unwrap()
    }

    fn create_test_atomical_id() -> AtomicalId {
        AtomicalId {
            txid: bitcoin::Txid::from_str(
                "1234567890123456789012345678901234567890123456789012345678901234"
            ).unwrap(),
            vout: 0,
        }
    }

    fn create_test_address() -> Address {
        Address::from_str("bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080").unwrap()
    }

    fn create_test_index_entry(index_type: IndexType, key: &str) -> IndexEntry {
        IndexEntry {
            index_type,
            key: key.to_string(),
            atomical_id: create_test_atomical_id(),
            timestamp: 1234567890,
        }
    }

    fn create_test_output(atomical_id: &AtomicalId, value: u64) -> AtomicalOutput {
        let mut rng = rand::thread_rng();
        let mut script = vec![0u8; 32];
        rng.fill(&mut script[..]);

        AtomicalOutput {
            txid: atomical_id.txid,
            vout: atomical_id.vout,
            output: TxOut {
                value,
                script_pubkey: bitcoin::Script::from_bytes(&script),
            },
            metadata: None,
            height: 100,
            timestamp: 1234567890,
            atomical_type: AtomicalType::NFT,
            sealed: false,
        }
    }

    #[test]
    fn test_add_index() -> Result<()> {
        let indexer = AtomicalsIndexer::new(create_test_storage());
        let entry = create_test_index_entry(IndexType::Address, "test_address");

        // 测试添加单个索引
        indexer.add_index(entry.clone())?;

        // 验证缓存
        let key = indexer.create_index_key(&entry);
        let cached = indexer.cache.get(&key).unwrap();
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].key, "test_address");

        // 验证存储
        let stored = indexer.storage.get_index(&key)?;
        assert!(stored.is_some());
        assert_eq!(stored.unwrap().key, "test_address");

        Ok(())
    }

    #[test]
    fn test_add_indexes() -> Result<()> {
        let indexer = AtomicalsIndexer::new(create_test_storage());
        let entries = vec![
            create_test_index_entry(IndexType::Address, "address1"),
            create_test_index_entry(IndexType::Type, "NFT"),
            create_test_index_entry(IndexType::Metadata, "name:test"),
        ];

        // 测试批量添加
        indexer.add_indexes(&entries)?;

        // 验证每个索引
        for entry in entries {
            let key = indexer.create_index_key(&entry);
            let cached = indexer.cache.get(&key).unwrap();
            assert_eq!(cached.len(), 1);
            
            let stored = indexer.storage.get_index(&key)?;
            assert!(stored.is_some());
        }

        Ok(())
    }

    #[test]
    fn test_find_by_address() -> Result<()> {
        let indexer = AtomicalsIndexer::new(create_test_storage());
        let address = create_test_address();
        let id = create_test_atomical_id();

        // 创建测试数据
        let entry = create_test_index_entry(IndexType::Address, &address.to_string());
        let output = create_test_output(&id, 1000);

        // 添加到索引和存储
        indexer.add_index(entry)?;
        indexer.storage.store_output(&id, &output)?;

        // 测试查询
        let outputs = indexer.find_by_address(&address)?;
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].atomical_id, id);
        assert_eq!(outputs[0].output.value, 1000);

        // 测试不存在的地址
        let nonexistent = Address::from_str("bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt081")?;
        let outputs = indexer.find_by_address(&nonexistent)?;
        assert!(outputs.is_empty());

        Ok(())
    }

    #[test]
    fn test_find_by_type() -> Result<()> {
        let indexer = AtomicalsIndexer::new(create_test_storage());
        let id = create_test_atomical_id();

        // 创建测试数据
        let entry = create_test_index_entry(IndexType::Type, "NFT");
        let output = create_test_output(&id, 1000);

        // 添加到索引和存储
        indexer.add_index(entry)?;
        indexer.storage.store_output(&id, &output)?;

        // 测试查询
        let outputs = indexer.find_by_type(AtomicalType::NFT)?;
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].atomical_id, id);

        // 测试不存在的类型
        let outputs = indexer.find_by_type(AtomicalType::FT)?;
        assert!(outputs.is_empty());

        Ok(())
    }

    #[test]
    fn test_find_by_metadata() -> Result<()> {
        let indexer = AtomicalsIndexer::new(create_test_storage());
        let id = create_test_atomical_id();

        // 创建测试数据
        let entry = create_test_index_entry(IndexType::Metadata, "name:Test NFT");
        let output = create_test_output(&id, 1000);

        // 添加到索引和存储
        indexer.add_index(entry)?;
        indexer.storage.store_output(&id, &output)?;

        // 测试查询
        let outputs = indexer.find_by_metadata("name", "Test NFT")?;
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].atomical_id, id);

        // 测试不存在的元数据
        let outputs = indexer.find_by_metadata("name", "Nonexistent")?;
        assert!(outputs.is_empty());

        Ok(())
    }

    #[test]
    fn test_find_by_time_range() -> Result<()> {
        let indexer = AtomicalsIndexer::new(create_test_storage());
        let id = create_test_atomical_id();

        // 创建测试数据
        let entry = create_test_index_entry(IndexType::Timestamp, "1234567890");
        let output = create_test_output(&id, 1000);

        // 添加到索引和存储
        indexer.add_index(entry)?;
        indexer.storage.store_output(&id, &output)?;

        // 测试时间范围查询
        let outputs = indexer.find_by_time_range(1234567880, 1234567900)?;
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].atomical_id, id);

        // 测试不在范围内
        let outputs = indexer.find_by_time_range(1234567900, 1234567910)?;
        assert!(outputs.is_empty());

        Ok(())
    }

    #[test]
    fn test_get_stats() -> Result<()> {
        let indexer = AtomicalsIndexer::new(create_test_storage());

        // 添加测试数据
        let entries = vec![
            create_test_index_entry(IndexType::Address, "address1"),
            create_test_index_entry(IndexType::Type, "NFT"),
            create_test_index_entry(IndexType::Metadata, "name:test"),
            create_test_index_entry(IndexType::Timestamp, "1234567890"),
        ];

        indexer.add_indexes(&entries)?;

        // 测试统计信息
        let stats = indexer.get_stats();
        assert_eq!(stats.total_count, 4);
        assert_eq!(*stats.counts.get(&IndexType::Address).unwrap(), 1);
        assert_eq!(*stats.counts.get(&IndexType::Type).unwrap(), 1);
        assert_eq!(*stats.counts.get(&IndexType::Metadata).unwrap(), 1);
        assert_eq!(*stats.counts.get(&IndexType::Timestamp).unwrap(), 1);
        assert!(stats.size_bytes > 0);

        Ok(())
    }

    #[test]
    fn test_create_index_key() {
        let indexer = AtomicalsIndexer::new(create_test_storage());

        // 测试不同类型的索引键生成
        let address_entry = create_test_index_entry(IndexType::Address, "test_address");
        assert_eq!(indexer.create_index_key(&address_entry), "address:test_address");

        let type_entry = create_test_index_entry(IndexType::Type, "NFT");
        assert_eq!(indexer.create_index_key(&type_entry), "type:NFT");

        let metadata_entry = create_test_index_entry(IndexType::Metadata, "name:test");
        assert_eq!(indexer.create_index_key(&metadata_entry), "metadata:name:test");

        let timestamp_entry = create_test_index_entry(IndexType::Timestamp, "1234567890");
        assert_eq!(indexer.create_index_key(&timestamp_entry), "timestamp:1234567890");
    }

    #[test]
    fn test_concurrent_operations() -> Result<()> {
        use std::thread;
        use std::sync::Arc;

        let indexer = Arc::new(AtomicalsIndexer::new(create_test_storage()));
        let mut handles = vec![];

        // 创建多个线程并发添加索引
        for i in 0..10 {
            let indexer = Arc::clone(&indexer);
            let handle = thread::spawn(move || -> Result<()> {
                let entry = create_test_index_entry(
                    IndexType::Address,
                    &format!("address{}", i)
                );
                indexer.add_index(entry)
            });
            handles.push(handle);
        }

        // 等待所有线程完成
        for handle in handles {
            handle.join().unwrap()?;
        }

        // 验证结果
        let stats = indexer.get_stats();
        assert_eq!(stats.total_count, 10);
        assert_eq!(*stats.counts.get(&IndexType::Address).unwrap(), 10);

        Ok(())
    }
}

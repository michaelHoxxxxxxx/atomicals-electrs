// Atomicals protocol implementation for electrs
//
// This module contains the implementation of the Atomicals protocol,
// providing support for NFTs and FTs on Bitcoin.

use anyhow::Result;
use std::path::Path;
use std::sync::Arc;

pub mod indexer;
pub mod protocol;
pub mod state;
pub mod storage;
pub mod tx_parser;
pub mod validation;

pub use protocol::{AtomicalId, AtomicalOperation, AtomicalType};
pub use state::{AtomicalOutput, AtomicalsState};
pub use storage::AtomicalsStorage;

/// Atomicals 管理器
pub struct AtomicalsManager {
    /// 存储引擎
    storage: Arc<AtomicalsStorage>,
    /// 状态
    state: AtomicalsState,
}

impl AtomicalsManager {
    /// 创建新的 Atomicals 管理器
    pub fn new(data_dir: &Path) -> anyhow::Result<Self> {
        let storage = AtomicalsStorage::new(data_dir)?;
        let state = AtomicalsState::new();
        
        Ok(Self {
            storage: Arc::new(storage),
            state,
        })
    }

    /// 处理交易
    pub fn process_transaction(&mut self, tx: &bitcoin::Transaction, height: u32, timestamp: u64) -> anyhow::Result<()> {
        // 解析交易中的 Atomicals 操作
        let operations = tx_parser::parse_transaction(tx)?;
        
        // 验证操作
        for op in &operations {
            validation::validate_operation(op, tx, &self.state)?;
        }
        
        // 更新状态
        self.state.apply_operations(operations, tx, height, timestamp)?;
        
        // 保存状态
        self.storage.store_state(&self.state)?;
        
        Ok(())
    }

    /// 获取 Atomical 输出
    pub fn get_output(&self, atomical_id: &AtomicalId) -> anyhow::Result<Option<AtomicalOutput>> {
        self.storage.get_output(atomical_id)
    }

    /// 获取 Atomical 元数据
    pub fn get_metadata(&self, atomical_id: &AtomicalId) -> anyhow::Result<Option<serde_json::Value>> {
        self.storage.get_metadata(atomical_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::absolute::LockTime;
    use bitcoin::transaction::Version;
    use std::str::FromStr;
    use tempfile::TempDir;

    fn create_test_transaction() -> bitcoin::Transaction {
        bitcoin::Transaction {
            version: Version(2),
            lock_time: LockTime::ZERO,
            input: vec![],
            output: vec![],
        }
    }

    fn create_test_atomical_id() -> AtomicalId {
        let txid = bitcoin::Txid::from_str(
            "1234567890123456789012345678901234567890123456789012345678901234"
        ).unwrap();
        AtomicalId { txid, vout: 0 }
    }

    #[test]
    fn test_manager() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let manager = AtomicalsManager::new(temp_dir.path())?;
        
        // 测试处理交易
        let tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();
        
        // 验证初始状态
        assert!(manager.get_output(&atomical_id)?.is_none());
        assert!(manager.get_metadata(&atomical_id)?.is_none());
        
        Ok(())
    }
}

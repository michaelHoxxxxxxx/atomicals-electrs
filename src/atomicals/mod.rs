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
pub mod rpc;
pub mod websocket;
pub mod metrics;
pub mod util;

pub use protocol::{AtomicalId, AtomicalOperation, AtomicalType, OperationType};
pub use state::{AtomicalOutput, AtomicalsState};
pub use storage::AtomicalsStorage;
pub use indexer::AtomicalsIndexer;
pub use validation::AtomicalsValidator;
pub use rpc::AtomicalsRpc;
pub use websocket::{WsServer, WsMessage, SubscriptionType, SubscribeRequest};

/// Atomicals 管理器
pub struct AtomicalsManager {
    /// 存储引擎
    storage: Arc<AtomicalsStorage>,
    /// 状态
    state: Arc<AtomicalsState>,
    /// 索引器
    indexer: Arc<AtomicalsIndexer>,
    /// RPC 服务
    rpc: Arc<AtomicalsRpc>,
    /// WebSocket 服务器
    ws_server: Arc<WsServer>,
}

impl AtomicalsManager {
    /// 创建新的 Atomicals 管理器
    pub fn new(
        data_dir: &Path,
        config: &crate::config::Config,
        metrics: Arc<crate::metrics::Metrics>,
        db: Arc<crate::db::DB>,
    ) -> anyhow::Result<Self> {
        let storage = Arc::new(AtomicalsStorage::new(data_dir)?);
        let state = Arc::new(AtomicalsState::new());
        let indexer = Arc::new(AtomicalsIndexer::new(
            Arc::clone(&state),
            Arc::clone(&db),
            Arc::clone(&metrics),
        ));
        let rpc = Arc::new(AtomicalsRpc::new(
            Arc::clone(&state),
            Arc::clone(&indexer),
        ));
        let ws_server = Arc::new(WsServer::new(
            Arc::clone(&state),
            config.clone(),
        ));
        
        Ok(Self {
            storage,
            state,
            indexer,
            rpc,
            ws_server,
        })
    }

    /// 处理交易
    pub async fn process_transaction(
        &self,
        tx: &bitcoin::Transaction,
        height: u32,
        timestamp: u64,
    ) -> anyhow::Result<()> {
        // 解析交易中的 Atomicals 操作
        let operations = tx_parser::parse_transaction(tx)?;
        
        // 验证操作
        for op in &operations {
            AtomicalsValidator::validate_operation(op, tx, &self.state)?;
        }
        
        // 更新状态
        self.state.apply_operations(operations, tx, height, timestamp).await?;
        
        // 更新索引
        self.indexer.index_transaction(tx, height, timestamp).await?;
        
        // 广播更新
        for op in operations {
            self.ws_server.broadcast(WsMessage::AtomicalUpdate(op.into()))?;
        }
        
        Ok(())
    }

    /// 获取 Atomical 输出
    pub async fn get_output(&self, atomical_id: &AtomicalId) -> anyhow::Result<Option<AtomicalOutput>> {
        self.state.get_output(atomical_id).await
    }

    /// 获取 Atomical 元数据
    pub async fn get_metadata(&self, atomical_id: &AtomicalId) -> anyhow::Result<Option<serde_json::Value>> {
        self.state.get_metadata(atomical_id).await
    }

    /// 启动服务
    pub async fn start(&self) -> anyhow::Result<()> {
        // 启动 WebSocket 服务器
        let ws_server = Arc::clone(&self.ws_server);
        tokio::spawn(async move {
            ws_server.start(0).await.unwrap();
        });
        
        Ok(())
    }

    /// 停止服务
    pub async fn stop(&self) -> anyhow::Result<()> {
        // 停止 WebSocket 服务器
        self.ws_server.shutdown().await?;
        
        Ok(())
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

    #[tokio::test]
    async fn test_manager() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config = crate::config::Config::default();
        let metrics = Arc::new(crate::metrics::Metrics::new(
            "test".to_string(),
            "127.0.0.1:0".parse().unwrap(),
        ));
        let db = Arc::new(crate::db::DB::open(&config)?);
        
        let manager = AtomicalsManager::new(
            temp_dir.path(),
            &config,
            Arc::clone(&metrics),
            Arc::clone(&db),
        )?;
        
        // 测试处理交易
        let tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();
        
        // 验证初始状态
        assert!(manager.get_output(&atomical_id).await?.is_none());
        assert!(manager.get_metadata(&atomical_id).await?.is_none());
        
        Ok(())
    }
}

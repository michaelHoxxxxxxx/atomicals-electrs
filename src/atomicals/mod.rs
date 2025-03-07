// Atomicals protocol implementation for electrs
//
// This module contains the implementation of the Atomicals protocol,
// providing support for NFTs and FTs on Bitcoin.

use anyhow::Result;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

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
pub mod dft;

pub use protocol::{AtomicalId, AtomicalOperation, AtomicalType, OperationType};
pub use state::{AtomicalOutput, AtomicalsState};
pub use storage::AtomicalsStorage;
pub use indexer::AtomicalsIndexer;
pub use validation::AtomicalsValidator;
pub use rpc::AtomicalsRpc;
pub use websocket::{WsServer, WsMessage, SubscriptionType, SubscribeRequest};
pub use dft::{DftManager, DftInfo, DftSupplyInfo, DftHolder, DftMintEvent, DftSubdomain, DftContainer};

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
    /// DFT 管理器
    dft_manager: Arc<RwLock<DftManager>>,
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
        let dft_manager = Arc::new(RwLock::new(DftManager::new()));
        let rpc = Arc::new(AtomicalsRpc::new(
            config.network,
            Arc::clone(&state),
            Arc::clone(&storage),
            Arc::clone(&dft_manager),
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
            dft_manager,
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
        self.state.apply_operations(operations.clone(), tx, height, timestamp).await?;
        
        // 更新索引
        self.indexer.index_transaction(tx, height, timestamp).await?;
        
        // 更新 DFT 管理器
        let mut dft_manager = self.dft_manager.write().await;
        for op in &operations {
            match op {
                AtomicalOperation::DeployDFT { id, ticker, max_supply, metadata } => {
                    dft_manager.register_dft(
                        id.clone(),
                        ticker.clone(),
                        *max_supply,
                        height,
                        timestamp,
                        metadata.clone(),
                    )?;
                },
                AtomicalOperation::MintDFT { parent_id, amount } => {
                    // 获取接收地址
                    let recipient = match bitcoin::Address::from_script(&tx.output[0].script_pubkey, self.state.get_network()) {
                        Ok(addr) => addr.to_string(),
                        Err(_) => "unknown".to_string(),
                    };
                    
                    dft_manager.mint_dft(
                        parent_id.clone(),
                        *amount,
                        recipient,
                        tx.compute_txid(),
                        height,
                        timestamp,
                    )?;
                },
                AtomicalOperation::Seal { id } => {
                    // 如果是 DFT，则封印
                    if dft_manager.exists(id) {
                        dft_manager.seal_dft(id.clone())?;
                    }
                },
                AtomicalOperation::CreateSubdomain { name, parent_id, metadata } => {
                    dft_manager.create_subdomain(
                        name.clone(),
                        parent_id.clone(),
                        height,
                        timestamp,
                        metadata.clone(),
                    )?;
                },
                AtomicalOperation::CreateContainer { id, name, metadata } => {
                    dft_manager.create_container(
                        id.clone(),
                        name.clone(),
                        height,
                        timestamp,
                        metadata.clone(),
                    )?;
                },
                AtomicalOperation::AddToSubdomain { dft_id, subdomain_name } => {
                    dft_manager.add_dft_to_subdomain(
                        dft_id,
                        subdomain_name,
                    )?;
                },
                AtomicalOperation::AddToContainer { dft_id, container_id } => {
                    dft_manager.add_dft_to_container(
                        dft_id,
                        container_id,
                    )?;
                },
                AtomicalOperation::RemoveFromSubdomain { dft_id, subdomain_name } => {
                    dft_manager.remove_dft_from_subdomain(
                        dft_id,
                        subdomain_name,
                    )?;
                },
                AtomicalOperation::RemoveFromContainer { dft_id, container_id } => {
                    dft_manager.remove_dft_from_container(
                        dft_id,
                        container_id,
                    )?;
                },
                AtomicalOperation::SealContainer { container_id } => {
                    dft_manager.seal_container(container_id)?;
                },
                AtomicalOperation::Event { id, data } => {
                    // 获取发送者地址
                    let sender = match bitcoin::Address::from_script(&tx.input[0].witness_script().unwrap_or_default(), self.state.get_network()) {
                        Ok(addr) => Some(addr.to_string()),
                        Err(_) => None,
                    };
                    
                    // 获取接收者地址
                    let recipient = match bitcoin::Address::from_script(&tx.output[0].script_pubkey, self.state.get_network()) {
                        Ok(addr) => Some(addr.to_string()),
                        Err(_) => None,
                    };
                    
                    // 提取事件类型
                    let event_type = match data.get("type") {
                        Some(t) => match t.as_str() {
                            Some(s) => s.to_string(),
                            None => "unknown".to_string(),
                        },
                        None => "unknown".to_string(),
                    };
                    
                    // 提取相关的 DFT ID
                    let dft_id = match data.get("dft_id") {
                        Some(d) => match d.as_str() {
                            Some(s) => {
                                let parts: Vec<&str> = s.split(':').collect();
                                if parts.len() == 2 {
                                    match (Txid::from_str(parts[0]), parts[1].parse::<u32>()) {
                                        (Ok(txid), Ok(vout)) => Some(AtomicalId { txid, vout }),
                                        _ => None,
                                    }
                                } else {
                                    None
                                }
                            },
                            None => None,
                        },
                        None => None,
                    };
                    
                    // 提取相关的子领域名称
                    let subdomain_name = match data.get("subdomain") {
                        Some(s) => match s.as_str() {
                            Some(name) => Some(name.to_string()),
                            None => None,
                        },
                        None => None,
                    };
                    
                    // 提取相关的容器 ID
                    let container_id = match data.get("container_id") {
                        Some(c) => match c.as_str() {
                            Some(s) => {
                                let parts: Vec<&str> = s.split(':').collect();
                                if parts.len() == 2 {
                                    match (Txid::from_str(parts[0]), parts[1].parse::<u32>()) {
                                        (Ok(txid), Ok(vout)) => Some(AtomicalId { txid, vout }),
                                        _ => None,
                                    }
                                } else {
                                    None
                                }
                            },
                            None => None,
                        },
                        None => None,
                    };
                    
                    // 提取标签
                    let tags = match data.get("tags") {
                        Some(t) => match t.as_array() {
                            Some(arr) => {
                                let mut tag_list = Vec::new();
                                for tag in arr {
                                    if let Some(tag_str) = tag.as_str() {
                                        tag_list.push(tag_str.to_string());
                                    }
                                }
                                if tag_list.is_empty() { None } else { Some(tag_list) }
                            },
                            None => None,
                        },
                        None => None,
                    };
                    
                    // 注册事件元数据
                    dft_manager.register_event(
                        event_type,
                        data.clone(),
                        timestamp,
                        sender,
                        recipient,
                        dft_id,
                        subdomain_name,
                        container_id,
                        tags,
                    )?;
                },
                AtomicalOperation::Data { id, data } => {
                    // 获取创建者地址
                    let creator = match bitcoin::Address::from_script(&tx.input[0].witness_script().unwrap_or_default(), self.state.get_network()) {
                        Ok(addr) => Some(addr.to_string()),
                        Err(_) => None,
                    };
                    
                    // 提取数据类型
                    let data_type = match data.get("type") {
                        Some(t) => match t.as_str() {
                            Some(s) => s.to_string(),
                            None => "unknown".to_string(),
                        },
                        None => "unknown".to_string(),
                    };
                    
                    // 提取相关的 DFT ID
                    let dft_id = match data.get("dft_id") {
                        Some(d) => match d.as_str() {
                            Some(s) => {
                                let parts: Vec<&str> = s.split(':').collect();
                                if parts.len() == 2 {
                                    match (Txid::from_str(parts[0]), parts[1].parse::<u32>()) {
                                        (Ok(txid), Ok(vout)) => Some(AtomicalId { txid, vout }),
                                        _ => None,
                                    }
                                } else {
                                    None
                                }
                            },
                            None => None,
                        },
                        None => None,
                    };
                    
                    // 提取相关的子领域名称
                    let subdomain_name = match data.get("subdomain") {
                        Some(s) => match s.as_str() {
                            Some(name) => Some(name.to_string()),
                            None => None,
                        },
                        None => None,
                    };
                    
                    // 提取相关的容器 ID
                    let container_id = match data.get("container_id") {
                        Some(c) => match c.as_str() {
                            Some(s) => {
                                let parts: Vec<&str> = s.split(':').collect();
                                if parts.len() == 2 {
                                    match (Txid::from_str(parts[0]), parts[1].parse::<u32>()) {
                                        (Ok(txid), Ok(vout)) => Some(AtomicalId { txid, vout }),
                                        _ => None,
                                    }
                                } else {
                                    None
                                }
                            },
                            None => None,
                        },
                        None => None,
                    };
                    
                    // 提取标签
                    let tags = match data.get("tags") {
                        Some(t) => match t.as_array() {
                            Some(arr) => {
                                let mut tag_list = Vec::new();
                                for tag in arr {
                                    if let Some(tag_str) = tag.as_str() {
                                        tag_list.push(tag_str.to_string());
                                    }
                                }
                                if tag_list.is_empty() { None } else { Some(tag_list) }
                            },
                            None => None,
                        },
                        None => None,
                    };
                    
                    // 提取版本
                    let version = match data.get("version") {
                        Some(v) => match v.as_str() {
                            Some(s) => Some(s.to_string()),
                            None => None,
                        },
                        None => None,
                    };
                    
                    // 计算数据大小
                    let size = Some(data.to_string().len() as u64);
                    
                    // 计算数据校验和
                    let checksum = Some(format!("{:x}", md5::compute(data.to_string())));
                    
                    // 注册数据操作元数据
                    dft_manager.register_data(
                        data_type,
                        data.clone(),
                        timestamp,
                        creator,
                        dft_id,
                        subdomain_name,
                        container_id,
                        tags,
                        version,
                        size,
                        checksum,
                    )?;
                },
                _ => {}, // 忽略其他操作类型
            }
        }
        
        // 广播更新
        for op in &operations {
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

use std::collections::HashMap;
use tokio::sync::{RwLock, broadcast};
use serde::{Serialize, Deserialize};
use anyhow::Result;
use bitcoin::{Transaction, TxOut, OutPoint, Txid, Amount};
use serde_json::Value;
use super::protocol::{AtomicalId, AtomicalOperation, AtomicalType};
use super::tx_parser;
use super::websocket::{WsMessage, AtomicalUpdate, UpdateType, OperationNotification, OperationStatus};

/// Atomical 所有者信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OwnerInfo {
    /// 脚本公钥
    pub script_pubkey: Vec<u8>,
    /// 值
    pub value: u64,
}

/// Atomical 输出
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomicalOutput {
    /// 所有者信息
    pub owner: OwnerInfo,
    /// Atomical ID
    pub atomical_id: AtomicalId,
    /// 元数据
    pub metadata: Option<Value>,
    /// 位置
    pub location: OutPoint,
    /// 是否花费
    pub spent: bool,
    /// 区块高度
    pub height: u32,
    /// 时间戳
    pub timestamp: u64,
}

/// Atomicals 状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomicalsState {
    /// 所有 Atomicals 的输出
    outputs: RwLock<HashMap<AtomicalId, AtomicalOutput>>,
    /// 已封印的 Atomicals
    sealed: RwLock<HashMap<AtomicalId, bool>>,
    /// 元数据
    metadata: RwLock<HashMap<AtomicalId, Value>>,
    /// WebSocket 广播通道
    broadcast_tx: broadcast::Sender<WsMessage>,
}

impl AtomicalsState {
    /// 创建新的状态
    pub async fn new() -> Result<Self> {
        let (broadcast_tx, _) = broadcast::channel(1024);
        
        Ok(Self {
            outputs: RwLock::new(HashMap::new()),
            sealed: RwLock::new(HashMap::new()),
            metadata: RwLock::new(HashMap::new()),
            broadcast_tx,
        })
    }

    /// 检查 Atomical 是否存在
    pub async fn exists(&self, atomical_id: &AtomicalId) -> Result<bool> {
        let outputs = self.outputs.read().await;
        Ok(outputs.contains_key(atomical_id))
    }

    /// 检查 Atomical 是否已被封印
    pub async fn is_sealed(&self, atomical_id: &AtomicalId) -> Result<bool> {
        let sealed = self.sealed.read().await;
        Ok(sealed.get(atomical_id).copied().unwrap_or(false))
    }

    /// 获取 Atomical 输出
    pub async fn get_output(&self, atomical_id: &AtomicalId) -> Result<Option<AtomicalOutput>> {
        let outputs = self.outputs.read().await;
        Ok(outputs.get(atomical_id).cloned())
    }

    /// 获取 Atomical 元数据
    pub async fn get_metadata(&self, atomical_id: &AtomicalId) -> Result<Option<Value>> {
        let metadata = self.metadata.read().await;
        Ok(metadata.get(atomical_id).cloned())
    }

    /// 获取交易中的 Atomicals 操作
    pub async fn get_operations(&self, tx: &Transaction) -> Result<Vec<AtomicalOperation>> {
        tx_parser::parse_transaction(tx)
    }

    /// 应用操作
    pub async fn apply_operations(&mut self, operations: Vec<AtomicalOperation>, tx: &Transaction, height: u32, timestamp: u64) -> Result<()> {
        for operation in operations {
            match operation {
                AtomicalOperation::Mint { atomical_type: _, metadata } => {
                    let atomical_id = AtomicalId {
                        txid: tx.compute_txid(),
                        vout: 0,
                    };

                    let output = AtomicalOutput {
                        owner: OwnerInfo {
                            script_pubkey: tx.output[0].script_pubkey.to_bytes(),
                            value: tx.output[0].value.to_sat(),
                        },
                        atomical_id: atomical_id.clone(),
                        metadata: metadata.as_ref().map(|m| m.clone()),
                        location: OutPoint::new(tx.compute_txid(), 0),
                        spent: false,
                        height,
                        timestamp,
                    };

                    self.outputs.write().await.insert(atomical_id.clone(), output);
                    if let Some(m) = metadata {
                        self.metadata.write().await.insert(atomical_id.clone(), m);
                    }
                    
                    // 发送状态更新通知
                    self.notify_state_update(&atomical_id).await?;
                }
                AtomicalOperation::Update { atomical_id, metadata } => {
                    if let Some(output) = self.outputs.write().await.get_mut(&atomical_id) {
                        output.metadata = Some(metadata.clone());
                        self.metadata.write().await.insert(atomical_id.clone(), metadata);
                    }
                    
                    // 发送状态更新通知
                    self.notify_state_update(&atomical_id).await?;
                }
                AtomicalOperation::Seal { atomical_id } => {
                    self.sealed.write().await.insert(atomical_id, true);
                    
                    // 发送封印状态变更通知
                    self.notify_seal_status_change(&atomical_id, true).await?;
                }
                AtomicalOperation::Transfer { atomical_id, output_index } => {
                    if let Some(output) = self.outputs.write().await.get_mut(&atomical_id) {
                        output.owner = OwnerInfo {
                            script_pubkey: tx.output[output_index as usize].script_pubkey.to_bytes(),
                            value: tx.output[output_index as usize].value.to_sat(),
                        };
                        output.location = OutPoint::new(tx.txid(), output_index);
                        output.height = height;
                        output.timestamp = timestamp;
                    }
                    
                    // 发送所有权变更通知
                    self.notify_ownership_change(&atomical_id, &tx.output[output_index as usize]).await?;
                }
            }
        }
        
        // 发送操作通知
        self.notify_operation(&operations, tx.txid().to_string(), Some(height)).await?;
        
        Ok(())
    }
    
    // WebSocket 通知方法
    
    /// 通知所有权变更
    async fn notify_ownership_change(&self, id: &AtomicalId, new_output: &TxOut) -> Result<()> {
        let update = AtomicalUpdate {
            id: id.clone(),
            info: self.get_atomical_info(id).await?,
            update_type: UpdateType::OwnershipChange,
        };
        
        self.broadcast_tx.send(WsMessage::AtomicalUpdate(update))
            .map_err(|e| anyhow!("Broadcast error: {}", e))?;
            
        Ok(())
    }
    
    /// 通知状态更新
    async fn notify_state_update(&self, id: &AtomicalId) -> Result<()> {
        let update = AtomicalUpdate {
            id: id.clone(),
            info: self.get_atomical_info(id).await?,
            update_type: UpdateType::StateUpdate,
        };
        
        self.broadcast_tx.send(WsMessage::AtomicalUpdate(update))
            .map_err(|e| anyhow!("Broadcast error: {}", e))?;
            
        Ok(())
    }
    
    /// 通知封印状态变更
    async fn notify_seal_status_change(&self, id: &AtomicalId, sealed: bool) -> Result<()> {
        let update = AtomicalUpdate {
            id: id.clone(),
            info: self.get_atomical_info(id).await?,
            update_type: UpdateType::SealStatusChange,
        };
        
        self.broadcast_tx.send(WsMessage::AtomicalUpdate(update))
            .map_err(|e| anyhow!("Broadcast error: {}", e))?;
            
        Ok(())
    }
    
    /// 通知新操作
    async fn notify_operation(&self, operations: &Vec<AtomicalOperation>, txid: String, height: Option<u32>) -> Result<()> {
        let status = match height {
            Some(h) => OperationStatus::Confirmed(h),
            None => OperationStatus::Unconfirmed,
        };
        
        let notification = OperationNotification {
            txid,
            operations: operations.clone(),
            status,
        };
        
        self.broadcast_tx.send(WsMessage::NewOperation(notification))
            .map_err(|e| anyhow!("Broadcast error: {}", e))?;
            
        Ok(())
    }
    
    /// 获取广播发送器
    pub fn broadcast_tx(&self) -> broadcast::Sender<WsMessage> {
        self.broadcast_tx.clone()
    }
    
    /// 获取 Atomical 信息
    async fn get_atomical_info(&self, id: &AtomicalId) -> Result<Value> {
        let outputs = self.outputs.read().await;
        let output = outputs.get(id).unwrap();
        let metadata = self.metadata.read().await.get(id).cloned();
        
        Ok(serde_json::json!({
            "owner": output.owner,
            "metadata": metadata,
            "location": output.location,
            "spent": output.spent,
            "height": output.height,
            "timestamp": output.timestamp,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{Amount, TxOut, Transaction};
    use bitcoin::script::Builder;
    use std::str::FromStr;

    fn create_test_atomical_id() -> AtomicalId {
        let txid = bitcoin::Txid::from_str(
            "1234567890123456789012345678901234567890123456789012345678901234"
        ).unwrap();
        AtomicalId { txid, vout: 0 }
    }

    fn create_test_transaction() -> Transaction {
        Transaction {
            version: bitcoin::transaction::Version(2),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![],
            output: vec![
                TxOut {
                    value: Amount::from_sat(1000).to_sat(),
                    script_pubkey: Builder::new().into_script(),
                }
            ],
        }
    }

    #[tokio::test]
    async fn test_state_creation() -> Result<()> {
        let state = AtomicalsState::new().await?;
        assert!(state.outputs.read().await.is_empty());
        assert!(state.sealed.read().await.is_empty());
        assert!(state.metadata.read().await.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_atomical_existence() -> Result<()> {
        let state = AtomicalsState::new().await?;
        let atomical_id = create_test_atomical_id();

        // 测试不存在的 Atomical
        assert!(!state.exists(&atomical_id).await?);

        // 添加 Atomical
        let output = AtomicalOutput {
            owner: OwnerInfo {
                script_pubkey: vec![],
                value: 1000,
            },
            atomical_id: atomical_id.clone(),
            metadata: None,
            location: OutPoint::new(atomical_id.txid, 0),
            spent: false,
            height: 0,
            timestamp: 0,
        };
        state.outputs.write().await.insert(atomical_id.clone(), output);

        // 测试存在的 Atomical
        assert!(state.exists(&atomical_id).await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_seal_operations() -> Result<()> {
        let state = AtomicalsState::new().await?;
        let atomical_id = create_test_atomical_id();

        // 测试未封印状态
        assert!(!state.is_sealed(&atomical_id).await?);

        // 封印 Atomical
        state.sealed.write().await.insert(atomical_id.clone(), true);

        // 测试已封印状态
        assert!(state.is_sealed(&atomical_id).await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_output_management() -> Result<()> {
        let state = AtomicalsState::new().await?;
        let atomical_id = create_test_atomical_id();

        // 测试获取不存在的输出
        assert!(state.get_output(&atomical_id).await?.is_none());

        // 添加输出
        let output = AtomicalOutput {
            owner: OwnerInfo {
                script_pubkey: vec![1, 2, 3],
                value: 1000,
            },
            atomical_id: atomical_id.clone(),
            metadata: Some(serde_json::json!({
                "name": "Test NFT",
                "description": "Test Description"
            })),
            location: OutPoint::new(atomical_id.txid, 0),
            spent: false,
            height: 100,
            timestamp: 1234567890,
        };
        state.outputs.write().await.insert(atomical_id.clone(), output.clone());

        // 测试获取存在的输出
        let retrieved_output = state.get_output(&atomical_id).await?.unwrap();
        assert_eq!(retrieved_output.owner.value, 1000);
        assert_eq!(retrieved_output.owner.script_pubkey, vec![1, 2, 3]);
        assert_eq!(retrieved_output.height, 100);
        assert_eq!(retrieved_output.timestamp, 1234567890);
        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_management() -> Result<()> {
        let state = AtomicalsState::new().await?;
        let atomical_id = create_test_atomical_id();

        // 测试获取不存在的元数据
        assert!(state.get_metadata(&atomical_id).await?.is_none());

        // 添加带元数据的输出
        let metadata = serde_json::json!({
            "name": "Test NFT",
            "description": "Test Description",
            "attributes": {
                "rarity": "rare",
                "level": 10
            }
        });
        let output = AtomicalOutput {
            owner: OwnerInfo {
                script_pubkey: vec![],
                value: 1000,
            },
            atomical_id: atomical_id.clone(),
            metadata: Some(metadata.clone()),
            location: OutPoint::new(atomical_id.txid, 0),
            spent: false,
            height: 0,
            timestamp: 0,
        };
        state.outputs.write().await.insert(atomical_id.clone(), output);
        state.metadata.write().await.insert(atomical_id.clone(), metadata);

        // 测试获取存在的元数据
        let retrieved_metadata = state.get_metadata(&atomical_id).await?.unwrap();
        assert_eq!(retrieved_metadata, metadata);
        Ok(())
    }

    #[tokio::test]
    async fn test_operation_application() -> Result<()> {
        let mut state = AtomicalsState::new().await?;
        let tx = create_test_transaction();
        let height = 100;
        let timestamp = 1234567890;

        // 测试铸造操作
        let mint_metadata = serde_json::json!({
            "name": "Test NFT",
            "description": "Test Description"
        });
        let mint_op = AtomicalOperation::Mint {
            atomical_type: AtomicalType::NFT,
            metadata: mint_metadata.clone(),
        };
        state.apply_operations(vec![mint_op], &tx, height, timestamp).await?;

        // 验证铸造结果
        let atomical_id = AtomicalId {
            txid: tx.txid(),
            vout: 0,
        };
        let output = state.get_output(&atomical_id).await?.unwrap();
        assert_eq!(output.metadata.unwrap(), mint_metadata);
        assert_eq!(output.height, height);
        assert_eq!(output.timestamp, timestamp);

        // 测试更新操作
        let update_metadata = serde_json::json!({
            "name": "Updated NFT",
            "description": "Updated Description"
        });
        let update_op = AtomicalOperation::Update {
            atomical_id: atomical_id.clone(),
            metadata: update_metadata.clone(),
        };
        state.apply_operations(vec![update_op], &tx, height + 1, timestamp + 3600).await?;

        // 验证更新结果
        let updated_output = state.get_output(&atomical_id).await?.unwrap();
        assert_eq!(updated_output.metadata.unwrap(), update_metadata);

        // 测试封印操作
        let seal_op = AtomicalOperation::Seal {
            atomical_id: atomical_id.clone(),
        };
        state.apply_operations(vec![seal_op], &tx, height + 2, timestamp + 7200).await?;

        // 验证封印结果
        assert!(state.is_sealed(&atomical_id).await?);

        // 测试转移操作
        let transfer_op = AtomicalOperation::Transfer {
            atomical_id: atomical_id.clone(),
            output_index: 0,
        };
        state.apply_operations(vec![transfer_op], &tx, height + 3, timestamp + 10800).await?;

        // 验证转移结果
        let transferred_output = state.get_output(&atomical_id).await?.unwrap();
        assert_eq!(transferred_output.height, height + 3);
        assert_eq!(transferred_output.timestamp, timestamp + 10800);
        Ok(())
    }

    #[tokio::test]
    async fn test_websocket_notifications() -> Result<()> {
        let mut state = AtomicalsState::new().await?;
        let atomical_id = create_test_atomical_id();
        let tx = create_test_transaction();

        // 创建一个接收通道
        let mut rx = state.broadcast_tx.subscribe();

        // 测试所有权变更通知
        let output = TxOut {
            value: Amount::from_sat(2000).to_sat(),
            script_pubkey: Builder::new().into_script(),
        };
        state.notify_ownership_change(&atomical_id, &output).await?;

        // 验证通知
        if let Ok(WsMessage::AtomicalUpdate(update)) = rx.recv().await {
            assert_eq!(update.id, atomical_id);
            assert_eq!(update.update_type, UpdateType::OwnershipChange);
        }

        // 测试状态更新通知
        state.notify_state_update(&atomical_id).await?;

        // 验证通知
        if let Ok(WsMessage::AtomicalUpdate(update)) = rx.recv().await {
            assert_eq!(update.id, atomical_id);
            assert_eq!(update.update_type, UpdateType::StateUpdate);
        }

        // 测试封印状态变更通知
        state.notify_seal_status_change(&atomical_id, true).await?;

        // 验证通知
        if let Ok(WsMessage::AtomicalUpdate(update)) = rx.recv().await {
            assert_eq!(update.id, atomical_id);
            assert_eq!(update.update_type, UpdateType::SealStatusChange);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_operations() -> Result<()> {
        let state = Arc::new(AtomicalsState::new().await?);
        let atomical_id = create_test_atomical_id();
        let tx = create_test_transaction();

        // 创建多个并发任务
        let mut handles = vec![];
        for i in 0..10 {
            let state = state.clone();
            let atomical_id = atomical_id.clone();
            let tx = tx.clone();
            
            let handle = tokio::spawn(async move {
                // 执行并发操作
                let metadata = serde_json::json!({
                    "name": format!("Test NFT {}", i),
                    "description": format!("Test Description {}", i)
                });
                
                let mint_op = AtomicalOperation::Mint {
                    atomical_type: AtomicalType::NFT,
                    metadata: metadata.clone(),
                };
                
                state.apply_operations(vec![mint_op], &tx, 100 + i, 1234567890 + i).await.unwrap();
            });
            
            handles.push(handle);
        }

        // 等待所有任务完成
        for handle in handles {
            handle.await.unwrap();
        }

        Ok(())
    }
}

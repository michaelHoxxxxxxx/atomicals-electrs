use std::collections::HashMap;
use tokio::sync::{RwLock, broadcast};
use serde::{Serialize, Deserialize};
use anyhow::Result;
use bitcoin::{Transaction, TxOut, OutPoint, Address};
use serde_json::Value;
use super::protocol::{AtomicalId, AtomicalOperation, AtomicalType};
use super::websocket::{WsMessage, AtomicalUpdate, UpdateType, OperationNotification, OperationStatus};

#[derive(Debug, Clone)]
pub struct AtomicalOutput {
    pub txid: bitcoin::Txid,
    pub vout: u32,
    pub output: TxOut,
    pub metadata: Value,
    pub height: u32,
    pub timestamp: u64,
    pub atomical_type: AtomicalType,
    pub sealed: bool,
}

#[derive(Debug)]
pub struct AtomicalsState {
    outputs: RwLock<HashMap<AtomicalId, AtomicalOutput>>,
    sealed: RwLock<HashMap<AtomicalId, bool>>,
    metadata: RwLock<HashMap<AtomicalId, Value>>,
    broadcast_tx: broadcast::Sender<WsMessage>,
    network: bitcoin::Network,
}

impl AtomicalsState {
    pub async fn new(network: bitcoin::Network) -> Result<Self> {
        let (broadcast_tx, _) = broadcast::channel(1024);
        Ok(Self {
            outputs: RwLock::new(HashMap::new()),
            sealed: RwLock::new(HashMap::new()),
            metadata: RwLock::new(HashMap::new()),
            broadcast_tx,
            network,
        })
    }

    pub async fn get_output(&self, atomical_id: &AtomicalId) -> Result<Option<AtomicalOutput>> {
        let outputs = self.outputs.read().await;
        Ok(outputs.get(atomical_id).cloned())
    }

    pub async fn get_metadata(&self, atomical_id: &AtomicalId) -> Result<Option<Value>> {
        let metadata = self.metadata.read().await;
        Ok(metadata.get(atomical_id).cloned())
    }

    pub async fn get_atomical_info(&self, id: &AtomicalId) -> Result<AtomicalInfo> {
        let outputs = self.outputs.read().await;
        let output = outputs.get(id).ok_or_else(|| anyhow::anyhow!("Atomical not found"))?;
        
        let metadata = self.metadata.read().await;
        let metadata_value = metadata.get(id).cloned();
        
        let sealed = self.sealed.read().await;
        let is_sealed = sealed.get(id).cloned().unwrap_or(false);
        
        Ok(AtomicalInfo {
            id: id.clone(),
            owner: Address::from_script(&output.output.script_pubkey, self.network).ok(),
            metadata: metadata_value,
            state: None,
            atomical_type: output.atomical_type.clone(),
            value: output.output.value.to_sat(),
            created_height: output.height,
            created_timestamp: output.timestamp,
            sealed: is_sealed,
        })
    }

    pub async fn apply_operations(&mut self, operations: Vec<AtomicalOperation>, tx: &Transaction, height: u32, timestamp: u64) -> Result<()> {
        let operations_clone = operations.clone();
        for operation in operations {
            match operation {
                AtomicalOperation::Mint { atomical_type, metadata } => {
                    let atomical_id = AtomicalId {
                        txid: tx.compute_txid(),
                        vout: 0,
                    };

                    let output = AtomicalOutput {
                        txid: tx.compute_txid(),
                        vout: 0,
                        output: tx.output[0].clone(),
                        metadata: metadata.clone().map_or(serde_json::json!({}), |m| m),
                        height,
                        timestamp,
                        atomical_type: atomical_type.clone(),
                        sealed: false,
                    };

                    self.outputs.write().await.insert(atomical_id.clone(), output);
                    if let Some(m) = metadata {
                        self.metadata.write().await.insert(atomical_id.clone(), m);
                    }
                    self.notify_state_update(&atomical_id).await?;
                }
                AtomicalOperation::Update { atomical_id, metadata } => {
                    if let Some(output) = self.outputs.write().await.get_mut(&atomical_id) {
                        let m = metadata.clone().map_or(serde_json::json!({}), |m| m);
                        output.metadata = m.clone();
                        self.metadata.write().await.insert(atomical_id.clone(), m);
                    }
                    
                    self.notify_state_update(&atomical_id).await?;
                }
                AtomicalOperation::Seal { atomical_id } => {
                    if let Some(output) = self.outputs.write().await.get_mut(&atomical_id) {
                        output.sealed = true;
                    }
                    self.sealed.write().await.insert(atomical_id.clone(), true);
                    
                    self.notify_seal_status_change(&atomical_id, true).await?;
                }
                AtomicalOperation::Transfer { atomical_id, output_index } => {
                    if let Some(output) = self.outputs.write().await.get_mut(&atomical_id) {
                        output.output = tx.output[output_index as usize].clone();
                        output.height = height;
                        output.timestamp = timestamp;
                    }
                    
                    self.notify_ownership_change(&atomical_id, &tx.output[output_index as usize]).await?;
                }
            }
        }

        self.notify_operation(&operations_clone, tx.compute_txid().to_string(), Some(height)).await?;
        Ok(())
    }

    pub fn broadcast_tx(&self) -> broadcast::Sender<WsMessage> {
        self.broadcast_tx.clone()
    }

    async fn notify_state_update(&self, id: &AtomicalId) -> Result<()> {
        let update = AtomicalUpdate {
            id: id.clone(),
            info: self.get_atomical_info(id).await?.into(),
            update_type: UpdateType::StateUpdate,
        };
        
        let _ = self.broadcast_tx.send(WsMessage::AtomicalUpdate(update));
        Ok(())
    }

    async fn notify_seal_status_change(&self, id: &AtomicalId, sealed: bool) -> Result<()> {
        let update = AtomicalUpdate {
            id: id.clone(),
            info: self.get_atomical_info(id).await?.into(),
            update_type: UpdateType::SealStatusChange,
        };
        
        let _ = self.broadcast_tx.send(WsMessage::AtomicalUpdate(update));
        Ok(())
    }

    async fn notify_ownership_change(&self, id: &AtomicalId, new_output: &TxOut) -> Result<()> {
        let update = AtomicalUpdate {
            id: id.clone(),
            info: self.get_atomical_info(id).await?.into(),
            update_type: UpdateType::OwnershipChange,
        };
        
        let _ = self.broadcast_tx.send(WsMessage::AtomicalUpdate(update));
        Ok(())
    }

    async fn notify_operation(&self, operations: &Vec<AtomicalOperation>, txid: String, height: Option<u32>) -> Result<()> {
        let status = match height {
            Some(h) => OperationStatus::Confirmed(h),
            None => OperationStatus::Unconfirmed,
        };

        let notification = OperationNotification {
            txid,
            operation: operations[0].clone(), // 假设只处理第一个操作
            status,
        };
        
        let _ = self.broadcast_tx.send(WsMessage::NewOperation(notification));
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomicalInfo {
    pub id: AtomicalId,
    pub owner: Option<Address>,
    pub metadata: Option<Value>,
    pub state: Option<String>,
    pub atomical_type: AtomicalType,
    pub value: u64,
    pub created_height: u32,
    pub created_timestamp: u64,
    pub sealed: bool,
}

impl From<AtomicalInfo> for super::rpc::AtomicalInfo {
    fn from(info: AtomicalInfo) -> Self {
        super::rpc::AtomicalInfo {
            id: info.id,
            owner: info.owner,
            metadata: info.metadata,
            state: info.state,
            atomical_type: info.atomical_type,
            value: info.value,
            created_height: info.created_height,
            created_timestamp: info.created_timestamp,
            sealed: info.sealed,
        }
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
        let state = AtomicalsState::new(bitcoin::Network::Testnet).await?;
        assert!(state.outputs.read().await.is_empty());
        assert!(state.sealed.read().await.is_empty());
        assert!(state.metadata.read().await.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_atomical_existence() -> Result<()> {
        let state = AtomicalsState::new(bitcoin::Network::Testnet).await?;
        let atomical_id = create_test_atomical_id();

        // 测试不存在的 Atomical
        assert!(!state.get_output(&atomical_id).await?.is_some());

        // 添加 Atomical
        let output = AtomicalOutput {
            txid: atomical_id.txid,
            vout: atomical_id.vout,
            output: TxOut {
                value: Amount::from_sat(1000).to_sat(),
                script_pubkey: Builder::new().into_script(),
            },
            metadata: serde_json::json!({}),
            height: 0,
            timestamp: 0,
            atomical_type: AtomicalType::NFT,
            sealed: false,
        };
        state.outputs.write().await.insert(atomical_id.clone(), output);

        // 测试存在的 Atomical
        assert!(state.get_output(&atomical_id).await?.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn test_seal_operations() -> Result<()> {
        let state = AtomicalsState::new(bitcoin::Network::Testnet).await?;
        let atomical_id = create_test_atomical_id();

        // 测试未封印状态
        assert!(!state.sealed.read().await.contains_key(&atomical_id));

        // 封印 Atomical
        state.sealed.write().await.insert(atomical_id.clone(), true);

        // 测试已封印状态
        assert!(state.sealed.read().await.contains_key(&atomical_id));
        Ok(())
    }

    #[tokio::test]
    async fn test_output_management() -> Result<()> {
        let state = AtomicalsState::new(bitcoin::Network::Testnet).await?;
        let atomical_id = create_test_atomical_id();

        // 测试获取不存在的输出
        assert!(!state.get_output(&atomical_id).await?.is_some());

        // 添加输出
        let output = AtomicalOutput {
            txid: atomical_id.txid,
            vout: atomical_id.vout,
            output: TxOut {
                value: Amount::from_sat(1000).to_sat(),
                script_pubkey: Builder::new().into_script(),
            },
            metadata: serde_json::json!({
                "name": "Test NFT",
                "description": "Test Description"
            }),
            height: 100,
            timestamp: 1234567890,
            atomical_type: AtomicalType::NFT,
            sealed: false,
        };
        state.outputs.write().await.insert(atomical_id.clone(), output.clone());

        // 测试获取存在的输出
        let retrieved_output = state.get_output(&atomical_id).await?.unwrap();
        assert_eq!(retrieved_output.output.value, 1000);
        assert_eq!(retrieved_output.height, 100);
        assert_eq!(retrieved_output.timestamp, 1234567890);
        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_management() -> Result<()> {
        let state = AtomicalsState::new(bitcoin::Network::Testnet).await?;
        let atomical_id = create_test_atomical_id();

        // 测试获取不存在的元数据
        assert!(!state.get_metadata(&atomical_id).await?.is_some());

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
            txid: atomical_id.txid,
            vout: atomical_id.vout,
            output: TxOut {
                value: Amount::from_sat(1000).to_sat(),
                script_pubkey: Builder::new().into_script(),
            },
            metadata: metadata.clone(),
            height: 0,
            timestamp: 0,
            atomical_type: AtomicalType::NFT,
            sealed: false,
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
        let mut state = AtomicalsState::new(bitcoin::Network::Testnet).await?;
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
            txid: tx.compute_txid(),
            vout: 0,
        };
        let output = state.get_output(&atomical_id).await?.unwrap();
        assert_eq!(output.metadata, mint_metadata);
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
        assert_eq!(updated_output.metadata, update_metadata);

        // 测试封印操作
        let seal_op = AtomicalOperation::Seal {
            atomical_id: atomical_id.clone(),
        };
        state.apply_operations(vec![seal_op], &tx, height + 2, timestamp + 7200).await?;

        // 验证封印结果
        assert!(state.sealed.read().await.contains_key(&atomical_id));

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
        let mut state = AtomicalsState::new(bitcoin::Network::Testnet).await?;
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
        let state = Arc::new(AtomicalsState::new(bitcoin::Network::Testnet).await?);
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

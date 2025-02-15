use std::collections::HashMap;
use anyhow::{anyhow, Result};
use bitcoin::{Transaction, OutPoint};
use serde::{Serialize, Deserialize};

use super::protocol::{AtomicalId, AtomicalOperation, AtomicalType};
use super::tx_parser;
use dashmap::DashMap;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::broadcast;

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
    pub metadata: Option<serde_json::Value>,
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
    outputs: DashMap<AtomicalId, AtomicalOutput>,
    /// 已封印的 Atomicals
    sealed: DashMap<AtomicalId, bool>,
    /// WebSocket 广播通道
    broadcast_tx: broadcast::Sender<WsMessage>,
}

impl AtomicalsState {
    /// 创建新的状态
    pub fn new() -> Result<Self> {
        let (broadcast_tx, _) = broadcast::channel(1024);
        
        Ok(Self {
            outputs: DashMap::new(),
            sealed: DashMap::new(),
            broadcast_tx,
        })
    }

    /// 检查 Atomical 是否存在
    pub fn exists(&self, atomical_id: &AtomicalId) -> Result<bool> {
        Ok(self.outputs.contains_key(atomical_id))
    }

    /// 检查 Atomical 是否已被封印
    pub fn is_sealed(&self, atomical_id: &AtomicalId) -> Result<bool> {
        Ok(self.sealed.get(atomical_id).copied().unwrap_or(false))
    }

    /// 获取 Atomical 输出
    pub fn get_output(&self, atomical_id: &AtomicalId) -> Result<Option<AtomicalOutput>> {
        Ok(self.outputs.get(atomical_id).cloned())
    }

    /// 获取 Atomical 元数据
    pub fn get_metadata(&self, atomical_id: &AtomicalId) -> Result<Option<serde_json::Value>> {
        Ok(self.outputs.get(atomical_id)
            .and_then(|output| output.metadata.clone()))
    }

    /// 获取交易中的 Atomicals 操作
    pub fn get_operations(&self, tx: &Transaction) -> Result<Vec<AtomicalOperation>> {
        tx_parser::parse_transaction(tx)
    }

    /// 应用操作
    pub fn apply_operations(&mut self, operations: Vec<AtomicalOperation>, tx: &Transaction, height: u32, timestamp: u64) -> Result<()> {
        for operation in operations {
            match operation {
                AtomicalOperation::Mint { atomical_type: _, metadata } => {
                    // 创建新的 Atomical ID
                    let atomical_id = AtomicalId {
                        txid: tx.txid(),
                        vout: 0,
                    };

                    // 创建新的输出
                    let output = AtomicalOutput {
                        owner: OwnerInfo {
                            script_pubkey: tx.output[0].script_pubkey.to_bytes(),
                            value: tx.output[0].value,
                        },
                        atomical_id: atomical_id.clone(),
                        metadata: Some(metadata),
                        location: OutPoint::new(tx.txid(), 0),
                        spent: false,
                        height,
                        timestamp,
                    };

                    // 保存输出
                    self.outputs.insert(atomical_id, output);
                    
                    // 发送状态更新通知
                    self.notify_state_update(&atomical_id)?;
                }
                AtomicalOperation::Update { atomical_id, metadata } => {
                    if let Some(output) = self.outputs.get_mut(&atomical_id) {
                        output.metadata = Some(metadata);
                    }
                    
                    // 发送状态更新通知
                    self.notify_state_update(&atomical_id)?;
                }
                AtomicalOperation::Seal { atomical_id } => {
                    self.sealed.insert(atomical_id, true);
                    
                    // 发送封印状态变更通知
                    self.notify_seal_status_change(&atomical_id, true)?;
                }
                AtomicalOperation::Transfer { atomical_id, output_index } => {
                    if let Some(output) = self.outputs.get_mut(&atomical_id) {
                        output.owner = OwnerInfo {
                            script_pubkey: tx.output[output_index as usize].script_pubkey.to_bytes(),
                            value: tx.output[output_index as usize].value,
                        };
                        output.location = OutPoint::new(tx.txid(), output_index);
                        output.height = height;
                        output.timestamp = timestamp;
                    }
                    
                    // 发送所有权变更通知
                    self.notify_ownership_change(&atomical_id, &tx.output[output_index as usize])?;
                }
            }
        }
        
        // 发送操作通知
        self.notify_operation(&operations, tx.txid().to_string(), Some(height))?;
        
        Ok(())
    }
    
    // WebSocket 通知方法
    
    /// 通知所有权变更
    fn notify_ownership_change(&self, id: &AtomicalId, new_output: &TxOut) -> Result<()> {
        let update = AtomicalUpdate {
            id: id.clone(),
            info: self.get_atomical_info(id)?,
            update_type: UpdateType::OwnershipChange,
        };
        
        self.broadcast_tx.send(WsMessage::AtomicalUpdate(update))
            .map_err(|e| anyhow!("Broadcast error: {}", e))?;
            
        Ok(())
    }
    
    /// 通知状态更新
    fn notify_state_update(&self, id: &AtomicalId) -> Result<()> {
        let update = AtomicalUpdate {
            id: id.clone(),
            info: self.get_atomical_info(id)?,
            update_type: UpdateType::StateUpdate,
        };
        
        self.broadcast_tx.send(WsMessage::AtomicalUpdate(update))
            .map_err(|e| anyhow!("Broadcast error: {}", e))?;
            
        Ok(())
    }
    
    /// 通知封印状态变更
    fn notify_seal_status_change(&self, id: &AtomicalId, sealed: bool) -> Result<()> {
        let update = AtomicalUpdate {
            id: id.clone(),
            info: self.get_atomical_info(id)?,
            update_type: UpdateType::SealStatusChange,
        };
        
        self.broadcast_tx.send(WsMessage::AtomicalUpdate(update))
            .map_err(|e| anyhow!("Broadcast error: {}", e))?;
            
        Ok(())
    }
    
    /// 通知新操作
    fn notify_operation(&self, operations: &Vec<AtomicalOperation>, txid: String, height: Option<u32>) -> Result<()> {
        let status = match height {
            Some(h) => OperationStatus::Confirmed(h),
            None => OperationStatus::Unconfirmed,
        };
        
        let notification = OperationNotification {
            txid,
            operation: operations.clone(),
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
    fn get_atomical_info(&self, id: &AtomicalId) -> Result<serde_json::Value> {
        let output = self.outputs.get(id).unwrap();
        let info = serde_json::json!({
            "owner": output.owner,
            "metadata": output.metadata,
            "location": output.location,
            "spent": output.spent,
            "height": output.height,
            "timestamp": output.timestamp,
        });
        
        Ok(info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn create_test_atomical_id() -> AtomicalId {
        let txid = bitcoin::Txid::from_str(
            "1234567890123456789012345678901234567890123456789012345678901234"
        ).unwrap();
        AtomicalId { txid, vout: 0 }
    }

    #[test]
    fn test_state() -> Result<()> {
        let mut state = AtomicalsState::new();
        let atomical_id = create_test_atomical_id();

        // 测试存在性检查
        assert!(!state.exists(&atomical_id)?);

        // 创建测试输出
        let output = AtomicalOutput {
            owner: OwnerInfo {
                script_pubkey: vec![],
                value: 1000,
            },
            atomical_id: atomical_id.clone(),
            metadata: Some(serde_json::json!({
                "name": "Test NFT",
            })),
            location: OutPoint::new(atomical_id.txid, 0),
            spent: false,
            height: 100,
            timestamp: 1644900000,
        };

        // 添加输出
        state.outputs.insert(atomical_id.clone(), output);

        // 测试存在性和封印状态
        assert!(state.exists(&atomical_id)?);
        assert!(!state.is_sealed(&atomical_id)?);

        // 测试获取输出和元数据
        let retrieved = state.get_output(&atomical_id)?.unwrap();
        assert_eq!(retrieved.owner.value, 1000);

        let metadata = state.get_metadata(&atomical_id)?.unwrap();
        assert_eq!(metadata["name"], "Test NFT");

        // 测试封印
        state.sealed.insert(atomical_id.clone(), true);
        assert!(state.is_sealed(&atomical_id)?);

        Ok(())
    }
}

use anyhow::{anyhow, Result};
use bitcoin::Transaction;
use serde_json::Value;

use super::protocol::{AtomicalId, AtomicalOperation, AtomicalType};
use super::state::AtomicalsState;

/// Atomicals 验证器
pub struct AtomicalsValidator {
    state: AtomicalsState,
}

impl AtomicalsValidator {
    /// 创建新的验证器
    pub fn new(state: AtomicalsState) -> Self {
        Self { state }
    }

    /// 验证 Atomicals 操作
    pub fn validate_operation(&self, tx: &Transaction, operation: &AtomicalOperation) -> Result<()> {
        validate_operation(operation, tx, &self.state)
    }
}

/// 验证 Atomicals 操作
pub fn validate_operation(
    operation: &AtomicalOperation,
    tx: &Transaction,
    state: &AtomicalsState,
) -> Result<()> {
    match operation {
        AtomicalOperation::Mint { atomical_type, metadata } => {
            validate_mint(atomical_type, metadata)
        }
        AtomicalOperation::Update { atomical_id, metadata } => {
            validate_update(state, tx, atomical_id)
        }
        AtomicalOperation::Seal { atomical_id } => {
            validate_seal(state, tx, atomical_id)
        }
        AtomicalOperation::Transfer { atomical_id, output_index } => {
            // 检查 Atomical 是否存在
            if !state.exists(atomical_id)? {
                return Err(anyhow!("Atomical does not exist"));
            }

            // 检查输出索引是否有效
            if output_index as usize >= tx.output.len() {
                return Err(anyhow!("Invalid output index"));
            }

            // 检查是否已被封印
            if state.is_sealed(atomical_id)? {
                return Err(anyhow!("Cannot transfer sealed Atomical"));
            }

            Ok(())
        }
    }
}

/// 验证铸造操作
fn validate_mint(atomical_type: &AtomicalType, metadata: &Value) -> Result<()> {
    // 验证元数据
    if !metadata.is_object() {
        return Err(anyhow!("Metadata must be a JSON object"));
    }
    
    // 验证类型特定的规则
    match atomical_type {
        AtomicalType::NFT => {
            // 验证 NFT 特定的规则
            if !metadata.get("name").and_then(Value::as_str).is_some() {
                return Err(anyhow!("NFT metadata must contain a name field"));
            }
        }
        AtomicalType::FT => {
            // 验证 FT 特定的规则
            if !metadata.get("ticker").and_then(Value::as_str).is_some() {
                return Err(anyhow!("FT metadata must contain a ticker field"));
            }
            if !metadata.get("max_supply").and_then(Value::as_u64).is_some() {
                return Err(anyhow!("FT metadata must contain a max_supply field"));
            }
        }
        AtomicalType::DID => {
            // 验证 DID 特定的规则
            if !metadata.get("did").and_then(Value::as_str).is_some() {
                return Err(anyhow!("DID metadata must contain a did field"));
            }
        }
        AtomicalType::Container => {
            // 验证 Container 特定的规则
            if !metadata.get("container_name").and_then(Value::as_str).is_some() {
                return Err(anyhow!("Container metadata must contain a container_name field"));
            }
        }
        AtomicalType::Realm => {
            // 验证 Realm 特定的规则
            if !metadata.get("realm_name").and_then(Value::as_str).is_some() {
                return Err(anyhow!("Realm metadata must contain a realm_name field"));
            }
        }
    }
    
    Ok(())
}

/// 验证更新操作
fn validate_update(state: &AtomicalsState, tx: &Transaction, atomical_id: &AtomicalId) -> Result<()> {
    // 检查 Atomical 是否存在
    if !state.exists(atomical_id)? {
        return Err(anyhow!("Atomical does not exist"));
    }
    
    // 检查 Atomical 是否已被封印
    if state.is_sealed(atomical_id)? {
        return Err(anyhow!("Cannot update sealed Atomical"));
    }
    
    Ok(())
}

/// 验证封印操作
fn validate_seal(state: &AtomicalsState, tx: &Transaction, atomical_id: &AtomicalId) -> Result<()> {
    // 检查 Atomical 是否存在
    if !state.exists(atomical_id)? {
        return Err(anyhow!("Atomical does not exist"));
    }
    
    // 检查 Atomical 是否已被封印
    if state.is_sealed(atomical_id)? {
        return Err(anyhow!("Atomical is already sealed"));
    }
    
    Ok(())
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
    fn test_validate_mint() {
        // 测试 NFT 铸造
        let nft_metadata = serde_json::json!({
            "name": "Test NFT",
            "description": "Test Description",
        });
        assert!(validate_mint(&AtomicalType::NFT, &nft_metadata).is_ok());
        
        // 测试 FT 铸造
        let ft_metadata = serde_json::json!({
            "ticker": "TEST",
            "max_supply": 1000000,
            "description": "Test Token",
        });
        assert!(validate_mint(&AtomicalType::FT, &ft_metadata).is_ok());
        
        // 测试无效的 NFT 元数据
        let invalid_nft_metadata = serde_json::json!({
            "description": "Missing name field",
        });
        assert!(validate_mint(&AtomicalType::NFT, &invalid_nft_metadata).is_err());
        
        // 测试无效的 FT 元数据
        let invalid_ft_metadata = serde_json::json!({
            "description": "Missing ticker and max_supply fields",
        });
        assert!(validate_mint(&AtomicalType::FT, &invalid_ft_metadata).is_err());
    }
}

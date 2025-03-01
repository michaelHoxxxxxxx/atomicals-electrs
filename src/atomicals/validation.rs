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
        AtomicalOperation::Mint { id: _, atomical_type, metadata } => {
            validate_mint(atomical_type, metadata)
        }
        AtomicalOperation::Update { id, metadata: _  } => {
            validate_update(state, tx, id)
        }
        AtomicalOperation::Seal { id } => {
            validate_seal(state, tx, id)
        }
        AtomicalOperation::Transfer { id, output_index } => {
            // 检查 Atomical 是否存在
            if !state.exists(id)? {
                return Err(anyhow!("Atomical does not exist"));
            }

            // 检查输出索引是否有效
            if *output_index as usize >= tx.output.len() {
                return Err(anyhow!("Invalid output index"));
            }

            // 检查是否已被封印
            if state.is_sealed(id)? {
                return Err(anyhow!("Cannot transfer sealed Atomical"));
            }

            Ok(())
        }
    }
}

/// 验证铸造操作
fn validate_mint(atomical_type: &AtomicalType, metadata: &Option<Value>) -> Result<()> {
    // 检查元数据是否存在
    let metadata = metadata.as_ref().ok_or_else(|| anyhow!("Metadata is required"))?;
    
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
        AtomicalType::Unknown => {
            // 对于未知类型，我们不进行特定验证
            return Err(anyhow!("Unknown atomical type"));
        }
    }
    
    Ok(())
}

/// 验证更新操作
fn validate_update(state: &AtomicalsState, _tx: &Transaction, id: &AtomicalId) -> Result<()> {
    // 检查 Atomical 是否存在
    if !state.exists(id)? {
        return Err(anyhow!("Atomical does not exist"));
    }
    
    // 检查 Atomical 是否已被封印
    if state.is_sealed(id)? {
        return Err(anyhow!("Cannot update sealed Atomical"));
    }
    
    Ok(())
}

/// 验证封印操作
fn validate_seal(state: &AtomicalsState, _tx: &Transaction, id: &AtomicalId) -> Result<()> {
    // 检查 Atomical 是否存在
    if !state.exists(id)? {
        return Err(anyhow!("Atomical does not exist"));
    }
    
    // 检查 Atomical 是否已被封印
    if state.is_sealed(id)? {
        return Err(anyhow!("Atomical is already sealed"));
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use bitcoin::{Amount, TxOut, Transaction};
    use bitcoin::script::Builder;
    
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

    fn create_test_state() -> AtomicalsState {
        // 创建一个模拟的状态对象
        AtomicalsState::new()
    }
    
    #[test]
    fn test_validate_mint() {
        // 测试 NFT 铸造
        let nft_metadata = serde_json::json!({
            "name": "Test NFT",
            "description": "Test Description",
        });
        assert!(validate_mint(&AtomicalType::NFT, &Some(nft_metadata)).is_ok());
        
        // 测试 FT 铸造
        let ft_metadata = serde_json::json!({
            "ticker": "TEST",
            "max_supply": 1000000,
            "description": "Test Token",
        });
        assert!(validate_mint(&AtomicalType::FT, &Some(ft_metadata)).is_ok());
        
        // 测试 DID 铸造
        let did_metadata = serde_json::json!({
            "did": "did:example:123",
            "description": "Test DID",
        });
        assert!(validate_mint(&AtomicalType::DID, &Some(did_metadata)).is_ok());

        // 测试 Container 铸造
        let container_metadata = serde_json::json!({
            "container_name": "test_container",
            "description": "Test Container",
        });
        assert!(validate_mint(&AtomicalType::Container, &Some(container_metadata)).is_ok());

        // 测试 Realm 铸造
        let realm_metadata = serde_json::json!({
            "realm_name": "test_realm",
            "description": "Test Realm",
        });
        assert!(validate_mint(&AtomicalType::Realm, &Some(realm_metadata)).is_ok());
        
        // 测试无效的 NFT 元数据
        let invalid_nft_metadata = serde_json::json!({
            "description": "Missing name field",
        });
        assert!(validate_mint(&AtomicalType::NFT, &Some(invalid_nft_metadata)).is_err());
        
        // 测试无效的 FT 元数据
        let invalid_ft_metadata = serde_json::json!({
            "description": "Missing ticker and max_supply fields",
        });
        assert!(validate_mint(&AtomicalType::FT, &Some(invalid_ft_metadata)).is_err());

        // 测试无效的 DID 元数据
        let invalid_did_metadata = serde_json::json!({
            "description": "Missing did field",
        });
        assert!(validate_mint(&AtomicalType::DID, &Some(invalid_did_metadata)).is_err());

        // 测试无效的 Container 元数据
        let invalid_container_metadata = serde_json::json!({
            "description": "Missing container_name field",
        });
        assert!(validate_mint(&AtomicalType::Container, &Some(invalid_container_metadata)).is_err());

        // 测试无效的 Realm 元数据
        let invalid_realm_metadata = serde_json::json!({
            "description": "Missing realm_name field",
        });
        assert!(validate_mint(&AtomicalType::Realm, &Some(invalid_realm_metadata)).is_err());

        // 测试非对象元数据
        let invalid_metadata = serde_json::json!("not an object");
        assert!(validate_mint(&AtomicalType::NFT, &Some(invalid_metadata)).is_err());

        // 测试缺失元数据
        assert!(validate_mint(&AtomicalType::NFT, &None).is_err());

        // 测试未知类型
        assert!(validate_mint(&AtomicalType::Unknown, &Some(nft_metadata)).is_err());
    }

    #[test]
    fn test_validate_update() {
        let state = create_test_state();
        let tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();

        // 测试更新不存在的 Atomical
        assert!(validate_update(&state, &tx, &atomical_id).is_err());

        // 模拟 Atomical 存在
        state.create(&atomical_id, AtomicalType::NFT).unwrap();
        assert!(validate_update(&state, &tx, &atomical_id).is_ok());

        // 测试更新已封印的 Atomical
        state.seal(&atomical_id).unwrap();
        assert!(validate_update(&state, &tx, &atomical_id).is_err());
    }

    #[test]
    fn test_validate_seal() {
        let state = create_test_state();
        let tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();

        // 测试封印不存在的 Atomical
        assert!(validate_seal(&state, &tx, &atomical_id).is_err());

        // 模拟 Atomical 存在
        state.create(&atomical_id, AtomicalType::NFT).unwrap();
        assert!(validate_seal(&state, &tx, &atomical_id).is_ok());

        // 测试重复封印
        state.seal(&atomical_id).unwrap();
        assert!(validate_seal(&state, &tx, &atomical_id).is_err());
    }

    #[test]
    fn test_validate_transfer() {
        let state = create_test_state();
        let mut tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();

        // 测试转移不存在的 Atomical
        assert!(validate_operation(
            &AtomicalOperation::Transfer { id: atomical_id, output_index: 0 },
            &tx,
            &state
        ).is_err());

        // 模拟 Atomical 存在
        state.create(&atomical_id, AtomicalType::NFT).unwrap();
        assert!(validate_operation(
            &AtomicalOperation::Transfer { id: atomical_id, output_index: 0 },
            &tx,
            &state
        ).is_ok());

        // 测试无效的输出索引
        assert!(validate_operation(
            &AtomicalOperation::Transfer { id: atomical_id, output_index: 1 },
            &tx,
            &state
        ).is_err());

        // 测试转移已封印的 Atomical
        state.seal(&atomical_id).unwrap();
        assert!(validate_operation(
            &AtomicalOperation::Transfer { id: atomical_id, output_index: 0 },
            &tx,
            &state
        ).is_err());
    }

    #[test]
    fn test_validator() {
        let state = create_test_state();
        let validator = AtomicalsValidator::new(state);
        let tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();

        // 测试铸造操作
        let mint_op = AtomicalOperation::Mint {
            id: atomical_id,
            atomical_type: AtomicalType::NFT,
            metadata: serde_json::json!({
                "name": "Test NFT",
                "description": "Test Description",
            }),
        };
        assert!(validator.validate_operation(&tx, &mint_op).is_ok());

        // 测试无效的铸造操作
        let invalid_mint_op = AtomicalOperation::Mint {
            id: atomical_id,
            atomical_type: AtomicalType::NFT,
            metadata: serde_json::json!({
                "description": "Missing name field",
            }),
        };
        assert!(validator.validate_operation(&tx, &invalid_mint_op).is_err());

        // 测试转移操作
        validator.state.create(&atomical_id, AtomicalType::NFT).unwrap();
        let transfer_op = AtomicalOperation::Transfer {
            id: atomical_id,
            output_index: 0,
        };
        assert!(validator.validate_operation(&tx, &transfer_op).is_ok());
    }
}

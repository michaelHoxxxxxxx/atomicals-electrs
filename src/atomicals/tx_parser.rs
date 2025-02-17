use anyhow::Result;
use bitcoin::{Transaction, TxOut};
use serde_json::Value;

use super::protocol::{AtomicalId, AtomicalOperation, AtomicalType};

const ATOMICALS_PREFIX: &[u8] = b"atom";

/// 解析输出脚本中的 Atomicals 操作
fn parse_output(output: &TxOut, vout: u32) -> Option<AtomicalOperation> {
    let script = output.script_pubkey.as_bytes();
    
    // 检查是否是 Atomicals 操作
    if script.len() < ATOMICALS_PREFIX.len() || &script[..ATOMICALS_PREFIX.len()] != ATOMICALS_PREFIX {
        return None;
    }

    // 解析操作类型
    let op_type = std::str::from_utf8(&script[ATOMICALS_PREFIX.len()..]).ok()?;
    
    match op_type {
        "mint" => {
            // 解析铸造操作
            let metadata = serde_json::from_slice(&script[ATOMICALS_PREFIX.len() + 4..]).ok()?;
            Some(AtomicalOperation::Mint {
                atomical_type: AtomicalType::NFT,
                metadata,
            })
        }
        "update" => {
            // 解析更新操作
            let atomical_id = AtomicalId {
                txid: bitcoin::Txid::from_slice(&script[ATOMICALS_PREFIX.len() + 6..38]).ok()?,
                vout: u32::from_be_bytes(script[38..42].try_into().ok()?),
            };
            let metadata = serde_json::from_slice(&script[42..]).ok()?;
            Some(AtomicalOperation::Update {
                atomical_id,
                metadata,
            })
        }
        "seal" => {
            // 解析封印操作
            let atomical_id = AtomicalId {
                txid: bitcoin::Txid::from_slice(&script[ATOMICALS_PREFIX.len() + 4..36]).ok()?,
                vout: u32::from_be_bytes(script[36..40].try_into().ok()?),
            };
            Some(AtomicalOperation::Seal {
                atomical_id,
            })
        }
        "transfer" => {
            // 解析转移操作
            let atomical_id = AtomicalId {
                txid: bitcoin::Txid::from_slice(&script[ATOMICALS_PREFIX.len() + 8..40]).ok()?,
                vout: u32::from_be_bytes(script[40..44].try_into().ok()?),
            };
            let output_index = u32::from_be_bytes(script[44..48].try_into().ok()?);
            Some(AtomicalOperation::Transfer {
                atomical_id,
                output_index,
            })
        }
        _ => None,
    }
}

/// 解析交易中的 Atomicals 操作
pub fn parse_transaction(tx: &Transaction) -> Result<Vec<AtomicalOperation>> {
    let mut operations = Vec::new();
    
    // 遍历所有输出
    for (vout, output) in tx.output.iter().enumerate() {
        if let Some(operation) = parse_output(output, vout as u32) {
            operations.push(operation);
        }
    }
    
    Ok(operations)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::Amount;
    use bitcoin::script::Builder;
    use std::str::FromStr;

    fn create_test_transaction() -> Transaction {
        Transaction {
            version: bitcoin::transaction::Version(2),
            lock_time: bitcoin::absolute::LockTime::ZERO,
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
    fn test_parse_mint() {
        let mut tx = create_test_transaction();
        
        // 创建铸造操作的输出
        let metadata = serde_json::json!({
            "name": "Test NFT",
            "description": "A test NFT"
        });
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.extend_from_slice(b"mint");
        script_data.extend_from_slice(&serde_json::to_vec(&metadata).unwrap());
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 解析交易
        let operations = parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::Mint { atomical_type, metadata: parsed_metadata } => {
                assert_eq!(*atomical_type, AtomicalType::NFT);
                assert_eq!(parsed_metadata["name"], "Test NFT");
                assert_eq!(parsed_metadata["description"], "A test NFT");
            }
            _ => panic!("Expected Mint operation"),
        }
    }

    #[test]
    fn test_parse_update() {
        let mut tx = create_test_transaction();
        
        // 创建更新操作的输出
        let atomical_id = create_test_atomical_id();
        let metadata = serde_json::json!({
            "updated": true
        });
        
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.extend_from_slice(b"update");
        script_data.extend_from_slice(&atomical_id.txid.to_vec());
        script_data.extend_from_slice(&atomical_id.vout.to_be_bytes());
        script_data.extend_from_slice(&serde_json::to_vec(&metadata).unwrap());
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 解析交易
        let operations = parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::Update { atomical_id: parsed_id, metadata: parsed_metadata } => {
                assert_eq!(*parsed_id, atomical_id);
                assert_eq!(parsed_metadata["updated"], true);
            }
            _ => panic!("Expected Update operation"),
        }
    }

    #[test]
    fn test_parse_seal() {
        let mut tx = create_test_transaction();
        
        // 创建封印操作的输出
        let atomical_id = create_test_atomical_id();
        
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.extend_from_slice(b"seal");
        script_data.extend_from_slice(&atomical_id.txid.to_vec());
        script_data.extend_from_slice(&atomical_id.vout.to_be_bytes());
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 解析交易
        let operations = parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::Seal { atomical_id: parsed_id } => {
                assert_eq!(*parsed_id, atomical_id);
            }
            _ => panic!("Expected Seal operation"),
        }
    }

    #[test]
    fn test_parse_transfer() {
        let mut tx = create_test_transaction();
        
        // 创建转移操作的输出
        let atomical_id = create_test_atomical_id();
        let output_index = 1u32;
        
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.extend_from_slice(b"transfer");
        script_data.extend_from_slice(&atomical_id.txid.to_vec());
        script_data.extend_from_slice(&atomical_id.vout.to_be_bytes());
        script_data.extend_from_slice(&output_index.to_be_bytes());
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 解析交易
        let operations = parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::Transfer { atomical_id: parsed_id, output_index: parsed_index } => {
                assert_eq!(*parsed_id, atomical_id);
                assert_eq!(*parsed_index, output_index);
            }
            _ => panic!("Expected Transfer operation"),
        }
    }

    #[test]
    fn test_invalid_operations() {
        let mut tx = create_test_transaction();
        
        // 1. 测试无效的前缀
        let mut invalid_prefix = b"invalid".to_vec();
        invalid_prefix.extend_from_slice(b"mint");
        let script = Builder::new()
            .push_slice(&invalid_prefix)
            .into_script();
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 2. 测试无效的操作类型
        let mut invalid_op = ATOMICALS_PREFIX.to_vec();
        invalid_op.extend_from_slice(b"invalid_op");
        let script = Builder::new()
            .push_slice(&invalid_op)
            .into_script();
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 3. 测试损坏的数据
        let mut corrupted_data = ATOMICALS_PREFIX.to_vec();
        corrupted_data.extend_from_slice(b"mint");
        corrupted_data.extend_from_slice(b"corrupted");
        let script = Builder::new()
            .push_slice(&corrupted_data)
            .into_script();
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 所有操作都应该被忽略
        let operations = parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 0);
    }

    #[test]
    fn test_multiple_operations() {
        let mut tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();
        
        // 1. 添加铸造操作
        let metadata = serde_json::json!({ "name": "Test NFT" });
        let mut mint_data = ATOMICALS_PREFIX.to_vec();
        mint_data.extend_from_slice(b"mint");
        mint_data.extend_from_slice(&serde_json::to_vec(&metadata).unwrap());
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: Builder::new().push_slice(&mint_data).into_script(),
        });

        // 2. 添加转移操作
        let mut transfer_data = ATOMICALS_PREFIX.to_vec();
        transfer_data.extend_from_slice(b"transfer");
        transfer_data.extend_from_slice(&atomical_id.txid.to_vec());
        transfer_data.extend_from_slice(&atomical_id.vout.to_be_bytes());
        transfer_data.extend_from_slice(&2u32.to_be_bytes());
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: Builder::new().push_slice(&transfer_data).into_script(),
        });

        // 3. 添加更新操作
        let update_metadata = serde_json::json!({ "updated": true });
        let mut update_data = ATOMICALS_PREFIX.to_vec();
        update_data.extend_from_slice(b"update");
        update_data.extend_from_slice(&atomical_id.txid.to_vec());
        update_data.extend_from_slice(&atomical_id.vout.to_be_bytes());
        update_data.extend_from_slice(&serde_json::to_vec(&update_metadata).unwrap());
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: Builder::new().push_slice(&update_data).into_script(),
        });

        // 验证所有操作都被正确解析
        let operations = parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 3);

        // 验证每个操作的类型
        assert!(matches!(operations[0], AtomicalOperation::Mint { .. }));
        assert!(matches!(operations[1], AtomicalOperation::Transfer { .. }));
        assert!(matches!(operations[2], AtomicalOperation::Update { .. }));
    }

    #[test]
    fn test_edge_cases() {
        let mut tx = create_test_transaction();
        
        // 1. 测试空脚本
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: Builder::new().into_script(),
        });

        // 2. 测试只有前缀的脚本
        let script = Builder::new()
            .push_slice(ATOMICALS_PREFIX)
            .into_script();
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 3. 测试超大元数据
        let mut large_metadata = serde_json::Map::new();
        for i in 0..1000 {
            large_metadata.insert(format!("key_{}", i), serde_json::Value::String("value".to_string()));
        }
        let mut large_data = ATOMICALS_PREFIX.to_vec();
        large_data.extend_from_slice(b"mint");
        large_data.extend_from_slice(&serde_json::to_vec(&large_metadata).unwrap());
        let script = Builder::new()
            .push_slice(&large_data)
            .into_script();
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 4. 测试零值输出
        let atomical_id = create_test_atomical_id();
        let mut transfer_data = ATOMICALS_PREFIX.to_vec();
        transfer_data.extend_from_slice(b"transfer");
        transfer_data.extend_from_slice(&atomical_id.txid.to_vec());
        transfer_data.extend_from_slice(&atomical_id.vout.to_be_bytes());
        transfer_data.extend_from_slice(&0u32.to_be_bytes());
        tx.output.push(TxOut {
            value: Amount::from_sat(0).to_sat(),
            script_pubkey: Builder::new().push_slice(&transfer_data).into_script(),
        });

        // 验证处理
        let operations = parse_transaction(&tx).unwrap();
        
        // 空脚本和只有前缀的脚本应该被忽略
        // 超大元数据应该能正常解析
        // 零值输出的操作应该能正常解析
        assert!(operations.len() >= 2);

        // 验证超大元数据的解析
        let has_large_metadata = operations.iter().any(|op| {
            matches!(op, AtomicalOperation::Mint { metadata: Some(m), .. } if m.as_object().unwrap().len() == 1000)
        });
        assert!(has_large_metadata);

        // 验证零值输出的操作
        let has_zero_value_transfer = operations.iter().any(|op| {
            matches!(op, AtomicalOperation::Transfer { atomical_id: id, output_index } if *id == atomical_id && *output_index == 0)
        });
        assert!(has_zero_value_transfer);
    }
}

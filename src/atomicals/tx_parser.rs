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
}

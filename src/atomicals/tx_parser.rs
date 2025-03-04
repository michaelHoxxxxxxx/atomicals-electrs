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

    // 尝试直接使用 from_tx_data 解析
    if let Some(op) = AtomicalOperation::from_tx_data(script) {
        return Some(op);
    }

    // 如果 from_tx_data 解析失败，使用原有的解析逻辑作为备选
    let op_type = std::str::from_utf8(&script[ATOMICALS_PREFIX.len()..]).ok()?;
    
    match op_type {
        "mint" => {
            // 解析铸造操作
            let metadata = serde_json::from_slice(&script[ATOMICALS_PREFIX.len() + 4..]).ok()?;
            Some(AtomicalOperation::Mint {
                id: AtomicalId {
                    txid: bitcoin::Txid::from_raw_hash(bitcoin::hashes::Hash::all_zeros()),
                    vout,
                },
                atomical_type: AtomicalType::NFT,
                metadata: Some(metadata),
            })
        }
        "update" => {
            // 解析更新操作
            let atomical_id = AtomicalId {
                txid: bitcoin::Txid::from_raw_hash(bitcoin::hashes::Hash::from_slice(&script[ATOMICALS_PREFIX.len() + 6..38]).ok()?),
                vout: u32::from_be_bytes(script[38..42].try_into().ok()?),
            };
            let metadata = serde_json::from_slice(&script[42..]).ok()?;
            Some(AtomicalOperation::Update {
                id: atomical_id,
                metadata,
            })
        }
        "seal" => {
            // 解析封印操作
            let atomical_id = AtomicalId {
                txid: bitcoin::Txid::from_raw_hash(bitcoin::hashes::Hash::from_slice(&script[ATOMICALS_PREFIX.len() + 4..36]).ok()?),
                vout: u32::from_be_bytes(script[36..40].try_into().ok()?),
            };
            Some(AtomicalOperation::Seal {
                id: atomical_id,
            })
        }
        "transfer" => {
            // 解析转移操作
            let atomical_id = AtomicalId {
                txid: bitcoin::Txid::from_raw_hash(bitcoin::hashes::Hash::from_slice(&script[ATOMICALS_PREFIX.len() + 8..40]).ok()?),
                vout: u32::from_be_bytes(script[40..44].try_into().ok()?),
            };
            let output_index = u32::from_be_bytes(script[44..48].try_into().ok()?);
            Some(AtomicalOperation::Transfer {
                id: atomical_id,
                output_index,
            })
        }
        "dft" => {
            // 解析分布式铸造代币部署操作
            let dft_data = serde_json::from_slice::<Value>(&script[ATOMICALS_PREFIX.len() + 3..]).ok()?;
            
            let ticker = dft_data.get("ticker")?.as_str()?.to_string();
            let max_supply = dft_data.get("max_supply")?.as_u64()?;
            let metadata = dft_data.get("meta").cloned();
            
            Some(AtomicalOperation::DeployDFT {
                id: AtomicalId {
                    txid: bitcoin::Txid::from_raw_hash(bitcoin::hashes::Hash::all_zeros()),
                    vout,
                },
                ticker,
                max_supply,
                metadata,
            })
        }
        "dmt" => {
            // 解析分布式铸造代币铸造操作
            let dmt_data = serde_json::from_slice::<Value>(&script[ATOMICALS_PREFIX.len() + 3..]).ok()?;
            
            let parent_id_str = dmt_data.get("parent_id")?.as_str()?;
            let parts: Vec<&str> = parent_id_str.split(':').collect();
            if parts.len() != 2 {
                return None;
            }
            
            let txid = bitcoin::Txid::from_str(parts[0]).ok()?;
            let vout = parts[1].parse::<u32>().ok()?;
            
            let parent_id = AtomicalId { txid, vout };
            let amount = dmt_data.get("amount")?.as_u64()?;
            
            Some(AtomicalOperation::MintDFT {
                parent_id,
                amount,
            })
        }
        "evt" => {
            // 解析事件操作
            let atomical_id = AtomicalId {
                txid: bitcoin::Txid::from_raw_hash(bitcoin::hashes::Hash::from_slice(&script[ATOMICALS_PREFIX.len() + 3..35]).ok()?),
                vout: u32::from_be_bytes(script[35..39].try_into().ok()?),
            };
            
            let data = serde_json::from_slice(&script[39..]).ok()?;
            
            Some(AtomicalOperation::Event {
                id: atomical_id,
                data,
            })
        }
        "dat" => {
            // 解析数据存储操作
            let atomical_id = AtomicalId {
                txid: bitcoin::Txid::from_raw_hash(bitcoin::hashes::Hash::from_slice(&script[ATOMICALS_PREFIX.len() + 3..35]).ok()?),
                vout: u32::from_be_bytes(script[35..39].try_into().ok()?),
            };
            
            let data = serde_json::from_slice(&script[39..]).ok()?;
            
            Some(AtomicalOperation::Data {
                id: atomical_id,
                data,
            })
        }
        "x" => {
            // 解析提取操作
            let atomical_id = AtomicalId {
                txid: bitcoin::Txid::from_raw_hash(bitcoin::hashes::Hash::from_slice(&script[ATOMICALS_PREFIX.len() + 1..33]).ok()?),
                vout: u32::from_be_bytes(script[33..37].try_into().ok()?),
            };
            
            Some(AtomicalOperation::Extract {
                id: atomical_id,
            })
        }
        "y" => {
            // 解析分割操作
            let atomical_id = AtomicalId {
                txid: bitcoin::Txid::from_raw_hash(bitcoin::hashes::Hash::from_slice(&script[ATOMICALS_PREFIX.len() + 1..33]).ok()?),
                vout: u32::from_be_bytes(script[33..37].try_into().ok()?),
            };
            
            let outputs_data = serde_json::from_slice::<Value>(&script[37..]).ok()?;
            
            let mut outputs = Vec::new();
            
            if let Some(outputs_array) = outputs_data.as_array() {
                for output in outputs_array {
                    if let (Some(idx), Some(amount)) = (output.get(0).and_then(|v| v.as_u64()), output.get(1).and_then(|v| v.as_u64())) {
                        outputs.push((idx as u32, amount));
                    }
                }
            }
            
            Some(AtomicalOperation::Split {
                id: atomical_id,
                outputs,
            })
        }
        _ => None,
    }
}

/// 交易解析器
#[derive(Debug)]
pub struct TxParser;

impl TxParser {
    /// 创建新的交易解析器
    pub fn new() -> Self {
        Self
    }
    
    /// 解析交易中的 Atomicals 操作
    pub fn parse_transaction(&self, tx: &Transaction) -> Result<Vec<AtomicalOperation>> {
        let mut operations = Vec::new();
        
        // 遍历所有输出
        for (vout, output) in tx.output.iter().enumerate() {
            if let Some(operation) = parse_output(output, vout as u32) {
                operations.push(operation);
            }
        }
        
        Ok(operations)
    }
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
        let parser = TxParser::new();
        let operations = parser.parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::Mint { id, atomical_type, metadata: parsed_metadata } => {
                assert_eq!(*atomical_type, AtomicalType::NFT);
                assert!(parsed_metadata.is_some());
                let parsed_metadata = parsed_metadata.as_ref().unwrap();
                assert_eq!(parsed_metadata["name"], "Test NFT");
                assert_eq!(parsed_metadata["description"], "A test NFT");
            },
            _ => panic!("Expected Mint operation"),
        }
    }

    #[test]
    fn test_parse_transfer() {
        let mut tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();
        
        // 创建转移操作的输出
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.extend_from_slice(b"transfer");
        script_data.extend_from_slice(&atomical_id.txid.to_byte_array());
        script_data.extend_from_slice(&atomical_id.vout.to_be_bytes());
        script_data.extend_from_slice(&1u32.to_be_bytes()); // 目标输出索引
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 解析交易
        let parser = TxParser::new();
        let operations = parser.parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::Transfer { id, output_index } => {
                assert_eq!(id.txid, atomical_id.txid);
                assert_eq!(id.vout, atomical_id.vout);
                assert_eq!(*output_index, 1);
            },
            _ => panic!("Expected Transfer operation"),
        }
    }

    #[test]
    fn test_parse_update() {
        let mut tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();
        
        // 创建更新操作的输出
        let metadata = serde_json::json!({
            "name": "Updated NFT",
            "description": "An updated NFT"
        });
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.extend_from_slice(b"update");
        script_data.extend_from_slice(&atomical_id.txid.to_byte_array());
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
        let parser = TxParser::new();
        let operations = parser.parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::Update { id, metadata: parsed_metadata } => {
                assert_eq!(id.txid, atomical_id.txid);
                assert_eq!(id.vout, atomical_id.vout);
                assert_eq!(parsed_metadata["name"], "Updated NFT");
                assert_eq!(parsed_metadata["description"], "An updated NFT");
            },
            _ => panic!("Expected Update operation"),
        }
    }

    #[test]
    fn test_parse_seal() {
        let mut tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();
        
        // 创建封印操作的输出
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.extend_from_slice(b"seal");
        script_data.extend_from_slice(&atomical_id.txid.to_byte_array());
        script_data.extend_from_slice(&atomical_id.vout.to_be_bytes());
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 解析交易
        let parser = TxParser::new();
        let operations = parser.parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::Seal { id } => {
                assert_eq!(id.txid, atomical_id.txid);
                assert_eq!(id.vout, atomical_id.vout);
            },
            _ => panic!("Expected Seal operation"),
        }
    }

    #[test]
    fn test_parse_deploy_dft() {
        let mut tx = create_test_transaction();
        
        // 创建 DFT 部署操作的输出
        let dft_data = serde_json::json!({
            "ticker": "TEST",
            "max_supply": 1000000,
            "meta": {
                "name": "Test Token",
                "description": "A test token"
            }
        });
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.push(3); // 操作类型长度
        script_data.extend_from_slice(b"dft");
        script_data.extend_from_slice(&serde_json::to_vec(&dft_data).unwrap());
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 解析交易
        let parser = TxParser::new();
        let operations = parser.parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::DeployDFT { id, ticker, max_supply, metadata } => {
                assert_eq!(id.vout, 0);
                assert_eq!(ticker, "TEST");
                assert_eq!(*max_supply, 1000000);
                assert!(metadata.is_some());
                let meta = metadata.as_ref().unwrap();
                assert_eq!(meta["name"], "Test Token");
                assert_eq!(meta["description"], "A test token");
            },
            _ => panic!("Expected DeployDFT operation"),
        }
    }

    #[test]
    fn test_parse_mint_dft() {
        let mut tx = create_test_transaction();
        let parent_id = create_test_atomical_id();
        
        // 创建 DFT 铸造操作的输出
        let dmt_data = serde_json::json!({
            "parent_id": format!("{}:{}", parent_id.txid, parent_id.vout),
            "amount": 100
        });
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.push(3); // 操作类型长度
        script_data.extend_from_slice(b"dmt");
        script_data.extend_from_slice(&serde_json::to_vec(&dmt_data).unwrap());
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 解析交易
        let parser = TxParser::new();
        let operations = parser.parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::MintDFT { parent_id: parsed_parent_id, amount } => {
                assert_eq!(parsed_parent_id.txid, parent_id.txid);
                assert_eq!(parsed_parent_id.vout, parent_id.vout);
                assert_eq!(*amount, 100);
            },
            _ => panic!("Expected MintDFT operation"),
        }
    }

    #[test]
    fn test_parse_event() {
        let mut tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();
        
        // 创建事件操作的输出
        let event_data = serde_json::json!({
            "type": "message",
            "content": "Hello, world!"
        });
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.push(3); // 操作类型长度
        script_data.extend_from_slice(b"evt");
        script_data.extend_from_slice(&atomical_id.txid.to_byte_array());
        script_data.extend_from_slice(&atomical_id.vout.to_be_bytes());
        script_data.extend_from_slice(&serde_json::to_vec(&event_data).unwrap());
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 解析交易
        let parser = TxParser::new();
        let operations = parser.parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::Event { id, data } => {
                assert_eq!(id.txid, atomical_id.txid);
                assert_eq!(id.vout, atomical_id.vout);
                assert_eq!(data["type"], "message");
                assert_eq!(data["content"], "Hello, world!");
            },
            _ => panic!("Expected Event operation"),
        }
    }

    #[test]
    fn test_parse_data() {
        let mut tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();
        
        // 创建数据存储操作的输出
        let data_content = serde_json::json!({
            "key1": "value1",
            "key2": "value2"
        });
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.push(3); // 操作类型长度
        script_data.extend_from_slice(b"dat");
        script_data.extend_from_slice(&atomical_id.txid.to_byte_array());
        script_data.extend_from_slice(&atomical_id.vout.to_be_bytes());
        script_data.extend_from_slice(&serde_json::to_vec(&data_content).unwrap());
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 解析交易
        let parser = TxParser::new();
        let operations = parser.parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::Data { id, data } => {
                assert_eq!(id.txid, atomical_id.txid);
                assert_eq!(id.vout, atomical_id.vout);
                assert_eq!(data["key1"], "value1");
                assert_eq!(data["key2"], "value2");
            },
            _ => panic!("Expected Data operation"),
        }
    }

    #[test]
    fn test_parse_extract() {
        let mut tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();
        
        // 创建提取操作的输出
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.push(1); // 操作类型长度
        script_data.extend_from_slice(b"x");
        script_data.extend_from_slice(&atomical_id.txid.to_byte_array());
        script_data.extend_from_slice(&atomical_id.vout.to_be_bytes());
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 解析交易
        let parser = TxParser::new();
        let operations = parser.parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::Extract { id } => {
                assert_eq!(id.txid, atomical_id.txid);
                assert_eq!(id.vout, atomical_id.vout);
            },
            _ => panic!("Expected Extract operation"),
        }
    }

    #[test]
    fn test_parse_split() {
        let mut tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();
        
        // 创建分割操作的输出
        let outputs_data = serde_json::json!([
            [0, 500],
            [1, 300],
            [2, 200]
        ]);
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.push(1); // 操作类型长度
        script_data.extend_from_slice(b"y");
        script_data.extend_from_slice(&atomical_id.txid.to_byte_array());
        script_data.extend_from_slice(&atomical_id.vout.to_be_bytes());
        script_data.extend_from_slice(&serde_json::to_vec(&outputs_data).unwrap());
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 解析交易
        let parser = TxParser::new();
        let operations = parser.parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::Split { id, outputs } => {
                assert_eq!(id.txid, atomical_id.txid);
                assert_eq!(id.vout, atomical_id.vout);
                assert_eq!(outputs.len(), 3);
                assert_eq!(outputs[0], (0, 500));
                assert_eq!(outputs[1], (1, 300));
                assert_eq!(outputs[2], (2, 200));
            },
            _ => panic!("Expected Split operation"),
        }
    }

    #[test]
    fn test_parse_large_metadata() {
        let mut tx = create_test_transaction();
        
        // 创建带有大元数据的铸造操作
        let mut large_metadata = serde_json::Map::new();
        for i in 0..100 {
            large_metadata.insert(format!("key{}", i), serde_json::Value::String(format!("value{}", i)));
        }
        let large_metadata = serde_json::Value::Object(large_metadata);
        
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
        
        // 解析交易
        let parser = TxParser::new();
        let operations = parser.parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::Mint { metadata, .. } => {
                assert!(metadata.is_some());
                let metadata = metadata.as_ref().unwrap();
                assert_eq!(metadata["key0"], "value0");
                assert_eq!(metadata["key99"], "value99");
            },
            _ => panic!("Expected Mint operation"),
        }
    }
}

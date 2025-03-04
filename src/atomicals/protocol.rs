use bitcoin::{OutPoint, Txid};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Atomical ID is a unique identifier for an Atomical
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AtomicalId {
    pub txid: Txid,
    pub vout: u32,
}

impl AtomicalId {
    pub fn new(outpoint: OutPoint) -> Self {
        Self {
            txid: outpoint.txid,
            vout: outpoint.vout,
        }
    }

    pub fn to_string(&self) -> String {
        format!("{}:{}", self.txid, self.vout)
    }
}

/// Type of Atomical
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AtomicalType {
    /// Non-Fungible Token
    NFT,
    /// Fungible Token
    FT,
    /// Decentralized Identity
    DID,
    /// Container
    Container,
    /// Realm
    Realm,
    /// Unknown type
    Unknown,
}

impl fmt::Display for AtomicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AtomicalType::NFT => write!(f, "NFT"),
            AtomicalType::FT => write!(f, "FT"),
            AtomicalType::DID => write!(f, "DID"),
            AtomicalType::Container => write!(f, "Container"),
            AtomicalType::Realm => write!(f, "Realm"),
            AtomicalType::Unknown => write!(f, "Unknown"),
        }
    }
}

impl FromStr for AtomicalType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NFT" => Ok(AtomicalType::NFT),
            "FT" => Ok(AtomicalType::FT),
            "DID" => Ok(AtomicalType::DID),
            "Container" => Ok(AtomicalType::Container),
            "Realm" => Ok(AtomicalType::Realm),
            "Unknown" => Ok(AtomicalType::Unknown),
            _ => Err(format!("Unknown AtomicalType: {}", s)),
        }
    }
}

/// Operation type for Atomicals
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AtomicalOperation {
    /// Mint a new Atomical
    Mint {
        /// ID of the Atomical being minted
        id: AtomicalId,
        /// Type of Atomical being minted
        atomical_type: AtomicalType,
        /// Optional metadata for the Atomical
        metadata: Option<serde_json::Value>,
    },
    /// Transfer an existing Atomical
    Transfer {
        /// ID of the Atomical being transferred
        id: AtomicalId,
        /// Destination output index
        output_index: u32,
    },
    /// Update Atomical metadata
    Update {
        /// ID of the Atomical being updated
        id: AtomicalId,
        /// New metadata
        metadata: serde_json::Value,
    },
    /// Seal an Atomical (make it immutable)
    Seal {
        /// ID of the Atomical being sealed
        id: AtomicalId,
    },
    /// Deploy a distributed mint fungible token (DFT)
    DeployDFT {
        /// ID of the Atomical being deployed
        id: AtomicalId,
        /// Ticker of the DFT
        ticker: String,
        /// Maximum supply
        max_supply: u64,
        /// Optional metadata
        metadata: Option<serde_json::Value>,
    },
    /// Mint tokens of a distributed mint type (DFT)
    MintDFT {
        /// ID of the parent DFT Atomical
        parent_id: AtomicalId,
        /// Amount to mint
        amount: u64,
    },
    /// Event operation (message/response/reply)
    Event {
        /// ID of the Atomical for the event
        id: AtomicalId,
        /// Event data
        data: serde_json::Value,
    },
    /// Store data on a transaction
    Data {
        /// ID of the Atomical
        id: AtomicalId,
        /// Data payload
        data: serde_json::Value,
    },
    /// Extract - move atomical to 0'th output
    Extract {
        /// ID of the Atomical
        id: AtomicalId,
    },
    /// Split operation
    Split {
        /// ID of the Atomical
        id: AtomicalId,
        /// Output indices and amounts
        outputs: Vec<(u32, u64)>,
    },
}

impl AtomicalOperation {
    /// Parse operation data from transaction
    pub fn from_tx_data(data: &[u8]) -> Option<Self> {
        if data.len() < 4 {
            return None;
        }

        // 检查是否以 "atom" 开头
        if &data[0..4] != b"atom" {
            return None;
        }

        let mut offset = 4;
        
        // 解析操作类型
        if offset >= data.len() {
            return None;
        }

        // 获取操作类型长度
        let op_len = data[offset] as usize;
        offset += 1;
        
        if offset + op_len > data.len() {
            return None;
        }
        
        // 获取操作类型字符串
        let op_type = match std::str::from_utf8(&data[offset..offset + op_len]) {
            Ok(s) => s,
            Err(_) => return None,
        };
        
        offset += op_len;

        // 根据操作类型解析不同的操作
        match op_type {
            "nft" => {
                // 解析 NFT 铸造操作
                if offset >= data.len() {
                    return None;
                }
                
                // 解析元数据
                let metadata = match serde_json::from_slice::<serde_json::Value>(&data[offset..]) {
                    Ok(value) => Some(value),
                    Err(_) => None,
                };
                
                // 创建一个临时 ID (将在后续处理中被替换)
                let id = AtomicalId {
                    txid: Txid::all_zeros(),
                    vout: 0,
                };
                
                Some(Self::Mint {
                    id,
                    atomical_type: AtomicalType::NFT,
                    metadata,
                })
            },
            "ft" => {
                // 解析 FT 铸造操作
                if offset >= data.len() {
                    return None;
                }
                
                // 解析元数据
                let metadata = match serde_json::from_slice::<serde_json::Value>(&data[offset..]) {
                    Ok(value) => Some(value),
                    Err(_) => None,
                };
                
                // 创建一个临时 ID (将在后续处理中被替换)
                let id = AtomicalId {
                    txid: Txid::all_zeros(),
                    vout: 0,
                };
                
                Some(Self::Mint {
                    id,
                    atomical_type: AtomicalType::FT,
                    metadata,
                })
            },
            "dft" => {
                // 解析分布式铸造代币部署操作
                if offset >= data.len() {
                    return None;
                }
                
                // 解析 DFT 数据
                let dft_data = match serde_json::from_slice::<serde_json::Value>(&data[offset..]) {
                    Ok(value) => value,
                    Err(_) => return None,
                };
                
                // 提取必要字段
                let ticker = match dft_data.get("ticker") {
                    Some(t) => match t.as_str() {
                        Some(s) => s.to_string(),
                        None => return None,
                    },
                    None => return None,
                };
                
                let max_supply = match dft_data.get("max_supply") {
                    Some(s) => match s.as_u64() {
                        Some(n) => n,
                        None => return None,
                    },
                    None => return None,
                };
                
                let metadata = dft_data.get("meta").cloned();
                
                // 创建一个临时 ID (将在后续处理中被替换)
                let id = AtomicalId {
                    txid: Txid::all_zeros(),
                    vout: 0,
                };
                
                Some(Self::DeployDFT {
                    id,
                    ticker,
                    max_supply,
                    metadata,
                })
            },
            "dmt" => {
                // 解析分布式铸造代币铸造操作
                if offset >= data.len() {
                    return None;
                }
                
                // 解析 DMT 数据
                let dmt_data = match serde_json::from_slice::<serde_json::Value>(&data[offset..]) {
                    Ok(value) => value,
                    Err(_) => return None,
                };
                
                // 提取父 Atomical ID
                let parent_id_str = match dmt_data.get("parent_id") {
                    Some(p) => match p.as_str() {
                        Some(s) => s,
                        None => return None,
                    },
                    None => return None,
                };
                
                // 解析父 ID 字符串
                let parts: Vec<&str> = parent_id_str.split(':').collect();
                if parts.len() != 2 {
                    return None;
                }
                
                let txid = match Txid::from_str(parts[0]) {
                    Ok(t) => t,
                    Err(_) => return None,
                };
                
                let vout = match parts[1].parse::<u32>() {
                    Ok(v) => v,
                    Err(_) => return None,
                };
                
                let parent_id = AtomicalId { txid, vout };
                
                // 提取铸造数量
                let amount = match dmt_data.get("amount") {
                    Some(a) => match a.as_u64() {
                        Some(n) => n,
                        None => return None,
                    },
                    None => return None,
                };
                
                Some(Self::MintDFT {
                    parent_id,
                    amount,
                })
            },
            "mod" => {
                // 解析更新操作
                if offset + 36 > data.len() {
                    return None;
                }
                
                // 解析 Atomical ID
                let txid_bytes = &data[offset..offset + 32];
                let txid = match Txid::from_slice(txid_bytes) {
                    Ok(t) => t,
                    Err(_) => return None,
                };
                
                offset += 32;
                
                let vout_bytes = &data[offset..offset + 4];
                let vout = u32::from_le_bytes([vout_bytes[0], vout_bytes[1], vout_bytes[2], vout_bytes[3]]);
                
                offset += 4;
                
                let id = AtomicalId { txid, vout };
                
                // 解析元数据
                let metadata = match serde_json::from_slice::<serde_json::Value>(&data[offset..]) {
                    Ok(value) => value,
                    Err(_) => return None,
                };
                
                Some(Self::Update {
                    id,
                    metadata,
                })
            },
            "sl" => {
                // 解析封印操作
                if offset + 36 > data.len() {
                    return None;
                }
                
                // 解析 Atomical ID
                let txid_bytes = &data[offset..offset + 32];
                let txid = match Txid::from_slice(txid_bytes) {
                    Ok(t) => t,
                    Err(_) => return None,
                };
                
                offset += 32;
                
                let vout_bytes = &data[offset..offset + 4];
                let vout = u32::from_le_bytes([vout_bytes[0], vout_bytes[1], vout_bytes[2], vout_bytes[3]]);
                
                let id = AtomicalId { txid, vout };
                
                Some(Self::Seal { id })
            },
            "evt" => {
                // 解析事件操作
                if offset + 36 > data.len() {
                    return None;
                }
                
                // 解析 Atomical ID
                let txid_bytes = &data[offset..offset + 32];
                let txid = match Txid::from_slice(txid_bytes) {
                    Ok(t) => t,
                    Err(_) => return None,
                };
                
                offset += 32;
                
                let vout_bytes = &data[offset..offset + 4];
                let vout = u32::from_le_bytes([vout_bytes[0], vout_bytes[1], vout_bytes[2], vout_bytes[3]]);
                
                offset += 4;
                
                let id = AtomicalId { txid, vout };
                
                // 解析事件数据
                let data = match serde_json::from_slice::<serde_json::Value>(&data[offset..]) {
                    Ok(value) => value,
                    Err(_) => return None,
                };
                
                Some(Self::Event { id, data })
            },
            "dat" => {
                // 解析数据存储操作
                if offset + 36 > data.len() {
                    return None;
                }
                
                // 解析 Atomical ID
                let txid_bytes = &data[offset..offset + 32];
                let txid = match Txid::from_slice(txid_bytes) {
                    Ok(t) => t,
                    Err(_) => return None,
                };
                
                offset += 32;
                
                let vout_bytes = &data[offset..offset + 4];
                let vout = u32::from_le_bytes([vout_bytes[0], vout_bytes[1], vout_bytes[2], vout_bytes[3]]);
                
                offset += 4;
                
                let id = AtomicalId { txid, vout };
                
                // 解析数据
                let data = match serde_json::from_slice::<serde_json::Value>(&data[offset..]) {
                    Ok(value) => value,
                    Err(_) => return None,
                };
                
                Some(Self::Data { id, data })
            },
            "x" => {
                // 解析提取操作
                if offset + 36 > data.len() {
                    return None;
                }
                
                // 解析 Atomical ID
                let txid_bytes = &data[offset..offset + 32];
                let txid = match Txid::from_slice(txid_bytes) {
                    Ok(t) => t,
                    Err(_) => return None,
                };
                
                offset += 32;
                
                let vout_bytes = &data[offset..offset + 4];
                let vout = u32::from_le_bytes([vout_bytes[0], vout_bytes[1], vout_bytes[2], vout_bytes[3]]);
                
                let id = AtomicalId { txid, vout };
                
                Some(Self::Extract { id })
            },
            "y" => {
                // 解析分割操作
                if offset + 36 > data.len() {
                    return None;
                }
                
                // 解析 Atomical ID
                let txid_bytes = &data[offset..offset + 32];
                let txid = match Txid::from_slice(txid_bytes) {
                    Ok(t) => t,
                    Err(_) => return None,
                };
                
                offset += 32;
                
                let vout_bytes = &data[offset..offset + 4];
                let vout = u32::from_le_bytes([vout_bytes[0], vout_bytes[1], vout_bytes[2], vout_bytes[3]]);
                
                offset += 4;
                
                let id = AtomicalId { txid, vout };
                
                // 解析输出信息
                let outputs_data = match serde_json::from_slice::<serde_json::Value>(&data[offset..]) {
                    Ok(value) => value,
                    Err(_) => return None,
                };
                
                let mut outputs = Vec::new();
                
                if let Some(outputs_array) = outputs_data.as_array() {
                    for output in outputs_array {
                        if let (Some(idx), Some(amount)) = (output.get(0).and_then(|v| v.as_u64()), output.get(1).and_then(|v| v.as_u64())) {
                            outputs.push((idx as u32, amount));
                        }
                    }
                }
                
                Some(Self::Split { id, outputs })
            },
            _ => None,
        }
    }

    /// Get the type of operation
    pub fn operation_type(&self) -> &'static str {
        match self {
            Self::Mint { .. } => "mint",
            Self::Transfer { .. } => "transfer",
            Self::Update { .. } => "update",
            Self::Seal { .. } => "seal",
            Self::DeployDFT { .. } => "deploy_dft",
            Self::MintDFT { .. } => "mint_dft",
            Self::Event { .. } => "event",
            Self::Data { .. } => "data",
            Self::Extract { .. } => "extract",
            Self::Split { .. } => "split",
        }
    }

    /// Check if the operation is a mint operation
    pub fn is_mint(&self) -> bool {
        matches!(self, Self::Mint { .. })
    }

    /// Check if the operation is a transfer operation
    pub fn is_transfer(&self) -> bool {
        matches!(self, Self::Transfer { .. })
    }

    /// Check if the operation is an update operation
    pub fn is_update(&self) -> bool {
        matches!(self, Self::Update { .. })
    }

    /// Check if the operation is a seal operation
    pub fn is_seal(&self) -> bool {
        matches!(self, Self::Seal { .. })
    }
    
    /// Check if the operation is a deploy DFT operation
    pub fn is_deploy_dft(&self) -> bool {
        matches!(self, Self::DeployDFT { .. })
    }
    
    /// Check if the operation is a mint DFT operation
    pub fn is_mint_dft(&self) -> bool {
        matches!(self, Self::MintDFT { .. })
    }
    
    /// Check if the operation is an event operation
    pub fn is_event(&self) -> bool {
        matches!(self, Self::Event { .. })
    }
    
    /// Check if the operation is a data operation
    pub fn is_data(&self) -> bool {
        matches!(self, Self::Data { .. })
    }
    
    /// Check if the operation is an extract operation
    pub fn is_extract(&self) -> bool {
        matches!(self, Self::Extract { .. })
    }
    
    /// Check if the operation is a split operation
    pub fn is_split(&self) -> bool {
        matches!(self, Self::Split { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::hex::FromHex;
    use serde_json::json;

    #[test]
    fn test_atomical_id() {
        // Test creation from OutPoint
        let txid = Txid::from_hex("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef").unwrap();
        let vout = 1;
        let outpoint = OutPoint::new(txid, vout);
        let atomical_id = AtomicalId::new(outpoint);

        assert_eq!(atomical_id.txid, txid);
        assert_eq!(atomical_id.vout, vout);

        // Test string representation
        let expected_str = format!("{}:{}", txid, vout);
        assert_eq!(atomical_id.to_string(), expected_str);

        // Test equality
        let same_atomical_id = AtomicalId { txid, vout };
        assert_eq!(atomical_id, same_atomical_id);

        // Test different IDs are not equal
        let different_vout = AtomicalId { txid, vout: 2 };
        assert_ne!(atomical_id, different_vout);

        let different_txid = AtomicalId {
            txid: Txid::from_hex("abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890").unwrap(),
            vout,
        };
        assert_ne!(atomical_id, different_txid);

        // Test hash implementation
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(atomical_id);
        assert!(set.contains(&same_atomical_id));
        assert!(!set.contains(&different_vout));
        assert!(!set.contains(&different_txid));

        // Test serialization/deserialization
        let serialized = serde_json::to_string(&atomical_id).unwrap();
        let deserialized: AtomicalId = serde_json::from_str(&serialized).unwrap();
        assert_eq!(atomical_id, deserialized);
    }

    #[test]
    fn test_atomical_type() {
        // Test all types are different
        assert_ne!(AtomicalType::NFT, AtomicalType::FT);
        assert_ne!(AtomicalType::NFT, AtomicalType::DID);
        assert_ne!(AtomicalType::NFT, AtomicalType::Container);
        assert_ne!(AtomicalType::NFT, AtomicalType::Realm);
        assert_ne!(AtomicalType::FT, AtomicalType::DID);
        assert_ne!(AtomicalType::FT, AtomicalType::Container);
        assert_ne!(AtomicalType::FT, AtomicalType::Realm);
        assert_ne!(AtomicalType::DID, AtomicalType::Container);
        assert_ne!(AtomicalType::DID, AtomicalType::Realm);
        assert_ne!(AtomicalType::Container, AtomicalType::Realm);
        assert_ne!(AtomicalType::NFT, AtomicalType::Unknown);
        assert_ne!(AtomicalType::FT, AtomicalType::Unknown);
        assert_ne!(AtomicalType::DID, AtomicalType::Unknown);
        assert_ne!(AtomicalType::Container, AtomicalType::Unknown);
        assert_ne!(AtomicalType::Realm, AtomicalType::Unknown);

        // Test equality with same type
        assert_eq!(AtomicalType::NFT, AtomicalType::NFT);
        assert_eq!(AtomicalType::FT, AtomicalType::FT);
        assert_eq!(AtomicalType::DID, AtomicalType::DID);
        assert_eq!(AtomicalType::Container, AtomicalType::Container);
        assert_eq!(AtomicalType::Realm, AtomicalType::Realm);
        assert_eq!(AtomicalType::Unknown, AtomicalType::Unknown);

        // Test serialization/deserialization for all types
        let types = vec![
            AtomicalType::NFT,
            AtomicalType::FT,
            AtomicalType::DID,
            AtomicalType::Container,
            AtomicalType::Realm,
            AtomicalType::Unknown,
        ];

        for atomical_type in types {
            let serialized = serde_json::to_string(&atomical_type).unwrap();
            let deserialized: AtomicalType = serde_json::from_str(&serialized).unwrap();
            assert_eq!(atomical_type, deserialized);
        }

        // Test invalid deserialization
        let result: Result<AtomicalType, _> = serde_json::from_str("\"INVALID\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_atomical_operation_mint() {
        // Test mint operation with metadata
        let metadata = json!({
            "name": "Test NFT",
            "description": "A test NFT",
            "image": "https://example.com/image.png",
            "attributes": [
                {
                    "trait_type": "Color",
                    "value": "Blue"
                }
            ]
        });

        let txid = Txid::from_hex("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef").unwrap();
        let atomical_id = AtomicalId { txid, vout: 0 };

        let mint_op = AtomicalOperation::Mint {
            id: atomical_id,
            atomical_type: AtomicalType::NFT,
            metadata: Some(metadata.clone()),
        };

        assert!(mint_op.is_mint());
        assert!(!mint_op.is_transfer());
        assert!(!mint_op.is_update());
        assert!(!mint_op.is_seal());
        assert_eq!(mint_op.operation_type(), "mint");

        // Test mint operation without metadata
        let mint_op_no_metadata = AtomicalOperation::Mint {
            id: atomical_id,
            atomical_type: AtomicalType::FT,
            metadata: None,
        };

        assert!(mint_op_no_metadata.is_mint());
        assert_eq!(mint_op_no_metadata.operation_type(), "mint");

        // Test mint operation for each Atomical type
        for atomical_type in [
            AtomicalType::NFT,
            AtomicalType::FT,
            AtomicalType::DID,
            AtomicalType::Container,
            AtomicalType::Realm,
            AtomicalType::Unknown,
        ] {
            let op = AtomicalOperation::Mint {
                id: atomical_id,
                atomical_type,
                metadata: Some(json!({"name": format!("Test {:#?}", atomical_type)})),
            };
            assert!(op.is_mint());
            assert_eq!(op.operation_type(), "mint");
        }

        // Test serialization/deserialization
        let serialized = serde_json::to_string(&mint_op).unwrap();
        let deserialized: AtomicalOperation = serde_json::from_str(&serialized).unwrap();
        assert!(deserialized.is_mint());
    }

    #[test]
    fn test_atomical_operation_transfer() {
        let txid = Txid::from_hex("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef").unwrap();
        let atomical_id = AtomicalId { txid, vout: 0 };

        // Test transfer operation
        let transfer_op = AtomicalOperation::Transfer {
            id: atomical_id,
            output_index: 1,
        };

        assert!(transfer_op.is_transfer());
        assert!(!transfer_op.is_mint());
        assert!(!transfer_op.is_update());
        assert!(!transfer_op.is_seal());
        assert_eq!(transfer_op.operation_type(), "transfer");

        // Test transfer with different output indices
        for output_index in 0..5 {
            let op = AtomicalOperation::Transfer {
                id: atomical_id,
                output_index,
            };
            assert!(op.is_transfer());
            assert_eq!(op.operation_type(), "transfer");
        }

        // Test serialization/deserialization
        let serialized = serde_json::to_string(&transfer_op).unwrap();
        let deserialized: AtomicalOperation = serde_json::from_str(&serialized).unwrap();
        assert!(deserialized.is_transfer());
    }

    #[test]
    fn test_atomical_operation_update() {
        let txid = Txid::from_hex("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef").unwrap();
        let atomical_id = AtomicalId { txid, vout: 0 };

        // Test update operation with simple metadata
        let simple_metadata = json!({
            "name": "Updated NFT",
            "description": "Updated description"
        });

        let update_op = AtomicalOperation::Update {
            id: atomical_id,
            metadata: simple_metadata.clone(),
        };

        assert!(update_op.is_update());
        assert!(!update_op.is_mint());
        assert!(!update_op.is_transfer());
        assert!(!update_op.is_seal());
        assert_eq!(update_op.operation_type(), "update");

        // Test update operation with complex metadata
        let complex_metadata = json!({
            "name": "Updated NFT",
            "description": "Updated description",
            "attributes": [
                {
                    "trait_type": "Color",
                    "value": "Red"
                },
                {
                    "trait_type": "Size",
                    "value": "Large"
                }
            ],
            "nested": {
                "field1": "value1",
                "field2": 42,
                "field3": true,
                "field4": null,
                "field5": ["item1", "item2"]
            }
        });

        let complex_update_op = AtomicalOperation::Update {
            id: atomical_id,
            metadata: complex_metadata.clone(),
        };

        assert!(complex_update_op.is_update());

        // Test serialization/deserialization
        let serialized = serde_json::to_string(&complex_update_op).unwrap();
        let deserialized: AtomicalOperation = serde_json::from_str(&serialized).unwrap();
        assert!(deserialized.is_update());
    }

    #[test]
    fn test_atomical_operation_seal() {
        let txid = Txid::from_hex("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef").unwrap();
        let atomical_id = AtomicalId { txid, vout: 0 };

        // Test seal operation
        let seal_op = AtomicalOperation::Seal { id: atomical_id };

        assert!(seal_op.is_seal());
        assert!(!seal_op.is_mint());
        assert!(!seal_op.is_transfer());
        assert!(!seal_op.is_update());
        assert_eq!(seal_op.operation_type(), "seal");

        // Test sealing with different atomical IDs
        for vout in 0..5 {
            let id = AtomicalId { txid, vout };
            let op = AtomicalOperation::Seal { id };
            assert!(op.is_seal());
            assert_eq!(op.operation_type(), "seal");
        }

        // Test serialization/deserialization
        let serialized = serde_json::to_string(&seal_op).unwrap();
        let deserialized: AtomicalOperation = serde_json::from_str(&serialized).unwrap();
        assert!(deserialized.is_seal());
    }

    #[test]
    fn test_operation_from_tx_data() {
        // 测试空数据
        assert!(AtomicalOperation::from_tx_data(&[]).is_none());

        // TODO: 一旦实现了 from_tx_data，添加更多测试用例
        // 1. 测试有效的铸造操作数据
        // 2. 测试有效的转移操作数据
        // 3. 测试有效的更新操作数据
        // 4. 测试有效的封印操作数据
        // 5. 测试无效的操作数据
    }

    #[test]
    fn test_operation_type_consistency() {
        let txid = Txid::from_hex("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef").unwrap();
        let atomical_id = AtomicalId { txid, vout: 0 };

        // 创建所有类型的操作
        let operations = vec![
            AtomicalOperation::Mint {
                id: atomical_id,
                atomical_type: AtomicalType::NFT,
                metadata: None,
            },
            AtomicalOperation::Transfer {
                id: atomical_id,
                output_index: 0,
            },
            AtomicalOperation::Update {
                id: atomical_id,
                metadata: json!({}),
            },
            AtomicalOperation::Seal { id: atomical_id },
            AtomicalOperation::DeployDFT {
                id: atomical_id,
                ticker: "TEST".to_string(),
                max_supply: 100,
                metadata: None,
            },
            AtomicalOperation::MintDFT {
                parent_id: atomical_id,
                amount: 10,
            },
            AtomicalOperation::Event {
                id: atomical_id,
                data: json!({}),
            },
            AtomicalOperation::Data {
                id: atomical_id,
                data: json!({}),
            },
            AtomicalOperation::Extract { id: atomical_id },
            AtomicalOperation::Split {
                id: atomical_id,
                outputs: vec![(0, 10), (1, 20)],
            },
        ];

        // 验证每个操作的类型检查方法是互斥的
        for op in operations {
            match op {
                AtomicalOperation::Mint { .. } => {
                    assert!(op.is_mint());
                    assert!(!op.is_transfer());
                    assert!(!op.is_update());
                    assert!(!op.is_seal());
                    assert!(!op.is_deploy_dft());
                    assert!(!op.is_mint_dft());
                    assert!(!op.is_event());
                    assert!(!op.is_data());
                    assert!(!op.is_extract());
                    assert!(!op.is_split());
                }
                AtomicalOperation::Transfer { .. } => {
                    assert!(!op.is_mint());
                    assert!(op.is_transfer());
                    assert!(!op.is_update());
                    assert!(!op.is_seal());
                    assert!(!op.is_deploy_dft());
                    assert!(!op.is_mint_dft());
                    assert!(!op.is_event());
                    assert!(!op.is_data());
                    assert!(!op.is_extract());
                    assert!(!op.is_split());
                }
                AtomicalOperation::Update { .. } => {
                    assert!(!op.is_mint());
                    assert!(!op.is_transfer());
                    assert!(op.is_update());
                    assert!(!op.is_seal());
                    assert!(!op.is_deploy_dft());
                    assert!(!op.is_mint_dft());
                    assert!(!op.is_event());
                    assert!(!op.is_data());
                    assert!(!op.is_extract());
                    assert!(!op.is_split());
                }
                AtomicalOperation::Seal { .. } => {
                    assert!(!op.is_mint());
                    assert!(!op.is_transfer());
                    assert!(!op.is_update());
                    assert!(op.is_seal());
                    assert!(!op.is_deploy_dft());
                    assert!(!op.is_mint_dft());
                    assert!(!op.is_event());
                    assert!(!op.is_data());
                    assert!(!op.is_extract());
                    assert!(!op.is_split());
                }
                AtomicalOperation::DeployDFT { .. } => {
                    assert!(!op.is_mint());
                    assert!(!op.is_transfer());
                    assert!(!op.is_update());
                    assert!(!op.is_seal());
                    assert!(op.is_deploy_dft());
                    assert!(!op.is_mint_dft());
                    assert!(!op.is_event());
                    assert!(!op.is_data());
                    assert!(!op.is_extract());
                    assert!(!op.is_split());
                }
                AtomicalOperation::MintDFT { .. } => {
                    assert!(!op.is_mint());
                    assert!(!op.is_transfer());
                    assert!(!op.is_update());
                    assert!(!op.is_seal());
                    assert!(!op.is_deploy_dft());
                    assert!(op.is_mint_dft());
                    assert!(!op.is_event());
                    assert!(!op.is_data());
                    assert!(!op.is_extract());
                    assert!(!op.is_split());
                }
                AtomicalOperation::Event { .. } => {
                    assert!(!op.is_mint());
                    assert!(!op.is_transfer());
                    assert!(!op.is_update());
                    assert!(!op.is_seal());
                    assert!(!op.is_deploy_dft());
                    assert!(!op.is_mint_dft());
                    assert!(op.is_event());
                    assert!(!op.is_data());
                    assert!(!op.is_extract());
                    assert!(!op.is_split());
                }
                AtomicalOperation::Data { .. } => {
                    assert!(!op.is_mint());
                    assert!(!op.is_transfer());
                    assert!(!op.is_update());
                    assert!(!op.is_seal());
                    assert!(!op.is_deploy_dft());
                    assert!(!op.is_mint_dft());
                    assert!(!op.is_event());
                    assert!(op.is_data());
                    assert!(!op.is_extract());
                    assert!(!op.is_split());
                }
                AtomicalOperation::Extract { .. } => {
                    assert!(!op.is_mint());
                    assert!(!op.is_transfer());
                    assert!(!op.is_update());
                    assert!(!op.is_seal());
                    assert!(!op.is_deploy_dft());
                    assert!(!op.is_mint_dft());
                    assert!(!op.is_event());
                    assert!(!op.is_data());
                    assert!(op.is_extract());
                    assert!(!op.is_split());
                }
                AtomicalOperation::Split { .. } => {
                    assert!(!op.is_mint());
                    assert!(!op.is_transfer());
                    assert!(!op.is_update());
                    assert!(!op.is_seal());
                    assert!(!op.is_deploy_dft());
                    assert!(!op.is_mint_dft());
                    assert!(!op.is_event());
                    assert!(!op.is_data());
                    assert!(!op.is_extract());
                    assert!(op.is_split());
                }
            }
        }
    }
}

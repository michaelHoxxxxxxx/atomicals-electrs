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
}

impl AtomicalOperation {
    /// Parse operation data from transaction
    pub fn from_tx_data(_data: &[u8]) -> Option<Self> {
        // TODO: Implement parsing logic for operation data
        // This will involve:
        // 1. Parsing the operation type
        // 2. Parsing operation-specific data
        // 3. Validating the data format
        None
    }

    /// Get the type of operation
    pub fn operation_type(&self) -> &'static str {
        match self {
            Self::Mint { .. } => "mint",
            Self::Transfer { .. } => "transfer",
            Self::Update { .. } => "update",
            Self::Seal { .. } => "seal",
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
        ];

        // 验证每个操作的类型检查方法是互斥的
        for op in operations {
            match op {
                AtomicalOperation::Mint { .. } => {
                    assert!(op.is_mint());
                    assert!(!op.is_transfer());
                    assert!(!op.is_update());
                    assert!(!op.is_seal());
                }
                AtomicalOperation::Transfer { .. } => {
                    assert!(!op.is_mint());
                    assert!(op.is_transfer());
                    assert!(!op.is_update());
                    assert!(!op.is_seal());
                }
                AtomicalOperation::Update { .. } => {
                    assert!(!op.is_mint());
                    assert!(!op.is_transfer());
                    assert!(op.is_update());
                    assert!(!op.is_seal());
                }
                AtomicalOperation::Seal { .. } => {
                    assert!(!op.is_mint());
                    assert!(!op.is_transfer());
                    assert!(!op.is_update());
                    assert!(op.is_seal());
                }
            }
        }
    }
}

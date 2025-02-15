use bitcoin::{OutPoint, Txid};
use serde::{Deserialize, Serialize};

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
}

/// Operation type for Atomicals
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AtomicalOperation {
    /// Mint a new Atomical
    Mint {
        /// Type of Atomical being minted
        atomical_type: AtomicalType,
        /// Optional metadata for the Atomical
        metadata: Option<serde_json::Value>,
    },
    /// Transfer an existing Atomical
    Transfer {
        /// ID of the Atomical being transferred
        atomical_id: AtomicalId,
        /// Destination output index
        output_index: u32,
    },
    /// Update Atomical metadata
    Update {
        /// ID of the Atomical being updated
        atomical_id: AtomicalId,
        /// New metadata
        metadata: serde_json::Value,
    },
    /// Seal an Atomical (make it immutable)
    Seal {
        /// ID of the Atomical being sealed
        atomical_id: AtomicalId,
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

        // Test equality with same type
        assert_eq!(AtomicalType::NFT, AtomicalType::NFT);
        assert_eq!(AtomicalType::FT, AtomicalType::FT);
        assert_eq!(AtomicalType::DID, AtomicalType::DID);
        assert_eq!(AtomicalType::Container, AtomicalType::Container);
        assert_eq!(AtomicalType::Realm, AtomicalType::Realm);

        // Test serialization/deserialization
        let nft_json = serde_json::to_string(&AtomicalType::NFT).unwrap();
        let nft_deserialized: AtomicalType = serde_json::from_str(&nft_json).unwrap();
        assert_eq!(nft_deserialized, AtomicalType::NFT);
    }

    #[test]
    fn test_atomical_operation_mint() {
        // Test mint operation with metadata
        let metadata = serde_json::json!({
            "name": "Test NFT",
            "description": "A test NFT",
            "image": "https://example.com/image.png"
        });

        let mint_op = AtomicalOperation::Mint {
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
            atomical_type: AtomicalType::FT,
            metadata: None,
        };

        assert!(mint_op_no_metadata.is_mint());
        assert_eq!(mint_op_no_metadata.operation_type(), "mint");
    }

    #[test]
    fn test_atomical_operation_transfer() {
        let txid = Txid::from_hex("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef").unwrap();
        let atomical_id = AtomicalId { txid, vout: 0 };
        let output_index = 1;

        let transfer_op = AtomicalOperation::Transfer {
            atomical_id,
            output_index,
        };

        assert!(transfer_op.is_transfer());
        assert!(!transfer_op.is_mint());
        assert!(!transfer_op.is_update());
        assert!(!transfer_op.is_seal());
        assert_eq!(transfer_op.operation_type(), "transfer");

        // Test serialization/deserialization
        let transfer_json = serde_json::to_string(&transfer_op).unwrap();
        let transfer_deserialized: AtomicalOperation = serde_json::from_str(&transfer_json).unwrap();
        
        match transfer_deserialized {
            AtomicalOperation::Transfer { atomical_id: id, output_index: idx } => {
                assert_eq!(id, atomical_id);
                assert_eq!(idx, output_index);
            }
            _ => panic!("Deserialized to wrong variant"),
        }
    }

    #[test]
    fn test_atomical_operation_update() {
        let txid = Txid::from_hex("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef").unwrap();
        let atomical_id = AtomicalId { txid, vout: 0 };
        let metadata = serde_json::json!({
            "updated_name": "Updated NFT",
            "updated_description": "An updated NFT"
        });

        let update_op = AtomicalOperation::Update {
            atomical_id,
            metadata: metadata.clone(),
        };

        assert!(update_op.is_update());
        assert!(!update_op.is_mint());
        assert!(!update_op.is_transfer());
        assert!(!update_op.is_seal());
        assert_eq!(update_op.operation_type(), "update");

        // Test serialization/deserialization
        let update_json = serde_json::to_string(&update_op).unwrap();
        let update_deserialized: AtomicalOperation = serde_json::from_str(&update_json).unwrap();
        
        match update_deserialized {
            AtomicalOperation::Update { atomical_id: id, metadata: meta } => {
                assert_eq!(id, atomical_id);
                assert_eq!(meta, metadata);
            }
            _ => panic!("Deserialized to wrong variant"),
        }
    }

    #[test]
    fn test_atomical_operation_seal() {
        let txid = Txid::from_hex("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef").unwrap();
        let atomical_id = AtomicalId { txid, vout: 0 };

        let seal_op = AtomicalOperation::Seal { atomical_id };

        assert!(seal_op.is_seal());
        assert!(!seal_op.is_mint());
        assert!(!seal_op.is_transfer());
        assert!(!seal_op.is_update());
        assert_eq!(seal_op.operation_type(), "seal");

        // Test serialization/deserialization
        let seal_json = serde_json::to_string(&seal_op).unwrap();
        let seal_deserialized: AtomicalOperation = serde_json::from_str(&seal_json).unwrap();
        
        match seal_deserialized {
            AtomicalOperation::Seal { atomical_id: id } => {
                assert_eq!(id, atomical_id);
            }
            _ => panic!("Deserialized to wrong variant"),
        }
    }

    #[test]
    fn test_operation_from_tx_data() {
        // TODO: Once from_tx_data is implemented, add tests for:
        // 1. Valid mint operation data
        // 2. Valid transfer operation data
        // 3. Valid update operation data
        // 4. Valid seal operation data
        // 5. Invalid operation data
        // 6. Empty data
        // 7. Malformed data
    }
}

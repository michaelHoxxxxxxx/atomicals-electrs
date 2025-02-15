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

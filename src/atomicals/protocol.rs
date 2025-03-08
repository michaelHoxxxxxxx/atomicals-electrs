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
    /// Create a DFT subdomain
    CreateSubdomain {
        /// Subdomain name
        name: String,
        /// Parent domain ID (optional)
        parent_id: Option<AtomicalId>,
        /// Optional metadata
        metadata: Option<serde_json::Value>,
    },
    /// Create a DFT container
    CreateContainer {
        /// Container ID
        id: AtomicalId,
        /// Container name
        name: String,
        /// Optional metadata
        metadata: Option<serde_json::Value>,
    },
    /// Add DFT to a subdomain
    AddToSubdomain {
        /// DFT ID
        dft_id: AtomicalId,
        /// Subdomain name
        subdomain_name: String,
    },
    /// Add DFT to a container
    AddToContainer {
        /// DFT ID
        dft_id: AtomicalId,
        /// Container ID
        container_id: AtomicalId,
    },
    /// Remove DFT from a subdomain
    RemoveFromSubdomain {
        /// DFT ID
        dft_id: AtomicalId,
        /// Subdomain name
        subdomain_name: String,
    },
    /// Remove DFT from a container
    RemoveFromContainer {
        /// DFT ID
        dft_id: AtomicalId,
        /// Container ID
        container_id: AtomicalId,
    },
    /// Seal a container
    SealContainer {
        /// Container ID
        container_id: AtomicalId,
    },
}

impl AtomicalOperation {
    /// Parse operation data from transaction
    pub fn from_tx_data(data: &[u8]) -> Result<Self, String> {
        if data.len() < 4 {
            return Err("Invalid data length".to_string());
        }

        // 检查是否以 "atom" 开头
        if &data[0..4] != b"atom" {
            return Err("Invalid magic bytes".to_string());
        }

        let mut offset = 4;
        
        // 解析操作类型
        if offset >= data.len() {
            return Err("Invalid operation type length".to_string());
        }

        // 获取操作类型长度
        let op_len = data[offset] as usize;
        offset += 1;
        
        if offset + op_len > data.len() {
            return Err("Invalid operation type".to_string());
        }
        
        // 获取操作类型字符串
        let op_type = match std::str::from_utf8(&data[offset..offset + op_len]) {
            Ok(s) => s,
            Err(_) => return Err("Invalid operation type string".to_string()),
        };
        offset += op_len;
        
        // 解析操作数据
        match op_type {
            "mint" => {
                // 解析 Atomical ID
                if offset + 36 > data.len() {
                    return Err("Invalid Atomical ID".to_string());
                }
                
                let txid = match Txid::from_slice(&data[offset..offset + 32]) {
                    Ok(id) => id,
                    Err(_) => return Err("Invalid Txid".to_string()),
                };
                offset += 32;
                
                let vout = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
                offset += 4;
                
                // 解析 Atomical 类型
                if offset >= data.len() {
                    return Err("Invalid Atomical type length".to_string());
                }
                
                let type_len = data[offset] as usize;
                offset += 1;
                
                if offset + type_len > data.len() {
                    return Err("Invalid Atomical type".to_string());
                }
                
                let atomical_type = match std::str::from_utf8(&data[offset..offset + type_len]) {
                    Ok(s) => match AtomicalType::from_str(s) {
                        Ok(t) => t,
                        Err(_) => return Err("Invalid Atomical type".to_string()),
                    },
                    Err(_) => return Err("Invalid Atomical type string".to_string()),
                };
                offset += type_len;
                
                // 解析元数据（如果有）
                let metadata = if offset < data.len() {
                    match serde_json::from_slice(&data[offset..]) {
                        Ok(v) => Some(v),
                        Err(_) => return Err("Invalid metadata".to_string()),
                    }
                } else {
                    None
                };
                
                Ok(Self::Mint {
                    id: AtomicalId { txid, vout },
                    atomical_type,
                    metadata,
                })
            },
            "transfer" => {
                // 解析 Atomical ID
                if offset + 36 > data.len() {
                    return Err("Invalid Atomical ID".to_string());
                }
                
                let txid = match Txid::from_slice(&data[offset..offset + 32]) {
                    Ok(id) => id,
                    Err(_) => return Err("Invalid Txid".to_string()),
                };
                offset += 32;
                
                let vout = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
                offset += 4;
                
                // 解析目标输出索引
                if offset + 4 > data.len() {
                    return Err("Invalid output index".to_string());
                }
                
                let output_index = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
                
                Ok(Self::Transfer {
                    id: AtomicalId { txid, vout },
                    output_index,
                })
            },
            "create_subdomain" => {
                // 解析子领域名称
                if offset >= data.len() {
                    return Err("Invalid subdomain name length".to_string());
                }
                
                let name_len = data[offset] as usize;
                offset += 1;
                
                if offset + name_len > data.len() {
                    return Err("Invalid subdomain name".to_string());
                }
                
                let name = match std::str::from_utf8(&data[offset..offset + name_len]) {
                    Ok(s) => s.to_string(),
                    Err(_) => return Err("Invalid subdomain name string".to_string()),
                };
                offset += name_len;
                
                // 解析可选的父域ID
                let parent_id = if offset < data.len() && data[offset] != 0 {
                    if offset + 37 > data.len() {
                        return Err("Invalid parent ID".to_string());
                    }
                    
                    offset += 1; // 跳过标志位
                    
                    let txid = match Txid::from_slice(&data[offset..offset + 32]) {
                        Ok(id) => id,
                        Err(_) => return Err("Invalid Txid".to_string()),
                    };
                    offset += 32;
                    
                    let vout = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
                    offset += 4;
                    
                    Some(AtomicalId { txid, vout })
                } else {
                    if offset < data.len() {
                        offset += 1; // 跳过标志位
                    }
                    None
                };
                
                // 解析可选的元数据
                let metadata = if offset < data.len() {
                    match serde_json::from_slice(&data[offset..]) {
                        Ok(value) => Some(value),
                        Err(_) => return Err("Invalid metadata".to_string()),
                    }
                } else {
                    None
                };
                
                Ok(Self::CreateSubdomain {
                    name,
                    parent_id,
                    metadata,
                })
            },
            "create_container" => {
                // 解析容器ID
                if offset + 36 > data.len() {
                    return Err("Invalid container ID".to_string());
                }
                
                let txid = match Txid::from_slice(&data[offset..offset + 32]) {
                    Ok(id) => id,
                    Err(_) => return Err("Invalid Txid".to_string()),
                };
                offset += 32;
                
                let vout = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
                offset += 4;
                
                // 解析容器名称
                if offset >= data.len() {
                    return Err("Invalid container name length".to_string());
                }
                
                let name_len = data[offset] as usize;
                offset += 1;
                
                if offset + name_len > data.len() {
                    return Err("Invalid container name".to_string());
                }
                
                let name = match std::str::from_utf8(&data[offset..offset + name_len]) {
                    Ok(s) => s.to_string(),
                    Err(_) => return Err("Invalid container name string".to_string()),
                };
                offset += name_len;
                
                // 解析可选的元数据
                let metadata = if offset < data.len() {
                    match serde_json::from_slice(&data[offset..]) {
                        Ok(value) => Some(value),
                        Err(_) => return Err("Invalid metadata".to_string()),
                    }
                } else {
                    None
                };
                
                Ok(Self::CreateContainer {
                    id: AtomicalId { txid, vout },
                    name,
                    metadata,
                })
            },
            "add_to_subdomain" => {
                // 解析DFT ID
                if offset + 36 > data.len() {
                    return Err("Invalid DFT ID".to_string());
                }
                
                let txid = match Txid::from_slice(&data[offset..offset + 32]) {
                    Ok(id) => id,
                    Err(_) => return Err("Invalid Txid".to_string()),
                };
                offset += 32;
                
                let vout = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
                offset += 4;
                
                // 解析子领域名称
                if offset >= data.len() {
                    return Err("Invalid subdomain name length".to_string());
                }
                
                let name_len = data[offset] as usize;
                offset += 1;
                
                if offset + name_len > data.len() {
                    return Err("Invalid subdomain name".to_string());
                }
                
                let subdomain_name = match std::str::from_utf8(&data[offset..offset + name_len]) {
                    Ok(s) => s.to_string(),
                    Err(_) => return Err("Invalid subdomain name string".to_string()),
                };
                
                Ok(Self::AddToSubdomain {
                    dft_id: AtomicalId { txid, vout },
                    subdomain_name,
                })
            },
            "add_to_container" => {
                // 解析DFT ID
                if offset + 36 > data.len() {
                    return Err("Invalid DFT ID".to_string());
                }
                
                let txid = match Txid::from_slice(&data[offset..offset + 32]) {
                    Ok(id) => id,
                    Err(_) => return Err("Invalid Txid".to_string()),
                };
                offset += 32;
                
                let vout = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
                offset += 4;
                
                // 解析容器ID
                if offset + 36 > data.len() {
                    return Err("Invalid container ID".to_string());
                }
                
                let container_txid = match Txid::from_slice(&data[offset..offset + 32]) {
                    Ok(id) => id,
                    Err(_) => return Err("Invalid Txid".to_string()),
                };
                offset += 32;
                
                let container_vout = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
                
                Ok(Self::AddToContainer {
                    dft_id: AtomicalId { txid, vout },
                    container_id: AtomicalId { txid: container_txid, vout: container_vout },
                })
            },
            "remove_from_subdomain" => {
                // 解析DFT ID
                if offset + 36 > data.len() {
                    return Err("Invalid DFT ID".to_string());
                }
                
                let txid = match Txid::from_slice(&data[offset..offset + 32]) {
                    Ok(id) => id,
                    Err(_) => return Err("Invalid Txid".to_string()),
                };
                offset += 32;
                
                let vout = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
                offset += 4;
                
                // 解析子领域名称
                if offset >= data.len() {
                    return Err("Invalid subdomain name length".to_string());
                }
                
                let name_len = data[offset] as usize;
                offset += 1;
                
                if offset + name_len > data.len() {
                    return Err("Invalid subdomain name".to_string());
                }
                
                let subdomain_name = match std::str::from_utf8(&data[offset..offset + name_len]) {
                    Ok(s) => s.to_string(),
                    Err(_) => return Err("Invalid subdomain name string".to_string()),
                };
                
                Ok(Self::RemoveFromSubdomain {
                    dft_id: AtomicalId { txid, vout },
                    subdomain_name,
                })
            },
            "remove_from_container" => {
                // 解析DFT ID
                if offset + 36 > data.len() {
                    return Err("Invalid DFT ID".to_string());
                }
                
                let txid = match Txid::from_slice(&data[offset..offset + 32]) {
                    Ok(id) => id,
                    Err(_) => return Err("Invalid Txid".to_string()),
                };
                offset += 32;
                
                let vout = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
                offset += 4;
                
                // 解析容器ID
                if offset + 36 > data.len() {
                    return Err("Invalid container ID".to_string());
                }
                
                let container_txid = match Txid::from_slice(&data[offset..offset + 32]) {
                    Ok(id) => id,
                    Err(_) => return Err("Invalid Txid".to_string()),
                };
                offset += 32;
                
                let container_vout = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
                
                Ok(Self::RemoveFromContainer {
                    dft_id: AtomicalId { txid, vout },
                    container_id: AtomicalId { txid: container_txid, vout: container_vout },
                })
            },
            "seal_container" => {
                // 解析容器ID
                if offset + 36 > data.len() {
                    return Err("Invalid container ID".to_string());
                }
                
                let txid = match Txid::from_slice(&data[offset..offset + 32]) {
                    Ok(id) => id,
                    Err(_) => return Err("Invalid Txid".to_string()),
                };
                offset += 32;
                
                let vout = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
                
                Ok(Self::SealContainer {
                    container_id: AtomicalId { txid, vout },
                })
            },
            _ => Err("Unknown operation type".to_string()),
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
            Self::CreateSubdomain { .. } => "create_subdomain",
            Self::CreateContainer { .. } => "create_container",
            Self::AddToSubdomain { .. } => "add_to_subdomain",
            Self::AddToContainer { .. } => "add_to_container",
            Self::RemoveFromSubdomain { .. } => "remove_from_subdomain",
            Self::RemoveFromContainer { .. } => "remove_from_container",
            Self::SealContainer { .. } => "seal_container",
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
    
    /// Check if the operation is a create subdomain operation
    pub fn is_create_subdomain(&self) -> bool {
        matches!(self, Self::CreateSubdomain { .. })
    }
    
    /// Check if the operation is a create container operation
    pub fn is_create_container(&self) -> bool {
        matches!(self, Self::CreateContainer { .. })
    }
    
    /// Check if the operation is an add to subdomain operation
    pub fn is_add_to_subdomain(&self) -> bool {
        matches!(self, Self::AddToSubdomain { .. })
    }
    
    /// Check if the operation is an add to container operation
    pub fn is_add_to_container(&self) -> bool {
        matches!(self, Self::AddToContainer { .. })
    }
    
    /// Check if the operation is a remove from subdomain operation
    pub fn is_remove_from_subdomain(&self) -> bool {
        matches!(self, Self::RemoveFromSubdomain { .. })
    }
    
    /// Check if the operation is a remove from container operation
    pub fn is_remove_from_container(&self) -> bool {
        matches!(self, Self::RemoveFromContainer { .. })
    }
    
    /// Check if the operation is a seal container operation
    pub fn is_seal_container(&self) -> bool {
        matches!(self, Self::SealContainer { .. })
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
        assert!(AtomicalOperation::from_tx_data(&[]).is_err());

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
            AtomicalOperation::CreateSubdomain {
                name: "test_subdomain".to_string(),
                parent_id: None,
                metadata: None,
            },
            AtomicalOperation::CreateContainer {
                id: atomical_id,
                name: "test_container".to_string(),
                metadata: None,
            },
            AtomicalOperation::AddToSubdomain {
                dft_id: atomical_id,
                subdomain_name: "test_subdomain".to_string(),
            },
            AtomicalOperation::AddToContainer {
                dft_id: atomical_id,
                container_id: atomical_id,
            },
            AtomicalOperation::RemoveFromSubdomain {
                dft_id: atomical_id,
                subdomain_name: "test_subdomain".to_string(),
            },
            AtomicalOperation::RemoveFromContainer {
                dft_id: atomical_id,
                container_id: atomical_id,
            },
            AtomicalOperation::SealContainer { container_id: atomical_id },
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
                    assert!(!op.is_create_subdomain());
                    assert!(!op.is_create_container());
                    assert!(!op.is_add_to_subdomain());
                    assert!(!op.is_add_to_container());
                    assert!(!op.is_remove_from_subdomain());
                    assert!(!op.is_remove_from_container());
                    assert!(!op.is_seal_container());
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
                    assert!(!op.is_create_subdomain());
                    assert!(!op.is_create_container());
                    assert!(!op.is_add_to_subdomain());
                    assert!(!op.is_add_to_container());
                    assert!(!op.is_remove_from_subdomain());
                    assert!(!op.is_remove_from_container());
                    assert!(!op.is_seal_container());
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
                    assert!(!op.is_create_subdomain());
                    assert!(!op.is_create_container());
                    assert!(!op.is_add_to_subdomain());
                    assert!(!op.is_add_to_container());
                    assert!(!op.is_remove_from_subdomain());
                    assert!(!op.is_remove_from_container());
                    assert!(!op.is_seal_container());
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
                    assert!(!op.is_create_subdomain());
                    assert!(!op.is_create_container());
                    assert!(!op.is_add_to_subdomain());
                    assert!(!op.is_add_to_container());
                    assert!(!op.is_remove_from_subdomain());
                    assert!(!op.is_remove_from_container());
                    assert!(!op.is_seal_container());
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
                    assert!(!op.is_create_subdomain());
                    assert!(!op.is_create_container());
                    assert!(!op.is_add_to_subdomain());
                    assert!(!op.is_add_to_container());
                    assert!(!op.is_remove_from_subdomain());
                    assert!(!op.is_remove_from_container());
                    assert!(!op.is_seal_container());
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
                    assert!(!op.is_create_subdomain());
                    assert!(!op.is_create_container());
                    assert!(!op.is_add_to_subdomain());
                    assert!(!op.is_add_to_container());
                    assert!(!op.is_remove_from_subdomain());
                    assert!(!op.is_remove_from_container());
                    assert!(!op.is_seal_container());
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
                    assert!(!op.is_create_subdomain());
                    assert!(!op.is_create_container());
                    assert!(!op.is_add_to_subdomain());
                    assert!(!op.is_add_to_container());
                    assert!(!op.is_remove_from_subdomain());
                    assert!(!op.is_remove_from_container());
                    assert!(!op.is_seal_container());
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
                    assert!(!op.is_create_subdomain());
                    assert!(!op.is_create_container());
                    assert!(!op.is_add_to_subdomain());
                    assert!(!op.is_add_to_container());
                    assert!(!op.is_remove_from_subdomain());
                    assert!(!op.is_remove_from_container());
                    assert!(!op.is_seal_container());
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
                    assert!(!op.is_create_subdomain());
                    assert!(!op.is_create_container());
                    assert!(!op.is_add_to_subdomain());
                    assert!(!op.is_add_to_container());
                    assert!(!op.is_remove_from_subdomain());
                    assert!(!op.is_remove_from_container());
                    assert!(!op.is_seal_container());
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
                    assert!(!op.is_create_subdomain());
                    assert!(!op.is_create_container());
                    assert!(!op.is_add_to_subdomain());
                    assert!(!op.is_add_to_container());
                    assert!(!op.is_remove_from_subdomain());
                    assert!(!op.is_remove_from_container());
                    assert!(!op.is_seal_container());
                }
                AtomicalOperation::CreateSubdomain { .. } => {
                    assert!(!op.is_mint());
                    assert!(!op.is_transfer());
                    assert!(!op.is_update());
                    assert!(!op.is_seal());
                    assert!(!op.is_deploy_dft());
                    assert!(!op.is_mint_dft());
                    assert!(!op.is_event());
                    assert!(!op.is_data());
                    assert!(!op.is_extract());
                    assert!(!op.is_split());
                    assert!(op.is_create_subdomain());
                    assert!(!op.is_create_container());
                    assert!(!op.is_add_to_subdomain());
                    assert!(!op.is_add_to_container());
                    assert!(!op.is_remove_from_subdomain());
                    assert!(!op.is_remove_from_container());
                    assert!(!op.is_seal_container());
                }
                AtomicalOperation::CreateContainer { .. } => {
                    assert!(!op.is_mint());
                    assert!(!op.is_transfer());
                    assert!(!op.is_update());
                    assert!(!op.is_seal());
                    assert!(!op.is_deploy_dft());
                    assert!(!op.is_mint_dft());
                    assert!(!op.is_event());
                    assert!(!op.is_data());
                    assert!(!op.is_extract());
                    assert!(!op.is_split());
                    assert!(!op.is_create_subdomain());
                    assert!(op.is_create_container());
                    assert!(!op.is_add_to_subdomain());
                    assert!(!op.is_add_to_container());
                    assert!(!op.is_remove_from_subdomain());
                    assert!(!op.is_remove_from_container());
                    assert!(!op.is_seal_container());
                }
                AtomicalOperation::AddToSubdomain { .. } => {
                    assert!(!op.is_mint());
                    assert!(!op.is_transfer());
                    assert!(!op.is_update());
                    assert!(!op.is_seal());
                    assert!(!op.is_deploy_dft());
                    assert!(!op.is_mint_dft());
                    assert!(!op.is_event());
                    assert!(!op.is_data());
                    assert!(!op.is_extract());
                    assert!(!op.is_split());
                    assert!(!op.is_create_subdomain());
                    assert!(!op.is_create_container());
                    assert!(op.is_add_to_subdomain());
                    assert!(!op.is_add_to_container());
                    assert!(!op.is_remove_from_subdomain());
                    assert!(!op.is_remove_from_container());
                    assert!(!op.is_seal_container());
                }
                AtomicalOperation::AddToContainer { .. } => {
                    assert!(!op.is_mint());
                    assert!(!op.is_transfer());
                    assert!(!op.is_update());
                    assert!(!op.is_seal());
                    assert!(!op.is_deploy_dft());
                    assert!(!op.is_mint_dft());
                    assert!(!op.is_event());
                    assert!(!op.is_data());
                    assert!(!op.is_extract());
                    assert!(!op.is_split());
                    assert!(!op.is_create_subdomain());
                    assert!(!op.is_create_container());
                    assert!(!op.is_add_to_subdomain());
                    assert!(op.is_add_to_container());
                    assert!(!op.is_remove_from_subdomain());
                    assert!(!op.is_remove_from_container());
                    assert!(!op.is_seal_container());
                }
                AtomicalOperation::RemoveFromSubdomain { .. } => {
                    assert!(!op.is_mint());
                    assert!(!op.is_transfer());
                    assert!(!op.is_update());
                    assert!(!op.is_seal());
                    assert!(!op.is_deploy_dft());
                    assert!(!op.is_mint_dft());
                    assert!(!op.is_event());
                    assert!(!op.is_data());
                    assert!(!op.is_extract());
                    assert!(!op.is_split());
                    assert!(!op.is_create_subdomain());
                    assert!(!op.is_create_container());
                    assert!(!op.is_add_to_subdomain());
                    assert!(!op.is_add_to_container());
                    assert!(op.is_remove_from_subdomain());
                    assert!(!op.is_remove_from_container());
                    assert!(!op.is_seal_container());
                }
                AtomicalOperation::RemoveFromContainer { .. } => {
                    assert!(!op.is_mint());
                    assert!(!op.is_transfer());
                    assert!(!op.is_update());
                    assert!(!op.is_seal());
                    assert!(!op.is_deploy_dft());
                    assert!(!op.is_mint_dft());
                    assert!(!op.is_event());
                    assert!(!op.is_data());
                    assert!(!op.is_extract());
                    assert!(!op.is_split());
                    assert!(!op.is_create_subdomain());
                    assert!(!op.is_create_container());
                    assert!(!op.is_add_to_subdomain());
                    assert!(!op.is_add_to_container());
                    assert!(!op.is_remove_from_subdomain());
                    assert!(op.is_remove_from_container());
                    assert!(!op.is_seal_container());
                }
                AtomicalOperation::SealContainer { .. } => {
                    assert!(!op.is_mint());
                    assert!(!op.is_transfer());
                    assert!(!op.is_update());
                    assert!(!op.is_seal());
                    assert!(!op.is_deploy_dft());
                    assert!(!op.is_mint_dft());
                    assert!(!op.is_event());
                    assert!(!op.is_data());
                    assert!(!op.is_extract());
                    assert!(!op.is_split());
                    assert!(!op.is_create_subdomain());
                    assert!(!op.is_create_container());
                    assert!(!op.is_add_to_subdomain());
                    assert!(!op.is_add_to_container());
                    assert!(!op.is_remove_from_subdomain());
                    assert!(!op.is_remove_from_container());
                    assert!(op.is_seal_container());
                }
            }
        }
    }

    #[test]
    fn test_create_subdomain_operation() {
        // 创建测试数据
        let mut data = Vec::new();
        data.extend_from_slice(b"atom");
        
        // 操作类型
        data.push(16); // 操作类型长度
        data.extend_from_slice(b"create_subdomain");
        
        // 子领域名称
        data.push(7); // 名称长度
        data.extend_from_slice(b"testdft");
        
        // 父域ID标志（无父域）
        data.push(0);
        
        // 元数据
        let metadata = r#"{"description":"Test Subdomain"}"#;
        data.extend_from_slice(metadata.as_bytes());
        
        // 解析操作
        let operation = AtomicalOperation::from_tx_data(&data).unwrap();
        
        // 验证结果
        match operation {
            AtomicalOperation::CreateSubdomain { name, parent_id, metadata } => {
                assert_eq!(name, "testdft");
                assert!(parent_id.is_none());
                assert!(metadata.is_some());
                if let Some(meta) = metadata {
                    assert_eq!(meta["description"], "Test Subdomain");
                }
            },
            _ => panic!("Expected CreateSubdomain operation"),
        }
        
        // 测试带父域ID的情况
        let mut data_with_parent = Vec::new();
        data_with_parent.extend_from_slice(b"atom");
        
        // 操作类型
        data_with_parent.push(16); // 操作类型长度
        data_with_parent.extend_from_slice(b"create_subdomain");
        
        // 子领域名称
        data_with_parent.push(7); // 名称长度
        data_with_parent.extend_from_slice(b"testdft");
        
        // 父域ID标志（有父域）
        data_with_parent.push(1);
        
        // 父域ID
        let parent_txid = [0u8; 32];
        data_with_parent.extend_from_slice(&parent_txid);
        data_with_parent.extend_from_slice(&[0, 0, 0, 0]); // vout
        
        // 解析操作
        let operation_with_parent = AtomicalOperation::from_tx_data(&data_with_parent).unwrap();
        
        // 验证结果
        match operation_with_parent {
            AtomicalOperation::CreateSubdomain { name, parent_id, metadata } => {
                assert_eq!(name, "testdft");
                assert!(parent_id.is_some());
                assert!(metadata.is_none());
            },
            _ => panic!("Expected CreateSubdomain operation with parent_id"),
        }
    }

    #[test]
    fn test_create_container_operation() {
        // 创建测试数据
        let mut data = Vec::new();
        data.extend_from_slice(b"atom");
        
        // 操作类型
        data.push(16); // 操作类型长度
        data.extend_from_slice(b"create_container");
        
        // 容器ID
        let container_txid = [0u8; 32];
        data.extend_from_slice(&container_txid);
        data.extend_from_slice(&[0, 0, 0, 0]); // vout
        
        // 容器名称
        data.push(10); // 名称长度
        data.extend_from_slice(b"testcontainer");
        
        // 元数据
        let metadata = r#"{"description":"Test Container"}"#;
        data.extend_from_slice(metadata.as_bytes());
        
        // 解析操作
        let operation = AtomicalOperation::from_tx_data(&data).unwrap();
        
        // 验证结果
        match operation {
            AtomicalOperation::CreateContainer { id, name, metadata } => {
                assert_eq!(id.txid, Txid::from_raw_hash(bitcoin::hashes::Hash::all_zeros()));
                assert_eq!(id.vout, 0);
                assert_eq!(name, "testcontainer");
                assert!(metadata.is_some());
                if let Some(meta) = metadata {
                    assert_eq!(meta["description"], "Test Container");
                }
            },
            _ => panic!("Expected CreateContainer operation"),
        }
    }

    #[test]
    fn test_add_to_subdomain_operation() {
        // 创建测试数据
        let mut data = Vec::new();
        data.extend_from_slice(b"atom");
        
        // 操作类型
        data.push(15); // 操作类型长度
        data.extend_from_slice(b"add_to_subdomain");
        
        // DFT ID
        let dft_txid = [0u8; 32];
        data.extend_from_slice(&dft_txid);
        data.extend_from_slice(&[0, 0, 0, 0]); // vout
        
        // 子领域名称
        data.push(7); // 名称长度
        data.extend_from_slice(b"testdft");
        
        // 解析操作
        let operation = AtomicalOperation::from_tx_data(&data).unwrap();
        
        // 验证结果
        match operation {
            AtomicalOperation::AddToSubdomain { dft_id, subdomain_name } => {
                assert_eq!(dft_id.txid, Txid::from_raw_hash(bitcoin::hashes::Hash::all_zeros()));
                assert_eq!(dft_id.vout, 0);
                assert_eq!(subdomain_name, "testdft");
            },
            _ => panic!("Expected AddToSubdomain operation"),
        }
    }

    #[test]
    fn test_add_to_container_operation() {
        // 创建测试数据
        let mut data = Vec::new();
        data.extend_from_slice(b"atom");
        
        // 操作类型
        data.push(15); // 操作类型长度
        data.extend_from_slice(b"add_to_container");
        
        // DFT ID
        let dft_txid = [0u8; 32];
        data.extend_from_slice(&dft_txid);
        data.extend_from_slice(&[0, 0, 0, 0]); // vout
        
        // 容器ID
        let container_txid = [1u8; 32];
        data.extend_from_slice(&container_txid);
        data.extend_from_slice(&[0, 0, 0, 1]); // vout
        
        // 解析操作
        let operation = AtomicalOperation::from_tx_data(&data).unwrap();
        
        // 验证结果
        match operation {
            AtomicalOperation::AddToContainer { dft_id, container_id } => {
                assert_eq!(dft_id.txid, Txid::from_raw_hash(bitcoin::hashes::Hash::all_zeros()));
                assert_eq!(dft_id.vout, 0);
                assert_eq!(container_id.txid, Txid::from_slice(&[1u8; 32]).unwrap());
                assert_eq!(container_id.vout, 1);
            },
            _ => panic!("Expected AddToContainer operation"),
        }
    }

    #[test]
    fn test_remove_from_subdomain_operation() {
        // 创建测试数据
        let mut data = Vec::new();
        data.extend_from_slice(b"atom");
        
        // 操作类型
        data.push(20); // 操作类型长度
        data.extend_from_slice(b"remove_from_subdomain");
        
        // DFT ID
        let dft_txid = [0u8; 32];
        data.extend_from_slice(&dft_txid);
        data.extend_from_slice(&[0, 0, 0, 0]); // vout
        
        // 子领域名称
        data.push(7); // 名称长度
        data.extend_from_slice(b"testdft");
        
        // 解析操作
        let operation = AtomicalOperation::from_tx_data(&data).unwrap();
        
        // 验证结果
        match operation {
            AtomicalOperation::RemoveFromSubdomain { dft_id, subdomain_name } => {
                assert_eq!(dft_id.txid, Txid::from_raw_hash(bitcoin::hashes::Hash::all_zeros()));
                assert_eq!(dft_id.vout, 0);
                assert_eq!(subdomain_name, "testdft");
            },
            _ => panic!("Expected RemoveFromSubdomain operation"),
        }
    }

    #[test]
    fn test_remove_from_container_operation() {
        // 创建测试数据
        let mut data = Vec::new();
        data.extend_from_slice(b"atom");
        
        // 操作类型
        data.push(20); // 操作类型长度
        data.extend_from_slice(b"remove_from_container");
        
        // DFT ID
        let dft_txid = [0u8; 32];
        data.extend_from_slice(&dft_txid);
        data.extend_from_slice(&[0, 0, 0, 0]); // vout
        
        // 容器ID
        let container_txid = [1u8; 32];
        data.extend_from_slice(&container_txid);
        data.extend_from_slice(&[0, 0, 0, 1]); // vout
        
        // 解析操作
        let operation = AtomicalOperation::from_tx_data(&data).unwrap();
        
        // 验证结果
        match operation {
            AtomicalOperation::RemoveFromContainer { dft_id, container_id } => {
                assert_eq!(dft_id.txid, Txid::from_raw_hash(bitcoin::hashes::Hash::all_zeros()));
                assert_eq!(dft_id.vout, 0);
                assert_eq!(container_id.txid, Txid::from_slice(&[1u8; 32]).unwrap());
                assert_eq!(container_id.vout, 1);
            },
            _ => panic!("Expected RemoveFromContainer operation"),
        }
    }

    #[test]
    fn test_seal_container_operation() {
        // 创建测试数据
        let mut data = Vec::new();
        data.extend_from_slice(b"atom");
        
        // 操作类型
        data.push(14); // 操作类型长度
        data.extend_from_slice(b"seal_container");
        
        // 容器ID
        let container_txid = [1u8; 32];
        data.extend_from_slice(&container_txid);
        data.extend_from_slice(&[0, 0, 0, 1]); // vout
        
        // 解析操作
        let operation = AtomicalOperation::from_tx_data(&data).unwrap();
        
        // 验证结果
        match operation {
            AtomicalOperation::SealContainer { container_id } => {
                assert_eq!(container_id.txid, Txid::from_slice(&[1u8; 32]).unwrap());
                assert_eq!(container_id.vout, 1);
            },
            _ => panic!("Expected SealContainer operation"),
        }
    }
}

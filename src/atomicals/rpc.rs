use anyhow::{anyhow, Result};
use bitcoin::{Address, Network, Transaction, Txid};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

use super::protocol::{AtomicalId, AtomicalOperation, AtomicalType};
use super::state::AtomicalsState;
use super::storage::AtomicalsStorage;

/// Atomicals RPC 处理器
pub struct AtomicalsRpc {
    network: Network,
    state: Arc<AtomicalsState>,
    storage: Arc<AtomicalsStorage>,
}

/// Atomical 信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomicalInfo {
    /// Atomical ID
    pub id: AtomicalId,
    /// 类型
    pub atomical_type: AtomicalType,
    /// 所有者地址
    pub owner: String,
    /// 值
    pub value: u64,
    /// 元数据
    pub metadata: Option<Value>,
    /// 创建高度
    pub created_height: u32,
    /// 创建时间
    pub created_timestamp: u64,
    /// 是否已封印
    pub sealed: bool,
}

impl AtomicalsRpc {
    /// 创建新的 RPC 处理器
    pub fn new(
        network: Network,
        state: Arc<AtomicalsState>,
        storage: Arc<AtomicalsStorage>,
    ) -> Self {
        Self {
            network,
            state,
            storage,
        }
    }

    /// 获取 Atomical 信息
    pub fn get_atomical_info(&self, id: &AtomicalId) -> Result<AtomicalInfo> {
        // 检查是否存在
        if !self.state.exists(id)? {
            return Err(anyhow!("Atomical not found"));
        }

        // 获取输出信息
        let output = self.state.get_output(id)?.ok_or_else(|| anyhow!("Output not found"))?;

        // 获取元数据
        let metadata = self.state.get_metadata(id)?;

        // 获取封印状态
        let sealed = self.state.is_sealed(id)?;

        // 转换地址
        let owner = Address::from_script(&bitcoin::Script::from_bytes(&output.owner.script_pubkey), self.network)
            .map_err(|_| anyhow!("Invalid script"))?
            .to_string();

        Ok(AtomicalInfo {
            id: id.clone(),
            atomical_type: AtomicalType::NFT, // 从状态中获取
            owner,
            value: output.owner.value,
            metadata,
            created_height: output.height,
            created_timestamp: output.timestamp,
            sealed,
        })
    }

    /// 获取地址拥有的 Atomicals
    pub fn get_atomicals_by_address(&self, address: &str) -> Result<Vec<AtomicalInfo>> {
        // 解析地址
        let addr = Address::from_str(address).map_err(|_| anyhow!("Invalid address"))?;
        let script = addr.script_pubkey();

        // 查询地址拥有的 Atomicals
        let atomicals = self.storage.get_atomicals_by_script(&script.as_bytes())?;

        // 获取每个 Atomical 的详细信息
        let mut result = Vec::new();
        for id in atomicals {
            if let Ok(info) = self.get_atomical_info(&id) {
                result.push(info);
            }
        }

        Ok(result)
    }

    /// 获取未确认的 Atomicals 操作
    pub fn get_pending_operations(&self) -> Result<HashMap<Txid, Vec<AtomicalOperation>>> {
        Ok(self.state.get_pending_operations()?)
    }

    /// 搜索 Atomicals
    pub fn search_atomicals(&self, query: &str) -> Result<Vec<AtomicalInfo>> {
        // 在元数据中搜索
        let results = self.storage.search_metadata(query)?;

        // 获取每个结果的详细信息
        let mut atomicals = Vec::new();
        for id in results {
            if let Ok(info) = self.get_atomical_info(&id) {
                atomicals.push(info);
            }
        }

        Ok(atomicals)
    }

    /// 获取 Atomicals 统计信息
    pub fn get_stats(&self) -> Result<Value> {
        Ok(serde_json::json!({
            "total_atomicals": self.state.get_total_count()?,
            "total_nft": self.state.get_nft_count()?,
            "total_ft": self.state.get_ft_count()?,
            "total_sealed": self.state.get_sealed_count()?,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use bitcoin::hashes::hex::FromHex;
    use bitcoin::secp256k1::rand::{self, Rng};
    use std::collections::HashMap;

    fn create_test_rpc() -> AtomicalsRpc {
        let network = Network::Regtest;
        let state = Arc::new(AtomicalsState::new());
        let storage = Arc::new(AtomicalsStorage::new_test().unwrap());
        AtomicalsRpc::new(network, state, storage)
    }

    fn create_test_atomical_id() -> AtomicalId {
        AtomicalId {
            txid: Txid::from_str(
                "1234567890123456789012345678901234567890123456789012345678901234"
            ).unwrap(),
            vout: 0,
        }
    }

    fn create_test_address() -> String {
        "bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080".to_string()
    }

    fn create_test_script() -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let mut script = vec![0; 32];
        rng.fill(&mut script[..]);
        script
    }

    #[test]
    fn test_get_atomical_info() -> Result<()> {
        let rpc = create_test_rpc();
        let id = create_test_atomical_id();

        // 测试不存在的 Atomical
        assert!(rpc.get_atomical_info(&id).is_err());

        // 创建一个测试 Atomical
        let script = create_test_script();
        let output = AtomicalOutput {
            owner: OwnerInfo {
                script_pubkey: script.clone(),
                value: 1000,
            },
            atomical_id: id.clone(),
            metadata: Some(json!({
                "name": "Test NFT",
                "description": "Test Description"
            })),
            height: 100,
            timestamp: 1234567890,
        };

        // 添加到状态
        rpc.state.add_atomical(&id, AtomicalType::NFT)?;
        rpc.state.update_output(&id, &output)?;
        rpc.state.set_metadata(&id, output.metadata.as_ref().unwrap())?;

        // 测试获取信息
        let info = rpc.get_atomical_info(&id)?;
        assert_eq!(info.id, id);
        assert_eq!(info.atomical_type, AtomicalType::NFT);
        assert_eq!(info.value, 1000);
        assert_eq!(info.created_height, 100);
        assert_eq!(info.created_timestamp, 1234567890);
        assert!(!info.sealed);

        // 测试元数据
        let metadata = info.metadata.unwrap();
        assert_eq!(metadata["name"], "Test NFT");
        assert_eq!(metadata["description"], "Test Description");

        Ok(())
    }

    #[test]
    fn test_get_atomicals_by_address() -> Result<()> {
        let rpc = create_test_rpc();
        let address = create_test_address();
        let id = create_test_atomical_id();

        // 测试空地址
        let atomicals = rpc.get_atomicals_by_address(&address)?;
        assert!(atomicals.is_empty());

        // 创建一个测试 Atomical 并关联到地址
        let addr = Address::from_str(&address)?;
        let script = addr.script_pubkey();
        let output = AtomicalOutput {
            owner: OwnerInfo {
                script_pubkey: script.as_bytes().to_vec(),
                value: 1000,
            },
            atomical_id: id.clone(),
            metadata: Some(json!({
                "name": "Test NFT",
                "description": "Test Description"
            })),
            height: 100,
            timestamp: 1234567890,
        };

        // 添加到状态
        rpc.state.add_atomical(&id, AtomicalType::NFT)?;
        rpc.state.update_output(&id, &output)?;
        rpc.state.set_metadata(&id, output.metadata.as_ref().unwrap())?;

        // 添加到存储
        rpc.storage.store_output(&id, &output)?;

        // 测试地址查询
        let atomicals = rpc.get_atomicals_by_address(&address)?;
        assert_eq!(atomicals.len(), 1);
        assert_eq!(atomicals[0].id, id);
        assert_eq!(atomicals[0].value, 1000);

        // 测试无效地址
        assert!(rpc.get_atomicals_by_address("invalid_address").is_err());

        Ok(())
    }

    #[test]
    fn test_get_pending_operations() -> Result<()> {
        let rpc = create_test_rpc();
        let id = create_test_atomical_id();

        // 创建一些测试操作
        let operations = vec![
            AtomicalOperation::Mint {
                atomical_type: AtomicalType::NFT,
                metadata: json!({"name": "Test NFT 1"}),
            },
            AtomicalOperation::Update {
                atomical_id: id.clone(),
                metadata: json!({"name": "Updated NFT"}),
            },
        ];

        // 添加到待处理操作
        let txid = Txid::from_str(
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        )?;
        rpc.state.add_pending_operations(txid, operations.clone())?;

        // 测试获取待处理操作
        let pending = rpc.get_pending_operations()?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[&txid].len(), 2);

        // 验证操作内容
        match &pending[&txid][0] {
            AtomicalOperation::Mint { atomical_type, metadata } => {
                assert_eq!(*atomical_type, AtomicalType::NFT);
                assert_eq!(metadata["name"], "Test NFT 1");
            }
            _ => panic!("Expected Mint operation"),
        }

        match &pending[&txid][1] {
            AtomicalOperation::Update { atomical_id, metadata } => {
                assert_eq!(*atomical_id, id);
                assert_eq!(metadata["name"], "Updated NFT");
            }
            _ => panic!("Expected Update operation"),
        }

        Ok(())
    }

    #[test]
    fn test_search_atomicals() -> Result<()> {
        let rpc = create_test_rpc();
        let id = create_test_atomical_id();

        // 创建一些测试 Atomicals
        let metadata1 = json!({
            "name": "Test NFT 1",
            "description": "First test NFT",
            "attributes": [{"trait_type": "Color", "value": "Blue"}]
        });

        let metadata2 = json!({
            "name": "Test NFT 2",
            "description": "Second test NFT",
            "attributes": [{"trait_type": "Color", "value": "Red"}]
        });

        // 添加到状态和存储
        let script = create_test_script();
        let output1 = AtomicalOutput {
            owner: OwnerInfo {
                script_pubkey: script.clone(),
                value: 1000,
            },
            atomical_id: id.clone(),
            metadata: Some(metadata1.clone()),
            height: 100,
            timestamp: 1234567890,
        };

        let id2 = AtomicalId {
            txid: Txid::from_str(
                "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
            )?,
            vout: 0,
        };

        let output2 = AtomicalOutput {
            owner: OwnerInfo {
                script_pubkey: script,
                value: 2000,
            },
            atomical_id: id2.clone(),
            metadata: Some(metadata2.clone()),
            height: 101,
            timestamp: 1234567891,
        };

        // 添加到状态
        rpc.state.add_atomical(&id, AtomicalType::NFT)?;
        rpc.state.update_output(&id, &output1)?;
        rpc.state.set_metadata(&id, &metadata1)?;

        rpc.state.add_atomical(&id2, AtomicalType::NFT)?;
        rpc.state.update_output(&id2, &output2)?;
        rpc.state.set_metadata(&id2, &metadata2)?;

        // 添加到存储
        rpc.storage.store_output(&id, &output1)?;
        rpc.storage.store_metadata(&id, &metadata1)?;
        rpc.storage.store_output(&id2, &output2)?;
        rpc.storage.store_metadata(&id2, &metadata2)?;

        // 测试搜索
        let results = rpc.search_atomicals("Blue")?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, id);

        let results = rpc.search_atomicals("Red")?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, id2);

        let results = rpc.search_atomicals("Test NFT")?;
        assert_eq!(results.len(), 2);

        // 测试空搜索
        let results = rpc.search_atomicals("NonExistent")?;
        assert!(results.is_empty());

        Ok(())
    }

    #[test]
    fn test_get_stats() -> Result<()> {
        let rpc = create_test_rpc();
        let id1 = create_test_atomical_id();
        let id2 = AtomicalId {
            txid: Txid::from_str(
                "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
            )?,
            vout: 0,
        };

        // 添加一些测试数据
        rpc.state.add_atomical(&id1, AtomicalType::NFT)?;
        rpc.state.add_atomical(&id2, AtomicalType::FT)?;
        rpc.state.seal_atomical(&id1)?;

        // 测试统计信息
        let stats = rpc.get_stats()?;
        assert_eq!(stats["total_atomicals"], 2);
        assert_eq!(stats["total_nft"], 1);
        assert_eq!(stats["total_ft"], 1);
        assert_eq!(stats["total_sealed"], 1);

        Ok(())
    }

    #[test]
    fn test_invalid_cases() -> Result<()> {
        let rpc = create_test_rpc();

        // 测试无效的 Atomical ID
        let invalid_id = AtomicalId {
            txid: Txid::from_str(
                "0000000000000000000000000000000000000000000000000000000000000000"
            )?,
            vout: 0,
        };
        assert!(rpc.get_atomical_info(&invalid_id).is_err());

        // 测试无效的地址
        assert!(rpc.get_atomicals_by_address("invalid_address").is_err());

        // 测试无效的搜索查询
        let results = rpc.search_atomicals("")?;
        assert!(results.is_empty());

        Ok(())
    }
}

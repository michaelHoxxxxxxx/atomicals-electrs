use std::collections::HashMap;
use std::sync::Arc;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use bitcoin::{Address, Network, Transaction, Txid};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::RwLock;

use crate::atomicals::{
    AtomicalId, AtomicalOperation, AtomicalType, AtomicalsState,
    storage::AtomicalsStorage, state::{AtomicalOutput, OwnerInfo},
    dft::{DftManager, DftInfo, DftSupplyInfo, DftHolder, DftMintEvent, DftSubdomain, DftContainer},
};

/// Atomical 信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomicalInfo {
    /// Atomical ID
    pub id: AtomicalId,
    /// 所有者地址
    pub owner: Option<String>,
    /// 元数据
    pub metadata: Option<Value>,
    /// 状态
    pub state: Option<Value>,
    /// 类型
    pub atomical_type: AtomicalType,
    /// 值
    pub value: u64,
    /// 创建高度
    pub created_height: u32,
    /// 创建时间
    pub created_timestamp: u64,
    /// 是否已封印
    pub sealed: bool,
}

/// Atomicals RPC 处理器
pub struct AtomicalsRpc {
    state: Arc<AtomicalsState>,
    storage: Arc<AtomicalsStorage>,
    network: Network,
    dft_manager: Arc<RwLock<DftManager>>,
}

impl AtomicalsRpc {
    pub fn new(
        network: Network,
        state: Arc<AtomicalsState>,
        storage: Arc<AtomicalsStorage>,
        dft_manager: Arc<RwLock<DftManager>>,
    ) -> Self {
        Self {
            state,
            storage,
            network,
            dft_manager,
        }
    }

    /// 获取 Atomical 信息
    pub fn get_atomical_info(&self, id: &AtomicalId) -> Result<AtomicalInfo> {
        // 检查是否存在
        if !self.state.exists(id)? {
            return Err(anyhow!("Atomical not found"));
        }

        // 获取输出
        let output = self.state.get_output_sync(id)?.ok_or_else(|| anyhow!("Output not found"))?;

        // 获取元数据
        let metadata = self.state.get_metadata_sync(id)?;

        // 获取封印状态
        let sealed = self.state.is_sealed(id)?;

        // 转换地址
        let owner = Address::from_script(&bitcoin::Script::from_bytes(&output.output.script_pubkey.as_bytes()), self.network)
            .map_err(|_| anyhow!("Invalid script"))?;

        Ok(AtomicalInfo {
            id: id.clone(),
            atomical_type: AtomicalType::NFT, // 从状态中获取
            owner: Some(owner.to_string()),
            value: output.output.value.to_sat(),
            metadata,
            created_height: output.height,
            created_timestamp: output.timestamp,
            sealed,
            state: None,
        })
    }

    /// 获取地址拥有的 Atomicals
    pub fn get_atomicals_by_address(&self, address: &str) -> Result<Vec<AtomicalInfo>> {
        // 解析地址
        let addr = Address::from_str(address).map_err(|_| anyhow!("Invalid address"))?;
        // 需要为地址指定网络类型
        let addr = addr.require_network(self.network).map_err(|_| anyhow!("Invalid network for address"))?;
        let script = addr.script_pubkey();

        // 查询地址拥有的 Atomicals
        let outputs = self.storage.get_script_atomicals(&script.as_bytes())?;
        
        // 获取每个 Atomical 的详细信息
        let mut result = Vec::new();
        for output in outputs {
            if let Ok(info) = self.get_atomical_info(&output.atomical_id) {
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
    pub async fn get_stats(&self) -> Result<Value> {
        Ok(serde_json::json!({
            "total_atomicals": self.state.get_total_count().await?,
            "total_nft": self.state.get_nft_count().await?,
            "total_ft": self.state.get_ft_count().await?,
            "total_sealed": self.state.get_sealed_count().await?,
        }))
    }

    /// 获取 Atomicals 统计信息（同步版本）
    pub fn get_stats_sync(&self) -> Result<Value> {
        Ok(serde_json::json!({
            "total_atomicals": self.state.get_total_count_sync()?,
            "total_nft": self.state.get_nft_count_sync()?,
            "total_ft": self.state.get_ft_count_sync()?,
            "total_sealed": self.state.get_sealed_count_sync()?,
        }))
    }

    /// 获取 DFT 信息
    pub async fn get_dft_info(&self, id: &AtomicalId) -> Result<DftInfo> {
        let dft_manager = self.dft_manager.read().await;
        dft_manager.get_dft_info(id)
    }

    /// 通过 ticker 获取 DFT 信息
    pub async fn get_dft_info_by_ticker(&self, ticker: &str) -> Result<DftInfo> {
        let dft_manager = self.dft_manager.read().await;
        dft_manager.get_dft_info_by_ticker(ticker)
    }

    /// 获取 DFT 供应量信息
    pub async fn get_dft_supply_info(&self, id: &AtomicalId) -> Result<DftSupplyInfo> {
        let dft_manager = self.dft_manager.read().await;
        dft_manager.get_supply_info(id)
    }

    /// 通过 ticker 获取 DFT 供应量信息
    pub async fn get_dft_supply_info_by_ticker(&self, ticker: &str) -> Result<DftSupplyInfo> {
        let dft_manager = self.dft_manager.read().await;
        let id = dft_manager.get_dft_info_by_ticker(ticker)?.id;
        dft_manager.get_supply_info(&id)
    }

    /// 获取 DFT 持有者列表
    pub async fn get_dft_holders(&self, id: &AtomicalId) -> Result<Vec<DftHolder>> {
        let dft_manager = self.dft_manager.read().await;
        dft_manager.get_holders(id)
    }

    /// 通过 ticker 获取 DFT 持有者列表
    pub async fn get_dft_holders_by_ticker(&self, ticker: &str) -> Result<Vec<DftHolder>> {
        let dft_manager = self.dft_manager.read().await;
        let id = dft_manager.get_dft_info_by_ticker(ticker)?.id;
        dft_manager.get_holders(&id)
    }

    /// 获取 DFT 铸造历史
    pub async fn get_dft_mint_history(&self, id: &AtomicalId) -> Result<Vec<DftMintEvent>> {
        let dft_manager = self.dft_manager.read().await;
        dft_manager.get_mint_history(id)
    }

    /// 通过 ticker 获取 DFT 铸造历史
    pub async fn get_dft_mint_history_by_ticker(&self, ticker: &str) -> Result<Vec<DftMintEvent>> {
        let dft_manager = self.dft_manager.read().await;
        let id = dft_manager.get_dft_info_by_ticker(ticker)?.id;
        dft_manager.get_mint_history(&id)
    }

    /// 获取所有 DFT 列表
    pub async fn get_all_dfts(&self) -> Result<Vec<DftInfo>> {
        let dft_manager = self.dft_manager.read().await;
        Ok(dft_manager.get_all_dfts())
    }

    /// 获取 DFT 总数
    pub async fn get_dft_count(&self) -> Result<usize> {
        let dft_manager = self.dft_manager.read().await;
        Ok(dft_manager.get_total_count())
    }

    /// 获取所有 DFT 子领域
    pub async fn get_all_dft_subdomains(&self) -> Result<Vec<DftSubdomain>> {
        let dft_manager = self.dft_manager.read().await;
        Ok(dft_manager.get_all_subdomains())
    }

    /// 获取子领域信息
    pub async fn get_dft_subdomain(&self, name: &str) -> Result<DftSubdomain> {
        let dft_manager = self.dft_manager.read().await;
        dft_manager.get_subdomain(name)
    }

    /// 获取子领域中的所有 DFT
    pub async fn get_dfts_in_subdomain(&self, name: &str) -> Result<Vec<DftInfo>> {
        let dft_manager = self.dft_manager.read().await;
        dft_manager.get_dfts_in_subdomain(name)
    }

    /// 获取所有 DFT 容器
    pub async fn get_all_dft_containers(&self) -> Result<Vec<DftContainer>> {
        let dft_manager = self.dft_manager.read().await;
        Ok(dft_manager.get_all_containers())
    }

    /// 获取容器信息
    pub async fn get_dft_container(&self, id: &AtomicalId) -> Result<DftContainer> {
        let dft_manager = self.dft_manager.read().await;
        dft_manager.get_container(id)
    }

    /// 获取容器中的所有 DFT
    pub async fn get_dfts_in_container(&self, id: &AtomicalId) -> Result<Vec<DftInfo>> {
        let dft_manager = self.dft_manager.read().await;
        dft_manager.get_dfts_in_container(id)
    }

    /// 获取所有事件元数据
    pub async fn get_all_events(&self) -> Result<Vec<serde_json::Value>> {
        let dft_manager = self.dft_manager.read().await;
        let events = dft_manager.get_all_events();
        
        let mut result = Vec::new();
        for event in events {
            result.push(serde_json::to_value(event)?);
        }
        
        Ok(result)
    }
    
    /// 获取特定事件的元数据
    pub async fn get_event(&self, id: &str) -> Result<serde_json::Value> {
        let dft_manager = self.dft_manager.read().await;
        let event = dft_manager.get_event(id)?;
        
        Ok(serde_json::to_value(event)?)
    }
    
    /// 获取与特定 DFT 相关的事件
    pub async fn get_events_by_dft(&self, dft_id: &AtomicalId) -> Result<Vec<serde_json::Value>> {
        let dft_manager = self.dft_manager.read().await;
        let events = dft_manager.get_events_by_dft(dft_id);
        
        let mut result = Vec::new();
        for event in events {
            result.push(serde_json::to_value(event)?);
        }
        
        Ok(result)
    }
    
    /// 获取与特定子领域相关的事件
    pub async fn get_events_by_subdomain(&self, subdomain_name: &str) -> Result<Vec<serde_json::Value>> {
        let dft_manager = self.dft_manager.read().await;
        let events = dft_manager.get_events_by_subdomain(subdomain_name);
        
        let mut result = Vec::new();
        for event in events {
            result.push(serde_json::to_value(event)?);
        }
        
        Ok(result)
    }
    
    /// 获取与特定容器相关的事件
    pub async fn get_events_by_container(&self, container_id: &AtomicalId) -> Result<Vec<serde_json::Value>> {
        let dft_manager = self.dft_manager.read().await;
        let events = dft_manager.get_events_by_container(container_id);
        
        let mut result = Vec::new();
        for event in events {
            result.push(serde_json::to_value(event)?);
        }
        
        Ok(result)
    }
    
    /// 获取特定类型的事件
    pub async fn get_events_by_type(&self, event_type: &str) -> Result<Vec<serde_json::Value>> {
        let dft_manager = self.dft_manager.read().await;
        let events = dft_manager.get_events_by_type(event_type);
        
        let mut result = Vec::new();
        for event in events {
            result.push(serde_json::to_value(event)?);
        }
        
        Ok(result)
    }
    
    /// 获取所有数据操作元数据
    pub async fn get_all_data(&self) -> Result<Vec<serde_json::Value>> {
        let dft_manager = self.dft_manager.read().await;
        let data_operations = dft_manager.get_all_data();
        
        let mut result = Vec::new();
        for data in data_operations {
            result.push(serde_json::to_value(data)?);
        }
        
        Ok(result)
    }
    
    /// 获取特定数据操作的元数据
    pub async fn get_data(&self, id: &str) -> Result<serde_json::Value> {
        let dft_manager = self.dft_manager.read().await;
        let data = dft_manager.get_data(id)?;
        
        Ok(serde_json::to_value(data)?)
    }
    
    /// 获取与特定 DFT 相关的数据
    pub async fn get_data_by_dft(&self, dft_id: &AtomicalId) -> Result<Vec<serde_json::Value>> {
        let dft_manager = self.dft_manager.read().await;
        let data_operations = dft_manager.get_data_by_dft(dft_id);
        
        let mut result = Vec::new();
        for data in data_operations {
            result.push(serde_json::to_value(data)?);
        }
        
        Ok(result)
    }
    
    /// 获取与特定子领域相关的数据
    pub async fn get_data_by_subdomain(&self, subdomain_name: &str) -> Result<Vec<serde_json::Value>> {
        let dft_manager = self.dft_manager.read().await;
        let data_operations = dft_manager.get_data_by_subdomain(subdomain_name);
        
        let mut result = Vec::new();
        for data in data_operations {
            result.push(serde_json::to_value(data)?);
        }
        
        Ok(result)
    }
    
    /// 获取与特定容器相关的数据
    pub async fn get_data_by_container(&self, container_id: &AtomicalId) -> Result<Vec<serde_json::Value>> {
        let dft_manager = self.dft_manager.read().await;
        let data_operations = dft_manager.get_data_by_container(container_id);
        
        let mut result = Vec::new();
        for data in data_operations {
            result.push(serde_json::to_value(data)?);
        }
        
        Ok(result)
    }
    
    /// 获取特定类型的数据
    pub async fn get_data_by_type(&self, data_type: &str) -> Result<Vec<serde_json::Value>> {
        let dft_manager = self.dft_manager.read().await;
        let data_operations = dft_manager.get_data_by_type(data_type);
        
        let mut result = Vec::new();
        for data in data_operations {
            result.push(serde_json::to_value(data)?);
        }
        
        Ok(result)
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
        let dft_manager = Arc::new(RwLock::new(DftManager::new()));
        AtomicalsRpc::new(network, state, storage, dft_manager)
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
                metadata: Some(json!({"name": "Test NFT 1"})),
            },
            AtomicalOperation::Update {
                id: id.clone(),
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
                assert_eq!(metadata.as_ref().unwrap()["name"], "Test NFT 1");
            }
            _ => panic!("Expected Mint operation"),
        }

        match &pending[&txid][1] {
            AtomicalOperation::Update { id: atomical_id, metadata } => {
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
        let stats = rpc.get_stats_sync()?;
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

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

    fn create_test_rpc() -> AtomicalsRpc {
        let network = Network::Regtest;
        let state = Arc::new(AtomicalsState::new());
        let storage = Arc::new(AtomicalsStorage::new_test().unwrap());
        AtomicalsRpc::new(network, state, storage)
    }

    #[test]
    fn test_get_atomical_info() -> Result<()> {
        let rpc = create_test_rpc();
        let id = AtomicalId {
            txid: Txid::from_str(
                "1234567890123456789012345678901234567890123456789012345678901234"
            )?,
            vout: 0,
        };

        // 测试不存在的 Atomical
        assert!(rpc.get_atomical_info(&id).is_err());

        Ok(())
    }

    #[test]
    fn test_get_atomicals_by_address() -> Result<()> {
        let rpc = create_test_rpc();
        let address = "bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080";

        // 测试地址查询
        let atomicals = rpc.get_atomicals_by_address(address)?;
        assert!(atomicals.is_empty());

        Ok(())
    }
}

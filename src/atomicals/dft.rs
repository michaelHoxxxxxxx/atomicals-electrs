use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use bitcoin::Amount;
use anyhow::{Result, anyhow};

use super::protocol::{AtomicalId, AtomicalType};
use super::state::AtomicalsState;

/// DFT 子领域信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DftSubdomain {
    /// 子领域名称
    pub name: String,
    /// 父领域 ID
    pub parent_id: Option<AtomicalId>,
    /// 创建区块高度
    pub created_height: u32,
    /// 创建时间戳
    pub created_timestamp: u64,
    /// 子领域内的 DFT 列表
    pub dft_ids: Vec<AtomicalId>,
    /// 子领域元数据
    pub metadata: Option<serde_json::Value>,
}

/// DFT 容器信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DftContainer {
    /// 容器 ID
    pub id: AtomicalId,
    /// 容器名称
    pub name: String,
    /// 创建区块高度
    pub created_height: u32,
    /// 创建时间戳
    pub created_timestamp: u64,
    /// 容器内的 DFT 列表
    pub dft_ids: Vec<AtomicalId>,
    /// 容器元数据
    pub metadata: Option<serde_json::Value>,
    /// 是否已封印
    pub sealed: bool,
}

/// DFT 代币信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DftInfo {
    /// 代币 ID
    pub id: AtomicalId,
    /// 代币符号
    pub ticker: String,
    /// 最大供应量
    pub max_supply: u64,
    /// 当前已铸造数量
    pub minted_supply: u64,
    /// 创建区块高度
    pub created_height: u32,
    /// 创建时间戳
    pub created_timestamp: u64,
    /// 是否已封印
    pub sealed: bool,
    /// 持有者信息
    pub holders: Vec<DftHolder>,
    /// 铸造历史
    pub mint_history: Vec<DftMintEvent>,
    /// 元数据
    pub metadata: Option<serde_json::Value>,
    /// 所属子领域
    pub subdomain: Option<String>,
    /// 所属容器 ID
    pub container_id: Option<AtomicalId>,
}

/// DFT 持有者信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DftHolder {
    /// 持有者地址
    pub address: String,
    /// 持有数量
    pub amount: u64,
    /// 最后更新时间戳
    pub last_updated: u64,
}

/// DFT 铸造事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DftMintEvent {
    /// 铸造交易 ID
    pub txid: bitcoin::Txid,
    /// 铸造数量
    pub amount: u64,
    /// 接收地址
    pub recipient: String,
    /// 铸造时间戳
    pub timestamp: u64,
    /// 铸造区块高度
    pub height: u32,
}

/// DFT 供应量信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DftSupplyInfo {
    /// 代币符号
    pub ticker: String,
    /// 最大供应量
    pub max_supply: u64,
    /// 当前已铸造数量
    pub minted_supply: u64,
    /// 剩余可铸造数量
    pub remaining_supply: u64,
    /// 铸造百分比
    pub mint_percentage: f64,
}

/// 事件元数据结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetadata {
    /// 事件ID
    pub id: String,
    /// 事件类型
    pub event_type: String,
    /// 事件时间戳
    pub timestamp: u64,
    /// 事件发送者
    pub sender: Option<String>,
    /// 事件接收者
    pub recipient: Option<String>,
    /// 事件内容
    pub content: serde_json::Value,
    /// 事件相关的 DFT ID
    pub dft_id: Option<AtomicalId>,
    /// 事件相关的子领域名称
    pub subdomain_name: Option<String>,
    /// 事件相关的容器 ID
    pub container_id: Option<AtomicalId>,
    /// 事件标签
    pub tags: Option<Vec<String>>,
}

/// 数据操作元数据结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataMetadata {
    /// 数据ID
    pub id: String,
    /// 数据类型
    pub data_type: String,
    /// 数据创建时间戳
    pub timestamp: u64,
    /// 数据创建者
    pub creator: Option<String>,
    /// 数据内容
    pub content: serde_json::Value,
    /// 数据相关的 DFT ID
    pub dft_id: Option<AtomicalId>,
    /// 数据相关的子领域名称
    pub subdomain_name: Option<String>,
    /// 数据相关的容器 ID
    pub container_id: Option<AtomicalId>,
    /// 数据标签
    pub tags: Option<Vec<String>>,
    /// 数据版本
    pub version: Option<String>,
    /// 数据大小（字节）
    pub size: Option<u64>,
    /// 数据校验和
    pub checksum: Option<String>,
}

/// DFT 管理器
#[derive(Debug)]
pub struct DftManager {
    /// 代币符号到 ID 的映射
    ticker_to_id: HashMap<String, AtomicalId>,
    /// ID 到代币信息的映射
    dft_info: HashMap<AtomicalId, DftInfo>,
    /// 代币符号到持有者映射的映射
    holders: HashMap<String, HashMap<String, u64>>,
    /// 子领域信息
    subdomains: HashMap<String, DftSubdomain>,
    /// 容器信息
    containers: HashMap<AtomicalId, DftContainer>,
    /// 事件元数据映射 (id -> EventMetadata)
    events: HashMap<String, EventMetadata>,
    /// 数据操作元数据映射 (id -> DataMetadata)
    data_operations: HashMap<String, DataMetadata>,
}

impl DftManager {
    /// 创建新的 DFT 管理器
    pub fn new() -> Self {
        Self {
            ticker_to_id: HashMap::new(),
            dft_info: HashMap::new(),
            holders: HashMap::new(),
            subdomains: HashMap::new(),
            containers: HashMap::new(),
            events: HashMap::new(),
            data_operations: HashMap::new(),
        }
    }

    /// 注册新的 DFT
    pub fn register_dft(&mut self, id: AtomicalId, ticker: String, max_supply: u64, height: u32, timestamp: u64, metadata: Option<serde_json::Value>) -> Result<()> {
        // 检查 ticker 是否已存在
        if self.ticker_to_id.contains_key(&ticker) {
            return Err(anyhow!("DFT ticker already exists"));
        }

        // 创建 DFT 信息
        let dft_info = DftInfo {
            id,
            ticker: ticker.clone(),
            max_supply,
            minted_supply: 0,
            created_height: height,
            created_timestamp: timestamp,
            sealed: false,
            holders: Vec::new(),
            mint_history: Vec::new(),
            metadata,
            subdomain: None,
            container_id: None,
        };

        // 添加到映射
        self.ticker_to_id.insert(ticker.clone(), id);
        self.dft_info.insert(id, dft_info);
        self.holders.insert(ticker, HashMap::new());

        Ok(())
    }

    /// 铸造 DFT
    pub fn mint_dft(&mut self, parent_id: AtomicalId, amount: u64, recipient: String, txid: bitcoin::Txid, height: u32, timestamp: u64) -> Result<()> {
        // 获取 DFT 信息
        let dft_info = self.dft_info.get_mut(&parent_id).ok_or_else(|| anyhow!("DFT not found"))?;
        
        // 检查是否已封印
        if dft_info.sealed {
            return Err(anyhow!("Cannot mint sealed DFT"));
        }
        
        // 检查是否超过最大供应量
        if dft_info.minted_supply + amount > dft_info.max_supply {
            return Err(anyhow!("Mint amount exceeds remaining supply"));
        }
        
        // 更新已铸造数量
        dft_info.minted_supply += amount;
        
        // 添加铸造事件
        let mint_event = DftMintEvent {
            txid,
            amount,
            recipient: recipient.clone(),
            timestamp,
            height,
        };
        dft_info.mint_history.push(mint_event);
        
        // 更新持有者信息
        let ticker = dft_info.ticker.clone();
        let holders_map = self.holders.get_mut(&ticker).unwrap();
        *holders_map.entry(recipient.clone()).or_insert(0) += amount;
        
        // 更新 DFT 持有者列表
        dft_info.holders = holders_map.iter()
            .map(|(address, amount)| DftHolder {
                address: address.clone(),
                amount: *amount,
                last_updated: timestamp,
            })
            .collect();
        
        Ok(())
    }

    /// 封印 DFT
    pub fn seal_dft(&mut self, id: AtomicalId) -> Result<()> {
        // 获取 DFT 信息
        let dft_info = self.dft_info.get_mut(&id).ok_or_else(|| anyhow!("DFT not found"))?;
        
        // 设置为已封印
        dft_info.sealed = true;
        
        Ok(())
    }

    /// 转移 DFT
    pub fn transfer_dft(&mut self, id: AtomicalId, from: String, to: String, amount: u64, timestamp: u64) -> Result<()> {
        // 获取 DFT 信息
        let dft_info = self.dft_info.get(&id).ok_or_else(|| anyhow!("DFT not found"))?;
        let ticker = dft_info.ticker.clone();
        
        // 获取持有者映射
        let holders_map = self.holders.get_mut(&ticker).unwrap();
        
        // 检查发送者余额
        let sender_balance = holders_map.get(&from).copied().unwrap_or(0);
        if sender_balance < amount {
            return Err(anyhow!("Insufficient balance"));
        }
        
        // 更新发送者余额
        if sender_balance == amount {
            holders_map.remove(&from);
        } else {
            *holders_map.get_mut(&from).unwrap() -= amount;
        }
        
        // 更新接收者余额
        *holders_map.entry(to.clone()).or_insert(0) += amount;
        
        // 更新 DFT 信息中的持有者列表
        let dft_info = self.dft_info.get_mut(&id).unwrap();
        dft_info.holders = holders_map.iter()
            .map(|(address, amount)| DftHolder {
                address: address.clone(),
                amount: *amount,
                last_updated: timestamp,
            })
            .collect();
        
        Ok(())
    }

    /// 获取 DFT 信息
    pub fn get_dft_info(&self, id: &AtomicalId) -> Result<DftInfo> {
        self.dft_info.get(id)
            .cloned()
            .ok_or_else(|| anyhow!("DFT not found"))
    }

    /// 通过 ticker 获取 DFT 信息
    pub fn get_dft_info_by_ticker(&self, ticker: &str) -> Result<DftInfo> {
        let id = self.ticker_to_id.get(ticker)
            .ok_or_else(|| anyhow!("DFT ticker not found"))?;
        self.get_dft_info(id)
    }

    /// 获取 DFT 供应量信息
    pub fn get_supply_info(&self, id: &AtomicalId) -> Result<DftSupplyInfo> {
        let dft_info = self.dft_info.get(id)
            .ok_or_else(|| anyhow!("DFT not found"))?;
        
        let remaining_supply = dft_info.max_supply - dft_info.minted_supply;
        let mint_percentage = if dft_info.max_supply > 0 {
            (dft_info.minted_supply as f64 / dft_info.max_supply as f64) * 100.0
        } else {
            0.0
        };
        
        Ok(DftSupplyInfo {
            ticker: dft_info.ticker.clone(),
            max_supply: dft_info.max_supply,
            minted_supply: dft_info.minted_supply,
            remaining_supply,
            mint_percentage,
        })
    }

    /// 获取 DFT 持有者列表
    pub fn get_holders(&self, id: &AtomicalId) -> Result<Vec<DftHolder>> {
        let dft_info = self.dft_info.get(id)
            .ok_or_else(|| anyhow!("DFT not found"))?;
        
        Ok(dft_info.holders.clone())
    }

    /// 获取 DFT 铸造历史
    pub fn get_mint_history(&self, id: &AtomicalId) -> Result<Vec<DftMintEvent>> {
        let dft_info = self.dft_info.get(id)
            .ok_or_else(|| anyhow!("DFT not found"))?;
        
        Ok(dft_info.mint_history.clone())
    }

    /// 检查 DFT 是否存在
    pub fn exists(&self, id: &AtomicalId) -> bool {
        self.dft_info.contains_key(id)
    }

    /// 通过 ticker 检查 DFT 是否存在
    pub fn exists_by_ticker(&self, ticker: &str) -> bool {
        self.ticker_to_id.contains_key(ticker)
    }

    /// 获取所有 DFT 列表
    pub fn get_all_dfts(&self) -> Vec<DftInfo> {
        self.dft_info.values().cloned().collect()
    }

    /// 获取 DFT 总数
    pub fn get_total_count(&self) -> usize {
        self.dft_info.len()
    }

    /// 创建子领域
    pub fn create_subdomain(&mut self, name: String, parent_id: Option<AtomicalId>, height: u32, timestamp: u64, metadata: Option<serde_json::Value>) -> Result<()> {
        // 检查子领域是否已存在
        if self.subdomains.contains_key(&name) {
            return Err(anyhow!("Subdomain already exists"));
        }

        // 创建子领域信息
        let subdomain = DftSubdomain {
            name: name.clone(),
            parent_id,
            created_height: height,
            created_timestamp: timestamp,
            dft_ids: Vec::new(),
            metadata,
        };

        // 添加到映射
        self.subdomains.insert(name, subdomain);

        Ok(())
    }

    /// 创建容器
    pub fn create_container(&mut self, id: AtomicalId, name: String, height: u32, timestamp: u64, metadata: Option<serde_json::Value>) -> Result<()> {
        // 检查容器是否已存在
        if self.containers.contains_key(&id) {
            return Err(anyhow!("Container already exists"));
        }

        // 创建容器信息
        let container = DftContainer {
            id,
            name,
            created_height: height,
            created_timestamp: timestamp,
            dft_ids: Vec::new(),
            metadata,
            sealed: false,
        };

        // 添加到映射
        self.containers.insert(id, container);

        Ok(())
    }

    /// 将 DFT 添加到子领域
    pub fn add_dft_to_subdomain(&mut self, dft_id: &AtomicalId, subdomain_name: &str) -> Result<()> {
        // 检查 DFT 是否存在
        let dft_info = self.dft_info.get_mut(dft_id).ok_or_else(|| anyhow!("DFT not found"))?;
        
        // 检查子领域是否存在
        let subdomain = self.subdomains.get_mut(subdomain_name).ok_or_else(|| anyhow!("Subdomain not found"))?;
        
        // 检查 DFT 是否已在其他子领域
        if let Some(current_subdomain) = &dft_info.subdomain {
            if current_subdomain != subdomain_name {
                return Err(anyhow!("DFT already belongs to another subdomain"));
            }
            // 已经在这个子领域中，无需操作
            return Ok(());
        }
        
        // 将 DFT 添加到子领域
        subdomain.dft_ids.push(dft_id.clone());
        dft_info.subdomain = Some(subdomain_name.to_string());
        
        Ok(())
    }

    /// 将 DFT 添加到容器
    pub fn add_dft_to_container(&mut self, dft_id: &AtomicalId, container_id: &AtomicalId) -> Result<()> {
        // 检查 DFT 是否存在
        let dft_info = self.dft_info.get_mut(dft_id).ok_or_else(|| anyhow!("DFT not found"))?;
        
        // 检查容器是否存在
        let container = self.containers.get_mut(container_id).ok_or_else(|| anyhow!("Container not found"))?;
        
        // 检查容器是否已封印
        if container.sealed {
            return Err(anyhow!("Container is sealed"));
        }
        
        // 检查 DFT 是否已在其他容器
        if let Some(current_container_id) = &dft_info.container_id {
            if current_container_id != container_id {
                return Err(anyhow!("DFT already belongs to another container"));
            }
            // 已经在这个容器中，无需操作
            return Ok(());
        }
        
        // 将 DFT 添加到容器
        container.dft_ids.push(dft_id.clone());
        dft_info.container_id = Some(container_id.clone());
        
        Ok(())
    }

    /// 从子领域移除 DFT
    pub fn remove_dft_from_subdomain(&mut self, dft_id: &AtomicalId, subdomain_name: &str) -> Result<()> {
        // 检查 DFT 是否存在
        let dft_info = self.dft_info.get_mut(dft_id).ok_or_else(|| anyhow!("DFT not found"))?;
        
        // 检查 DFT 是否在指定子领域
        if let Some(current_subdomain) = &dft_info.subdomain {
            if current_subdomain != subdomain_name {
                return Err(anyhow!("DFT does not belong to the specified subdomain"));
            }
        } else {
            return Err(anyhow!("DFT does not belong to any subdomain"));
        }
        
        // 检查子领域是否存在
        let subdomain = self.subdomains.get_mut(subdomain_name).ok_or_else(|| anyhow!("Subdomain not found"))?;
        
        // 从子领域移除 DFT
        subdomain.dft_ids.retain(|id| id != dft_id);
        dft_info.subdomain = None;
        
        Ok(())
    }

    /// 从容器移除 DFT
    pub fn remove_dft_from_container(&mut self, dft_id: &AtomicalId, container_id: &AtomicalId) -> Result<()> {
        // 检查 DFT 是否存在
        let dft_info = self.dft_info.get_mut(dft_id).ok_or_else(|| anyhow!("DFT not found"))?;
        
        // 检查 DFT 是否在指定容器
        if let Some(current_container_id) = &dft_info.container_id {
            if current_container_id != container_id {
                return Err(anyhow!("DFT does not belong to the specified container"));
            }
        } else {
            return Err(anyhow!("DFT does not belong to any container"));
        }
        
        // 检查容器是否存在
        let container = self.containers.get_mut(container_id).ok_or_else(|| anyhow!("Container not found"))?;
        
        // 检查容器是否已封印
        if container.sealed {
            return Err(anyhow!("Container is sealed"));
        }
        
        // 从容器移除 DFT
        container.dft_ids.retain(|id| id != dft_id);
        dft_info.container_id = None;
        
        Ok(())
    }

    /// 封印容器
    pub fn seal_container(&mut self, container_id: &AtomicalId) -> Result<()> {
        // 检查容器是否存在
        let container = self.containers.get_mut(container_id).ok_or_else(|| anyhow!("Container not found"))?;
        
        // 设置为已封印
        container.sealed = true;
        
        Ok(())
    }

    /// 获取子领域信息
    pub fn get_subdomain(&self, name: &str) -> Result<DftSubdomain> {
        self.subdomains.get(name)
            .cloned()
            .ok_or_else(|| anyhow!("Subdomain not found"))
    }

    /// 获取容器信息
    pub fn get_container(&self, id: &AtomicalId) -> Result<DftContainer> {
        self.containers.get(id)
            .cloned()
            .ok_or_else(|| anyhow!("Container not found"))
    }

    /// 获取所有子领域
    pub fn get_all_subdomains(&self) -> Vec<DftSubdomain> {
        self.subdomains.values().cloned().collect()
    }

    /// 获取所有容器
    pub fn get_all_containers(&self) -> Vec<DftContainer> {
        self.containers.values().cloned().collect()
    }

    /// 获取子领域中的所有 DFT
    pub fn get_dfts_in_subdomain(&self, name: &str) -> Result<Vec<DftInfo>> {
        let subdomain = self.subdomains.get(name)
            .ok_or_else(|| anyhow!("Subdomain not found"))?;
        
        let mut dfts = Vec::new();
        for dft_id in &subdomain.dft_ids {
            if let Some(dft_info) = self.dft_info.get(dft_id) {
                dfts.push(dft_info.clone());
            }
        }
        
        Ok(dfts)
    }

    /// 获取容器中的所有 DFT
    pub fn get_dfts_in_container(&self, id: &AtomicalId) -> Result<Vec<DftInfo>> {
        let container = self.containers.get(id)
            .ok_or_else(|| anyhow!("Container not found"))?;
        
        let mut dfts = Vec::new();
        for dft_id in &container.dft_ids {
            if let Some(dft_info) = self.dft_info.get(dft_id) {
                dfts.push(dft_info.clone());
            }
        }
        
        Ok(dfts)
    }

    /// 注册事件元数据
    pub fn register_event(
        &mut self,
        event_type: String,
        content: serde_json::Value,
        timestamp: u64,
        sender: Option<String>,
        recipient: Option<String>,
        dft_id: Option<AtomicalId>,
        subdomain_name: Option<String>,
        container_id: Option<AtomicalId>,
        tags: Option<Vec<String>>,
    ) -> Result<String> {
        // 生成唯一的事件ID
        let id = format!("evt_{}", uuid::Uuid::new_v4().to_string());
        
        // 创建事件元数据
        let event_metadata = EventMetadata {
            id: id.clone(),
            event_type,
            timestamp,
            sender,
            recipient,
            content,
            dft_id,
            subdomain_name,
            container_id,
            tags,
        };
        
        // 存储事件元数据
        self.events.insert(id.clone(), event_metadata);
        
        Ok(id)
    }
    
    /// 获取事件元数据
    pub fn get_event(&self, id: &str) -> Result<EventMetadata> {
        self.events.get(id)
            .cloned()
            .ok_or_else(|| anyhow!("事件不存在: {}", id))
    }
    
    /// 获取所有事件元数据
    pub fn get_all_events(&self) -> Vec<EventMetadata> {
        self.events.values().cloned().collect()
    }
    
    /// 获取与特定 DFT 相关的事件
    pub fn get_events_by_dft(&self, dft_id: &AtomicalId) -> Vec<EventMetadata> {
        self.events.values()
            .filter(|e| e.dft_id.as_ref().map_or(false, |id| id == dft_id))
            .cloned()
            .collect()
    }
    
    /// 获取与特定子领域相关的事件
    pub fn get_events_by_subdomain(&self, subdomain_name: &str) -> Vec<EventMetadata> {
        self.events.values()
            .filter(|e| e.subdomain_name.as_ref().map_or(false, |name| name == subdomain_name))
            .cloned()
            .collect()
    }
    
    /// 获取与特定容器相关的事件
    pub fn get_events_by_container(&self, container_id: &AtomicalId) -> Vec<EventMetadata> {
        self.events.values()
            .filter(|e| e.container_id.as_ref().map_or(false, |id| id == container_id))
            .cloned()
            .collect()
    }
    
    /// 获取特定类型的事件
    pub fn get_events_by_type(&self, event_type: &str) -> Vec<EventMetadata> {
        self.events.values()
            .filter(|e| e.event_type == event_type)
            .cloned()
            .collect()
    }
    
    /// 注册数据操作元数据
    pub fn register_data(
        &mut self,
        data_type: String,
        content: serde_json::Value,
        timestamp: u64,
        creator: Option<String>,
        dft_id: Option<AtomicalId>,
        subdomain_name: Option<String>,
        container_id: Option<AtomicalId>,
        tags: Option<Vec<String>>,
        version: Option<String>,
        size: Option<u64>,
        checksum: Option<String>,
    ) -> Result<String> {
        // 生成唯一的数据ID
        let id = format!("data_{}", uuid::Uuid::new_v4().to_string());
        
        // 创建数据操作元数据
        let data_metadata = DataMetadata {
            id: id.clone(),
            data_type,
            timestamp,
            creator,
            content,
            dft_id,
            subdomain_name,
            container_id,
            tags,
            version,
            size,
            checksum,
        };
        
        // 存储数据操作元数据
        self.data_operations.insert(id.clone(), data_metadata);
        
        Ok(id)
    }
    
    /// 获取数据操作元数据
    pub fn get_data(&self, id: &str) -> Result<DataMetadata> {
        self.data_operations.get(id)
            .cloned()
            .ok_or_else(|| anyhow!("数据不存在: {}", id))
    }
    
    /// 获取所有数据操作元数据
    pub fn get_all_data(&self) -> Vec<DataMetadata> {
        self.data_operations.values().cloned().collect()
    }
    
    /// 获取与特定 DFT 相关的数据
    pub fn get_data_by_dft(&self, dft_id: &AtomicalId) -> Vec<DataMetadata> {
        self.data_operations.values()
            .filter(|d| d.dft_id.as_ref().map_or(false, |id| id == dft_id))
            .cloned()
            .collect()
    }
    
    /// 获取与特定子领域相关的数据
    pub fn get_data_by_subdomain(&self, subdomain_name: &str) -> Vec<DataMetadata> {
        self.data_operations.values()
            .filter(|d| d.subdomain_name.as_ref().map_or(false, |name| name == subdomain_name))
            .cloned()
            .collect()
    }
    
    /// 获取与特定容器相关的数据
    pub fn get_data_by_container(&self, container_id: &AtomicalId) -> Vec<DataMetadata> {
        self.data_operations.values()
            .filter(|d| d.container_id.as_ref().map_or(false, |id| id == container_id))
            .cloned()
            .collect()
    }
    
    /// 获取特定类型的数据
    pub fn get_data_by_type(&self, data_type: &str) -> Vec<DataMetadata> {
        self.data_operations.values()
            .filter(|d| d.data_type == data_type)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::Txid;

    fn create_test_atomical_id() -> AtomicalId {
        AtomicalId {
            txid: Txid::from_str("0000000000000000000000000000000000000000000000000000000000000001").unwrap(),
            vout: 0,
        }
    }

    #[test]
    fn test_dft_registration() {
        let mut manager = DftManager::new();
        
        // 注册新的 DFT
        let id = create_test_atomical_id();
        let result = manager.register_dft(
            id,
            "TEST".to_string(),
            1000000,
            100,
            1609459200, // 2021-01-01
            Some(serde_json::json!({"name": "Test Token", "description": "A test token"})),
        );
        
        assert!(result.is_ok());
        assert!(manager.exists(&id));
        assert!(manager.exists_by_ticker("TEST"));
        
        // 尝试注册相同的 ticker
        let id2 = AtomicalId {
            txid: Txid::from_str("0000000000000000000000000000000000000000000000000000000000000002").unwrap(),
            vout: 0,
        };
        let result = manager.register_dft(
            id2,
            "TEST".to_string(),
            2000000,
            101,
            1609459300,
            None,
        );
        
        assert!(result.is_err());
    }

    #[test]
    fn test_dft_minting() {
        let mut manager = DftManager::new();
        
        // 注册新的 DFT
        let id = create_test_atomical_id();
        manager.register_dft(
            id,
            "TEST".to_string(),
            1000000,
            100,
            1609459200,
            None,
        ).unwrap();
        
        // 铸造代币
        let txid = Txid::from_str("0000000000000000000000000000000000000000000000000000000000000003").unwrap();
        let result = manager.mint_dft(
            id,
            500000,
            "addr1".to_string(),
            txid,
            101,
            1609459300,
        );
        
        assert!(result.is_ok());
        
        // 检查供应量
        let supply_info = manager.get_supply_info(&id).unwrap();
        assert_eq!(supply_info.minted_supply, 500000);
        assert_eq!(supply_info.remaining_supply, 500000);
        assert_eq!(supply_info.mint_percentage, 50.0);
        
        // 检查持有者
        let holders = manager.get_holders(&id).unwrap();
        assert_eq!(holders.len(), 1);
        assert_eq!(holders[0].address, "addr1");
        assert_eq!(holders[0].amount, 500000);
        
        // 尝试超额铸造
        let txid = Txid::from_str("0000000000000000000000000000000000000000000000000000000000000004").unwrap();
        let result = manager.mint_dft(
            id,
            600000,
            "addr2".to_string(),
            txid,
            102,
            1609459400,
        );
        
        assert!(result.is_err());
    }

    #[test]
    fn test_dft_transfer() {
        let mut manager = DftManager::new();
        
        // 注册新的 DFT
        let id = create_test_atomical_id();
        manager.register_dft(
            id,
            "TEST".to_string(),
            1000000,
            100,
            1609459200,
            None,
        ).unwrap();
        
        // 铸造代币
        let txid = Txid::from_str("0000000000000000000000000000000000000000000000000000000000000003").unwrap();
        manager.mint_dft(
            id,
            500000,
            "addr1".to_string(),
            txid,
            101,
            1609459300,
        ).unwrap();
        
        // 转移代币
        let result = manager.transfer_dft(
            id,
            "addr1".to_string(),
            "addr2".to_string(),
            200000,
            1609459400,
        );
        
        assert!(result.is_ok());
        
        // 检查持有者
        let holders = manager.get_holders(&id).unwrap();
        assert_eq!(holders.len(), 2);
        
        // 找到 addr1 和 addr2 的持有量
        let addr1_amount = holders.iter().find(|h| h.address == "addr1").map(|h| h.amount).unwrap_or(0);
        let addr2_amount = holders.iter().find(|h| h.address == "addr2").map(|h| h.amount).unwrap_or(0);
        
        assert_eq!(addr1_amount, 300000);
        assert_eq!(addr2_amount, 200000);
        
        // 尝试转移超过余额的代币
        let result = manager.transfer_dft(
            id,
            "addr1".to_string(),
            "addr3".to_string(),
            400000,
            1609459500,
        );
        
        assert!(result.is_err());
    }

    #[test]
    fn test_dft_sealing() {
        let mut manager = DftManager::new();
        
        // 注册新的 DFT
        let id = create_test_atomical_id();
        manager.register_dft(
            id,
            "TEST".to_string(),
            1000000,
            100,
            1609459200,
            None,
        ).unwrap();
        
        // 铸造代币
        let txid = Txid::from_str("0000000000000000000000000000000000000000000000000000000000000003").unwrap();
        manager.mint_dft(
            id,
            500000,
            "addr1".to_string(),
            txid,
            101,
            1609459300,
        ).unwrap();
        
        // 封印 DFT
        let result = manager.seal_dft(id);
        assert!(result.is_ok());
        
        // 尝试在封印后铸造
        let txid = Txid::from_str("0000000000000000000000000000000000000000000000000000000000000004").unwrap();
        let result = manager.mint_dft(
            id,
            100000,
            "addr2".to_string(),
            txid,
            102,
            1609459400,
        );
        
        assert!(result.is_err());
    }
}

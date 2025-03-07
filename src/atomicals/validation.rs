use anyhow::{anyhow, Result};
use bitcoin::Transaction;
use serde_json::Value;
use std::collections::HashSet;

use super::protocol::{AtomicalId, AtomicalOperation, AtomicalType};
use super::state::AtomicalsStateInterface;

/// Atomicals 验证器
/// 
/// 负责验证各种 Atomicals 操作的有效性，确保操作符合协议规则。
/// 验证器检查各种条件，如 Atomical 是否存在、是否已封印、输出索引是否有效等。
pub struct AtomicalsValidator<S: AtomicalsStateInterface> {
    state: S,
}

impl<S: AtomicalsStateInterface> AtomicalsValidator<S> {
    /// 创建新的验证器
    /// 
    /// # 参数
    /// * `state` - Atomicals 状态接口的实现，用于查询 Atomical 的状态
    pub fn new(state: S) -> Self {
        Self { state }
    }

    /// 验证 Atomicals 操作
    /// 
    /// 检查给定的 Atomical 操作是否有效。这是对 `validate_operation` 函数的封装，
    /// 使用验证器内部的状态。
    /// 
    /// # 参数
    /// * `tx` - 包含操作的交易
    /// * `operation` - 要验证的 Atomical 操作
    /// 
    /// # 返回
    /// * `Result<()>` - 如果操作有效，返回 Ok(())；否则返回包含错误信息的 Err
    pub fn validate_operation(&self, tx: &Transaction, operation: &AtomicalOperation) -> Result<()> {
        validate_operation(operation, tx, &self.state)
    }
}

/// 验证 Atomicals 操作
/// 
/// 根据操作类型执行不同的验证逻辑，确保操作符合 Atomicals 协议的规则。
/// 
/// # 参数
/// * `operation` - 要验证的 Atomical 操作
/// * `tx` - 包含操作的交易
/// * `state` - Atomicals 状态接口，用于查询 Atomical 的状态
/// 
/// # 返回
/// * `Result<()>` - 如果操作有效，返回 Ok(())；否则返回包含错误信息的 Err
pub fn validate_operation<S: AtomicalsStateInterface>(
    operation: &AtomicalOperation,
    tx: &Transaction,
    state: &S,
) -> Result<()> {
    // 通用验证：检查交易是否有效
    if tx.input.is_empty() || tx.output.is_empty() {
        return Err(anyhow!("Transaction must have at least one input and one output"));
    }

    match operation {
        AtomicalOperation::Mint { id, atomical_type, metadata } => {
            // 验证铸造操作
            /// 验证铸造操作的参数，包括 Atomical 类型和元数据。
            /// 
            /// # 参数
            /// * `atomical_type` - Atomical 的类型（NFT、FT 等）
            /// * `metadata` - 可选的元数据
            /// 
            /// # 返回
            /// * `Result<()>` - 如果参数有效，返回 Ok(())；否则返回包含错误信息的 Err
            validate_mint(atomical_type, metadata)?;
            // 检查 Atomical ID 是否已存在
            if state.exists(id)? {
                return Err(anyhow!("Atomical ID already exists"));
            }
        }
        AtomicalOperation::Transfer { id, output_index } => {
            // 验证转移操作
            /// 验证转移操作的参数，包括 Atomical ID 和输出索引。
            /// 
            /// # 参数
            /// * `id` - Atomical 的 ID
            /// * `output_index` - 输出索引
            /// 
            /// # 返回
            /// * `Result<()>` - 如果参数有效，返回 Ok(())；否则返回包含错误信息的 Err
            // 检查 Atomical 是否存在
            if !state.exists(id)? {
                return Err(anyhow!("Atomical does not exist"));
            }
            
            // 检查 Atomical 是否已封印
            if state.is_sealed(id)? {
                return Err(anyhow!("Cannot transfer sealed Atomical"));
            }
            
            // 检查输出索引是否有效
            if *output_index as usize >= tx.output.len() {
                return Err(anyhow!("Invalid output index"));
            }
        }
        AtomicalOperation::Update { id, metadata: _ } => {
            // 验证更新操作
            /// 验证更新操作的参数，包括 Atomical ID 和元数据。
            /// 
            /// # 参数
            /// * `id` - Atomical 的 ID
            /// * `metadata` - 可选的元数据
            /// 
            /// # 返回
            /// * `Result<()>` - 如果参数有效，返回 Ok(())；否则返回包含错误信息的 Err
            validate_update(state, tx, id)?;
        }
        AtomicalOperation::Seal { id } => {
            // 验证封印操作
            /// 验证封印操作的参数，包括 Atomical ID。
            /// 
            /// # 参数
            /// * `id` - Atomical 的 ID
            /// 
            /// # 返回
            /// * `Result<()>` - 如果参数有效，返回 Ok(())；否则返回包含错误信息的 Err
            validate_seal(state, tx, id)?;
        }
        AtomicalOperation::DeployDFT { id, ticker, max_supply, metadata } => {
            // 验证部署分布式铸造代币操作
            /// 验证部署分布式铸造代币操作的参数，包括 Atomical ID、ticker、最大供应量和元数据。
            /// 
            /// # 参数
            /// * `id` - Atomical 的 ID
            /// * `ticker` - 代币的 ticker 符号
            /// * `max_supply` - 代币的最大供应量
            /// * `metadata` - 可选的元数据
            /// 
            /// # 返回
            /// * `Result<()>` - 如果参数有效，返回 Ok(())；否则返回包含错误信息的 Err
            // 检查 Atomical ID 是否已存在
            if state.exists(id)? {
                return Err(anyhow!("Atomical ID already exists"));
            }
            
            // 验证 ticker 格式
            // ticker 必须是非空的
            if ticker.is_empty() {
                return Err(anyhow!("Ticker cannot be empty"));
            }
            
            // ticker 长度必须在 1-10 个字符之间
            if ticker.len() > 10 {
                return Err(anyhow!("Ticker length must be between 1 and 10 characters"));
            }
            
            // ticker 必须只包含大写字母和数字
            if !ticker.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()) {
                return Err(anyhow!("Ticker must only contain uppercase letters and digits"));
            }
            
            // ticker 必须以字母开头
            if !ticker.chars().next().unwrap().is_ascii_alphabetic() {
                return Err(anyhow!("Ticker must start with a letter"));
            }
            
            // 验证最大供应量
            // 最大供应量必须大于 0
            if *max_supply == 0 {
                return Err(anyhow!("Maximum supply must be greater than 0"));
            }
            
            // 最大供应量必须在合理范围内
            // 设置一个上限，例如 21 万亿（比特币的 100 万倍）
            if *max_supply > 21_000_000_000_000 {
                return Err(anyhow!("Maximum supply exceeds reasonable limits"));
            }
            
            // 验证元数据
            if let Some(metadata_value) = metadata {
                // 检查元数据大小
                let metadata_size = serde_json::to_string(metadata_value)?.len();
                if metadata_size > 1024 * 1024 { // 1MB 限制
                    return Err(anyhow!("Metadata size exceeds 1MB limit"));
                }
            }
        }
        AtomicalOperation::MintDFT { parent_id, amount } => {
            // 验证铸造分布式代币操作
            /// 验证铸造分布式代币操作的参数，包括父 Atomical ID 和铸造数量。
            /// 
            /// # 参数
            /// * `parent_id` - 父 Atomical 的 ID
            /// * `amount` - 铸造数量
            /// 
            /// # 返回
            /// * `Result<()>` - 如果参数有效，返回 Ok(())；否则返回包含错误信息的 Err
            // 检查父 Atomical 是否存在
            if !state.exists(parent_id)? {
                return Err(anyhow!("Parent Atomical does not exist"));
            }
            
            // 检查父 Atomical 的类型是否为 FT
            let parent_type = state.get_atomical_type(parent_id)?;
            if parent_type != AtomicalType::FT {
                return Err(anyhow!("Parent Atomical is not a fungible token"));
            }
            
            // 验证铸造数量
            if *amount == 0 {
                return Err(anyhow!("Mint amount must be greater than 0"));
            }
            
            // 这里可以添加更多的验证逻辑，例如检查是否超过最大供应量
            // 但这需要获取父 Atomical 的更多信息，如最大供应量和当前已铸造数量
        }
        AtomicalOperation::Event { id, data } => {
            // 验证事件操作
            /// 验证事件操作的参数，包括 Atomical ID 和事件数据。
            /// 
            /// # 参数
            /// * `id` - Atomical 的 ID
            /// * `data` - 事件数据
            /// 
            /// # 返回
            /// * `Result<()>` - 如果参数有效，返回 Ok(())；否则返回包含错误信息的 Err
            // 检查 Atomical 是否存在
            if !state.exists(id)? {
                return Err(anyhow!("Atomical does not exist"));
            }
            
            // 检查 Atomical 是否已封印
            if state.is_sealed(id)? {
                return Err(anyhow!("Cannot add event to sealed Atomical"));
            }
            
            // 验证数据格式
            if !data.is_object() {
                return Err(anyhow!("Event data must be a JSON object"));
            }
            
            // 检查数据大小
            let data_size = serde_json::to_string(data)?.len();
            if data_size > 1024 * 1024 { // 1MB 限制
                return Err(anyhow!("Event data size exceeds 1MB limit"));
            }
        }
        AtomicalOperation::Data { id, data } => {
            // 验证数据操作
            /// 验证数据操作的参数，包括 Atomical ID 和数据。
            /// 
            /// # 参数
            /// * `id` - Atomical 的 ID
            /// * `data` - 数据
            /// 
            /// # 返回
            /// * `Result<()>` - 如果参数有效，返回 Ok(())；否则返回包含错误信息的 Err
            // 检查 Atomical 是否存在
            if !state.exists(id)? {
                return Err(anyhow!("Atomical does not exist"));
            }
            
            // 检查 Atomical 是否已封印
            if state.is_sealed(id)? {
                return Err(anyhow!("Cannot add data to sealed Atomical"));
            }
            
            // 验证数据格式
            if !data.is_object() {
                return Err(anyhow!("Data must be a JSON object"));
            }
            
            // 检查数据大小
            let data_size = serde_json::to_string(data)?.len();
            if data_size > 1024 * 1024 { // 1MB 限制
                return Err(anyhow!("Data size exceeds 1MB limit"));
            }
        }
        AtomicalOperation::Extract { id } => {
            // 验证提取操作
            /// 验证提取操作的参数，包括 Atomical ID。
            /// 
            /// # 参数
            /// * `id` - Atomical 的 ID
            /// 
            /// # 返回
            /// * `Result<()>` - 如果参数有效，返回 Ok(())；否则返回包含错误信息的 Err
            // 检查 Atomical 是否存在
            if !state.exists(id)? {
                return Err(anyhow!("Atomical does not exist"));
            }
            
            // 检查 Atomical 是否已封印
            if state.is_sealed(id)? {
                return Err(anyhow!("Cannot extract sealed Atomical"));
            }
            
            // 检查交易是否有有效的输出
            if tx.output.len() < 2 {
                return Err(anyhow!("Extract operation requires at least two outputs"));
            }
        }
        AtomicalOperation::Split { id, outputs } => {
            // 验证分割操作
            /// 验证分割操作的参数，包括 Atomical ID 和输出列表。
            /// 
            /// # 参数
            /// * `id` - Atomical 的 ID
            /// * `outputs` - 输出列表
            /// 
            /// # 返回
            /// * `Result<()>` - 如果参数有效，返回 Ok(())；否则返回包含错误信息的 Err
            // 检查 Atomical 是否存在
            if !state.exists(id)? {
                return Err(anyhow!("Atomical does not exist"));
            }
            
            // 检查 Atomical 是否已封印
            if state.is_sealed(id)? {
                return Err(anyhow!("Cannot split sealed Atomical"));
            }
            
            // 检查输出列表是否为空
            if outputs.is_empty() {
                return Err(anyhow!("Split operation must have at least one output"));
            }
            
            // 检查输出索引是否有效
            for (output_index, _) in outputs {
                if *output_index as usize >= tx.output.len() {
                    return Err(anyhow!("Invalid output index in split operation"));
                }
            }
            
            // 检查分割金额是否有效
            for (_, amount) in outputs {
                if *amount == 0 {
                    return Err(anyhow!("Split amount must be greater than 0"));
                }
            }
            
            // 检查输出索引是否有重复
            let mut output_indices = std::collections::HashSet::new();
            for (output_index, _) in outputs {
                if !output_indices.insert(*output_index) {
                    return Err(anyhow!("Duplicate output index in split operation"));
                }
            }
            
            // 这里可以添加更多的验证逻辑，例如检查是否超过最大供应量
            // 但这需要获取 Atomical 的更多信息，如最大供应量和当前已铸造数量
        }
    }

    Ok(())
}

/// 验证铸造操作
/// 
/// 检查铸造操作的参数是否有效，包括 Atomical 类型和元数据。
/// 
/// # 参数
/// * `atomical_type` - Atomical 的类型（NFT、FT 等）
/// * `metadata` - 可选的元数据
/// 
/// # 返回
/// * `Result<()>` - 如果参数有效，返回 Ok(())；否则返回包含错误信息的 Err
fn validate_mint(atomical_type: &AtomicalType, metadata: &Option<Value>) -> Result<()> {
    // 检查元数据是否存在
    let metadata = metadata.as_ref().ok_or_else(|| anyhow!("Metadata is required"))?;
    
    // 验证元数据
    if !metadata.is_object() {
        return Err(anyhow!("Metadata must be a JSON object"));
    }
    
    // 检查元数据大小
    let metadata_str = metadata.to_string();
    if metadata_str.len() > 1_000_000 { // 1MB 限制
        return Err(anyhow!("Metadata size exceeds 1MB limit"));
    }
    
    // 验证类型特定的规则
    match atomical_type {
        AtomicalType::NFT => {
            // 验证 NFT 特定的规则
            if !metadata.get("name").and_then(Value::as_str).is_some() {
                return Err(anyhow!("NFT metadata must contain a name field"));
            }
            
            // 验证名称长度
            if let Some(name) = metadata.get("name").and_then(Value::as_str) {
                if name.is_empty() || name.len() > 64 {
                    return Err(anyhow!("NFT name must be 1-64 characters"));
                }
            }
        }
        AtomicalType::FT => {
            // 验证 FT 特定的规则
            if !metadata.get("ticker").and_then(Value::as_str).is_some() {
                return Err(anyhow!("FT metadata must contain a ticker field"));
            }
            
            // 验证 ticker 格式
            if let Some(ticker) = metadata.get("ticker").and_then(Value::as_str) {
                if ticker.is_empty() || ticker.len() > 10 {
                    return Err(anyhow!("Ticker must be 1-10 characters"));
                }
                
                // 验证 ticker 只包含大写字母和数字
                if !ticker.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()) {
                    return Err(anyhow!("Ticker must contain only uppercase ASCII letters and digits"));
                }
                
                // 验证 ticker 必须以字母开头
                if let Some(first_char) = ticker.chars().next() {
                    if !first_char.is_ascii_alphabetic() {
                        return Err(anyhow!("Ticker must start with an alphabetic character"));
                    }
                }
            }
            
            if !metadata.get("max_supply").and_then(Value::as_u64).is_some() {
                return Err(anyhow!("FT metadata must contain a max_supply field"));
            }
            
            // 验证最大供应量
            if let Some(max_supply) = metadata.get("max_supply").and_then(Value::as_u64) {
                if max_supply == 0 {
                    return Err(anyhow!("Max supply must be greater than 0"));
                }
                
                if max_supply > 21_000_000_000_000_000 { // 21 quadrillion
                    return Err(anyhow!("Max supply exceeds reasonable limit"));
                }
            }
        }
        AtomicalType::DID => {
            // 验证 DID 特定的规则
            if !metadata.get("did").and_then(Value::as_str).is_some() {
                return Err(anyhow!("DID metadata must contain a did field"));
            }
            
            // 验证 DID 格式
            if let Some(did) = metadata.get("did").and_then(Value::as_str) {
                if did.is_empty() || did.len() > 64 {
                    return Err(anyhow!("DID must be 1-64 characters"));
                }
                
                // 验证 DID 只包含有效字符
                if !did.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
                    return Err(anyhow!("DID must contain only alphanumeric characters, hyphens, and underscores"));
                }
            }
        }
        AtomicalType::Container => {
            // 验证 Container 特定的规则
            if !metadata.get("container_name").and_then(Value::as_str).is_some() {
                return Err(anyhow!("Container metadata must contain a container_name field"));
            }
            
            // 验证容器名称格式
            if let Some(container_name) = metadata.get("container_name").and_then(Value::as_str) {
                if container_name.is_empty() || container_name.len() > 64 {
                    return Err(anyhow!("Container name must be 1-64 characters"));
                }
                
                // 验证容器名称只包含有效字符
                if !container_name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
                    return Err(anyhow!("Container name must contain only alphanumeric characters, hyphens, and underscores"));
                }
            }
        }
        AtomicalType::Realm => {
            // 验证 Realm 特定的规则
            if !metadata.get("realm_name").and_then(Value::as_str).is_some() {
                return Err(anyhow!("Realm metadata must contain a realm_name field"));
            }
            
            // 验证领域名称格式
            if let Some(realm_name) = metadata.get("realm_name").and_then(Value::as_str) {
                if realm_name.is_empty() || realm_name.len() > 64 {
                    return Err(anyhow!("Realm name must be 1-64 characters"));
                }
                
                // 验证领域名称只包含有效字符
                if !realm_name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.') {
                    return Err(anyhow!("Realm name must contain only alphanumeric characters, hyphens, underscores, and dots"));
                }
            }
        }
        AtomicalType::Unknown => {
            // 对于未知类型，我们不进行特定验证
            return Err(anyhow!("Unknown atomical type"));
        }
    }
    
    Ok(())
}

/// 验证更新操作
/// 
/// 检查更新操作的参数是否有效，包括 Atomical ID 和元数据。
/// 
/// # 参数
/// * `state` - Atomicals 状态接口，用于查询 Atomical 的状态
/// * `tx` - 包含操作的交易
/// * `id` - Atomical 的 ID
/// 
/// # 返回
/// * `Result<()>` - 如果参数有效，返回 Ok(())；否则返回包含错误信息的 Err
fn validate_update<S: AtomicalsStateInterface>(state: &S, tx: &Transaction, id: &AtomicalId) -> Result<()> {
    // 检查 Atomical 是否存在
    if !state.exists(id)? {
        return Err(anyhow!("Atomical does not exist"));
    }
    
    // 检查 Atomical 是否已被封印
    if state.is_sealed(id)? {
        return Err(anyhow!("Cannot update sealed Atomical"));
    }
    
    // 检查交易是否有输出
    if tx.output.is_empty() {
        return Err(anyhow!("Transaction must have at least one output"));
    }
    
    // 检查第一个输出是否有效
    if tx.output[0].script_pubkey.is_empty() {
        return Err(anyhow!("First output script cannot be empty"));
    }
    
    // 检查第一个输出金额是否足够
    if tx.output[0].value == bitcoin::Amount::from_sat(0) {
        return Err(anyhow!("First output value must be greater than 0"));
    }
    
    Ok(())
}

/// 验证封印操作
/// 
/// 检查封印操作的参数是否有效，包括 Atomical ID。
/// 
/// # 参数
/// * `state` - Atomicals 状态接口，用于查询 Atomical 的状态
/// * `tx` - 包含操作的交易
/// * `id` - Atomical 的 ID
/// 
/// # 返回
/// * `Result<()>` - 如果参数有效，返回 Ok(())；否则返回包含错误信息的 Err
fn validate_seal<S: AtomicalsStateInterface>(state: &S, tx: &Transaction, id: &AtomicalId) -> Result<()> {
    // 检查 Atomical 是否存在
    if !state.exists(id)? {
        return Err(anyhow!("Atomical does not exist"));
    }
    
    // 检查 Atomical 是否已被封印
    if state.is_sealed(id)? {
        return Err(anyhow!("Atomical is already sealed"));
    }
    
    // 检查交易是否有输出
    if tx.output.is_empty() {
        return Err(anyhow!("Transaction must have at least one output"));
    }
    
    // 检查第一个输出是否有效
    if tx.output[0].script_pubkey.is_empty() {
        return Err(anyhow!("First output script cannot be empty"));
    }
    
    // 检查第一个输出金额是否足够
    if tx.output[0].value == bitcoin::Amount::from_sat(0) {
        return Err(anyhow!("First output value must be greater than 0"));
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use bitcoin::{Amount, OutPoint, Script, Sequence, TxIn, TxOut, Txid, Witness};
    use bitcoin::script::Builder;
    use bitcoin::hashes::Hash;
    
    fn create_test_atomical_id() -> AtomicalId {
        let txid = bitcoin::Txid::from_str(
            "1234567890123456789012345678901234567890123456789012345678901234"
        ).unwrap();
        AtomicalId { txid, vout: 0 }
    }

    fn create_test_transaction() -> Transaction {
        Transaction {
            version: bitcoin::transaction::Version(2),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![],
            output: vec![
                TxOut {
                    value: Amount::from_sat(1000).to_sat(),
                    script_pubkey: Builder::new().into_script(),
                }
            ],
        }
    }

    fn create_test_state() -> impl AtomicalsStateInterface {
        struct MockAtomicalsState {
            sealed_ids: std::collections::HashSet<AtomicalId>,
            existing_ids: std::collections::HashSet<AtomicalId>,
            atomical_types: std::collections::HashMap<AtomicalId, AtomicalType>,
        }
        
        impl AtomicalsStateInterface for MockAtomicalsState {
            fn exists(&self, id: &AtomicalId) -> Result<bool> {
                Ok(self.existing_ids.contains(id))
            }
            
            fn is_sealed(&self, id: &AtomicalId) -> Result<bool> {
                Ok(self.sealed_ids.contains(id))
            }
            
            fn create(&mut self, id: &AtomicalId, atomical_type: AtomicalType) -> Result<()> {
                self.existing_ids.insert(id.clone());
                self.atomical_types.insert(id.clone(), atomical_type);
                Ok(())
            }
            
            fn seal(&mut self, id: &AtomicalId) -> Result<()> {
                if !self.existing_ids.contains(id) {
                    return Err(anyhow!("Atomical does not exist"));
                }
                self.sealed_ids.insert(id.clone());
                Ok(())
            }
            
            fn get_atomical_type(&self, id: &AtomicalId) -> Result<AtomicalType> {
                self.atomical_types.get(id)
                    .cloned()
                    .ok_or_else(|| anyhow!("Atomical type not found"))
            }
        }
        
        MockAtomicalsState {
            sealed_ids: std::collections::HashSet::new(),
            existing_ids: std::collections::HashSet::new(),
            atomical_types: std::collections::HashMap::new(),
        }
    }
    
    #[test]
    fn test_validate_mint() {
        // 测试有效的 NFT 元数据
        let nft_metadata = serde_json::json!({
            "name": "Test NFT",
            "description": "A test NFT"
        });
        assert!(validate_mint(&AtomicalType::NFT, &Some(nft_metadata.clone())).is_ok());

        // 测试有效的 FT 元数据
        let ft_metadata = serde_json::json!({
            "name": "Test FT",
            "description": "A test FT",
            "supply": 1000
        });
        assert!(validate_mint(&AtomicalType::FT, &Some(ft_metadata)).is_ok());

        // 测试缺少必需字段的 NFT
        let invalid_nft = serde_json::json!({
            "description": "Missing name"
        });
        assert!(validate_mint(&AtomicalType::NFT, &Some(invalid_nft)).is_err());

        // 测试缺少必需字段的 FT
        let invalid_ft = serde_json::json!({
            "name": "Missing description and supply"
        });
        assert!(validate_mint(&AtomicalType::FT, &Some(invalid_ft)).is_err());

        // 测试无效的供应量
        let invalid_supply = serde_json::json!({
            "name": "Invalid Supply",
            "description": "FT with invalid supply",
            "supply": -1
        });
        assert!(validate_mint(&AtomicalType::FT, &Some(invalid_supply)).is_err());

        // 测试无元数据
        assert!(validate_mint(&AtomicalType::NFT, &None).is_err());
        assert!(validate_mint(&AtomicalType::FT, &None).is_err());

        // 测试无效的类型
        assert!(validate_mint(&AtomicalType::Unknown, &Some(nft_metadata.clone())).is_err());
    }

    #[test]
    fn test_validate_update() {
        let mut state = create_test_state();
        let tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();

        // 测试更新不存在的 Atomical
        assert!(validate_update(&state, &tx, &atomical_id).is_err());

        // 模拟 Atomical 存在
        state.create(&atomical_id, AtomicalType::NFT).unwrap();
        assert!(validate_update(&state, &tx, &atomical_id).is_ok());

        // 测试更新已封印的 Atomical
        state.seal(&atomical_id).unwrap();
        assert!(validate_update(&state, &tx, &atomical_id).is_err());
    }

    #[test]
    fn test_validate_seal() {
        let mut state = create_test_state();
        let tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();

        // 测试封印不存在的 Atomical
        assert!(validate_seal(&state, &tx, &atomical_id).is_err());

        // 模拟 Atomical 存在
        state.create(&atomical_id, AtomicalType::NFT).unwrap();
        assert!(validate_seal(&state, &tx, &atomical_id).is_ok());

        // 测试重复封印
        state.seal(&atomical_id).unwrap();
        assert!(validate_seal(&state, &tx, &atomical_id).is_err());
    }

    #[test]
    fn test_validate_transfer() {
        let mut state = create_test_state();
        let mut tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();

        // 测试转移不存在的 Atomical
        assert!(validate_operation(
            &AtomicalOperation::Transfer { id: atomical_id, output_index: 0 },
            &tx,
            &state
        ).is_err());

        // 模拟 Atomical 存在
        state.create(&atomical_id, AtomicalType::NFT).unwrap();
        assert!(validate_operation(
            &AtomicalOperation::Transfer { id: atomical_id, output_index: 0 },
            &tx,
            &state
        ).is_ok());

        // 测试无效的输出索引
        assert!(validate_operation(
            &AtomicalOperation::Transfer { id: atomical_id, output_index: 1 },
            &tx,
            &state
        ).is_err());

        // 测试转移已封印的 Atomical
        state.seal(&atomical_id).unwrap();
        assert!(validate_operation(
            &AtomicalOperation::Transfer { id: atomical_id, output_index: 0 },
            &tx,
            &state
        ).is_err());
    }

    #[test]
    fn test_validator() {
        let mut state = create_test_state();
        let validator = AtomicalsValidator::new(state);
        let tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();

        // 测试铸造操作
        let mint_op = AtomicalOperation::Mint {
            id: atomical_id,
            atomical_type: AtomicalType::NFT,
            metadata: serde_json::json!({
                "name": "Test NFT",
                "description": "Test Description",
            }),
        };
        assert!(validator.validate_operation(&tx, &mint_op).is_ok());

        // 测试无效的铸造操作
        let invalid_mint_op = AtomicalOperation::Mint {
            id: atomical_id,
            atomical_type: AtomicalType::NFT,
            metadata: serde_json::json!({
                "description": "Missing name field",
            }),
        };
        assert!(validator.validate_operation(&tx, &invalid_mint_op).is_err());

        // 测试转移操作
        validator.state.create(&atomical_id, AtomicalType::NFT).unwrap();
        let transfer_op = AtomicalOperation::Transfer {
            id: atomical_id,
            output_index: 0,
        };
        assert!(validator.validate_operation(&tx, &transfer_op).is_ok());
    }

    #[test]
    fn test_validate_deploy_dft() {
        let tx = create_test_transaction();
        let mut state = create_test_state();
        let id = create_test_atomical_id();
        
        // 测试有效的 DeployDFT 操作
        let valid_deploy = AtomicalOperation::DeployDFT {
            id: id.clone(),
            ticker: "ABC".to_string(),
            max_supply: 1000,
            metadata: Some(serde_json::json!({
                "name": "Test Token",
                "description": "A test token"
            })),
        };
        assert!(validate_operation(&valid_deploy, &tx, &state).is_ok());
        
        // 测试无效的 ticker（空）
        let invalid_ticker_empty = AtomicalOperation::DeployDFT {
            id: id.clone(),
            ticker: "".to_string(),
            max_supply: 1000,
            metadata: Some(serde_json::json!({
                "name": "Test Token",
                "description": "A test token"
            })),
        };
        assert!(validate_operation(&invalid_ticker_empty, &tx, &state).is_err());
        
        // 测试无效的 ticker（太长）
        let invalid_ticker_long = AtomicalOperation::DeployDFT {
            id: id.clone(),
            ticker: "ABCDEFGHIJK".to_string(), // 11 characters
            max_supply: 1000,
            metadata: Some(serde_json::json!({
                "name": "Test Token",
                "description": "A test token"
            })),
        };
        assert!(validate_operation(&invalid_ticker_long, &tx, &state).is_err());
        
        // 测试无效的 ticker（非大写字母）
        let invalid_ticker_lowercase = AtomicalOperation::DeployDFT {
            id: id.clone(),
            ticker: "abc".to_string(),
            max_supply: 1000,
            metadata: Some(serde_json::json!({
                "name": "Test Token",
                "description": "A test token"
            })),
        };
        assert!(validate_operation(&invalid_ticker_lowercase, &tx, &state).is_err());
        
        // 测试无效的 ticker（以数字开头）
        let invalid_ticker_start_with_digit = AtomicalOperation::DeployDFT {
            id: id.clone(),
            ticker: "1ABC".to_string(),
            max_supply: 1000,
            metadata: Some(serde_json::json!({
                "name": "Test Token",
                "description": "A test token"
            })),
        };
        assert!(validate_operation(&invalid_ticker_start_with_digit, &tx, &state).is_err());
        
        // 测试无效的最大供应量（零）
        let invalid_supply_zero = AtomicalOperation::DeployDFT {
            id: id.clone(),
            ticker: "ABC".to_string(),
            max_supply: 0,
            metadata: Some(serde_json::json!({
                "name": "Test Token",
                "description": "A test token"
            })),
        };
        assert!(validate_operation(&invalid_supply_zero, &tx, &state).is_err());
        
        // 测试无效的最大供应量（超出合理范围）
        let invalid_supply_large = AtomicalOperation::DeployDFT {
            id: id.clone(),
            ticker: "ABC".to_string(),
            max_supply: 22_000_000_000_000_000, // 超出限制
            metadata: Some(serde_json::json!({
                "name": "Test Token",
                "description": "A test token"
            })),
        };
        assert!(validate_operation(&invalid_supply_large, &tx, &state).is_err());
    }
    
    #[test]
    fn test_validate_mint_dft() {
        let tx = create_test_transaction();
        let mut state = create_test_state();
        let parent_id = create_test_atomical_id();
        
        // 测试有效的 MintDFT 操作
        let valid_mint = AtomicalOperation::MintDFT {
            parent_id: parent_id.clone(),
            amount: 100,
        };
        assert!(validate_operation(&valid_mint, &tx, &state).is_err()); // 因为父 Atomical 不存在
        
        // 模拟父 Atomical 存在
        state.create(&parent_id, AtomicalType::FT).unwrap();
        assert!(validate_operation(&valid_mint, &tx, &state).is_ok());
        
        // 测试无效的铸造数量（零）
        let invalid_amount_zero = AtomicalOperation::MintDFT {
            parent_id: parent_id.clone(),
            amount: 0,
        };
        assert!(validate_operation(&invalid_amount_zero, &tx, &state).is_err());

        // 测试无效的父 Atomical 类型
        state.create(&parent_id, AtomicalType::NFT).unwrap();
        
        let invalid_parent_type = AtomicalOperation::MintDFT {
            parent_id: parent_id,
            amount: 100,
        };
        assert!(validate_operation(&invalid_parent_type, &tx, &state).is_err());
    }
    
    #[test]
    fn test_validate_event_and_data() {
        let tx = create_test_transaction();
        let state = create_test_state();
        let id = create_test_atomical_id();
        
        // 测试 Event 操作
        let valid_event = AtomicalOperation::Event {
            id: id.clone(),
            data: serde_json::json!({
                "type": "test",
                "message": "This is a test event"
            }),
        };
        // 由于 Atomical 不存在，应该返回错误
        assert!(validate_operation(&valid_event, &tx, &state).is_err());
        
        // 测试 Data 操作
        let valid_data = AtomicalOperation::Data {
            id: id.clone(),
            data: serde_json::json!({
                "key": "value",
                "number": 123
            }),
        };
        // 由于 Atomical 不存在，应该返回错误
        assert!(validate_operation(&valid_data, &tx, &state).is_err());
        
        // 测试无效的数据格式（非对象）
        let invalid_data_format = AtomicalOperation::Data {
            id: id.clone(),
            data: serde_json::json!("not an object"),
        };
        // 由于 Atomical 不存在，错误信息会是关于不存在而不是格式问题
        assert!(validate_operation(&invalid_data_format, &tx, &state).is_err());
    }
    
    #[test]
    fn test_validate_extract_and_split() {
        let tx = create_test_transaction();
        let state = create_test_state();
        let id = create_test_atomical_id();
        
        // 测试 Extract 操作
        let extract_op = AtomicalOperation::Extract {
            id: id.clone(),
        };
        // 由于 Atomical 不存在，应该返回错误
        assert!(validate_operation(&extract_op, &tx, &state).is_err());
        
        // 测试有效的 Split 操作
        let valid_split = AtomicalOperation::Split {
            id: id.clone(),
            outputs: vec![(0, 50), (1, 50)],
        };
        // 由于 Atomical 不存在，应该返回错误
        assert!(validate_operation(&valid_split, &tx, &state).is_err());
        
        // 测试无效的 Split 操作（空输出）
        let invalid_split_empty = AtomicalOperation::Split {
            id: id.clone(),
            outputs: vec![],
        };
        // 由于 Atomical 不存在，错误信息会是关于不存在而不是空输出
        assert!(validate_operation(&invalid_split_empty, &tx, &state).is_err());
        
        // 测试无效的 Split 操作（输出索引无效）
        let invalid_split_index = AtomicalOperation::Split {
            id: id.clone(),
            outputs: vec![(99, 50)], // 索引超出范围
        };
        // 由于 Atomical 不存在，错误信息会是关于不存在而不是索引问题
        assert!(validate_operation(&invalid_split_index, &tx, &state).is_err());
        
        // 测试无效的 Split 操作（金额为零）
        let invalid_split_amount = AtomicalOperation::Split {
            id: id.clone(),
            outputs: vec![(0, 0)], // 金额为零
        };
        // 由于 Atomical 不存在，错误信息会是关于不存在而不是金额问题
        assert!(validate_operation(&invalid_split_amount, &tx, &state).is_err());
        
        // 这里可以添加更多的验证逻辑，例如检查是否超过最大供应量
        // 但这需要获取父 Atomical 的更多信息，如最大供应量和当前已铸造数量
    }
    
    #[test]
    fn test_validate_event_and_data_extended() {
        let tx = create_test_transaction();
        let mut state = create_test_state();
        let id = create_test_atomical_id();
        
        // 创建 Atomical 以测试更具体的验证
        state.create(&id, AtomicalType::NFT).unwrap();
        
        // 测试有效的 Event 操作
        let valid_event = AtomicalOperation::Event {
            id: id.clone(),
            data: serde_json::json!({
                "type": "test",
                "message": "This is a test event"
            }),
        };
        assert!(validate_operation(&valid_event, &tx, &state).is_ok());
        
        // 测试有效的 Data 操作
        let valid_data = AtomicalOperation::Data {
            id: id.clone(),
            data: serde_json::json!({
                "key": "value",
                "number": 123
            }),
        };
        assert!(validate_operation(&valid_data, &tx, &state).is_ok());
        
        // 测试无效的 Event 数据格式（非对象）
        let invalid_event_format = AtomicalOperation::Event {
            id: id.clone(),
            data: serde_json::json!("not an object"),
        };
        assert!(validate_operation(&invalid_event_format, &tx, &state).is_err());
        
        // 测试无效的 Data 数据格式（非对象）
        let invalid_data_format = AtomicalOperation::Data {
            id: id.clone(),
            data: serde_json::json!("not an object"),
        };
        assert!(validate_operation(&invalid_data_format, &tx, &state).is_err());
        
        // 测试无效的 Event 数据（过大）
        let large_string = "a".repeat(1024 * 1024 + 1); // 超过 1MB
        let invalid_event_size = AtomicalOperation::Event {
            id: id.clone(),
            data: serde_json::json!({
                "large_data": large_string
            }),
        };
        assert!(validate_operation(&invalid_event_size, &tx, &state).is_err());
        
        // 测试无效的 Data 数据（过大）
        let invalid_data_size = AtomicalOperation::Data {
            id: id.clone(),
            data: serde_json::json!({
                "large_data": large_string
            }),
        };
        assert!(validate_operation(&invalid_data_size, &tx, &state).is_err());
        
        // 测试封印后的 Atomical
        state.seal(&id).unwrap();
        
        // 封印后的 Atomical 不能进行 Event 操作
        assert!(validate_operation(&valid_event, &tx, &state).is_err());
        
        // 封印后的 Atomical 不能进行 Data 操作
        assert!(validate_operation(&valid_data, &tx, &state).is_err());
    }
    
    #[test]
    fn test_validate_extract_and_split_extended() {
        let tx = create_test_transaction();
        let mut state = create_test_state();
        let id = create_test_atomical_id();
        
        // 测试 Extract 操作
        let extract_op = AtomicalOperation::Extract {
            id: id.clone(),
        };
        // 由于 Atomical 不存在，应该返回错误
        assert!(validate_operation(&extract_op, &tx, &state).is_err());
        
        // 测试有效的 Split 操作
        let valid_split = AtomicalOperation::Split {
            id: id.clone(),
            outputs: vec![(0, 50), (1, 50)],
        };
        // 由于 Atomical 不存在，应该返回错误
        assert!(validate_operation(&valid_split, &tx, &state).is_err());
        
        // 测试无效的 Split 操作（空输出）
        let invalid_split_empty = AtomicalOperation::Split {
            id: id.clone(),
            outputs: vec![],
        };
        // 由于 Atomical 不存在，错误信息会是关于不存在而不是空输出
        assert!(validate_operation(&invalid_split_empty, &tx, &state).is_err());
        
        // 测试无效的 Split 操作（输出索引无效）
        let invalid_split_index = AtomicalOperation::Split {
            id: id.clone(),
            outputs: vec![(99, 50)], // 索引超出范围
        };
        // 由于 Atomical 不存在，错误信息会是关于不存在而不是索引问题
        assert!(validate_operation(&invalid_split_index, &tx, &state).is_err());
        
        // 测试无效的 Split 操作（金额为零）
        let invalid_split_amount = AtomicalOperation::Split {
            id: id.clone(),
            outputs: vec![(0, 0)], // 金额为零
        };
        // 由于 Atomical 不存在，错误信息会是关于不存在而不是金额问题
        assert!(validate_operation(&invalid_split_amount, &tx, &state).is_err());
        
        // 这里可以添加更多的验证逻辑，例如检查是否超过最大供应量
        // 但这需要获取父 Atomical 的更多信息，如最大供应量和当前已铸造数量
    }
    
    #[test]
    fn test_validate_deploy_dft_extended() {
        let tx = create_test_transaction();
        let mut state = create_test_state();
        let id = create_test_atomical_id();
        
        // 测试有效的 DeployDFT 操作（带数字的 ticker）
        let valid_deploy_with_digits = AtomicalOperation::DeployDFT {
            id: id.clone(),
            ticker: "ABC123".to_string(),
            max_supply: 1000,
            metadata: Some(serde_json::json!({
                "name": "Test Token",
                "description": "A test token with digits in ticker"
            })),
        };
        assert!(validate_operation(&valid_deploy_with_digits, &tx, &state).is_ok());
        
        // 测试无效的 ticker（特殊字符）
        let invalid_ticker_special_chars = AtomicalOperation::DeployDFT {
            id: id.clone(),
            ticker: "ABC!".to_string(),
            max_supply: 1000,
            metadata: Some(serde_json::json!({
                "name": "Test Token",
                "description": "A test token with special chars in ticker"
            })),
        };
        assert!(validate_operation(&invalid_ticker_special_chars, &tx, &state).is_err());
        
        // 测试无效的元数据（过大）
        let large_string = "a".repeat(1024 * 1024 + 1); // 超过 1MB
        let invalid_metadata_size = AtomicalOperation::DeployDFT {
            id: id.clone(),
            ticker: "ABC".to_string(),
            max_supply: 1000,
            metadata: Some(serde_json::json!({
                "name": "Test Token",
                "description": "A test token with large metadata",
                "large_data": large_string
            })),
        };
        assert!(validate_operation(&invalid_metadata_size, &tx, &state).is_err());
        
        // 测试 ID 已存在的情况
        state.create(&id, AtomicalType::NFT).unwrap();
        assert!(validate_operation(&valid_deploy_with_digits, &tx, &state).is_err());
    }
    
    #[test]
    fn test_validate_mint_dft_extended() {
        let tx = create_test_transaction();
        let mut state = create_test_state();
        let parent_id = create_test_atomical_id();
        
        // 创建父 Atomical（FT 类型）
        state.create(&parent_id, AtomicalType::FT).unwrap();
        
        // 测试有效的 MintDFT 操作（最小数量）
        let valid_mint_min = AtomicalOperation::MintDFT {
            parent_id: parent_id.clone(),
            amount: 1,
        };
        assert!(validate_operation(&valid_mint_min, &tx, &state).is_ok());
        
        // 测试有效的 MintDFT 操作（大数量）
        let valid_mint_large = AtomicalOperation::MintDFT {
            parent_id: parent_id.clone(),
            amount: 1_000_000_000,
        };
        assert!(validate_operation(&valid_mint_large, &tx, &state).is_ok());
        
        // 测试无效的 MintDFT 操作（父 ID 不存在）
        let another_id = AtomicalId {
            txid: parent_id.txid,
            vout: 1, // 不同的 vout
        };
        let invalid_parent = AtomicalOperation::MintDFT {
            parent_id: another_id,
            amount: 100,
        };
        assert!(validate_operation(&invalid_parent, &tx, &state).is_err());
        
        // 测试无效的 MintDFT 操作（金额为零）
        let invalid_amount = AtomicalOperation::MintDFT {
            parent_id: parent_id.clone(),
            amount: 0,
        };
        assert!(validate_operation(&invalid_amount, &tx, &state).is_err());

        // 测试无效的 MintDFT 操作（父 Atomical 类型不是 FT）
        let nft_id = AtomicalId {
            txid: parent_id.txid,
            vout: 2, // 不同的 vout
        };
        state.create(&nft_id, AtomicalType::NFT).unwrap();
        
        let invalid_parent_type = AtomicalOperation::MintDFT {
            parent_id: nft_id,
            amount: 100,
        };
        assert!(validate_operation(&invalid_parent_type, &tx, &state).is_err());
    }
    
    #[test]
    fn test_validate_operation_with_invalid_transaction() {
        // 创建无效的交易（无输入）
        let invalid_tx_no_inputs = Transaction {
            version: bitcoin::transaction::Version(2),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![],
            output: vec![
                TxOut {
                    value: Amount::from_sat(1000).to_sat(),
                    script_pubkey: Builder::new().into_script(),
                }
            ],
        };
        
        // 创建无效的交易（无输出）
        let invalid_tx_no_outputs = Transaction {
            version: bitcoin::transaction::Version(2),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_str("0000000000000000000000000000000000000000000000000000000000000000").unwrap(),
                    vout: 0,
                },
                script_sig: Script::new(),
                sequence: Sequence(0),
                witness: Witness::new(),
            }],
            output: vec![],
        };
        
        let mut state = create_test_state();
        let id = create_test_atomical_id();
        
        // 测试各种操作类型与无效交易
        let mint_op = AtomicalOperation::Mint {
            id: id.clone(),
            atomical_type: AtomicalType::NFT,
            metadata: Some(serde_json::json!({
                "name": "Test NFT",
                "description": "A test NFT"
            })),
        };
        
        assert!(validate_operation(&mint_op, &invalid_tx_no_inputs, &state).is_err());
        assert!(validate_operation(&mint_op, &invalid_tx_no_outputs, &state).is_err());
        
        let transfer_op = AtomicalOperation::Transfer {
            id: id.clone(),
            output_index: 0,
        };
        
        assert!(validate_operation(&transfer_op, &invalid_tx_no_inputs, &state).is_err());
        assert!(validate_operation(&transfer_op, &invalid_tx_no_outputs, &state).is_err());
        
        // 测试 DeployDFT 操作
        let deploy_dft_op = AtomicalOperation::DeployDFT {
            id: id.clone(),
            ticker: "TEST".to_string(),
            max_supply: 1000,
            metadata: None,
        };
        
        assert!(validate_operation(&deploy_dft_op, &invalid_tx_no_inputs, &state).is_err());
        assert!(validate_operation(&deploy_dft_op, &invalid_tx_no_outputs, &state).is_err());
        
        // 测试 MintDFT 操作
        let mint_dft_op = AtomicalOperation::MintDFT {
            parent_id: id.clone(),
            amount: 100,
        };
        
        assert!(validate_operation(&mint_dft_op, &invalid_tx_no_inputs, &state).is_err());
        assert!(validate_operation(&mint_dft_op, &invalid_tx_no_outputs, &state).is_err());
    }
    
    #[test]
    fn test_validate_transfer_extended() {
        let tx = create_test_transaction();
        let mut state = create_test_state();
        let id = create_test_atomical_id();
        
        // 创建 Atomical
        state.create(&id, AtomicalType::NFT).unwrap();
        
        // 测试有效的转移操作
        let valid_transfer = AtomicalOperation::Transfer {
            id: id.clone(),
            output_index: 0,
        };
        assert!(validate_operation(&valid_transfer, &tx, &state).is_ok());
        
        // 测试无效的输出索引
        let invalid_output_index = AtomicalOperation::Transfer {
            id: id.clone(),
            output_index: 99, // 超出范围
        };
        assert!(validate_operation(&invalid_output_index, &tx, &state).is_err());
        
        // 测试封印后的 Atomical
        state.seal(&id).unwrap();
        assert!(validate_operation(&valid_transfer, &tx, &state).is_err());
        
        // 测试不存在的 Atomical
        let non_existent_id = AtomicalId {
            txid: id.txid,
            vout: 99, // 不存在的 vout
        };
        let transfer_non_existent = AtomicalOperation::Transfer {
            id: non_existent_id,
            output_index: 0,
        };
        assert!(validate_operation(&transfer_non_existent, &tx, &state).is_err());
        
        // 测试无输出的交易
        let tx_no_outputs = Transaction {
            version: bitcoin::transaction::Version(2),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_str("0000000000000000000000000000000000000000000000000000000000000000").unwrap(),
                    vout: 0,
                },
                script_sig: Script::new(),
                sequence: Sequence(0),
                witness: Witness::new(),
            }],
            output: vec![],
        };
        
        // 创建新的 Atomical（未封印）
        let id2 = AtomicalId {
            txid: id.txid,
            vout: 1, // 不同的 vout
        };
        state.create(&id2, AtomicalType::NFT).unwrap();
        
        assert!(validate_operation(&AtomicalOperation::Transfer {
            id: id2.clone(),
            output_index: 0,
        }, &tx_no_outputs, &state).is_err());
    }
    
    #[test]
    fn test_validate_update_extended() {
        let tx = create_test_transaction();
        let mut state = create_test_state();
        let id = create_test_atomical_id();
        
        // 测试 Atomical 不存在的情况
        let update_op = AtomicalOperation::Update {
            id: id.clone(),
            metadata: serde_json::json!({
                "name": "Updated NFT",
                "description": "An updated NFT"
            }),
        };
        assert!(validate_operation(&update_op, &tx, &state).is_err());
        
        // 创建 Atomical
        state.create(&id, AtomicalType::NFT).unwrap();
        assert!(validate_operation(&update_op, &tx, &state).is_ok());
        
        // 测试封印后的 Atomical
        state.seal(&id).unwrap();
        assert!(validate_operation(&update_op, &tx, &state).is_err());
        
        // 测试无输出的交易
        let tx_no_outputs = Transaction {
            version: bitcoin::transaction::Version(2),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_str("0000000000000000000000000000000000000000000000000000000000000000").unwrap(),
                    vout: 0,
                },
                script_sig: Script::new(),
                sequence: Sequence(0),
                witness: Witness::new(),
            }],
            output: vec![],
        };
        
        // 创建新的 Atomical（未封印）
        let id2 = AtomicalId {
            txid: id.txid,
            vout: 1, // 不同的 vout
        };
        state.create(&id2, AtomicalType::NFT).unwrap();
        
        assert!(validate_operation(&AtomicalOperation::Update {
            id: id2.clone(),
            metadata: serde_json::json!({
                "name": "Updated NFT",
                "description": "An updated NFT"
            }),
        }, &tx_no_outputs, &state).is_err());
        
        // 测试无效的元数据（过大）
        let large_string = "a".repeat(1024 * 1024 + 1); // 超过 1MB
        let invalid_metadata_size = AtomicalOperation::Update {
            id: id2.clone(),
            metadata: serde_json::json!({
                "name": "Updated NFT",
                "description": "An updated NFT with large metadata",
                "large_data": large_string
            }),
        };
        assert!(validate_operation(&invalid_metadata_size, &tx, &state).is_err());
    }
    
    #[test]
    fn test_validate_seal_extended() {
        let tx = create_test_transaction();
        let mut state = create_test_state();
        let id = create_test_atomical_id();
        
        // 测试 Atomical 不存在的情况
        let seal_op = AtomicalOperation::Seal {
            id: id.clone(),
        };
        assert!(validate_operation(&seal_op, &tx, &state).is_err());
        
        // 创建 Atomical
        state.create(&id, AtomicalType::NFT).unwrap();
        assert!(validate_operation(&seal_op, &tx, &state).is_ok());
        
        // 测试已封印的 Atomical
        state.seal(&id).unwrap();
        assert!(validate_operation(&seal_op, &tx, &state).is_err());
        
        // 测试无输出的交易
        let tx_no_outputs = Transaction {
            version: bitcoin::transaction::Version(2),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_str("0000000000000000000000000000000000000000000000000000000000000000").unwrap(),
                    vout: 0,
                },
                script_sig: Script::new(),
                sequence: Sequence(0),
                witness: Witness::new(),
            }],
            output: vec![],
        };
        
        // 创建新的 Atomical（未封印）
        let id2 = AtomicalId {
            txid: id.txid,
            vout: 1, // 不同的 vout
        };
        state.create(&id2, AtomicalType::NFT).unwrap();
        
        assert!(validate_operation(&AtomicalOperation::Seal {
            id: id2.clone(),
        }, &tx_no_outputs, &state).is_err());
    }
}

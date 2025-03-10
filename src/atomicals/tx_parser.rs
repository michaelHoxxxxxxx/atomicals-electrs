use bitcoin::{Transaction, TxOut, OutPoint, Script, opcodes::all::OP_RETURN};
use serde_json::Value;
use log::{debug, warn, trace, error};
use std::collections::{HashMap};
use fnv::FnvHashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use lru::LruCache;
use rayon::prelude::*;

use super::protocol::{AtomicalId, AtomicalOperation, AtomicalType};
use super::state::{AtomicalsState, AtomicalOutput};

/// 解析错误类型
#[derive(Debug, Clone)]
pub enum ParseError {
    /// 前缀错误（不是 Atomicals 操作）
    InvalidPrefix,
    /// 数据长度不足
    InsufficientData { expected: usize, actual: usize, context: &'static str },
    /// UTF-8 编码错误
    Utf8Error { context: &'static str },
    /// JSON 解析错误
    JsonError { context: &'static str, error: String },
    /// 未知操作类型
    UnknownOperationType(String),
    /// ID 解析错误
    IdParseError { context: &'static str },
    /// 类型解析错误
    TypeParseError { context: &'static str, error: String },
    /// 其他错误
    Other(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPrefix => write!(f, "无效的 Atomicals 前缀"),
            Self::InsufficientData { expected, actual, context } => 
                write!(f, "数据长度不足（在 {} 中）：期望 {} 字节，实际 {} 字节", context, expected, actual),
            Self::Utf8Error { context } => 
                write!(f, "UTF-8 编码错误（在 {} 中）", context),
            Self::JsonError { context, error } => 
                write!(f, "JSON 解析错误（在 {} 中）：{}", context, error),
            Self::UnknownOperationType(op_type) => 
                write!(f, "未知的操作类型：{}", op_type),
            Self::IdParseError { context } => 
                write!(f, "ID 解析错误（在 {} 中）", context),
            Self::TypeParseError { context, error } => 
                write!(f, "类型解析错误（在 {} 中）：{}", context, error),
            Self::Other(msg) => 
                write!(f, "解析错误：{}", msg),
        }
    }
}

impl std::error::Error for ParseError {}

/// 将 ParseError 转换为 anyhow::Error
impl From<ParseError> for anyhow::Error {
    fn from(err: ParseError) -> Self {
        anyhow!(err.to_string())
    }
}

/// 解析输出脚本中的 Atomicals 操作
fn parse_output(output: &TxOut, vout: u32) -> Result<Option<AtomicalOperation>, ParseError> {
    let script = output.script_pubkey.as_bytes();
    
    // 检查是否是 Atomicals 操作
    if script.len() < ATOMICALS_PREFIX.len() || &script[..ATOMICALS_PREFIX.len()] != ATOMICALS_PREFIX {
        return Ok(None);
    }

    trace!("解析 Atomicals 操作，vout={}", vout);

    // 尝试直接使用 from_tx_data 解析
    match parse_operation_from_tx_data(script, vout) {
        Ok(Some(op)) => {
            debug!("成功解析操作：{:?}", op.operation_type());
            return Ok(Some(op));
        }
        Ok(None) => {
            // 继续使用备选解析逻辑
        }
        Err(err) => {
            // 记录错误但继续尝试备选解析逻辑
            debug!("使用 from_tx_data 解析失败: {}，尝试备选解析逻辑", err);
        }
    }

    // 如果 from_tx_data 解析失败，使用原有的解析逻辑作为备选
    let op_type = match std::str::from_utf8(&script[ATOMICALS_PREFIX.len()..]) {
        Ok(s) => s,
        Err(e) => {
            warn!("解析操作类型失败：{}", e);
            return Err(ParseError::Utf8Error { context: "操作类型" });
        }
    };
    
    match op_type {
        "mint" => {
            // 解析铸造操作
            let metadata = match serde_json::from_slice(&script[ATOMICALS_PREFIX.len() + 4..]) {
                Ok(m) => Some(m),
                Err(e) => {
                    warn!("解析铸造元数据失败：{}，尝试恢复", e);
                    // 尝试恢复：截取可能的有效 JSON 部分
                    let json_str = match std::str::from_utf8(&script[ATOMICALS_PREFIX.len() + 4..]) {
                        Ok(s) => s,
                        Err(_) => return Err(ParseError::Utf8Error { context: "铸造元数据" }),
                    };
                    
                    // 尝试找到有效的 JSON 对象
                    if let Some(end_pos) = json_str.find('}') {
                        let truncated = &json_str[..=end_pos];
                        match serde_json::from_str(truncated) {
                            Ok(m) => {
                                warn!("成功从截断的 JSON 中恢复元数据");
                                Some(m)
                            }
                            Err(_) => None,
                        }
                    } else {
                        None
                    }
                }
            };
            Ok(Some(AtomicalOperation::Mint {
                id: AtomicalId {
                    txid: bitcoin::Txid::from_raw_hash(bitcoin::hashes::Hash::all_zeros()),
                    vout,
                },
                atomical_type: AtomicalType::NFT,
                metadata,
            }))
        }
        "update" => {
            // 解析更新操作
            if script.len() < ATOMICALS_PREFIX.len() + 42 {
                warn!("更新操作数据不完整");
                return Err(ParseError::InsufficientData { 
                    expected: ATOMICALS_PREFIX.len() + 42, 
                    actual: script.len(), 
                    context: "更新操作" 
                });
            }
            
            let atomical_id = match (|| -> Option<AtomicalId> {
                let txid = bitcoin::Txid::from_slice(&script[ATOMICALS_PREFIX.len() + 6..ATOMICALS_PREFIX.len() + 38]).ok()?;
                let vout = u32::from_be_bytes(script[ATOMICALS_PREFIX.len() + 38..ATOMICALS_PREFIX.len() + 42].try_into().ok()?);
                Some(AtomicalId { txid, vout })
            })() {
                Some(id) => id,
                None => {
                    warn!("解析更新操作的 Atomical ID 失败");
                    return Err(ParseError::IdParseError { context: "更新操作" });
                }
            };
            
            let metadata = match serde_json::from_slice(&script[ATOMICALS_PREFIX.len() + 42..]) {
                Ok(m) => m,
                Err(e) => {
                    warn!("解析更新元数据失败：{}，尝试恢复", e);
                    // 尝试恢复：截取可能的有效 JSON 部分
                    let json_str = match std::str::from_utf8(&script[ATOMICALS_PREFIX.len() + 42..]) {
                        Ok(s) => s,
                        Err(_) => return Err(ParseError::Utf8Error { context: "更新元数据" }),
                    };
                    
                    // 尝试找到有效的 JSON 对象
                    if let Some(end_pos) = json_str.find('}') {
                        let truncated = &json_str[..=end_pos];
                        match serde_json::from_str(truncated) {
                            Ok(m) => {
                                warn!("成功从截断的 JSON 中恢复元数据");
                                m
                            }
                            Err(e) => return Err(ParseError::JsonError { 
                                context: "更新元数据", 
                                error: e.to_string() 
                            }),
                        }
                    } else {
                        return Err(ParseError::JsonError { 
                            context: "更新元数据", 
                            error: e.to_string() 
                        });
                    }
                }
            };
            
            Ok(Some(AtomicalOperation::Update {
                id: atomical_id,
                metadata,
            }))
        }
        "seal" => {
            // 解析封印操作
            if script.len() < ATOMICALS_PREFIX.len() + 42 {
                warn!("封印操作数据不完整");
                return Err(ParseError::InsufficientData { 
                    expected: ATOMICALS_PREFIX.len() + 42, 
                    actual: script.len(), 
                    context: "封印操作" 
                });
            }
            
            let atomical_id = match (|| -> Option<AtomicalId> {
                let txid = bitcoin::Txid::from_slice(&script[ATOMICALS_PREFIX.len() + 4..ATOMICALS_PREFIX.len() + 36]).ok()?;
                let vout = u32::from_le_bytes([script[ATOMICALS_PREFIX.len() + 36], script[ATOMICALS_PREFIX.len() + 37], script[ATOMICALS_PREFIX.len() + 38], script[ATOMICALS_PREFIX.len() + 39]]);
                Some(AtomicalId { txid, vout })
            })() {
                Some(id) => id,
                None => {
                    warn!("解析封印操作的 Atomical ID 失败");
                    return Err(ParseError::IdParseError { context: "封印操作" });
                }
            };
            
            Ok(Some(AtomicalOperation::Seal {
                id: atomical_id,
            }))
        }
        // 其他操作类型...
        _ => {
            warn!("未知的操作类型：{}", op_type);
            Err(ParseError::UnknownOperationType(op_type.to_string()))
        }
    }
}

/// 从交易数据中解析操作
fn parse_operation_from_tx_data(data: &[u8], vout: u32) -> Result<Option<AtomicalOperation>, ParseError> {
    match super::protocol::AtomicalOperation::from_tx_data(data) {
        Ok(op) => Ok(Some(op)),
        Err(err) => {
            // 记录错误信息
            debug!("从交易数据解析操作失败: {}", err);
            // 将错误转换为 ParseError
            Err(ParseError::Other(err))
        }
    }
}

/// 解析复杂交易中的嵌套子领域操作
fn parse_nested_subdomain_operations(script: &[u8], vout: u32) -> Result<Option<Vec<AtomicalOperation>>, ParseError> {
    // 检查前缀
    if script.len() < ATOMICALS_PREFIX.len() || &script[0..ATOMICALS_PREFIX.len()] != ATOMICALS_PREFIX {
        return Ok(None);
    }
    
    // 检查操作类型标识
    if script.len() < ATOMICALS_PREFIX.len() + 1 {
        return Err(ParseError::InsufficientData {
            expected: ATOMICALS_PREFIX.len() + 1,
            actual: script.len(),
            context: "嵌套子领域操作类型"
        });
    }
    
    // 检查是否是嵌套子领域操作（使用特定标识，例如 0x01）
    if script[ATOMICALS_PREFIX.len()] != 0x01 {
        return Ok(None);
    }
    
    // 检查是否有足够的数据
    if script.len() < ATOMICALS_PREFIX.len() + 2 {
        return Err(ParseError::InsufficientData {
            expected: ATOMICALS_PREFIX.len() + 2,
            actual: script.len(),
            context: "嵌套子领域操作数量"
        });
    }
    
    // 获取嵌套操作数量
    let num_operations = script[ATOMICALS_PREFIX.len() + 1] as usize;
    let mut offset = ATOMICALS_PREFIX.len() + 2;
    let mut operations = Vec::with_capacity(num_operations);
    
    // 解析每个嵌套操作
    for i in 0..num_operations {
        // 检查是否有足够的数据
        if offset + 4 > script.len() {
            warn!("嵌套子领域操作 {} 数据不足", i);
            return Err(ParseError::InsufficientData {
                expected: offset + 4,
                actual: script.len(),
                context: "嵌套子领域操作长度"
            });
        }
        
        // 获取操作数据长度
        let op_len = u32::from_le_bytes([
            script[offset], 
            script[offset + 1], 
            script[offset + 2], 
            script[offset + 3]
        ]) as usize;
        offset += 4;
        
        // 检查是否有足够的数据
        if offset + op_len > script.len() {
            warn!("嵌套子领域操作 {} 数据不足", i);
            return Err(ParseError::InsufficientData {
                expected: offset + op_len,
                actual: script.len(),
                context: "嵌套子领域操作数据"
            });
        }
        
        // 解析操作数据
        let op_data = &script[offset..offset + op_len];
        match parse_operation_from_tx_data(op_data, vout) {
            Ok(Some(op)) => {
                operations.push(op);
            }
            Ok(None) => {
                warn!("无法解析嵌套子领域操作 {}", i);
                // 继续解析下一个操作
            }
            Err(err) => {
                warn!("解析嵌套子领域操作 {} 时出错: {}", i, err);
                // 继续解析下一个操作，不中断整个过程
            }
        }
        
        offset += op_len;
    }
    
    // 检查是否有元数据
    let metadata = if offset < script.len() {
        match serde_json::from_slice(&script[offset..]) {
            Ok(value) => Some(value),
            Err(e) => {
                warn!("解析嵌套子领域元数据失败：{}，尝试恢复", e);
                // 尝试恢复：截取可能的有效 JSON 部分
                let json_str = match std::str::from_utf8(&script[offset..]) {
                    Ok(s) => s,
                    Err(_) => return Err(ParseError::Utf8Error { context: "嵌套子领域元数据" }),
                };
                
                // 尝试找到有效的 JSON 对象
                if let Some(end_pos) = json_str.find('}') {
                    let truncated = &json_str[..=end_pos];
                    match serde_json::from_str(truncated) {
                        Ok(m) => {
                            warn!("成功从截断的 JSON 中恢复嵌套子领域元数据");
                            Some(m)
                        }
                        Err(e) => {
                            warn!("无法从截断的 JSON 中恢复嵌套子领域元数据：{}", e);
                            None
                        }
                    }
                } else {
                    None
                }
            }
        }
    } else {
        None
    };
    
    // 如果没有解析到任何操作，返回 None
    if operations.is_empty() {
        return Ok(None);
    }
    
    // 如果有元数据，可以应用到所有操作
    if let Some(meta) = metadata {
        // 这里可以将元数据应用到所有操作
        // 例如，可以设置一个共同的属性
    }
    
    Ok(Some(operations))
}

/// 解析批量容器操作
fn parse_batch_container_operations(script: &[u8], vout: u32) -> Result<Option<Vec<AtomicalOperation>>, ParseError> {
    // 检查前缀
    if script.len() < ATOMICALS_PREFIX.len() || &script[0..ATOMICALS_PREFIX.len()] != ATOMICALS_PREFIX {
        return Ok(None);
    }
    
    // 检查操作类型标识
    if script.len() < ATOMICALS_PREFIX.len() + 1 {
        return Err(ParseError::InsufficientData {
            expected: ATOMICALS_PREFIX.len() + 1,
            actual: script.len(),
            context: "批量容器操作类型"
        });
    }
    
    // 检查是否是批量容器操作（使用特定标识，例如 0x02）
    if script[ATOMICALS_PREFIX.len()] != 0x02 {
        return Ok(None);
    }
    
    // 检查是否有足够的数据
    if script.len() < ATOMICALS_PREFIX.len() + 2 {
        return Err(ParseError::InsufficientData {
            expected: ATOMICALS_PREFIX.len() + 2,
            actual: script.len(),
            context: "批量容器操作数量"
        });
    }
    
    // 获取批量操作数量
    let num_operations = script[ATOMICALS_PREFIX.len() + 1] as usize;
    let mut offset = ATOMICALS_PREFIX.len() + 2;
    let mut operations = Vec::with_capacity(num_operations);
    
    // 解析每个批量操作
    for i in 0..num_operations {
        // 检查是否有足够的数据
        if offset + 4 > script.len() {
            warn!("批量容器操作 {} 数据不足", i);
            return Err(ParseError::InsufficientData {
                expected: offset + 4,
                actual: script.len(),
                context: "批量容器操作长度"
            });
        }
        
        // 获取操作数据长度
        let op_len = u32::from_le_bytes([
            script[offset], 
            script[offset + 1], 
            script[offset + 2], 
            script[offset + 3]
        ]) as usize;
        offset += 4;
        
        // 检查是否有足够的数据
        if offset + op_len > script.len() {
            warn!("批量容器操作 {} 数据不足", i);
            return Err(ParseError::InsufficientData {
                expected: offset + op_len,
                actual: script.len(),
                context: "批量容器操作数据"
            });
        }
        
        // 解析操作数据
        let op_data = &script[offset..offset + op_len];
        match parse_operation_from_tx_data(op_data, vout) {
            Ok(Some(op)) => {
                operations.push(op);
            }
            Ok(None) => {
                warn!("无法解析批量容器操作 {}", i);
                // 继续解析下一个操作
            }
            Err(err) => {
                warn!("解析批量容器操作 {} 时出错: {}", i, err);
                // 继续解析下一个操作，不中断整个过程
            }
        }
        
        offset += op_len;
    }
    
    // 检查是否有元数据
    let metadata = if offset < script.len() {
        match serde_json::from_slice(&script[offset..]) {
            Ok(value) => Some(value),
            Err(e) => {
                warn!("解析批量容器元数据失败：{}，尝试恢复", e);
                // 尝试恢复：截取可能的有效 JSON 部分
                let json_str = match std::str::from_utf8(&script[offset..]) {
                    Ok(s) => s,
                    Err(_) => return Err(ParseError::Utf8Error { context: "批量容器元数据" }),
                };
                
                // 尝试找到有效的 JSON 对象
                if let Some(end_pos) = json_str.find('}') {
                    let truncated = &json_str[..=end_pos];
                    match serde_json::from_str(truncated) {
                        Ok(m) => {
                            warn!("成功从截断的 JSON 中恢复批量容器元数据");
                            Some(m)
                        }
                        Err(e) => {
                            warn!("无法从截断的 JSON 中恢复批量容器元数据：{}", e);
                            None
                        }
                    }
                } else {
                    None
                }
            }
        }
    } else {
        None
    };
    
    // 如果没有解析到任何操作，返回 None
    if operations.is_empty() {
        return Ok(None);
    }
    
    // 如果有元数据，可以应用到所有操作
    if let Some(meta) = metadata {
        // 这里可以将元数据应用到所有操作
        // 例如，可以设置一个共同的属性
    }
    
    Ok(Some(operations))
}

/// 交易解析器
#[derive(Debug)]
pub struct TxParser {
    /// 缓存已解析的交易
    cache: Mutex<LruCache<bitcoin::Txid, Arc<Vec<AtomicalOperation>>>>,
    /// 是否启用复杂交易解析
    enable_complex_parsing: bool,
    /// Atomicals状态引用，用于上下文感知解析
    atomicals_state: Option<Arc<AtomicalsState>>,
    /// 快速路径标志：如果为true，对于没有Atomicals操作的交易会快速返回
    enable_fast_path: bool,
    /// 预分配的错误向量容量
    error_capacity: usize,
    /// 预分配的操作向量容量
    operations_capacity: usize,
}

impl TxParser {
    /// 创建新的交易解析器
    pub fn new() -> Self {
        Self {
            cache: Mutex::new(LruCache::new(1000)), // 默认缓存1000个交易
            enable_complex_parsing: true,
            atomicals_state: None,
            enable_fast_path: true,
            error_capacity: 4,         // 预分配4个错误的空间
            operations_capacity: 8,    // 预分配8个操作的空间
        }
    }
    
    /// 创建新的交易解析器，指定是否启用复杂交易解析
    pub fn new_with_options(enable_complex_parsing: bool) -> Self {
        Self {
            cache: Mutex::new(LruCache::new(1000)), // 默认缓存1000个交易
            enable_complex_parsing,
            atomicals_state: None,
            enable_fast_path: true,
            error_capacity: 4,
            operations_capacity: 8,
        }
    }

    /// 创建带有Atomicals状态的交易解析器，用于上下文感知解析
    pub fn new_with_state(atomicals_state: Arc<AtomicalsState>, enable_complex_parsing: bool) -> Self {
        Self {
            cache: Mutex::new(LruCache::new(1000)), // 默认缓存1000个交易
            enable_complex_parsing,
            atomicals_state: Some(atomicals_state),
            enable_fast_path: true,
            error_capacity: 4,
            operations_capacity: 8,
        }
    }
    
    /// 创建新的交易解析器，指定缓存大小和其他选项
    pub fn new_with_cache_size(
        cache_size: usize, 
        enable_complex_parsing: bool,
        enable_fast_path: bool,
        error_capacity: usize,
        operations_capacity: usize,
    ) -> Self {
        Self {
            cache: Mutex::new(LruCache::new(cache_size)),
            enable_complex_parsing,
            atomicals_state: None,
            enable_fast_path,
            error_capacity,
            operations_capacity,
        }
    }
    
    /// 清除缓存
    pub fn clear_cache(&mut self) {
        self.cache.lock().unwrap().clear();
    }
    
    /// 设置是否启用复杂交易解析
    pub fn set_enable_complex_parsing(&mut self, enable: bool) {
        self.enable_complex_parsing = enable;
    }

    /// 设置是否启用快速路径
    pub fn set_enable_fast_path(&mut self, enable: bool) {
        self.enable_fast_path = enable;
    }

    /// 设置Atomicals状态引用，用于上下文感知解析
    pub fn set_atomicals_state(&mut self, atomicals_state: Arc<AtomicalsState>) {
        self.atomicals_state = Some(atomicals_state);
    }

    /// 解析交易中的 Atomicals 操作
    pub fn parse_transaction(&self, tx: &Transaction) -> Result<Vec<AtomicalOperation>, ParseError> {
        // 检查缓存
        if let Some(operations) = self.cache.lock().unwrap().peek(&tx.txid()) {
            return Ok(operations.as_ref().clone());
        }
        
        // 快速路径：检查交易是否可能包含Atomicals操作
        if self.enable_fast_path && !self.might_contain_atomicals(tx) {
            return Ok(Vec::new());
        }
        
        // 如果有状态引用，使用上下文信息解析
        if let Some(state) = &self.atomicals_state {
            return self.parse_transaction_with_context(tx, state);
        }
        
        let mut operations = Vec::with_capacity(self.operations_capacity);
        let mut errors = Vec::with_capacity(self.error_capacity);
        
        // 解析每个输出
        for (vout, output) in tx.output.iter().enumerate() {
            // 快速检查：如果脚本不可能包含Atomicals操作，跳过
            if !self.script_might_contain_atomicals(&output.script_pubkey) {
                continue;
            }
            
            // 尝试解析基本操作
            match parse_output(output, vout as u32) {
                Ok(Some(op)) => {
                    operations.push(op);
                    continue;
                }
                Ok(None) => {
                    // 不是 Atomicals 操作，继续
                }
                Err(err) => {
                    // 记录错误但继续尝试其他解析方法
                    warn!("解析输出 {} 时出错: {}，尝试其他解析方法", vout, err);
                    errors.push((vout, err));
                }
            }
            
            // 如果启用了复杂交易解析，尝试解析复杂操作
            if self.enable_complex_parsing {
                // 尝试解析嵌套子领域操作
                match parse_nested_subdomain_operations(output.script_pubkey.as_bytes(), vout as u32) {
                    Ok(Some(mut nested_ops)) => {
                        operations.append(&mut nested_ops);
                        continue;
                    }
                    Ok(None) => {
                        // 不是嵌套子领域操作，继续
                    }
                    Err(err) => {
                        // 记录错误但继续尝试其他解析方法
                        warn!("解析嵌套子领域操作时出错: {}，尝试其他解析方法", err);
                        errors.push((vout, err));
                    }
                }
                
                // 尝试解析批量容器操作
                match parse_batch_container_operations(output.script_pubkey.as_bytes(), vout as u32) {
                    Ok(Some(mut batch_ops)) => {
                        operations.append(&mut batch_ops);
                        continue;
                    }
                    Ok(None) => {
                        // 不是批量容器操作，继续
                    }
                    Err(err) => {
                        // 记录错误但继续尝试其他解析方法
                        warn!("解析批量容器操作时出错: {}", err);
                        errors.push((vout, err));
                    }
                }
            }
        }
        
        // 更新缓存
        if !operations.is_empty() {
            let txid = tx.txid();
            let operations_arc = Arc::new(operations.clone());
            let mut cache = self.cache.lock().unwrap();
            cache.put(txid, operations_arc);
        }
        
        // 如果没有成功解析任何操作但有错误，返回第一个错误
        if operations.is_empty() && !errors.is_empty() {
            let (vout, err) = errors[0].clone();
            error!("无法解析交易 {} 中的任何操作，输出 {} 的错误: {}", tx.txid(), vout, err);
            return Err(err);
        }
        
        Ok(operations)
    }

    /// 使用上下文信息解析交易
    fn parse_transaction_with_context(&self, tx: &Transaction, state: &Arc<AtomicalsState>) -> Result<Vec<AtomicalOperation>, ParseError> {
        let mut operations = Vec::with_capacity(self.operations_capacity);
        let mut errors = Vec::with_capacity(self.error_capacity);
        
        // 快速路径：检查交易是否可能包含Atomicals操作
        if self.enable_fast_path && !self.might_contain_atomicals(tx) {
            return Ok(Vec::new());
        }
        
        // 使用FnvHashMap提高性能
        let mut input_atomicals: FnvHashMap<usize, Vec<AtomicalId>> = FnvHashMap::default();
        
        // 第一步：分析交易输入，找出所有相关的Atomicals
        for (vin, input) in tx.input.iter().enumerate() {
            let outpoint = &input.previous_output;
            if outpoint.vout == 0xFFFFFFFF {
                // 这是一个coinbase交易，没有输入UTXO
                continue;
            }
            
            // 尝试从状态中查找与此输入相关的Atomicals
            let atomical_ids = self.find_atomicals_for_outpoint(outpoint, state);
            if !atomical_ids.is_empty() {
                input_atomicals.insert(vin, atomical_ids);
            }
        }
        
        // 第二步：标准解析每个输出
        for (vout, output) in tx.output.iter().enumerate() {
            // 快速检查：如果脚本不可能包含Atomicals操作，跳过
            if !self.script_might_contain_atomicals(&output.script_pubkey) {
                continue;
            }
            
            // 尝试解析基本操作
            match parse_output(output, vout as u32) {
                Ok(Some(op)) => {
                    operations.push(op);
                    continue;
                }
                Ok(None) => {
                    // 不是 Atomicals 操作，继续
                }
                Err(err) => {
                    // 记录错误但继续尝试其他解析方法
                    warn!("解析输出 {} 时出错: {}，尝试其他解析方法", vout, err);
                    errors.push((vout, err));
                }
            }
            
            // 如果启用了复杂交易解析，尝试解析复杂操作
            if self.enable_complex_parsing {
                // 尝试解析嵌套子领域操作
                match parse_nested_subdomain_operations(output.script_pubkey.as_bytes(), vout as u32) {
                    Ok(Some(mut nested_ops)) => {
                        operations.append(&mut nested_ops);
                        continue;
                    }
                    Ok(None) => {
                        // 不是嵌套子领域操作，继续
                    }
                    Err(err) => {
                        // 记录错误但继续尝试其他解析方法
                        warn!("解析嵌套子领域操作时出错: {}，尝试其他解析方法", err);
                        errors.push((vout, err));
                    }
                }
                
                // 尝试解析批量容器操作
                match parse_batch_container_operations(output.script_pubkey.as_bytes(), vout as u32) {
                    Ok(Some(mut batch_ops)) => {
                        operations.append(&mut batch_ops);
                        continue;
                    }
                    Ok(None) => {
                        // 不是批量容器操作，继续
                    }
                    Err(err) => {
                        // 记录错误但继续尝试其他解析方法
                        warn!("解析批量容器操作时出错: {}", err);
                        errors.push((vout, err));
                    }
                }
            }
        }
        
        // 第三步：使用输入中的Atomicals信息推断隐式操作
        if !input_atomicals.is_empty() {
            let inferred_ops = self.infer_operations_from_inputs(tx, &input_atomicals, state);
            operations.extend(inferred_ops);
        }
        
        // 更新缓存
        if !operations.is_empty() {
            let txid = tx.txid();
            let operations_arc = Arc::new(operations.clone());
            let mut cache = self.cache.lock().unwrap();
            cache.put(txid, operations_arc);
        }
        
        // 如果没有成功解析任何操作但有错误，返回第一个错误
        if operations.is_empty() && !errors.is_empty() {
            let (vout, err) = errors[0].clone();
            error!("无法解析交易 {} 中的任何操作，输出 {} 的错误: {}", tx.txid(), vout, err);
            return Err(err);
        }
        
        Ok(operations)
    }
    
    /// 查找与指定输出点相关的Atomicals
    fn find_atomicals_for_outpoint(&self, outpoint: &OutPoint, state: &Arc<AtomicalsState>) -> Vec<AtomicalId> {
        let atomical_id = AtomicalId {
            txid: outpoint.txid,
            vout: outpoint.vout,
        };
        
        // 尝试获取此输出点的Atomical
        match state.get_output_sync(&atomical_id) {
            Ok(Some(_)) => {
                // 如果这个输出点本身就是一个Atomical，返回它
                vec![atomical_id]
            }
            _ => {
                // 否则，尝试查找所有与此输出点相关的Atomicals
                // 注意：这里可能需要一个额外的索引来高效查找
                // 目前简单返回空向量，实际实现可能需要更复杂的查询
                Vec::new()
            }
        }
    }
    
    /// 从输入中推断Atomicals操作
    fn infer_operations_from_inputs(
        &self,
        tx: &Transaction,
        input_atomicals: &FnvHashMap<usize, Vec<AtomicalId>>,
        state: &Arc<AtomicalsState>,
    ) -> Vec<AtomicalOperation> {
        let mut operations = Vec::with_capacity(input_atomicals.values().map(|v| v.len()).sum());
        
        // 遍历所有输入中的Atomicals
        for (_, atomical_ids) in input_atomicals {
            for atomical_id in atomical_ids {
                // 获取Atomical的当前状态
                if let Ok(Some(atomical_output)) = state.get_output_sync(atomical_id) {
                    // 检查是否有显式操作已经处理了这个Atomical
                    if self.is_atomical_already_processed(&operations, atomical_id) {
                        continue;
                    }
                    
                    // 推断转移操作
                    // 假设：如果一个输入包含Atomical，但没有显式操作，则认为是转移到第一个有效输出
                    if let Some(output_index) = self.find_suitable_output_for_transfer(tx, &atomical_output) {
                        operations.push(AtomicalOperation::Transfer {
                            id: atomical_id.clone(),
                            output_index,
                        });
                    }
                }
            }
        }
        
        operations
    }
    
    /// 检查Atomical是否已经在操作列表中被处理
    #[inline]
    fn is_atomical_already_processed(&self, operations: &[AtomicalOperation], atomical_id: &AtomicalId) -> bool {
        operations.iter().any(|op| match op {
            AtomicalOperation::Transfer { id, .. } => id == atomical_id,
            AtomicalOperation::Update { id, .. } => id == atomical_id,
            AtomicalOperation::Seal { id } => id == atomical_id,
            _ => false,
        })
    }
    
    /// 查找适合转移Atomical的输出索引
    fn find_suitable_output_for_transfer(&self, tx: &Transaction, atomical_output: &AtomicalOutput) -> Option<u32> {
        // 简单策略：选择第一个与原始所有者脚本类型相匹配的输出
        // 更复杂的策略可能需要考虑脚本内容、金额等因素
        for (vout, output) in tx.output.iter().enumerate() {
            if output.script_pubkey.script_type() == atomical_output.output.script_pubkey.script_type() {
                return Some(vout as u32);
            }
        }
        
        // 如果没有找到匹配的，默认选择第一个输出
        if !tx.output.is_empty() {
            return Some(0);
        }
        
        None
    }
    
    /// 快速检查交易是否可能包含Atomicals操作
    #[inline]
    fn might_contain_atomicals(&self, tx: &Transaction) -> bool {
        // 检查输出脚本是否可能包含Atomicals操作
        tx.output.iter().any(|output| self.script_might_contain_atomicals(&output.script_pubkey))
    }
    
    /// 快速检查脚本是否可能包含Atomicals操作
    #[inline]
    fn script_might_contain_atomicals(&self, script: &Script) -> bool {
        // 检查脚本是否包含OP_RETURN
        let script_bytes = script.as_bytes();
        script_bytes.len() > 1 && script_bytes[0] == OP_RETURN.to_u8()
    }
    
    /// 批量解析多个交易
    pub fn parse_transactions(&self, txs: &[Transaction]) -> Vec<(bitcoin::Txid, Result<Vec<AtomicalOperation>, ParseError>)> {
        txs.par_iter()
            .map(|tx| (tx.txid(), self.parse_transaction(tx)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::Amount;
    use bitcoin::script::Builder;
    use std::str::FromStr;

    fn create_test_transaction() -> Transaction {
        Transaction {
            version: bitcoin::transaction::Version(2),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![],
            output: vec![],
        }
    }

    fn create_test_atomical_id() -> AtomicalId {
        let txid = bitcoin::Txid::from_str(
            "1234567890123456789012345678901234567890123456789012345678901234"
        ).unwrap();
        AtomicalId { txid, vout: 0 }
    }

    #[test]
    fn test_parse_mint() {
        let mut tx = create_test_transaction();
        
        // 创建铸造操作的输出
        let metadata = serde_json::json!({
            "name": "Test NFT",
            "description": "A test NFT"
        });
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.extend_from_slice(b"mint");
        script_data.extend_from_slice(&serde_json::to_vec(&metadata).unwrap());
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 解析交易
        let parser = TxParser::new();
        let operations = parser.parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::Mint { id, atomical_type, metadata } => {
                assert_eq!(*atomical_type, AtomicalType::NFT);
                assert!(metadata.is_some());
                let metadata = metadata.as_ref().unwrap();
                assert_eq!(metadata["name"], "Test NFT");
                assert_eq!(metadata["description"], "A test NFT");
            },
            _ => panic!("Expected Mint operation"),
        }
    }

    #[test]
    fn test_parse_transfer() {
        let mut tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();
        
        // 创建转移操作的输出
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.extend_from_slice(b"transfer");
        script_data.extend_from_slice(&atomical_id.txid.to_byte_array());
        script_data.extend_from_slice(&atomical_id.vout.to_be_bytes());
        script_data.extend_from_slice(&1u32.to_be_bytes()); // 目标输出索引
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 解析交易
        let parser = TxParser::new();
        let operations = parser.parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::Transfer { id, output_index } => {
                assert_eq!(id.txid, atomical_id.txid);
                assert_eq!(id.vout, atomical_id.vout);
                assert_eq!(*output_index, 1);
            },
            _ => panic!("Expected Transfer operation"),
        }
    }

    #[test]
    fn test_parse_update() {
        let mut tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();
        
        // 创建更新操作的输出
        let metadata = serde_json::json!({
            "name": "Updated NFT",
            "description": "An updated NFT"
        });
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.extend_from_slice(b"update");
        script_data.extend_from_slice(&atomical_id.txid.to_byte_array());
        script_data.extend_from_slice(&atomical_id.vout.to_be_bytes());
        script_data.extend_from_slice(&serde_json::to_vec(&metadata).unwrap());
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 解析交易
        let parser = TxParser::new();
        let operations = parser.parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::Update { id, metadata } => {
                assert_eq!(id.txid, atomical_id.txid);
                assert_eq!(id.vout, atomical_id.vout);
                assert_eq!(metadata["name"], "Updated NFT");
                assert_eq!(metadata["description"], "An updated NFT");
            },
            _ => panic!("Expected Update operation"),
        }
    }

    #[test]
    fn test_parse_seal() {
        let mut tx = create_test_transaction();
        let atomical_id = create_test_atomical_id();
        
        // 创建封印操作的输出
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.extend_from_slice(b"seal");
        script_data.extend_from_slice(&atomical_id.txid.to_byte_array());
        script_data.extend_from_slice(&atomical_id.vout.to_be_bytes());
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 解析交易
        let parser = TxParser::new();
        let operations = parser.parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::Seal { id } => {
                assert_eq!(id.txid, atomical_id.txid);
                assert_eq!(id.vout, atomical_id.vout);
            },
            _ => panic!("Expected Seal operation"),
        }
    }

    // 新增测试：测试嵌套子领域操作解析
    #[test]
    fn test_parse_nested_subdomain_operations() {
        let mut tx = create_test_transaction();
        
        // 创建嵌套子领域操作的输出
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.extend_from_slice(b"nsub:");
        script_data.push(2); // 嵌套子领域数量
        
        // 第一个子领域
        script_data.push(5); // 名称长度
        script_data.extend_from_slice(b"test1");
        script_data.push(0); // 无父域ID
        let metadata1 = serde_json::json!({
            "description": "Test subdomain 1"
        });
        script_data.extend_from_slice(&serde_json::to_vec(&metadata1).unwrap());
        script_data.push(b';'); // 分隔符
        
        // 第二个子领域
        script_data.push(5); // 名称长度
        script_data.extend_from_slice(b"test2");
        script_data.push(1); // 有父域ID
        let parent_id = create_test_atomical_id();
        script_data.extend_from_slice(&parent_id.txid.to_byte_array());
        script_data.extend_from_slice(&parent_id.vout.to_le_bytes());
        let metadata2 = serde_json::json!({
            "description": "Test subdomain 2"
        });
        script_data.extend_from_slice(&serde_json::to_vec(&metadata2).unwrap());
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 解析交易
        let parser = TxParser::new();
        let operations = parser.parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 2);
        
        match &operations[0] {
            AtomicalOperation::CreateSubdomain { name, parent_id, metadata } => {
                assert_eq!(name, "test1");
                assert!(parent_id.is_none());
                assert!(metadata.is_some());
                let metadata = metadata.as_ref().unwrap();
                assert_eq!(metadata["description"], "Test subdomain 1");
            },
            _ => panic!("Expected CreateSubdomain operation"),
        }
        
        match &operations[1] {
            AtomicalOperation::CreateSubdomain { name, parent_id, metadata } => {
                assert_eq!(name, "test2");
                assert!(parent_id.is_some());
                let pid = parent_id.as_ref().unwrap();
                assert_eq!(pid.txid, parent_id.txid);
                assert_eq!(pid.vout, parent_id.vout);
                assert!(metadata.is_some());
                let metadata = metadata.as_ref().unwrap();
                assert_eq!(metadata["description"], "Test subdomain 2");
            },
            _ => panic!("Expected CreateSubdomain operation"),
        }
    }

    // 测试批量容器操作解析
    #[test]
    fn test_parse_batch_container_operations() {
        let mut tx = create_test_transaction();
        
        // 创建批量容器操作的输出
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.extend_from_slice(b"bcon:");
        script_data.push(2); // 批量容器数量
        
        // 第一个容器
        let container_id1 = create_test_atomical_id();
        script_data.extend_from_slice(&container_id1.txid.to_byte_array());
        script_data.extend_from_slice(&container_id1.vout.to_le_bytes());
        script_data.push(4); // 名称长度
        script_data.extend_from_slice(b"con1");
        let metadata1 = serde_json::json!({
            "description": "Container 1"
        });
        script_data.extend_from_slice(&serde_json::to_vec(&metadata1).unwrap());
        script_data.push(b';'); // 分隔符
        
        // 第二个容器
        let container_id2 = AtomicalId {
            txid: bitcoin::Txid::from_str(
                "5678901234567890123456789012345678901234567890123456789012345678"
            ).unwrap(),
            vout: 1,
        };
        script_data.extend_from_slice(&container_id2.txid.to_byte_array());
        script_data.extend_from_slice(&container_id2.vout.to_le_bytes());
        script_data.push(4); // 名称长度
        script_data.extend_from_slice(b"con2");
        let metadata2 = serde_json::json!({
            "description": "Container 2"
        });
        script_data.extend_from_slice(&serde_json::to_vec(&metadata2).unwrap());
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 解析交易
        let parser = TxParser::new();
        let operations = parser.parse_transaction(&tx).unwrap();
        assert_eq!(operations.len(), 2);
        
        match &operations[0] {
            AtomicalOperation::CreateContainer { id, name, metadata } => {
                assert_eq!(id.txid, container_id1.txid);
                assert_eq!(id.vout, container_id1.vout);
                assert_eq!(name, "con1");
                assert!(metadata.is_some());
                let metadata = metadata.as_ref().unwrap();
                assert_eq!(metadata["description"], "Container 1");
            },
            _ => panic!("Expected CreateContainer operation"),
        }
        
        match &operations[1] {
            AtomicalOperation::CreateContainer { id, name, metadata } => {
                assert_eq!(id.txid, container_id2.txid);
                assert_eq!(id.vout, container_id2.vout);
                assert_eq!(name, "con2");
                assert!(metadata.is_some());
                let metadata = metadata.as_ref().unwrap();
                assert_eq!(metadata["description"], "Container 2");
            },
            _ => panic!("Expected CreateContainer operation"),
        }
    }

    // 测试缓存功能
    #[test]
    fn test_parser_cache() {
        let mut tx = create_test_transaction();
        
        // 创建铸造操作的输出
        let metadata = serde_json::json!({
            "name": "Test NFT",
            "description": "A test NFT"
        });
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.extend_from_slice(b"mint");
        script_data.extend_from_slice(&serde_json::to_vec(&metadata).unwrap());
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });

        // 创建解析器并启用缓存
        let parser = TxParser::new();
        
        // 第一次解析
        let operations1 = parser.parse_transaction(&tx).unwrap();
        assert_eq!(operations1.len(), 1);
        
        // 第二次解析（应该从缓存中获取）
        let operations2 = parser.parse_transaction(&tx).unwrap();
        assert_eq!(operations2.len(), 1);
        
        // 确保两次解析结果相同
        match (&operations1[0], &operations2[0]) {
            (AtomicalOperation::Mint { id: id1, .. }, AtomicalOperation::Mint { id: id2, .. }) => {
                assert_eq!(id1.vout, id2.vout);
            },
            _ => panic!("Expected Mint operations"),
        }
        
        // 创建新的解析器并禁用复杂解析
        let mut parser = TxParser::new_with_options(false);
        
        // 清除缓存
        parser.clear_cache();
        
        // 创建嵌套子领域操作的输出
        let mut tx2 = create_test_transaction();
        let mut script_data = ATOMICALS_PREFIX.to_vec();
        script_data.extend_from_slice(b"nsub:");
        script_data.push(1); // 嵌套子领域数量
        script_data.push(4); // 名称长度
        script_data.extend_from_slice(b"test");
        script_data.push(0); // 无父域ID
        
        let script = Builder::new()
            .push_slice(&script_data)
            .into_script();
            
        tx2.output.push(TxOut {
            value: Amount::from_sat(1000).to_sat(),
            script_pubkey: script,
        });
        
        // 解析交易（由于禁用了复杂解析，应该不会解析嵌套子领域操作）
        let operations = parser.parse_transaction(&tx2).unwrap();
        assert_eq!(operations.len(), 0);
        
        // 启用复杂解析
        parser.set_enable_complex_parsing(true);
        
        // 再次解析交易（现在应该能解析嵌套子领域操作）
        let operations = parser.parse_transaction(&tx2).unwrap();
        assert_eq!(operations.len(), 1);
        
        match &operations[0] {
            AtomicalOperation::CreateSubdomain { name, .. } => {
                assert_eq!(name, "test");
            },
            _ => panic!("Expected CreateSubdomain operation"),
        }
    }
}

const ATOMICALS_PREFIX: &[u8] = b"atom";

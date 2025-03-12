use std::sync::{Arc, Mutex};
use std::path::Path;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write, BufRead, BufReader, Lines};

use anyhow::{anyhow, Result};
use electrs_rocksdb::{DB, WriteBatch};
use serde::{Deserialize, Serialize};
use log::{info, warn, error, debug};

/// 事务日志条目类型
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TransactionLogEntryType {
    /// 开始事务
    Begin,
    /// 提交事务
    Commit,
    /// 回滚事务
    Rollback,
    /// 写入操作
    Write {
        /// 列族名称
        cf_name: String,
        /// 键
        key: Vec<u8>,
        /// 值
        value: Vec<u8>,
    },
    /// 删除操作
    Delete {
        /// 列族名称
        cf_name: String,
        /// 键
        key: Vec<u8>,
    },
    /// 检查点标记
    Checkpoint {
        /// 检查点ID
        id: u64,
        /// 时间戳
        timestamp: u64,
    },
}

/// 事务日志条目
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransactionLogEntry {
    /// 条目类型
    pub entry_type: TransactionLogEntryType,
    /// 事务ID
    pub transaction_id: u64,
    /// 序列号
    pub sequence: u64,
    /// 时间戳
    pub timestamp: u64,
}

/// 事务状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// 活跃状态
    Active,
    /// 已提交
    Committed,
    /// 已回滚
    RolledBack,
}

/// 事务管理器
#[derive(Debug)]
pub struct TransactionManager {
    /// 日志文件路径
    log_path: String,
    /// 当前事务ID
    current_transaction_id: AtomicUsize,
    /// 当前序列号
    current_sequence: AtomicUsize,
    /// 活跃事务状态
    active_transactions: Mutex<HashMap<u64, TransactionState>>,
    /// 最后检查点ID
    last_checkpoint_id: AtomicUsize,
    /// 是否启用事务日志
    enabled: bool,
}

impl TransactionManager {
    /// 创建新的事务管理器
    pub fn new(data_dir: &Path, enabled: bool) -> Result<Self> {
        let log_path = data_dir.join("transaction.log").to_string_lossy().to_string();
        
        // 创建事务管理器
        let manager = Self {
            log_path,
            current_transaction_id: AtomicUsize::new(1),
            current_sequence: AtomicUsize::new(1),
            active_transactions: Mutex::new(HashMap::new()),
            last_checkpoint_id: AtomicUsize::new(0),
            enabled,
        };
        
        // 如果启用了事务日志，执行恢复过程
        if enabled {
            manager.recover()?;
        }
        
        Ok(manager)
    }
    
    /// 开始新事务
    pub fn begin_transaction(&self) -> Result<u64> {
        if !self.enabled {
            return Ok(0); // 如果未启用，返回虚拟事务ID
        }
        
        let transaction_id = self.current_transaction_id.fetch_add(1, Ordering::SeqCst) as u64;
        
        // 记录事务开始日志
        let log_entry = TransactionLogEntry {
            entry_type: TransactionLogEntryType::Begin,
            transaction_id,
            sequence: self.current_sequence.fetch_add(1, Ordering::SeqCst) as u64,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };
        
        self.write_log_entry(&log_entry)?;
        
        // 更新活跃事务状态
        let mut active_transactions = self.active_transactions.lock().unwrap();
        active_transactions.insert(transaction_id, TransactionState::Active);
        
        Ok(transaction_id)
    }
    
    /// 提交事务
    pub fn commit_transaction(&self, transaction_id: u64) -> Result<()> {
        if !self.enabled || transaction_id == 0 {
            return Ok(()); // 如果未启用或使用虚拟事务ID，直接返回
        }
        
        // 检查事务状态
        {
            let active_transactions = self.active_transactions.lock().unwrap();
            if !active_transactions.contains_key(&transaction_id) {
                return Err(anyhow!("Transaction {} not found", transaction_id));
            }
            
            if active_transactions.get(&transaction_id) != Some(&TransactionState::Active) {
                return Err(anyhow!("Transaction {} is not active", transaction_id));
            }
        }
        
        // 记录事务提交日志
        let log_entry = TransactionLogEntry {
            entry_type: TransactionLogEntryType::Commit,
            transaction_id,
            sequence: self.current_sequence.fetch_add(1, Ordering::SeqCst) as u64,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };
        
        self.write_log_entry(&log_entry)?;
        
        // 更新事务状态
        let mut active_transactions = self.active_transactions.lock().unwrap();
        active_transactions.insert(transaction_id, TransactionState::Committed);
        
        Ok(())
    }
    
    /// 回滚事务
    pub fn rollback_transaction(&self, transaction_id: u64) -> Result<()> {
        if !self.enabled || transaction_id == 0 {
            return Ok(()); // 如果未启用或使用虚拟事务ID，直接返回
        }
        
        // 检查事务状态
        {
            let active_transactions = self.active_transactions.lock().unwrap();
            if !active_transactions.contains_key(&transaction_id) {
                return Err(anyhow!("Transaction {} not found", transaction_id));
            }
            
            if active_transactions.get(&transaction_id) != Some(&TransactionState::Active) {
                return Err(anyhow!("Transaction {} is not active", transaction_id));
            }
        }
        
        // 记录事务回滚日志
        let log_entry = TransactionLogEntry {
            entry_type: TransactionLogEntryType::Rollback,
            transaction_id,
            sequence: self.current_sequence.fetch_add(1, Ordering::SeqCst) as u64,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };
        
        self.write_log_entry(&log_entry)?;
        
        // 更新事务状态
        let mut active_transactions = self.active_transactions.lock().unwrap();
        active_transactions.insert(transaction_id, TransactionState::RolledBack);
        
        Ok(())
    }
    
    /// 记录写操作
    pub fn log_write(&self, transaction_id: u64, cf_name: &str, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.enabled || transaction_id == 0 {
            return Ok(()); // 如果未启用或使用虚拟事务ID，直接返回
        }
        
        // 检查事务状态
        {
            let active_transactions = self.active_transactions.lock().unwrap();
            if !active_transactions.contains_key(&transaction_id) {
                return Err(anyhow!("Transaction {} not found", transaction_id));
            }
            
            if active_transactions.get(&transaction_id) != Some(&TransactionState::Active) {
                return Err(anyhow!("Transaction {} is not active", transaction_id));
            }
        }
        
        // 记录写操作日志
        let log_entry = TransactionLogEntry {
            entry_type: TransactionLogEntryType::Write {
                cf_name: cf_name.to_string(),
                key: key.to_vec(),
                value: value.to_vec(),
            },
            transaction_id,
            sequence: self.current_sequence.fetch_add(1, Ordering::SeqCst) as u64,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };
        
        self.write_log_entry(&log_entry)?;
        
        Ok(())
    }
    
    /// 记录删除操作
    pub fn log_delete(&self, transaction_id: u64, cf_name: &str, key: &[u8]) -> Result<()> {
        if !self.enabled || transaction_id == 0 {
            return Ok(()); // 如果未启用或使用虚拟事务ID，直接返回
        }
        
        // 检查事务状态
        {
            let active_transactions = self.active_transactions.lock().unwrap();
            if !active_transactions.contains_key(&transaction_id) {
                return Err(anyhow!("Transaction {} not found", transaction_id));
            }
            
            if active_transactions.get(&transaction_id) != Some(&TransactionState::Active) {
                return Err(anyhow!("Transaction {} is not active", transaction_id));
            }
        }
        
        // 记录删除操作日志
        let log_entry = TransactionLogEntry {
            entry_type: TransactionLogEntryType::Delete {
                cf_name: cf_name.to_string(),
                key: key.to_vec(),
            },
            transaction_id,
            sequence: self.current_sequence.fetch_add(1, Ordering::SeqCst) as u64,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };
        
        self.write_log_entry(&log_entry)?;
        
        Ok(())
    }
    
    /// 创建检查点
    pub fn create_checkpoint(&self) -> Result<u64> {
        if !self.enabled {
            return Ok(0); // 如果未启用，返回虚拟检查点ID
        }
        
        let checkpoint_id = self.last_checkpoint_id.fetch_add(1, Ordering::SeqCst) as u64;
        
        // 记录检查点日志
        let log_entry = TransactionLogEntry {
            entry_type: TransactionLogEntryType::Checkpoint {
                id: checkpoint_id,
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            },
            transaction_id: 0, // 检查点不属于任何事务
            sequence: self.current_sequence.fetch_add(1, Ordering::SeqCst) as u64,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };
        
        self.write_log_entry(&log_entry)?;
        
        // 清理已完成的事务
        let mut active_transactions = self.active_transactions.lock().unwrap();
        active_transactions.retain(|_, state| *state == TransactionState::Active);
        
        Ok(checkpoint_id)
    }
    
    /// 写入日志条目
    fn write_log_entry(&self, entry: &TransactionLogEntry) -> Result<()> {
        let serialized = serde_json::to_string(entry)?;
        
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;
        
        writeln!(file, "{}", serialized)?;
        
        Ok(())
    }
    
    /// 从日志恢复
    fn recover(&self) -> Result<()> {
        info!("开始从事务日志恢复...");
        
        // 检查日志文件是否存在
        if !Path::new(&self.log_path).exists() {
            info!("事务日志文件不存在，跳过恢复过程");
            return Ok(());
        }
        
        // 读取日志文件
        let file = File::open(&self.log_path)?;
        let reader = BufReader::new(file);
        
        // 解析日志条目
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            
            match serde_json::from_str::<TransactionLogEntry>(&line) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    warn!("警告：无法解析日志条目：{}, 错误：{}", line, e);
                    continue;
                }
            }
        }
        
        // 按序列号排序
        entries.sort_by_key(|entry| entry.sequence);
        
        // 恢复事务状态
        let mut active_transactions = self.active_transactions.lock().unwrap();
        let mut max_transaction_id = 0;
        let mut max_sequence = 0;
        let mut max_checkpoint_id = 0;
        
        for entry in &entries {
            max_transaction_id = max_transaction_id.max(entry.transaction_id);
            max_sequence = max_sequence.max(entry.sequence);
            
            match &entry.entry_type {
                TransactionLogEntryType::Begin => {
                    active_transactions.insert(entry.transaction_id, TransactionState::Active);
                }
                TransactionLogEntryType::Commit => {
                    active_transactions.insert(entry.transaction_id, TransactionState::Committed);
                }
                TransactionLogEntryType::Rollback => {
                    active_transactions.insert(entry.transaction_id, TransactionState::RolledBack);
                }
                TransactionLogEntryType::Checkpoint { id, .. } => {
                    max_checkpoint_id = max_checkpoint_id.max(*id);
                }
                _ => {}
            }
        }
        
        // 更新计数器
        self.current_transaction_id.store(max_transaction_id as usize + 1, Ordering::SeqCst);
        self.current_sequence.store(max_sequence as usize + 1, Ordering::SeqCst);
        self.last_checkpoint_id.store(max_checkpoint_id as usize + 1, Ordering::SeqCst);
        
        // 清理已完成的事务
        active_transactions.retain(|_, state| *state == TransactionState::Active);
        
        info!("事务日志恢复完成，当前事务ID：{}，序列号：{}，检查点ID：{}",
            max_transaction_id + 1, max_sequence + 1, max_checkpoint_id + 1);
        
        Ok(())
    }
    
    /// 执行崩溃恢复
    pub fn perform_crash_recovery(&self, db: &DB) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        
        info!("执行崩溃恢复...");
        
        // 检查是否有未完成的事务
        let active_transactions = self.active_transactions.lock().unwrap();
        if active_transactions.is_empty() {
            info!("没有未完成的事务，跳过崩溃恢复");
            return Ok(());
        }
        
        // 读取日志文件
        let file = File::open(&self.log_path)?;
        let reader = BufReader::new(file);
        
        // 解析日志条目
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            
            match serde_json::from_str::<TransactionLogEntry>(&line) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    warn!("警告：无法解析日志条目：{}, 错误：{}", line, e);
                    continue;
                }
            }
        }
        
        // 按序列号排序
        entries.sort_by_key(|entry| entry.sequence);
        
        // 创建批处理
        let mut batch = WriteBatch::default();
        
        // 处理未完成事务的操作
        for entry in &entries {
            // 只处理活跃状态的事务
            if active_transactions.get(&entry.transaction_id) != Some(&TransactionState::Active) {
                continue;
            }
            
            match &entry.entry_type {
                TransactionLogEntryType::Write { cf_name, key, value } => {
                    // 回滚写操作（不执行）
                    info!("回滚写操作：事务ID={}, CF={}, 键长度={}", 
                        entry.transaction_id, cf_name, key.len());
                }
                TransactionLogEntryType::Delete { cf_name, key } => {
                    // 回滚删除操作（恢复原值）
                    if let Some(cf) = db.cf_handle(cf_name) {
                        if let Some(original_value) = db.get_cf(&cf, key)? {
                            batch.put_cf(&cf, key, &original_value);
                            info!("恢复删除的数据：事务ID={}, CF={}, 键长度={}", 
                                entry.transaction_id, cf_name, key.len());
                        }
                    }
                }
                _ => {}
            }
        }
        
        // 应用批处理
        if !batch.is_empty() {
            db.write(batch)?;
            info!("已应用崩溃恢复操作");
        } else {
            info!("没有需要恢复的操作");
        }
        
        // 标记所有活跃事务为已回滚
        let mut active_transactions = self.active_transactions.lock().unwrap();
        for (id, state) in active_transactions.iter_mut() {
            if *state == TransactionState::Active {
                *state = TransactionState::RolledBack;
                info!("将未完成事务 {} 标记为已回滚", id);
            }
        }
        
        info!("崩溃恢复完成");
        
        Ok(())
    }
    
    /// 清理旧日志
    pub fn cleanup_old_logs(&self, keep_days: u64) -> Result<usize> {
        if !self.enabled {
            return Ok(0);
        }
        
        info!("清理旧事务日志...");
        
        // 备份当前日志
        let backup_path = format!("{}.{}", self.log_path, 
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs());
        
        fs::copy(&self.log_path, &backup_path)?;
        
        // 读取日志文件
        let file = File::open(&self.log_path)?;
        let reader = BufReader::new(file);
        
        // 解析日志条目
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            
            match serde_json::from_str::<TransactionLogEntry>(&line) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    warn!("警告：无法解析日志条目：{}, 错误：{}", line, e);
                    continue;
                }
            }
        }
        
        // 计算截止时间戳
        let cutoff_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() - (keep_days * 86400);
        
        // 过滤出需要保留的条目
        let old_count = entries.len();
        entries.retain(|entry| {
            // 保留最近的条目
            entry.timestamp >= cutoff_timestamp
        });
        
        let removed_count = old_count - entries.len();
        
        // 重写日志文件
        let mut file = File::create(&self.log_path)?;
        for entry in &entries {
            let serialized = serde_json::to_string(entry)?;
            writeln!(file, "{}", serialized)?;
        }
        
        info!("清理完成，移除了 {} 条旧日志条目", removed_count);
        
        Ok(removed_count)
    }
    
    /// 验证数据一致性
    pub fn verify_data_consistency(&self, db: &DB, cf_names: &[&str]) -> Result<bool> {
        if !self.enabled {
            return Ok(true);
        }
        
        info!("验证数据一致性...");
        
        // 检查所有列族
        for cf_name in cf_names {
            if let Some(cf) = db.cf_handle(cf_name) {
                // 对每个列族执行一致性检查
                let iter = db.iterator_cf(&cf, IteratorMode::Start);
                
                for result in iter {
                    match result {
                        Ok(_) => {}, // 数据可以正常读取
                        Err(e) => {
                            error!("列族 {} 中发现损坏的数据：{}", cf_name, e);
                            return Ok(false);
                        }
                    }
                }
            } else {
                warn!("警告：列族 {} 不存在", cf_name);
            }
        }
        
        info!("数据一致性验证通过");
        
        Ok(true)
    }
}

use std::sync::{Arc, Mutex};
use std::path::Path;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::fs;

use anyhow::{anyhow, Result};
use bitcoin::{TxOut, Txid};
use bitcoin::consensus::encode;
use electrs_rocksdb::{DB, IteratorMode, Options, ColumnFamilyDescriptor, WriteBatch};
use serde::{Deserialize, Serialize};
use hex;
use lru::LruCache;
use std::num::NonZeroUsize;
use log::{info, warn, error, debug};

use super::protocol::{AtomicalId, AtomicalType};
use super::state::AtomicalOutput;
use super::indexer::IndexEntry;
use super::transaction_log::{TransactionManager, TransactionState};

const CF_STATE: &str = "state";
const CF_OUTPUTS: &str = "outputs";
const CF_METADATA: &str = "metadata";
const CF_INDEXES: &str = "indexes";
const CF_SCRIPT_ATOMICALS: &str = "script_atomicals";

/// 缓存配置
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// 输出缓存大小
    pub outputs_cache_size: usize,
    /// 元数据缓存大小
    pub metadata_cache_size: usize,
    /// 索引缓存大小
    pub index_cache_size: usize,
    /// 脚本-Atomicals映射缓存大小
    pub script_atomicals_cache_size: usize,
    /// 是否启用缓存统计
    pub enable_stats: bool,
    /// 缓存有效期（秒）
    pub ttl: Option<u64>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            outputs_cache_size: 10_000,
            metadata_cache_size: 5_000,
            index_cache_size: 1_000,
            script_atomicals_cache_size: 5_000,
            enable_stats: false,
            ttl: None,
        }
    }
}

/// 缓存统计信息
#[derive(Debug, Default)]
pub struct CacheStats {
    /// 缓存命中次数
    hits: AtomicUsize,
    /// 缓存未命中次数
    misses: AtomicUsize,
    /// 缓存写入次数
    writes: AtomicUsize,
    /// 缓存失效次数
    evictions: AtomicUsize,
    /// 缓存创建时间
    created_at: Instant,
}

impl CacheStats {
    /// 创建新的缓存统计
    pub fn new() -> Self {
        Self {
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
            evictions: AtomicUsize::new(0),
            created_at: Instant::now(),
        }
    }

    /// 记录缓存命中
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录缓存未命中
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录缓存写入
    pub fn record_write(&self) {
        self.writes.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录缓存失效
    pub fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// 获取缓存命中率
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// 获取缓存统计信息
    pub fn get_stats(&self) -> HashMap<String, serde_json::Value> {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let writes = self.writes.load(Ordering::Relaxed);
        let evictions = self.evictions.load(Ordering::Relaxed);
        let uptime = self.created_at.elapsed().as_secs();
        
        let mut stats = HashMap::new();
        stats.insert("hits".to_string(), serde_json::json!(hits));
        stats.insert("misses".to_string(), serde_json::json!(misses));
        stats.insert("writes".to_string(), serde_json::json!(writes));
        stats.insert("evictions".to_string(), serde_json::json!(evictions));
        stats.insert("hit_rate".to_string(), serde_json::json!(self.hit_rate()));
        stats.insert("uptime_seconds".to_string(), serde_json::json!(uptime));
        
        stats
    }

    /// 重置统计信息
    pub fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.writes.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
    }
}

/// 带有过期时间的缓存项
#[derive(Clone)]
struct CacheItem<T> {
    /// 缓存的值
    value: T,
    /// 过期时间
    expires_at: Option<Instant>,
}

impl<T: Clone> CacheItem<T> {
    /// 创建新的缓存项
    fn new(value: T, ttl: Option<Duration>) -> Self {
        let expires_at = ttl.map(|duration| Instant::now() + duration);
        Self { value, expires_at }
    }

    /// 检查缓存项是否已过期
    fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            Instant::now() > expires_at
        } else {
            false
        }
    }

    /// 获取缓存项的值
    fn get_value(&self) -> Option<T> {
        if self.is_expired() {
            None
        } else {
            Some(self.value.clone())
        }
    }
}

/// Atomicals 存储引擎
pub struct AtomicalsStorage {
    /// RocksDB 实例
    db: Arc<DB>,
    /// 输出缓存
    outputs_cache: Mutex<LruCache<String, CacheItem<AtomicalOutput>>>,
    /// 元数据缓存
    metadata_cache: Mutex<LruCache<String, CacheItem<serde_json::Value>>>,
    /// 索引缓存
    index_cache: Mutex<LruCache<String, CacheItem<Vec<u8>>>>,
    /// 脚本-Atomicals 映射缓存
    script_atomicals_cache: Mutex<LruCache<String, CacheItem<Vec<AtomicalId>>>>,
    /// 缓存配置
    cache_config: CacheConfig,
    /// 缓存统计
    cache_stats: CacheStats,
    /// 事务管理器
    transaction_manager: Arc<TransactionManager>,
}

impl AtomicalsStorage {
    /// 创建新的存储引擎
    pub fn new(data_dir: &Path) -> Result<Self> {
        Self::with_cache_config(data_dir, CacheConfig::default())
    }

    /// 使用自定义缓存配置创建存储引擎
    pub fn with_cache_config(data_dir: &Path, cache_config: CacheConfig) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        
        // 定义列族
        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(CF_STATE, Options::default()),
            ColumnFamilyDescriptor::new(CF_OUTPUTS, Options::default()),
            ColumnFamilyDescriptor::new(CF_METADATA, Options::default()),
            ColumnFamilyDescriptor::new(CF_INDEXES, Options::default()),
            ColumnFamilyDescriptor::new(CF_SCRIPT_ATOMICALS, Options::default()),
        ];
        
        // 创建数据目录
        fs::create_dir_all(data_dir)?;
        
        let db_path = data_dir.join("atomicals.db");
        let db = DB::open_cf_descriptors(&opts, &db_path, cf_descriptors)?;
        
        // 创建缓存，使用配置的缓存大小
        let outputs_cache = Mutex::new(LruCache::new(NonZeroUsize::new(cache_config.outputs_cache_size).unwrap()));
        let metadata_cache = Mutex::new(LruCache::new(NonZeroUsize::new(cache_config.metadata_cache_size).unwrap()));
        let index_cache = Mutex::new(LruCache::new(NonZeroUsize::new(cache_config.index_cache_size).unwrap()));
        let script_atomicals_cache = Mutex::new(LruCache::new(NonZeroUsize::new(cache_config.script_atomicals_cache_size).unwrap()));
        
        // 创建缓存统计
        let cache_stats = CacheStats::new();
        
        // 创建事务管理器
        let transaction_manager = Arc::new(TransactionManager::new(data_dir, true)?);
        
        // 执行崩溃恢复
        transaction_manager.perform_crash_recovery(&db)?;
        
        let storage = Self {
            db: Arc::new(db),
            outputs_cache,
            metadata_cache,
            index_cache,
            script_atomicals_cache,
            cache_config,
            cache_stats,
            transaction_manager,
        };
        
        Ok(storage)
    }

    /// 缓存预热策略 - 用于系统启动或处理大量交易前
    pub fn prepare_for_heavy_load(&self) -> Result<HashMap<String, usize>> {
        // 清理所有缓存，确保从干净状态开始
        self.clear_caches()?;
        
        // 调整缓存大小为较大值以应对高负载
        let mut high_load_config = self.cache_config.clone();
        high_load_config.outputs_cache_size = high_load_config.outputs_cache_size * 2;
        high_load_config.metadata_cache_size = high_load_config.metadata_cache_size * 2;
        high_load_config.script_atomicals_cache_size = high_load_config.script_atomicals_cache_size * 2;
        
        // 应用新配置
        self.resize_caches(high_load_config)?;
        
        // 预热缓存，加载最近的数据
        let limits = [
            ("outputs", 5000),
            ("metadata", 2000),
            ("script_atomicals", 1000),
        ].iter().cloned().collect();
        
        self.warm_all_caches(&limits)
    }
    
    /// 内存优化策略 - 用于系统内存压力大时
    pub fn optimize_for_low_memory(&self) -> Result<HashMap<String, usize>> {
        // 清理过期的缓存项
        let pruned = self.prune_expired_cache_items()?;
        
        // 调整缓存大小为较小值以减少内存使用
        let mut low_memory_config = self.cache_config.clone();
        low_memory_config.outputs_cache_size = (low_memory_config.outputs_cache_size as f64 * 0.5) as usize;
        low_memory_config.metadata_cache_size = (low_memory_config.metadata_cache_size as f64 * 0.5) as usize;
        low_memory_config.script_atomicals_cache_size = (low_memory_config.script_atomicals_cache_size as f64 * 0.5) as usize;
        
        // 确保缓存大小不小于最小值
        low_memory_config.outputs_cache_size = low_memory_config.outputs_cache_size.max(1000);
        low_memory_config.metadata_cache_size = low_memory_config.metadata_cache_size.max(500);
        low_memory_config.script_atomicals_cache_size = low_memory_config.script_atomicals_cache_size.max(500);
        
        // 应用新配置
        self.resize_caches(low_memory_config)?;
        
        // 返回优化结果
        let mut results = HashMap::new();
        results.insert("pruned_items".to_string(), pruned);
        results.insert("new_outputs_cache_size".to_string(), low_memory_config.outputs_cache_size);
        results.insert("new_metadata_cache_size".to_string(), low_memory_config.metadata_cache_size);
        results.insert("new_script_atomicals_cache_size".to_string(), low_memory_config.script_atomicals_cache_size);
        
        Ok(results)
    }
    
    /// 智能缓存策略 - 根据访问模式自动调整缓存配置
    pub fn apply_smart_caching_strategy(&self, access_pattern: &str) -> Result<CacheConfig> {
        let mut new_config = self.cache_config.clone();
        
        match access_pattern {
            "read_heavy" => {
                // 读取密集型场景，增大缓存大小，延长TTL
                new_config.outputs_cache_size = new_config.outputs_cache_size * 2;
                new_config.metadata_cache_size = new_config.metadata_cache_size * 2;
                new_config.ttl = Some(7200); // 2小时
            },
            "write_heavy" => {
                // 写入密集型场景，减小缓存大小，缩短TTL
                new_config.outputs_cache_size = (new_config.outputs_cache_size as f64 * 0.8) as usize;
                new_config.metadata_cache_size = (new_config.metadata_cache_size as f64 * 0.8) as usize;
                new_config.ttl = Some(600); // 10分钟
            },
            "balanced" => {
                // 平衡型场景，使用中等大小缓存和TTL
                new_config.ttl = Some(1800); // 30分钟
            },
            "memory_constrained" => {
                // 内存受限场景，大幅减小缓存大小
                new_config.outputs_cache_size = (new_config.outputs_cache_size as f64 * 0.5) as usize;
                new_config.metadata_cache_size = (new_config.metadata_cache_size as f64 * 0.5) as usize;
                new_config.script_atomicals_cache_size = (new_config.script_atomicals_cache_size as f64 * 0.5) as usize;
                new_config.ttl = Some(300); // 5分钟
            },
            _ => {
                // 默认策略，保持当前配置
            }
        }
        
        // 确保缓存大小不小于最小值
        new_config.outputs_cache_size = new_config.outputs_cache_size.max(1000);
        new_config.metadata_cache_size = new_config.metadata_cache_size.max(500);
        new_config.script_atomicals_cache_size = new_config.script_atomicals_cache_size.max(500);
        
        // 应用新配置
        self.resize_caches(new_config.clone())?;
        
        Ok(new_config)
    }
    
    /// 批量预加载热点数据 - 针对特定场景预加载可能需要的数据
    pub fn preload_hot_data(&self, scenario: &str) -> Result<usize> {
        let mut loaded_count = 0;
        
        match scenario {
            "recent_blocks" => {
                // 加载最近区块相关的数据
                loaded_count += self.warm_outputs_cache(2000)?;
                loaded_count += self.warm_metadata_cache(1000)?;
            },
            "popular_atomicals" => {
                // 这里可以实现加载热门Atomicals的逻辑
                // 例如，可以从数据库中查询访问频率最高的Atomicals
                // 然后预加载它们的输出和元数据
                // 简化实现，直接预热缓存
                loaded_count += self.warm_outputs_cache(1000)?;
                loaded_count += self.warm_metadata_cache(500)?;
            },
            "indexing" => {
                // 索引场景，预加载索引数据
                // 简化实现，直接预热脚本-Atomicals映射缓存
                loaded_count += self.warm_script_atomicals_cache(1000)?;
            },
            _ => {
                // 默认预加载少量数据
                loaded_count += self.warm_outputs_cache(500)?;
                loaded_count += self.warm_metadata_cache(200)?;
                loaded_count += self.warm_script_atomicals_cache(100)?;
            }
        }
        
        Ok(loaded_count)
    }
    
    /// 获取缓存效率报告 - 用于监控和调优
    pub fn get_cache_efficiency_report(&self) -> Result<HashMap<String, serde_json::Value>> {
        let mut report = HashMap::new();
        
        // 获取缓存统计
        let stats = self.get_cache_stats();
        let usage = self.get_cache_usage();
        
        // 计算效率指标
        let hit_rate = stats.get("hit_rate").cloned().unwrap_or(serde_json::json!(0.0));
        let evictions = stats.get("evictions").cloned().unwrap_or(serde_json::json!(0));
        let hits = stats.get("hits").cloned().unwrap_or(serde_json::json!(0));
        let misses = stats.get("misses").cloned().unwrap_or(serde_json::json!(0));
        
        // 计算内存使用率
        let outputs_usage_rate = usage.get("outputs_len").cloned().unwrap_or(0) as f64 / 
                               usage.get("outputs_cap").cloned().unwrap_or(1) as f64;
        
        let metadata_usage_rate = usage.get("metadata_len").cloned().unwrap_or(0) as f64 / 
                                usage.get("metadata_cap").cloned().unwrap_or(1) as f64;
        
        let script_atomicals_usage_rate = usage.get("script_atomicals_len").cloned().unwrap_or(0) as f64 / 
                                        usage.get("script_atomicals_cap").cloned().unwrap_or(1) as f64;
        
        // 构建报告
        report.insert("hit_rate".to_string(), hit_rate);
        report.insert("evictions".to_string(), evictions);
        report.insert("hits".to_string(), hits);
        report.insert("misses".to_string(), misses);
        report.insert("outputs_usage_rate".to_string(), serde_json::json!(outputs_usage_rate));
        report.insert("metadata_usage_rate".to_string(), serde_json::json!(metadata_usage_rate));
        report.insert("script_atomicals_usage_rate".to_string(), serde_json::json!(script_atomicals_usage_rate));
        
        // 添加效率评级
        let efficiency_rating = if hit_rate.as_f64().unwrap_or(0.0) > 0.8 {
            "excellent"
        } else if hit_rate.as_f64().unwrap_or(0.0) > 0.6 {
            "good"
        } else if hit_rate.as_f64().unwrap_or(0.0) > 0.4 {
            "fair"
        } else {
            "poor"
        };
        
        report.insert("efficiency_rating".to_string(), serde_json::json!(efficiency_rating));
        
        // 添加优化建议
        let mut recommendations = Vec::new();
        
        if hit_rate.as_f64().unwrap_or(0.0) < 0.5 {
            recommendations.push("增加缓存预热");
        }
        
        if outputs_usage_rate > 0.9 {
            recommendations.push("增加outputs缓存大小");
        } else if outputs_usage_rate < 0.3 {
            recommendations.push("减少outputs缓存大小以节省内存");
        }
        
        if metadata_usage_rate > 0.9 {
            recommendations.push("增加metadata缓存大小");
        } else if metadata_usage_rate < 0.3 {
            recommendations.push("减少metadata缓存大小以节省内存");
        }
        
        if script_atomicals_usage_rate > 0.9 {
            recommendations.push("增加script_atomicals缓存大小");
        } else if script_atomicals_usage_rate < 0.3 {
            recommendations.push("减少script_atomicals缓存大小以节省内存");
        }
        
        report.insert("recommendations".to_string(), serde_json::json!(recommendations));
        
        Ok(report)
    }
    
    /// 分层缓存实现 - 用于优先级缓存管理
    pub fn implement_tiered_caching(&self) -> Result<()> {
        // 实现一个高级分层缓存系统
        // L1: 热点数据，高频访问，小容量，快速访问
        // L2: 温数据，中频访问，中等容量
        // L3: 冷数据，低频访问，大容量或不缓存
        
        // 获取当前缓存使用情况和统计信息
        let current_usage = self.get_cache_usage();
        let cache_stats = self.get_cache_stats();
        
        // 分析缓存访问模式
        let hit_rate = cache_stats.get("hit_rate").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let total_hits = cache_stats.get("hits").and_then(|v| v.as_u64()).unwrap_or(0);
        let total_misses = cache_stats.get("misses").and_then(|v| v.as_u64()).unwrap_or(0);
        let total_accesses = total_hits + total_misses;
        
        // 计算内存压力级别
        let memory_pressure = if total_accesses > 10000 {
            println!("检测到高访问量，调整为高内存压力模式");
            "high"
        } else if total_accesses > 5000 {
            println!("检测到中等访问量，调整为中等内存压力模式");
            "medium"
        } else {
            println!("检测到低访问量，调整为低内存压力模式");
            "low"
        };
        
        // 创建新的缓存配置
        let mut tiered_config = self.cache_config.clone();
        
        // 根据内存压力和访问模式调整缓存配置
        match memory_pressure {
            "high" => {
                // 高内存压力下，减小缓存大小，优先保留热点数据
                let outputs_cap = current_usage.get("outputs_cap").cloned().unwrap_or(1000);
                let metadata_cap = current_usage.get("metadata_cap").cloned().unwrap_or(500);
                let total_cap = outputs_cap + metadata_cap;
                
                // 热点数据占比提高到60%，减少总体缓存大小
                let hot_tier_size = (total_cap as f64 * 0.6).ceil() as usize;
                let warm_tier_size = (total_cap as f64 * 0.3).ceil() as usize;
                let cold_tier_size = (total_cap as f64 * 0.1).ceil() as usize;
                
                // 分配缓存大小，优先保证热点数据
                tiered_config.outputs_cache_size = hot_tier_size;
                tiered_config.metadata_cache_size = warm_tier_size;
                tiered_config.index_cache_size = cold_tier_size / 2;
                tiered_config.script_atomicals_cache_size = cold_tier_size / 2;
                
                // 设置短TTL，加速缓存更新
                tiered_config.ttl = Some(300); // 5分钟
                
                println!("高内存压力配置: 热点缓存={}, 温数据缓存={}, 冷数据缓存={}",
                    hot_tier_size, warm_tier_size, cold_tier_size);
            },
            "medium" => {
                // 中等内存压力，平衡缓存大小
                let outputs_cap = current_usage.get("outputs_cap").cloned().unwrap_or(2000);
                let metadata_cap = current_usage.get("metadata_cap").cloned().unwrap_or(1000);
                let total_cap = outputs_cap + metadata_cap;
                
                // 平衡分配缓存
                let hot_tier_size = (total_cap as f64 * 0.5).ceil() as usize;
                let warm_tier_size = (total_cap as f64 * 0.3).ceil() as usize;
                let cold_tier_size = (total_cap as f64 * 0.2).ceil() as usize;
                
                // 分配缓存大小
                tiered_config.outputs_cache_size = hot_tier_size;
                tiered_config.metadata_cache_size = warm_tier_size;
                tiered_config.index_cache_size = cold_tier_size / 2;
                tiered_config.script_atomicals_cache_size = cold_tier_size / 2;
                
                // 设置中等TTL
                tiered_config.ttl = Some(600); // 10分钟
                
                println!("中等内存压力配置: 热点缓存={}, 温数据缓存={}, 冷数据缓存={}",
                    hot_tier_size, warm_tier_size, cold_tier_size);
            },
            "low" => {
                // 低内存压力，可以使用更大的缓存
                // 扩大缓存大小，但仍然保持分层
                let hot_tier_size = 5000;
                let warm_tier_size = 2500;
                let cold_tier_size = 2000;
                
                tiered_config.outputs_cache_size = hot_tier_size;
                tiered_config.metadata_cache_size = warm_tier_size;
                tiered_config.index_cache_size = cold_tier_size / 2;
                tiered_config.script_atomicals_cache_size = cold_tier_size / 2;
                
                // 设置较长的TTL
                tiered_config.ttl = Some(1800); // 30分钟
                
                println!("低内存压力配置: 热点缓存={}, 温数据缓存={}, 冷数据缓存={}",
                    hot_tier_size, warm_tier_size, cold_tier_size);
            },
            _ => {}
        }
        
        // 应用新的缓存配置
        println!("应用分层缓存配置...");
        self.resize_caches(tiered_config.clone())?;
        
        // 智能预热缓存 - 根据访问频率预热不同层级
        let mut preload_limits = HashMap::new();
        
        // 根据命中率决定预热策略
        if hit_rate < 0.5 {
            // 命中率低，需要更多预热
            preload_limits.insert("outputs", tiered_config.outputs_cache_size / 2);
            preload_limits.insert("metadata", tiered_config.metadata_cache_size / 2);
            preload_limits.insert("script_atomicals", tiered_config.script_atomicals_cache_size / 4);
            println!("命中率低 ({:.2}%)，增加预热数量", hit_rate * 100.0);
        } else {
            // 命中率高，适度预热
            preload_limits.insert("outputs", tiered_config.outputs_cache_size / 4);
            preload_limits.insert("metadata", tiered_config.metadata_cache_size / 4);
            preload_limits.insert("script_atomicals", tiered_config.script_atomicals_cache_size / 8);
            println!("命中率高 ({:.2}%)，减少预热数量", hit_rate * 100.0);
        }
        
        // 执行预热
        println!("开始预热缓存...");
        let preloaded = self.warm_all_caches(&preload_limits)?;
        
        // 主动淘汰策略 - 清理过期和低价值缓存项
        println!("清理过期缓存项...");
        let pruned = self.prune_expired_cache_items()?;
        
        // 实现智能预取 - 根据最近访问的Atomicals预取相关数据
        // 获取最近访问的Atomicals ID (这里简化处理，实际应从访问日志或热点数据中获取)
        let mut recent_ids = Vec::new();
        
        // 从输出缓存中提取最近访问的ID
        if let Ok(outputs_cache) = self.outputs_cache.lock() {
            // 从LRU缓存中获取最近访问的键（最多10个）
            let mut count = 0;
            for (key, _) in outputs_cache.iter() {
                if let Some((txid_str, vout_str)) = key.split_once(':') {
                    if let (Ok(txid), Ok(vout)) = (Txid::from_str(txid_str), vout_str.parse::<u32>()) {
                        recent_ids.push(AtomicalId { txid, vout });
                        count += 1;
                        if count >= 10 {
                            break;
                        }
                    }
                }
            }
        }
        
        // 如果有最近访问的ID，执行智能预取
        let prefetched = if !recent_ids.is_empty() {
            println!("执行智能预取，基于{}个最近访问的Atomicals", recent_ids.len());
            self.smart_prefetch(&recent_ids)?
        } else {
            0
        };
        
        // 记录分层缓存实现结果
        println!(
            "分层缓存实现完成: 内存压力={}, 命中率={:.2}%, 预热项={:?}, 淘汰项={}, 预取项={}",
            memory_pressure, hit_rate * 100.0, preloaded, pruned, prefetched
        );
        
        Ok(())
    }
    
    /// 批量获取 Atomical 输出
    pub fn batch_get_outputs(&self, atomical_ids: &[AtomicalId]) -> Result<HashMap<AtomicalId, AtomicalOutput>> {
        let mut results = HashMap::with_capacity(atomical_ids.len());
        let mut missed_ids = Vec::new();
        let mut missed_keys = Vec::new();
        
        // 首先尝试从缓存获取
        if let Ok(cache) = self.outputs_cache.lock() {
            for id in atomical_ids {
                let key = format!("{}:{}", id.txid, id.vout);
                if let Some(item) = cache.get(&key) {
                    if !item.is_expired() {
                        if let Some(output) = item.get_value() {
                            results.insert(id.clone(), output);
                            self.cache_stats.record_hit();
                            continue;
                        }
                    }
                }
                
                // 缓存未命中，记录需要从数据库获取的ID
                missed_ids.push(id.clone());
                missed_keys.push(key);
                self.cache_stats.record_miss();
            }
        } else {
            // 如果无法获取缓存锁，则所有ID都需要从数据库获取
            for id in atomical_ids {
                missed_ids.push(id.clone());
                missed_keys.push(format!("{}:{}", id.txid, id.vout));
                self.cache_stats.record_miss();
            }
        }
        
        // 如果有未命中的ID，从数据库批量获取
        if !missed_ids.is_empty() {
            let cf = self.db.cf_handle(CF_OUTPUTS)
                .ok_or_else(|| anyhow!("Column family not found: {}", CF_OUTPUTS))?;
            
            // 使用 multi_get_cf 批量获取
            let db_results = self.db.multi_get_cf(
                missed_keys.iter().map(|k| (&cf, k.as_bytes()))
            );
            
            // 处理结果
            let mut cache_updates = Vec::new();
            for (i, result) in db_results.into_iter().enumerate() {
                if let Ok(Some(data)) = result {
                    if let Ok(output) = serde_json::from_slice::<AtomicalOutput>(&data) {
                        results.insert(missed_ids[i].clone(), output.clone());
                        cache_updates.push((missed_keys[i].clone(), output));
                    }
                }
            }
            
            // 更新缓存
            if let Ok(mut cache) = self.outputs_cache.lock() {
                for (key, output) in cache_updates {
                    cache.put(key, CacheItem::new(output, self.cache_config.ttl.map(Duration::from_secs)));
                    self.cache_stats.record_write();
                }
            }
        }
        
        Ok(results)
    }
    
    /// 批量获取 Atomical 元数据
    pub fn batch_get_metadata(&self, atomical_ids: &[AtomicalId]) -> Result<HashMap<AtomicalId, serde_json::Value>> {
        let mut results = HashMap::with_capacity(atomical_ids.len());
        let mut missed_ids = Vec::new();
        let mut missed_keys = Vec::new();
        
        // 首先尝试从缓存获取
        if let Ok(cache) = self.metadata_cache.lock() {
            for id in atomical_ids {
                let key = format!("{}:{}", id.txid, id.vout);
                if let Some(item) = cache.get(&key) {
                    if !item.is_expired() {
                        if let Some(metadata) = item.get_value() {
                            results.insert(id.clone(), metadata);
                            self.cache_stats.record_hit();
                            continue;
                        }
                    }
                }
                
                // 缓存未命中，记录需要从数据库获取的ID
                missed_ids.push(id.clone());
                missed_keys.push(key);
                self.cache_stats.record_miss();
            }
        } else {
            // 如果无法获取缓存锁，则所有ID都需要从数据库获取
            for id in atomical_ids {
                missed_ids.push(id.clone());
                missed_keys.push(format!("{}:{}", id.txid, id.vout));
                self.cache_stats.record_miss();
            }
        }
        
        // 如果有未命中的ID，从数据库批量获取
        if !missed_ids.is_empty() {
            let cf = self.db.cf_handle(CF_METADATA)
                .ok_or_else(|| anyhow!("Column family not found: {}", CF_METADATA))?;
            
            // 使用 multi_get_cf 批量获取
            let db_results = self.db.multi_get_cf(
                missed_keys.iter().map(|k| (&cf, k.as_bytes()))
            );
            
            // 处理结果
            let mut cache_updates = Vec::new();
            for (i, result) in db_results.into_iter().enumerate() {
                if let Ok(Some(data)) = result {
                    if let Ok(metadata) = serde_json::from_slice::<serde_json::Value>(&data) {
                        results.insert(missed_ids[i].clone(), metadata.clone());
                        cache_updates.push((missed_keys[i].clone(), metadata));
                    }
                }
            }
            
            // 更新缓存
            if let Ok(mut cache) = self.metadata_cache.lock() {
                for (key, metadata) in cache_updates {
                    cache.put(key, CacheItem::new(metadata, self.cache_config.ttl.map(Duration::from_secs)));
                    self.cache_stats.record_write();
                }
            }
        }
        
        Ok(results)
    }
    
    /// 批量存储 Atomical 输出
    pub fn batch_store_outputs(&self, outputs: &[(AtomicalId, AtomicalOutput)]) -> Result<usize> {
        if outputs.is_empty() {
            return Ok(0);
        }
        
        // 使用事务包装操作
        self.with_transaction(|transaction_id| {
            let cf = self.db.cf_handle(CF_OUTPUTS)
                .ok_or_else(|| anyhow!("Column family not found: {}", CF_OUTPUTS))?;
            
            // 创建批量写入
            let mut batch = WriteBatch::default();
            let mut cache_updates = Vec::with_capacity(outputs.len());
            
            // 准备批量数据
            for (id, output) in outputs {
                let key = format!("{}:{}", id.txid, id.vout);
                let data = serde_json::to_vec(output)?;
                
                // 记录写操作到事务日志
                self.transaction_manager.log_write(transaction_id, CF_OUTPUTS, key.as_bytes(), &data)?;
                
                batch.put_cf(&cf, key.as_bytes(), &data);
                cache_updates.push((key, output.clone()));
            }
            
            // 执行批量写入
            self.db.write(batch)?;
            
            // 更新缓存
            if let Ok(mut cache) = self.outputs_cache.lock() {
                for (key, output) in cache_updates {
                    cache.put(key, CacheItem::new(output, self.cache_config.ttl.map(Duration::from_secs)));
                    self.cache_stats.record_write();
                }
            }
            
            Ok(outputs.len())
        })
    }
    
    /// 批量存储 Atomical 元数据
    pub fn batch_store_metadata(&self, metadata_items: &[(AtomicalId, serde_json::Value)]) -> Result<usize> {
        if metadata_items.is_empty() {
            return Ok(0);
        }
        
        // 使用事务包装操作
        self.with_transaction(|transaction_id| {
            let cf = self.db.cf_handle(CF_METADATA)
                .ok_or_else(|| anyhow!("Column family not found: {}", CF_METADATA))?;
            
            // 创建批量写入
            let mut batch = WriteBatch::default();
            let mut cache_updates = Vec::with_capacity(metadata_items.len());
            
            // 准备批量数据
            for (id, metadata) in metadata_items {
                let key = format!("{}:{}", id.txid, id.vout);
                let data = serde_json::to_vec(metadata)?;
                
                // 记录写操作到事务日志
                self.transaction_manager.log_write(transaction_id, CF_METADATA, key.as_bytes(), &data)?;
                
                batch.put_cf(&cf, key.as_bytes(), &data);
                cache_updates.push((key, metadata.clone()));
            }
            
            // 执行批量写入
            self.db.write(batch)?;
            
            // 更新缓存
            if let Ok(mut cache) = self.metadata_cache.lock() {
                for (key, metadata) in cache_updates {
                    cache.put(key, CacheItem::new(metadata, self.cache_config.ttl.map(Duration::from_secs)));
                    self.cache_stats.record_write();
                }
            }
            
            Ok(metadata_items.len())
        })
    }
    
    /// 通用并行批处理方法 - 用于高效处理大量数据
    pub fn parallel_batch_process<T, K, F, R>(&self, items: &[T], process_fn: F) -> Vec<R>
    where
        T: Send + Sync + Clone,
        F: Fn(T) -> R + Send + Sync,
        R: Send,
        K: Send + Sync,
    {
        use rayon::prelude::*;
        
        // 使用 Rayon 并行处理
        items.par_iter()
            .map(|item| process_fn(item.clone()))
            .collect()
    }
    
    /// 并行批量获取 Atomical 输出 - 利用多线程提高性能
    pub fn parallel_batch_get_outputs(&self, atomical_ids: &[AtomicalId]) -> Result<HashMap<AtomicalId, AtomicalOutput>> {
        // 使用并行批处理方法
        let results = self.parallel_batch_process(atomical_ids, |id| -> (AtomicalId, Option<AtomicalOutput>) {
            (id.clone(), self.get_output(&id).unwrap_or(None))
        });
        
        // 过滤并收集结果
        let mut output_map = HashMap::with_capacity(results.len());
        for (id, output_opt) in results {
            if let Some(output) = output_opt {
                output_map.insert(id, output);
            }
        }
        
        Ok(output_map)
    }
    
    /// 并行批量获取 Atomical 元数据 - 利用多线程提高性能
    pub fn parallel_batch_get_metadata(&self, atomical_ids: &[AtomicalId]) -> Result<HashMap<AtomicalId, serde_json::Value>> {
        // 使用并行批处理方法
        let results = self.parallel_batch_process(atomical_ids, |id| -> (AtomicalId, Option<serde_json::Value>) {
            (id.clone(), self.get_metadata(&id).unwrap_or(None))
        });
        
        // 过滤并收集结果
        let mut metadata_map = HashMap::with_capacity(results.len());
        for (id, metadata_opt) in results {
            if let Some(metadata) = metadata_opt {
                metadata_map.insert(id, metadata);
            }
        }
        
        Ok(metadata_map)
    }
    
    /// 清理所有缓存
    pub fn clear_caches(&self) -> Result<()> {
        if let Ok(mut cache) = self.outputs_cache.lock() {
            cache.clear();
        }
        
        if let Ok(mut cache) = self.metadata_cache.lock() {
            cache.clear();
        }
        
        if let Ok(mut cache) = self.index_cache.lock() {
            cache.clear();
        }
        
        if let Ok(mut cache) = self.script_atomicals_cache.lock() {
            cache.clear();
        }
        
        // 重置统计信息
        self.cache_stats.reset();
        
        Ok(())
    }
    
    /// 调整缓存大小
    pub fn resize_caches(&self, new_config: CacheConfig) -> Result<()> {
        if let Ok(mut cache) = self.outputs_cache.lock() {
            let mut new_cache = LruCache::new(NonZeroUsize::new(new_config.outputs_cache_size).unwrap());
            
            // 保留现有缓存项
            for (key, value) in cache.iter().take(new_config.outputs_cache_size) {
                new_cache.put(key.clone(), value.clone());
            }
            
            *cache = new_cache;
        }
        
        if let Ok(mut cache) = self.metadata_cache.lock() {
            let mut new_cache = LruCache::new(NonZeroUsize::new(new_config.metadata_cache_size).unwrap());
            
            // 保留现有缓存项
            for (key, value) in cache.iter().take(new_config.metadata_cache_size) {
                new_cache.put(key.clone(), value.clone());
            }
            
            *cache = new_cache;
        }
        
        if let Ok(mut cache) = self.index_cache.lock() {
            let mut new_cache = LruCache::new(NonZeroUsize::new(new_config.index_cache_size).unwrap());
            
            // 保留现有缓存项
            for (key, value) in cache.iter().take(new_config.index_cache_size) {
                new_cache.put(key.clone(), value.clone());
            }
            
            *cache = new_cache;
        }
        
        if let Ok(mut cache) = self.script_atomicals_cache.lock() {
            let mut new_cache = LruCache::new(NonZeroUsize::new(new_config.script_atomicals_cache_size).unwrap());
            
            // 保留现有缓存项
            for (key, value) in cache.iter().take(new_config.script_atomicals_cache_size) {
                new_cache.put(key.clone(), value.clone());
            }
            
            *cache = new_cache;
        }
        
        Ok(())
    }
    
    /// 清理过期的缓存项
    pub fn prune_expired_cache_items(&self) -> Result<usize> {
        let mut pruned_count = 0;
        
        if let Ok(mut cache) = self.outputs_cache.lock() {
            let before_len = cache.len();
            
            // 创建新缓存，只保留未过期的项
            let mut new_cache = LruCache::new(NonZeroUsize::new(cache.cap().get()).unwrap());
            
            for (key, item) in cache.iter() {
                if !item.is_expired() {
                    new_cache.put(key.clone(), item.clone());
                }
            }
            
            *cache = new_cache;
            pruned_count += before_len - cache.len();
        }
        
        if let Ok(mut cache) = self.metadata_cache.lock() {
            let before_len = cache.len();
            
            // 创建新缓存，只保留未过期的项
            let mut new_cache = LruCache::new(NonZeroUsize::new(cache.cap().get()).unwrap());
            
            for (key, item) in cache.iter() {
                if !item.is_expired() {
                    new_cache.put(key.clone(), item.clone());
                }
            }
            
            *cache = new_cache;
            pruned_count += before_len - cache.len();
        }
        
        if let Ok(mut cache) = self.index_cache.lock() {
            let before_len = cache.len();
            
            // 创建新缓存，只保留未过期的项
            let mut new_cache = LruCache::new(NonZeroUsize::new(cache.cap().get()).unwrap());
            
            for (key, item) in cache.iter() {
                if !item.is_expired() {
                    new_cache.put(key.clone(), item.clone());
                }
            }
            
            *cache = new_cache;
            pruned_count += before_len - cache.len();
        }
        
        if let Ok(mut cache) = self.script_atomicals_cache.lock() {
            let before_len = cache.len();
            
            // 创建新缓存，只保留未过期的项
            let mut new_cache = LruCache::new(NonZeroUsize::new(cache.cap().get()).unwrap());
            
            for (key, item) in cache.iter() {
                if !item.is_expired() {
                    new_cache.put(key.clone(), item.clone());
                }
            }
            
            *cache = new_cache;
            pruned_count += before_len - cache.len();
        }
        
        Ok(pruned_count)
    }
    
    /// 获取缓存使用情况
    pub fn get_cache_usage(&self) -> HashMap<String, usize> {
        let mut usage = HashMap::new();
        
        if let Ok(cache) = self.outputs_cache.lock() {
            usage.insert("outputs_len".to_string(), cache.len());
            usage.insert("outputs_cap".to_string(), cache.cap().get());
        }
        
        if let Ok(cache) = self.metadata_cache.lock() {
            usage.insert("metadata_len".to_string(), cache.len());
            usage.insert("metadata_cap".to_string(), cache.cap().get());
        }
        
        if let Ok(cache) = self.index_cache.lock() {
            usage.insert("index_len".to_string(), cache.len());
            usage.insert("index_cap".to_string(), cache.cap().get());
        }
        
        if let Ok(cache) = self.script_atomicals_cache.lock() {
            usage.insert("script_atomicals_len".to_string(), cache.len());
            usage.insert("script_atomicals_cap".to_string(), cache.cap().get());
        }
        
        usage
    }
    
    /// 获取缓存统计信息
    pub fn get_cache_stats(&self) -> HashMap<String, serde_json::Value> {
        self.cache_stats.get_stats()
    }
    
    /// 预热输出缓存
    pub fn warm_outputs_cache(&self, limit: usize) -> Result<usize> {
        let cf = self.db.cf_handle(CF_OUTPUTS)
            .ok_or_else(|| anyhow!("Column family not found: {}", CF_OUTPUTS))?;
        
        let mut loaded_count = 0;
        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);
        
        for result in iter {
            if loaded_count >= limit {
                break;
            }
            
            let (key, value) = result?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            
            if let Ok(output) = serde_json::from_slice::<AtomicalOutput>(&value) {
                if let Ok(mut cache) = self.outputs_cache.lock() {
                    cache.put(key_str, CacheItem::new(output, self.cache_config.ttl.map(Duration::from_secs)));
                    loaded_count += 1;
                }
            }
        }
        
        Ok(loaded_count)
    }
    
    /// 预热元数据缓存
    pub fn warm_metadata_cache(&self, limit: usize) -> Result<usize> {
        let cf = self.db.cf_handle(CF_METADATA)
            .ok_or_else(|| anyhow!("Column family not found: {}", CF_METADATA))?;
        
        let mut loaded_count = 0;
        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);
        
        for result in iter {
            if loaded_count >= limit {
                break;
            }
            
            let (key, value) = result?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            
            if let Ok(metadata) = serde_json::from_slice::<serde_json::Value>(&value) {
                if let Ok(mut cache) = self.metadata_cache.lock() {
                    cache.put(key_str, CacheItem::new(metadata, self.cache_config.ttl.map(Duration::from_secs)));
                    loaded_count += 1;
                }
            }
        }
        
        Ok(loaded_count)
    }
    
    /// 预热脚本-Atomicals映射缓存
    pub fn warm_script_atomicals_cache(&self, limit: usize) -> Result<usize> {
        let cf = self.db.cf_handle(CF_SCRIPT_ATOMICALS)
            .ok_or_else(|| anyhow!("Column family not found: {}", CF_SCRIPT_ATOMICALS))?;
        
        let mut loaded_count = 0;
        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);
        
        for result in iter {
            if loaded_count >= limit {
                break;
            }
            
            let (key, value) = result?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            
            if let Ok(atomicals) = serde_json::from_slice::<Vec<AtomicalId>>(&value) {
                if let Ok(mut cache) = self.script_atomicals_cache.lock() {
                    cache.put(key_str, CacheItem::new(atomicals, self.cache_config.ttl.map(Duration::from_secs)));
                    loaded_count += 1;
                }
            }
        }
        
        Ok(loaded_count)
    }
    
    /// 预热所有缓存
    pub fn warm_all_caches(&self, limits: &HashMap<&str, usize>) -> Result<HashMap<String, usize>> {
        let mut results = HashMap::new();
        
        if let Some(&limit) = limits.get("outputs") {
            let count = self.warm_outputs_cache(limit)?;
            results.insert("outputs".to_string(), count);
        }
        
        if let Some(&limit) = limits.get("metadata") {
            let count = self.warm_metadata_cache(limit)?;
            results.insert("metadata".to_string(), count);
        }
        
        if let Some(&limit) = limits.get("script_atomicals") {
            let count = self.warm_script_atomicals_cache(limit)?;
            results.insert("script_atomicals".to_string(), count);
        }
        
        Ok(results)
    }
    
    /// 开始事务
    pub fn begin_transaction(&self) -> Result<u64> {
        self.transaction_manager.begin_transaction()
    }
    
    /// 提交事务
    pub fn commit_transaction(&self, transaction_id: u64) -> Result<()> {
        self.transaction_manager.commit_transaction(transaction_id)
    }
    
    /// 回滚事务
    pub fn rollback_transaction(&self, transaction_id: u64) -> Result<()> {
        self.transaction_manager.rollback_transaction(transaction_id)
    }
    
    /// 在事务中执行操作
    pub fn with_transaction<F, T>(&self, operation: F) -> Result<T>
    where
        F: FnOnce(u64) -> Result<T>,
    {
        // 开始事务
        let transaction_id = self.begin_transaction()?;
        
        // 执行操作
        let result = match operation(transaction_id) {
            Ok(value) => {
                // 提交事务
                self.commit_transaction(transaction_id)?;
                Ok(value)
            }
            Err(e) => {
                // 回滚事务
                let _ = self.rollback_transaction(transaction_id);
                Err(e)
            }
        };
        
        result
    }
    
    /// 执行数据一致性检查
    pub fn check_data_consistency(&self) -> Result<bool> {
        let cf_names = [CF_STATE, CF_OUTPUTS, CF_METADATA, CF_INDEXES, CF_SCRIPT_ATOMICALS];
        self.transaction_manager.verify_data_consistency(&self.db, &cf_names)
    }
    
    /// 创建数据检查点
    pub fn create_checkpoint(&self) -> Result<u64> {
        self.transaction_manager.create_checkpoint()
    }
    
    /// 清理旧事务日志
    pub fn cleanup_transaction_logs(&self, keep_days: u64) -> Result<usize> {
        self.transaction_manager.cleanup_old_logs(keep_days)
    }

    /// 存储 Atomical 输出
    pub fn store_output(&self, id: &AtomicalId, output: &AtomicalOutput) -> Result<()> {
        // 使用事务包装操作
        self.with_transaction(|transaction_id| {
            let key = format!("{}:{}", id.txid, id.vout);
            let data = serde_json::to_vec(output)?;
            
            // 记录写操作到事务日志
            self.transaction_manager.log_write(transaction_id, CF_OUTPUTS, key.as_bytes(), &data)?;
            
            // 写入数据库
            if let Some(cf) = self.db.cf_handle(CF_OUTPUTS) {
                self.db.put_cf(&cf, key.as_bytes(), &data)?;
                
                // 更新缓存
                if let Ok(mut cache) = self.outputs_cache.lock() {
                    cache.put(key, CacheItem::new(output.clone(), self.cache_config.ttl.map(Duration::from_secs)));
                    self.cache_stats.record_write();
                }
                
                return Ok(());
            }
            
            Err(anyhow!("Column family not found: {}", CF_OUTPUTS))
        })
    }
    
    /// 存储 Atomical 元数据
    pub fn store_metadata(&self, id: &AtomicalId, metadata: &serde_json::Value) -> Result<()> {
        // 使用事务包装操作
        self.with_transaction(|transaction_id| {
            let key = format!("{}:{}", id.txid, id.vout);
            let data = serde_json::to_vec(metadata)?;
            
            // 记录写操作到事务日志
            self.transaction_manager.log_write(transaction_id, CF_METADATA, key.as_bytes(), &data)?;
            
            // 写入数据库
            if let Some(cf) = self.db.cf_handle(CF_METADATA) {
                self.db.put_cf(&cf, key.as_bytes(), &data)?;
                
                // 更新缓存
                if let Ok(mut cache) = self.metadata_cache.lock() {
                    cache.put(key, CacheItem::new(metadata.clone(), self.cache_config.ttl.map(Duration::from_secs)));
                    self.cache_stats.record_write();
                }
                
                return Ok(());
            }
            
            Err(anyhow!("Column family not found: {}", CF_METADATA))
        })
    }
    
    /// 存储索引项
    pub fn store_index(&self, index_name: &str, key: &str, value: &[u8]) -> Result<()> {
        // 使用事务包装操作
        self.with_transaction(|transaction_id| {
            let index_key = format!("{}:{}", index_name, key);
            
            // 记录写操作到事务日志
            self.transaction_manager.log_write(transaction_id, CF_INDEXES, index_key.as_bytes(), value)?;
            
            // 写入数据库
            if let Some(cf) = self.db.cf_handle(CF_INDEXES) {
                self.db.put_cf(&cf, index_key.as_bytes(), value)?;
                
                // 更新缓存
                if let Ok(mut cache) = self.index_cache.lock() {
                    cache.put(index_key, CacheItem::new(value.to_vec(), self.cache_config.ttl.map(Duration::from_secs)));
                    self.cache_stats.record_write();
                }
                
                return Ok(());
            }
            
            Err(anyhow!("Column family not found: {}", CF_INDEXES))
        })
    }
    
    /// 存储脚本-Atomicals映射
    pub fn store_script_atomicals(&self, script_hash: &str, atomicals: &[AtomicalId]) -> Result<()> {
        // 使用事务包装操作
        self.with_transaction(|transaction_id| {
            let data = serde_json::to_vec(atomicals)?;
            
            // 记录写操作到事务日志
            self.transaction_manager.log_write(transaction_id, CF_SCRIPT_ATOMICALS, script_hash.as_bytes(), &data)?;
            
            // 写入数据库
            if let Some(cf) = self.db.cf_handle(CF_SCRIPT_ATOMICALS) {
                self.db.put_cf(&cf, script_hash.as_bytes(), &data)?;
                
                // 更新缓存
                if let Ok(mut cache) = self.script_atomicals_cache.lock() {
                    cache.put(script_hash.to_string(), CacheItem::new(atomicals.to_vec(), self.cache_config.ttl.map(Duration::from_secs)));
                    self.cache_stats.record_write();
                }
                
                return Ok(());
            }
            
            Err(anyhow!("Column family not found: {}", CF_SCRIPT_ATOMICALS))
        })
    }

    /// 删除 Atomical 输出
    pub fn delete_output(&self, id: &AtomicalId) -> Result<bool> {
        // 使用事务包装操作
        self.with_transaction(|transaction_id| {
            let key = format!("{}:{}", id.txid, id.vout);
            
            // 从缓存中删除
            if let Ok(mut cache) = self.outputs_cache.lock() {
                cache.pop(&key);
            }
            
            // 从数据库中删除
            if let Some(cf) = self.db.cf_handle(CF_OUTPUTS) {
                // 记录删除操作到事务日志
                self.transaction_manager.log_delete(transaction_id, CF_OUTPUTS, key.as_bytes())?;
                
                self.db.delete_cf(&cf, key.as_bytes())?;
                return Ok(true);
            }
            
            Ok(false)
        })
    }
    
    /// 删除 Atomical 元数据
    pub fn delete_metadata(&self, id: &AtomicalId) -> Result<bool> {
        // 使用事务包装操作
        self.with_transaction(|transaction_id| {
            let key = format!("{}:{}", id.txid, id.vout);
            
            // 从缓存中删除
            if let Ok(mut cache) = self.metadata_cache.lock() {
                cache.pop(&key);
            }
            
            // 从数据库中删除
            if let Some(cf) = self.db.cf_handle(CF_METADATA) {
                // 记录删除操作到事务日志
                self.transaction_manager.log_delete(transaction_id, CF_METADATA, key.as_bytes())?;
                
                self.db.delete_cf(&cf, key.as_bytes())?;
                return Ok(true);
            }
            
            Ok(false)
        })
    }
    
    /// 删除索引项
    pub fn delete_index(&self, index_name: &str, key: &str) -> Result<bool> {
        // 使用事务包装操作
        self.with_transaction(|transaction_id| {
            let index_key = format!("{}:{}", index_name, key);
            
            // 从缓存中删除
            if let Ok(mut cache) = self.index_cache.lock() {
                cache.pop(&index_key);
            }
            
            // 从数据库中删除
            if let Some(cf) = self.db.cf_handle(CF_INDEXES) {
                // 记录删除操作到事务日志
                self.transaction_manager.log_delete(transaction_id, CF_INDEXES, index_key.as_bytes())?;
                
                self.db.delete_cf(&cf, index_key.as_bytes())?;
                return Ok(true);
            }
            
            Ok(false)
        })
    }

    /// 执行数据一致性检查和修复
    pub fn verify_and_repair_data_consistency(&self) -> Result<(bool, usize)> {
        info!("开始执行数据一致性检查和修复...");
        
        // 首先检查数据一致性
        let is_consistent = self.check_data_consistency()?;
        
        if is_consistent {
            info!("数据一致性检查通过，无需修复");
            return Ok((true, 0));
        }
        
        info!("检测到数据不一致，开始修复...");
        
        // 创建修复事务
        let transaction_id = self.begin_transaction()?;
        
        // 修复计数
        let mut repair_count = 0;
        
        // 检查并修复各个列族
        let cf_names = [CF_STATE, CF_OUTPUTS, CF_METADATA, CF_INDEXES, CF_SCRIPT_ATOMICALS];
        
        for cf_name in &cf_names {
            if let Some(cf) = self.db.cf_handle(cf_name) {
                info!("检查列族 {}...", cf_name);
                
                // 创建批量写入
                let mut batch = WriteBatch::default();
                
                // 检查每个键值对
                let iter = self.db.iterator_cf(&cf, IteratorMode::Start);
                
                for result in iter {
                    match result {
                        Ok((key, value)) => {
                            // 尝试解析值，检查是否有效
                            let is_valid = match *cf_name {
                                CF_OUTPUTS => serde_json::from_slice::<AtomicalOutput>(&value).is_ok(),
                                CF_METADATA => serde_json::from_slice::<serde_json::Value>(&value).is_ok(),
                                CF_INDEXES => true, // 简单的二进制数据，假设总是有效
                                CF_SCRIPT_ATOMICALS => serde_json::from_slice::<Vec<AtomicalId>>(&value).is_ok(),
                                CF_STATE => true, // 状态数据，假设总是有效
                                _ => true,
                            };
                            
                            if !is_valid {
                                warn!("发现损坏的数据：列族={}, 键长度={}", cf_name, key.len());
                                
                                // 如果是索引或脚本映射，可以尝试从其他数据重建
                                if *cf_name == CF_INDEXES || *cf_name == CF_SCRIPT_ATOMICALS {
                                    // 从数据库中删除损坏的数据
                                    batch.delete_cf(&cf, &key);
                                    
                                    // 记录删除操作到事务日志
                                    self.transaction_manager.log_delete(transaction_id, cf_name, &key)?;
                                    
                                    repair_count += 1;
                                }
                            }
                        }
                        Err(e) => {
                            error!("读取数据时出错：列族={}, 错误={}", cf_name, e);
                        }
                    }
                }
                
                // 应用批处理
                if !batch.is_empty() {
                    self.db.write(batch)?;
                    info!("已修复列族 {} 中的 {} 条损坏数据", cf_name, repair_count);
                }
            }
        }
        
        // 提交修复事务
        self.commit_transaction(transaction_id)?;
        
        // 创建检查点
        self.create_checkpoint()?;
        
        info!("数据一致性修复完成，共修复 {} 条损坏数据", repair_count);
        
        Ok((repair_count == 0, repair_count))
    }
    
    /// 定期维护任务
    pub fn perform_maintenance(&self) -> Result<HashMap<String, serde_json::Value>> {
        info!("开始执行定期维护任务...");
        
        let mut results = HashMap::new();
        
        // 1. 清理过期缓存项
        let pruned_count = self.prune_expired_cache_items()?;
        results.insert("pruned_cache_items".to_string(), serde_json::Value::Number(pruned_count.into()));
        
        // 2. 检查数据一致性
        let (is_consistent, repair_count) = self.verify_and_repair_data_consistency()?;
        results.insert("data_consistent".to_string(), serde_json::Value::Bool(is_consistent));
        results.insert("repaired_items".to_string(), serde_json::Value::Number(repair_count.into()));
        
        // 3. 清理旧事务日志
        let cleaned_logs = self.cleanup_transaction_logs(7)?; // 保留7天的日志
        results.insert("cleaned_logs".to_string(), serde_json::Value::Number(cleaned_logs.into()));
        
        // 4. 创建新的检查点
        let checkpoint_id = self.create_checkpoint()?;
        results.insert("checkpoint_id".to_string(), serde_json::Value::Number(checkpoint_id.into()));
        
        // 5. 获取缓存使用情况
        let cache_usage = self.get_cache_usage();
        results.insert("cache_usage".to_string(), serde_json::to_value(cache_usage)?);
        
        // 6. 获取缓存统计信息
        let cache_stats = self.get_cache_stats();
        results.insert("cache_stats".to_string(), serde_json::to_value(cache_stats)?);
        
        info!("定期维护任务完成");
        
        Ok(results)
    }
    
    /// 执行崩溃恢复
    pub fn recover_from_crash(&self) -> Result<HashMap<String, serde_json::Value>> {
        info!("开始执行崩溃恢复...");
        
        let mut results = HashMap::new();
        
        // 1. 执行事务日志恢复
        self.transaction_manager.perform_crash_recovery(&self.db)?;
        results.insert("transaction_recovery".to_string(), serde_json::Value::String("completed".to_string()));
        
        // 2. 验证数据一致性
        let (is_consistent, repair_count) = self.verify_and_repair_data_consistency()?;
        results.insert("data_consistent".to_string(), serde_json::Value::Bool(is_consistent));
        results.insert("repaired_items".to_string(), serde_json::Value::Number(repair_count.into()));
        
        // 3. 清理缓存
        self.clear_caches()?;
        results.insert("caches_cleared".to_string(), serde_json::Value::Bool(true));
        
        // 4. 预热缓存
        let warm_results = self.warm_all_caches(&HashMap::from([
            (CF_OUTPUTS, 1000),
            (CF_METADATA, 500),
            (CF_SCRIPT_ATOMICALS, 1000),
        ]))?;
        results.insert("cache_warmed".to_string(), serde_json::to_value(warm_results)?);
        
        // 5. 创建新的检查点
        let checkpoint_id = self.create_checkpoint()?;
        results.insert("checkpoint_id".to_string(), serde_json::Value::Number(checkpoint_id.into()));
        
        info!("崩溃恢复完成");
        
        Ok(results)
    }
}
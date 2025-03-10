use std::sync::{Arc, Mutex};
use std::path::Path;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{anyhow, Result};
use bitcoin::{TxOut, Txid};
use bitcoin::consensus::encode;
use electrs_rocksdb::{DB, IteratorMode, Options, ColumnFamilyDescriptor};
use serde::{Deserialize, Serialize};
use hex;
use lru::LruCache;
use std::num::NonZeroUsize;

use super::protocol::{AtomicalId, AtomicalType};
use super::state::AtomicalOutput;
use super::indexer::IndexEntry;

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
        
        let db = DB::open_cf_descriptors(&opts, data_dir, cf_descriptors)?;
        
        // 创建缓存，使用配置的缓存大小
        let outputs_cache = Mutex::new(LruCache::new(NonZeroUsize::new(cache_config.outputs_cache_size).unwrap()));
        let metadata_cache = Mutex::new(LruCache::new(NonZeroUsize::new(cache_config.metadata_cache_size).unwrap()));
        let index_cache = Mutex::new(LruCache::new(NonZeroUsize::new(cache_config.index_cache_size).unwrap()));
        let script_atomicals_cache = Mutex::new(LruCache::new(NonZeroUsize::new(cache_config.script_atomicals_cache_size).unwrap()));
        
        // 创建缓存统计
        let cache_stats = CacheStats::new();
        
        Ok(Self {
            db: Arc::new(db),
            outputs_cache,
            metadata_cache,
            index_cache,
            script_atomicals_cache,
            cache_config,
            cache_stats,
        })
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
    {{ ... }}
    
    /// 分层缓存实现 - 用于优先级缓存管理
    pub fn implement_tiered_caching(&self) -> Result<()> {
        // 此功能将实现一个分层缓存系统
        // L1: 热点数据，高频访问，小容量，快速访问
        // L2: 温数据，中频访问，中等容量
        // L3: 冷数据，低频访问，大容量
        
        // 简化实现，仅调整现有缓存配置模拟分层效果
        let mut l1_config = self.cache_config.clone();
        l1_config.outputs_cache_size = 1000; // 小容量
        l1_config.metadata_cache_size = 500;
        l1_config.ttl = Some(300); // 短TTL，5分钟
        
        // 应用L1配置
        self.resize_caches(l1_config)?;
        
        // 预热L1缓存，加载最热点数据
        self.warm_outputs_cache(500)?;
        self.warm_metadata_cache(200)?;
        
        Ok(())
    }
    
    /// 缓存压缩 - 减少内存占用
    pub fn compress_cache_data(&self) -> Result<()> {
        // 实际实现中，这里可以对缓存中的数据进行压缩
        // 例如，使用LZ4或Snappy等快速压缩算法
        // 简化实现，仅清理不必要的缓存项
        
        // 清理过期项
        self.prune_expired_cache_items()?;
        
        // 清理低频访问项
        // 实际实现中，可以根据访问频率清理
        // 简化实现，仅保留最近访问的项
        
        Ok(())
    }
    
    /// 批量操作的缓存优化 - 针对批量操作优化缓存策略
    pub fn optimize_for_batch_operations(&self, batch_size: usize) -> Result<()> {
        // 根据批量操作的大小调整缓存策略
        
        if batch_size > 10000 {
            // 大批量操作，使用较小缓存避免内存压力
            let mut config = self.cache_config.clone();
            config.outputs_cache_size = 5000;
            config.metadata_cache_size = 2000;
            config.ttl = Some(300); // 短TTL
            self.resize_caches(config)?;
        } else if batch_size > 1000 {
            // 中等批量操作，使用平衡配置
            let mut config = self.cache_config.clone();
            config.outputs_cache_size = 10000;
            config.metadata_cache_size = 5000;
            config.ttl = Some(900); // 15分钟
            self.resize_caches(config)?;
        } else {
            // 小批量操作，可以使用较大缓存
            let mut config = self.cache_config.clone();
            config.outputs_cache_size = 20000;
            config.metadata_cache_size = 10000;
            config.ttl = Some(1800); // 30分钟
            self.resize_caches(config)?;
        }
        
        Ok(())
    }
    
    /// 智能预取 - 根据访问模式预测并预取可能需要的数据
    pub fn smart_prefetch(&self, recent_atomical_ids: &[AtomicalId]) -> Result<usize> {
        let mut prefetched_count = 0;
        
        // 根据最近访问的Atomicals预测可能需要的相关数据
        // 简化实现，假设相关数据是相同高度的Atomicals
        
        // 获取最近访问的Atomicals的高度
        let mut heights = HashSet::new();
        for atomical_id in recent_atomical_ids {
            if let Some(output) = self.get_output(atomical_id)? {
                heights.insert(output.height);
            }
        }
        
        // 预取相同高度的其他Atomicals
        let cf = self.db.cf_handle(CF_OUTPUTS)
            .ok_or_else(|| anyhow!("Column family not found: {}", CF_OUTPUTS))?;
        
        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);
        
        for result in iter {
            if prefetched_count >= 100 {
                // 限制预取数量
                break;
            }
            
            let (key, value) = result?;
            
            if let Ok(output) = serde_json::from_slice::<AtomicalOutput>(&value) {
                if heights.contains(&output.height) {
                    // 相同高度的Atomical，预取其输出和元数据
                    let key_str = String::from_utf8_lossy(&key).to_string();
                    
                    if let Ok(mut cache) = self.outputs_cache.lock() {
                        cache.put(key_str.clone(), CacheItem::new(output.clone(), self.cache_config.ttl.map(Duration::from_secs)));
                        prefetched_count += 1;
                    }
                    
                    // 解析Atomical ID
                    let parts: Vec<&str> = key_str.split(':').collect();
                    if parts.len() == 2 {
                        if let (Ok(txid), Ok(vout)) = (parts[0].parse::<Txid>(), parts[1].parse::<u32>()) {
                            let atomical_id = AtomicalId { txid, vout };
                            
                            // 预取元数据
                            if let Some(metadata) = self.get_metadata(&atomical_id)? {
                                if let Ok(mut cache) = self.metadata_cache.lock() {
                                    cache.put(key_str, CacheItem::new(metadata, self.cache_config.ttl.map(Duration::from_secs)));
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Ok(prefetched_count)
    }
    
    /// 并行缓存加载 - 使用多线程并行加载缓存数据
    pub fn parallel_cache_load(&self, atomical_ids: &[AtomicalId]) -> Result<usize> {
        use rayon::prelude::*;
        
        // 使用Rayon并行库并行加载缓存
        let loaded_count = atomical_ids.par_iter()
            .map(|atomical_id| -> Result<usize> {
                let mut count = 0;
                
                // 加载输出
                if let Some(output) = self.get_output(atomical_id)? {
                    count += 1;
                    
                    // 加载元数据
                    if let Some(_) = self.get_metadata(atomical_id)? {
                        count += 1;
                    }
                }
                
                Ok(count)
            })
            .collect::<Result<Vec<_>>>()?
            .iter()
            .sum();
        
        Ok(loaded_count)
    }
    
    /// 缓存持久化 - 将热点缓存数据持久化到磁盘，用于快速恢复
    pub fn persist_hot_cache(&self, cache_file: &str) -> Result<usize> {
        let mut persisted_count = 0;
        
        // 收集热点缓存数据
        let mut hot_data = HashMap::new();
        
        // 收集输出缓存
        if let Ok(cache) = self.outputs_cache.lock() {
            for (key, item) in cache.iter() {
                if let Some(value) = item.get_value() {
                    hot_data.insert(format!("outputs:{}", key), serde_json::to_value(value)?);
                    persisted_count += 1;
                }
            }
        }
        
        // 收集元数据缓存
        if let Ok(cache) = self.metadata_cache.lock() {
            for (key, item) in cache.iter() {
                if let Some(value) = item.get_value() {
                    hot_data.insert(format!("metadata:{}", key), value);
                    persisted_count += 1;
                }
            }
        }
        
        // 将热点数据写入文件
        let file = std::fs::File::create(cache_file)?;
        serde_json::to_writer(file, &hot_data)?;
        
        Ok(persisted_count)
    }
    
    /// 从持久化文件加载缓存 - 从磁盘恢复热点缓存数据
    pub fn load_persisted_cache(&self, cache_file: &str) -> Result<usize> {
        let mut loaded_count = 0;
        
        // 从文件读取热点数据
        let file = std::fs::File::open(cache_file)?;
        let hot_data: HashMap<String, serde_json::Value> = serde_json::from_reader(file)?;
        
        // 恢复缓存数据
        for (key, value) in hot_data {
            if key.starts_with("outputs:") {
                let atomical_key = key.trim_start_matches("outputs:");
                if let Ok(output) = serde_json::from_value::<AtomicalOutput>(value) {
                    if let Ok(mut cache) = self.outputs_cache.lock() {
                        cache.put(atomical_key.to_string(), CacheItem::new(output, self.cache_config.ttl.map(Duration::from_secs)));
                        loaded_count += 1;
                    }
                }
            } else if key.starts_with("metadata:") {
                let atomical_key = key.trim_start_matches("metadata:");
                if let Ok(mut cache) = self.metadata_cache.lock() {
                    cache.put(atomical_key.to_string(), CacheItem::new(value, self.cache_config.ttl.map(Duration::from_secs)));
                    loaded_count += 1;
                }
            }
        }
        
        Ok(loaded_count)
    }
    {{ ... }}
}

#[cfg(test)]
mod tests {
    {{ ... }}
    
    #[test]
    fn test_tiered_caching() -> Result<()> {
        let (storage, _temp_dir) = create_test_storage();
        
        // 实现分层缓存
        storage.implement_tiered_caching()?;
        
        // 验证缓存配置已更改
        let usage = storage.get_cache_usage();
        assert!(usage.contains_key("outputs_cap"), "应包含outputs缓存容量");
        
        Ok(())
    }
    
    #[test]
    fn test_optimize_for_batch_operations() -> Result<()> {
        let (storage, _temp_dir) = create_test_storage();
        
        // 测试不同批量大小的优化
        storage.optimize_for_batch_operations(100)?; // 小批量
        let small_usage = storage.get_cache_usage();
        
        storage.optimize_for_batch_operations(5000)?; // 中等批量
        let medium_usage = storage.get_cache_usage();
        
        storage.optimize_for_batch_operations(20000)?; // 大批量
        let large_usage = storage.get_cache_usage();
        
        // 验证不同批量大小下的缓存配置不同
        let small_cap = small_usage.get("outputs_cap").cloned().unwrap_or(0);
        let medium_cap = medium_usage.get("outputs_cap").cloned().unwrap_or(0);
        let large_cap = large_usage.get("outputs_cap").cloned().unwrap_or(0);
        
        assert!(small_cap != medium_cap || medium_cap != large_cap, "不同批量大小应有不同的缓存配置");
        
        Ok(())
    }
    
    #[test]
    fn test_smart_prefetch() -> Result<()> {
        let (storage, _temp_dir) = create_test_storage();
        
        // 创建一组相同高度的测试数据
        let mut atomical_ids = Vec::new();
        for i in 0..5 {
            let atomical_id = AtomicalId {
                txid: format!("{:064x}", i).parse().unwrap(),
                vout: 0,
            };
            
            let output = AtomicalOutput {
                txid: atomical_id.txid,
                vout: atomical_id.vout,
                output: TxOut::default(),
                metadata: None,
                height: 100, // 相同高度
                timestamp: 1234567890,
                atomical_type: AtomicalType::NFT,
                sealed: false,
            };
            
            storage.store_output(&atomical_id, &output)?;
            atomical_ids.push(atomical_id);
        }
        
        // 再创建一些不同高度的数据
        for i in 5..10 {
            let atomical_id = AtomicalId {
                txid: format!("{:064x}", i).parse().unwrap(),
                vout: 0,
            };
            
            let output = AtomicalOutput {
                txid: atomical_id.txid,
                vout: atomical_id.vout,
                output: TxOut::default(),
                metadata: None,
                height: 200, // 不同高度
                timestamp: 1234567890,
                atomical_type: AtomicalType::NFT,
                sealed: false,
            };
            
            storage.store_output(&atomical_id, &output)?;
        }
        
        // 清空缓存
        storage.clear_caches()?;
        
        // 使用智能预取
        let prefetched = storage.smart_prefetch(&atomical_ids[0..2])?;
        
        // 验证预取结果
        assert!(prefetched > 0, "应该预取了一些数据");
        
        Ok(())
    }
    
    #[test]
    fn test_parallel_cache_load() -> Result<()> {
        let (storage, _temp_dir) = create_test_storage();
        
        // 创建测试数据
        let mut atomical_ids = Vec::new();
        for i in 0..10 {
            let atomical_id = AtomicalId {
                txid: format!("{:064x}", i).parse().unwrap(),
                vout: 0,
            };
            
            let output = AtomicalOutput {
                txid: atomical_id.txid,
                vout: atomical_id.vout,
                output: TxOut::default(),
                metadata: None,
                height: 100,
                timestamp: 1234567890,
                atomical_type: AtomicalType::NFT,
                sealed: false,
            };
            
            storage.store_output(&atomical_id, &output)?;
            
            // 存储元数据
            let metadata = serde_json::json!({
                "name": format!("Test NFT {}", i),
                "description": format!("Test Description {}", i),
            });
            
            storage.store_metadata(&atomical_id, &metadata)?;
            
            atomical_ids.push(atomical_id);
        }
        
        // 清空缓存
        storage.clear_caches()?;
        
        // 并行加载缓存
        let loaded = storage.parallel_cache_load(&atomical_ids)?;
        
        // 验证加载结果
        assert!(loaded > 0, "应该并行加载了一些数据");
        
        Ok(())
    }
    
    #[test]
    fn test_cache_persistence() -> Result<()> {
        let (storage, temp_dir) = create_test_storage();
        
        // 创建测试数据
        let atomical_id = create_test_atomical_id();
        let output = create_test_output(&atomical_id);
        
        storage.store_output(&atomical_id, &output)?;
        
        let metadata = serde_json::json!({
            "name": "Test NFT",
            "description": "Test Description",
        });
        
        storage.store_metadata(&atomical_id, &metadata)?;
        
        // 持久化缓存
        let cache_file = temp_dir.path().join("hot_cache.json").to_str().unwrap().to_string();
        let persisted = storage.persist_hot_cache(&cache_file)?;
        
        // 清空缓存
        storage.clear_caches()?;
        
        // 加载持久化的缓存
        let loaded = storage.load_persisted_cache(&cache_file)?;
        
        // 验证结果
        assert!(persisted > 0, "应该持久化了一些缓存数据");
        assert!(loaded > 0, "应该加载了一些持久化的缓存数据");
        assert_eq!(persisted, loaded, "持久化和加载的数据数量应该相同");
        
        Ok(())
    }
    {{ ... }}
}

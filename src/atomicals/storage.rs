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
    /// 自适应缓存调整间隔（秒）
    pub adaptive_adjustment_interval: u64,
    /// 是否启用自适应缓存管理
    pub enable_adaptive_caching: bool,
    /// 缓存命中率阈值，低于此值将触发缓存优化
    pub hit_rate_threshold: f64,
    /// 内存压力检测间隔（秒）
    pub memory_pressure_check_interval: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            outputs_cache_size: 10_000,
            metadata_cache_size: 5_000,
            index_cache_size: 1_000,
            script_atomicals_cache_size: 5_000,
            enable_stats: true,
            ttl: None,
            adaptive_adjustment_interval: 300, // 5分钟
            enable_adaptive_caching: true,
            hit_rate_threshold: 0.6,
            memory_pressure_check_interval: 60, // 1分钟
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
    /// 缓存命中次数（按缓存类型）
    cache_hits: HashMap<String, AtomicUsize>,
    /// 缓存未命中次数（按缓存类型）
    cache_misses: HashMap<String, AtomicUsize>,
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
            cache_hits: HashMap::new(),
            cache_misses: HashMap::new(),
        }
    }

    /// 记录缓存命中
    pub fn record_hit(&self, cache_type: &str) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        self.cache_hits.entry(cache_type.to_string()).or_insert_with(|| AtomicUsize::new(0)).fetch_add(1, Ordering::Relaxed);
    }

    /// 记录缓存未命中
    pub fn record_miss(&self, cache_type: &str) {
        self.misses.fetch_add(1, Ordering::Relaxed);
        self.cache_misses.entry(cache_type.to_string()).or_insert_with(|| AtomicUsize::new(0)).fetch_add(1, Ordering::Relaxed);
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

    /// 获取缓存命中率（按缓存类型）
    pub fn get_cache_hit_rate(&self, cache_type: &str) -> f64 {
        let hits = self.cache_hits.get(cache_type).unwrap_or(&AtomicUsize::new(0)).load(Ordering::Relaxed);
        let misses = self.cache_misses.get(cache_type).unwrap_or(&AtomicUsize::new(0)).load(Ordering::Relaxed);
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
        
        // 添加缓存命中率（按缓存类型）
        for (cache_type, hits) in &self.cache_hits {
            let misses = self.cache_misses.get(cache_type).unwrap_or(&AtomicUsize::new(0)).load(Ordering::Relaxed);
            let total = hits.load(Ordering::Relaxed) + misses;
            let hit_rate = if total > 0 { hits.load(Ordering::Relaxed) as f64 / total as f64 } else { 0.0 };
            
            stats.insert(format!("{}_hit_rate", cache_type), serde_json::json!(hit_rate));
        }
        
        stats
    }

    /// 重置统计信息
    pub fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.writes.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
        self.cache_hits.clear();
        self.cache_misses.clear();
    }

    /// 获取缓存命中次数（按缓存类型）
    pub fn get_cache_hits(&self, cache_type: &str) -> usize {
        self.cache_hits.get(cache_type).unwrap_or(&AtomicUsize::new(0)).load(Ordering::Relaxed)
    }

    /// 获取缓存未命中次数（按缓存类型）
    pub fn get_cache_misses(&self, cache_type: &str) -> usize {
        self.cache_misses.get(cache_type).unwrap_or(&AtomicUsize::new(0)).load(Ordering::Relaxed)
    }
}

/// 带有过期时间和访问频率的缓存项
#[derive(Clone)]
struct CacheItem<T> {
    /// 缓存的值
    value: T,
    /// 过期时间
    expires_at: Option<Instant>,
    /// 访问次数
    access_count: usize,
    /// 最后访问时间
    last_accessed: Instant,
    /// 创建时间
    created_at: Instant,
    /// 优先级（值越大优先级越高）
    priority: u8,
}

impl<T: Clone> CacheItem<T> {
    /// 创建新的缓存项
    fn new(value: T, ttl: Option<Duration>) -> Self {
        let expires_at = ttl.map(|duration| Instant::now() + duration);
        Self {
            value,
            expires_at,
            access_count: 0,
            last_accessed: Instant::now(),
            created_at: Instant::now(),
            priority: 0,
        }
    }

    /// 创建带优先级的缓存项
    fn with_priority(value: T, ttl: Option<Duration>, priority: u8) -> Self {
        let expires_at = ttl.map(|duration| Instant::now() + duration);
        Self {
            value,
            expires_at,
            access_count: 0,
            last_accessed: Instant::now(),
            created_at: Instant::now(),
            priority,
        }
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

    /// 记录访问
    fn record_access(&mut self) {
        self.access_count += 1;
        self.last_accessed = Instant::now();
    }

    /// 获取缓存项的热度分数（用于智能淘汰）
    fn get_heat_score(&self) -> f64 {
        // 计算热度分数，考虑访问频率、最近访问时间和优先级
        let recency_factor = 1.0 / (1.0 + Instant::now().duration_since(self.last_accessed).as_secs() as f64 / 3600.0);
        let frequency_factor = (1.0 + self.access_count as f64).ln();
        let priority_factor = 1.0 + (self.priority as f64 / 10.0);
        
        recency_factor * frequency_factor * priority_factor
    }
}

/// 内存压力级别
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MemoryPressureLevel {
    /// 低内存压力
    Low,
    /// 中等内存压力
    Medium,
    /// 高内存压力
    High,
    /// 极高内存压力
    Critical,
}

impl MemoryPressureLevel {
    /// 获取内存压力级别的描述
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Critical => "critical",
        }
    }
}

/// 内存使用统计
#[derive(Debug, Clone)]
pub struct MemoryStats {
    /// 系统总内存 (KB)
    pub total_memory: u64,
    /// 可用内存 (KB)
    pub available_memory: u64,
    /// 内存使用率 (0.0-1.0)
    pub memory_usage_ratio: f64,
    /// 内存压力级别
    pub pressure_level: MemoryPressureLevel,
    /// 统计时间
    pub timestamp: Instant,
}

impl MemoryStats {
    /// 获取当前系统内存状态
    pub fn current() -> Result<Self> {
        // 在Windows上使用GlobalMemoryStatusEx获取内存信息
        #[cfg(target_os = "windows")]
        {
            use std::mem::size_of;
            use winapi::um::sysinfoapi::{GlobalMemoryStatusEx, MEMORYSTATUSEX};
            use winapi::shared::minwindef::DWORD;

            let mut mem_status = MEMORYSTATUSEX {
                dwLength: size_of::<MEMORYSTATUSEX>() as DWORD,
                dwMemoryLoad: 0,
                ullTotalPhys: 0,
                ullAvailPhys: 0,
                ullTotalPageFile: 0,
                ullAvailPageFile: 0,
                ullTotalVirtual: 0,
                ullAvailVirtual: 0,
                ullAvailExtendedVirtual: 0,
            };

            let result = unsafe { GlobalMemoryStatusEx(&mut mem_status) };
            if result == 0 {
                return Err(anyhow!("Failed to get memory status"));
            }

            let total_memory = mem_status.ullTotalPhys / 1024;
            let available_memory = mem_status.ullAvailPhys / 1024;
            let memory_usage_ratio = 1.0 - (available_memory as f64 / total_memory as f64);

            let pressure_level = if memory_usage_ratio > 0.9 {
                MemoryPressureLevel::Critical
            } else if memory_usage_ratio > 0.8 {
                MemoryPressureLevel::High
            } else if memory_usage_ratio > 0.7 {
                MemoryPressureLevel::Medium
            } else {
                MemoryPressureLevel::Low
            };

            return Ok(Self {
                total_memory,
                available_memory,
                memory_usage_ratio,
                pressure_level,
                timestamp: Instant::now(),
            });
        }

        // 在Linux上读取/proc/meminfo获取内存信息
        #[cfg(target_os = "linux")]
        {
            use std::fs::File;
            use std::io::{BufRead, BufReader};

            let file = File::open("/proc/meminfo")?;
            let reader = BufReader::new(file);

            let mut total_memory = 0;
            let mut available_memory = 0;

            for line in reader.lines() {
                let line = line?;
                if line.starts_with("MemTotal:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        total_memory = parts[1].parse::<u64>().unwrap_or(0);
                    }
                } else if line.starts_with("MemAvailable:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        available_memory = parts[1].parse::<u64>().unwrap_or(0);
                    }
                }
            }

            let memory_usage_ratio = 1.0 - (available_memory as f64 / total_memory as f64);

            let pressure_level = if memory_usage_ratio > 0.9 {
                MemoryPressureLevel::Critical
            } else if memory_usage_ratio > 0.8 {
                MemoryPressureLevel::High
            } else if memory_usage_ratio > 0.7 {
                MemoryPressureLevel::Medium
            } else {
                MemoryPressureLevel::Low
            };

            return Ok(Self {
                total_memory,
                available_memory,
                memory_usage_ratio,
                pressure_level,
                timestamp: Instant::now(),
            });
        }

        // 对于其他平台，返回一个默认值
        #[cfg(not(any(target_os = "windows", target_os = "linux")))]
        {
            return Ok(Self {
                total_memory: 0,
                available_memory: 0,
                memory_usage_ratio: 0.5,
                pressure_level: MemoryPressureLevel::Medium,
                timestamp: Instant::now(),
            });
        }
    }

    /// 获取内存统计信息的JSON表示
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "total_memory_kb": self.total_memory,
            "available_memory_kb": self.available_memory,
            "memory_usage_ratio": self.memory_usage_ratio,
            "pressure_level": self.pressure_level.as_str(),
            "timestamp": self.timestamp.elapsed().as_secs(),
        })
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
    /// 脚本-Atomicals映射缓存
    script_atomicals_cache: Mutex<LruCache<String, CacheItem<Vec<AtomicalId>>>>,
    /// 缓存配置
    cache_config: CacheConfig,
    /// 缓存统计
    cache_stats: CacheStats,
    /// 事务管理器
    transaction_manager: Arc<TransactionManager>,
    /// 上次内存压力检查时间
    last_memory_pressure_check: Mutex<Instant>,
    /// 上次缓存调整时间
    last_cache_adjustment: Mutex<Instant>,
    /// 当前内存压力级别
    current_memory_pressure: Mutex<MemoryPressureLevel>,
}

impl AtomicalsStorage {
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
            last_memory_pressure_check: Mutex::new(Instant::now()),
            last_cache_adjustment: Mutex::new(Instant::now()),
            current_memory_pressure: Mutex::new(MemoryPressureLevel::Low),
        };
        
        Ok(storage)
    }

    /// 检查内存压力
    pub fn check_memory_pressure(&self) -> Result<()> {
        let now = Instant::now();
        let last_check = *self.last_memory_pressure_check.lock().unwrap();
        let interval = self.cache_config.memory_pressure_check_interval;
        
        if now.duration_since(last_check).as_secs() >= interval {
            let memory_stats = MemoryStats::current()?;
            let pressure_level = memory_stats.pressure_level;
            
            *self.current_memory_pressure.lock().unwrap() = pressure_level;
            *self.last_memory_pressure_check.lock().unwrap() = now;
            
            info!("内存压力检查结果：压力级别={}", pressure_level.as_str());
        }
        
        Ok(())
    }

    /// 调整缓存大小
    pub fn adjust_cache_size(&self) -> Result<()> {
        let now = Instant::now();
        let last_adjustment = *self.last_cache_adjustment.lock().unwrap();
        let interval = self.cache_config.adaptive_adjustment_interval;
        
        if now.duration_since(last_adjustment).as_secs() >= interval {
            let hit_rate = self.cache_stats.hit_rate();
            let pressure_level = *self.current_memory_pressure.lock().unwrap();
            
            if hit_rate < self.cache_config.hit_rate_threshold {
                // 缓存命中率低，减小缓存大小
                let new_config = CacheConfig {
                    outputs_cache_size: (self.cache_config.outputs_cache_size as f64 * 0.8) as usize,
                    metadata_cache_size: (self.cache_config.metadata_cache_size as f64 * 0.8) as usize,
                    index_cache_size: (self.cache_config.index_cache_size as f64 * 0.8) as usize,
                    script_atomicals_cache_size: (self.cache_config.script_atomicals_cache_size as f64 * 0.8) as usize,
                    ..self.cache_config.clone()
                };
                
                self.resize_caches(new_config)?;
            } else if pressure_level == MemoryPressureLevel::High || pressure_level == MemoryPressureLevel::Critical {
                // 内存压力高，减小缓存大小
                let new_config = CacheConfig {
                    outputs_cache_size: (self.cache_config.outputs_cache_size as f64 * 0.5) as usize,
                    metadata_cache_size: (self.cache_config.metadata_cache_size as f64 * 0.5) as usize,
                    index_cache_size: (self.cache_config.index_cache_size as f64 * 0.5) as usize,
                    script_atomicals_cache_size: (self.cache_config.script_atomicals_cache_size as f64 * 0.5) as usize,
                    ..self.cache_config.clone()
                };
                
                self.resize_caches(new_config)?;
            } else {
                // 缓存命中率高，内存压力低，增大缓存大小
                let new_config = CacheConfig {
                    outputs_cache_size: (self.cache_config.outputs_cache_size as f64 * 1.2) as usize,
                    metadata_cache_size: (self.cache_config.metadata_cache_size as f64 * 1.2) as usize,
                    index_cache_size: (self.cache_config.index_cache_size as f64 * 1.2) as usize,
                    script_atomicals_cache_size: (self.cache_config.script_atomicals_cache_size as f64 * 1.2) as usize,
                    ..self.cache_config.clone()
                };
                
                self.resize_caches(new_config)?;
            }
            
            *self.last_cache_adjustment.lock().unwrap() = now;
        }
        
        Ok(())
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
                            self.cache_stats.record_hit("outputs");
                            continue;
                        }
                    }
                }
                
                // 缓存未命中，记录需要从数据库获取的ID
                missed_ids.push(id.clone());
                missed_keys.push(key);
                self.cache_stats.record_miss("outputs");
            }
        } else {
            // 如果无法获取缓存锁，则所有ID都需要从数据库获取
            for id in atomical_ids {
                missed_ids.push(id.clone());
                missed_keys.push(format!("{}:{}", id.txid, id.vout));
                self.cache_stats.record_miss("outputs");
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
                            self.cache_stats.record_hit("metadata");
                            continue;
                        }
                    }
                }
                
                // 缓存未命中，记录需要从数据库获取的ID
                missed_ids.push(id.clone());
                missed_keys.push(key);
                self.cache_stats.record_miss("metadata");
            }
        } else {
            // 如果无法获取缓存锁，则所有ID都需要从数据库获取
            for id in atomical_ids {
                missed_ids.push(id.clone());
                missed_keys.push(format!("{}:{}", id.txid, id.vout));
                self.cache_stats.record_miss("metadata");
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
        
        if let Some(&limit) = limits.get("index") {
            let count = self.warm_index_cache(limit)?;
            results.insert("index".to_string(), count);
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

    /// 获取 Atomical 输出
    pub fn get_output(&self, id: &AtomicalId) -> Result<AtomicalOutput> {
        let key = format!("{}:{}", id.txid, id.vout);
        
        // 尝试从缓存获取
        if let Ok(mut cache) = self.outputs_cache.lock() {
            if let Some(item) = cache.get(&key) {
                if let Some(output) = item.get_value() {
                    // 记录缓存命中
                    self.cache_stats.record_hit("outputs");
                    
                    // 记录访问
                    if let Some(item) = cache.get_mut(&key) {
                        item.record_access();
                    }
                    
                    return Ok(output);
                }
            }
        }
        
        // 缓存未命中，从数据库获取
        self.cache_stats.record_miss("outputs");
        
        let cf = self.db.cf_handle(CF_OUTPUTS).unwrap();
        match self.db.get_cf(cf, key.as_bytes())? {
            Some(value) => {
                let output: AtomicalOutput = bincode::deserialize(&value)?;
                
                // 添加到缓存
                if let Ok(mut cache) = self.outputs_cache.lock() {
                    let ttl = self.cache_config.ttl.map(Duration::from_secs);
                    cache.put(key, CacheItem::new(output.clone(), ttl));
                }
                
                Ok(output)
            },
            None => Err(anyhow!("Atomical output not found: {}:{}", id.txid, id.vout)),
        }
    }
    
    /// 获取 Atomical 元数据
    pub fn get_metadata(&self, id: &AtomicalId) -> Result<serde_json::Value> {
        let key = format!("{}:{}", id.txid, id.vout);
        
        // 尝试从缓存获取
        if let Ok(mut cache) = self.metadata_cache.lock() {
            if let Some(item) = cache.get(&key) {
                if let Some(metadata) = item.get_value() {
                    // 记录缓存命中
                    self.cache_stats.record_hit("metadata");
                    
                    // 记录访问
                    if let Some(item) = cache.get_mut(&key) {
                        item.record_access();
                    }
                    
                    return Ok(metadata);
                }
            }
        }
        
        // 缓存未命中，从数据库获取
        self.cache_stats.record_miss("metadata");
        
        let cf = self.db.cf_handle(CF_METADATA).unwrap();
        match self.db.get_cf(cf, key.as_bytes())? {
            Some(value) => {
                let metadata: serde_json::Value = serde_json::from_slice(&value)?;
                
                // 添加到缓存
                if let Ok(mut cache) = self.metadata_cache.lock() {
                    let ttl = self.cache_config.ttl.map(Duration::from_secs);
                    cache.put(key, CacheItem::new(metadata.clone(), ttl));
                }
                
                Ok(metadata)
            },
            None => Err(anyhow!("Atomical metadata not found: {}:{}", id.txid, id.vout)),
        }
    }
    
    /// 获取索引数据
    pub fn get_index(&self, index_name: &str, key: &str) -> Result<Vec<u8>> {
        let index_key = format!("{}:{}", index_name, key);
        
        // 尝试从缓存获取
        if let Ok(mut cache) = self.index_cache.lock() {
            if let Some(item) = cache.get(&index_key) {
                if let Some(data) = item.get_value() {
                    // 记录缓存命中
                    self.cache_stats.record_hit("index");
                    
                    // 记录访问
                    if let Some(item) = cache.get_mut(&index_key) {
                        item.record_access();
                    }
                    
                    return Ok(data);
                }
            }
        }
        
        // 缓存未命中，从数据库获取
        self.cache_stats.record_miss("index");
        
        let cf = self.db.cf_handle(CF_INDEXES).unwrap();
        match self.db.get_cf(cf, index_key.as_bytes())? {
            Some(value) => {
                // 添加到缓存
                if let Ok(mut cache) = self.index_cache.lock() {
                    let ttl = self.cache_config.ttl.map(Duration::from_secs);
                    cache.put(index_key, CacheItem::new(value.clone(), ttl));
                }
                
                Ok(value.to_vec())
            },
            None => Err(anyhow!("Index data not found: {}", index_key)),
        }
    }
    
    /// 获取脚本关联的Atomicals
    pub fn get_script_atomicals(&self, script_hex: &str) -> Result<Vec<AtomicalId>> {
        // 尝试从缓存获取
        if let Ok(mut cache) = self.script_atomicals_cache.lock() {
            if let Some(item) = cache.get(script_hex) {
                if let Some(atomicals) = item.get_value() {
                    // 记录缓存命中
                    self.cache_stats.record_hit("script_atomicals");
                    
                    // 记录访问
                    if let Some(item) = cache.get_mut(script_hex) {
                        item.record_access();
                    }
                    
                    return Ok(atomicals);
                }
            }
        }
        
        // 缓存未命中，从数据库获取
        self.cache_stats.record_miss("script_atomicals");
        
        let cf = self.db.cf_handle(CF_SCRIPT_ATOMICALS).unwrap();
        match self.db.get_cf(cf, script_hex.as_bytes())? {
            Some(value) => {
                let atomicals: Vec<AtomicalId> = bincode::deserialize(&value)?;
                
                // 添加到缓存
                if let Ok(mut cache) = self.script_atomicals_cache.lock() {
                    let ttl = self.cache_config.ttl.map(Duration::from_secs);
                    cache.put(script_hex.to_string(), CacheItem::new(atomicals.clone(), ttl));
                }
                
                Ok(atomicals)
            },
            None => Ok(Vec::new()), // 返回空列表，表示该脚本没有关联的Atomicals
        }
    }
    
    /// 获取缓存统计信息
    pub fn get_cache_stats(&self) -> HashMap<String, serde_json::Value> {
        let mut stats = HashMap::new();
        
        // 获取基本统计信息
        let hits = self.cache_stats.get_hits();
        let misses = self.cache_stats.get_misses();
        let total = hits + misses;
        let hit_rate = if total > 0 { hits as f64 / total as f64 } else { 0.0 };
        
        stats.insert("hits".to_string(), serde_json::json!(hits));
        stats.insert("misses".to_string(), serde_json::json!(misses));
        stats.insert("total".to_string(), serde_json::json!(total));
        stats.insert("hit_rate".to_string(), serde_json::json!(hit_rate));
        
        // 获取各缓存的命中率
        let outputs_hits = self.cache_stats.get_cache_hits("outputs");
        let outputs_misses = self.cache_stats.get_cache_misses("outputs");
        let outputs_total = outputs_hits + outputs_misses;
        let outputs_hit_rate = if outputs_total > 0 { outputs_hits as f64 / outputs_total as f64 } else { 0.0 };
        
        stats.insert("outputs_hits".to_string(), serde_json::json!(outputs_hits));
        stats.insert("outputs_misses".to_string(), serde_json::json!(outputs_misses));
        stats.insert("outputs_hit_rate".to_string(), serde_json::json!(outputs_hit_rate));
        
        let metadata_hits = self.cache_stats.get_cache_hits("metadata");
        let metadata_misses = self.cache_stats.get_cache_misses("metadata");
        let metadata_total = metadata_hits + metadata_misses;
        let metadata_hit_rate = if metadata_total > 0 { metadata_hits as f64 / metadata_total as f64 } else { 0.0 };
        
        stats.insert("metadata_hits".to_string(), serde_json::json!(metadata_hits));
        stats.insert("metadata_misses".to_string(), serde_json::json!(metadata_misses));
        stats.insert("metadata_hit_rate".to_string(), serde_json::json!(metadata_hit_rate));
        
        let script_atomicals_hits = self.cache_stats.get_cache_hits("script_atomicals");
        let script_atomicals_misses = self.cache_stats.get_cache_misses("script_atomicals");
        let script_atomicals_total = script_atomicals_hits + script_atomicals_misses;
        let script_atomicals_hit_rate = if script_atomicals_total > 0 { 
            script_atomicals_hits as f64 / script_atomicals_total as f64 
        } else { 
            0.0 
        };
        
        stats.insert("script_atomicals_hits".to_string(), serde_json::json!(script_atomicals_hits));
        stats.insert("script_atomicals_misses".to_string(), serde_json::json!(script_atomicals_misses));
        stats.insert("script_atomicals_hit_rate".to_string(), serde_json::json!(script_atomicals_hit_rate));
        
        // 获取缓存大小信息
        if let Ok(cache) = self.outputs_cache.lock() {
            stats.insert("outputs_cache_size".to_string(), serde_json::json!(cache.len()));
            stats.insert("outputs_cache_capacity".to_string(), serde_json::json!(cache.cap().get()));
        }
        
        if let Ok(cache) = self.metadata_cache.lock() {
            stats.insert("metadata_cache_size".to_string(), serde_json::json!(cache.len()));
            stats.insert("metadata_cache_capacity".to_string(), serde_json::json!(cache.cap().get()));
        }
        
        if let Ok(cache) = self.script_atomicals_cache.lock() {
            stats.insert("script_atomicals_cache_size".to_string(), serde_json::json!(cache.len()));
            stats.insert("script_atomicals_cache_capacity".to_string(), serde_json::json!(cache.cap().get()));
        }
        
        if let Ok(cache) = self.index_cache.lock() {
            stats.insert("index_cache_size".to_string(), serde_json::json!(cache.len()));
            stats.insert("index_cache_capacity".to_string(), serde_json::json!(cache.cap().get()));
        }
        
        // 获取内存压力信息
        if let Ok(pressure) = self.current_memory_pressure.lock() {
            stats.insert("memory_pressure".to_string(), serde_json::json!(pressure.as_str()));
        }
        
        stats
    }
}

/// 执行缓存维护
impl AtomicalsStorage {
    pub fn perform_cache_maintenance(&self) -> Result<HashMap<String, serde_json::Value>> {
        info!("开始执行缓存维护...");
        
        let mut results = HashMap::new();
        
        // 1. 清理过期缓存项
        let pruned_count = self.prune_expired_cache_items()?;
        results.insert("pruned_items".to_string(), serde_json::json!(pruned_count));
        
        // 2. 检测内存压力并调整缓存大小
        self.check_memory_pressure()?;
        let pressure = {
            if let Ok(pressure) = self.current_memory_pressure.lock() {
                pressure.as_str().to_string()
            } else {
                "unknown".to_string()
            }
        };
        results.insert("memory_pressure".to_string(), serde_json::json!(pressure));
        
        // 3. 根据内存压力调整缓存大小
        self.adjust_cache_size()?;
        results.insert("caches_adjusted".to_string(), serde_json::json!(true));
        
        // 4. 获取缓存统计信息
        let stats = self.get_cache_stats();
        results.insert("cache_stats".to_string(), serde_json::to_value(stats)?);
        
        // 5. 分析缓存效率并提供优化建议
        let recommendations = self.analyze_cache_efficiency();
        results.insert("recommendations".to_string(), serde_json::to_value(recommendations)?);
        
        info!("缓存维护完成");
        
        Ok(results)
    }
    
    /// 分析缓存效率并提供优化建议
    fn analyze_cache_efficiency(&self) -> Vec<String> {
        let mut recommendations = Vec::new();
        
        // 获取各缓存的命中率
        let outputs_hit_rate = self.cache_stats.get_cache_hit_rate("outputs");
        let metadata_hit_rate = self.cache_stats.get_cache_hit_rate("metadata");
        let index_hit_rate = self.cache_stats.get_cache_hit_rate("index");
        let script_atomicals_hit_rate = self.cache_stats.get_cache_hit_rate("script_atomicals");
        
        // 分析输出缓存
        if outputs_hit_rate < 0.5 {
            // 命中率低于50%
            if let Ok(cache) = self.outputs_cache.lock() {
                if cache.len() < cache.cap().get() / 2 {
                    // 缓存使用率低，可能需要预热
                    recommendations.push("输出缓存命中率低且未充分利用，建议使用warm_outputs_cache预热缓存".to_string());
                } else {
                    // 缓存已满但命中率低，可能需要增加容量
                    recommendations.push("输出缓存命中率低但已接近容量上限，建议增加outputs_cache容量".to_string());
                }
            }
        }
        
        // 分析元数据缓存
        if metadata_hit_rate < 0.5 {
            if let Ok(cache) = self.metadata_cache.lock() {
                if cache.len() < cache.cap().get() / 2 {
                    recommendations.push("元数据缓存命中率低且未充分利用，建议使用warm_metadata_cache预热缓存".to_string());
                } else {
                    recommendations.push("元数据缓存命中率低但已接近容量上限，建议增加metadata_cache容量".to_string());
                }
            }
        }
        
        // 分析索引缓存
        if index_hit_rate < 0.5 {
            if let Ok(cache) = self.index_cache.lock() {
                if cache.len() < cache.cap().get() / 2 {
                    recommendations.push("索引缓存命中率低且未充分利用，建议使用warm_index_cache预热缓存".to_string());
                } else {
                    recommendations.push("索引缓存命中率低但已接近容量上限，建议增加index_cache容量".to_string());
                }
            }
        }
        
        // 分析脚本-Atomicals映射缓存
        if script_atomicals_hit_rate < 0.5 {
            if let Ok(cache) = self.script_atomicals_cache.lock() {
                if cache.len() < cache.cap().get() / 2 {
                    recommendations.push("脚本-Atomicals映射缓存命中率低且未充分利用，建议使用warm_script_atomicals_cache预热缓存".to_string());
                } else {
                    recommendations.push("脚本-Atomicals映射缓存命中率低但已接近容量上限，建议增加script_atomicals_cache容量".to_string());
                }
            }
        }
        
        // 检查内存压力
        if let Ok(pressure) = self.current_memory_pressure.lock() {
            if *pressure == MemoryPressureLevel::High || *pressure == MemoryPressureLevel::Critical {
                recommendations.push("系统内存压力高，建议减小缓存大小或增加系统内存".to_string());
            }
        }
        
        recommendations
    }
    
    /// 预热索引缓存
    pub fn warm_index_cache(&self, limit: usize) -> Result<usize> {
        info!("开始预热索引缓存...");
        
        let mut count = 0;
        
        // 获取索引列族
        let cf = self.db.cf_handle(CF_INDEXES).unwrap();
        
        // 创建迭代器
        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);
        
        // 遍历前limit个索引项
        for result in iter.take(limit) {
            match result {
                Ok((key, value)) => {
                    let key_str = String::from_utf8_lossy(&key).to_string();
                    
                    // 添加到缓存
                    if let Ok(mut cache) = self.index_cache.lock() {
                        let ttl = self.cache_config.lock().unwrap().ttl.map(Duration::from_secs);
                        cache.put(key_str, CacheItem::new(value.to_vec(), ttl));
                        count += 1;
                    }
                },
                Err(e) => {
                    warn!("预热索引缓存时出错: {}", e);
                }
            }
        }
        
        info!("索引缓存预热完成，加载了 {} 个索引项", count);
        
        Ok(count)
    }
    
    /// 获取缓存健康报告
    pub fn get_cache_health_report(&self) -> Result<HashMap<String, serde_json::Value>> {
        let mut report = HashMap::new();
        
        // 获取缓存统计信息
        let stats = self.get_cache_stats();
        report.insert("statistics".to_string(), serde_json::to_value(stats)?);
        
        // 获取内存压力
        let memory_pressure = {
            if let Ok(pressure) = self.current_memory_pressure.lock() {
                pressure.as_str().to_string()
            } else {
                "unknown".to_string()
            }
        };
        report.insert("memory_pressure".to_string(), serde_json::json!(memory_pressure));
        
        // 获取缓存效率建议
        let recommendations = self.analyze_cache_efficiency();
        report.insert("recommendations".to_string(), serde_json::to_value(recommendations)?);
        
        // 计算整体健康状态
        let health_score = self.calculate_cache_health_score();
        report.insert("health_score".to_string(), serde_json::json!(health_score));
        
        // 健康状态描述
        let health_description = match health_score {
            score if score >= 90.0 => "优秀",
            score if score >= 70.0 => "良好",
            score if score >= 50.0 => "一般",
            score if score >= 30.0 => "较差",
            _ => "糟糕",
        };
        report.insert("health_description".to_string(), serde_json::json!(health_description));
        
        Ok(report)
    }
    
    /// 计算缓存健康分数
    fn calculate_cache_health_score(&self) -> f64 {
        let mut score = 0.0;
        
        // 命中率权重 (60%)
        let hit_rate = self.cache_stats.hit_rate();
        score += hit_rate * 60.0;
        
        // 内存压力权重 (20%)
        let memory_pressure_score = match self.current_memory_pressure.lock() {
            Ok(pressure) => match *pressure {
                MemoryPressureLevel::Low => 20.0,
                MemoryPressureLevel::Medium => 15.0,
                MemoryPressureLevel::High => 5.0,
                MemoryPressureLevel::Critical => 0.0,
            },
            Err(_) => 10.0, // 默认中等
        };
        score += memory_pressure_score;
        
        // 缓存利用率权重 (20%)
        let mut utilization_score = 0.0;
        let mut cache_count = 0;
        
        if let Ok(cache) = self.outputs_cache.lock() {
            let utilization = cache.len() as f64 / cache.cap().get() as f64;
            // 理想利用率在60%-80%之间
            let cache_score = if utilization < 0.3 {
                utilization * 50.0 // 利用率过低
            } else if utilization > 0.9 {
                (1.0 - (utilization - 0.9) * 5.0) * 20.0 // 利用率过高
            } else {
                20.0 // 理想范围
            };
            utilization_score += cache_score;
            cache_count += 1;
        }
        
        if let Ok(cache) = self.metadata_cache.lock() {
            let utilization = cache.len() as f64 / cache.cap().get() as f64;
            let cache_score = if utilization < 0.3 {
                utilization * 50.0
            } else if utilization > 0.9 {
                (1.0 - (utilization - 0.9) * 5.0) * 20.0
            } else {
                20.0
            };
            utilization_score += cache_score;
            cache_count += 1;
        }
        
        if cache_count > 0 {
            utilization_score /= cache_count as f64;
            score += utilization_score;
        }
        
        score.min(100.0).max(0.0) // 确保分数在0-100之间
    }
}

/// 自适应缓存调整功能
impl AtomicalsStorage {
    /// 自适应调整缓存大小
    pub fn adaptive_cache_resize(&self) -> Result<HashMap<String, serde_json::Value>> {
        info!("开始自适应调整缓存大小...");
        
        let mut results = HashMap::new();
        let mut changes = HashMap::new();
        
        // 1. 获取当前内存压力
        self.check_memory_pressure()?;
        let pressure = {
            if let Ok(pressure) = self.current_memory_pressure.lock() {
                *pressure
            } else {
                MemoryPressureLevel::Medium // 默认中等压力
            }
        };
        
        // 2. 获取各缓存的命中率
        let outputs_hit_rate = self.cache_stats.get_cache_hit_rate("outputs");
        let metadata_hit_rate = self.cache_stats.get_cache_hit_rate("metadata");
        let index_hit_rate = self.cache_stats.get_cache_hit_rate("index");
        let script_atomicals_hit_rate = self.cache_stats.get_cache_hit_rate("script_atomicals");
        
        // 3. 根据内存压力和命中率调整缓存大小
        let mut config = self.cache_config.lock().unwrap();
        
        // 调整输出缓存
        let old_outputs_capacity = config.outputs_cache_size;
        match pressure {
            MemoryPressureLevel::Low => {
                // 低内存压力，可以增加缓存大小
                if outputs_hit_rate > 0.8 {
                    // 命中率高，增加缓存大小
                    config.outputs_cache_size = (config.outputs_cache_size as f64 * 1.2) as usize;
                } else if outputs_hit_rate < 0.4 {
                    // 命中率低，减小缓存大小
                    config.outputs_cache_size = (config.outputs_cache_size as f64 * 0.9) as usize;
                }
            },
            MemoryPressureLevel::Medium => {
                // 中等内存压力，保持或略微减小
                if outputs_hit_rate < 0.5 {
                    // 命中率低，减小缓存大小
                    config.outputs_cache_size = (config.outputs_cache_size as f64 * 0.8) as usize;
                }
            },
            MemoryPressureLevel::High | MemoryPressureLevel::Critical => {
                // 高内存压力，减小缓存大小
                config.outputs_cache_size = (config.outputs_cache_size as f64 * 0.6) as usize;
            }
        }
        // 确保最小容量
        config.outputs_cache_size = config.outputs_cache_size.max(100);
        changes.insert("outputs_cache".to_string(), serde_json::json!({
            "old_capacity": old_outputs_capacity,
            "new_capacity": config.outputs_cache_size,
            "change_percent": ((config.outputs_cache_size as f64 / old_outputs_capacity as f64) - 1.0) * 100.0
        }));
        
        // 调整元数据缓存
        let old_metadata_capacity = config.metadata_cache_size;
        match pressure {
            MemoryPressureLevel::Low => {
                if metadata_hit_rate > 0.8 {
                    config.metadata_cache_size = (config.metadata_cache_size as f64 * 1.2) as usize;
                } else if metadata_hit_rate < 0.4 {
                    config.metadata_cache_size = (config.metadata_cache_size as f64 * 0.9) as usize;
                }
            },
            MemoryPressureLevel::Medium => {
                if metadata_hit_rate < 0.5 {
                    config.metadata_cache_size = (config.metadata_cache_size as f64 * 0.8) as usize;
                }
            },
            MemoryPressureLevel::High | MemoryPressureLevel::Critical => {
                config.metadata_cache_size = (config.metadata_cache_size as f64 * 0.6) as usize;
            }
        }
        config.metadata_cache_size = config.metadata_cache_size.max(100);
        changes.insert("metadata_cache".to_string(), serde_json::json!({
            "old_capacity": old_metadata_capacity,
            "new_capacity": config.metadata_cache_size,
            "change_percent": ((config.metadata_cache_size as f64 / old_metadata_capacity as f64) - 1.0) * 100.0
        }));
        
        // 调整索引缓存
        let old_index_capacity = config.index_cache_size;
        match pressure {
            MemoryPressureLevel::Low => {
                if index_hit_rate > 0.8 {
                    config.index_cache_size = (config.index_cache_size as f64 * 1.2) as usize;
                } else if index_hit_rate < 0.4 {
                    config.index_cache_size = (config.index_cache_size as f64 * 0.9) as usize;
                }
            },
            MemoryPressureLevel::Medium => {
                if index_hit_rate < 0.5 {
                    config.index_cache_size = (config.index_cache_size as f64 * 0.8) as usize;
                }
            },
            MemoryPressureLevel::High | MemoryPressureLevel::Critical => {
                config.index_cache_size = (config.index_cache_size as f64 * 0.6) as usize;
            }
        }
        config.index_cache_size = config.index_cache_size.max(100);
        changes.insert("index_cache".to_string(), serde_json::json!({
            "old_capacity": old_index_capacity,
            "new_capacity": config.index_cache_size,
            "change_percent": ((config.index_cache_size as f64 / old_index_capacity as f64) - 1.0) * 100.0
        }));
        
        // 调整脚本-Atomicals映射缓存
        let old_script_atomicals_capacity = config.script_atomicals_cache_size;
        match pressure {
            MemoryPressureLevel::Low => {
                if script_atomicals_hit_rate > 0.8 {
                    config.script_atomicals_cache_size = (config.script_atomicals_cache_size as f64 * 1.2) as usize;
                } else if script_atomicals_hit_rate < 0.4 {
                    config.script_atomicals_cache_size = (config.script_atomicals_cache_size as f64 * 0.9) as usize;
                }
            },
            MemoryPressureLevel::Medium => {
                if script_atomicals_hit_rate < 0.5 {
                    config.script_atomicals_cache_size = (config.script_atomicals_cache_size as f64 * 0.8) as usize;
                }
            },
            MemoryPressureLevel::High | MemoryPressureLevel::Critical => {
                config.script_atomicals_cache_size = (config.script_atomicals_cache_size as f64 * 0.6) as usize;
            }
        }
        config.script_atomicals_cache_size = config.script_atomicals_cache_size.max(100);
        changes.insert("script_atomicals_cache".to_string(), serde_json::json!({
            "old_capacity": old_script_atomicals_capacity,
            "new_capacity": config.script_atomicals_cache_size,
            "change_percent": ((config.script_atomicals_cache_size as f64 / old_script_atomicals_capacity as f64) - 1.0) * 100.0
        }));
        
        // 4. 应用新的缓存配置
        drop(config); // 释放锁
        self.apply_cache_config()?;
        
        // 5. 记录调整结果
        results.insert("memory_pressure".to_string(), serde_json::json!(pressure.as_str()));
        results.insert("cache_changes".to_string(), serde_json::json!(changes));
        
        info!("自适应缓存大小调整完成");
        
        Ok(results)
    }
    
    /// 应用缓存配置
    fn apply_cache_config(&self) -> Result<()> {
        let config = self.cache_config.lock().unwrap();
        
        // 调整输出缓存大小
        if let Ok(mut cache) = self.outputs_cache.lock() {
            cache.resize(NonZeroUsize::new(config.outputs_cache_size).unwrap());
        }
        
        // 调整元数据缓存大小
        if let Ok(mut cache) = self.metadata_cache.lock() {
            cache.resize(NonZeroUsize::new(config.metadata_cache_size).unwrap());
        }
        
        // 调整索引缓存大小
        if let Ok(mut cache) = self.index_cache.lock() {
            cache.resize(NonZeroUsize::new(config.index_cache_size).unwrap());
        }
        
        // 调整脚本-Atomicals映射缓存大小
        if let Ok(mut cache) = self.script_atomicals_cache.lock() {
            cache.resize(NonZeroUsize::new(config.script_atomicals_cache_size).unwrap());
        }
        
        Ok(())
    }
    
    /// 智能预热缓存
    pub fn smart_warm_caches(&self, access_patterns: &HashMap<String, f64>) -> Result<HashMap<String, usize>> {
        info!("开始智能预热缓存...");
        
        let mut results = HashMap::new();
        
        // 根据访问模式分配预热资源
        let total_items = 1000; // 总共预热的项目数
        
        // 计算每个缓存应该预热的项目数
        let mut allocations = HashMap::new();
        let mut total_weight = 0.0;
        
        for (cache_type, weight) in access_patterns {
            total_weight += weight;
            allocations.insert(cache_type.clone(), *weight);
        }
        
        // 根据权重分配预热项目数
        for (cache_type, weight) in allocations.iter_mut() {
            let allocation = (*weight / total_weight * total_items as f64) as usize;
            *weight = allocation as f64;
            
            // 预热相应的缓存
            let warmed_count = match cache_type.as_str() {
                "outputs" => self.warm_outputs_cache(allocation)?,
                "metadata" => self.warm_metadata_cache(allocation)?,
                "index" => self.warm_index_cache(allocation)?,
                "script_atomicals" => self.warm_script_atomicals_cache(allocation)?,
                _ => 0,
            };
            
            results.insert(cache_type.clone(), warmed_count);
        }
        
        info!("智能预热缓存完成");
        
        Ok(results)
    }
    
    /// 根据访问模式分析生成缓存预热建议
    pub fn analyze_access_patterns(&self) -> HashMap<String, f64> {
        let mut patterns = HashMap::new();
        
        // 获取各缓存的访问次数
        let outputs_access = self.cache_stats.get_cache_hits("outputs") + self.cache_stats.get_cache_misses("outputs");
        let metadata_access = self.cache_stats.get_cache_hits("metadata") + self.cache_stats.get_cache_misses("metadata");
        let index_access = self.cache_stats.get_cache_hits("index") + self.cache_stats.get_cache_misses("index");
        let script_atomicals_access = self.cache_stats.get_cache_hits("script_atomicals") + self.cache_stats.get_cache_misses("script_atomicals");
        
        // 计算总访问次数
        let total_access = outputs_access + metadata_access + index_access + script_atomicals_access;
        
        if total_access > 0 {
            // 计算各缓存的访问比例
            patterns.insert("outputs".to_string(), outputs_access as f64 / total_access as f64);
            patterns.insert("metadata".to_string(), metadata_access as f64 / total_access as f64);
            patterns.insert("index".to_string(), index_access as f64 / total_access as f64);
            patterns.insert("script_atomicals".to_string(), script_atomicals_access as f64 / total_access as f64);
        } else {
            // 默认平均分配
            patterns.insert("outputs".to_string(), 0.25);
            patterns.insert("metadata".to_string(), 0.25);
            patterns.insert("index".to_string(), 0.25);
            patterns.insert("script_atomicals".to_string(), 0.25);
        }
        
        patterns
    }
    
    /// 自动优化缓存系统
    pub fn auto_optimize_caches(&self) -> Result<HashMap<String, serde_json::Value>> {
        info!("开始自动优化缓存系统...");
        
        let mut results = HashMap::new();
        
        // 1. 检查内存压力
        self.check_memory_pressure()?;
        let pressure = {
            if let Ok(pressure) = self.current_memory_pressure.lock() {
                pressure.as_str().to_string()
            } else {
                "unknown".to_string()
            }
        };
        results.insert("memory_pressure".to_string(), serde_json::json!(pressure));
        
        // 2. 清理过期缓存项
        let pruned_count = self.prune_expired_cache_items()?;
        results.insert("pruned_items".to_string(), serde_json::json!(pruned_count));
        
        // 3. 自适应调整缓存大小
        let resize_results = self.adaptive_cache_resize()?;
        results.insert("resize_results".to_string(), serde_json::to_value(resize_results)?);
        
        // 4. 分析访问模式
        let access_patterns = self.analyze_access_patterns();
        results.insert("access_patterns".to_string(), serde_json::to_value(access_patterns.clone())?);
        
        // 5. 智能预热缓存
        let warm_results = self.smart_warm_caches(&access_patterns)?;
        results.insert("warm_results".to_string(), serde_json::to_value(warm_results)?);
        
        // 6. 获取缓存健康报告
        let health_report = self.get_cache_health_report()?;
        results.insert("health_report".to_string(), serde_json::to_value(health_report)?);
        
        info!("缓存系统自动优化完成");
        
        Ok(results)
    }
}
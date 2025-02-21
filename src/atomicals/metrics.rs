use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use serde::Serialize;
use tokio::sync::RwLock;
use std::collections::HashMap;

/// WebSocket 连接指标
#[derive(Debug, Default, Clone, Serialize)]
pub struct ConnectionMetrics {
    /// 当前连接数
    pub current_connections: usize,
    /// 历史最大连接数
    pub max_connections: usize,
    /// 总连接数
    pub total_connections: u64,
    /// 认证成功数
    pub auth_success: u64,
    /// 认证失败数
    pub auth_failures: u64,
}

/// 消息指标
#[derive(Debug, Default, Clone, Serialize)]
pub struct MessageMetrics {
    /// 发送消息数
    pub messages_sent: u64,
    /// 接收消息数
    pub messages_received: u64,
    /// 压缩消息数
    pub messages_compressed: u64,
    /// 解压消息数
    pub messages_decompressed: u64,
    /// 压缩字节数
    pub bytes_compressed: u64,
    /// 原始字节数
    pub bytes_original: u64,
}

/// 订阅指标
#[derive(Debug, Default, Clone, Serialize)]
pub struct SubscriptionMetrics {
    /// 当前订阅数
    pub current_subscriptions: usize,
    /// 历史最大订阅数
    pub max_subscriptions: usize,
    /// 总订阅数
    pub total_subscriptions: u64,
}

/// 错误指标
#[derive(Debug, Default, Clone, Serialize)]
pub struct ErrorMetrics {
    /// 连接错误数
    pub connection_errors: u64,
    /// 消息错误数
    pub message_errors: u64,
    /// 认证错误数
    pub auth_errors: u64,
    /// 压缩错误数
    pub compression_errors: u64,
}

/// 延迟指标
#[derive(Debug, Clone, Serialize)]
pub struct LatencyMetrics {
    /// 消息处理延迟（毫秒）
    pub message_processing: Vec<u64>,
    /// 压缩延迟（毫秒）
    pub compression: Vec<u64>,
    /// 认证延迟（毫秒）
    pub authentication: Vec<u64>,
}

impl Default for LatencyMetrics {
    fn default() -> Self {
        Self {
            message_processing: Vec::with_capacity(100),
            compression: Vec::with_capacity(100),
            authentication: Vec::with_capacity(100),
        }
    }
}

/// IP 统计
#[derive(Debug, Default, Clone, Serialize)]
pub struct IpStats {
    /// 连接次数
    pub connection_count: u64,
    /// 认证失败次数
    pub auth_failures: u64,
    /// 最后连接时间
    pub last_connection: u64,
}

/// WebSocket 监控系统
#[derive(Debug)]
pub struct WebSocketMetrics {
    /// 连接指标
    connection: Arc<ConnectionMetricsInner>,
    /// 消息指标
    message: Arc<MessageMetricsInner>,
    /// 订阅指标
    subscription: Arc<SubscriptionMetricsInner>,
    /// 错误指标
    error: Arc<ErrorMetricsInner>,
    /// 延迟指标
    latency: Arc<RwLock<LatencyMetrics>>,
    /// IP 统计
    ip_stats: Arc<RwLock<HashMap<String, IpStats>>>,
}

#[derive(Debug)]
struct ConnectionMetricsInner {
    current: AtomicUsize,
    max: AtomicUsize,
    total: AtomicU64,
    auth_success: AtomicU64,
    auth_failures: AtomicU64,
}

#[derive(Debug)]
struct MessageMetricsInner {
    sent: AtomicU64,
    received: AtomicU64,
    compressed: AtomicU64,
    decompressed: AtomicU64,
    bytes_compressed: AtomicU64,
    bytes_original: AtomicU64,
}

#[derive(Debug)]
struct SubscriptionMetricsInner {
    current: AtomicUsize,
    max: AtomicUsize,
    total: AtomicU64,
}

#[derive(Debug)]
struct ErrorMetricsInner {
    connection: AtomicU64,
    message: AtomicU64,
    auth: AtomicU64,
    compression: AtomicU64,
}

impl WebSocketMetrics {
    /// 创建新的监控系统
    pub fn new() -> Self {
        Self {
            connection: Arc::new(ConnectionMetricsInner {
                current: AtomicUsize::new(0),
                max: AtomicUsize::new(0),
                total: AtomicU64::new(0),
                auth_success: AtomicU64::new(0),
                auth_failures: AtomicU64::new(0),
            }),
            message: Arc::new(MessageMetricsInner {
                sent: AtomicU64::new(0),
                received: AtomicU64::new(0),
                compressed: AtomicU64::new(0),
                decompressed: AtomicU64::new(0),
                bytes_compressed: AtomicU64::new(0),
                bytes_original: AtomicU64::new(0),
            }),
            subscription: Arc::new(SubscriptionMetricsInner {
                current: AtomicUsize::new(0),
                max: AtomicUsize::new(0),
                total: AtomicU64::new(0),
            }),
            error: Arc::new(ErrorMetricsInner {
                connection: AtomicU64::new(0),
                message: AtomicU64::new(0),
                auth: AtomicU64::new(0),
                compression: AtomicU64::new(0),
            }),
            latency: Arc::new(RwLock::new(LatencyMetrics::default())),
            ip_stats: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 记录新连接
    pub async fn record_connection(&self, ip: &str) {
        self.connection.current.fetch_add(1, Ordering::Relaxed);
        self.connection.total.fetch_add(1, Ordering::Relaxed);
        
        let current = self.connection.current.load(Ordering::Relaxed);
        let mut max = self.connection.max.load(Ordering::Relaxed);
        while current > max {
            match self.connection.max.compare_exchange_weak(
                max,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => max = x,
            }
        }

        // 更新 IP 统计
        let mut ip_stats = self.ip_stats.write().await;
        let stats = ip_stats.entry(ip.to_string()).or_default();
        stats.connection_count += 1;
        stats.last_connection = chrono::Utc::now().timestamp() as u64;
    }

    /// 记录连接断开
    pub fn record_disconnection(&self) {
        self.connection.current.fetch_sub(1, Ordering::Relaxed);
    }

    /// 记录认证成功
    pub fn record_auth_success(&self) {
        self.connection.auth_success.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录认证失败
    pub async fn record_auth_failure(&self, ip: &str) {
        self.connection.auth_failures.fetch_add(1, Ordering::Relaxed);
        
        // 更新 IP 统计
        let mut ip_stats = self.ip_stats.write().await;
        let stats = ip_stats.entry(ip.to_string()).or_default();
        stats.auth_failures += 1;
    }

    /// 记录消息发送
    pub fn record_message_sent(&self, size: usize) {
        self.message.sent.fetch_add(1, Ordering::Relaxed);
        self.message.bytes_original.fetch_add(size as u64, Ordering::Relaxed);
    }

    /// 记录消息接收
    pub fn record_message_received(&self, size: usize) {
        self.message.received.fetch_add(1, Ordering::Relaxed);
        self.message.bytes_original.fetch_add(size as u64, Ordering::Relaxed);
    }

    /// 记录消息压缩
    pub fn record_compression(&self, _original_size: usize, compressed_size: usize) {
        self.message.compressed.fetch_add(1, Ordering::Relaxed);
        self.message.bytes_compressed.fetch_add(compressed_size as u64, Ordering::Relaxed);
    }

    /// 记录消息解压
    pub fn record_decompression(&self, _compressed_size: usize, original_size: usize) {
        self.message.decompressed.fetch_add(1, Ordering::Relaxed);
        self.message.bytes_original.fetch_add(original_size as u64, Ordering::Relaxed);
    }

    /// 记录订阅
    pub fn record_subscription(&self) {
        self.subscription.current.fetch_add(1, Ordering::Relaxed);
        self.subscription.total.fetch_add(1, Ordering::Relaxed);
        
        let current = self.subscription.current.load(Ordering::Relaxed);
        let mut max = self.subscription.max.load(Ordering::Relaxed);
        while current > max {
            match self.subscription.max.compare_exchange_weak(
                max,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => max = x,
            }
        }
    }

    /// 记录取消订阅
    pub fn record_unsubscription(&self) {
        self.subscription.current.fetch_sub(1, Ordering::Relaxed);
    }

    /// 记录错误
    pub fn record_error(&self, error_type: ErrorType) {
        match error_type {
            ErrorType::Connection => self.error.connection.fetch_add(1, Ordering::Relaxed),
            ErrorType::Message => self.error.message.fetch_add(1, Ordering::Relaxed),
            ErrorType::Auth => self.error.auth.fetch_add(1, Ordering::Relaxed),
            ErrorType::Compression => self.error.compression.fetch_add(1, Ordering::Relaxed),
        };
    }

    /// 记录延迟
    pub async fn record_latency(&self, latency_type: LatencyType, duration: Duration) {
        let mut metrics = self.latency.write().await;
        let ms = duration.as_millis() as u64;
        
        match latency_type {
            LatencyType::MessageProcessing => {
                if metrics.message_processing.len() >= 100 {
                    metrics.message_processing.remove(0);
                }
                metrics.message_processing.push(ms);
            }
            LatencyType::Compression => {
                if metrics.compression.len() >= 100 {
                    metrics.compression.remove(0);
                }
                metrics.compression.push(ms);
            }
            LatencyType::Authentication => {
                if metrics.authentication.len() >= 100 {
                    metrics.authentication.remove(0);
                }
                metrics.authentication.push(ms);
            }
        }
    }

    /// 获取所有指标
    pub async fn get_metrics(&self) -> WebSocketMetricsSnapshot {
        WebSocketMetricsSnapshot {
            connection: ConnectionMetrics {
                current_connections: self.connection.current.load(Ordering::Relaxed),
                max_connections: self.connection.max.load(Ordering::Relaxed),
                total_connections: self.connection.total.load(Ordering::Relaxed),
                auth_success: self.connection.auth_success.load(Ordering::Relaxed),
                auth_failures: self.connection.auth_failures.load(Ordering::Relaxed),
            },
            message: MessageMetrics {
                messages_sent: self.message.sent.load(Ordering::Relaxed),
                messages_received: self.message.received.load(Ordering::Relaxed),
                messages_compressed: self.message.compressed.load(Ordering::Relaxed),
                messages_decompressed: self.message.decompressed.load(Ordering::Relaxed),
                bytes_compressed: self.message.bytes_compressed.load(Ordering::Relaxed),
                bytes_original: self.message.bytes_original.load(Ordering::Relaxed),
            },
            subscription: SubscriptionMetrics {
                current_subscriptions: self.subscription.current.load(Ordering::Relaxed),
                max_subscriptions: self.subscription.max.load(Ordering::Relaxed),
                total_subscriptions: self.subscription.total.load(Ordering::Relaxed),
            },
            error: ErrorMetrics {
                connection_errors: self.error.connection.load(Ordering::Relaxed),
                message_errors: self.error.message.load(Ordering::Relaxed),
                auth_errors: self.error.auth.load(Ordering::Relaxed),
                compression_errors: self.error.compression.load(Ordering::Relaxed),
            },
            latency: self.latency.read().await.clone(),
            ip_stats: self.ip_stats.read().await.clone(),
        }
    }
}

/// 错误类型
#[derive(Debug, Clone, Copy)]
pub enum ErrorType {
    Connection,
    Message,
    Auth,
    Compression,
}

/// 延迟类型
#[derive(Debug, Clone, Copy)]
pub enum LatencyType {
    MessageProcessing,
    Compression,
    Authentication,
}

/// WebSocket 指标快照
#[derive(Debug, Clone, Serialize)]
pub struct WebSocketMetricsSnapshot {
    pub connection: ConnectionMetrics,
    pub message: MessageMetrics,
    pub subscription: SubscriptionMetrics,
    pub error: ErrorMetrics,
    pub latency: LatencyMetrics,
    pub ip_stats: HashMap<String, IpStats>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::runtime::Runtime;

    #[test]
    fn test_connection_metrics() {
        let rt = Runtime::new().unwrap();
        let metrics = WebSocketMetrics::new();

        // 测试连接记录
        rt.block_on(async {
            metrics.record_connection("127.0.0.1").await;
            metrics.record_connection("192.168.1.1").await;
            
            let snapshot = metrics.get_metrics().await;
            assert_eq!(snapshot.connection.current_connections, 2);
            assert_eq!(snapshot.connection.max_connections, 2);
            assert_eq!(snapshot.connection.total_connections, 2);
        });

        // 测试断开连接
        rt.block_on(async {
            metrics.record_disconnection();
            
            let snapshot = metrics.get_metrics().await;
            assert_eq!(snapshot.connection.current_connections, 1);
            assert_eq!(snapshot.connection.max_connections, 2);
            assert_eq!(snapshot.connection.total_connections, 2);
        });

        // 测试认证
        rt.block_on(async {
            metrics.record_auth_success();
            metrics.record_auth_failure("127.0.0.1").await;
            
            let snapshot = metrics.get_metrics().await;
            assert_eq!(snapshot.connection.auth_success, 1);
            assert_eq!(snapshot.connection.auth_failures, 1);
        });
    }

    #[test]
    fn test_message_metrics() {
        let rt = Runtime::new().unwrap();
        let metrics = WebSocketMetrics::new();

        // 测试消息记录
        rt.block_on(async {
            metrics.record_message_sent(100);
            metrics.record_message_received(200);
            
            let snapshot = metrics.get_metrics().await;
            assert_eq!(snapshot.message.messages_sent, 1);
            assert_eq!(snapshot.message.messages_received, 1);
            assert_eq!(snapshot.message.bytes_original, 300);
        });

        // 测试压缩记录
        rt.block_on(async {
            metrics.record_compression(1000, 500);
            metrics.record_decompression(500, 1000);
            
            let snapshot = metrics.get_metrics().await;
            assert_eq!(snapshot.message.messages_compressed, 1);
            assert_eq!(snapshot.message.messages_decompressed, 1);
            assert_eq!(snapshot.message.bytes_compressed, 500);
        });
    }

    #[test]
    fn test_subscription_metrics() {
        let rt = Runtime::new().unwrap();
        let metrics = WebSocketMetrics::new();

        // 测试订阅记录
        rt.block_on(async {
            metrics.record_subscription();
            metrics.record_subscription();
            
            let snapshot = metrics.get_metrics().await;
            assert_eq!(snapshot.subscription.current_subscriptions, 2);
            assert_eq!(snapshot.subscription.max_subscriptions, 2);
            assert_eq!(snapshot.subscription.total_subscriptions, 2);
        });

        // 测试取消订阅
        rt.block_on(async {
            metrics.record_unsubscription();
            
            let snapshot = metrics.get_metrics().await;
            assert_eq!(snapshot.subscription.current_subscriptions, 1);
            assert_eq!(snapshot.subscription.max_subscriptions, 2);
            assert_eq!(snapshot.subscription.total_subscriptions, 2);
        });
    }

    #[test]
    fn test_error_metrics() {
        let rt = Runtime::new().unwrap();
        let metrics = WebSocketMetrics::new();

        // 测试错误记录
        rt.block_on(async {
            metrics.record_error(ErrorType::Connection);
            metrics.record_error(ErrorType::Message);
            metrics.record_error(ErrorType::Authentication);
            metrics.record_error(ErrorType::Compression);
            
            let snapshot = metrics.get_metrics().await;
            assert_eq!(snapshot.error.connection_errors, 1);
            assert_eq!(snapshot.error.message_errors, 1);
            assert_eq!(snapshot.error.auth_errors, 1);
            assert_eq!(snapshot.error.compression_errors, 1);
        });
    }

    #[test]
    fn test_latency_metrics() {
        let rt = Runtime::new().unwrap();
        let metrics = WebSocketMetrics::new();

        // 测试延迟记录
        rt.block_on(async {
            metrics.record_latency(LatencyType::MessageProcessing, Duration::from_millis(100));
            metrics.record_latency(LatencyType::Compression, Duration::from_millis(50));
            metrics.record_latency(LatencyType::Authentication, Duration::from_millis(75));
            
            let snapshot = metrics.get_metrics().await;
            assert!(!snapshot.latency.message_processing.is_empty());
            assert!(!snapshot.latency.compression.is_empty());
            assert!(!snapshot.latency.authentication.is_empty());

            // 验证延迟值
            assert_eq!(snapshot.latency.message_processing[0], 100);
            assert_eq!(snapshot.latency.compression[0], 50);
            assert_eq!(snapshot.latency.authentication[0], 75);
        });
    }

    #[test]
    fn test_ip_stats() {
        let rt = Runtime::new().unwrap();
        let metrics = WebSocketMetrics::new();

        // 测试 IP 统计
        rt.block_on(async {
            // 记录连接和认证失败
            metrics.record_connection("127.0.0.1").await;
            metrics.record_auth_failure("127.0.0.1").await;
            metrics.record_connection("127.0.0.1").await;
            
            let snapshot = metrics.get_metrics().await;
            let ip_stats = snapshot.ip_stats.get("127.0.0.1").unwrap();
            
            assert_eq!(ip_stats.connection_count, 2);
            assert_eq!(ip_stats.auth_failures, 1);
            assert!(ip_stats.last_connection > 0);
        });
    }

    #[test]
    fn test_metrics_snapshot() {
        let rt = Runtime::new().unwrap();
        let metrics = WebSocketMetrics::new();

        // 测试指标快照
        rt.block_on(async {
            // 添加各种指标数据
            metrics.record_connection("127.0.0.1").await;
            metrics.record_auth_success();
            metrics.record_message_sent(100);
            metrics.record_subscription();
            metrics.record_error(ErrorType::Connection);
            metrics.record_latency(LatencyType::MessageProcessing, Duration::from_millis(100));

            // 获取快照并验证
            let snapshot = metrics.get_metrics().await;
            
            // 验证连接指标
            assert_eq!(snapshot.connection.current_connections, 1);
            assert_eq!(snapshot.connection.auth_success, 1);

            // 验证消息指标
            assert_eq!(snapshot.message.messages_sent, 1);
            assert_eq!(snapshot.message.bytes_original, 100);

            // 验证订阅指标
            assert_eq!(snapshot.subscription.current_subscriptions, 1);

            // 验证错误指标
            assert_eq!(snapshot.error.connection_errors, 1);

            // 验证延迟指标
            assert!(!snapshot.latency.message_processing.is_empty());
        });
    }

    #[test]
    fn test_concurrent_metrics() {
        let rt = Runtime::new().unwrap();
        let metrics = Arc::new(WebSocketMetrics::new());

        rt.block_on(async {
            let mut handles = vec![];
            
            // 创建多个任务并发更新指标
            for i in 0..10 {
                let metrics = metrics.clone();
                let handle = tokio::spawn(async move {
                    // 记录连接
                    metrics.record_connection(&format!("127.0.0.{}", i)).await;
                    metrics.record_message_sent(100);
                    metrics.record_subscription();
                    metrics.record_latency(LatencyType::MessageProcessing, Duration::from_millis(100));
                    
                    // 模拟一些操作后断开连接
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    metrics.record_disconnection();
                });
                handles.push(handle);
            }

            // 等待所有任务完成
            for handle in handles {
                handle.await.unwrap();
            }

            // 验证最终状态
            let snapshot = metrics.get_metrics().await;
            
            // 所有连接都已断开
            assert_eq!(snapshot.connection.current_connections, 0);
            // 总共有 10 个连接
            assert_eq!(snapshot.connection.total_connections, 10);
            // 最大同时连接数应该是 10
            assert_eq!(snapshot.connection.max_connections, 10);
            // 验证消息和订阅数
            assert_eq!(snapshot.message.messages_sent, 10);
            assert_eq!(snapshot.subscription.total_subscriptions, 10);
        });
    }
}

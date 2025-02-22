use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::RwLock;
use warp::ws::{Message as WarpMessage, WebSocket, Ws};
use warp::Filter;
use serde::{Deserialize, Serialize};
use jsonwebtoken::{decode, DecodingKey, Validation};
use anyhow::{anyhow, Result};
use log::{error, info};
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;
use bytes::Bytes;
use flate2::Compression;

use crate::atomicals::state::AtomicalsState;
use crate::atomicals::protocol::{AtomicalId, AtomicalOperation};
use crate::atomicals::metrics::{WebSocketMetrics, LatencyType, ErrorType};
use crate::atomicals::rpc::AtomicalInfo;

#[derive(Debug, Clone, Copy)]
pub enum CompressionLevel {
    None,
    Fast,
    Default,
    Best,
}

impl From<CompressionLevel> for Compression {
    fn from(level: CompressionLevel) -> Self {
        match level {
            CompressionLevel::None => Compression::none(),
            CompressionLevel::Fast => Compression::fast(),
            CompressionLevel::Default => Compression::default(),
            CompressionLevel::Best => Compression::best(),
        }
    }
}

/// 认证令牌声明
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    /// 用户 ID
    sub: String,
    /// 过期时间
    exp: usize,
    /// 权限列表
    permissions: Vec<String>,
}

/// 认证消息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthMessage {
    /// 认证令牌
    pub token: String,
}

/// 认证响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthResponse {
    /// 是否成功
    pub success: bool,
    /// 错误信息
    pub error: Option<String>,
}

/// WebSocket 消息类型
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum WsMessage {
    /// 认证消息
    Auth(AuthMessage),
    /// 认证响应
    AuthResponse(AuthResponse),
    /// 订阅请求
    Subscribe(SubscribeRequest),
    /// 取消订阅请求
    Unsubscribe(UnsubscribeRequest),
    /// Atomical 状态更新
    AtomicalUpdate(AtomicalUpdate),
    /// 新的 Atomical 操作
    NewOperation(OperationNotification),
    /// 错误消息
    Error(String),
    /// 心跳消息
    Heartbeat(HeartbeatMessage),
    /// 订阅确认消息
    SubscriptionConfirmed(String),
    /// 取消订阅确认消息
    UnsubscriptionConfirmed(String),
    /// pong 消息
    Pong,
}

/// 订阅请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeRequest {
    pub id: AtomicalId,
    pub subscription_type: SubscriptionType,
}

/// 取消订阅请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsubscribeRequest {
    /// 订阅 ID
    pub subscription_id: String,
}

/// 订阅类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubscriptionType {
    /// 订阅特定 Atomical
    Atomical(AtomicalId),
    /// 订阅所有新操作
    All,
}

/// 订阅更新消息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomicalUpdate {
    pub id: AtomicalId,
    pub info: AtomicalInfo,
}

/// 操作通知消息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationNotification {
    pub id: AtomicalId,
    pub operation: AtomicalOperation,
    pub timestamp: u64,
}

/// 心跳消息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMessage {
    /// 时间戳
    pub timestamp: u64,
    /// 服务器状态
    pub status: ServerStatus,
}

/// 服务器状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerStatus {
    /// 连接数
    pub connections: usize,
    /// 内存使用
    pub memory_usage: u64,
    /// CPU 使用
    pub cpu_usage: f64,
}

/// 连接状态
#[derive(Debug, Clone, PartialEq)]
enum ConnectionState {
    /// 未认证
    Unauthenticated,
    /// 已认证
    Authenticated(Claims),
}

/// 连接信息
#[derive(Debug)]
struct Connection {
    /// 连接 ID
    id: String,
    /// 最后活跃时间
    last_active: Instant,
    /// 订阅列表
    subscriptions: HashSet<String>,
    /// WebSocket 发送端
    tx: futures::channel::mpsc::UnboundedSender<Message>,
    /// 连接状态
    state: ConnectionState,
}

/// WebSocket 配置结构体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketConfig {
    /// 最大连接数
    pub max_connections: usize,
    /// 心跳间隔
    pub heartbeat_interval: u64,
    /// 心跳超时
    pub heartbeat_timeout: u64,
    /// 连接清理间隔
    pub cleanup_interval: u64,
    /// 连接超时
    pub connection_timeout: u64,
    /// 压缩级别
    pub compression_level: CompressionLevel,
    /// 压缩阈值（字节）
    pub compression_threshold: usize,
    /// JWT 密钥
    pub jwt_secret: String,
    /// 每个连接的最大订阅数
    pub max_subscriptions_per_connection: usize,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            max_connections: 1000,
            heartbeat_interval: 30,
            heartbeat_timeout: 60,
            cleanup_interval: 300,
            connection_timeout: 60,
            compression_level: CompressionLevel::Default,
            compression_threshold: 1024,
            jwt_secret: "".to_string(),
            max_subscriptions_per_connection: 100,
        }
    }
}

impl WebSocketConfig {
    pub fn test_config() -> Self {
        Self {
            jwt_secret: "test_secret".to_string(),
            max_connections: 100,
            max_subscriptions_per_connection: 10,
            connection_timeout: 60,
            heartbeat_timeout: 60,
            compression_level: CompressionLevel::Fast,
            compression_threshold: 1024,
            heartbeat_interval: 30,
            cleanup_interval: 60,
        }
    }
}

/// 消息处理器
struct MessageHandler {
    /// 压缩级别
    compression_level: CompressionLevel,
    /// 压缩阈值（字节）
    compression_threshold: usize,
}

impl MessageHandler {
    /// 创建新的消息处理器
    fn new(compression_level: CompressionLevel, compression_threshold: usize) -> Self {
        Self {
            compression_level,
            compression_threshold,
        }
    }

    /// 压缩消息
    fn compress(&self, data: &[u8]) -> Result<Bytes> {
        // 如果数据小于阈值或压缩级别为 None，则不压缩
        if data.len() < self.compression_threshold {
            return Ok(Bytes::copy_from_slice(data));
        }

        let mut encoder = GzEncoder::new(Vec::new(), self.compression_level.into());
        encoder.write_all(data)?;
        let compressed = encoder.finish()?;
        Ok(Bytes::from(compressed))
    }

    /// 解压消息
    fn decompress(&self, data: &[u8]) -> Result<Bytes> {
        // 尝试解压，如果失败则返回原始数据
        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        match decoder.read_to_end(&mut decompressed) {
            Ok(_) => Ok(Bytes::from(decompressed)),
            Err(_) => Ok(Bytes::copy_from_slice(data)),
        }
    }

    /// 处理发送消息
    fn handle_outgoing(&self, msg: Message) -> Result<Message> {
        match msg {
            Message::Text(text) => {
                let compressed = self.compress(text.as_bytes())?;
                Ok(Message::Binary(compressed.to_vec()))
            }
            Message::Binary(data) => {
                let compressed = self.compress(&data)?;
                Ok(Message::Binary(compressed.to_vec()))
            }
            _ => Ok(msg),
        }
    }

    /// 处理接收消息
    fn handle_incoming(&self, msg: Message) -> Result<Message> {
        match msg {
            Message::Binary(data) => {
                let decompressed = self.decompress(&data)?;
                // 尝试将解压后的数据转换为文本
                match String::from_utf8(decompressed.to_vec()) {
                    Ok(text) => Ok(Message::Text(text)),
                    Err(_) => Ok(Message::Binary(decompressed.to_vec())),
                }
            }
            _ => Ok(msg),
        }
    }
}

/// WebSocket 服务器
pub struct WsServer {
    /// WebSocket 配置
    websocket_config: WebSocketConfig,
    /// 连接池
    connections: Arc<RwLock<HashMap<String, Connection>>>,
    /// 当前连接数
    connection_count: AtomicUsize,
    /// Atomicals 状态
    state: Arc<AtomicalsState>,
    /// 消息处理器
    message_handler: MessageHandler,
    /// 指标收集器
    metrics: Arc<WebSocketMetrics>,
}

impl WsServer {
    /// 创建新的 WebSocket 服务器
    pub fn new(state: Arc<AtomicalsState>, config: WebSocketConfig) -> Self {
        let config_clone = config.clone();
        Self {
            websocket_config: config,
            connections: Arc::new(RwLock::new(HashMap::new())),
            connection_count: AtomicUsize::new(0),
            state,
            message_handler: MessageHandler::new(
                config_clone.compression_level,
                config_clone.compression_threshold,
            ),
            metrics: Arc::new(WebSocketMetrics::new()),
        }
    }

    /// 启动 WebSocket 服务器
    pub async fn start_server(self: Arc<Self>) -> Result<()> {
        // 启动心跳检查任务
        let server = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(server.websocket_config.heartbeat_interval));
            loop {
                interval.tick().await;
                if let Err(e) = server.check_heartbeats().await {
                    error!("Failed to check heartbeats: {}", e);
                }
            }
        });

        // 启动连接清理任务
        let server = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(server.websocket_config.cleanup_interval));
            loop {
                interval.tick().await;
                if let Err(e) = server.cleanup_connections().await {
                    error!("Failed to cleanup connections: {}", e);
                }
            }
        });

        // WebSocket 路由
        let ws_route = warp::path("ws")
            .and(warp::ws())
            .and(warp::addr::remote())
            .map(move |ws: Ws, addr: Option<std::net::SocketAddr>| {
                let server = self.clone();
                let ip = addr.map(|a| a.ip().to_string()).unwrap_or_else(|| "unknown".to_string());
                ws.on_upgrade(move |socket| server.handle_connection(socket, ip))
            });

        // 启动服务器
        warp::serve(ws_route)
            .run(([127, 0, 0, 1], 3000))
            .await;

        Ok(())
    }

    /// 处理新的 WebSocket 连接
    async fn handle_connection(self: Arc<Self>, ws: WebSocket, ip: String) {
        // 记录新连接
        self.metrics.record_connection(&ip).await;

        // 检查是否达到最大连接数
        let current_connections = self.connection_count.load(Ordering::Relaxed);
        if current_connections >= self.websocket_config.max_connections {
            let _ = ws.close().await;
            return;
        }

        // 创建连接 ID
        let connection_id = Uuid::new_v4().to_string();

        // 分离发送和接收端
        let (mut ws_tx, mut ws_rx) = ws.split();
        let (tx, rx) = futures::channel::mpsc::unbounded();

        // 创建连接对象，初始状态为未认证
        let connection = Connection {
            id: connection_id.clone(),
            last_active: Instant::now(),
            subscriptions: HashSet::new(),
            tx,
            state: ConnectionState::Unauthenticated,
        };

        // 添加到连接池
        {
            let mut connections = self.connections.write().await;
            connections.insert(connection_id.clone(), connection);
        }
        self.connection_count.fetch_add(1, Ordering::Relaxed);

        // 转发消息到 WebSocket
        let server_clone = self.clone();
        let forward_task = tokio::spawn(async move {
            let mut rx = rx;
            while let Some(msg) = rx.next().await {
                // 压缩发送的消息
                if let Ok(compressed_msg) = server_clone.message_handler.handle_outgoing(msg) {
                    // 将 tungstenite::Message 转换为 warp::ws::Message
                    let warp_msg = match compressed_msg {
                        Message::Text(text) => WarpMessage::text(text),
                        Message::Binary(data) => WarpMessage::binary(data),
                        Message::Ping(data) => WarpMessage::ping(data),
                        Message::Pong(data) => WarpMessage::pong(data),
                        Message::Close(_) => WarpMessage::close(),
                        _ => continue,
                    };
                    if ws_tx.send(warp_msg).await.is_err() {
                        break;
                    }
                }
            }
        });

        // 处理入站消息
        let server = self.clone();
        let message_handle = tokio::spawn(async move {
            while let Some(result) = ws_rx.next().await {
                if let Ok(msg) = result {
                    let start = Instant::now();
                    let compressed_size = if let Ok(text) = msg.to_str() {
                        text.len()
                    } else {
                        msg.as_bytes().len()
                    };

                    // 将 warp::ws::Message 转换为 tungstenite::Message
                    let tungstenite_msg = if let Ok(text) = msg.to_str() {
                        Message::Text(text.to_string())
                    } else {
                        Message::Binary(msg.as_bytes().to_vec())
                    };

                    // 解压接收的消息
                    if let Ok(decompressed_msg) = server.message_handler.handle_incoming(tungstenite_msg) {
                        let original_size = match decompressed_msg {
                            Message::Text(ref text) => text.len(),
                            Message::Binary(ref data) => data.len(),
                            _ => 0,
                        };
                        server.metrics.record_decompression(compressed_size, original_size);
                        server.metrics.record_message_received(original_size);

                        // 记录解压延迟
                        server.metrics.record_latency(
                            LatencyType::Compression,
                            start.elapsed(),
                        ).await;

                        // 更新最后活跃时间
                        if let Some(conn) = server.connections.write().await.get_mut(&connection_id) {
                            conn.last_active = Instant::now();
                        }

                        // 处理消息
                        if let Message::Text(text) = decompressed_msg {
                            let msg_start = Instant::now();
                            if let Ok(ws_msg) = serde_json::from_str::<WsMessage>(&text) {
                                match ws_msg {
                                    WsMessage::Auth(auth_msg) => {
                                        let auth_start = Instant::now();
                                        let mut auth_response = AuthResponse {
                                            success: false,
                                            error: None,
                                        };

                                        // 验证令牌
                                        match server.verify_token(&auth_msg.token) {
                                            Ok(claims) => {
                                                if let Some(conn) = server.connections.write().await.get_mut(&connection_id) {
                                                    conn.state = ConnectionState::Authenticated(claims);
                                                    auth_response.success = true;
                                                    server.metrics.record_auth_success();
                                                }
                                            }
                                            Err(e) => {
                                                auth_response.error = Some(e.to_string());
                                                server.metrics.record_auth_failure(&ip).await;
                                                server.metrics.record_error(ErrorType::Auth);
                                            }
                                        }

                                        // 记录认证延迟
                                        server.metrics.record_latency(
                                            LatencyType::Authentication,
                                            auth_start.elapsed(),
                                        ).await;

                                        // 发送认证响应
                                        if let Some(conn) = server.connections.read().await.get(&connection_id) {
                                            let response = serde_json::to_string(&WsMessage::AuthResponse(auth_response)).unwrap();
                                            let _ = conn.tx.unbounded_send(Message::Text(response));
                                        }
                                    }
                                    WsMessage::Subscribe(req) => {
                                        // 检查认证状态
                                        if let Some(conn) = server.connections.read().await.get(&connection_id) {
                                            match &conn.state {
                                                ConnectionState::Authenticated(claims) => {
                                                    // 检查订阅权限
                                                    if server.check_permission(claims, "subscribe") {
                                                        if let Ok(sub_id) = server.handle_subscribe(req.clone()).await {
                                                            if let Some(conn) = server.connections.write().await.get_mut(&connection_id) {
                                                                conn.subscriptions.insert(sub_id.clone());
                                                                server.metrics.record_subscription();
                                                                let response = serde_json::to_string(&WsMessage::SubscriptionConfirmed(sub_id)).unwrap();
                                                                let _ = conn.tx.unbounded_send(Message::Text(response));
                                                            }
                                                        }
                                                    } else {
                                                        let error = "Permission denied: subscribe".to_string();
                                                        let _ = conn.tx.unbounded_send(Message::Text(serde_json::to_string(&WsMessage::Error(error)).unwrap()));
                                                        server.metrics.record_error(ErrorType::Auth);
                                                    }
                                                }
                                                ConnectionState::Unauthenticated => {
                                                    let error = "Authentication required".to_string();
                                                    let _ = conn.tx.unbounded_send(Message::Text(serde_json::to_string(&WsMessage::Error(error)).unwrap()));
                                                    server.metrics.record_error(ErrorType::Auth);
                                                }
                                            }
                                        }
                                    }
                                    WsMessage::Unsubscribe(req) => {
                                        // 检查认证状态
                                        if let Some(conn) = server.connections.read().await.get(&connection_id) {
                                            match &conn.state {
                                                ConnectionState::Authenticated(_) => {
                                                    // 移除订阅
                                                    if let Some(conn) = server.connections.write().await.get_mut(&connection_id) {
                                                        conn.subscriptions.remove(&req.subscription_id);
                                                        server.metrics.record_unsubscription();
                                                        let response = serde_json::to_string(&WsMessage::UnsubscriptionConfirmed(req.subscription_id)).unwrap();
                                                        let _ = conn.tx.unbounded_send(Message::Text(response));
                                                    }
                                                }
                                                ConnectionState::Unauthenticated => {
                                                    let error = "Authentication required".to_string();
                                                    let _ = conn.tx.unbounded_send(Message::Text(serde_json::to_string(&WsMessage::Error(error)).unwrap()));
                                                    server.metrics.record_error(ErrorType::Auth);
                                                }
                                            }
                                        }
                                    }
                                    WsMessage::Pong => {
                                        server.update_last_pong();
                                    }
                                    _ => {}
                                }
                            }

                            // 记录消息处理延迟
                            server.metrics.record_latency(
                                LatencyType::MessageProcessing,
                                msg_start.elapsed(),
                            ).await;
                        }
                    } else {
                        server.metrics.record_error(ErrorType::Compression);
                    }
                } else {
                    server.metrics.record_error(ErrorType::Message);
                    break;
                }
            }

            // 连接断开，清理资源
            server.remove_connection(&connection_id).await;
            server.metrics.record_disconnection();
        });

        // 等待任务完成
        tokio::select! {
            _ = forward_task => {},
            _ = message_handle => {},
        }
    }

    /// 移除连接
    async fn remove_connection(&self, connection_id: &str) {
        let mut connections = self.connections.write().await;
        if connections.remove(connection_id).is_some() {
            self.connection_count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// 检查心跳
    async fn check_heartbeats(&self) -> Result<()> {
        let mut to_remove = Vec::new();
        let now = Instant::now();

        let connections = self.connections.read().await;
        for (id, conn) in connections.iter() {
            if now.duration_since(conn.last_active) > Duration::from_secs(self.websocket_config.heartbeat_timeout) {
                to_remove.push(id.clone());
            }
        }
        drop(connections);

        for id in to_remove {
            if let Some(conn) = self.connections.write().await.remove(&id) {
                self.connection_count.fetch_sub(1, Ordering::Relaxed);
                self.metrics.record_disconnect();
                info!("Connection {} timed out", id);
            }
        }

        Ok(())
    }

    /// 清理连接
    async fn cleanup_connections(&self) -> Result<()> {
        let mut to_remove = Vec::new();
        let now = Instant::now();

        let connections = self.connections.read().await;
        for (id, conn) in connections.iter() {
            if now.duration_since(conn.last_active) > Duration::from_secs(self.websocket_config.connection_timeout) {
                to_remove.push(id.clone());
            }
        }
        drop(connections);

        for id in to_remove {
            if let Some(conn) = self.connections.write().await.remove(&id) {
                self.connection_count.fetch_sub(1, Ordering::Relaxed);
                self.metrics.record_disconnect();
                info!("Connection {} removed due to inactivity", id);
            }
        }

        Ok(())
    }

    /// 处理订阅请求
    async fn handle_subscribe(&self, req: SubscribeRequest) -> Result<String> {
        let subscription_id = Uuid::new_v4().to_string();

        match req.subscription_type {
            SubscriptionType::Atomical(id) => {
                // 验证 Atomical 是否存在
                if !self.state.exists(&id).await? {
                    return Err(anyhow!("Atomical not found"));
                }
            }
            SubscriptionType::All => {
                // 不需要特殊验证
            }
        }

        Ok(subscription_id)
    }

    /// 验证 JWT 令牌
    fn verify_token(&self, token: &str) -> Result<Claims> {
        let key = DecodingKey::from_secret(self.websocket_config.jwt_secret.as_bytes());
        let validation = Validation::default();
        
        let token_data = decode::<Claims>(token, &key, &validation)
            .map_err(|e| anyhow!("Invalid token: {}", e))?;
            
        Ok(token_data.claims)
    }

    /// 检查权限
    fn check_permission(&self, claims: &Claims, required_permission: &str) -> bool {
        claims.permissions.contains(&required_permission.to_string())
    }

    /// 更新最后一次 pong 时间
    fn update_last_pong(&self) {
        let now = chrono::Utc::now().timestamp();
        self.last_pong.store(now as u64, Ordering::Relaxed);
    }

    /// 广播消息
    pub fn broadcast(&self, message: WsMessage) -> Result<()> {
        self.broadcast_tx.send(message).map_err(|e| anyhow!("Broadcast error: {}", e))?;
        Ok(())
    }

    /// 获取监控指标
    pub async fn get_metrics(&self) -> serde_json::Value {
        serde_json::to_value(self.metrics.get_metrics().await).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::secp256k1::rand::{self, Rng};
    use std::time::SystemTime;
    use tokio::runtime::Runtime;
    use tokio::sync::mpsc;
    use tokio::time::{sleep, Duration};
    use warp::test::WsClient;

    fn create_test_config() -> AppConfig {
        AppConfig {
            websocket_config: WebSocketConfig {
                jwt_secret: "test_secret".to_string(),
                max_connections: 100,
                max_subscriptions_per_connection: 10,
                connection_timeout: 60,
                compression_level: CompressionLevel::Fast,
                compression_threshold: 1024,
                heartbeat_interval: 30,
                cleanup_interval: 60,
            },
        }
    }

    fn create_test_claims() -> Claims {
        Claims {
            sub: "test_user".to_string(),
            exp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as usize + 3600,
            permissions: vec!["read".to_string(), "write".to_string()],
        }
    }

    fn create_test_atomical_update() -> AtomicalUpdate {
        AtomicalUpdate {
            id: AtomicalId {
                txid: bitcoin::Txid::from_str(
                    "1234567890123456789012345678901234567890123456789012345678901234"
                ).unwrap(),
                vout: 0,
            },
            info: AtomicalInfo {
                id: AtomicalId {
                    txid: bitcoin::Txid::from_str(
                        "1234567890123456789012345678901234567890123456789012345678901234"
                    ).unwrap(),
                    vout: 0,
                },
                atomical_type: AtomicalType::NFT,
                owner: "bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080".to_string(),
                value: 1000,
                metadata: Some(serde_json::json!({
                    "name": "Test NFT",
                    "description": "Test Description"
                })),
                created_height: 100,
                created_timestamp: 1234567890,
                sealed: false,
            },
        }
    }

    #[test]
    fn test_message_handler() -> Result<()> {
        let handler = MessageHandler::new(CompressionLevel::Fast, 1024);

        // 测试压缩和解压
        let test_data = b"Hello, WebSocket!";
        let compressed = handler.compress(test_data)?;
        let decompressed = handler.decompress(&compressed)?;
        assert_eq!(decompressed, Bytes::from(test_data.to_vec()));

        // 测试消息处理
        let msg = Message::Text("Test message".to_string());
        let handled = handler.handle_outgoing(msg.clone())?;
        assert!(matches!(handled, Message::Binary(_)));

        // 测试大消息压缩
        let large_data = vec![b'x'; 2048];
        let msg = Message::Binary(large_data.clone());
        let handled = handler.handle_outgoing(msg)?;
        assert!(matches!(handled, Message::Binary(_)));

        Ok(())
    }

    #[tokio::test]
    async fn test_ws_server() -> Result<()> {
        let config = create_test_config();
        let state = Arc::new(AtomicalsState::new());
        let server = Arc::new(WsServer::new(state, config.clone()));

        // 测试服务器启动
        let server_clone = Arc::clone(&server);
        tokio::spawn(async move {
            server_clone.start_server().await.unwrap();
        });

        // 等待服务器启动
        sleep(Duration::from_millis(100)).await;

        // 创建测试客户端
        let (ws_stream, _) = tokio_tungstenite::connect_async("ws://127.0.0.1:3000")
            .await
            .unwrap();
        let (mut write, mut read) = ws_stream.split();

        // 测试认证
        let auth_msg = WsMessage::Auth(AuthMessage {
            token: "test_token".to_string(),
        });
        let msg = Message::Text(serde_json::to_string(&auth_msg).unwrap());
        write.send(msg).await.unwrap();

        // 等待认证响应
        if let Some(response) = read.next().await {
            let response = response.unwrap();
            match response {
                Message::Text(text) => {
                    let msg: WsMessage = serde_json::from_str(&text).unwrap();
                    match msg {
                        WsMessage::AuthResponse(resp) => {
                            assert!(!resp.success);
                            assert!(resp.error.is_some());
                        }
                        _ => panic!("Expected AuthResponse"),
                    }
                }
                _ => panic!("Expected Text message"),
            }
        }

        Ok(())
    }

    #[test]
    fn test_config() {
        let config = WebSocketConfig::default();
        assert_eq!(config.max_connections, 1000);
        assert_eq!(config.heartbeat_interval, 30);
        assert_eq!(config.compression_threshold, 1024);
    }

    #[test]
    fn test_token_verification() -> Result<()> {
        let config = create_test_config();
        let state = Arc::new(AtomicalsState::new());
        let server = WsServer::new(state, config.clone());

        // 创建有效令牌
        let claims = create_test_claims();
        let token = jsonwebtoken::encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(config.websocket_config.jwt_secret.as_ref().unwrap().as_bytes()),
        )?;

        // 测试有效令牌
        let verified_claims = server.verify_token(&token)?;
        assert_eq!(verified_claims.sub, claims.sub);
        assert_eq!(verified_claims.permissions, claims.permissions);

        // 测试无效令牌
        assert!(server.verify_token("invalid_token").is_err());

        // 测试过期令牌
        let mut expired_claims = claims;
        expired_claims.exp = 0;
        let expired_token = jsonwebtoken::encode(
            &Header::default(),
            &expired_claims,
            &EncodingKey::from_secret(config.websocket_config.jwt_secret.as_ref().unwrap().as_bytes()),
        )?;
        assert!(server.verify_token(&expired_token).is_err());

        Ok(())
    }

    #[test]
    fn test_permission_check() {
        let config = create_test_config();
        let state = Arc::new(AtomicalsState::new());
        let server = WsServer::new(state, config);

        let claims = create_test_claims();

        // 测试有权限
        assert!(server.check_permission(&claims, "read"));
        assert!(server.check_permission(&claims, "write"));

        // 测试无权限
        assert!(!server.check_permission(&claims, "admin"));
    }

    #[tokio::test]
    async fn test_subscription_handling() -> Result<()> {
        let config = create_test_config();
        let state = Arc::new(AtomicalsState::new());
        let server = Arc::new(WsServer::new(state, config.clone()));

        // 测试订阅请求
        let req = SubscribeRequest {
            id: AtomicalId {
                txid: bitcoin::Txid::from_str(
                    "1234567890123456789012345678901234567890123456789012345678901234"
                ).unwrap(),
                vout: 0,
            },
            subscription_type: SubscriptionType::All,
        };

        let sub_id = server.handle_subscribe(&req).await?;
        assert!(!sub_id.is_empty());

        // 测试超出最大订阅数
        for _ in 0..config.websocket_config.max_connections {
            let req = SubscribeRequest {
                id: AtomicalId {
                    txid: bitcoin::Txid::from_str(
                        "1234567890123456789012345678901234567890123456789012345678901234"
                    ).unwrap(),
                    vout: 0,
                },
                subscription_type: SubscriptionType::All,
            };
            server.handle_subscribe(&req).await?;
        }

        let req = SubscribeRequest {
            id: AtomicalId {
                txid: bitcoin::Txid::from_str(
                    "1234567890123456789012345678901234567890123456789012345678901234"
                ).unwrap(),
                vout: 0,
            },
            subscription_type: SubscriptionType::All,
        };
        assert!(server.handle_subscribe(&req).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_broadcast() -> Result<()> {
        let config = create_test_config();
        let state = Arc::new(AtomicalsState::new());
        let server = Arc::new(WsServer::new(state, config));

        // 创建测试消息
        let update = create_test_atomical_update();
        let msg = WsMessage::AtomicalUpdate(update);

        // 测试广播
        server.broadcast(msg.clone())?;

        // 测试无连接时的广播
        assert!(server.broadcast(msg).is_ok());

        Ok(())
    }

    #[test]
    fn test_metrics() {
        let config = create_test_config();
        let state = Arc::new(AtomicalsState::new());
        let server = WsServer::new(state, config);

        let metrics = server.get_metrics();
        assert!(metrics.get("connections").is_some());
        assert!(metrics.get("subscriptions").is_some());
        assert!(metrics.get("messages_sent").is_some());
        assert!(metrics.get("messages_received").is_some());
    }
}

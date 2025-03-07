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
use flate2::write::GzEncoder;
use flate2::read::GzDecoder;
use std::io::{Write, Read};

use crate::atomicals::state::AtomicalsState;
use crate::atomicals::protocol::{AtomicalId, AtomicalOperation};
use crate::atomicals::metrics::{WebSocketMetrics, LatencyType, ErrorType};
use crate::atomicals::rpc::AtomicalInfo;
use crate::atomicals::AtomicalType;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompressionLevel {
    /// 不压缩
    None,
    /// 快速压缩
    Fast,
    /// 最佳压缩
    Best,
}

impl From<CompressionLevel> for Compression {
    fn from(level: CompressionLevel) -> Self {
        match level {
            CompressionLevel::None => Compression::none(),
            CompressionLevel::Fast => Compression::fast(),
            CompressionLevel::Best => Compression::best(),
        }
    }
}

impl Default for CompressionLevel {
    fn default() -> Self {
        Self::Fast
    }
}

/// 认证令牌声明
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
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

/// 更新类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateType {
    /// 新创建
    New,
    /// 更新
    Update,
    /// 删除
    Delete,
}

/// 操作状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperationStatus {
    /// 成功
    Success,
    /// 失败
    Failure,
    /// 待处理
    Pending,
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
    /// 最后一次 pong 时间
    last_pong: Instant,
    /// 订阅列表
    subscriptions: HashSet<String>,
    /// WebSocket 发送端
    tx: futures::channel::mpsc::UnboundedSender<Message>,
    /// 连接状态
    state: ConnectionState,
}

impl Connection {
    fn new(id: String, tx: futures::channel::mpsc::UnboundedSender<Message>) -> Self {
        let now = Instant::now();
        Self {
            id,
            last_active: now,
            last_pong: now,
            subscriptions: HashSet::new(),
            tx,
            state: ConnectionState::Unauthenticated,
        }
    }

    fn update_last_pong(&mut self) {
        self.last_pong = Instant::now();
    }
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
    /// 端口号
    pub port: u16,
    /// 是否启用压缩
    pub compression_enabled: bool,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            max_connections: 1000,
            heartbeat_interval: 30,
            heartbeat_timeout: 60,
            cleanup_interval: 60,
            connection_timeout: 60,
            compression_level: CompressionLevel::Fast,
            compression_threshold: 1024,
            jwt_secret: "".to_string(),
            max_subscriptions_per_connection: 100,
            port: 3000,
            compression_enabled: false,
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
            port: 3000,
            compression_enabled: true,
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

    /// 解压缩数据
    fn decompress_data(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
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
                let decompressed = self.decompress_data(&data)?;
                // 尝试将解压后的数据转换为文本
                match String::from_utf8(decompressed.clone()) {
                    Ok(text) => Ok(Message::Text(text)),
                    Err(_) => Ok(Message::Binary(decompressed)),
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
        let ws_server = self.clone();
        
        // 创建 WebSocket 路由
        let ws_route = warp::path("ws")
            .and(warp::ws())
            .and(warp::addr::remote())
            .map(move |ws: Ws, addr: Option<std::net::SocketAddr>| {
                let ip = addr.map(|a| a.ip().to_string()).unwrap_or_else(|| "unknown".to_string());
                let ws_server_clone = ws_server.clone();
                
                ws.on_upgrade(move |websocket| {
                    let ws_server = ws_server_clone.clone();
                    let ip_clone = ip.clone();
                    async move {
                        ws_server.handle_connection(websocket, ip_clone).await;
                    }
                })
            });
        
        // 启动心跳检查任务
        let heartbeat_server = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(heartbeat_server.websocket_config.heartbeat_interval));
            loop {
                interval.tick().await;
                if let Err(e) = heartbeat_server.check_heartbeats().await {
                    error!("Error checking heartbeats: {}", e);
                }
            }
        });
        
        // 启动连接清理任务
        let cleanup_server = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(cleanup_server.websocket_config.cleanup_interval));
            loop {
                interval.tick().await;
                if let Err(e) = cleanup_server.cleanup_connections().await {
                    error!("Error cleaning up connections: {}", e);
                }
            }
        });
        
        // 启动服务器
        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], self.websocket_config.port));
        info!("WebSocket server listening on {}", addr);
        warp::serve(ws_route).run(addr).await;
        
        Ok(())
    }

    /// 处理新的 WebSocket 连接
    pub async fn handle_connection(&self, websocket: WebSocket, _ip: String) {
        let (mut ws_tx, mut ws_rx) = websocket.split();
        let (tx, mut rx) = futures::channel::mpsc::unbounded();
        let connection_id = Uuid::new_v4().to_string();
        
        // 创建新连接
        let connection = Connection::new(connection_id.clone(), tx.clone());
        
        // 存储连接
        {
            let mut connections = self.connections.write().await;
            connections.insert(connection_id.clone(), connection);
            self.connection_count.fetch_add(1, Ordering::SeqCst);
        }
        
        // 处理接收到的消息
        let ws_server = self.clone();
        let handle_messages = async move {
            while let Some(result) = ws_rx.next().await {
                match result {
                    Ok(warp_msg) => {
                        // 将 warp::ws::Message 转换为 tokio_tungstenite::tungstenite::Message
                        let tungstenite_msg = if warp_msg.is_text() {
                            if let Ok(text) = warp_msg.to_str() {
                                Message::Text(text.to_string())
                            } else {
                                Message::Binary(warp_msg.as_bytes().to_vec())
                            }
                        } else if warp_msg.is_binary() {
                            Message::Binary(warp_msg.as_bytes().to_vec())
                        } else if warp_msg.is_ping() {
                            Message::Ping(warp_msg.as_bytes().to_vec())
                        } else if warp_msg.is_pong() {
                            Message::Pong(warp_msg.as_bytes().to_vec())
                        } else if warp_msg.is_close() {
                            Message::Close(None)
                        } else {
                            // 默认作为二进制消息处理
                            Message::Binary(warp_msg.as_bytes().to_vec())
                        };
                        
                        // 处理消息
                        if let Err(e) = ws_server.handle_message(tungstenite_msg, &connection_id).await {
                            error!("Error handling message: {}", e);
                            if let Err(e) = ws_server.broadcast(WsMessage::Error(e.to_string())).await {
                                error!("Failed to broadcast error: {}", e);
                            }
                            break;
                        }
                    }
                    Err(e) => {
                        error!("WebSocket error: {}", e);
                        break;
                    }
                }
            }
            
            // 连接关闭，清理资源
            ws_server.remove_connection(&connection_id);
        };
        
        // 转发消息到客户端
        let forward_messages = async move {
            while let Some(msg) = rx.next().await {
                // 将 tokio_tungstenite::tungstenite::Message 转换为 warp::ws::Message
                let warp_msg = match msg {
                    Message::Text(text) => WarpMessage::text(text),
                    Message::Binary(data) => WarpMessage::binary(data),
                    Message::Ping(data) => WarpMessage::ping(data),
                    Message::Pong(data) => WarpMessage::pong(data),
                    Message::Close(frame) => {
                        if let Some(frame) = frame {
                            WarpMessage::close_with(frame.code, frame.reason)
                        } else {
                            WarpMessage::close()
                        }
                    },
                    Message::Frame(_) => {
                        // 忽略 Frame 消息
                        continue;
                    }
                };
                
                if let Err(e) = ws_tx.send(warp_msg).await {
                    error!("Failed to send message: {}", e);
                    break;
                }
            }
        };
        
        // 同时运行两个任务
        tokio::select! {
            _ = handle_messages => {},
            _ = forward_messages => {},
        }
    }

    /// 移除连接
    async fn remove_connection(&self, connection_id: &str) {
        let mut connections = self.connections.write().await;
        if connections.remove(connection_id).is_some() {
            self.connection_count.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// 检查心跳
    async fn check_heartbeats(&self) -> Result<()> {
        let mut to_remove = Vec::new();
        let now = Instant::now();
        let timeout = Duration::from_secs(self.websocket_config.heartbeat_interval * 3);

        let connections = self.connections.read().await;
        for (id, conn) in connections.iter() {
            if now.duration_since(conn.last_pong) > timeout {
                to_remove.push(id.clone());
            }
        }
        drop(connections);

        for id in to_remove {
            if let Some(_conn) = self.connections.write().await.remove(&id) {
                self.connection_count.fetch_sub(1, Ordering::SeqCst);
                self.metrics.record_disconnection();
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
            if let Some(_conn) = self.connections.write().await.remove(&id) {
                self.connection_count.fetch_sub(1, Ordering::SeqCst);
                self.metrics.record_disconnection();
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
                if !self.state.exists(&id)? {
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
    pub async fn update_last_pong(&self, connection_id: &str) {
        if let Some(conn) = self.connections.write().await.get_mut(connection_id) {
            conn.update_last_pong();
        }
    }

    /// 广播消息
    pub async fn broadcast(&self, message: WsMessage) -> Result<()> {
        let connections = self.connections.read().await;
        let json = serde_json::to_string(&message)?;
        let ws_message = Message::Text(json);
        
        for connection in connections.values() {
            if let Err(e) = connection.tx.unbounded_send(ws_message.clone()) {
                error!("Failed to send message to connection {}: {}", connection.id, e);
            }
        }
        
        Ok(())
    }

    /// 获取监控指标
    pub async fn get_metrics(&self) -> serde_json::Value {
        serde_json::to_value(self.metrics.get_metrics().await).unwrap()
    }

    async fn handle_message(&self, msg: Message, connection_id: &str) -> Result<()> {
        match msg {
            Message::Text(text) => {
                let ws_message: WsMessage = serde_json::from_str(&text)?;
                match ws_message {
                    WsMessage::Auth(auth) => {
                        // 处理认证
                        match self.verify_token(&auth.token) {
                            Ok(claims) => {
                                let mut connections = self.connections.write().await;
                                if let Some(conn) = connections.get_mut(connection_id) {
                                    conn.state = ConnectionState::Authenticated(claims);
                                }
                                let response = WsMessage::AuthResponse(AuthResponse {
                                    success: true,
                                    error: None,
                                });
                                if let Some(conn) = connections.get(connection_id) {
                                    conn.tx.unbounded_send(Message::Text(serde_json::to_string(&response)?))?;
                                }
                            }
                            Err(e) => {
                                let response = WsMessage::AuthResponse(AuthResponse {
                                    success: false,
                                    error: Some(e.to_string()),
                                });
                                self.broadcast(response).await?;
                            }
                        }
                    }
                    WsMessage::Subscribe(req) => {
                        let subscription_id = self.handle_subscribe(req).await?;
                        let response = WsMessage::SubscriptionConfirmed(subscription_id);
                        self.broadcast(response).await?;
                    }
                    WsMessage::Unsubscribe(req) => {
                        let mut connections = self.connections.write().await;
                        if let Some(conn) = connections.get_mut(connection_id) {
                            conn.subscriptions.remove(&req.subscription_id);
                            let response = WsMessage::UnsubscriptionConfirmed(req.subscription_id);
                            self.broadcast(response).await?;
                        }
                    }
                    WsMessage::Heartbeat(_) => {
                        self.update_last_pong(connection_id).await;
                        let response = WsMessage::Pong;
                        self.broadcast(response).await?;
                    }
                    _ => {
                        error!("Unexpected message type");
                        self.broadcast(WsMessage::Error("Unexpected message type".to_string())).await?;
                    }
                }
            }
            Message::Binary(_) => {
                error!("Binary messages are not supported");
                self.broadcast(WsMessage::Error("Binary messages are not supported".to_string())).await?;
            }
            Message::Ping(_) => {
                self.update_last_pong(connection_id).await;
            }
            Message::Pong(_) => {
                self.update_last_pong(connection_id).await;
            }
            Message::Close(_) => {
                return Ok(());
            }
            Message::Frame(_) => {
                // 忽略 Frame 消息
                debug!("Ignoring Frame message");
            }
        }
        Ok(())
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
    use crate::atomicals::AtomicalType;

    fn create_test_config() -> WebSocketConfig {
        WebSocketConfig {
            max_connections: 10,
            heartbeat_interval: 1,
            heartbeat_timeout: 3,
            cleanup_interval: 1,
            connection_timeout: 60,
            compression_level: CompressionLevel::Fast,
            compression_threshold: 1024,
            jwt_secret: "test_secret".to_string(),
            max_subscriptions_per_connection: 100,
            port: 3000,
            compression_enabled: true,
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

    #[tokio::test]
    async fn test_connection_management() -> Result<()> {
        let config = create_test_config();
        let state = Arc::new(AtomicalsState::new());
        let server = Arc::new(WsServer::new(state, config));

        // 创建测试连接
        let (tx, _rx) = futures::channel::mpsc::unbounded();
        let connection = Connection::new("test_id".to_string(), tx);

        // 添加连接
        {
            let mut connections = server.connections.write().await;
            connections.insert(connection.id.clone(), connection);
            server.connection_count.fetch_add(1, Ordering::SeqCst);
        }

        // 等待一段时间
        sleep(Duration::from_secs(2)).await;

        // 检查心跳
        server.check_heartbeats().await?;

        // 验证连接是否被移除
        {
            let connections = server.connections.read().await;
            assert_eq!(connections.len(), 0);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_broadcast() -> Result<()> {
        let config = create_test_config();
        let state = Arc::new(AtomicalsState::new());
        let server = Arc::new(WsServer::new(state, config));

        // 创建多个测试连接
        for i in 0..3 {
            let (tx, _rx) = futures::channel::mpsc::unbounded();
            let connection = Connection::new(format!("test_id_{}", i), tx);
            let mut connections = server.connections.write().await;
            connections.insert(connection.id.clone(), connection);
            server.connection_count.fetch_add(1, Ordering::SeqCst);
        }

        // 创建测试消息
        let update = AtomicalUpdate {
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
        };
        let msg = WsMessage::AtomicalUpdate(update);

        // 测试广播
        server.broadcast(msg.clone()).await?;

        // 测试无连接时的广播
        assert!(server.broadcast(msg).await.is_ok());

        Ok(())
    }
}

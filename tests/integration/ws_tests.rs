use super::*;
use crate::common::TestDataGenerator;
use crate::atomicals::websocket::{WsMessage, AuthMessage, SubscribeRequest, SubscriptionType};
use futures::{SinkExt, StreamExt};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use serde_json::json;
use std::net::SocketAddr;
use jsonwebtoken::{encode, EncodingKey, Header};

/// WebSocket 测试客户端
struct TestWsClient {
    write: futures::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>
        >,
        Message,
    >,
    read: futures::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>
        >,
    >,
}

impl TestWsClient {
    /// 创建新的测试客户端
    async fn new(addr: SocketAddr) -> anyhow::Result<Self> {
        let url = format!("ws://{}", addr);
        let (ws_stream, _) = connect_async(url).await?;
        let (write, read) = ws_stream.split();
        Ok(Self { write, read })
    }

    /// 发送消息
    async fn send(&mut self, msg: WsMessage) -> anyhow::Result<()> {
        let msg_str = serde_json::to_string(&msg)?;
        self.write.send(Message::Text(msg_str)).await?;
        Ok(())
    }

    /// 接收消息
    async fn receive(&mut self) -> anyhow::Result<WsMessage> {
        if let Some(msg) = self.read.next().await {
            let msg = msg?;
            match msg {
                Message::Text(text) => {
                    let ws_msg: WsMessage = serde_json::from_str(&text)?;
                    Ok(ws_msg)
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            }
        } else {
            Err(anyhow::anyhow!("Connection closed"))
        }
    }
}

/// 测试 WebSocket 连接和认证
#[tokio::test]
async fn test_ws_connection_and_auth() -> anyhow::Result<()> {
    // 1. 设置测试环境
    let env = setup_test_environment();
    
    // 2. 启动 WebSocket 服务器
    let server = Arc::clone(&env.ws_server);
    let server_addr = env.config.ws_addr;
    tokio::spawn(async move {
        server.start(server_addr.port()).await.unwrap();
    });
    sleep(Duration::from_millis(100)).await;
    
    // 3. 创建测试客户端
    let mut client = TestWsClient::new(server_addr).await?;
    
    // 4. 测试无效认证
    let invalid_auth = WsMessage::Auth(AuthMessage {
        token: "invalid_token".to_string(),
    });
    client.send(invalid_auth).await?;
    
    let response = client.receive().await?;
    match response {
        WsMessage::AuthResponse(resp) => {
            assert!(!resp.success);
            assert!(resp.error.is_some());
        }
        _ => panic!("Expected AuthResponse"),
    }
    
    // 5. 测试有效认证
    let valid_token = create_valid_token(&env.config.jwt_secret)?;
    let valid_auth = WsMessage::Auth(AuthMessage {
        token: valid_token,
    });
    client.send(valid_auth).await?;
    
    let response = client.receive().await?;
    match response {
        WsMessage::AuthResponse(resp) => {
            assert!(resp.success);
            assert!(resp.error.is_none());
        }
        _ => panic!("Expected AuthResponse"),
    }
    
    // 6. 清理测试数据
    cleanup_test_data(&env);
    
    Ok(())
}

/// 测试订阅功能
#[tokio::test]
async fn test_ws_subscription() -> anyhow::Result<()> {
    // 1. 设置测试环境
    let env = setup_test_environment();
    
    // 2. 启动 WebSocket 服务器
    let server = Arc::clone(&env.ws_server);
    let server_addr = env.config.ws_addr;
    tokio::spawn(async move {
        server.start(server_addr.port()).await.unwrap();
    });
    sleep(Duration::from_millis(100)).await;
    
    // 3. 创建并认证客户端
    let mut client = TestWsClient::new(server_addr).await?;
    authenticate_client(&mut client, &env.config.jwt_secret).await?;
    
    // 4. 测试订阅
    let atomical_id = TestDataGenerator::generate_atomicals_operation().atomical_id;
    let subscribe_req = WsMessage::Subscribe(SubscribeRequest {
        subscription_type: SubscriptionType::Atomical(atomical_id.clone()),
        params: json!({}),
    });
    client.send(subscribe_req).await?;
    
    let response = client.receive().await?;
    match response {
        WsMessage::SubscriptionConfirmed(sub_id) => {
            assert!(!sub_id.is_empty());
        }
        _ => panic!("Expected SubscriptionConfirmed"),
    }
    
    // 5. 清理测试数据
    cleanup_test_data(&env);
    
    Ok(())
}

/// 测试消息广播
#[tokio::test]
async fn test_ws_broadcast() -> anyhow::Result<()> {
    // 1. 设置测试环境
    let env = setup_test_environment();
    
    // 2. 启动 WebSocket 服务器
    let server = Arc::clone(&env.ws_server);
    let server_addr = env.config.ws_addr;
    tokio::spawn(async move {
        server.start(server_addr.port()).await.unwrap();
    });
    sleep(Duration::from_millis(100)).await;
    
    // 3. 创建多个客户端
    let mut clients = Vec::new();
    for _ in 0..3 {
        let mut client = TestWsClient::new(server_addr).await?;
        authenticate_client(&mut client, &env.config.jwt_secret).await?;
        clients.push(client);
    }
    
    // 4. 订阅相同的 Atomical
    let atomical_id = TestDataGenerator::generate_atomicals_operation().atomical_id;
    for client in &mut clients {
        let subscribe_req = WsMessage::Subscribe(SubscribeRequest {
            subscription_type: SubscriptionType::Atomical(atomical_id.clone()),
            params: json!({}),
        });
        client.send(subscribe_req).await?;
        let _ = client.receive().await?; // 等待订阅确认
    }
    
    // 5. 广播更新消息
    let update = TestDataGenerator::generate_atomicals_operation();
    let update_msg = WsMessage::AtomicalUpdate(update.into());
    env.ws_server.broadcast(update_msg.clone())?;
    
    // 6. 验证所有客户端都收到更新
    for client in &mut clients {
        let received_msg = client.receive().await?;
        assert!(matches!(received_msg, WsMessage::AtomicalUpdate(_)));
    }
    
    // 7. 清理测试数据
    cleanup_test_data(&env);
    
    Ok(())
}

/// 测试心跳机制
#[tokio::test]
async fn test_ws_heartbeat() -> anyhow::Result<()> {
    // 1. 设置测试环境
    let env = setup_test_environment();
    
    // 2. 启动 WebSocket 服务器
    let server = Arc::clone(&env.ws_server);
    let server_addr = env.config.ws_addr;
    tokio::spawn(async move {
        server.start(server_addr.port()).await.unwrap();
    });
    sleep(Duration::from_millis(100)).await;
    
    // 3. 创建客户端
    let mut client = TestWsClient::new(server_addr).await?;
    authenticate_client(&mut client, &env.config.jwt_secret).await?;
    
    // 4. 等待心跳消息
    let heartbeat = client.receive().await?;
    match heartbeat {
        WsMessage::Heartbeat(_) => {
            // 发送 pong 响应
            client.send(WsMessage::Pong).await?;
        }
        _ => panic!("Expected Heartbeat"),
    }
    
    // 5. 清理测试数据
    cleanup_test_data(&env);
    
    Ok(())
}

/// 测试压缩功能
#[tokio::test]
async fn test_ws_compression() -> anyhow::Result<()> {
    // 1. 设置测试环境
    let env = setup_test_environment();
    
    // 2. 启动 WebSocket 服务器
    let server = Arc::clone(&env.ws_server);
    let server_addr = env.config.ws_addr;
    tokio::spawn(async move {
        server.start(server_addr.port()).await.unwrap();
    });
    sleep(Duration::from_millis(100)).await;
    
    // 3. 创建客户端
    let mut client = TestWsClient::new(server_addr).await?;
    authenticate_client(&mut client, &env.config.jwt_secret).await?;
    
    // 4. 发送大消息触发压缩
    let large_data = vec!['x' as u8; 10000];
    let large_msg = WsMessage::Error(String::from_utf8(large_data)?);
    client.send(large_msg).await?;
    
    // 5. 验证消息被正确压缩和解压
    let response = client.receive().await?;
    assert!(matches!(response, WsMessage::Error(_)));
    
    // 6. 清理测试数据
    cleanup_test_data(&env);
    
    Ok(())
}

/// 创建有效的 JWT 令牌
fn create_valid_token(secret: &str) -> anyhow::Result<String> {
    let claims = json!({
        "sub": "test_user",
        "exp": chrono::Utc::now().timestamp() + 3600,
        "permissions": ["read", "write"]
    });
    
    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )?;
    
    Ok(token)
}

/// 认证客户端
async fn authenticate_client(client: &mut TestWsClient, jwt_secret: &str) -> anyhow::Result<()> {
    let token = create_valid_token(jwt_secret)?;
    let auth_msg = WsMessage::Auth(AuthMessage { token });
    client.send(auth_msg).await?;
    
    let response = client.receive().await?;
    match response {
        WsMessage::AuthResponse(resp) if resp.success => Ok(()),
        _ => Err(anyhow::anyhow!("Authentication failed")),
    }
}

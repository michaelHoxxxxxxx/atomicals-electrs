use criterion::{black_box, Criterion};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use std::time::Duration;

use super::common::{TestEnvironment, TestDataGenerator};

pub fn bench_websocket_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let env = TestEnvironment::new();
    let operation = TestDataGenerator::generate_atomicals_operation();
    
    let mut group = c.benchmark_group("WebSocket Operations");
    group.sample_size(50);
    
    // 测试消息广播
    group.bench_function("Message Broadcast", |b| {
        b.to_async(&rt).iter(|| async {
            let msg = operation.clone().into();
            black_box(env.ws_server.broadcast(msg))
        });
    });
    
    // 测试订阅管理
    group.bench_function("Subscription Management", |b| {
        b.to_async(&rt).iter(|| async {
            let req = SubscribeRequest {
                subscription_type: SubscriptionType::Atomical(operation.atomical_id.clone()),
                params: serde_json::json!({}),
            };
            black_box(env.ws_server.handle_subscribe(req).await)
        });
    });
    
    // 测试消息压缩
    group.bench_function("Message Compression", |b| {
        b.to_async(&rt).iter(|| async {
            let large_data = TestDataGenerator::generate_test_data(10000);
            let msg = WsMessage::Binary(large_data);
            black_box(env.ws_server.compress_message(&msg).await)
        });
    });
    
    // 测试并发广播
    group.bench_function("Concurrent Broadcast", |b| {
        b.to_async(&rt).iter(|| async {
            let mut handles = Vec::new();
            for _ in 0..10 {
                let ws_server = Arc::clone(&env.ws_server);
                let msg = operation.clone().into();
                let handle = tokio::spawn(async move {
                    ws_server.broadcast(msg)?;
                    Ok::<_, anyhow::Error>(())
                });
                handles.push(handle);
            }
            
            for handle in handles {
                handle.await??;
            }
            
            Ok::<_, anyhow::Error>(())
        });
    });
    
    // 测试消息处理延迟
    group.bench_function("Message Processing Latency", |b| {
        b.to_async(&rt).iter(|| async {
            let (tx, mut rx) = mpsc::channel(100);
            
            // 启动消息处理器
            let ws_server = Arc::clone(&env.ws_server);
            let handle = tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    ws_server.process_message(msg).await?;
                }
                Ok::<_, anyhow::Error>(())
            });
            
            // 发送消息
            let start = tokio::time::Instant::now();
            for _ in 0..100 {
                let msg = operation.clone().into();
                tx.send(msg).await?;
            }
            
            // 等待处理完成
            drop(tx);
            handle.await??;
            
            Ok::<_, anyhow::Error>(start.elapsed())
        });
    });
    
    group.finish();
}

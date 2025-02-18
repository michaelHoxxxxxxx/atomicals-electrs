use super::*;
use crate::common::TestDataGenerator;
use crate::atomicals::{AtomicalsOperation, AtomicalId, AtomicalType, OperationType};
use bitcoin::{Address, Transaction};
use jsonrpc::{Client, Request, Response};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::time::Duration;

/// RPC 测试客户端
struct TestRpcClient {
    client: Client,
    url: String,
}

impl TestRpcClient {
    /// 创建新的测试客户端
    fn new(addr: SocketAddr) -> Self {
        let url = format!("http://{}", addr);
        let client = Client::new(url.clone(), None, None);
        Self { client, url }
    }

    /// 发送 RPC 请求
    async fn call(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let request = Request {
            method: method.to_string(),
            params: vec![params],
            id: json!(1),
            jsonrpc: "2.0".to_string(),
        };
        
        let response: Response = self.client.send_request(&request).await?;
        match response.result {
            Some(result) => Ok(result),
            None => Err(anyhow::anyhow!("RPC error: {:?}", response.error)),
        }
    }
}

/// 测试 Atomicals 查询接口
#[tokio::test]
async fn test_atomicals_queries() -> anyhow::Result<()> {
    // 1. 设置测试环境
    let env = setup_test_environment();
    
    // 2. 启动 RPC 服务器
    let server_addr = env.config.electrum_rpc_addr;
    tokio::spawn(async move {
        env.daemon.run_rpc_server().await.unwrap();
    });
    sleep(Duration::from_millis(100)).await;
    
    // 3. 创建测试客户端
    let client = TestRpcClient::new(server_addr);
    
    // 4. 测试获取 Atomical 信息
    let atomical = TestDataGenerator::generate_atomicals_operation();
    let result = client.call(
        "blockchain.atomicals.get_atomical_info",
        json!(atomical.atomical_id),
    ).await?;
    
    assert_eq!(result["atomical_id"], json!(atomical.atomical_id));
    assert_eq!(result["atomical_type"], json!(atomical.atomical_type));
    
    // 5. 清理测试数据
    cleanup_test_data(&env);
    
    Ok(())
}

/// 测试地址相关查询
#[tokio::test]
async fn test_address_queries() -> anyhow::Result<()> {
    // 1. 设置测试环境
    let env = setup_test_environment();
    
    // 2. 启动 RPC 服务器
    let server_addr = env.config.electrum_rpc_addr;
    tokio::spawn(async move {
        env.daemon.run_rpc_server().await.unwrap();
    });
    sleep(Duration::from_millis(100)).await;
    
    // 3. 创建测试客户端
    let client = TestRpcClient::new(server_addr);
    
    // 4. 测试获取地址的 Atomicals
    let address = TestDataGenerator::generate_test_address();
    let result = client.call(
        "blockchain.atomicals.list_atomicals_by_address",
        json!(address.to_string()),
    ).await?;
    
    assert!(result.as_array().unwrap().is_empty());
    
    // 5. 清理测试数据
    cleanup_test_data(&env);
    
    Ok(())
}

/// 测试交易相关查询
#[tokio::test]
async fn test_transaction_queries() -> anyhow::Result<()> {
    // 1. 设置测试环境
    let env = setup_test_environment();
    
    // 2. 启动 RPC 服务器
    let server_addr = env.config.electrum_rpc_addr;
    tokio::spawn(async move {
        env.daemon.run_rpc_server().await.unwrap();
    });
    sleep(Duration::from_millis(100)).await;
    
    // 3. 创建测试客户端
    let client = TestRpcClient::new(server_addr);
    
    // 4. 测试获取交易中的 Atomicals 操作
    let tx = TestDataGenerator::generate_test_transaction();
    let result = client.call(
        "blockchain.atomicals.get_tx_atomicals",
        json!(tx.txid().to_string()),
    ).await?;
    
    assert!(result.as_array().unwrap().is_empty());
    
    // 5. 清理测试数据
    cleanup_test_data(&env);
    
    Ok(())
}

/// 测试 UTXO 相关查询
#[tokio::test]
async fn test_utxo_queries() -> anyhow::Result<()> {
    // 1. 设置测试环境
    let env = setup_test_environment();
    
    // 2. 启动 RPC 服务器
    let server_addr = env.config.electrum_rpc_addr;
    tokio::spawn(async move {
        env.daemon.run_rpc_server().await.unwrap();
    });
    sleep(Duration::from_millis(100)).await;
    
    // 3. 创建测试客户端
    let client = TestRpcClient::new(server_addr);
    
    // 4. 测试获取 UTXO 的 Atomicals 信息
    let tx = TestDataGenerator::generate_test_transaction();
    let outpoint = format!("{}:{}", tx.txid(), 0);
    let result = client.call(
        "blockchain.atomicals.get_utxo_atomicals",
        json!(outpoint),
    ).await?;
    
    assert!(result.as_array().unwrap().is_empty());
    
    // 5. 清理测试数据
    cleanup_test_data(&env);
    
    Ok(())
}

/// 测试统计信息查询
#[tokio::test]
async fn test_stats_queries() -> anyhow::Result<()> {
    // 1. 设置测试环境
    let env = setup_test_environment();
    
    // 2. 启动 RPC 服务器
    let server_addr = env.config.electrum_rpc_addr;
    tokio::spawn(async move {
        env.daemon.run_rpc_server().await.unwrap();
    });
    sleep(Duration::from_millis(100)).await;
    
    // 3. 创建测试客户端
    let client = TestRpcClient::new(server_addr);
    
    // 4. 测试获取统计信息
    let result = client.call(
        "blockchain.atomicals.get_stats",
        json!({}),
    ).await?;
    
    assert!(result.get("total_atomicals").is_some());
    assert!(result.get("total_nfts").is_some());
    assert!(result.get("total_ft_holders").is_some());
    
    // 5. 清理测试数据
    cleanup_test_data(&env);
    
    Ok(())
}

/// 测试错误处理
#[tokio::test]
async fn test_error_handling() -> anyhow::Result<()> {
    // 1. 设置测试环境
    let env = setup_test_environment();
    
    // 2. 启动 RPC 服务器
    let server_addr = env.config.electrum_rpc_addr;
    tokio::spawn(async move {
        env.daemon.run_rpc_server().await.unwrap();
    });
    sleep(Duration::from_millis(100)).await;
    
    // 3. 创建测试客户端
    let client = TestRpcClient::new(server_addr);
    
    // 4. 测试无效的 Atomical ID
    let result = client.call(
        "blockchain.atomicals.get_atomical_info",
        json!("invalid_id"),
    ).await;
    assert!(result.is_err());
    
    // 5. 测试无效的地址
    let result = client.call(
        "blockchain.atomicals.list_atomicals_by_address",
        json!("invalid_address"),
    ).await;
    assert!(result.is_err());
    
    // 6. 测试无效的方法
    let result = client.call(
        "blockchain.atomicals.invalid_method",
        json!({}),
    ).await;
    assert!(result.is_err());
    
    // 7. 清理测试数据
    cleanup_test_data(&env);
    
    Ok(())
}

/// 测试并发查询
#[tokio::test]
async fn test_concurrent_queries() -> anyhow::Result<()> {
    // 1. 设置测试环境
    let env = setup_test_environment();
    
    // 2. 启动 RPC 服务器
    let server_addr = env.config.electrum_rpc_addr;
    tokio::spawn(async move {
        env.daemon.run_rpc_server().await.unwrap();
    });
    sleep(Duration::from_millis(100)).await;
    
    // 3. 创建测试客户端
    let client = TestRpcClient::new(server_addr);
    
    // 4. 并发发送多个请求
    let mut handles = Vec::new();
    for _ in 0..10 {
        let client = client.clone();
        let handle = tokio::spawn(async move {
            let result = client.call(
                "blockchain.atomicals.get_stats",
                json!({}),
            ).await;
            assert!(result.is_ok());
        });
        handles.push(handle);
    }
    
    // 5. 等待所有请求完成
    for handle in handles {
        handle.await?;
    }
    
    // 6. 清理测试数据
    cleanup_test_data(&env);
    
    Ok(())
}

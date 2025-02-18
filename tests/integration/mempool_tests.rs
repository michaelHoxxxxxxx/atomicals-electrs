use super::*;
use crate::common::TestDataGenerator;
use crate::atomicals::{AtomicalsOperation, OperationType};
use bitcoin::Transaction;
use std::time::Duration;
use std::collections::HashSet;

/// 测试内存池的基本交易处理
#[tokio::test]
async fn test_mempool_basic_processing() -> anyhow::Result<()> {
    // 1. 设置测试环境
    let env = setup_test_environment();
    
    // 2. 生成测试交易
    let tx = TestDataGenerator::generate_test_transaction();
    
    // 3. 添加到内存池
    env.add_to_mempool(tx.clone()).await?;
    
    // 4. 验证内存池状态
    assert_mempool_state(&env, &tx).await?;
    
    // 5. 清理测试数据
    cleanup_test_data(&env);
    
    Ok(())
}

/// 测试内存池的 Atomicals 操作处理
#[tokio::test]
async fn test_mempool_atomicals_processing() -> anyhow::Result<()> {
    // 1. 设置测试环境
    let env = setup_test_environment();
    
    // 2. 生成包含 Atomicals 操作的测试交易
    let tx = TestDataGenerator::generate_test_transaction();
    let operation = TestDataGenerator::generate_atomicals_operation();
    
    // 3. 添加到内存池
    env.add_to_mempool(tx.clone()).await?;
    
    // 4. 验证 Atomicals 临时状态
    assert_atomicals_temp_state(&env, &tx, &operation).await?;
    
    // 5. 清理测试数据
    cleanup_test_data(&env);
    
    Ok(())
}

/// 测试内存池的冲突检测
#[tokio::test]
async fn test_mempool_conflict_detection() -> anyhow::Result<()> {
    // 1. 设置测试环境
    let env = setup_test_environment();
    
    // 2. 生成两个冲突的交易（使用相同的 UTXO）
    let tx1 = TestDataGenerator::generate_test_transaction();
    let mut tx2 = tx1.clone();
    tx2.output[0].value = 1000; // 修改输出以创建不同的交易
    
    // 3. 添加第一个交易
    env.add_to_mempool(tx1.clone()).await?;
    
    // 4. 尝试添加冲突交易（应该失败）
    let result = env.add_to_mempool(tx2.clone()).await;
    assert!(result.is_err());
    
    // 5. 验证内存池状态（应该只包含第一个交易）
    assert_mempool_state(&env, &tx1).await?;
    
    // 6. 清理测试数据
    cleanup_test_data(&env);
    
    Ok(())
}

/// 测试内存池的 Atomicals 双花检测
#[tokio::test]
async fn test_mempool_atomicals_double_spend() -> anyhow::Result<()> {
    // 1. 设置测试环境
    let env = setup_test_environment();
    
    // 2. 生成两个使用相同 Atomical 的交易
    let tx1 = TestDataGenerator::generate_test_transaction();
    let operation1 = TestDataGenerator::generate_atomicals_operation();
    
    let mut tx2 = tx1.clone();
    tx2.output[0].value = 1000; // 修改输出以创建不同的交易
    let mut operation2 = operation1.clone();
    operation2.operation_type = OperationType::Transfer;
    
    // 3. 添加第一个交易
    env.add_to_mempool(tx1.clone()).await?;
    
    // 4. 尝试添加双花交易（应该失败）
    let result = env.add_to_mempool(tx2.clone()).await;
    assert!(result.is_err());
    
    // 5. 验证 Atomicals 临时状态
    assert_atomicals_temp_state(&env, &tx1, &operation1).await?;
    
    // 6. 清理测试数据
    cleanup_test_data(&env);
    
    Ok(())
}

/// 测试内存池的交易过期和清理
#[tokio::test]
async fn test_mempool_expiry_and_cleanup() -> anyhow::Result<()> {
    // 1. 设置测试环境
    let env = setup_test_environment();
    
    // 2. 生成测试交易
    let tx = TestDataGenerator::generate_test_transaction();
    
    // 3. 添加到内存池
    env.add_to_mempool(tx.clone()).await?;
    
    // 4. 模拟时间流逝
    simulate_network_delay(Duration::from_secs(3600)).await;
    
    // 5. 触发清理
    env.daemon.cleanup_mempool().await?;
    
    // 6. 验证交易已被清理
    assert_mempool_empty(&env).await?;
    
    // 7. 清理测试数据
    cleanup_test_data(&env);
    
    Ok(())
}

/// 测试内存池的大小限制
#[tokio::test]
async fn test_mempool_size_limit() -> anyhow::Result<()> {
    // 1. 设置测试环境
    let env = setup_test_environment();
    
    // 2. 生成多个测试交易直到超过大小限制
    let mut txs = Vec::new();
    let size_limit = env.config.mempool_size_mb * 1024 * 1024;
    let mut current_size = 0;
    
    while current_size < size_limit {
        let tx = TestDataGenerator::generate_test_transaction();
        current_size += tx.size();
        txs.push(tx);
    }
    
    // 3. 添加交易到内存池
    for tx in txs {
        let result = env.add_to_mempool(tx.clone()).await;
        if current_size > size_limit {
            assert!(result.is_err());
            break;
        }
    }
    
    // 4. 验证内存池大小未超过限制
    assert_mempool_size_within_limit(&env).await?;
    
    // 5. 清理测试数据
    cleanup_test_data(&env);
    
    Ok(())
}

/// 验证内存池状态
async fn assert_mempool_state(env: &TestEnvironment, tx: &Transaction) -> anyhow::Result<()> {
    let mempool_txs = env.daemon.get_mempool_transactions().await?;
    assert!(mempool_txs.contains(&tx.txid()));
    Ok(())
}

/// 验证 Atomicals 临时状态
async fn assert_atomicals_temp_state(
    env: &TestEnvironment,
    tx: &Transaction,
    operation: &AtomicalsOperation,
) -> anyhow::Result<()> {
    let temp_state = env.state.get_temp_state(tx.txid()).await?;
    assert!(temp_state.is_some());
    // TODO: 实现更详细的状态验证
    Ok(())
}

/// 验证内存池为空
async fn assert_mempool_empty(env: &TestEnvironment) -> anyhow::Result<()> {
    let mempool_txs = env.daemon.get_mempool_transactions().await?;
    assert!(mempool_txs.is_empty());
    Ok(())
}

/// 验证内存池大小在限制内
async fn assert_mempool_size_within_limit(env: &TestEnvironment) -> anyhow::Result<()> {
    let size_limit = env.config.mempool_size_mb * 1024 * 1024;
    let current_size = env.daemon.get_mempool_size().await?;
    assert!(current_size <= size_limit);
    Ok(())
}

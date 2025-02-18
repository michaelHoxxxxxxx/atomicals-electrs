use criterion::{black_box, Criterion};
use tokio::runtime::Runtime;

use super::common::{TestEnvironment, TestDataGenerator};

pub fn bench_mempool_processing(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let env = TestEnvironment::new();
    let tx = TestDataGenerator::generate_test_transaction();
    
    let mut group = c.benchmark_group("Mempool Processing");
    group.sample_size(50);
    
    // 测试交易验证
    group.bench_function("Transaction Validation", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(env.state.validate_mempool_tx(&tx).await)
        });
    });
    
    // 测试临时状态管理
    group.bench_function("Temporary State Management", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(env.state.process_mempool_tx(&tx).await)
        });
    });
    
    // 测试完整的内存池处理流程
    group.bench_function("Full Mempool Processing", |b| {
        b.to_async(&rt).iter(|| async {
            // 1. 验证交易
            env.state.validate_mempool_tx(&tx).await?;
            
            // 2. 更新临时状态
            env.state.process_mempool_tx(&tx).await?;
            
            // 3. 更新索引
            env.indexer.index_mempool_tx(&tx).await?;
            
            // 4. 广播更新
            let operations = env.indexer.parse_transaction(&tx).await?;
            for op in operations {
                env.ws_server.broadcast(op.into())?;
            }
            
            Ok::<_, anyhow::Error>(())
        });
    });
    
    // 测试并发处理
    group.bench_function("Concurrent Processing", |b| {
        b.to_async(&rt).iter(|| async {
            let mut handles = Vec::new();
            for _ in 0..10 {
                let tx = tx.clone();
                let state = Arc::clone(&env.state);
                let handle = tokio::spawn(async move {
                    state.validate_mempool_tx(&tx).await?;
                    state.process_mempool_tx(&tx).await?;
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
    
    group.finish();
}

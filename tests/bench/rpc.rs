use criterion::{black_box, Criterion};
use tokio::runtime::Runtime;

use super::common::{TestEnvironment, TestDataGenerator};

pub fn bench_rpc_queries(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let env = TestEnvironment::new();
    let operation = TestDataGenerator::generate_atomicals_operation();
    let address = TestDataGenerator::generate_test_address();
    
    let mut group = c.benchmark_group("RPC Queries");
    group.sample_size(50);
    
    // 测试 Atomicals 信息查询
    group.bench_function("Atomicals Info Query", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(env.state.get_atomical_info(&operation.atomical_id).await)
        });
    });
    
    // 测试地址查询
    group.bench_function("Address Query", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(env.state.get_address_atomicals(&address).await)
        });
    });
    
    // 测试复杂查询
    group.bench_function("Complex Query", |b| {
        b.to_async(&rt).iter(|| async {
            // 1. 获取 Atomical 信息
            let info = env.state.get_atomical_info(&operation.atomical_id).await?;
            
            // 2. 获取历史记录
            let history = env.state.get_atomical_history(&operation.atomical_id).await?;
            
            // 3. 获取相关地址
            let addresses = env.state.get_atomical_addresses(&operation.atomical_id).await?;
            
            Ok::<_, anyhow::Error>((info, history, addresses))
        });
    });
    
    // 测试并发查询
    group.bench_function("Concurrent Queries", |b| {
        b.to_async(&rt).iter(|| async {
            let mut handles = Vec::new();
            for _ in 0..10 {
                let state = Arc::clone(&env.state);
                let atomical_id = operation.atomical_id.clone();
                let handle = tokio::spawn(async move {
                    state.get_atomical_info(&atomical_id).await?;
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
    
    // 测试批量查询
    group.bench_function("Batch Queries", |b| {
        b.to_async(&rt).iter(|| async {
            let atomical_ids = (0..10).map(|_| operation.atomical_id.clone()).collect::<Vec<_>>();
            black_box(env.state.get_atomicals_info(&atomical_ids).await)
        });
    });
    
    group.finish();
}

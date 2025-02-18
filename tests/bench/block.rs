use criterion::{black_box, Criterion};
use std::sync::Arc;
use tokio::runtime::Runtime;

use super::common::{TestEnvironment, TestDataGenerator};

pub fn bench_block_processing(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let env = TestEnvironment::new();
    let block = TestDataGenerator::generate_test_block();
    
    let mut group = c.benchmark_group("Block Processing");
    group.sample_size(50);
    
    // 测试区块解析
    group.bench_function("Block Parsing", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(env.indexer.parse_block(&block).await)
        });
    });
    
    // 测试状态更新
    group.bench_function("State Update", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(env.state.process_block(&block).await)
        });
    });
    
    // 测试索引更新
    group.bench_function("Index Update", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(env.indexer.index_block(&block).await)
        });
    });
    
    // 测试完整的区块处理流程
    group.bench_function("Full Block Processing", |b| {
        b.to_async(&rt).iter(|| async {
            // 1. 解析区块
            let operations = env.indexer.parse_block(&block).await?;
            
            // 2. 更新状态
            env.state.process_block(&block).await?;
            
            // 3. 更新索引
            env.indexer.index_block(&block).await?;
            
            // 4. 广播更新
            for op in operations {
                env.ws_server.broadcast(op.into())?;
            }
            
            Ok::<_, anyhow::Error>(())
        });
    });
    
    group.finish();
}

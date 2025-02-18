use criterion::{black_box, Criterion};
use tokio::runtime::Runtime;

use super::common::{TestEnvironment, TestDataGenerator};

pub fn bench_database_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let env = TestEnvironment::new();
    let operation = TestDataGenerator::generate_atomicals_operation();
    
    let mut group = c.benchmark_group("Database Operations");
    group.sample_size(50);
    
    // 准备测试数据
    let key = operation.atomical_id.to_string();
    let value = TestDataGenerator::generate_test_data(1000);
    
    // 写入一些测试数据
    rt.block_on(async {
        env.db.put(&key, &value).await?;
        Ok::<_, anyhow::Error>(())
    }).unwrap();
    
    // 测试读取操作
    group.bench_function("Read Operation", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(env.db.get(&key).await)
        });
    });
    
    // 测试写入操作
    group.bench_function("Write Operation", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(env.db.put(&key, &value).await)
        });
    });
    
    // 测试批量操作
    group.bench_function("Batch Operation", |b| {
        b.to_async(&rt).iter(|| async {
            let batch = TestDataGenerator::generate_test_batch(100);
            let mut db_batch = env.db.batch();
            for (k, v) in batch {
                db_batch.put(&k, &v);
            }
            black_box(db_batch.write().await)
        });
    });
    
    // 测试范围查询
    group.bench_function("Range Query", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(env.db.scan_prefix("test_").await)
        });
    });
    
    // 测试并发读写
    group.bench_function("Concurrent Operations", |b| {
        b.to_async(&rt).iter(|| async {
            let mut handles = Vec::new();
            
            // 并发读取
            for i in 0..5 {
                let db = Arc::clone(&env.db);
                let key = format!("key_{}", i);
                let handle = tokio::spawn(async move {
                    db.get(&key).await?;
                    Ok::<_, anyhow::Error>(())
                });
                handles.push(handle);
            }
            
            // 并发写入
            for i in 5..10 {
                let db = Arc::clone(&env.db);
                let key = format!("key_{}", i);
                let value = TestDataGenerator::generate_test_data(100);
                let handle = tokio::spawn(async move {
                    db.put(&key, &value).await?;
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
    
    // 测试压缩性能
    group.bench_function("Compression Performance", |b| {
        b.to_async(&rt).iter(|| async {
            let large_value = TestDataGenerator::generate_test_data(10000);
            black_box(env.db.put_compressed(&key, &large_value).await)
        });
    });
    
    group.finish();
}

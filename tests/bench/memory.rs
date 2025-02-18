use criterion::{black_box, Criterion};
use tokio::runtime::Runtime;
use std::time::Instant;

use super::common::{TestEnvironment, TestDataGenerator};

pub fn bench_memory_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let env = TestEnvironment::new();
    let operation = TestDataGenerator::generate_atomicals_operation();
    
    let mut group = c.benchmark_group("Memory Operations");
    group.sample_size(50);
    
    // 测试内存分配
    group.bench_function("Memory Allocation", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(env.state.allocate_atomical(&operation).await)
        });
    });
    
    // 测试缓存性能
    rt.block_on(async {
        env.state.cache_atomical(&operation).await.unwrap();
    });
    
    group.bench_function("Cache Performance", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(env.state.get_cached_atomical(&operation.atomical_id).await)
        });
    });
    
    // 测试内存使用
    group.bench_function("Memory Usage", |b| {
        b.iter(|| {
            let start_mem = get_memory_usage();
            let data = TestDataGenerator::generate_test_data(1000000);
            black_box(&data);
            let end_mem = get_memory_usage();
            black_box(end_mem - start_mem)
        });
    });
    
    // 测试缓存淘汰
    group.bench_function("Cache Eviction", |b| {
        b.to_async(&rt).iter(|| async {
            // 添加大量数据到缓存
            for _ in 0..1000 {
                let op = TestDataGenerator::generate_atomicals_operation();
                env.state.cache_atomical(&op).await?;
            }
            
            // 触发缓存淘汰
            env.state.evict_cache().await?;
            
            Ok::<_, anyhow::Error>(())
        });
    });
    
    // 测试内存碎片
    group.bench_function("Memory Fragmentation", |b| {
        b.iter(|| {
            let mut data = Vec::new();
            for _ in 0..1000 {
                data.push(TestDataGenerator::generate_test_data(100));
            }
            for i in (0..1000).step_by(2) {
                data.remove(i);
            }
            black_box(data)
        });
    });
    
    // 测试内存泄漏
    group.bench_function("Memory Leak Detection", |b| {
        b.to_async(&rt).iter(|| async {
            let start_mem = get_memory_usage();
            
            // 执行一系列操作
            for _ in 0..100 {
                let op = TestDataGenerator::generate_atomicals_operation();
                env.state.cache_atomical(&op).await?;
                env.state.evict_cache().await?;
            }
            
            let end_mem = get_memory_usage();
            assert!(end_mem - start_mem < 1024 * 1024); // 确保内存增长不超过 1MB
            
            Ok::<_, anyhow::Error>(())
        });
    });
    
    group.finish();
}

fn get_memory_usage() -> usize {
    #[cfg(target_os = "linux")]
    {
        use std::fs::File;
        use std::io::Read;
        
        let mut status = String::new();
        File::open("/proc/self/status")
            .unwrap()
            .read_to_string(&mut status)
            .unwrap();
        
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                return line
                    .split_whitespace()
                    .nth(1)
                    .unwrap()
                    .parse::<usize>()
                    .unwrap() * 1024;
            }
        }
        0
    }
    #[cfg(not(target_os = "linux"))]
    {
        // 在其他平台上使用一个简单的估计
        std::mem::size_of::<usize>() * 1024 * 1024
    }
}

use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Duration;

mod common;
mod block;
mod mempool;
mod rpc;
mod websocket;
mod database;
mod memory;

pub use common::{TestDataGenerator, TestEnvironment};

fn configure_criterion() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_secs(5))
        .measurement_time(Duration::from_secs(10))
        .sample_size(100)
        .noise_threshold(0.05)
        .configure_from_args()
}

criterion_group! {
    name = atomicals_benches;
    config = configure_criterion();
    targets = 
        block::bench_block_processing,
        mempool::bench_mempool_processing,
        rpc::bench_rpc_queries,
        websocket::bench_websocket_operations,
        database::bench_database_operations,
        memory::bench_memory_operations
}

criterion_main!(atomicals_benches);

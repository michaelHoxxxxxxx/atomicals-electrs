#![feature(test)]

extern crate test;

use test::Bencher;
use bitcoin::{Block, Transaction, Address};
use std::time::Duration;
use std::sync::Arc;

use electrs::atomicals::{
    AtomicalsState, AtomicalsIndexer, AtomicalsOperation,
    websocket::{WsServer, WsMessage},
};
use electrs::config::Config;
use electrs::metrics::Metrics;
use electrs::db::DB;

/// 创建测试环境
fn setup_bench_environment() -> (Arc<AtomicalsState>, Arc<AtomicalsIndexer>, Arc<WsServer>, Arc<DB>) {
    let config = Config::default();
    let metrics = Arc::new(Metrics::new(
        "bench".to_string(),
        "127.0.0.1:0".parse().unwrap(),
    ));
    let db = Arc::new(DB::open(&config).unwrap());
    let state = Arc::new(AtomicalsState::new());
    let indexer = Arc::new(AtomicalsIndexer::new(
        Arc::clone(&state),
        Arc::clone(&db),
        Arc::clone(&metrics),
    ));
    let ws_server = Arc::new(WsServer::new(
        Arc::clone(&state),
        config,
    ));
    
    (state, indexer, ws_server, db)
}

/// 创建测试区块
fn create_test_block() -> Block {
    // 使用比特币测试网的创世区块作为基础
    let mut block = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Testnet);
    
    // 添加一些测试交易
    let tx = create_test_transaction();
    block.txdata.push(tx);
    
    block
}

/// 创建测试交易
fn create_test_transaction() -> Transaction {
    let tx_hex = "0100000001a15d57094aa7a21a28cb20b59aab8fc7d1149a3bdbcddba9c622e4f5f6a99ece010000006c493046022100f93bb0e7d8db7bd46e40132d1f8242026e045f03a0efe71bbb8e3f475e970d790221009337cd7f1f929f00cc6ff01f03729b069a7c21b59b1736ddfee5db5946c5da8c0121033b9b137ee87d5a812d6f506efdd37f0affa7ffc310711c06c7f3e097c9447c52ffffffff0100e1f505000000001976a9140389035a9225b3839e2bbf32d826a1e222031fd888ac00000000";
    let tx_bytes = hex::decode(tx_hex).unwrap();
    bitcoin::consensus::encode::deserialize(&tx_bytes).unwrap()
}

/// 创建测试 Atomicals 操作
fn create_test_atomicals_operation() -> AtomicalsOperation {
    // 从 tests/common/test_data.rs 复用测试数据生成器
    electrs::tests::common::TestDataGenerator::generate_atomicals_operation()
}

// 1. 区块处理性能测试

#[bench]
fn bench_block_parsing(b: &mut Bencher) {
    let (state, indexer, _, _) = setup_bench_environment();
    let block = create_test_block();
    
    b.iter(|| {
        test::black_box(indexer.parse_block(&block));
    });
}

#[bench]
fn bench_state_update(b: &mut Bencher) {
    let (state, _, _, _) = setup_bench_environment();
    let block = create_test_block();
    
    b.iter(|| {
        test::black_box(state.process_block(&block));
    });
}

#[bench]
fn bench_index_update(b: &mut Bencher) {
    let (state, indexer, _, _) = setup_bench_environment();
    let block = create_test_block();
    
    b.iter(|| {
        test::black_box(indexer.index_block(&block));
    });
}

// 2. 内存池性能测试

#[bench]
fn bench_tx_validation(b: &mut Bencher) {
    let (state, _, _, _) = setup_bench_environment();
    let tx = create_test_transaction();
    
    b.iter(|| {
        test::black_box(state.validate_mempool_tx(&tx));
    });
}

#[bench]
fn bench_temp_state_management(b: &mut Bencher) {
    let (state, _, _, _) = setup_bench_environment();
    let tx = create_test_transaction();
    
    b.iter(|| {
        test::black_box(state.process_mempool_tx(&tx));
    });
}

// 3. RPC 性能测试

#[bench]
fn bench_atomicals_queries(b: &mut Bencher) {
    let (state, _, _, _) = setup_bench_environment();
    let operation = create_test_atomicals_operation();
    
    b.iter(|| {
        test::black_box(state.get_atomical_info(&operation.atomical_id));
    });
}

#[bench]
fn bench_address_queries(b: &mut Bencher) {
    let (state, _, _, _) = setup_bench_environment();
    let address = Address::from_str("bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080").unwrap();
    
    b.iter(|| {
        test::black_box(state.get_address_atomicals(&address));
    });
}

// 4. WebSocket 性能测试

#[bench]
fn bench_broadcast(b: &mut Bencher) {
    let (_, _, ws_server, _) = setup_bench_environment();
    let operation = create_test_atomicals_operation();
    let msg = WsMessage::AtomicalUpdate(operation.into());
    
    b.iter(|| {
        test::black_box(ws_server.broadcast(msg.clone()));
    });
}

#[bench]
fn bench_subscription_management(b: &mut Bencher) {
    let (_, _, ws_server, _) = setup_bench_environment();
    let operation = create_test_atomicals_operation();
    
    b.iter(|| {
        test::black_box(ws_server.handle_subscribe(SubscribeRequest {
            subscription_type: SubscriptionType::Atomical(operation.atomical_id.clone()),
            params: serde_json::json!({}),
        }));
    });
}

// 5. 数据库性能测试

#[bench]
fn bench_db_read(b: &mut Bencher) {
    let (_, _, _, db) = setup_bench_environment();
    let operation = create_test_atomicals_operation();
    let key = operation.atomical_id.to_string();
    
    // 先写入一些测试数据
    db.put(&key, &[1, 2, 3]).unwrap();
    
    b.iter(|| {
        test::black_box(db.get(&key));
    });
}

#[bench]
fn bench_db_write(b: &mut Bencher) {
    let (_, _, _, db) = setup_bench_environment();
    let operation = create_test_atomicals_operation();
    let key = operation.atomical_id.to_string();
    let value = vec![1, 2, 3];
    
    b.iter(|| {
        test::black_box(db.put(&key, &value));
    });
}

#[bench]
fn bench_db_batch(b: &mut Bencher) {
    let (_, _, _, db) = setup_bench_environment();
    let operation = create_test_atomicals_operation();
    let key = operation.atomical_id.to_string();
    let value = vec![1, 2, 3];
    
    b.iter(|| {
        let mut batch = db.batch();
        batch.put(&key, &value);
        test::black_box(batch.write());
    });
}

// 6. 内存使用测试

#[bench]
fn bench_memory_allocation(b: &mut Bencher) {
    let (state, _, _, _) = setup_bench_environment();
    let operation = create_test_atomicals_operation();
    
    b.iter(|| {
        test::black_box(state.allocate_atomical(&operation));
    });
}

#[bench]
fn bench_cache_performance(b: &mut Bencher) {
    let (state, _, _, _) = setup_bench_environment();
    let operation = create_test_atomicals_operation();
    
    // 先添加到缓存
    state.cache_atomical(&operation);
    
    b.iter(|| {
        test::black_box(state.get_cached_atomical(&operation.atomical_id));
    });
}

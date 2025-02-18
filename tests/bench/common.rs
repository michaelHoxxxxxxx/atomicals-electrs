use std::sync::Arc;
use std::path::PathBuf;
use tempfile::TempDir;
use bitcoin::{Block, Transaction, Address, Network};
use bitcoin::consensus::encode::serialize;

use electrs::atomicals::{
    AtomicalsState, AtomicalsIndexer, AtomicalsOperation, AtomicalId,
    AtomicalType, OperationType, WsServer,
};
use electrs::config::Config;
use electrs::metrics::Metrics;
use electrs::db::DB;

pub struct TestEnvironment {
    pub temp_dir: TempDir,
    pub config: Config,
    pub db: Arc<DB>,
    pub metrics: Arc<Metrics>,
    pub state: Arc<AtomicalsState>,
    pub indexer: Arc<AtomicalsIndexer>,
    pub ws_server: Arc<WsServer>,
}

impl TestEnvironment {
    pub fn new() -> Self {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::default();
        config.db_dir = temp_dir.path().to_path_buf();
        
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
            config.clone(),
        ));
        
        Self {
            temp_dir,
            config,
            db,
            metrics,
            state,
            indexer,
            ws_server,
        }
    }
}

impl Drop for TestEnvironment {
    fn drop(&mut self) {
        self.temp_dir.close().unwrap();
    }
}

pub struct TestDataGenerator;

impl TestDataGenerator {
    /// 生成测试区块
    pub fn generate_test_block() -> Block {
        // 使用比特币测试网的创世区块作为基础
        let mut block = bitcoin::blockdata::constants::genesis_block(Network::Testnet);
        
        // 添加一些测试交易
        let tx = Self::generate_test_transaction();
        block.txdata.push(tx);
        
        block
    }

    /// 生成测试交易
    pub fn generate_test_transaction() -> Transaction {
        let tx_hex = "0100000001a15d57094aa7a21a28cb20b59aab8fc7d1149a3bdbcddba9c622e4f5f6a99ece010000006c493046022100f93bb0e7d8db7bd46e40132d1f8242026e045f03a0efe71bbb8e3f475e970d790221009337cd7f1f929f00cc6ff01f03729b069a7c21b59b1736ddfee5db5946c5da8c0121033b9b137ee87d5a812d6f506efdd37f0affa7ffc310711c06c7f3e097c9447c52ffffffff0100e1f505000000001976a9140389035a9225b3839e2bbf32d826a1e222031fd888ac00000000";
        let tx_bytes = hex::decode(tx_hex).unwrap();
        bitcoin::consensus::encode::deserialize(&tx_bytes).unwrap()
    }

    /// 生成测试 Atomicals 操作
    pub fn generate_atomicals_operation() -> AtomicalsOperation {
        let txid = bitcoin::Txid::from_str(
            "1234567890123456789012345678901234567890123456789012345678901234"
        ).unwrap();
        
        AtomicalsOperation {
            operation_type: OperationType::Mint,
            atomical_id: AtomicalId { txid, vout: 0 },
            atomical_type: AtomicalType::NFT,
            metadata: Some(serde_json::json!({
                "name": "Test NFT",
                "description": "Test Description",
                "image": "https://example.com/image.png"
            })),
            payload: vec![0x01, 0x02, 0x03],
        }
    }

    /// 生成测试地址
    pub fn generate_test_address() -> Address {
        Address::from_str("bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080").unwrap()
    }

    /// 生成大量测试数据
    pub fn generate_test_data(size: usize) -> Vec<u8> {
        let mut data = Vec::with_capacity(size);
        for i in 0..size {
            data.push((i % 256) as u8);
        }
        data
    }

    /// 生成测试批处理
    pub fn generate_test_batch(size: usize) -> Vec<(String, Vec<u8>)> {
        let mut batch = Vec::with_capacity(size);
        for i in 0..size {
            let key = format!("key_{}", i);
            let value = Self::generate_test_data(100);
            batch.push((key, value));
        }
        batch
    }
}

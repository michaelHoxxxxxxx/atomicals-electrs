use bitcoin::{Block, Transaction, Txid, Script, Address, Network};
use bitcoin::blockdata::constants::genesis_block;
use bitcoin::consensus::encode::serialize;
use bitcoin::util::uint::Uint256;
use bitcoin::hashes::Hash;
use std::str::FromStr;
use crate::atomicals::{AtomicalsOperation, AtomicalId, AtomicalType, OperationType};

pub struct TestDataGenerator;

impl TestDataGenerator {
    /// 生成测试区块
    pub fn generate_test_block() -> Block {
        // 使用比特币测试网的创世区块作为基础
        let mut block = genesis_block(Network::Testnet);
        
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

    /// 生成 Atomicals 操作
    pub fn generate_atomicals_operation() -> AtomicalsOperation {
        AtomicalsOperation {
            operation_type: OperationType::Mint,
            atomical_id: AtomicalId {
                txid: Txid::from_str(
                    "1234567890123456789012345678901234567890123456789012345678901234"
                ).unwrap(),
                vout: 0,
            },
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

    /// 生成测试脚本
    pub fn generate_test_script() -> Script {
        Address::from_str("bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080")
            .unwrap()
            .script_pubkey()
    }

    /// 生成测试区块哈希
    pub fn generate_test_block_hash() -> Uint256 {
        let block = Self::generate_test_block();
        block.block_hash().to_uint256()
    }
}

use bitcoin::Txid;
use parking_lot::RwLock;

use std::collections::HashMap;
use std::sync::Arc;

use crate::metrics::{self, Histogram, Metrics};

pub(crate) struct Cache {
    txs: Arc<RwLock<HashMap<Txid, Box<[u8]>>>>,

    // stats
    txs_size: Histogram,
}

impl Cache {
    pub fn new(metrics: &Metrics) -> Self {
        Cache {
            txs: Default::default(),
            txs_size: metrics.histogram_vec(
                "cache_txs_size",
                "Cached transactions' size (in bytes)",
                "type",
                metrics::default_size_buckets(),
            ),
        }
    }

    pub fn add_tx(&self, txid: Txid, f: impl FnOnce() -> Box<[u8]>) {
        self.txs.write().entry(txid).or_insert_with(|| {
            let tx = f();
            self.txs_size.observe("serialized", tx.len() as f64);
            tx
        });
    }

    pub fn get_tx<F, T>(&self, txid: &Txid, f: F) -> Option<T>
    where
        F: FnOnce(&[u8]) -> T,
    {
        self.txs.read().get(txid).map(|tx_bytes| f(tx_bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::consensus::encode::serialize;
    use bitcoin::Transaction;
    use std::str::FromStr;

    fn create_test_metrics() -> Metrics {
        Metrics::new("test_cache".to_string(), "127.0.0.1:12345".parse().unwrap())
    }

    fn create_test_tx() -> Transaction {
        let tx_hex = "0100000001a15d57094aa7a21a28cb20b59aab8fc7d1149a3bdbcddba9c622e4f5f6a99ece010000006c493046022100f93bb0e7d8db7bd46e40132d1f8242026e045f03a0efe71bbb8e3f475e970d790221009337cd7f1f929f00cc6ff01f03729b069a7c21b59b1736ddfee5db5946c5da8c0121033b9b137ee87d5a812d6f506efdd37f0affa7ffc310711c06c7f3e097c9447c52ffffffff0100e1f505000000001976a9140389035a9225b3839e2bbf32d826a1e222031fd888ac00000000";
        let tx_bytes = hex::decode(tx_hex).unwrap();
        bitcoin::consensus::encode::deserialize(&tx_bytes).unwrap()
    }

    #[test]
    fn test_cache_new() {
        let metrics = create_test_metrics();
        let cache = Cache::new(&metrics);
        assert!(cache.txs.read().is_empty());
    }

    #[test]
    fn test_cache_add_tx() {
        let metrics = create_test_metrics();
        let cache = Cache::new(&metrics);
        let tx = create_test_tx();
        let txid = tx.txid();
        let tx_bytes = serialize(&tx);

        // 测试添加交易
        cache.add_tx(txid, || tx_bytes.clone().into_boxed_slice());
        assert_eq!(cache.txs.read().len(), 1);
        assert!(cache.txs.read().contains_key(&txid));

        // 测试重复添加相同交易
        cache.add_tx(txid, || tx_bytes.clone().into_boxed_slice());
        assert_eq!(cache.txs.read().len(), 1);
    }

    #[test]
    fn test_cache_get_tx() {
        let metrics = create_test_metrics();
        let cache = Cache::new(&metrics);
        let tx = create_test_tx();
        let txid = tx.txid();
        let tx_bytes = serialize(&tx);

        // 测试获取不存在的交易
        let result = cache.get_tx(&txid, |bytes| bytes.len());
        assert!(result.is_none());

        // 添加交易并测试获取
        cache.add_tx(txid, || tx_bytes.clone().into_boxed_slice());
        let result = cache.get_tx(&txid, |bytes| bytes.len());
        assert_eq!(result, Some(tx_bytes.len()));

        // 测试自定义转换函数
        let result = cache.get_tx(&txid, |bytes| {
            bitcoin::consensus::encode::deserialize::<Transaction>(bytes).unwrap()
        });
        assert_eq!(result.unwrap().txid(), tx.txid());
    }

    #[test]
    fn test_cache_metrics() {
        let metrics = create_test_metrics();
        let cache = Cache::new(&metrics);
        let tx = create_test_tx();
        let txid = tx.txid();
        let tx_bytes = serialize(&tx);

        // 添加交易并验证指标
        cache.add_tx(txid, || tx_bytes.clone().into_boxed_slice());
        
        // 验证交易大小指标
        let size = tx_bytes.len() as f64;
        assert!(size > 0.0);
    }
}

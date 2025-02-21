use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use bitcoin::{Amount, OutPoint, Transaction, Txid};
use log::warn;
use parking_lot::RwLock;

use crate::{
    daemon::Daemon,
    metrics::{Gauge, Metrics},
    signals::ExitFlag,
    types::ScriptHash,
    atomicals::{AtomicalsState, AtomicalOperation},
};

#[derive(Debug, Clone)]
pub struct Entry {
    pub txid: Txid,
    pub tx: Transaction,
    pub fee: Amount,
    pub vsize: u64,
    pub has_unconfirmed_inputs: bool,
    pub fee_per_vbyte: f32,
}

/// Mempool current state
#[derive(Debug)]
pub struct Mempool {
    entries: HashMap<Txid, Entry>,
    by_funding: HashSet<(ScriptHash, Txid)>,
    by_spending: HashSet<(OutPoint, Txid)>,
    // stats
    vsize: Gauge,
    count: Gauge,
    atomicals: Arc<AtomicalsState>,
    pending_operations: RwLock<HashMap<Txid, Vec<AtomicalOperation>>>,
    fee_histogram: FeeHistogram,
    stats: RwLock<Stats>,
}

#[derive(Debug, Default)]
struct Stats {
    vsize: f32,
    fee: u64,
    count: usize,
}

impl Mempool {
    pub fn new(metrics: &Metrics, atomicals: Arc<AtomicalsState>) -> Self {
        Self {
            entries: Default::default(),
            by_funding: Default::default(),
            by_spending: Default::default(),
            vsize: metrics.gauge(
                "mempool_txs_vsize",
                "Total vsize of mempool transactions (in bytes)",
                "fee_rate",
            ),
            count: metrics.gauge(
                "mempool_txs_count",
                "Total number of mempool transactions",
                "fee_rate",
            ),
            atomicals,
            pending_operations: RwLock::new(HashMap::new()),
            fee_histogram: FeeHistogram::new(),
            stats: RwLock::new(Stats::default()),
        }
    }

    pub fn get(&self, txid: &Txid) -> Option<&Entry> {
        self.entries.get(txid)
    }

    pub fn filter_by_funding(&self, scripthash: &ScriptHash) -> Vec<&Entry> {
        self.by_funding
            .iter()
            .filter(|(sh, _)| sh == scripthash)
            .filter_map(|(_, txid)| self.entries.get(txid))
            .collect()
    }

    pub fn filter_by_spending(&self, outpoint: &OutPoint) -> Vec<&Entry> {
        self.by_spending
            .iter()
            .filter(|(op, _)| op == outpoint)
            .filter_map(|(_, txid)| self.entries.get(txid))
            .collect()
    }

    pub fn sync(&mut self, daemon: &Daemon, exit_flag: &ExitFlag) {
        let txids = match daemon.get_mempool_txids() {
            Ok(txids) => txids,
            Err(e) => {
                warn!("failed to get mempool txids: {}", e);
                return;
            }
        };

        // Remove transactions that are no longer in mempool
        self.entries.retain(|txid, _| txids.contains(txid));

        // Add new transactions
        for txid in txids {
            if exit_flag.is_set() {
                break;
            }

            if self.entries.contains_key(&txid) {
                continue;
            }

            match daemon.get_mempool_entry(&txid) {
                Ok(entry) => {
                    let tx = match daemon.get_transaction(&txid) {
                        Ok(tx) => tx,
                        Err(e) => {
                            warn!("failed to get transaction {}: {}", txid, e);
                            continue;
                        }
                    };

                    let has_unconfirmed_inputs = tx
                        .input
                        .iter()
                        .any(|txin| self.entries.contains_key(&txin.previous_output.txid));

                    let entry = Entry {
                        txid,
                        tx,
                        fee: Amount::from_sat(entry.fee),
                        vsize: entry.vsize,
                        has_unconfirmed_inputs,
                        fee_per_vbyte: entry.fee as f32 / entry.vsize as f32,
                    };

                    // Update indexes
                    for txin in &entry.tx.input {
                        self.by_spending.insert((txin.previous_output, txid));
                    }
                    for (i, txout) in entry.tx.output.iter().enumerate() {
                        let scripthash = ScriptHash::new(&txout.script_pubkey);
                        self.by_funding.insert((scripthash, txid));
                    }

                    // Update stats
                    self.vsize.inc_by(entry.vsize as i64);
                    self.count.inc();

                    // Process Atomicals operations
                    if let Ok(operations) = self.atomicals.get_operations(&entry.tx) {
                        if !operations.is_empty() {
                            self.pending_operations.write().insert(txid, operations);
                        }
                    }

                    self.entries.insert(txid, entry);
                }
                Err(e) => {
                    warn!("failed to get mempool entry {}: {}", txid, e);
                    continue;
                }
            }
        }
    }

    pub fn fees_histogram(&self) -> &[(f32, u32)] {
        self.fee_histogram.as_slice()
    }

    pub fn update_fees_histogram(&mut self, txs: &[Transaction]) {
        let mut histogram = FeeHistogram::new();
        
        for tx in txs {
            let size = tx.size() as f32;
            let fee_rate = tx.output.iter()
                .map(|o| o.value)
                .sum::<Amount>()
                .to_sat() as f32 / size;
                
            histogram.add(fee_rate, 1);
        }
        
        self.fee_histogram = histogram;
    }

    async fn update_fee_histogram(&mut self) -> Result<()> {
        let mut entries: Vec<Entry> = self.entries.values().cloned().collect();
        entries.sort_unstable_by(|a, b| a.fee_per_vbyte.partial_cmp(&b.fee_per_vbyte).unwrap());

        let mut histogram = self.fee_histogram.write().await;
        histogram.clear();
        let mut vsize_sum = 0f32;

        for entry in entries.iter() {
            if let Ok(operations) = self.atomicals.get_atomical_operations(&entry.tx).await {
                if !operations.is_empty() {
                    vsize_sum += entry.tx.vsize() as f32;
                    histogram.push((entry.fee_per_vbyte, vsize_sum));
                }
            }
        }

        Ok(())
    }

    async fn update_stats(&mut self) -> Result<()> {
        let mut vsize = 0f32;
        let mut fee = 0u64;
        let mut count = 0usize;

        for entry in self.entries.values() {
            let tx = &entry.tx;
            let tx_vsize = tx.vsize() as f32;
            vsize += tx_vsize;
            fee += entry.fee;
            count += 1;
        }

        let mut stats = self.stats.write().await;
        stats.vsize = vsize;
        stats.fee = fee;
        stats.count = count;

        Ok(())
    }

    pub async fn update(&mut self, exit_flag: &ExitFlag) -> Result<()> {
        // 获取新的 mempool 交易
        let new_txids = self.daemon.get_mempool_txids().await?;
        let mut missing_txids = new_txids
            .iter()
            .filter(|txid| !self.entries.contains_key(&txid))
            .cloned()
            .collect::<Vec<Txid>>();

        // 获取新交易详情
        while !missing_txids.is_empty() {
            let batch = missing_txids.split_off(
                missing_txids.len().saturating_sub(1000),
            );
            let txs = self.daemon.get_mempool_txs(&batch).await?;
            for tx in txs {
                let txid = tx.txid();
                let fee = self.daemon.get_mempool_tx_fee(&txid).await?;
                let vsize = tx.vsize() as f32;
                let fee_per_vbyte = fee as f32 / vsize;

                self.entries.insert(
                    txid,
                    Entry {
                        tx,
                        fee,
                        vsize: tx.vsize(),
                        has_unconfirmed_inputs: tx
                            .input
                            .iter()
                            .any(|txin| self.entries.contains_key(&txin.previous_output.txid)),
                        fee_per_vbyte,
                    },
                );
            }
            exit_flag.check().context("mempool update interrupted")?;
        }

        // 移除已确认或过期的交易
        let mut removed = Vec::new();
        for txid in self.entries.keys() {
            if !new_txids.contains(txid) {
                removed.push(*txid);
            }
        }
        for txid in removed {
            self.entries.remove(&txid);
        }

        // 更新统计信息
        self.update_stats().await?;
        self.update_fee_histogram().await?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct FeeHistogram(Vec<(f32, u32)>);

impl FeeHistogram {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn add(&mut self, fee_rate: f32, count: u32) {
        self.0.push((fee_rate, count));
    }

    pub fn as_slice(&self) -> &[(f32, u32)] {
        &self.0
    }
}

/// An update to [`Mempool`]'s internal state. This can be fetched
/// asynchronously using [`MempoolSyncUpdate::poll`], and applied
/// using [`Mempool::apply_sync_update`].
#[derive(Debug)]
pub struct MempoolSyncUpdate {
    pub new_entries: Vec<Entry>,
    pub removed_entries: HashSet<Txid>,
}

impl MempoolSyncUpdate {
    /// Poll the bitcoin node and compute a [`MempoolSyncUpdate`] based on the given set of
    /// `old_txids` which are already cached.
    pub fn poll(
        daemon: &Daemon,
        old_txids: HashSet<Txid>,
        exit_flag: &ExitFlag,
    ) -> Result<MempoolSyncUpdate> {
        let txids = daemon.get_mempool_txids()?;
        debug!("loading {} mempool transactions", txids.len());

        let new_txids = HashSet::<Txid>::from_iter(txids);

        let to_add = &new_txids - &old_txids;
        let to_remove = &old_txids - &new_txids;

        let to_add: Vec<Txid> = to_add.into_iter().collect();
        let mut new_entries = Vec::with_capacity(to_add.len());

        for txids_chunk in to_add.chunks(1000) {
            exit_flag.poll().context("mempool update interrupted")?;
            let entries = daemon.get_mempool_entries(txids_chunk)?;
            ensure!(
                txids_chunk.len() == entries.len(),
                "got {} mempools entries, expected {}",
                entries.len(),
                txids_chunk.len()
            );
            let txs = daemon.get_mempool_transactions(txids_chunk)?;
            ensure!(
                txids_chunk.len() == txs.len(),
                "got {} mempools transactions, expected {}",
                txs.len(),
                txids_chunk.len()
            );
            let chunk_entries: Vec<Entry> = txids_chunk
                .iter()
                .zip(entries.into_iter().zip(txs.into_iter()))
                .filter_map(|(txid, (entry, tx))| {
                    let entry = match entry {
                        Some(entry) => entry,
                        None => {
                            debug!("missing mempool entry: {}", txid);
                            return None;
                        }
                    };
                    let tx = match tx {
                        Some(tx) => tx,
                        None => {
                            debug!("missing mempool tx: {}", txid);
                            return None;
                        }
                    };
                    Some(Entry {
                        txid: *txid,
                        tx,
                        vsize: entry.vsize,
                        fee: entry.fees.base,
                        has_unconfirmed_inputs: !entry.depends.is_empty(),
                        fee_per_vbyte: entry.fees.base as f32 / entry.vsize as f32,
                    })
                })
                .collect();

            new_entries.extend(chunk_entries);
        }

        let update = MempoolSyncUpdate {
            new_entries,
            removed_entries: to_remove,
        };
        Ok(update)
    }
}

#[cfg(test)]
mod tests {
    use super::FeeHistogram;
    use bitcoin::Amount;
    use serde_json::json;

    #[test]
    fn test_histogram() {
        let items = vec![
            (Amount::from_sat(20), 10),
            (Amount::from_sat(10), 10),
            (Amount::from_sat(60), 10),
            (Amount::from_sat(30), 10),
            (Amount::from_sat(70), 10),
            (Amount::from_sat(50), 10),
            (Amount::from_sat(40), 10),
            (Amount::from_sat(80), 10),
            (Amount::from_sat(1), 100),
        ];
        let mut hist = FeeHistogram::default();
        for (amount, vsize) in items {
            let bin_index = FeeHistogram::bin_index(amount, vsize);
            hist.insert(bin_index, vsize);
        }
        assert_eq!(
            json!(hist),
            json!([[15, 10], [7, 40], [3, 20], [1, 10], [0, 100]])
        );

        {
            let bin_index = FeeHistogram::bin_index(Amount::from_sat(5), 1); // 5 sat/byte
            hist.remove(bin_index, 11);
            assert_eq!(
                json!(hist),
                json!([[15, 10], [7, 29], [3, 20], [1, 10], [0, 100]])
            );
        }

        {
            let bin_index = FeeHistogram::bin_index(Amount::from_sat(13), 1); // 13 sat/byte
            hist.insert(bin_index, 80);
            assert_eq!(
                json!(hist),
                json!([[15, 90], [7, 29], [3, 20], [1, 10], [0, 100]])
            );
        }

        {
            let bin_index = FeeHistogram::bin_index(Amount::from_sat(99), 1); // 99 sat/byte
            hist.insert(bin_index, 15);
            assert_eq!(
                json!(hist),
                json!([
                    [127, 15],
                    [63, 0],
                    [31, 0],
                    [15, 90],
                    [7, 29],
                    [3, 20],
                    [1, 10],
                    [0, 100]
                ])
            );
        }
    }
}

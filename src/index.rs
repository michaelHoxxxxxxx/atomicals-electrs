use anyhow::{Context, Result};
use bitcoin::consensus::{deserialize, Decodable, Encodable};
use bitcoin::hashes::Hash;
use bitcoin::{BlockHash, OutPoint, Txid};
use bitcoin_slices::{bsl, Visit, Visitor};
use std::ops::ControlFlow;
use std::thread;

use crate::{
    chain::{Chain, NewHeader},
    daemon::Daemon,
    db::{DBStore, WriteBatch},
    metrics::{self, Gauge, Histogram, Metrics},
    signals::ExitFlag,
    types::{
        bsl_txid, HashPrefixRow, HeaderRow, ScriptHash, ScriptHashRow, SerBlock, SpendingPrefixRow,
        TxidRow,
    },
};

#[derive(Clone, Default)]
struct Stats {
    update_duration: Histogram,
    update_size: Histogram,
    height: Gauge,
    db_properties: Gauge,
}

impl Stats {
    fn new(metrics: &Metrics) -> Self {
        Self {
            update_duration: metrics.histogram_vec(
                "index_update_duration",
                "Index update duration (in seconds)",
                "step",
                metrics::default_duration_buckets(),
            ),
            update_size: metrics.histogram_vec(
                "index_update_size",
                "Index update size (in bytes)",
                "step",
                metrics::default_size_buckets(),
            ),
            height: metrics.gauge("index_height", "Indexed block height", "type"),
            db_properties: metrics.gauge("index_db_properties", "Index DB properties", "name"),
        }
    }

    fn observe_duration<T>(&self, label: &str, f: impl FnOnce() -> T) -> T {
        self.update_duration.observe_duration(label, f)
    }

    fn observe_size<const N: usize>(&self, label: &str, rows: &[[u8; N]]) {
        self.update_size.observe(label, (rows.len() * N) as f64);
    }

    fn observe_batch(&self, batch: &WriteBatch) {
        self.observe_size("write_funding_rows", &batch.funding_rows);
        self.observe_size("write_spending_rows", &batch.spending_rows);
        self.observe_size("write_txid_rows", &batch.txid_rows);
        self.observe_size("write_header_rows", &batch.header_rows);
        debug!(
            "writing {} funding and {} spending rows from {} transactions, {} blocks",
            batch.funding_rows.len(),
            batch.spending_rows.len(),
            batch.txid_rows.len(),
            batch.header_rows.len()
        );
    }

    fn observe_chain(&self, chain: &Chain) {
        self.height.set("tip", chain.height() as f64);
    }

    fn observe_db(&self, store: &DBStore) {
        for (cf, name, value) in store.get_properties() {
            self.db_properties
                .set(&format!("{}:{}", name, cf), value as f64);
        }
    }
}

/// Confirmed transactions' address index
#[derive(Default)]
pub struct Index {
    store: DBStore,
    batch_size: usize,
    lookup_limit: Option<usize>,
    chain: Chain,
    stats: Stats,
    is_ready: bool,
    flush_needed: bool,
}

impl Index {
    pub(crate) fn load(
        store: DBStore,
        mut chain: Chain,
        metrics: &Metrics,
        batch_size: usize,
        lookup_limit: Option<usize>,
        reindex_last_blocks: usize,
    ) -> Result<Self> {
        if let Some(row) = store.get_tip() {
            let tip = deserialize(&row).expect("invalid tip");
            let headers = store
                .iter_headers()
                .map(|row| HeaderRow::from_db_row(row).header);
            chain.load(headers, tip);
            chain.drop_last_headers(reindex_last_blocks);
        };
        let stats = Stats::new(metrics);
        stats.observe_chain(&chain);
        stats.observe_db(&store);
        Ok(Index {
            store,
            batch_size,
            lookup_limit,
            chain,
            stats,
            is_ready: false,
            flush_needed: false,
        })
    }

    pub(crate) fn chain(&self) -> &Chain {
        &self.chain
    }

    pub(crate) fn limit_result<T>(&self, entries: impl Iterator<Item = T>) -> Result<Vec<T>> {
        let mut entries = entries.fuse();
        let result: Vec<T> = match self.lookup_limit {
            Some(lookup_limit) => entries.by_ref().take(lookup_limit).collect(),
            None => entries.by_ref().collect(),
        };
        if entries.next().is_some() {
            bail!(">{} index entries, query may take too long", result.len())
        }
        Ok(result)
    }

    pub(crate) fn filter_by_txid(&self, txid: Txid) -> impl Iterator<Item = BlockHash> + '_ {
        self.store
            .iter_txid(TxidRow::scan_prefix(txid))
            .map(|row| HashPrefixRow::from_db_row(row).height())
            .filter_map(move |height| self.chain.get_block_hash(height))
    }

    pub(crate) fn filter_by_funding(
        &self,
        scripthash: ScriptHash,
    ) -> impl Iterator<Item = BlockHash> + '_ {
        self.store
            .iter_funding(ScriptHashRow::scan_prefix(scripthash))
            .map(|row| HashPrefixRow::from_db_row(row).height())
            .filter_map(move |height| self.chain.get_block_hash(height))
    }

    pub(crate) fn filter_by_spending(
        &self,
        outpoint: OutPoint,
    ) -> impl Iterator<Item = BlockHash> + '_ {
        self.store
            .iter_spending(SpendingPrefixRow::scan_prefix(outpoint))
            .map(|row| HashPrefixRow::from_db_row(row).height())
            .filter_map(move |height| self.chain.get_block_hash(height))
    }

    // Return `Ok(true)` when the chain is fully synced and the index is compacted.
    pub(crate) fn sync(&mut self, daemon: &Daemon, exit_flag: &ExitFlag) -> Result<bool> {
        let new_headers = self
            .stats
            .observe_duration("headers", || daemon.get_new_headers(&self.chain))?;
        match (new_headers.first(), new_headers.last()) {
            (Some(first), Some(last)) => {
                let count = new_headers.len();
                info!(
                    "indexing {} blocks: [{}..{}]",
                    count,
                    first.height(),
                    last.height()
                );
            }
            _ => {
                if self.flush_needed {
                    self.store.flush(); // full compaction is performed on the first flush call
                    self.flush_needed = false;
                }
                self.is_ready = true;
                return Ok(true); // no more blocks to index (done for now)
            }
        }

        thread::scope(|scope| -> Result<()> {
            let (tx, rx) = crossbeam_channel::bounded(1);

            let chunks = new_headers.chunks(self.batch_size);
            let index = &self; // to be moved into reader thread
            let reader = thread::Builder::new()
                .name("index_build".into())
                .spawn_scoped(scope, move || -> Result<()> {
                    for chunk in chunks {
                        exit_flag.poll().with_context(|| {
                            format!(
                                "indexing interrupted at height: {}",
                                chunk.first().unwrap().height()
                            )
                        })?;
                        let batch = index.index_blocks(daemon, chunk)?;
                        tx.send(batch).context("writer disconnected")?;
                    }
                    Ok(()) // `tx` is dropped, to stop the iteration on `rx`
                })
                .expect("spawn failed");

            let index = &self; // to be moved into writer thread
            let writer = thread::Builder::new()
                .name("index_write".into())
                .spawn_scoped(scope, move || {
                    let stats = &index.stats;
                    for mut batch in rx {
                        stats.observe_duration("sort", || batch.sort()); // pre-sort to optimize DB writes
                        stats.observe_batch(&batch);
                        stats.observe_duration("write", || index.store.write(&batch));
                        stats.observe_db(&index.store);
                    }
                })
                .expect("spawn failed");

            reader.join().expect("reader thread panic")?;
            writer.join().expect("writer thread panic");
            Ok(())
        })?;
        self.chain.update(new_headers);
        self.stats.observe_chain(&self.chain);
        self.flush_needed = true;
        Ok(false) // sync is not done
    }

    fn index_blocks(&self, daemon: &Daemon, chunk: &[NewHeader]) -> Result<WriteBatch> {
        let blockhashes: Vec<BlockHash> = chunk.iter().map(|h| h.hash()).collect();
        let mut heights = chunk.iter().map(|h| h.height());

        let mut batch = WriteBatch::default();

        daemon.for_blocks(blockhashes, |blockhash, block| {
            let height = heights.next().expect("unexpected block");
            self.stats.observe_duration("block", || {
                index_single_block(blockhash, block, height, &mut batch);
            });
            self.stats.height.set("tip", height as f64);
        })?;
        let heights: Vec<_> = heights.collect();
        assert!(
            heights.is_empty(),
            "some blocks were not indexed: {:?}",
            heights
        );
        Ok(batch)
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.is_ready
    }
}

fn index_single_block(
    block_hash: BlockHash,
    block: SerBlock,
    height: usize,
    batch: &mut WriteBatch,
) {
    struct IndexBlockVisitor<'a> {
        batch: &'a mut WriteBatch,
        height: usize,
    }

    impl Visitor for IndexBlockVisitor<'_> {
        fn visit_transaction(&mut self, tx: &bsl::Transaction) -> ControlFlow<()> {
            let txid = bsl_txid(tx);
            self.batch
                .txid_rows
                .push(TxidRow::row(txid, self.height).to_db_row());
            ControlFlow::Continue(())
        }

        fn visit_tx_out(&mut self, _vout: usize, tx_out: &bsl::TxOut) -> ControlFlow<()> {
            let script = bitcoin::Script::from_bytes(tx_out.script_pubkey());
            // skip indexing unspendable outputs
            if !script.is_op_return() {
                let row = ScriptHashRow::row(ScriptHash::new(script), self.height);
                self.batch.funding_rows.push(row.to_db_row());
            }
            ControlFlow::Continue(())
        }

        fn visit_tx_in(&mut self, _vin: usize, tx_in: &bsl::TxIn) -> ControlFlow<()> {
            let prevout: OutPoint = tx_in.prevout().into();
            // skip indexing coinbase transactions' input
            if !prevout.is_null() {
                let row = SpendingPrefixRow::row(prevout, self.height);
                self.batch.spending_rows.push(row.to_db_row());
            }
            ControlFlow::Continue(())
        }

        fn visit_block_header(&mut self, header: &bsl::BlockHeader) -> ControlFlow<()> {
            let header = bitcoin::block::Header::consensus_decode(&mut header.as_ref())
                .expect("block header was already validated");
            self.batch
                .header_rows
                .push(HeaderRow::new(header).to_db_row());
            ControlFlow::Continue(())
        }
    }

    let mut index_block = IndexBlockVisitor { batch, height };
    bsl::Block::visit(&block, &mut index_block).expect("core returned invalid block");

    let len = block_hash
        .consensus_encode(&mut (&mut batch.tip_row as &mut [u8]))
        .expect("in-memory writers don't error");
    debug_assert_eq!(len, BlockHash::LEN);
}

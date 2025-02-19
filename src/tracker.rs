use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bitcoin::{BlockHash, Txid};
use bitcoin_slices::{bsl, Error::VisitBreak, Visit, Visitor};
use tokio::time;

use crate::{
    atomicals::AtomicalsState,
    cache::Cache,
    chain::Chain,
    config::Config,
    daemon::Daemon,
    db::DBStore,
    index::Index,
    mempool::{FeeHistogram, Mempool},
    metrics::Metrics,
    signals::ExitFlag,
    status::{Balance, ScriptHashStatus, UnspentEntry},
    types::bsl_txid,
};

/// Electrum protocol subscriptions' tracker
pub struct Tracker {
    index: Index,
    mempool: Mempool,
    metrics: Arc<Metrics>,
    ignore_mempool: bool,
    atomicals: Arc<AtomicalsState>,
    chain: Arc<Chain>,
    daemon: Arc<Daemon>,
}

pub(crate) enum Error {
    NotReady,
}

impl Tracker {
    pub fn new(
        daemon: Arc<Daemon>,
        chain: Arc<Chain>,
        metrics: Arc<Metrics>,
        atomicals: Arc<AtomicalsState>,
    ) -> Self {
        let metrics_ref: &Metrics = &metrics;
        Self {
            daemon,
            chain,
            mempool: Mempool::new(metrics_ref, Arc::clone(&atomicals)),
            metrics,
            atomicals,
            index: Index::default(),
            ignore_mempool: false,
        }
    }

    pub(crate) fn chain(&self) -> &Chain {
        &self.chain
    }

    pub(crate) fn fees_histogram(&self) -> &[(f32, u32)] {
        self.mempool.fees_histogram()
    }

    pub(crate) fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    pub(crate) fn get_unspent(&self, status: &ScriptHashStatus) -> Vec<UnspentEntry> {
        status.get_unspent(self.chain())
    }

    pub(crate) async fn sync(&mut self, exit_flag: &ExitFlag) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_secs(60));

        while !exit_flag.is_set() {
            interval.tick().await;

            // 同步 mempool
            self.mempool.sync(&self.daemon, exit_flag)?;

            // 更新统计信息
            self.update_stats();
        }

        Ok(())
    }

    fn update_stats(&self) {
        // 实现统计信息更新逻辑
    }

    pub fn handle_reorg(&mut self, old_tip: &BlockHash, new_tip: &BlockHash) -> Result<()> {
        self.chain.handle_reorg(old_tip, new_tip)
    }

    pub(crate) fn status(&self) -> Result<(), Error> {
        if self.index.is_ready() {
            return Ok(());
        }
        Err(Error::NotReady)
    }

    pub(crate) fn update_scripthash_status(
        &self,
        status: &mut ScriptHashStatus,
        daemon: &Daemon,
        cache: &Cache,
    ) -> Result<bool> {
        let prev_statushash = status.statushash();
        status.sync(&self.index, &self.mempool, daemon, cache)?;
        Ok(prev_statushash != status.statushash())
    }

    pub(crate) fn get_balance(&self, status: &ScriptHashStatus) -> Balance {
        status.get_balance(self.chain())
    }

    pub(crate) fn lookup_transaction(
        &self,
        daemon: &Daemon,
        txid: Txid,
    ) -> Result<Option<(BlockHash, Box<[u8]>)>> {
        // Note: there are two blocks with coinbase transactions having same txid (see BIP-30)
        let blockhashes = self.index.filter_by_txid(txid);
        let mut result = None;
        daemon.for_blocks(blockhashes, |blockhash, block| {
            if result.is_some() {
                return; // keep first matching transaction
            }
            let mut visitor = FindTransaction::new(txid);
            result = match bsl::Block::visit(&block, &mut visitor) {
                Ok(_) | Err(VisitBreak) => visitor.found.map(|tx| (blockhash, tx)),
                Err(e) => panic!("core returned invalid block: {:?}", e),
            };
        })?;
        Ok(result)
    }

    pub fn track_mempool(&mut self, daemon: &Daemon, exit_flag: &ExitFlag) -> Result<()> {
        self.mempool.sync(daemon, exit_flag);
        while !exit_flag.is_set() {
            self.mempool.sync(daemon, exit_flag);
        }
        Ok(())
    }
}

pub struct FindTransaction {
    txid: Txid,
    found: Option<Box<[u8]>>, // no need to deserialize
}

impl FindTransaction {
    pub fn new(txid: Txid) -> Self {
        Self { txid, found: None }
    }
}
impl Visitor for FindTransaction {
    fn visit_transaction(&mut self, tx: &bsl::Transaction) -> ControlFlow<()> {
        if self.txid == bsl_txid(tx) {
            self.found = Some(tx.as_ref().into());
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }
}

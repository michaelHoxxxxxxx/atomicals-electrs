#[macro_use]
extern crate log;
#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate serde_derive;

pub mod atomicals {
    mod protocol;
    mod state;
    mod storage;
    mod tx_parser;
    mod validation;
    mod rpc;
    mod websocket;
    mod indexer;
    mod metrics;
    mod util;

    pub use protocol::{AtomicalId, AtomicalOperation, AtomicalType};
    pub use state::{AtomicalsState, AtomicalOutput};
    pub use storage::AtomicalsStorage;
    pub use rpc::AtomicalsRpc;
    pub use websocket::{WsServer, WsMessage, SubscriptionType, SubscribeRequest};
    pub use indexer::AtomicalsIndexer;
    pub use validation::AtomicalsValidator;
}

pub mod chain;
pub mod config;
pub mod daemon;
pub mod db;
pub mod index;
pub mod mempool;
pub mod metrics;
pub mod p2p;
pub mod status;
pub mod cache;
pub mod merkle;
pub mod signals;
pub mod thread;
pub mod tracker;
pub mod types;

pub use crate::chain::Chain;
pub use crate::config::Config;
pub use crate::daemon::Daemon;
pub use crate::index::Index;
pub use crate::mempool::Mempool;
pub use crate::metrics::Metrics;
pub use electrs_rocksdb::DB;

pub use atomicals::{
    AtomicalId,
    AtomicalOperation,
    AtomicalType,
    AtomicalsState,
    AtomicalsStorage,
    AtomicalsRpc,
    AtomicalsIndexer,
    AtomicalsValidator,
};

pub use signals::ExitFlag;
pub use types::ScriptHash;

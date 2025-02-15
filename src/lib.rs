#[macro_use]
extern crate anyhow;

#[macro_use]
extern crate serde_derive;

pub mod atomicals {
    mod protocol;
    mod state;
    mod storage;
    mod tx_parser;
    mod rpc;
    mod websocket;

    pub use protocol::{AtomicalId, AtomicalOperation, AtomicalType};
    pub use state::AtomicalsState;
    pub use storage::AtomicalsStorage;
    pub use rpc::AtomicalsRpc;
    pub use websocket::{WsServer, WsMessage, SubscriptionType};
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

pub use crate::chain::Chain;
pub use crate::config::Config;
pub use crate::daemon::Daemon;
pub use crate::index::Index;
pub use crate::mempool::Mempool;
pub use crate::metrics::Metrics;
pub use crate::p2p::P2P;
pub use crate::status::{Status, StatusBackend};

pub use atomicals::{AtomicalId, AtomicalsState, AtomicalOutput};

use anyhow::{bail, Context, Result};
use bitcoin::{
    consensus::{deserialize, encode::serialize_hex},
    hashes::hex::FromHex,
    BlockHash, Txid,
};
use crossbeam_channel::Receiver;
use rayon::prelude::*;
use serde_json::{self, json, Value};
use std::collections::{hash_map::Entry, HashMap};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use crate::{
    atomicals::{
        AtomicalsState, 
        AtomicalsStorage,
        TxParser,
    },
    cache::Cache,
    chain::Chain,
    config::{Config, ELECTRS_VERSION},
    daemon::{self, Daemon, extract_bitcoind_error},
    merkle::Proof,
    metrics::{self, Histogram, Metrics},
    signals::{ExitFlag, Signal},
    status::ScriptHashStatus,
    tracker::Tracker,
    types::ScriptHash,
};

const PROTOCOL_VERSION: &str = "1.4";
const UNKNOWN_FEE: isize = -1; // (allowed by Electrum protocol)

const UNSUBSCRIBED_QUERY_MESSAGE: &str = "your wallet uses less efficient method of querying electrs, consider contacting the developer of your wallet. Reason:";

/// Per-client Electrum protocol state
#[derive(Default)]
pub struct Client {
    tip: Option<BlockHash>,
    scripthashes: HashMap<ScriptHash, ScriptHashStatus>,
}

#[derive(Deserialize)]
struct Request {
    id: Value,
    method: String,

    #[serde(default)]
    params: Value,
}

impl Request {
    fn response(self, result: Result<Value>) -> Value {
        match result {
            Ok(result) => Response::new(self.id, result),
            Err(e) => {
                warn!("rpc #{} {} failed: {}", self.id, self.method, e);
                Response::error(e)
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Requests {
    Single(Request),
    Batch(Vec<Request>),
}

#[derive(Debug, Clone)]
enum VersionRequest {
    Single(String),
    Tuple(String, String),
}

impl ToString for VersionRequest {
    fn to_string(&self) -> String {
        match self {
            VersionRequest::Single(s) => s.clone(),
            VersionRequest::Tuple(_, v) => v.clone(),
        }
    }
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
enum TxGetArgs {
    Txid((Txid,)),
    TxidVerbose(Txid, bool),
}

impl From<&TxGetArgs> for (Txid, bool) {
    fn from(args: &TxGetArgs) -> Self {
        match args {
            TxGetArgs::Txid((txid,)) => (*txid, false),
            TxGetArgs::TxidVerbose(txid, verbose) => (*txid, *verbose),
        }
    }
}

#[derive(Debug)]
enum RpcError {
    ParseError,
    InvalidRequest,
    MethodNotFound,
    InvalidParams,
    InternalError,
    UnavailableIndex,
    DaemonError(daemon::RpcError),
    BadRequest(anyhow::Error),
}

impl RpcError {
    fn code(&self) -> i32 {
        match *self {
            // JSON-RPC 2.0 error codes
            RpcError::ParseError => -32700,
            RpcError::InvalidRequest => -32600,
            RpcError::MethodNotFound => -32601,
            RpcError::InvalidParams => -32602,
            RpcError::InternalError => -32603,
            // Custom error codes
            RpcError::UnavailableIndex => -32000,
            RpcError::DaemonError(ref e) => e.code,
            RpcError::BadRequest(_) => -32001,
        }
    }

    fn message(&self) -> String {
        match *self {
            RpcError::ParseError => "Parse error".to_string(),
            RpcError::InvalidRequest => "Invalid request".to_string(),
            RpcError::MethodNotFound => "Method not found".to_string(),
            RpcError::InvalidParams => "Invalid params".to_string(),
            RpcError::InternalError => "Internal error".to_string(),
            RpcError::UnavailableIndex => "Index is still warming up".to_string(),
            RpcError::DaemonError(ref e) => e.message.clone(),
            RpcError::BadRequest(ref e) => format!("Bad request: {}", e),
        }
    }

    fn from_error(e: anyhow::Error) -> Self {
        if let Some(daemon_error) = e.downcast_ref::<bitcoincore_rpc::Error>() {
            if let Some(rpc_error) = extract_bitcoind_error(daemon_error) {
                RpcError::DaemonError(rpc_error.clone())
            } else {
                RpcError::BadRequest(e)
            }
        } else {
            RpcError::BadRequest(e)
        }
    }
}

/// Electrum RPC handler
pub struct Rpc {
    tracker: Tracker,
    cache: Cache,
    rpc_duration: Histogram,
    daemon: Daemon,
    signal: Signal,
    banner: String,
    port: u16,
}

impl Rpc {
    /// Perform initial index sync (may take a while on first run).
    pub fn new(config: &Config, metrics: Metrics) -> Result<Self> {
        let rpc_duration = metrics.histogram_vec(
            "rpc_duration",
            "RPC duration (in seconds)",
            "method",
            metrics::default_duration_buckets(),
        );

        let signal = Signal::new();
        let metrics_arc = Arc::new(metrics);
        
        // 创建 AtomicalsState
        let storage = Arc::new(AtomicalsStorage::new(&config.db_dir)?);
        let tx_parser = TxParser::new();
        let runtime = tokio::runtime::Runtime::new()?;
        let atomicals_state = Arc::new(runtime.block_on(async {
            AtomicalsState::new(config.network, Arc::clone(&storage), tx_parser).await
        })?);
        
        // 创建 Daemon
        let daemon = Arc::new(Daemon::connect(config, signal.exit_flag(), &metrics_arc)?);
        
        // 创建 Chain
        let chain = Arc::new(Chain::new(config.network, Arc::clone(&atomicals_state)));
        
        // 创建 Tracker
        let tracker = Tracker::new(
            Arc::clone(&daemon),
            Arc::clone(&chain),
            Arc::clone(&metrics_arc),
            Arc::clone(&atomicals_state),
        );
        
        let cache = Cache::new(&metrics_arc);
        
        // 从 Arc 中提取 Daemon
        let daemon_value = match Arc::try_unwrap(daemon) {
            Ok(d) => d,
            Err(arc) => {
                // 如果无法提取，则创建一个新的 Daemon 实例
                Daemon::connect(config, signal.exit_flag(), &metrics_arc)?
            }
        };
        
        Ok(Self {
            tracker,
            cache,
            rpc_duration,
            daemon: daemon_value,
            signal,
            banner: config.server_banner.clone(),
            port: config.electrum_rpc_addr.port(),
        })
    }

    pub(crate) fn signal(&self) -> &Signal {
        &self.signal
    }

    pub fn new_block_notification(&self) -> Receiver<()> {
        self.daemon.new_block_notification()
    }

    pub async fn sync(&mut self) -> Result<bool> {
        self.tracker.sync(self.signal.exit_flag()).await?;
        Ok(true)
    }

    pub fn update_client(&self, client: &mut Client) -> Result<Vec<String>> {
        let chain = self.tracker.chain();
        let mut notifications = client
            .scripthashes
            .par_iter_mut()
            .filter_map(|(scripthash, status)| -> Option<Result<Value>> {
                match self
                    .tracker
                    .update_scripthash_status(status, &self.daemon, &self.cache)
                {
                    Ok(true) => Some(Ok(notification(
                        "blockchain.scripthash.subscribe",
                        &[json!(scripthash), json!(status.statushash())],
                    ))),
                    Ok(false) => None, // statushash is the same
                    Err(e) => Some(Err(e)),
                }
            })
            .collect::<Result<Vec<Value>>>()
            .context("failed to update status")?;

        if let Some(old_tip) = client.tip {
            let new_tip = self.tracker.chain().tip();
            if old_tip != new_tip {
                client.tip = Some(new_tip);
                let height = chain.height();
                let header = chain.get_block_header(height).unwrap();
                notifications.push(notification(
                    "blockchain.headers.subscribe",
                    &[json!({"hex": serialize_hex(&header), "height": height})],
                ));
            }
        }
        Ok(notifications.into_iter().map(|v| v.to_string()).collect())
    }

    fn headers_subscribe(&self, client: &mut Client) -> Result<Value> {
        let chain = self.tracker.chain();
        client.tip = Some(chain.tip());
        let height = chain.height();
        let header = chain.get_block_header(height).unwrap();
        Ok(json!({"hex": serialize_hex(header), "height": height}))
    }

    fn block_header(&self, (height,): (usize,)) -> Result<Value> {
        let chain = self.tracker.chain();
        let header = match chain.get_block_header(height) {
            None => bail!("no header at {}", height),
            Some(header) => header,
        };
        Ok(json!(serialize_hex(header)))
    }

    fn block_headers(&self, (start_height, count): (usize, usize)) -> Result<Value> {
        let chain = self.tracker.chain();
        let max_count = 2016usize;
        // return only the available block headers
        let end_height = std::cmp::min(
            chain.height() + 1,
            start_height + std::cmp::min(count, max_count),
        );
        let heights = start_height..end_height;
        let count = heights.len();
        let hex_headers =
            heights.filter_map(|height| chain.get_block_header(height).map(serialize_hex));

        Ok(json!({"count": count, "hex": String::from_iter(hex_headers), "max": max_count}))
    }

    fn estimate_fee(&self, (nblocks,): (u16,)) -> Result<Value> {
        Ok(self
            .daemon
            .estimate_fee(nblocks)?
            .map(|fee_rate| json!(fee_rate.to_btc()))
            .unwrap_or_else(|| json!(UNKNOWN_FEE)))
    }

    fn relayfee(&self) -> Result<Value> {
        Ok(json!(self.daemon.get_relay_fee()?.to_btc())) // [BTC/kB]
    }

    fn scripthash_get_balance(
        &self,
        client: &Client,
        (scripthash,): &(ScriptHash,),
    ) -> Result<Value> {
        let balance = match client.scripthashes.get(scripthash) {
            Some(status) => self.tracker.get_balance(status),
            None => {
                info!(
                    "{} blockchain.scripthash.get_balance called for unsubscribed scripthash",
                    UNSUBSCRIBED_QUERY_MESSAGE
                );
                self.tracker.get_balance(&self.new_status(*scripthash)?)
            }
        };
        Ok(json!(balance))
    }

    fn scripthash_get_history(
        &self,
        client: &Client,
        (scripthash,): &(ScriptHash,),
    ) -> Result<Value> {
        let history_entries = match client.scripthashes.get(scripthash) {
            Some(status) => json!(status.get_history()),
            None => {
                info!(
                    "{} blockchain.scripthash.get_history called for unsubscribed scripthash",
                    UNSUBSCRIBED_QUERY_MESSAGE
                );
                json!(self.new_status(*scripthash)?.get_history())
            }
        };
        Ok(history_entries)
    }

    fn scripthash_list_unspent(
        &self,
        client: &Client,
        (scripthash,): &(ScriptHash,),
    ) -> Result<Value> {
        let unspent_entries = match client.scripthashes.get(scripthash) {
            Some(status) => self.tracker.get_unspent(status),
            None => {
                info!(
                    "{} blockchain.scripthash.listunspent called for unsubscribed scripthash",
                    UNSUBSCRIBED_QUERY_MESSAGE
                );
                self.tracker.get_unspent(&self.new_status(*scripthash)?)
            }
        };
        Ok(json!(unspent_entries))
    }

    fn scripthash_subscribe(
        &self,
        client: &mut Client,
        (scripthash,): &(ScriptHash,),
    ) -> Result<Value> {
        self.scripthashes_subscribe(client, &[*scripthash])
            .next()
            .unwrap()
    }

    fn scripthash_unsubscribe(
        &self,
        client: &mut Client,
        (scripthash,): &(ScriptHash,),
    ) -> Result<Value> {
        let removed = client.scripthashes.remove(scripthash).is_some();
        Ok(json!(removed))
    }

    fn scripthashes_subscribe<'a>(
        &self,
        client: &'a mut Client,
        scripthashes: &'a [ScriptHash],
    ) -> impl Iterator<Item = Result<Value>> + 'a {
        let new_scripthashes: Vec<ScriptHash> = scripthashes
            .iter()
            .copied()
            .filter(|scripthash| !client.scripthashes.contains_key(scripthash))
            .collect();

        let mut results: HashMap<ScriptHash, Result<ScriptHashStatus>> = new_scripthashes
            .into_par_iter()
            .map(|scripthash| (scripthash, self.new_status(scripthash)))
            .collect();

        scripthashes.iter().map(move |scripthash| {
            let statushash = match client.scripthashes.entry(*scripthash) {
                Entry::Occupied(e) => e.get().statushash(),
                Entry::Vacant(e) => {
                    let status = results
                        .remove(scripthash)
                        .expect("missing scripthash status")?; // return an error for failed subscriptions
                    e.insert(status).statushash()
                }
            };
            Ok(json!(statushash))
        })
    }

    fn new_status(&self, scripthash: ScriptHash) -> Result<ScriptHashStatus> {
        let mut status = ScriptHashStatus::new(scripthash);
        self.tracker
            .update_scripthash_status(&mut status, &self.daemon, &self.cache)?;
        Ok(status)
    }

    fn transaction_broadcast(&self, (tx_hex,): &(String,)) -> Result<Value> {
        let tx_bytes = Vec::from_hex(tx_hex).context("non-hex transaction")?;
        let tx = deserialize(&tx_bytes).context("invalid transaction")?;
        let txid = self.daemon.broadcast(&tx)?;
        Ok(json!(txid))
    }

    fn transaction_get(&self, args: &TxGetArgs) -> Result<Value> {
        let (txid, verbose) = args.into();
        if verbose {
            let blockhash = self
                .tracker
                .lookup_transaction(&self.daemon, txid)?
                .map(|(blockhash, _tx)| blockhash);
            return self.daemon.get_transaction_info(&txid, blockhash);
        }
        // if the scripthash was subscribed, tx should be cached
        if let Some(tx_hex) = self
            .cache
            .get_tx(&txid, |tx_bytes| tx_bytes.to_lower_hex_string())
        {
            return Ok(json!(tx_hex));
        }
        debug!("tx cache miss: txid={}", txid);
        // use internal index to load confirmed transaction
        if let Some(tx_hex) = self
            .tracker
            .lookup_transaction(&self.daemon, txid)?
            .map(|(_blockhash, tx)| tx.to_lower_hex_string())
        {
            return Ok(json!(tx_hex));
        }
        // load unconfirmed transaction via RPC
        Ok(json!(self.daemon.get_transaction_hex(&txid, None)?))
    }

    fn transaction_get_merkle(&self, (txid, height): &(Txid, usize)) -> Result<Value> {
        let chain = self.tracker.chain();
        let blockhash = match chain.get_block_hash(*height) {
            None => bail!("missing block at {}", height),
            Some(blockhash) => blockhash,
        };
        let txids = self.daemon.get_block_txids(blockhash)?;
        match txids.iter().position(|current_txid| *current_txid == *txid) {
            None => bail!("missing txid {} in block {}", txid, blockhash),
            Some(position) => {
                let proof = Proof::create(&txids, position);
                Ok(json!({
                    "block_height": height,
                    "pos": proof.position(),
                    "merkle": proof.to_hex(),
                }))
            }
        }
    }

    fn transaction_from_pos(
        &self,
        (height, tx_pos, merkle): (usize, usize, bool),
    ) -> Result<Value> {
        let chain = self.tracker.chain();
        let blockhash = match chain.get_block_hash(height) {
            None => bail!("missing block at {}", height),
            Some(blockhash) => blockhash,
        };
        let txids = self.daemon.get_block_txids(blockhash)?;
        if tx_pos >= txids.len() {
            bail!("invalid tx_pos {} in block at height {}", tx_pos, height);
        }
        let txid: Txid = txids[tx_pos];
        if merkle {
            let proof = Proof::create(&txids, tx_pos);
            Ok(json!({"tx_id": txid, "merkle": proof.to_hex()}))
        } else {
            Ok(json!({ "tx_id": txid }))
        }
    }

    fn get_fee_histogram(&self) -> Result<Value> {
        Ok(json!(self.tracker.fees_histogram()))
    }

    fn server_id(&self) -> String {
        format!("electrs/{}", ELECTRS_VERSION)
    }

    fn version(&self, (client_id, client_version): &(String, VersionRequest)) -> Result<Value> {
        match client_version {
            VersionRequest::Single(exact) => check_between(PROTOCOL_VERSION, exact, exact),
            VersionRequest::Tuple(min, max) => check_between(PROTOCOL_VERSION, min, max),
        }
        .with_context(|| format!("unsupported request {:?} by {}", client_version, client_id))?;
        Ok(json!([self.server_id(), PROTOCOL_VERSION]))
    }

    fn features(&self) -> Result<Value> {
        Ok(json!({
            "genesis_hash": self.tracker.chain().get_block_hash(0),
            "hosts": { "tcp_port": self.port },
            "protocol_max": PROTOCOL_VERSION,
            "protocol_min": PROTOCOL_VERSION,
            "pruning": null,
            "server_version": self.server_id(),
            "hash_function": "sha256"
        }))
    }

    pub fn handle_requests(&self, client: &mut Client, lines: &[String]) -> Vec<String> {
        lines
            .iter()
            .map(|line| {
                parse_requests(line)
                    .map(Calls::parse)
                    .map_err(error_msg_no_id)
            })
            .map(|calls| self.handle_calls(client, calls).to_string())
            .collect()
    }

    pub fn handle_calls(&self, client: &mut Client, calls: Result<Calls, Value>) -> Value {
        let calls: Calls = match calls {
            Ok(calls) => calls,
            Err(response) => return response, // JSON parsing failed - the response does not contain request id
        };

        match calls {
            Calls::Batch(batch) => {
                if let Some(result) = self.try_multi_call(client, &batch) {
                    return json!(result);
                }
                json!(batch
                    .into_iter()
                    .map(|result| self.single_call(client, result))
                    .collect::<Vec<Value>>())
            }
            Calls::Single(result) => self.single_call(client, result),
        }
    }

    pub fn handle_scripthash_subscriptions(&self, client: &mut Client) -> Result<Vec<String>> {
        let mut result = Vec::new();
        for (script_hash, status_hash) in client.status_hashes() {
            let status = self.tracker.status(&script_hash);
            if status.hash().map(|h| h != *status_hash).unwrap_or(true) {
                result.push(script_hash.to_string());
            }
        }
        Ok(result)
    }

    pub fn handle_call(&self, client: &mut Client, call: Request) -> Value {
        let method = call.method.clone();
        let result = self.rpc_duration.observe_duration(&method, || {
            self.handle_call_internal(client, call)
        });
        match result {
            Ok(result) => result,
            Err(e) => {
                warn!("rpc #{} {} failed: {}", client.id(), method, e);
                Response::error(e)
            }
        }
    }

    fn handle_call_internal(&self, client: &mut Client, call: Request) -> Result<Value> {
        let method = call.method.clone();
        
        // 根据方法名解析参数
        let result = match call.method.as_str() {
            "blockchain.block.header" => {
                match parse_params::<usize>(&call.params) {
                    Ok(height) => self.block_header((height,)),
                    Err(e) => Err(e),
                }
            },
            "blockchain.block.headers" => {
                match parse_params::<(usize, usize)>(&call.params) {
                    Ok(args) => self.block_headers(args),
                    Err(e) => Err(e),
                }
            },
            "blockchain.estimatefee" => {
                match parse_params::<u16>(&call.params) {
                    Ok(blocks) => self.estimate_fee((blocks,)),
                    Err(e) => Err(e),
                }
            },
            "blockchain.headers.subscribe" => self.headers_subscribe(client),
            "blockchain.relayfee" => self.relayfee(),
            "blockchain.scripthash.get_balance" => {
                match parse_params::<ScriptHash>(&call.params) {
                    Ok(script_hash) => {
                        let sh = script_hash.clone();
                        self.scripthash_get_balance(client, &(sh,))
                    },
                    Err(e) => Err(e),
                }
            },
            "blockchain.scripthash.get_history" => {
                match parse_params::<ScriptHash>(&call.params) {
                    Ok(script_hash) => {
                        let sh = script_hash.clone();
                        self.scripthash_get_history(client, &(sh,))
                    },
                    Err(e) => Err(e),
                }
            },
            "blockchain.scripthash.listunspent" => {
                match parse_params::<ScriptHash>(&call.params) {
                    Ok(script_hash) => {
                        let sh = script_hash.clone();
                        self.scripthash_list_unspent(client, &(sh,))
                    },
                    Err(e) => Err(e),
                }
            },
            "blockchain.scripthash.subscribe" => {
                match parse_params::<ScriptHash>(&call.params) {
                    Ok(script_hash) => {
                        let sh = script_hash.clone();
                        self.scripthash_subscribe(client, &(sh,))
                    },
                    Err(e) => Err(e),
                }
            },
            "blockchain.scripthash.unsubscribe" => {
                match parse_params::<ScriptHash>(&call.params) {
                    Ok(script_hash) => {
                        let sh = script_hash.clone();
                        self.scripthash_unsubscribe(client, &(sh,))
                    },
                    Err(e) => Err(e),
                }
            },
            "blockchain.transaction.broadcast" => {
                match parse_params::<String>(&call.params) {
                    Ok(tx_hex) => {
                        let tx = tx_hex.to_string();
                        self.transaction_broadcast(&(tx,))
                    },
                    Err(e) => Err(e),
                }
            },
            "blockchain.transaction.get" => {
                match parse_params::<TxGetArgs>(&call.params) {
                    Ok(args) => self.transaction_get(&args),
                    Err(e) => Err(e),
                }
            },
            "blockchain.transaction.get_merkle" => {
                match parse_params::<(Txid, u32)>(&call.params) {
                    Ok(args) => {
                        let tx_id = args.0.clone();
                        let h = args.1 as usize;
                        self.transaction_get_merkle(&(tx_id, h))
                    },
                    Err(e) => Err(e),
                }
            },
            "blockchain.transaction.id_from_pos" => {
                match parse_params::<(u32, u32, bool)>(&call.params) {
                    Ok(args) => self.transaction_from_pos((args.0 as usize, args.1 as usize, args.2)),
                    Err(e) => Err(e),
                }
            },
            "mempool.get_fee_histogram" => self.get_fee_histogram(),
            "server.banner" => Ok(json!(self.banner)),
            "server.donation_address" => Ok(Value::Null),
            "server.features" => self.features(),
            "server.peers.subscribe" => Ok(json!([])),
            "server.ping" => Ok(Value::Null),
            "server.version" => {
                match parse_params::<VersionRequest>(&call.params) {
                    Ok(client_id) => {
                        let id = client_id.to_string();
                        let client_version = VersionRequest::Single("1.4".to_string());
                        self.version(&(id, client_version))
                    },
                    Err(e) => Err(e),
                }
            },
            _ => {
                Err(anyhow::anyhow!("unknown method: {}", method))
            }
        };
        
        Ok(result?)
    }

    fn try_multi_call(
        &self,
        client: &mut Client,
        calls: &[Result<Call, Value>],
    ) -> Option<Vec<Value>> {
        // exit if any call failed to parse
        let valid_calls = calls
            .iter()
            .map(|result| result.as_ref().ok())
            .collect::<Option<Vec<&Call>>>()?;

        // only "blockchain.scripthashes.subscribe" are supported
        let scripthashes: Vec<ScriptHash> = valid_calls
            .iter()
            .map(|call| match &call.params {
                Params::ScriptHashSubscribe((scripthash,)) => Some(*scripthash),
                _ => None, // exit if any of the calls is not supported
            })
            .collect::<Option<Vec<ScriptHash>>>()?;

        Some(
            self.rpc_duration
                .observe_duration("blockchain.scripthash.subscribe:multi", || {
                    self.scripthashes_subscribe(client, &scripthashes)
                        .zip(valid_calls)
                        .map(|(result, call)| call.response(result))
                        .collect::<Vec<Value>>()
                }),
        )
    }

    fn single_call(&self, client: &mut Client, call: Result<Call, Value>) -> Value {
        let call = match call {
            Ok(call) => call,
            Err(response) => return response, // params parsing may fail - the response contains request id
        };
        self.rpc_duration.observe_duration(&call.method, || {
            if self.tracker.status().is_err() {
                // Allow only a few RPC (for sync status notification) not requiring index DB being compacted.
                match &call.params {
                    Params::Banner => (),
                    Params::BlockHeader(_) => (),
                    Params::BlockHeaders(_) => (),
                    Params::HeadersSubscribe => (),
                    Params::Version(_) => (),
                    _ => return error_msg(&call.id, RpcError::UnavailableIndex),
                };
            }
            let result = match call.method.as_str() {
                "server.banner" => Ok(json!(self.banner)),
                "blockchain.block.header" => {
                    match parse_params::<u32>(&call.params) {
                        Ok(height) => self.block_header((height as usize,)),
                        Err(e) => Err(e),
                    }
                },
                "blockchain.block.headers" => {
                    match parse_params::<(u32, u32)>(&call.params) {
                        Ok((start_height, count)) => self.block_headers((start_height as usize, count as usize)),
                        Err(e) => Err(e),
                    }
                },
                "server.donation_address" => Ok(Value::Null),
                "blockchain.estimatefee" => {
                    match parse_params::<u16>(&call.params) {
                        Ok(blocks) => self.estimate_fee((blocks,)),
                        Err(e) => Err(e),
                    }
                },
                "server.features" => self.features(),
                "blockchain.headers.subscribe" => self.headers_subscribe(client),
                "mempool.get_fee_histogram" => self.get_fee_histogram(),
                "server.peers.subscribe" => Ok(json!([])),
                "server.ping" => Ok(Value::Null),
                "blockchain.relayfee" => self.relayfee(),
                "blockchain.scripthash.get_balance" => {
                    match parse_params::<ScriptHash>(&call.params) {
                        Ok(script_hash) => {
                            let sh = script_hash.clone();
                            self.scripthash_get_balance(client, &(sh,))
                        },
                        Err(e) => Err(e),
                    }
                },
                "blockchain.scripthash.get_history" => {
                    match parse_params::<ScriptHash>(&call.params) {
                        Ok(script_hash) => {
                            let sh = script_hash.clone();
                            self.scripthash_get_history(client, &(sh,))
                        },
                        Err(e) => Err(e),
                    }
                },
                "blockchain.scripthash.listunspent" => {
                    match parse_params::<ScriptHash>(&call.params) {
                        Ok(script_hash) => {
                            let sh = script_hash.clone();
                            self.scripthash_list_unspent(client, &(sh,))
                        },
                        Err(e) => Err(e),
                    }
                },
                "blockchain.scripthash.subscribe" => {
                    match parse_params::<ScriptHash>(&call.params) {
                        Ok(script_hash) => {
                            let sh = script_hash.clone();
                            self.scripthash_subscribe(client, &(sh,))
                        },
                        Err(e) => Err(e),
                    }
                },
                "blockchain.scripthash.unsubscribe" => {
                    match parse_params::<ScriptHash>(&call.params) {
                        Ok(script_hash) => {
                            let sh = script_hash.clone();
                            self.scripthash_unsubscribe(client, &(sh,))
                        },
                        Err(e) => Err(e),
                    }
                },
                "blockchain.transaction.broadcast" => {
                    match parse_params::<String>(&call.params) {
                        Ok(tx_hex) => {
                            let tx = tx_hex.to_string();
                            self.transaction_broadcast(&(tx,))
                        },
                        Err(e) => Err(e),
                    }
                },
                "blockchain.transaction.get" => {
                    match parse_params::<TxGetArgs>(&call.params) {
                        Ok(args) => self.transaction_get(&args),
                        Err(e) => Err(e),
                    }
                },
                "blockchain.transaction.get_merkle" => {
                    match parse_params::<(Txid, u32)>(&call.params) {
                        Ok((txid, height)) => {
                            self.transaction_get_merkle(&(txid, height as usize))
                        },
                        Err(e) => Err(e),
                    }
                },
                "blockchain.transaction.id_from_pos" => {
                    match parse_params::<(u32, u32, bool)>(&call.params) {
                        Ok((height, tx_pos, merkle)) => {
                            self.transaction_from_pos((height as usize, tx_pos as usize, merkle))
                        },
                        Err(e) => Err(e),
                    }
                },
                "server.version" => {
                    match parse_params::<VersionRequest>(&call.params) {
                        Ok(client_id) => {
                            let id = client_id.to_string();
                            let client_version = VersionRequest::Single("1.4".to_string());
                            self.version(&(id, client_version))
                        },
                        Err(e) => Err(e),
                    }
                },
                _ => {
                    Err(anyhow::anyhow!("unknown method: {}", call.method))
                }
            };
            
            call.response(result)
        })
    }

    fn handle_multi_call(&self, client: &mut Client, calls: Calls) -> Value {
        match calls {
            Calls::Single(call_result) => match call_result {
                Ok(call) => {
                    if let Err(e) = check_request(&call) {
                        return call.response(Err(e));
                    }
                    
                    if self.tracker.status().is_err() {
                        // Allow only a few RPC (for sync status notification) not requiring index DB being compacted.
                        if !call.method.starts_with("blockchain.headers.subscribe") 
                           && !call.method.starts_with("server.version") 
                           && !call.method.starts_with("blockchain.block.header") 
                           && !call.method.starts_with("blockchain.block.headers") {
                            return error_msg(&call.id, RpcError::UnavailableIndex);
                        }
                    }
                    
                    let result = match call.method.as_str() {
                        "server.banner" => Ok(json!(self.banner)),
                        "blockchain.block.header" => {
                            match parse_params::<u32>(&call.params) {
                                Ok(height) => self.block_header((height as usize,)),
                                Err(e) => Err(e),
                            }
                        },
                        "blockchain.block.headers" => {
                            match parse_params::<(u32, u32)>(&call.params) {
                                Ok((start_height, count)) => self.block_headers((start_height as usize, count as usize)),
                                Err(e) => Err(e),
                            }
                        },
                        "server.donation_address" => Ok(Value::Null),
                        "blockchain.estimatefee" => {
                            match parse_params::<u16>(&call.params) {
                                Ok(blocks) => self.estimate_fee((blocks,)),
                                Err(e) => Err(e),
                            }
                        },
                        "server.features" => self.features(),
                        "blockchain.headers.subscribe" => self.headers_subscribe(client),
                        "mempool.get_fee_histogram" => self.get_fee_histogram(),
                        "server.peers.subscribe" => Ok(json!([])),
                        "server.ping" => Ok(Value::Null),
                        "blockchain.relayfee" => self.relayfee(),
                        "blockchain.scripthash.get_balance" => {
                            match parse_params::<ScriptHash>(&call.params) {
                                Ok(script_hash) => {
                                    let sh = script_hash.clone();
                                    self.scripthash_get_balance(client, &(sh,))
                                },
                                Err(e) => Err(e),
                            }
                        },
                        "blockchain.scripthash.get_history" => {
                            match parse_params::<ScriptHash>(&call.params) {
                                Ok(script_hash) => {
                                    let sh = script_hash.clone();
                                    self.scripthash_get_history(client, &(sh,))
                                },
                                Err(e) => Err(e),
                            }
                        },
                        "blockchain.scripthash.listunspent" => {
                            match parse_params::<ScriptHash>(&call.params) {
                                Ok(script_hash) => {
                                    let sh = script_hash.clone();
                                    self.scripthash_list_unspent(client, &(sh,))
                                },
                                Err(e) => Err(e),
                            }
                        },
                        "blockchain.scripthash.subscribe" => {
                            match parse_params::<ScriptHash>(&call.params) {
                                Ok(script_hash) => {
                                    let sh = script_hash.clone();
                                    self.scripthash_subscribe(client, &(sh,))
                                },
                                Err(e) => Err(e),
                            }
                        },
                        "blockchain.scripthash.unsubscribe" => {
                            match parse_params::<ScriptHash>(&call.params) {
                                Ok(script_hash) => {
                                    let sh = script_hash.clone();
                                    self.scripthash_unsubscribe(client, &(sh,))
                                },
                                Err(e) => Err(e),
                            }
                        },
                        "blockchain.transaction.broadcast" => {
                            match parse_params::<String>(&call.params) {
                                Ok(tx_hex) => {
                                    let tx = tx_hex.to_string();
                                    self.transaction_broadcast(&(tx,))
                                },
                                Err(e) => Err(e),
                            }
                        },
                        "blockchain.transaction.get" => {
                            match parse_params::<TxGetArgs>(&call.params) {
                                Ok(args) => self.transaction_get(&args),
                                Err(e) => Err(e),
                            }
                        },
                        "blockchain.transaction.get_merkle" => {
                            match parse_params::<(Txid, u32)>(&call.params) {
                                Ok((txid, height)) => {
                                    self.transaction_get_merkle(&(txid, height as usize))
                                },
                                Err(e) => Err(e),
                            }
                        },
                        "blockchain.transaction.id_from_pos" => {
                            match parse_params::<(u32, u32, bool)>(&call.params) {
                                Ok((height, tx_pos, merkle)) => {
                                    self.transaction_from_pos((height as usize, tx_pos as usize, merkle))
                                },
                                Err(e) => Err(e),
                            }
                        },
                        "server.version" => {
                            match parse_params::<VersionRequest>(&call.params) {
                                Ok(client_id) => {
                                    let id = client_id.to_string();
                                    let client_version = VersionRequest::Single("1.4".to_string());
                                    self.version(&(id, client_version))
                                },
                                Err(e) => Err(e),
                            }
                        },
                        _ => {
                            Err(anyhow::anyhow!("unknown method: {}", call.method))
                        }
                    };
                    
                    call.response(result)
                }
                Err(response) => response,
            },
            Calls::Batch(batch) => {
                let mut responses = Vec::with_capacity(batch.len());
                for call_result in batch {
                    match call_result {
                        Ok(call) => {
                            if let Err(e) = check_request(&call) {
                                responses.push(call.response(Err(e)));
                                continue;
                            }
                            
                            if self.tracker.status().is_err() {
                                // Allow only a few RPC (for sync status notification) not requiring index DB being compacted.
                                if !call.method.starts_with("blockchain.headers.subscribe") 
                                   && !call.method.starts_with("server.version") 
                                   && !call.method.starts_with("blockchain.block.header") 
                                   && !call.method.starts_with("blockchain.block.headers") {
                                    responses.push(error_msg(&call.id, RpcError::UnavailableIndex));
                                    continue;
                                }
                            }
                            
                            let result = match call.method.as_str() {
                                "server.banner" => Ok(json!(self.banner)),
                                "blockchain.block.header" => {
                                    match parse_params::<u32>(&call.params) {
                                        Ok(height) => self.block_header((height as usize,)),
                                        Err(e) => Err(e),
                                    }
                                },
                                "blockchain.block.headers" => {
                                    match parse_params::<(u32, u32)>(&call.params) {
                                        Ok((start_height, count)) => self.block_headers((start_height as usize, count as usize)),
                                        Err(e) => Err(e),
                                    }
                                },
                                "server.donation_address" => Ok(Value::Null),
                                "blockchain.estimatefee" => {
                                    match parse_params::<u16>(&call.params) {
                                        Ok(blocks) => self.estimate_fee((blocks,)),
                                        Err(e) => Err(e),
                                    }
                                },
                                "server.features" => self.features(),
                                "blockchain.headers.subscribe" => self.headers_subscribe(client),
                                "mempool.get_fee_histogram" => self.get_fee_histogram(),
                                "server.peers.subscribe" => Ok(json!([])),
                                "server.ping" => Ok(Value::Null),
                                "blockchain.relayfee" => self.relayfee(),
                                "blockchain.scripthash.get_balance" => {
                                    match parse_params::<ScriptHash>(&call.params) {
                                        Ok(script_hash) => {
                                            let sh = script_hash.clone();
                                            self.scripthash_get_balance(client, &(sh,))
                                        },
                                        Err(e) => Err(e),
                                    }
                                },
                                "blockchain.scripthash.get_history" => {
                                    match parse_params::<ScriptHash>(&call.params) {
                                        Ok(script_hash) => {
                                            let sh = script_hash.clone();
                                            self.scripthash_get_history(client, &(sh,))
                                        },
                                        Err(e) => Err(e),
                                    }
                                },
                                "blockchain.scripthash.listunspent" => {
                                    match parse_params::<ScriptHash>(&call.params) {
                                        Ok(script_hash) => {
                                            let sh = script_hash.clone();
                                            self.scripthash_list_unspent(client, &(sh,))
                                        },
                                        Err(e) => Err(e),
                                    }
                                },
                                "blockchain.scripthash.subscribe" => {
                                    match parse_params::<ScriptHash>(&call.params) {
                                        Ok(script_hash) => {
                                            let sh = script_hash.clone();
                                            self.scripthash_subscribe(client, &(sh,))
                                        },
                                        Err(e) => Err(e),
                                    }
                                },
                                "blockchain.scripthash.unsubscribe" => {
                                    match parse_params::<ScriptHash>(&call.params) {
                                        Ok(script_hash) => {
                                            let sh = script_hash.clone();
                                            self.scripthash_unsubscribe(client, &(sh,))
                                        },
                                        Err(e) => Err(e),
                                    }
                                },
                                "blockchain.transaction.broadcast" => {
                                    match parse_params::<String>(&call.params) {
                                        Ok(tx_hex) => {
                                            let tx = tx_hex.to_string();
                                            self.transaction_broadcast(&(tx,))
                                        },
                                        Err(e) => Err(e),
                                    }
                                },
                                "blockchain.transaction.get" => {
                                    match parse_params::<TxGetArgs>(&call.params) {
                                        Ok(args) => self.transaction_get(&args),
                                        Err(e) => Err(e),
                                    }
                                },
                                "blockchain.transaction.get_merkle" => {
                                    match parse_params::<(Txid, u32)>(&call.params) {
                                        Ok((txid, height)) => {
                                            self.transaction_get_merkle(&(txid, height as usize))
                                        },
                                        Err(e) => Err(e),
                                    }
                                },
                                "blockchain.transaction.id_from_pos" => {
                                    match parse_params::<(u32, u32, bool)>(&call.params) {
                                        Ok((height, tx_pos, merkle)) => {
                                            self.transaction_from_pos((height as usize, tx_pos as usize, merkle))
                                        },
                                        Err(e) => Err(e),
                                    }
                                },
                                "server.version" => {
                                    match parse_params::<VersionRequest>(&call.params) {
                                        Ok(client_id) => {
                                            let id = client_id.to_string();
                                            let client_version = VersionRequest::Single("1.4".to_string());
                                            self.version(&(id, client_version))
                                        },
                                        Err(e) => Err(e),
                                    }
                                },
                                _ => {
                                    Err(anyhow::anyhow!("unknown method: {}", call.method))
                                }
                            };
                            
                            responses.push(call.response(result));
                        }
                        Err(response) => responses.push(response),
                    }
                }
                json!(responses)
            }
        }
    }
}

enum Params {
    Banner,
    BlockHeader(u32),
    BlockHeaders(BlockHeadersArgs),
    Donation,
    EstimateFee(u16),
    Features,
    HeadersSubscribe,
    MempoolFeeHistogram,
    PeersSubscribe,
    Ping,
    RelayFee,
    ScriptHashGetBalance(ScriptHash),
    ScriptHashGetHistory(ScriptHash),
    ScriptHashListUnspent(ScriptHash),
    ScriptHashSubscribe(ScriptHash),
    ScriptHashUnsubscribe(ScriptHash),
    TransactionBroadcast(String),
    TransactionGet(TxGetArgs),
    TransactionGetMerkle((Txid, u32)),
    TransactionFromPosition((u32, u32, bool)),
    Version(VersionRequest),
}

#[derive(Debug, Clone, Copy)]
struct BlockHeadersArgs {
    start_height: u32,
    count: u32,
}

#[derive(Debug, Clone, Copy)]
struct TransactionPositionArgs {
    height: u32,
    position: u32,
    merkle: bool,
}

struct Call {
    id: Value,
    method: String,
    params: Value,
}

impl Call {
    fn from_request(request: Request) -> std::result::Result<Call, Value> {
        let id = request.id;
        let method = request.method;
        let params = request.params;
        Ok(Call { id, method, params })
    }

    fn response(&self, result: Result<Value>) -> Value {
        match result {
            Ok(result) => Response::new(self.id.clone(), result),
            Err(err) => {
                warn!("RPC {} failed: {:#}", self.method, err);
                error_msg(&self.id, RpcError::from_error(err))
            }
        }
    }
}

enum Calls {
    Single(std::result::Result<Call, Value>),
    Batch(Vec<std::result::Result<Call, Value>>),
}

impl Calls {
    fn parse(requests: Requests) -> Calls {
        match requests {
            Requests::Single(request) => Calls::Single(Call::from_request(request)),
            Requests::Batch(batch) => {
                Calls::Batch(batch.into_iter().map(Call::from_request).collect())
            }
        }
    }
}

fn parse_params<T>(params: &Value) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(params.clone())
        .with_context(|| format!("invalid params: {}", params))
}

fn convert<T>(params: Value) -> std::result::Result<T, RpcError>
where
    T: serde::de::DeserializeOwned,
{
    let params_str = params.to_string();
    serde_json::from_value(params).map_err(|err| {
        warn!("invalid params {}: {}", params_str, err);
        RpcError::InvalidParams
    })
}

fn notification(method: &str, params: &[Value]) -> Value {
    json!({"jsonrpc": "2.0", "method": method, "params": params})
}

fn result_msg(id: &Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn error_msg(id: &Value, error: RpcError) -> Value {
    error_code_msg(id, error.code(), error.message())
}

fn error_msg_no_id(err: RpcError) -> Value {
    json!({
        "jsonrpc": "2.0",
        "error": {
            "code": err.code(),
            "message": err.message(),
        },
        "id": Value::Null,
    })
}

fn parse_requests(line: &str) -> Result<Requests, RpcError> {
    serde_json::from_str(line).map_err(|_| RpcError::ParseError)
}

fn check_request(call: &Call) -> Result<()> {
    if call.method.starts_with("blockchain.") && !call.method.starts_with("blockchain.atomicals.") {
        // Allow standard blockchain.* methods
        return Ok(());
    }
    
    if call.method.starts_with("server.") {
        // Allow standard server.* methods
        return Ok(());
    }
    
    if call.method.starts_with("mempool.") {
        // Allow standard mempool.* methods
        return Ok(());
    }
    
    Err(anyhow::anyhow!("method not found: {}", call.method))
}

#[derive(Debug)]
struct Response {
    id: Value,
    result: Value,
}

impl Response {
    fn new(id: Value, result: Value) -> Value {
        json!({
            "id": id,
            "result": result,
        })
    }

    fn error<E: std::error::Error>(e: E) -> Value {
        json!({
            "error": {
                "code": -1,
                "message": e.to_string(),
            },
            "id": Value::Null,
        })
    }
}

fn parse_version(version: &str) -> Result<Version> {
    let result = version
        .split('.')
        .map(|part| part.parse::<usize>().with_context(|| format!("invalid version {}", version)))
        .collect::<Result<Vec<usize>>>()?;
    if result.len() < 2 {
        bail!("invalid version: {}", version);
    }
    Ok(Version {
        major: result[0],
        minor: result[1],
        patch: result.get(2).cloned().unwrap_or(0),
    })
}

#[derive(PartialOrd, PartialEq, Debug)]
struct Version {
    major: usize,
    minor: usize,
    patch: usize,
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (i, v) in [&self.major, &self.minor, &self.patch].iter().enumerate() {
            if i > 0 {
                write!(f, ".")?;
            }
            write!(f, "{}", v)?;
        }
        Ok(())
    }
}

fn check_between(version_str: &str, min_str: &str, max_str: &str) -> Result<()> {
    let version = parse_version(version_str)?;
    let min = parse_version(min_str)?;
    if version < min {
        bail!("version {} < {}", version, min);
    }
    let max = parse_version(max_str)?;
    if version > max {
        bail!("version {} > {}", version, max);
    }
    Ok(())
}

fn error_code_msg(id: &Value, code: i32, message: String) -> Value {
    json!({
        "id": id,
        "error": {
            "code": code,
            "message": message,
        },
        "jsonrpc": "2.0",
    })
}

#[cfg(test)]
mod tests {
    use super::{check_between, parse_version, Version};

    #[test]
    fn test_version() {
        assert_eq!(parse_version("1").unwrap(), Version { major: 1, minor: 0, patch: 0 });
        assert_eq!(parse_version("1.2").unwrap(), Version { major: 1, minor: 2, patch: 0 });
        assert_eq!(parse_version("1.2.345").unwrap(), Version { major: 1, minor: 2, patch: 345 });

        assert!(parse_version("1.2").unwrap() < parse_version("1.100").unwrap());
    }

    #[test]
    fn test_between() {
        assert!(check_between("1.4", "1.4", "1.4").is_ok());
        assert!(check_between("1.4", "1.4", "1.5").is_ok());
        assert!(check_between("1.4", "1.3", "1.4").is_ok());
        assert!(check_between("1.4", "1.3", "1.5").is_ok());

        assert!(check_between("1.4", "1.5", "1.5").is_err());
        assert!(check_between("1.4", "1.3", "1.3").is_err());
        assert!(check_between("1.4", "1.4.1", "1.5").is_err());
        assert!(check_between("1.4", "1", "1").is_err());
    }
}

async fn handle_request(
    server: &Rpc,
    client: &mut Client,
    line: String,
) -> Result<Option<Value>> {
    let requests = match parse_requests(&line) {
        Ok(requests) => requests,
        Err(e) => return Ok(Some(error_msg_no_id(e))),
    };
    
    let calls = Calls::parse(requests);
    let response = server.handle_multi_call(client, calls);
    Ok(Some(response))
}

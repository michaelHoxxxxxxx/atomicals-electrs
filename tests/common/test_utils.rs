use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tempfile::TempDir;
use bitcoin::Network;

use crate::config::Config;
use crate::daemon::Daemon;
use crate::db::DB;
use crate::metrics::Metrics;
use crate::atomicals::{AtomicalsState, AtomicalsIndexer};
use crate::atomicals::websocket::WsServer;

pub struct TestEnvironment {
    pub temp_dir: TempDir,
    pub config: Config,
    pub db: Arc<DB>,
    pub daemon: Arc<Daemon>,
    pub metrics: Arc<Metrics>,
    pub state: Arc<AtomicalsState>,
    pub indexer: Arc<AtomicalsIndexer>,
    pub ws_server: Arc<WsServer>,
}

impl TestEnvironment {
    /// 处理区块
    pub async fn process_block(&self, block: bitcoin::Block) -> anyhow::Result<()> {
        // 1. 更新区块链状态
        self.daemon.process_block(&block).await?;
        
        // 2. 更新 Atomicals 状态
        self.state.process_block(&block).await?;
        
        // 3. 更新索引
        self.indexer.index_block(&block).await?;
        
        Ok(())
    }

    /// 添加交易到内存池
    pub async fn add_to_mempool(&self, tx: bitcoin::Transaction) -> anyhow::Result<()> {
        // 1. 添加到守护进程的内存池
        self.daemon.add_to_mempool(&tx).await?;
        
        // 2. 更新 Atomicals 临时状态
        self.state.process_mempool_tx(&tx).await?;
        
        Ok(())
    }
}

/// 创建测试环境
pub fn setup_test_environment() -> TestEnvironment {
    // 1. 创建临时目录
    let temp_dir = TempDir::new().unwrap();
    
    // 2. 创建配置
    let config = Config {
        network: Network::Testnet,
        db_dir: temp_dir.path().to_path_buf(),
        daemon_dir: temp_dir.path().to_path_buf(),
        electrum_rpc_addr: "127.0.0.1:0".parse().unwrap(),
        http_addr: "127.0.0.1:0".parse().unwrap(),
        ws_addr: "127.0.0.1:0".parse().unwrap(),
        ..Default::default()
    };
    
    // 3. 创建数据库
    let db = Arc::new(DB::open(&config).unwrap());
    
    // 4. 创建守护进程
    let daemon = Arc::new(Daemon::new(&config).unwrap());
    
    // 5. 创建指标收集器
    let metrics = Arc::new(Metrics::new(
        "test".to_string(),
        config.monitoring_addr,
    ));
    
    // 6. 创建 Atomicals 状态
    let state = Arc::new(AtomicalsState::new());
    
    // 7. 创建索引器
    let indexer = Arc::new(AtomicalsIndexer::new(
        Arc::clone(&state),
        Arc::clone(&db),
        Arc::clone(&metrics),
    ));
    
    // 8. 创建 WebSocket 服务器
    let ws_server = Arc::new(WsServer::new(
        Arc::clone(&state),
        config.clone(),
    ));
    
    TestEnvironment {
        temp_dir,
        config,
        db,
        daemon,
        metrics,
        state,
        indexer,
        ws_server,
    }
}

/// 清理测试数据
pub fn cleanup_test_data(env: &TestEnvironment) {
    // 关闭数据库连接
    drop(env.db.clone());
    
    // 删除临时目录
    env.temp_dir.close().unwrap();
}

/// 模拟网络延迟
pub async fn simulate_network_delay(duration: Duration) {
    sleep(duration).await;
}

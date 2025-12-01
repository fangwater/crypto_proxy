mod app;
mod cfg;
mod connection;
mod forwarder;
mod mkt_msg;
mod parser;
mod proxy;
mod receiver;
mod rest_fetcher;
mod restart_checker;
mod sub_msg;
use app::CryptoProxyApp;
use cfg::Config;
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

#[derive(Clone, Debug, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Exchange {
    Binance,
    #[value(name = "binance-futures")]
    #[serde(rename = "binance-futures")]
    BinanceFutures,
    Okex,
    #[value(name = "okex-swap")]
    #[serde(rename = "okex-swap")]
    OkexSwap,
    Bybit,
    #[value(name = "bybit-spot")]
    #[serde(rename = "bybit-spot")]
    BybitSpot,
}

#[derive(Parser)]
#[command(name = "crypto_proxy")]
#[command(about = "Cryptocurrency market data proxy")]
struct Args {
    /// Exchange to connect to
    #[arg(short, long)]
    exchange: Exchange,

    /// Override Binance spot REST base URL
    #[arg(long)]
    binance_url: Option<String>,

    /// Override Binance futures REST base URL
    #[arg(long)]
    binance_futures_url: Option<String>,
}

#[tokio::main(worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "INFO");
    env_logger::init();

    // 解析命令行参数
    let Args {
        exchange,
        binance_url,
        binance_futures_url,
    } = Args::parse();

    // 固定配置文件路径
    let config_path = "mkt_cfg.yaml";

    static CFG: OnceCell<Config> = OnceCell::const_new();

    let mut config = Config::load_config(config_path, exchange.clone())
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    match exchange {
        Exchange::Binance | Exchange::BinanceFutures => {
            let spot_url = binance_url.ok_or_else(|| {
                anyhow::anyhow!("--binance-url must be provided for binance exchanges")
            })?;
            let futures_url = binance_futures_url.ok_or_else(|| {
                anyhow::anyhow!("--binance-futures-url must be provided for binance exchanges")
            })?;
            config.binance_rest.binance_url = spot_url;
            config.binance_rest.binance_futures_url = futures_url;
        }
        _ => {
            if let Some(url) = binance_url {
                config.binance_rest.binance_url = url;
            }
            if let Some(url) = binance_futures_url {
                config.binance_rest.binance_futures_url = url;
            }
        }
    }

    CFG.set(config)
        .map_err(|_| anyhow::anyhow!("Config already initialized"))?;
    let config = CFG.get().expect("config initialized");

    // 创建并运行应用
    let app = CryptoProxyApp::new(config).await?;
    app.run().await
}

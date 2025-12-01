//! Binance REST API Fetcher Module
//!
//! 独立的 REST 请求模块，基于本地时钟定时触发，不受 WebSocket 连接重启影响。
//!
//! 功能：
//! - 每分钟请求：PremiumIndex, OpenInterest
//! - 每5分钟请求：TopAccount, TopPosition, GlobalAccount, OpenInterestHist

use bytes::Bytes;
use log::{error, info, warn};
use reqwest::Client;
use serde::Deserialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio::time::{sleep_until, Instant};

use crate::mkt_msg::{BarClose1mMsg, PremiumIndexKlineMsg, TopLongShortRatioMsg};

// ============================================================================
// 常量定义
// ============================================================================

const ONE_MINUTE_MILLIS: i64 = 60_000;
const FIVE_MINUTE_MILLIS: i64 = 5 * ONE_MINUTE_MILLIS;
const REST_MONITOR_TAG: &str = "[REST-MON]";

/// 请求超时时间
const REQUEST_TIMEOUT: Duration = Duration::from_millis(500);

/// 最大重试次数
const MAX_RETRIES: u32 = 2;

/// 请求延迟（等待交易所数据准备好）
const REQUEST_DELAY_MS: u64 = 1000;

/// 5分钟请求额外延迟
const FIVE_MIN_REQUEST_DELAY_SECS: u64 = 180;

// ============================================================================
// 错误类型
// ============================================================================

#[derive(Debug, Clone)]
pub enum FetchError {
    Request(String),
    Http(u16),
    Json(String),
    EmptyResponse,
    MatchFailure,
    MissingField(&'static str),
    Timeout,
}

impl FetchError {
    pub fn detail(&self) -> String {
        match self {
            FetchError::Request(err) => format!("请求错误: {}", err),
            FetchError::Http(code) => format!("HTTP {}", code),
            FetchError::Json(err) => format!("JSON错误: {}", err),
            FetchError::EmptyResponse => "空响应".to_string(),
            FetchError::MatchFailure => "匹配失败".to_string(),
            FetchError::MissingField(field) => format!("缺少字段 {}", field),
            FetchError::Timeout => "请求超时".to_string(),
        }
    }
}

// ============================================================================
// 数据结构
// ============================================================================

/// Premium Index Kline 数据
#[derive(Debug, Clone)]
pub struct PremiumIndexData {
    pub symbol: String,
    pub open_time: i64,
    pub open_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub close_price: f64,
}

/// Open Interest 数据
#[derive(Debug, Clone)]
pub struct OpenInterestData {
    pub symbol: String,
    pub open_interest: f64,
    pub timestamp: i64,
}

/// Ratio Metrics 数据 (用于 TopAccount, TopPosition, GlobalAccount)
#[derive(Debug, Clone)]
pub struct RatioMetricsData {
    pub symbol: String,
    pub long_value: f64,
    pub short_value: f64,
    pub ratio_value: f64,
    pub timestamp: i64,
}

/// Open Interest History 数据
#[derive(Debug, Clone)]
pub struct OpenInterestHistData {
    pub symbol: String,
    pub sum_open_interest: f64,
    pub sum_open_interest_value: f64,
    pub cmc_circulating_supply: f64,
    pub timestamp: i64,
}

/// 1分钟汇总结果
#[derive(Debug)]
pub struct OneMinuteResult {
    pub close_time: i64,
    pub premium_index: Vec<Result<PremiumIndexData, (String, FetchError)>>,
    pub open_interest: Vec<Result<OpenInterestData, (String, FetchError)>>,
}

/// 5分钟汇总结果
#[derive(Debug)]
pub struct FiveMinuteResult {
    pub close_time: i64,
    pub top_account: Vec<Result<RatioMetricsData, (String, FetchError)>>,
    pub top_position: Vec<Result<RatioMetricsData, (String, FetchError)>>,
    pub global_account: Vec<Result<RatioMetricsData, (String, FetchError)>>,
    pub open_interest_hist: Vec<Result<OpenInterestHistData, (String, FetchError)>>,
}

// ============================================================================
// Symbol 获取
// ============================================================================

#[derive(Debug, Deserialize)]
struct ExchangeInfoResponse {
    symbols: Vec<SymbolInfo>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SymbolInfo {
    symbol: String,
    status: String,
    quote_asset: String,
    #[serde(default)]
    contract_type: Option<String>,
}

/// 从 Binance exchangeInfo 获取 USDT 永续合约 symbol 列表
pub async fn fetch_futures_symbols(base_url: &str) -> Result<Vec<String>, FetchError> {
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| FetchError::Request(e.to_string()))?;

    let url = format!("{}/fapi/v1/exchangeInfo", base_url);

    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| FetchError::Request(e.to_string()))?;

    if !response.status().is_success() {
        return Err(FetchError::Http(response.status().as_u16()));
    }

    let body = response
        .text()
        .await
        .map_err(|e| FetchError::Request(e.to_string()))?;

    let info: ExchangeInfoResponse = serde_json::from_str(&body)
        .map_err(|e| FetchError::Json(e.to_string()))?;

    let symbols: Vec<String> = info
        .symbols
        .into_iter()
        .filter(|s| {
            s.quote_asset == "USDT"
                && s.status == "TRADING"
                && s.contract_type.as_deref() == Some("PERPETUAL")
        })
        .map(|s| s.symbol)
        .collect();

    Ok(symbols)
}

// ============================================================================
// REST 请求实现
// ============================================================================

/// 带重试的 HTTP GET 请求
async fn fetch_with_retry(
    client: &Client,
    url: &str,
    params: &[(&str, &str)],
    label: &str,
    symbol: &str,
) -> Result<String, FetchError> {
    let mut last_error = FetchError::Request("未执行".to_string());

    for attempt in 0..MAX_RETRIES {
        if attempt > 0 {
            // 重试前短暂等待
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let result = client
            .get(url)
            .query(params)
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await;

        match result {
            Ok(response) => {
                let status = response.status();
                if !status.is_success() {
                    last_error = FetchError::Http(status.as_u16());
                    if attempt + 1 < MAX_RETRIES {
                        warn!(
                            "{REST_MONITOR_TAG} [{}] {} HTTP {} (attempt {}/{})",
                            label,
                            symbol,
                            status,
                            attempt + 1,
                            MAX_RETRIES
                        );
                    }
                    continue;
                }

                match response.text().await {
                    Ok(body) => return Ok(body),
                    Err(e) => {
                        last_error = FetchError::Request(e.to_string());
                        continue;
                    }
                }
            }
            Err(e) => {
                if e.is_timeout() {
                    last_error = FetchError::Timeout;
                } else {
                    last_error = FetchError::Request(e.to_string());
                }
                if attempt + 1 < MAX_RETRIES {
                    warn!(
                        "{REST_MONITOR_TAG} [{}] {} request error (attempt {}/{}): {}",
                        label,
                        symbol,
                        attempt + 1,
                        MAX_RETRIES,
                        e
                    );
                }
            }
        }
    }

    Err(last_error)
}

/// 获取 Premium Index Klines
async fn fetch_premium_index(
    client: &Client,
    base_url: &str,
    symbol: &str,
    close_time: i64,
) -> Result<PremiumIndexData, FetchError> {
    let url = format!("{}/fapi/v1/premiumIndexKlines", base_url);
    let body = fetch_with_retry(
        client,
        &url,
        &[("symbol", symbol), ("interval", "1m"), ("limit", "2")],
        "PremiumIndex",
        symbol,
    )
    .await?;

    let records: Vec<Vec<serde_json::Value>> =
        serde_json::from_str(&body).map_err(|e| FetchError::Json(e.to_string()))?;

    if records.is_empty() {
        return Err(FetchError::EmptyResponse);
    }

    // 解析记录
    let parse_record = |record: &Vec<serde_json::Value>| -> Option<(i64, f64, f64, f64, f64)> {
        let parse_i64 = |idx: usize| -> Option<i64> {
            record.get(idx).and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_str()?.parse::<i64>().ok())
            })
        };
        let parse_f64 = |idx: usize| -> Option<f64> {
            record.get(idx).and_then(|v| {
                v.as_f64()
                    .or_else(|| v.as_str()?.parse::<f64>().ok())
            })
        };
        Some((
            parse_i64(0)?,
            parse_f64(1)?,
            parse_f64(2)?,
            parse_f64(3)?,
            parse_f64(4)?,
        ))
    };

    // 计算 open_time (close_time - 60000 或 close_time - 59999)
    let expected_open_time = close_time - ONE_MINUTE_MILLIS;

    let primary = parse_record(&records[0]).ok_or(FetchError::MissingField("record"))?;
    let secondary = records.get(1).and_then(parse_record);

    // 匹配时间戳
    let selected = if let Some(sec) = secondary {
        if sec.0 == expected_open_time {
            sec
        } else if primary.0 == expected_open_time {
            primary
        } else {
            // 时间戳不匹配，使用最新的
            warn!(
                "{REST_MONITOR_TAG} [PremiumIndex] {} timestamp mismatch: expected={}, got=[{}, {:?}]",
                symbol,
                expected_open_time,
                primary.0,
                secondary.map(|s| s.0)
            );
            primary
        }
    } else if primary.0 == expected_open_time {
        primary
    } else {
        warn!(
            "{REST_MONITOR_TAG} [PremiumIndex] {} timestamp mismatch: expected={}, got={}",
            symbol, expected_open_time, primary.0
        );
        primary
    };

    Ok(PremiumIndexData {
        symbol: symbol.to_string(),
        open_time: selected.0,
        open_price: selected.1,
        high_price: selected.2,
        low_price: selected.3,
        close_price: selected.4,
    })
}

/// 获取 Open Interest
async fn fetch_open_interest(
    client: &Client,
    base_url: &str,
    symbol: &str,
) -> Result<OpenInterestData, FetchError> {
    let url = format!("{}/fapi/v1/openInterest", base_url);
    let body = fetch_with_retry(
        client,
        &url,
        &[("symbol", symbol)],
        "OpenInterest",
        symbol,
    )
    .await?;

    let json: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| FetchError::Json(e.to_string()))?;

    let oi_str = json
        .get("openInterest")
        .and_then(|v| v.as_str())
        .ok_or(FetchError::MissingField("openInterest"))?;

    let time = json
        .get("time")
        .and_then(|v| v.as_i64())
        .ok_or(FetchError::MissingField("time"))?;

    let oi = oi_str
        .parse::<f64>()
        .map_err(|_| FetchError::MissingField("openInterest parse"))?;

    Ok(OpenInterestData {
        symbol: symbol.to_string(),
        open_interest: oi,
        timestamp: time,
    })
}

/// 获取 Ratio Metrics (TopAccount, TopPosition, GlobalAccount)
async fn fetch_ratio_metrics(
    client: &Client,
    base_url: &str,
    endpoint: &str,
    symbol: &str,
    label: &str,
    long_key: &'static str,
    short_key: &'static str,
    close_time: i64,
) -> Result<RatioMetricsData, FetchError> {
    let url = format!("{}/{}", base_url, endpoint);
    let body = fetch_with_retry(
        client,
        &url,
        &[("symbol", symbol), ("period", "5m"), ("limit", "2")],
        label,
        symbol,
    )
    .await?;

    let entries: Vec<serde_json::Value> =
        serde_json::from_str(&body).map_err(|e| FetchError::Json(e.to_string()))?;

    if entries.is_empty() {
        return Err(FetchError::EmptyResponse);
    }

    let to_i64 = |value: &serde_json::Value| -> Option<i64> {
        value
            .as_i64()
            .or_else(|| value.as_str()?.parse::<i64>().ok())
    };

    // 匹配时间戳
    let entry = entries
        .iter()
        .find(|entry| {
            let ts = entry.get("timestamp").and_then(|v| to_i64(v));
            ts == Some(close_time) || ts == Some(close_time + 1)
        })
        .ok_or_else(|| {
            warn!(
                "{REST_MONITOR_TAG} [{}] {} timestamp mismatch at close_time {}",
                label,
                symbol,
                close_time
            );
            FetchError::MatchFailure
        })?;

    let parse_value = |key: &str| -> Option<f64> {
        entry
            .get(key)
            .and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse::<f64>().ok()))
    };

    let long_value = parse_value(long_key).ok_or(FetchError::MissingField(long_key))?;
    let short_value = parse_value(short_key).ok_or(FetchError::MissingField(short_key))?;
    let ratio_value =
        parse_value("longShortRatio").ok_or(FetchError::MissingField("longShortRatio"))?;
    let timestamp = entry
        .get("timestamp")
        .and_then(|v| to_i64(v))
        .unwrap_or(close_time);

    Ok(RatioMetricsData {
        symbol: symbol.to_string(),
        long_value,
        short_value,
        ratio_value,
        timestamp,
    })
}

/// 获取 Open Interest History
async fn fetch_open_interest_hist(
    client: &Client,
    base_url: &str,
    symbol: &str,
    close_time: i64,
) -> Result<OpenInterestHistData, FetchError> {
    let url = format!("{}/futures/data/openInterestHist", base_url);
    let body = fetch_with_retry(
        client,
        &url,
        &[("symbol", symbol), ("period", "5m"), ("limit", "2")],
        "OpenInterestHist",
        symbol,
    )
    .await?;

    let entries: Vec<serde_json::Value> =
        serde_json::from_str(&body).map_err(|e| FetchError::Json(e.to_string()))?;

    if entries.is_empty() {
        return Err(FetchError::EmptyResponse);
    }

    let to_i64 = |value: &serde_json::Value| -> Option<i64> {
        value
            .as_i64()
            .or_else(|| value.as_str()?.parse::<i64>().ok())
    };

    let entry = entries
        .iter()
        .find(|entry| {
            let ts = entry.get("timestamp").and_then(|v| to_i64(v));
            ts == Some(close_time) || ts == Some(close_time + 1)
        })
        .ok_or_else(|| {
            warn!(
                "{REST_MONITOR_TAG} [OpenInterestHist] {} timestamp mismatch at close_time {}",
                symbol,
                close_time
            );
            FetchError::MatchFailure
        })?;

    let parse_f64 = |key: &str| -> Option<f64> {
        entry
            .get(key)
            .and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse::<f64>().ok()))
    };

    let sum_open_interest =
        parse_f64("sumOpenInterest").ok_or(FetchError::MissingField("sumOpenInterest"))?;
    let sum_open_interest_value = parse_f64("sumOpenInterestValue")
        .ok_or(FetchError::MissingField("sumOpenInterestValue"))?;
    let cmc_circulating_supply = parse_f64("CMCCirculatingSupply").unwrap_or(0.0);
    let timestamp = entry
        .get("timestamp")
        .and_then(|v| to_i64(v))
        .unwrap_or(close_time);

    Ok(OpenInterestHistData {
        symbol: symbol.to_string(),
        sum_open_interest,
        sum_open_interest_value,
        cmc_circulating_supply,
        timestamp,
    })
}

// ============================================================================
// REST Fetcher 主结构
// ============================================================================

pub struct BinanceRestFetcher {
    base_url: String,
    client: Client,
    symbols: Vec<String>,
}

impl BinanceRestFetcher {
    /// 创建新的 REST Fetcher
    pub async fn new(base_url: String) -> Result<Self, FetchError> {
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .pool_max_idle_per_host(100)
            .build()
            .map_err(|e| FetchError::Request(e.to_string()))?;

        // 获取 symbol 列表
        info!("{REST_MONITOR_TAG} Fetching futures symbols from {}", base_url);
        let symbols = fetch_futures_symbols(&base_url).await?;
        info!(
            "{REST_MONITOR_TAG} Fetched {} futures symbols",
            symbols.len()
        );

        Ok(Self {
            base_url,
            client,
            symbols,
        })
    }

    /// 获取当前 symbol 列表
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }

    /// 刷新 symbol 列表
    pub async fn refresh_symbols(&mut self) -> Result<(), FetchError> {
        let symbols = fetch_futures_symbols(&self.base_url).await?;
        info!(
            "{REST_MONITOR_TAG} Refreshed symbols: {} -> {}",
            self.symbols.len(),
            symbols.len()
        );
        self.symbols = symbols;
        Ok(())
    }

    /// 执行1分钟请求（PremiumIndex + OpenInterest）
    pub async fn fetch_one_minute(&self, close_time: i64) -> OneMinuteResult {
        let mut premium_index_results = Vec::with_capacity(self.symbols.len());
        let mut open_interest_results = Vec::with_capacity(self.symbols.len());

        // 并发请求所有 symbol
        let premium_futures: Vec<_> = self
            .symbols
            .iter()
            .map(|symbol| {
                let client = self.client.clone();
                let base_url = self.base_url.clone();
                let symbol = symbol.clone();
                async move {
                    let result = fetch_premium_index(&client, &base_url, &symbol, close_time).await;
                    (symbol, result)
                }
            })
            .collect();

        let oi_futures: Vec<_> = self
            .symbols
            .iter()
            .map(|symbol| {
                let client = self.client.clone();
                let base_url = self.base_url.clone();
                let symbol = symbol.clone();
                async move {
                    let result = fetch_open_interest(&client, &base_url, &symbol).await;
                    (symbol, result)
                }
            })
            .collect();

        // 等待所有请求完成
        let premium_results = futures::future::join_all(premium_futures).await;
        let oi_results = futures::future::join_all(oi_futures).await;

        for (symbol, result) in premium_results {
            match result {
                Ok(data) => premium_index_results.push(Ok(data)),
                Err(e) => premium_index_results.push(Err((symbol, e))),
            }
        }

        for (symbol, result) in oi_results {
            match result {
                Ok(data) => open_interest_results.push(Ok(data)),
                Err(e) => open_interest_results.push(Err((symbol, e))),
            }
        }

        OneMinuteResult {
            close_time,
            premium_index: premium_index_results,
            open_interest: open_interest_results,
        }
    }

    /// 执行5分钟请求
    pub async fn fetch_five_minute(&self, close_time: i64) -> FiveMinuteResult {
        let mut top_account_results = Vec::with_capacity(self.symbols.len());
        let mut top_position_results = Vec::with_capacity(self.symbols.len());
        let mut global_account_results = Vec::with_capacity(self.symbols.len());
        let mut oi_hist_results = Vec::with_capacity(self.symbols.len());

        // TopAccount
        let top_account_futures: Vec<_> = self
            .symbols
            .iter()
            .map(|symbol| {
                let client = self.client.clone();
                let base_url = self.base_url.clone();
                let symbol = symbol.clone();
                async move {
                    let result = fetch_ratio_metrics(
                        &client,
                        &base_url,
                        "futures/data/topLongShortAccountRatio",
                        &symbol,
                        "TopAccount",
                        "longAccount",
                        "shortAccount",
                        close_time,
                    )
                    .await;
                    (symbol, result)
                }
            })
            .collect();

        // TopPosition
        let top_position_futures: Vec<_> = self
            .symbols
            .iter()
            .map(|symbol| {
                let client = self.client.clone();
                let base_url = self.base_url.clone();
                let symbol = symbol.clone();
                async move {
                    let result = fetch_ratio_metrics(
                        &client,
                        &base_url,
                        "futures/data/topLongShortPositionRatio",
                        &symbol,
                        "TopPosition",
                        "longAccount",
                        "shortAccount",
                        close_time,
                    )
                    .await;
                    (symbol, result)
                }
            })
            .collect();

        // GlobalAccount
        let global_account_futures: Vec<_> = self
            .symbols
            .iter()
            .map(|symbol| {
                let client = self.client.clone();
                let base_url = self.base_url.clone();
                let symbol = symbol.clone();
                async move {
                    let result = fetch_ratio_metrics(
                        &client,
                        &base_url,
                        "futures/data/globalLongShortAccountRatio",
                        &symbol,
                        "GlobalAccount",
                        "longAccount",
                        "shortAccount",
                        close_time,
                    )
                    .await;
                    (symbol, result)
                }
            })
            .collect();

        // OpenInterestHist
        let oi_hist_futures: Vec<_> = self
            .symbols
            .iter()
            .map(|symbol| {
                let client = self.client.clone();
                let base_url = self.base_url.clone();
                let symbol = symbol.clone();
                async move {
                    let result = fetch_open_interest_hist(&client, &base_url, &symbol, close_time).await;
                    (symbol, result)
                }
            })
            .collect();

        // 等待所有请求完成
        let (top_account, top_position, global_account, oi_hist) = tokio::join!(
            futures::future::join_all(top_account_futures),
            futures::future::join_all(top_position_futures),
            futures::future::join_all(global_account_futures),
            futures::future::join_all(oi_hist_futures),
        );

        for (symbol, result) in top_account {
            match result {
                Ok(data) => top_account_results.push(Ok(data)),
                Err(e) => top_account_results.push(Err((symbol, e))),
            }
        }

        for (symbol, result) in top_position {
            match result {
                Ok(data) => top_position_results.push(Ok(data)),
                Err(e) => top_position_results.push(Err((symbol, e))),
            }
        }

        for (symbol, result) in global_account {
            match result {
                Ok(data) => global_account_results.push(Ok(data)),
                Err(e) => global_account_results.push(Err((symbol, e))),
            }
        }

        for (symbol, result) in oi_hist {
            match result {
                Ok(data) => oi_hist_results.push(Ok(data)),
                Err(e) => oi_hist_results.push(Err((symbol, e))),
            }
        }

        FiveMinuteResult {
            close_time,
            top_account: top_account_results,
            top_position: top_position_results,
            global_account: global_account_results,
            open_interest_hist: oi_hist_results,
        }
    }
}

// ============================================================================
// 定时器和运行逻辑
// ============================================================================

/// 计算下一个分钟边界的时间
fn next_minute_boundary() -> (Instant, i64) {
    let now = SystemTime::now();
    let since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    let total_millis = since_epoch.as_millis() as i64;

    // 计算下一个分钟边界
    let current_minute = total_millis / ONE_MINUTE_MILLIS;
    let next_minute = current_minute + 1;
    let next_minute_millis = next_minute * ONE_MINUTE_MILLIS;
    let wait_millis = next_minute_millis - total_millis;

    let next_instant = Instant::now() + Duration::from_millis(wait_millis as u64);
    let close_time = next_minute_millis; // close_time 是分钟边界

    (next_instant, close_time)
}

/// 判断是否是5分钟边界
fn is_five_minute_boundary(close_time: i64) -> bool {
    close_time % FIVE_MINUTE_MILLIS == 0
}

/// 打印1分钟请求汇总
fn print_one_minute_summary(result: &OneMinuteResult) {
    let pi_success = result.premium_index.iter().filter(|r| r.is_ok()).count();
    let pi_fail = result.premium_index.len() - pi_success;

    let oi_success = result.open_interest.iter().filter(|r| r.is_ok()).count();
    let oi_fail = result.open_interest.len() - oi_success;

    info!(
        "{REST_MONITOR_TAG} [1min Summary] close_time={} | PremiumIndex: {}/{} success | OpenInterest: {}/{} success",
        result.close_time, pi_success, result.premium_index.len(), oi_success, result.open_interest.len()
    );

    if pi_fail > 0 || oi_fail > 0 {
        // 打印失败详情（只打印前5个）
        let mut fail_count = 0;
        for r in &result.premium_index {
            if let Err((symbol, e)) = r {
                if fail_count < 5 {
                    warn!(
                        "{REST_MONITOR_TAG}   PremiumIndex failed: {} - {}",
                        symbol,
                        e.detail()
                    );
                    fail_count += 1;
                }
            }
        }
        fail_count = 0;
        for r in &result.open_interest {
            if let Err((symbol, e)) = r {
                if fail_count < 5 {
                    warn!(
                        "{REST_MONITOR_TAG}   OpenInterest failed: {} - {}",
                        symbol,
                        e.detail()
                    );
                    fail_count += 1;
                }
            }
        }
    }
}

/// 打印5分钟请求汇总
fn print_five_minute_summary(result: &FiveMinuteResult) {
    let ta_success = result.top_account.iter().filter(|r| r.is_ok()).count();
    let tp_success = result.top_position.iter().filter(|r| r.is_ok()).count();
    let ga_success = result.global_account.iter().filter(|r| r.is_ok()).count();
    let oh_success = result.open_interest_hist.iter().filter(|r| r.is_ok()).count();

    let total = result.top_account.len();

    info!(
        "{REST_MONITOR_TAG} [5min Summary] close_time={} | TopAccount: {}/{} | TopPosition: {}/{} | GlobalAccount: {}/{} | OIHist: {}/{}",
        result.close_time, ta_success, total, tp_success, total, ga_success, total, oh_success, total
    );
}

// ============================================================================
// 带消息推送的运行函数
// ============================================================================

/// 运行 REST Fetcher 主循环（带消息推送）
pub async fn run_rest_fetcher_with_sender(base_url: String, sender: broadcast::Sender<Bytes>) {
    info!(
        "{REST_MONITOR_TAG} Starting BinanceRestFetcher with base_url: {} (with message sender)",
        base_url
    );

    let mut fetcher = match BinanceRestFetcher::new(base_url).await {
        Ok(f) => f,
        Err(e) => {
            error!("{REST_MONITOR_TAG} Failed to create BinanceRestFetcher: {:?}", e);
            return;
        }
    };

    info!(
        "{REST_MONITOR_TAG} BinanceRestFetcher initialized with {} symbols",
        fetcher.symbols().len()
    );

    loop {
        // 等待下一个分钟边界
        let (next_instant, close_time) = next_minute_boundary();
        info!(
            "{REST_MONITOR_TAG} waiting for next minute boundary | close_time={} | wait={:?}",
            close_time, next_instant - Instant::now()
        );
        sleep_until(next_instant).await;

        // 如果是5分钟边界，先刷新 symbol 列表
        if is_five_minute_boundary(close_time) {
            info!("{REST_MONITOR_TAG} 5-minute boundary: refreshing symbol list...");
            match fetcher.refresh_symbols().await {
                Ok(()) => {
                    info!(
                        "{REST_MONITOR_TAG} symbol list refreshed | total_symbols={}",
                        fetcher.symbols().len()
                    );
                }
                Err(e) => {
                    warn!(
                        "{REST_MONITOR_TAG} Failed to refresh symbols: {:?}, continuing with existing list",
                        e
                    );
                }
            }
        }

        info!(
            "{REST_MONITOR_TAG} close_time={} | active_symbols={}",
            close_time,
            fetcher.symbols().len()
        );

        // 延迟等待交易所数据准备好
        tokio::time::sleep(Duration::from_millis(REQUEST_DELAY_MS)).await;

        // 执行1分钟请求
        info!(
            "{REST_MONITOR_TAG} executing 1-minute requests | close_time={} | symbols={}",
            close_time,
            fetcher.symbols().len()
        );
        let one_min_result = fetcher.fetch_one_minute(close_time).await;

        // 发送1分钟消息
        send_one_minute_messages(&one_min_result, &sender);
        print_one_minute_summary(&one_min_result);

        // 发送1分钟封bar消息
        let bar_close_msg = BarClose1mMsg::create(close_time);
        if let Err(e) = sender.send(bar_close_msg.to_bytes()) {
            error!(
                "{REST_MONITOR_TAG} Failed to send BarClose1mMsg for close_time={}: {}",
                close_time, e
            );
        } else {
            info!("{REST_MONITOR_TAG} [BarClose1m] sent for close_time={}", close_time);
        }

        // 如果是5分钟边界，执行5分钟请求
        if is_five_minute_boundary(close_time) {
            info!(
                "{REST_MONITOR_TAG} 5-minute boundary detected, waiting {}s before 5-min requests",
                FIVE_MIN_REQUEST_DELAY_SECS
            );
            tokio::time::sleep(Duration::from_secs(FIVE_MIN_REQUEST_DELAY_SECS)).await;

            info!(
                "{REST_MONITOR_TAG} executing 5-minute requests | close_time={} | symbols={}",
                close_time,
                fetcher.symbols().len()
            );
            let five_min_result = fetcher.fetch_five_minute(close_time).await;

            // 发送5分钟消息
            send_five_minute_messages(&five_min_result, &sender);
            print_five_minute_summary(&five_min_result);
        }
    }
}

/// 发送1分钟消息（PremiumIndexKline）
fn send_one_minute_messages(result: &OneMinuteResult, sender: &broadcast::Sender<Bytes>) {
    let close_time = result.close_time;

    // 将 PremiumIndex 和 OpenInterest 合并成 PremiumIndexKlineMsg
    // 创建一个 HashMap 来匹配 symbol
    let mut oi_map: std::collections::HashMap<String, &OpenInterestData> = std::collections::HashMap::new();
    for r in &result.open_interest {
        if let Ok(data) = r {
            oi_map.insert(data.symbol.clone(), data);
        }
    }

    let mut sent_count = 0;
    for r in &result.premium_index {
        if let Ok(pi_data) = r {
            let mut msg = PremiumIndexKlineMsg::create(
                pi_data.symbol.clone(),
                pi_data.open_price,
                pi_data.high_price,
                pi_data.low_price,
                pi_data.close_price,
                pi_data.open_time,
            );

            // 查找对应的 OpenInterest 数据
            if let Some(oi_data) = oi_map.get(&pi_data.symbol) {
                msg.set_open_interest(oi_data.open_interest, oi_data.timestamp);
            }

            if let Err(e) = sender.send(msg.to_bytes()) {
                error!(
                    "{REST_MONITOR_TAG} Failed to send PremiumIndexKlineMsg for {}: {}",
                    pi_data.symbol, e
                );
            } else {
                sent_count += 1;
            }
        }
    }

    info!(
        "{REST_MONITOR_TAG} [1min Broadcast] close_time={} | sent {} PremiumIndexKlineMsg",
        close_time, sent_count
    );
}

/// 发送5分钟消息（TopLongShortRatioMsg）
fn send_five_minute_messages(result: &FiveMinuteResult, sender: &broadcast::Sender<Bytes>) {
    let close_time = result.close_time;

    // 创建 HashMap 来匹配各类数据
    let mut top_account_map: std::collections::HashMap<String, &RatioMetricsData> = std::collections::HashMap::new();
    let mut top_position_map: std::collections::HashMap<String, &RatioMetricsData> = std::collections::HashMap::new();
    let mut global_account_map: std::collections::HashMap<String, &RatioMetricsData> = std::collections::HashMap::new();
    let mut oi_hist_map: std::collections::HashMap<String, &OpenInterestHistData> = std::collections::HashMap::new();

    for r in &result.top_account {
        if let Ok(data) = r {
            top_account_map.insert(data.symbol.clone(), data);
        }
    }
    for r in &result.top_position {
        if let Ok(data) = r {
            top_position_map.insert(data.symbol.clone(), data);
        }
    }
    for r in &result.global_account {
        if let Ok(data) = r {
            global_account_map.insert(data.symbol.clone(), data);
        }
    }
    for r in &result.open_interest_hist {
        if let Ok(data) = r {
            oi_hist_map.insert(data.symbol.clone(), data);
        }
    }

    // 收集所有 symbol
    let mut all_symbols: std::collections::HashSet<String> = std::collections::HashSet::new();
    for r in &result.top_account {
        if let Ok(data) = r {
            all_symbols.insert(data.symbol.clone());
        }
    }

    let mut sent_count = 0;
    for symbol in &all_symbols {
        // 必须有 top_account, top_position, global_account 才发送
        if let (Some(account), Some(position), Some(global)) = (
            top_account_map.get(symbol),
            top_position_map.get(symbol),
            global_account_map.get(symbol),
        ) {
            let mut msg = TopLongShortRatioMsg::create(
                symbol.clone(),
                close_time,
                account.long_value,
                account.short_value,
                account.ratio_value,
                position.long_value,
                position.short_value,
                position.ratio_value,
                global.long_value,
                global.short_value,
                global.ratio_value,
                account.timestamp,
                position.timestamp,
                global.timestamp,
            );

            // 添加 OpenInterestHist 数据（可选）
            if let Some(oi_hist) = oi_hist_map.get(symbol) {
                msg.set_open_interest_hist(
                    oi_hist.sum_open_interest,
                    oi_hist.sum_open_interest_value,
                    oi_hist.cmc_circulating_supply,
                    oi_hist.timestamp,
                );
            }

            if let Err(e) = sender.send(msg.to_bytes()) {
                error!(
                    "{REST_MONITOR_TAG} Failed to send TopLongShortRatioMsg for {}: {}",
                    symbol, e
                );
            } else {
                sent_count += 1;
            }
        }
    }

    info!(
        "{REST_MONITOR_TAG} [5min Broadcast] close_time={} | sent {} TopLongShortRatioMsg",
        close_time, sent_count
    );
}

// ============================================================================
// 测试入口
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fetch_symbols() {
        let base_url = "https://fapi.binance.com";
        let result = fetch_futures_symbols(base_url).await;
        assert!(result.is_ok());
        let symbols = result.unwrap();
        assert!(!symbols.is_empty());
        println!("Fetched {} symbols", symbols.len());
        for s in symbols.iter().take(10) {
            println!("  {}", s);
        }
    }
}

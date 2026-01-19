//! Binance REST API Fetcher Module
//!
//! 独立的 REST 请求模块，基于本地时钟定时触发，不受 WebSocket 连接重启影响。
//!
//! 功能：
//! - 每分钟请求：PremiumIndex, OpenInterest
//! - 每5分钟请求：TopAccount, TopPosition, GlobalAccount, OpenInterestHist

use base64::engine::general_purpose;
use base64::Engine as _;
use bytes::Bytes;
use ed25519_dalek::{Signer, SigningKey};
use flate2::read::GzDecoder;
use log::{error, info, warn};
use pkcs8::DecodePrivateKey;
use prost::Message;
use reqwest::{
    header::{CONTENT_ENCODING, CONTENT_TYPE},
    Client,
};
use serde::de::{self, Deserializer, Visitor};
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;
use std::io::Read;
use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio::time::{sleep_until, Instant};
use url::form_urlencoded;

use crate::mkt_msg::{
    BarClose1mMsg,
    BinanceMktStatusMsg,
    PremiumIndexKlineMsg,
    TopLongShortRatioMsg,
};
use crate::pb::message_old::{BinanceMktStatus, BorrowStatus};

// ============================================================================
// 常量定义
// ============================================================================

const ONE_MINUTE_MILLIS: i64 = 60_000;
const FIVE_MINUTE_MILLIS: i64 = 5 * ONE_MINUTE_MILLIS;
const REST_MONITOR_TAG: &str = "[REST-MON]";
const BAPI_BORROW_REPAY_PATH: &str = "bapi/margin/v1/public/margin/statistics/24h-borrow-and-repay";
const SAPI_BASE_URL: &str = "https://api.binance.com";
const SAPI_AVAILABLE_INVENTORY_PATH: &str = "sapi/v1/margin/available-inventory";

/// 请求超时时间
const REQUEST_TIMEOUT: Duration = Duration::from_millis(1000);

/// 最大重试次数
const MAX_RETRIES: u32 = 2;

/// 请求延迟（等待交易所数据准备好）
const REQUEST_DELAY_MS: u64 = 1000;

/// 5分钟请求额外延迟
const FIVE_MIN_REQUEST_DELAY_SECS: u64 = 180;
const PERIOD_INIT_TP_MS: i64 = 1_704_067_200_000;
const PERIOD_BASIC_MS: i64 = 3000;

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
    pub bapi_borrow_repay: Result<String, FetchError>,
    pub bapi_available_inventory: Result<String, FetchError>,
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BapiBorrowRepayResponse {
    data: BapiBorrowRepayData,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BapiBorrowRepayData {
    calculation_time: i64,
    coins: Vec<BapiBorrowCoin>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BapiBorrowCoin {
    asset: String,
    total_borrow: f64,
    total_repay: f64,
    total_borrow_in_usdt: f64,
    total_repay_in_usdt: f64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SapiAvailableInventoryResponse {
    #[serde(flatten)]
    data: SapiAvailableInventoryData,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SapiAvailableInventoryData {
    #[serde(deserialize_with = "deserialize_assets_map")]
    assets: HashMap<String, f64>,
    #[serde(deserialize_with = "deserialize_i64_from_string_or_number")]
    update_time: i64,
}

#[derive(Debug, Default)]
struct BinanceMktStatusCache {
    borrow: Option<BapiBorrowRepayResponse>,
    inventory: Option<SapiAvailableInventoryResponse>,
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

fn join_url(base: &str, path: &str) -> String {
    let base = base.trim_end_matches('/');
    let path = path.trim_start_matches('/');
    format!("{}/{}", base, path)
}

fn body_preview(body: &str) -> String {
    const LIMIT: usize = 200;
    let bytes = body.as_bytes();
    let end = bytes.len().min(LIMIT);
    let mut preview = String::from_utf8_lossy(&bytes[..end]).to_string();
    if bytes.len() > end {
        preview.push_str("...");
    }
    preview = preview.replace('\n', "\\n").replace('\r', "\\r");
    preview
}

fn build_query_string(params: &[(&str, &str)]) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    serializer.extend_pairs(params);
    serializer.finish()
}

fn load_binance_ed25519_key() -> Result<SigningKey, FetchError> {
    let key_path = std::env::var("BINANCE_PRIVATE_KEY_PATH").map_err(|_| {
        FetchError::Request("BINANCE_PRIVATE_KEY_PATH not set".to_string())
    })?;
    let pem = fs::read_to_string(&key_path)
        .map_err(|e| FetchError::Request(format!("read private key failed: {}", e)))?;

    if pem.contains("ENCRYPTED PRIVATE KEY") {
        let password = std::env::var("BINANCE_PRIVATE_KEY_PASSWORD").map_err(|_| {
            FetchError::Request("BINANCE_PRIVATE_KEY_PASSWORD not set".to_string())
        })?;
        SigningKey::from_pkcs8_encrypted_pem(pem.as_str(), password.as_str())
            .map_err(|e| FetchError::Request(format!("load private key failed: {}", e)))
    } else {
        SigningKey::from_pkcs8_pem(pem.as_str())
            .map_err(|e| FetchError::Request(format!("load private key failed: {}", e)))
    }
}

fn sign_payload_ed25519(payload: &str, signing_key: &SigningKey) -> String {
    let signature = signing_key.sign(payload.as_bytes());
    general_purpose::STANDARD.encode(signature.to_bytes())
}

fn normalize_timestamp_millis(ts: i64) -> i64 {
    if ts > 0 && ts < 1_000_000_000_000 {
        ts * 1000
    } else {
        ts
    }
}

fn deserialize_assets_map<'de, D>(deserializer: D) -> Result<HashMap<String, f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = HashMap::<String, serde_json::Value>::deserialize(deserializer)?;
    let mut parsed = HashMap::with_capacity(raw.len());
    for (asset, value) in raw {
        let amount = if let Some(num) = value.as_f64() {
            num
        } else if let Some(text) = value.as_str() {
            text.parse::<f64>().map_err(de::Error::custom)?
        } else {
            return Err(de::Error::custom("invalid asset value"));
        };
        parsed.insert(asset, amount);
    }
    Ok(parsed)
}

fn calc_period(tp_ms: i64) -> i64 {
    if tp_ms <= PERIOD_INIT_TP_MS {
        0
    } else {
        (tp_ms - PERIOD_INIT_TP_MS) / PERIOD_BASIC_MS
    }
}

fn decode_response_body(label: &str, body: &[u8]) -> Result<String, FetchError> {
    let is_bapi = label.starts_with("Bapi");
    let is_gzip = body.len() >= 2 && body[0] == 0x1f && body[1] == 0x8b;
    if is_bapi && is_gzip {
        let mut decoder = GzDecoder::new(body);
        let mut decoded = String::new();
        decoder
            .read_to_string(&mut decoded)
            .map_err(|e| FetchError::Request(format!("gzip decode error: {}", e)))?;
        return Ok(decoded);
    }

    Ok(String::from_utf8_lossy(body).to_string())
}

fn deserialize_i64_from_string_or_number<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    struct I64Visitor;

    impl<'de> Visitor<'de> for I64Visitor {
        type Value = i64;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("i64 or string")
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
            Ok(value)
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            i64::try_from(value).map_err(E::custom)
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            value.parse::<i64>().map_err(E::custom)
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str(&value)
        }
    }

    deserializer.deserialize_any(I64Visitor)
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

    let info: ExchangeInfoResponse =
        serde_json::from_str(&body).map_err(|e| FetchError::Json(e.to_string()))?;

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
    headers: &[(&str, &str)],
    label: &str,
    symbol: &str,
) -> Result<String, FetchError> {
    let mut last_error = FetchError::Request("未执行".to_string());

    for attempt in 0..MAX_RETRIES {
        if attempt > 0 {
            // 重试前短暂等待
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let mut request = client.get(url).query(params).timeout(REQUEST_TIMEOUT);
        for (key, value) in headers {
            request = request.header(*key, *value);
        }
        let result = request.send().await;

        match result {
            Ok(response) => {
                let status = response.status();
                let content_type = response
                    .headers()
                    .get(CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("-")
                    .to_string();
                let content_encoding = response
                    .headers()
                    .get(CONTENT_ENCODING)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("-")
                    .to_string();
                let raw_body = match response.bytes().await {
                    Ok(body) => body,
                    Err(e) => {
                        last_error = FetchError::Request(e.to_string());
                        warn!(
                            "{REST_MONITOR_TAG} [{}] {} read body error (attempt {}/{}): {}",
                            label,
                            symbol,
                            attempt + 1,
                            MAX_RETRIES,
                            e
                        );
                        continue;
                    }
                };
                let body = match decode_response_body(label, &raw_body) {
                    Ok(body) => body,
                    Err(e) => {
                        let detail = e.detail();
                        last_error = e;
                        warn!(
                            "{REST_MONITOR_TAG} [{}] {} decode body error (attempt {}/{}): {}",
                            label,
                            symbol,
                            attempt + 1,
                            MAX_RETRIES,
                            detail
                        );
                        continue;
                    }
                };

                if !status.is_success() {
                    last_error = FetchError::Http(status.as_u16());
                    warn!(
                        "{REST_MONITOR_TAG} [{}] {} HTTP {} (attempt {}/{}) content-type={} content-encoding={} body_len={} body=\"{}\"",
                        label,
                        symbol,
                        status,
                        attempt + 1,
                        MAX_RETRIES,
                        content_type,
                        content_encoding,
                        body.len(),
                        body_preview(&body)
                    );
                    continue;
                }

                if body.trim().is_empty() {
                    last_error = FetchError::EmptyResponse;
                    warn!(
                        "{REST_MONITOR_TAG} [{}] {} empty body (attempt {}/{}) content-type={} content-encoding={}",
                        label,
                        symbol,
                        attempt + 1,
                        MAX_RETRIES,
                        content_type,
                        content_encoding
                    );
                    continue;
                }

                return Ok(body);
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

async fn fetch_bapi_endpoint(
    client: &Client,
    base_url: &str,
    path: &str,
    label: &str,
) -> Result<String, FetchError> {
    let url = join_url(base_url, path);
    fetch_with_retry(client, &url, &[], &[], label, "bapi").await
}

async fn fetch_sapi_available_inventory(client: &Client) -> Result<String, FetchError> {
    let api_key = std::env::var("BINANCE_API_KEY")
        .map_err(|_| FetchError::Request("BINANCE_API_KEY not set".to_string()))?;
    let signing_key = load_binance_ed25519_key()?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| FetchError::Request(format!("clock error: {}", e)))?
        .as_millis()
        .to_string();
    let recv_window = "5000".to_string();
    let margin_type = "margin".to_string();

    let query_params = vec![
        ("type", margin_type.as_str()),
        ("timestamp", timestamp.as_str()),
        ("recvWindow", recv_window.as_str()),
    ];
    let payload = build_query_string(&query_params);
    let signature = sign_payload_ed25519(&payload, &signing_key);
    let signed_params = vec![
        ("type", margin_type.as_str()),
        ("timestamp", timestamp.as_str()),
        ("recvWindow", recv_window.as_str()),
        ("signature", signature.as_str()),
    ];

    let url = join_url(SAPI_BASE_URL, SAPI_AVAILABLE_INVENTORY_PATH);
    fetch_with_retry(
        client,
        &url,
        &signed_params,
        &[("X-MBX-APIKEY", api_key.as_str())],
        "SapiAvailableInventory",
        "sapi",
    )
    .await
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
        &[],
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
            record
                .get(idx)
                .and_then(|v| v.as_i64().or_else(|| v.as_str()?.parse::<i64>().ok()))
        };
        let parse_f64 = |idx: usize| -> Option<f64> {
            record
                .get(idx)
                .and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse::<f64>().ok()))
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
    let body =
        fetch_with_retry(client, &url, &[("symbol", symbol)], &[], "OpenInterest", symbol).await?;

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
        &[],
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
                label, symbol, close_time
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
        &[],
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
                symbol, close_time
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
    spot_base_url: String,
    futures_base_url: String,
    client: Client,
    symbols: Vec<String>,
}

impl BinanceRestFetcher {
    /// 创建新的 REST Fetcher
    pub async fn new(spot_base_url: String, futures_base_url: String) -> Result<Self, FetchError> {
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .pool_max_idle_per_host(100)
            .build()
            .map_err(|e| FetchError::Request(e.to_string()))?;

        // 获取 symbol 列表
        info!(
            "{REST_MONITOR_TAG} Fetching futures symbols from {}",
            futures_base_url
        );
        let symbols = fetch_futures_symbols(&futures_base_url).await?;
        info!(
            "{REST_MONITOR_TAG} Fetched {} futures symbols",
            symbols.len()
        );

        Ok(Self {
            spot_base_url,
            futures_base_url,
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
        let symbols = fetch_futures_symbols(&self.futures_base_url).await?;
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
                let base_url = self.futures_base_url.clone();
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
                let base_url = self.futures_base_url.clone();
                let symbol = symbol.clone();
                async move {
                    let result = fetch_open_interest(&client, &base_url, &symbol).await;
                    (symbol, result)
                }
            })
            .collect();

        let bapi_client = self.client.clone();
        let bapi_base_url = self.spot_base_url.clone();
        let bapi_borrow_future = fetch_bapi_endpoint(
            &bapi_client,
            &bapi_base_url,
            BAPI_BORROW_REPAY_PATH,
            "BapiBorrowRepay",
        );
        let bapi_inventory_future = fetch_sapi_available_inventory(&bapi_client);

        // 等待所有请求完成
        let (premium_results, oi_results, bapi_borrow_repay, bapi_available_inventory) = tokio::join!(
            futures::future::join_all(premium_futures),
            futures::future::join_all(oi_futures),
            bapi_borrow_future,
            bapi_inventory_future,
        );

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
            bapi_borrow_repay,
            bapi_available_inventory,
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
                let base_url = self.futures_base_url.clone();
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
                let base_url = self.futures_base_url.clone();
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
                let base_url = self.futures_base_url.clone();
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
                let base_url = self.futures_base_url.clone();
                let symbol = symbol.clone();
                async move {
                    let result =
                        fetch_open_interest_hist(&client, &base_url, &symbol, close_time).await;
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

    let bapi_borrow_ok = result.bapi_borrow_repay.is_ok();
    let bapi_inventory_ok = result.bapi_available_inventory.is_ok();

    info!(
        "{REST_MONITOR_TAG} [1min Summary] close_time={} | PremiumIndex: {}/{} success | OpenInterest: {}/{} success | BAPI BorrowRepay: {} | SAPI Inventory: {}",
        result.close_time,
        pi_success,
        result.premium_index.len(),
        oi_success,
        result.open_interest.len(),
        if bapi_borrow_ok { "ok" } else { "err" },
        if bapi_inventory_ok { "ok" } else { "err" }
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

    if let Err(e) = &result.bapi_borrow_repay {
        warn!("{REST_MONITOR_TAG}   BAPI BorrowRepay failed: {}", e.detail());
    }
    if let Err(e) = &result.bapi_available_inventory {
        warn!("{REST_MONITOR_TAG}   SAPI Inventory failed: {}", e.detail());
    }
}

/// 打印5分钟请求汇总
fn print_five_minute_summary(result: &FiveMinuteResult) {
    let ta_success = result.top_account.iter().filter(|r| r.is_ok()).count();
    let tp_success = result.top_position.iter().filter(|r| r.is_ok()).count();
    let ga_success = result.global_account.iter().filter(|r| r.is_ok()).count();
    let oh_success = result
        .open_interest_hist
        .iter()
        .filter(|r| r.is_ok())
        .count();

    let total = result.top_account.len();

    info!(
        "{REST_MONITOR_TAG} [5min Summary] close_time={} | TopAccount: {}/{} | TopPosition: {}/{} | GlobalAccount: {}/{} | OIHist: {}/{}",
        result.close_time, ta_success, total, tp_success, total, ga_success, total, oh_success, total
    );
}

struct BinanceMktStatusPayload {
    period: i64,
    info_count: i64,
    payload: Bytes,
}

fn update_bapi_borrow_cache(
    cache: &mut BinanceMktStatusCache,
    body: &str,
) -> Result<(), FetchError> {
    let mut parsed: BapiBorrowRepayResponse = serde_json::from_str(body).map_err(|e| {
        warn!(
            "{REST_MONITOR_TAG} BAPI BorrowRepay JSON parse failed: {} | body_len={} | body=\"{}\"",
            e,
            body.len(),
            body_preview(body)
        );
        FetchError::Json(e.to_string())
    })?;
    parsed.data.calculation_time = normalize_timestamp_millis(parsed.data.calculation_time);
    let new_time = parsed.data.calculation_time;
    if let Some(old) = cache.borrow.as_ref() {
        let old_time = old.data.calculation_time;
        if new_time < old_time {
            warn!(
                "{REST_MONITOR_TAG} BAPI BorrowRepay time rollback: new={} old={}",
                new_time, old_time
            );
            return Ok(());
        }
    }
    cache.borrow = Some(parsed);
    Ok(())
}

fn update_sapi_inventory_cache(
    cache: &mut BinanceMktStatusCache,
    body: &str,
) -> Result<(), FetchError> {
    let mut parsed: SapiAvailableInventoryResponse = serde_json::from_str(body).map_err(|e| {
        warn!(
            "{REST_MONITOR_TAG} SAPI Inventory JSON parse failed: {} | body_len={} | body=\"{}\"",
            e,
            body.len(),
            body_preview(body)
        );
        FetchError::Json(e.to_string())
    })?;
    parsed.data.update_time = normalize_timestamp_millis(parsed.data.update_time);
    {
        let assets = &parsed.data.assets;
        let btc = assets.get("BTC").copied().unwrap_or(0.0);
        let eth = assets.get("ETH").copied().unwrap_or(0.0);
        let sol = assets.get("SOL").copied().unwrap_or(0.0);
        info!(
            "{REST_MONITOR_TAG} SAPI available-inventory BTC={} ETH={} SOL={} update_time={}",
            btc, eth, sol, parsed.data.update_time
        );
    }
    let new_time = parsed.data.update_time;
    if let Some(old) = cache.inventory.as_ref() {
        let old_time = old.data.update_time;
        if new_time < old_time {
            warn!(
                "{REST_MONITOR_TAG} SAPI Inventory time rollback: new={} old={}",
                new_time, old_time
            );
            return Ok(());
        }
    }
    cache.inventory = Some(parsed);
    Ok(())
}

fn build_binance_mkt_status_payload(
    borrow_resp: &BapiBorrowRepayResponse,
    inventory_resp: &SapiAvailableInventoryResponse,
    period: i64,
) -> Result<BinanceMktStatusPayload, FetchError> {
    let mut status_map: HashMap<String, BorrowStatus> = HashMap::new();
    for coin in &borrow_resp.data.coins {
        status_map.insert(
            coin.asset.clone(),
            BorrowStatus {
                asset: coin.asset.clone(),
                total_borrow: coin.total_borrow,
                total_repay: coin.total_repay,
                total_borrow_in_usdt: coin.total_borrow_in_usdt,
                total_repay_in_usdt: coin.total_repay_in_usdt,
                available_inventory: 0.0,
            },
        );
    }

    for (asset, available_inventory) in &inventory_resp.data.assets {
        status_map
            .entry(asset.clone())
            .and_modify(|status| status.available_inventory = *available_inventory)
            .or_insert(BorrowStatus {
                asset: asset.clone(),
                total_borrow: 0.0,
                total_repay: 0.0,
                total_borrow_in_usdt: 0.0,
                total_repay_in_usdt: 0.0,
                available_inventory: *available_inventory,
            });
    }

    let mut borrow_statuses: Vec<BorrowStatus> = status_map.into_values().collect();
    borrow_statuses.sort_by(|a, b| a.asset.cmp(&b.asset));

    let calculation_time = borrow_resp.data.calculation_time;
    let update_time = inventory_resp.data.update_time;
    let last_time = calculation_time.max(update_time);

    let info_count = borrow_statuses.len() as i64;
    let status = BinanceMktStatus {
        last_calculation_time: last_time,
        borrow_statuses,
        period,
        last_update_time: update_time,
    };

    let payload = status.encode_to_vec();
    Ok(BinanceMktStatusPayload {
        period,
        info_count,
        payload: Bytes::from(payload),
    })
}

// ============================================================================
// 带消息推送的运行函数
// ============================================================================

/// 运行 REST Fetcher 主循环（带消息推送）
pub async fn run_rest_fetcher_with_sender(
    spot_base_url: String,
    futures_base_url: String,
    sender: broadcast::Sender<Bytes>,
) {
    info!(
        "{REST_MONITOR_TAG} Starting BinanceRestFetcher | spot_base_url={} | futures_base_url={} (with message sender)",
        spot_base_url, futures_base_url
    );

    let mut fetcher = match BinanceRestFetcher::new(spot_base_url, futures_base_url).await {

        Ok(f) => f,
        Err(e) => {
            error!(
                "{REST_MONITOR_TAG} Failed to create BinanceRestFetcher: {:?}",
                e
            );
            return;
        }
    };

    info!(
        "{REST_MONITOR_TAG} BinanceRestFetcher initialized with {} symbols",
        fetcher.symbols().len()
    );

    let mut pending_five_min: Option<(i64, Instant)> = None;
    let mut bapi_cache = BinanceMktStatusCache::default();

    loop {
        // 等待下一个分钟边界
        let (next_instant, close_time) = next_minute_boundary();
        info!(
            "{REST_MONITOR_TAG} waiting for next minute boundary | close_time={} | wait={:?}",
            close_time,
            next_instant - Instant::now()
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
            let ready_instant = Instant::now() + Duration::from_secs(FIVE_MIN_REQUEST_DELAY_SECS);
            pending_five_min = Some((close_time, ready_instant));
            info!(
                "{REST_MONITOR_TAG} scheduled 5-minute requests | close_time={} | will run after {}s",
                close_time, FIVE_MIN_REQUEST_DELAY_SECS
            );
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
        send_binance_mkt_status_message(&one_min_result, &mut bapi_cache, &sender);
        print_one_minute_summary(&one_min_result);

        // 发送1分钟封bar消息
        let bar_close_msg = BarClose1mMsg::create(close_time);
        info!(
            "{REST_MONITOR_TAG} [BarClose1m] 准备发送封bar消息 | close_time={}",
            close_time
        );
        if let Err(e) = sender.send(bar_close_msg.to_bytes()) {
            error!(
                "{REST_MONITOR_TAG} Failed to send BarClose1mMsg for close_time={}: {}",
                close_time, e
            );
        } else {
            info!(
                "{REST_MONITOR_TAG} [BarClose1m] 封bar消息发送成功 | close_time={}",
                close_time
            );
        }

        // 如果是5分钟边界，执行5分钟请求
        if let Some((scheduled_close_time, ready_instant)) = pending_five_min {
            if Instant::now() >= ready_instant {
                info!(
                    "{REST_MONITOR_TAG} executing 5-minute requests | close_time={} | symbols={}",
                    scheduled_close_time,
                    fetcher.symbols().len()
                );
                let five_min_result = fetcher.fetch_five_minute(scheduled_close_time).await;

                // 发送5分钟消息
                send_five_minute_messages(&five_min_result, &sender);
                print_five_minute_summary(&five_min_result);
                pending_five_min = None;
            }
        }
    }
}

/// 发送1分钟消息（PremiumIndexKline）
fn send_one_minute_messages(result: &OneMinuteResult, sender: &broadcast::Sender<Bytes>) {
    let close_time = result.close_time;

    // 将 PremiumIndex 和 OpenInterest 合并成 PremiumIndexKlineMsg
    // 创建一个 HashMap 来匹配 symbol
    let mut oi_map: std::collections::HashMap<String, &OpenInterestData> =
        std::collections::HashMap::new();
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

fn send_binance_mkt_status_message(
    result: &OneMinuteResult,
    cache: &mut BinanceMktStatusCache,
    sender: &broadcast::Sender<Bytes>,
) {
    if let Ok(body) = &result.bapi_borrow_repay {
        if let Err(e) = update_bapi_borrow_cache(cache, body) {
            warn!(
                "{REST_MONITOR_TAG} Failed to parse BAPI BorrowRepay: {}",
                e.detail()
            );
        }
    }
    if let Ok(body) = &result.bapi_available_inventory {
        if let Err(e) = update_sapi_inventory_cache(cache, body) {
            warn!(
                "{REST_MONITOR_TAG} Failed to parse SAPI Inventory: {}",
                e.detail()
            );
        }
    }

    let (borrow_resp, inventory_resp) = match (&cache.borrow, &cache.inventory) {
        (Some(borrow_resp), Some(inventory_resp)) => (borrow_resp, inventory_resp),
        _ => {
            info!("{REST_MONITOR_TAG} BAPI/SAPI cache not ready, skip BinanceMktStatus");
            return;
        }
    };

    let period = calc_period(result.close_time);
    match build_binance_mkt_status_payload(borrow_resp, inventory_resp, period) {
        Ok(payload) => {
            let msg = BinanceMktStatusMsg::create(
                payload.period,
                payload.info_count,
                payload.payload,
            );
            if let Err(e) = sender.send(msg.to_bytes()) {
                error!("{REST_MONITOR_TAG} Failed to send BinanceMktStatus msg: {}", e);
            }
        }
        Err(e) => {
            warn!(
                "{REST_MONITOR_TAG} Failed to build BinanceMktStatus payload: {}",
                e.detail()
            );
        }
    }
}

/// 发送5分钟消息（TopLongShortRatioMsg）
fn send_five_minute_messages(result: &FiveMinuteResult, sender: &broadcast::Sender<Bytes>) {
    let close_time = result.close_time;

    // 创建 HashMap 来匹配各类数据
    let mut top_account_map: std::collections::HashMap<String, &RatioMetricsData> =
        std::collections::HashMap::new();
    let mut top_position_map: std::collections::HashMap<String, &RatioMetricsData> =
        std::collections::HashMap::new();
    let mut global_account_map: std::collections::HashMap<String, &RatioMetricsData> =
        std::collections::HashMap::new();
    let mut oi_hist_map: std::collections::HashMap<String, &OpenInterestHistData> =
        std::collections::HashMap::new();

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
// 轻量级 Bar Close 定时器（无 REST 请求）
// ============================================================================

/// 运行轻量级的 Bar Close 定时器（仅发送 BarClose1mMsg，不执行任何 REST 请求）
/// 用于现货等不需要 REST 数据但需要封 bar 信号的交易所
pub async fn run_bar_close_timer(exchange_name: String, sender: broadcast::Sender<Bytes>) {
    info!(
        "{REST_MONITOR_TAG} Starting lightweight BarClose timer for exchange: {}",
        exchange_name
    );

    loop {
        // 等待下一个分钟边界
        let (next_instant, close_time) = next_minute_boundary();
        info!(
            "{REST_MONITOR_TAG} [{}] waiting for next minute boundary | close_time={} | wait={:?}",
            exchange_name,
            close_time,
            next_instant - Instant::now()
        );
        sleep_until(next_instant).await;

        // 延迟等待交易所数据准备好
        tokio::time::sleep(Duration::from_millis(REQUEST_DELAY_MS)).await;

        // 发送封 bar 消息
        let bar_close_msg = BarClose1mMsg::create(close_time);
        info!(
            "{REST_MONITOR_TAG} [{}] [BarClose1m] 准备发送封bar消息 | close_time={}",
            exchange_name, close_time
        );
        if let Err(e) = sender.send(bar_close_msg.to_bytes()) {
            error!(
                "{REST_MONITOR_TAG} [{}] Failed to send BarClose1mMsg for close_time={}: {}",
                exchange_name, close_time, e
            );
        } else {
            info!(
                "{REST_MONITOR_TAG} [{}] [BarClose1m] 封bar消息发送成功 | close_time={}",
                exchange_name, close_time
            );
        }
    }
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

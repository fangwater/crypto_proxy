use crate::cfg::BinanceRestCfg;
use crate::mkt_msg::{
    BinanceIncSeqNoMsg, FundingRateMsg, IncMsg, IndexPriceMsg, KlineMsg, Level, LiquidationMsg,
    MarkPriceMsg, PremiumIndexKlineMsg, RestRequestType, RestSummary1mMsg, RestSummary5mMsg,
    RestSummaryEntry, SignalMsg, SignalSource, TopLongShortRatioMsg, TradeMsg,
};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use log::{error, info, warn};
use reqwest::{self, StatusCode};
use std::collections::HashSet;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};

const ONE_MINUTE_MILLIS: i64 = 60_000;
const FIVE_MINUTE_MILLIS: i64 = 5 * ONE_MINUTE_MILLIS;
const PREMIUM_INDEX_DELAY_SECS: u64 = 10;

#[derive(Clone)]
struct RestResult {
    request: RestRequestType,
    success: bool,
    detail: String,
}

impl RestResult {
    fn success(request: RestRequestType, detail: impl Into<String>) -> Self {
        Self {
            request,
            success: true,
            detail: detail.into(),
        }
    }

    fn failure(request: RestRequestType, detail: impl Into<String>) -> Self {
        Self {
            request,
            success: false,
            detail: detail.into(),
        }
    }
}

enum RestSummaryStage {
    OneMinute,
    FiveMinute,
}

impl RestSummaryStage {
    fn requests(&self) -> &'static [RestRequestType] {
        match self {
            RestSummaryStage::OneMinute => {
                &[RestRequestType::PremiumIndex, RestRequestType::OpenInterest]
            }
            RestSummaryStage::FiveMinute => &[
                RestRequestType::TopAccount,
                RestRequestType::TopPosition,
                RestRequestType::GlobalAccount,
                RestRequestType::OpenInterestHist,
            ],
        }
    }

    fn label(&self) -> &'static str {
        match self {
            RestSummaryStage::OneMinute => "1m",
            RestSummaryStage::FiveMinute => "5m",
        }
    }
}

#[derive(Default)]
struct RestSummaryCollector {
    premium_index: Option<RestResult>,
    open_interest: Option<RestResult>,
    top_account: Option<RestResult>,
    top_position: Option<RestResult>,
    global_account: Option<RestResult>,
    open_interest_hist: Option<RestResult>,
}

impl RestSummaryCollector {
    fn update(&mut self, result: RestResult) {
        match result.request {
            RestRequestType::PremiumIndex => self.premium_index = Some(result),
            RestRequestType::OpenInterest => self.open_interest = Some(result),
            RestRequestType::TopAccount => self.top_account = Some(result),
            RestRequestType::TopPosition => self.top_position = Some(result),
            RestRequestType::GlobalAccount => self.global_account = Some(result),
            RestRequestType::OpenInterestHist => self.open_interest_hist = Some(result),
        }
    }

    fn get(&self, request: RestRequestType) -> Option<&RestResult> {
        match request {
            RestRequestType::PremiumIndex => self.premium_index.as_ref(),
            RestRequestType::OpenInterest => self.open_interest.as_ref(),
            RestRequestType::TopAccount => self.top_account.as_ref(),
            RestRequestType::TopPosition => self.top_position.as_ref(),
            RestRequestType::GlobalAccount => self.global_account.as_ref(),
            RestRequestType::OpenInterestHist => self.open_interest_hist.as_ref(),
        }
    }
}

fn report_rest_summary(
    sender: &broadcast::Sender<Bytes>,
    symbol: &str,
    close_time: i64,
    collector: &RestSummaryCollector,
    stage: RestSummaryStage,
) {
    let requests = stage.requests();
    let mut summary_entries: Vec<RestSummaryEntry> = Vec::with_capacity(requests.len());

    for request in requests.iter() {
        if let Some(result) = collector.get(*request) {
            summary_entries.push(RestSummaryEntry::new(
                result.request,
                result.success,
                result.detail.clone(),
            ));
        } else {
            summary_entries.push(RestSummaryEntry::new(*request, false, "未执行"));
        }
    }

    let send_result = match stage {
        RestSummaryStage::OneMinute => {
            let summary_msg = RestSummary1mMsg::create(
                symbol.to_string(),
                close_time,
                summary_entries[0].clone(),
                summary_entries[1].clone(),
            );
            sender.send(summary_msg.to_bytes())
        }
        RestSummaryStage::FiveMinute => {
            let summary_msg = RestSummary5mMsg::create(
                symbol.to_string(),
                close_time,
                summary_entries[0].clone(),
                summary_entries[1].clone(),
                summary_entries[2].clone(),
                summary_entries[3].clone(),
            );
            sender.send(summary_msg.to_bytes())
        }
    };

    if let Err(err) = send_result {
        error!("Failed to broadcast REST summary for {}: {}", symbol, err);
    }

    if summary_entries.iter().all(|entry| entry.success) {
        return;
    }

    let mut table = String::new();
    table.push_str("\n+----------------------+----------+----------------------+");
    table.push_str("\n| Request              | Status   | Detail               |");
    table.push_str("\n+----------------------+----------+----------------------+");
    for entry in &summary_entries {
        let status = if entry.success { "成功" } else { "失败" };
        table.push_str(&format!(
            "\n| {:<20} | {:<8} | {:<20} |",
            entry.request_type.as_str(),
            status,
            entry.detail.as_str()
        ));
    }
    table.push_str("\n+----------------------+----------+----------------------+");

    info!(
        "[Binance REST Summary {}] symbol={} close_time={}{}",
        stage.label(),
        symbol,
        close_time,
        table
    );
}

enum FetchError {
    Request(String),
    Http(StatusCode),
    Json(String),
    EmptyResponse,
    MatchFailure,
    MissingField(&'static str),
}

impl FetchError {
    fn detail(&self) -> String {
        match self {
            FetchError::Request(err) => format!("请求错误: {}", err),
            FetchError::Http(code) => format!("HTTP {}", code),
            FetchError::Json(err) => format!("JSON错误: {}", err),
            FetchError::EmptyResponse => "空响应".to_string(),
            FetchError::MatchFailure => "匹配失败".to_string(),
            FetchError::MissingField(field) => format!("缺少字段 {}", field),
        }
    }
}

pub struct BinanceSignalParser {
    source: SignalSource,
}

impl BinanceSignalParser {
    pub fn new(is_ipc: bool) -> Self {
        Self {
            source: if is_ipc {
                SignalSource::Ipc
            } else {
                SignalSource::Tcp
            },
        }
    }
}

impl Parser for BinanceSignalParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse Binance depth message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Extract Binance timestamp field "E"
                if let Some(timestamp) = json_value.get("E").and_then(|v| v.as_i64()) {
                    // Create signal message
                    let signal_msg = SignalMsg::create(self.source, timestamp);
                    let signal_bytes = signal_msg.to_bytes();

                    // Send signal
                    if let Err(_) = sender.send(signal_bytes) {
                        return 0;
                    }

                    return 1;
                }
            }
        }
        0
    }
}

pub struct BinanceKlineParser {
    is_future: bool,
    http_client: Option<reqwest::Client>,
    premium_index_klines_url: Option<String>,
    open_interest_url: Option<String>,
    open_interest_hist_url: Option<String>,
    top_long_short_account_ratio_url: Option<String>,
    top_long_short_position_ratio_url: Option<String>,
    global_long_short_account_ratio_url: Option<String>,
}

impl BinanceKlineParser {
    pub fn new(is_future: bool, rest_cfg: Option<&BinanceRestCfg>) -> Self {
        if is_future {
            let cfg = rest_cfg.expect("Binance futures kline parser requires REST config");
            Self {
                is_future,
                http_client: Some(reqwest::Client::new()),
                premium_index_klines_url: Some(cfg.premium_index_klines_url()),
                open_interest_url: Some(cfg.open_interest_url()),
                open_interest_hist_url: Some(cfg.open_interest_hist_url()),
                top_long_short_account_ratio_url: Some(cfg.top_long_short_account_ratio_url()),
                top_long_short_position_ratio_url: Some(cfg.top_long_short_position_ratio_url()),
                global_long_short_account_ratio_url: Some(
                    cfg.global_long_short_account_ratio_url(),
                ),
            }
        } else {
            Self {
                is_future,
                http_client: None,
                premium_index_klines_url: None,
                open_interest_url: None,
                open_interest_hist_url: None,
                top_long_short_account_ratio_url: None,
                top_long_short_position_ratio_url: None,
                global_long_short_account_ratio_url: None,
            }
        }
    }
}

impl Parser for BinanceKlineParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse Binance kline message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 从顶层s字段直接获取symbol
                if let Some(symbol) = json_value.get("s").and_then(|v| v.as_str()) {
                    // 获取k对象中的K线数据
                    if let Some(kline_obj) = json_value.get("k") {
                        // 检查x字段 - 只处理已关闭的K线
                        if let Some(is_closed) = kline_obj.get("x").and_then(|v| v.as_bool()) {
                            if !is_closed {
                                return 0; // K线未关闭，不处理
                            }
                        } else {
                            return 0; // x字段无效或缺失
                        }

                        // 从k对象中提取OHLCV数据
                        // 币安额外3个字段：n(成交笔数), V(主动买入成交量), Q(主动买入成交额)
                        if let (
                            Some(open_str),
                            Some(high_str),
                            Some(low_str),
                            Some(close_str),
                            Some(volume_str),
                            Some(turnover_str),
                            Some(timestamp),
                            Some(trade_num),
                            Some(taker_buy_vol_str),
                            Some(taker_buy_quote_vol_str),
                        ) = (
                            kline_obj.get("o").and_then(|v| v.as_str()),
                            kline_obj.get("h").and_then(|v| v.as_str()),
                            kline_obj.get("l").and_then(|v| v.as_str()),
                            kline_obj.get("c").and_then(|v| v.as_str()),
                            kline_obj.get("v").and_then(|v| v.as_str()), //成交量
                            kline_obj.get("q").and_then(|v| v.as_str()), //成交额
                            kline_obj.get("t").and_then(|v| v.as_i64()),
                            kline_obj.get("n").and_then(|v| v.as_i64()), //成交笔数
                            kline_obj.get("V").and_then(|v| v.as_str()), //主动买入成交量
                            kline_obj.get("Q").and_then(|v| v.as_str()), //主动买入成交额
                        ) {
                            let close_time = kline_obj
                                .get("T")
                                .and_then(|v| v.as_i64())
                                .unwrap_or(timestamp + 1);
                            // 只为BTCUSDT打印OHLCV数据
                            if symbol.to_lowercase() == "btcusdt" {
                                info!("[Binance Kline] BTCUSDT OHLCV: o={}, h={}, l={}, c={}, v={}, q={}, t={}, n={}, V={}, Q={}", 
                                      open_str, high_str, low_str, close_str, volume_str, turnover_str, timestamp, trade_num, taker_buy_vol_str, taker_buy_quote_vol_str);
                            }
                            // 解析价格和成交量数据
                            if let (
                                Ok(open),
                                Ok(high),
                                Ok(low),
                                Ok(close),
                                Ok(volume),
                                Ok(turnover),
                                Ok(taker_buy_vol),
                                Ok(taker_buy_quote_vol),
                            ) = (
                                open_str.parse::<f64>(),
                                high_str.parse::<f64>(),
                                low_str.parse::<f64>(),
                                close_str.parse::<f64>(),
                                volume_str.parse::<f64>(),
                                turnover_str.parse::<f64>(),
                                taker_buy_vol_str.parse::<f64>(),
                                taker_buy_quote_vol_str.parse::<f64>(),
                            ) {
                                // 创建K线消息
                                let mut kline_msg = KlineMsg::create(
                                    symbol.to_string(),
                                    open,
                                    high,
                                    low,
                                    close,
                                    volume,
                                    turnover,
                                    timestamp,
                                );

                                // 设置币安专属字段
                                kline_msg.set_binance_fields(
                                    trade_num,
                                    taker_buy_vol,
                                    taker_buy_quote_vol,
                                );

                                if self.is_future {
                                    if let Some(client) = &self.http_client {
                                        let premium_index_url = match &self.premium_index_klines_url
                                        {
                                            Some(url) => url.clone(),
                                            None => {
                                                error!("Missing Binance futures premium index kline URL in configuration");
                                                return 0;
                                            }
                                        };
                                        let open_interest_url = match &self.open_interest_url {
                                            Some(url) => url.clone(),
                                            None => {
                                                error!("Missing Binance futures open interest URL in configuration");
                                                return 0;
                                            }
                                        };
                                        let open_interest_hist_url = match &self
                                            .open_interest_hist_url
                                        {
                                            Some(url) => url.clone(),
                                            None => {
                                                error!("Missing Binance futures open interest history URL in configuration");
                                                return 0;
                                            }
                                        };
                                        let top_account_ratio_url = match &self
                                            .top_long_short_account_ratio_url
                                        {
                                            Some(url) => url.clone(),
                                            None => {
                                                error!("Missing Binance futures top long short account ratio URL in configuration");
                                                return 0;
                                            }
                                        };
                                        let top_position_ratio_url = match &self
                                            .top_long_short_position_ratio_url
                                        {
                                            Some(url) => url.clone(),
                                            None => {
                                                error!(
                                                        "Missing Binance futures top long short position ratio URL in configuration"
                                                    );
                                                return 0;
                                            }
                                        };
                                        let global_account_ratio_url = match &self
                                            .global_long_short_account_ratio_url
                                        {
                                            Some(url) => url.clone(),
                                            None => {
                                                error!(
                                                        "Missing Binance futures global long short account ratio URL in configuration"
                                                    );
                                                return 0;
                                            }
                                        };
                                        let sender_clone = sender.clone();
                                        let symbol_owned = symbol.to_string();
                                        let client_clone = client.clone();
                                        let kline_open_tp = timestamp;
                                        let mut kline_close_tp = close_time;

                                        tokio::spawn(async move {
                                            if PREMIUM_INDEX_DELAY_SECS > 0 {
                                                sleep(Duration::from_secs(
                                                    PREMIUM_INDEX_DELAY_SECS,
                                                ))
                                                .await;
                                            }
                                            let mut rest_summary = RestSummaryCollector::default();
                                            let premium_resp = client_clone
                                                .get(premium_index_url.as_str())
                                                .query(&[
                                                    ("symbol", symbol_owned.as_str()),
                                                    ("interval", "1m"),
                                                    ("limit", "2"),
                                                ])
                                                .timeout(Duration::from_secs(5))
                                                .send()
                                                .await;

                                            let premium_resp = match premium_resp {
                                                Ok(resp) => resp,
                                                Err(err) => {
                                                    error!(
                                                        "Premium Index request error for {}: {}",
                                                        symbol_owned, err
                                                    );
                                                    rest_summary.update(RestResult::failure(
                                                        RestRequestType::PremiumIndex,
                                                        format!("请求错误: {}", err),
                                                    ));
                                                    report_rest_summary(
                                                        &sender_clone,
                                                        symbol_owned.as_str(),
                                                        kline_close_tp,
                                                        &rest_summary,
                                                        RestSummaryStage::OneMinute,
                                                    );
                                                    return;
                                                }
                                            };

                                            let status = premium_resp.status();
                                            let body =
                                                premium_resp.text().await.unwrap_or_else(|_| {
                                                    "Unable to read response body".to_string()
                                                });

                                            if !status.is_success() {
                                                if status != StatusCode::SERVICE_UNAVAILABLE
                                                    && status != StatusCode::REQUEST_TIMEOUT
                                                {
                                                    error!(
                                                        "Premium Index HTTP {} for {}: {}",
                                                        status, symbol_owned, body
                                                    );
                                                }
                                                rest_summary.update(RestResult::failure(
                                                    RestRequestType::PremiumIndex,
                                                    format!("HTTP {}", status),
                                                ));
                                                report_rest_summary(
                                                    &sender_clone,
                                                    symbol_owned.as_str(),
                                                    kline_close_tp,
                                                    &rest_summary,
                                                    RestSummaryStage::OneMinute,
                                                );
                                                return;
                                            }

                                            let records: Vec<Vec<serde_json::Value>> =
                                                match serde_json::from_str(&body) {
                                                    Ok(data) => data,
                                                    Err(err) => {
                                                        error!(
                                                            "Premium Index JSON parse error for {}: {}",
                                                            symbol_owned, err
                                                        );
                                                        rest_summary.update(RestResult::failure(
                                                            RestRequestType::PremiumIndex,
                                                            format!("JSON错误: {}", err),
                                                        ));
                                                        report_rest_summary(
                                                            &sender_clone,
                                                            symbol_owned.as_str(),
                                                            kline_close_tp,
                                                            &rest_summary,
                                                            RestSummaryStage::OneMinute,
                                                        );
                                                        return;
                                                    }
                                                };

                                            if records.is_empty() {
                                                error!(
                                                    "Premium Index response empty for {}",
                                                    symbol_owned
                                                );
                                                rest_summary.update(RestResult::failure(
                                                    RestRequestType::PremiumIndex,
                                                    "空响应",
                                                ));
                                                report_rest_summary(
                                                    &sender_clone,
                                                    symbol_owned.as_str(),
                                                    kline_close_tp,
                                                    &rest_summary,
                                                    RestSummaryStage::OneMinute,
                                                );
                                                return;
                                            }

                                            let parse_record = |record: &Vec<serde_json::Value>| -> Option<(i64, f64, f64, f64, f64)> {
                                                let parse_i64 = |idx: usize, field: &str| -> Option<i64> {
                                                    record
                                                        .get(idx)
                                                        .and_then(|v| {
                                                            v.as_i64().or_else(|| {
                                                                v.as_str()?.parse::<i64>().ok()
                                                            })
                                                        })
                                                        .or_else(|| {
                                                            error!(
                                                                "Premium Index invalid {} for {}",
                                                                field, symbol_owned
                                                            );
                                                            None
                                                        })
                                                };

                                                let parse_f64 = |idx: usize, field: &str| -> Option<f64> {
                                                    record
                                                        .get(idx)
                                                        .and_then(|v| {
                                                            v.as_f64().or_else(|| {
                                                                v.as_str()?.parse::<f64>().ok()
                                                            })
                                                        })
                                                        .or_else(|| {
                                                            error!(
                                                                "Premium Index invalid {} for {}",
                                                                field, symbol_owned
                                                            );
                                                            None
                                                        })
                                                };

                                                Some((
                                                    parse_i64(0, "open time")?,
                                                    parse_f64(1, "open price")?,
                                                    parse_f64(2, "high price")?,
                                                    parse_f64(3, "low price")?,
                                                    parse_f64(4, "close price")?,
                                                ))
                                            };

                                            let primary = match parse_record(&records[0]) {
                                                Some(values) => values,
                                                None => {
                                                    rest_summary.update(RestResult::failure(
                                                        RestRequestType::PremiumIndex,
                                                        "记录解析失败",
                                                    ));
                                                    report_rest_summary(
                                                        &sender_clone,
                                                        symbol_owned.as_str(),
                                                        kline_close_tp,
                                                        &rest_summary,
                                                        RestSummaryStage::OneMinute,
                                                    );
                                                    return;
                                                }
                                            };
                                            let secondary = records.get(1).and_then(parse_record);

                                            let primary_open_time = primary.0;
                                            let selected_record =
                                                if let Some(secondary_record) = secondary {
                                                    if secondary_record.0 == kline_open_tp {
                                                        Some(secondary_record)
                                                    } else if primary_open_time == kline_open_tp {
                                                        Some(primary)
                                                    } else {
                                                        None
                                                    }
                                                } else if primary_open_time == kline_open_tp {
                                                    Some(primary)
                                                } else {
                                                    None
                                                };

                                            let (
                                                open_time,
                                                open_price,
                                                high_price,
                                                low_price,
                                                close_price,
                                            ) = match selected_record {
                                                Some(record) => {
                                                    rest_summary.update(RestResult::success(
                                                        RestRequestType::PremiumIndex,
                                                        format!("ts={}", record.0),
                                                    ));
                                                    record
                                                }
                                                None => {
                                                    rest_summary.update(RestResult::failure(
                                                        RestRequestType::PremiumIndex,
                                                        "匹配失败",
                                                    ));
                                                    if let Some((second_time, _, _, _, _)) =
                                                        secondary
                                                    {
                                                        warn!(
                                                            "[Premium Index Kline] Timestamp mismatch for {}: kline_ts={}, premium_index_ts0={}, premium_index_ts1={}, using latest record",
                                                            symbol_owned,
                                                            kline_open_tp,
                                                            primary_open_time,
                                                            second_time
                                                        );
                                                    } else {
                                                        warn!(
                                                            "[Premium Index Kline] Timestamp mismatch for {}: kline_ts={}, premium_index_ts0={}, using latest record",
                                                            symbol_owned, kline_open_tp, primary_open_time
                                                        );
                                                    }
                                                    primary
                                                }
                                            };
                                            let pkline_matches_kline = open_time == kline_open_tp;

                                            let mut msg = PremiumIndexKlineMsg::create(
                                                symbol_owned.clone(),
                                                open_price,
                                                high_price,
                                                low_price,
                                                close_price,
                                                open_time,
                                            );

                                            let open_interest_resp = client_clone
                                                .get(open_interest_url.as_str())
                                                .query(&[("symbol", symbol_owned.as_str())])
                                                .timeout(Duration::from_secs(3))
                                                .send()
                                                .await;

                                            let open_interest_resp = match open_interest_resp {
                                                Ok(resp) => resp,
                                                Err(err) => {
                                                    error!(
                                                        "Open Interest request error for {}: {}",
                                                        symbol_owned, err
                                                    );
                                                    rest_summary.update(RestResult::failure(
                                                        RestRequestType::OpenInterest,
                                                        format!("请求错误: {}", err),
                                                    ));
                                                    report_rest_summary(
                                                        &sender_clone,
                                                        symbol_owned.as_str(),
                                                        kline_close_tp,
                                                        &rest_summary,
                                                        RestSummaryStage::OneMinute,
                                                    );
                                                    return;
                                                }
                                            };

                                            let oi_status = open_interest_resp.status();
                                            let oi_body =
                                                open_interest_resp.text().await.unwrap_or_else(
                                                    |_| "Unable to read response body".to_string(),
                                                );

                                            if !oi_status.is_success() {
                                                if oi_status != StatusCode::SERVICE_UNAVAILABLE
                                                    && oi_status != StatusCode::REQUEST_TIMEOUT
                                                {
                                                    error!(
                                                        "Open Interest HTTP {} for {}: {}",
                                                        oi_status, symbol_owned, oi_body
                                                    );
                                                }
                                                rest_summary.update(RestResult::failure(
                                                    RestRequestType::OpenInterest,
                                                    format!("HTTP {}", oi_status),
                                                ));
                                                report_rest_summary(
                                                    &sender_clone,
                                                    symbol_owned.as_str(),
                                                    kline_close_tp,
                                                    &rest_summary,
                                                    RestSummaryStage::OneMinute,
                                                );
                                                return;
                                            }

                                            let json: serde_json::Value =
                                                match serde_json::from_str(&oi_body) {
                                                    Ok(value) => value,
                                                    Err(err) => {
                                                        error!(
                                                        "Open Interest JSON parse error for {}: {}",
                                                        symbol_owned, err
                                                    );
                                                        rest_summary.update(RestResult::failure(
                                                            RestRequestType::OpenInterest,
                                                            format!("JSON错误: {}", err),
                                                        ));
                                                        report_rest_summary(
                                                            &sender_clone,
                                                            symbol_owned.as_str(),
                                                            kline_close_tp,
                                                            &rest_summary,
                                                            RestSummaryStage::OneMinute,
                                                        );
                                                        return;
                                                    }
                                                };

                                            if let (Some(oi_str), Some(time)) = (
                                                json.get("openInterest").and_then(|v| v.as_str()),
                                                json.get("time").and_then(|v| v.as_i64()),
                                            ) {
                                                match oi_str.parse::<f64>() {
                                                    Ok(oi) => {
                                                        msg.set_open_interest(oi, time);
                                                        rest_summary.update(RestResult::success(
                                                            RestRequestType::OpenInterest,
                                                            format!("ts={}", time),
                                                        ));
                                                        if !pkline_matches_kline {
                                                            let border = "+----------------------+------------------------------------------------------------+";
                                                            let symbol_row = format!(
                                                                "| {:<20} | {:<60} |",
                                                                "symbol",
                                                                symbol_owned.as_str()
                                                            );
                                                            let header_row = format!(
                                                                "| {:<20} | {:<60} |",
                                                                "请求", "时间tp(ms)"
                                                            );
                                                            let kline_row = format!(
                                                                "| {:<20} | {:<60} |",
                                                                "kline",
                                                                format!(
                                                                    "open={}, close={}",
                                                                    kline_open_tp, kline_close_tp
                                                                )
                                                            );
                                                            let pkline_row = format!(
                                                                "| {:<20} | {:<60} |",
                                                                "pkline",
                                                                open_time.to_string()
                                                            );
                                                            let open_interest_row = format!(
                                                                "| {:<20} | {:<60} |",
                                                                "openinterst",
                                                                time.to_string()
                                                            );
                                                            let table = format!(
                                                                "\n{border}\n{symbol_row}\n{border}\n{header_row}\n{kline_row}\n{pkline_row}\n{open_interest_row}\n{border}",
                                                                border = border,
                                                                symbol_row = symbol_row,
                                                                header_row = header_row,
                                                                kline_row = kline_row,
                                                                pkline_row = pkline_row,
                                                                open_interest_row =
                                                                    open_interest_row,
                                                            );
                                                            info!("{}", table);
                                                        }
                                                    }
                                                    Err(err) => {
                                                        rest_summary.update(RestResult::failure(
                                                            RestRequestType::OpenInterest,
                                                            format!("解析失败: {}", err),
                                                        ));
                                                        error!(
                                                            "Open Interest parse error for {}: {} ({})",
                                                            symbol_owned, oi_str, err
                                                        );
                                                    }
                                                }
                                            } else {
                                                rest_summary.update(RestResult::failure(
                                                    RestRequestType::OpenInterest,
                                                    "缺少字段 openInterest/time",
                                                ));
                                                error!(
                                                    "Open Interest missing fields for {}",
                                                    symbol_owned
                                                );
                                            }

                                            if let Err(err) = sender_clone.send(msg.to_bytes()) {
                                                error!(
                                                    "Failed to broadcast premium index kline for {}: {}",
                                                    symbol_owned, err
                                                );
                                            }
                                            //修正
                                            kline_close_tp += 1;
                                            report_rest_summary(
                                                &sender_clone,
                                                symbol_owned.as_str(),
                                                kline_close_tp,
                                                &rest_summary,
                                                RestSummaryStage::OneMinute,
                                            );
                                            if is_five_minute_boundary(kline_close_tp) {
                                                sleep(Duration::from_secs(180)).await;
                                                let ratio_symbol = symbol_owned.clone();
                                                let ratio_client = client_clone.clone();
                                                let ratio_sender = sender_clone.clone();
                                                let (
                                                    account_res,
                                                    position_res,
                                                    global_res,
                                                    oi_hist_res,
                                                ) = tokio::join!(
                                                    fetch_ratio_metrics(
                                                        ratio_client.clone(),
                                                        top_account_ratio_url.clone(),
                                                        ratio_symbol.clone(),
                                                        "top-account",
                                                        "longAccount",
                                                        "shortAccount",
                                                        kline_close_tp
                                                    ),
                                                    fetch_ratio_metrics(
                                                        ratio_client.clone(),
                                                        top_position_ratio_url.clone(),
                                                        ratio_symbol.clone(),
                                                        "top-position",
                                                        "longAccount",
                                                        "shortAccount",
                                                        kline_close_tp
                                                    ),
                                                    fetch_ratio_metrics(
                                                        ratio_client.clone(),
                                                        global_account_ratio_url.clone(),
                                                        ratio_symbol.clone(),
                                                        "global-account",
                                                        "longAccount",
                                                        "shortAccount",
                                                        kline_close_tp
                                                    ),
                                                    fetch_open_interest_hist(
                                                        ratio_client,
                                                        open_interest_hist_url.clone(),
                                                        ratio_symbol.clone(),
                                                        kline_close_tp
                                                    )
                                                );

                                                let mut account_data: Option<RatioMetrics> = None;
                                                match account_res {
                                                    Ok(data) => {
                                                        rest_summary.update(RestResult::success(
                                                            RestRequestType::TopAccount,
                                                            format!("ts={}", data.timestamp),
                                                        ));
                                                        account_data = Some(data);
                                                    }
                                                    Err(err) => {
                                                        rest_summary.update(RestResult::failure(
                                                            RestRequestType::TopAccount,
                                                            err.detail(),
                                                        ));
                                                    }
                                                }

                                                let mut position_data: Option<RatioMetrics> = None;
                                                match position_res {
                                                    Ok(data) => {
                                                        rest_summary.update(RestResult::success(
                                                            RestRequestType::TopPosition,
                                                            format!("ts={}", data.timestamp),
                                                        ));
                                                        position_data = Some(data);
                                                    }
                                                    Err(err) => {
                                                        rest_summary.update(RestResult::failure(
                                                            RestRequestType::TopPosition,
                                                            err.detail(),
                                                        ));
                                                    }
                                                }

                                                let mut global_data: Option<RatioMetrics> = None;
                                                match global_res {
                                                    Ok(data) => {
                                                        rest_summary.update(RestResult::success(
                                                            RestRequestType::GlobalAccount,
                                                            format!("ts={}", data.timestamp),
                                                        ));
                                                        global_data = Some(data);
                                                    }
                                                    Err(err) => {
                                                        rest_summary.update(RestResult::failure(
                                                            RestRequestType::GlobalAccount,
                                                            err.detail(),
                                                        ));
                                                    }
                                                }

                                                let mut oi_hist_data: Option<OpenInterestHist> =
                                                    None;
                                                match oi_hist_res {
                                                    Ok(data) => {
                                                        rest_summary.update(RestResult::success(
                                                            RestRequestType::OpenInterestHist,
                                                            format!("ts={}", data.timestamp),
                                                        ));
                                                        oi_hist_data = Some(data);
                                                    }
                                                    Err(err) => {
                                                        rest_summary.update(RestResult::failure(
                                                            RestRequestType::OpenInterestHist,
                                                            err.detail(),
                                                        ));
                                                    }
                                                }

                                                if let (
                                                    Some(account),
                                                    Some(position),
                                                    Some(global),
                                                ) = (
                                                    account_data.as_ref(),
                                                    position_data.as_ref(),
                                                    global_data.as_ref(),
                                                ) {
                                                    let mut ratio_msg =
                                                        TopLongShortRatioMsg::create(
                                                            ratio_symbol.clone(),
                                                            kline_close_tp,
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

                                                    if let Some(oi_hist) = oi_hist_data.as_ref() {
                                                        ratio_msg.set_open_interest_hist(
                                                            oi_hist.sum_open_interest,
                                                            oi_hist.sum_open_interest_value,
                                                            oi_hist.cmc_circulating_supply,
                                                            oi_hist.timestamp,
                                                        );
                                                    }

                                                    if let Err(err) =
                                                        ratio_sender.send(ratio_msg.to_bytes())
                                                    {
                                                        error!(
                                                            "Failed to broadcast top long/short ratio for {}: {}",
                                                            ratio_symbol, err
                                                        );
                                                    }
                                                    if ratio_symbol.to_lowercase() == "btcusdt" {
                                                        info!(
                                                            "[Binance Top LongShort] {}: account(long={}, short={}, ratio={}, ts={}), position(long={}, short={}, ratio={}, ts={}), global(long={}, short={}, ratio={}, ts={})",
                                                            ratio_symbol.to_lowercase(),
                                                            account.long_value,
                                                            account.short_value,
                                                            account.ratio_value,
                                                            account.timestamp,
                                                            position.long_value,
                                                            position.short_value,
                                                            position.ratio_value,
                                                            position.timestamp,
                                                            global.long_value,
                                                            global.short_value,
                                                            global.ratio_value,
                                                            global.timestamp
                                                        );
                                                    }
                                                }
                                                report_rest_summary(
                                                    &sender_clone,
                                                    symbol_owned.as_str(),
                                                    kline_close_tp,
                                                    &rest_summary,
                                                    RestSummaryStage::FiveMinute,
                                                );
                                            }
                                        });
                                    }
                                }
                                // 发送K线消息
                                if sender.send(kline_msg.to_bytes()).is_ok() {
                                    return 1;
                                }
                            }
                        }
                    }
                }
            }
        }
        0
    }
}

struct RatioMetrics {
    long_value: f64,
    short_value: f64,
    ratio_value: f64,
    timestamp: i64,
}

fn is_five_minute_boundary(close_time: i64) -> bool {
    close_time % FIVE_MINUTE_MILLIS == 0
}

async fn fetch_ratio_metrics(
    client: reqwest::Client,
    url: String,
    symbol: String,
    label: &'static str,
    long_key: &'static str,
    short_key: &'static str,
    close_time: i64,
) -> Result<RatioMetrics, FetchError> {
    let response = client
        .get(url.as_str())
        .query(&[
            ("symbol", symbol.as_str()),
            ("period", "5m"),
            ("limit", "2"),
        ])
        .timeout(Duration::from_secs(5))
        .send()
        .await;

    let response = match response {
        Ok(resp) => resp,
        Err(err) => {
            info!("{} request error for {}: {}", label, symbol, err);
            return Err(FetchError::Request(err.to_string()));
        }
    };

    let status = response.status();
    let body = response
        .text()
        .await
        .unwrap_or_else(|_| "Unable to read response body".to_string());

    if !status.is_success() {
        if status != StatusCode::SERVICE_UNAVAILABLE && status != StatusCode::REQUEST_TIMEOUT {
            info!("{} HTTP {} for {}: {}", label, status, symbol, body);
        } else {
            info!("{} temporary HTTP {} for {}", label, status, symbol);
        }
        return Err(FetchError::Http(status));
    }

    let entries: Vec<serde_json::Value> = match serde_json::from_str(&body) {
        Ok(value) => value,
        Err(err) => {
            info!("{} JSON parse error for {}: {}", label, symbol, err);
            return Err(FetchError::Json(err.to_string()));
        }
    };

    if entries.is_empty() {
        info!("{} response empty for {}", label, symbol);
        return Err(FetchError::EmptyResponse);
    }

    let to_i64 = |value: &serde_json::Value| -> Option<i64> {
        value
            .as_i64()
            .or_else(|| value.as_str()?.parse::<i64>().ok())
    };

    let entry = match entries.iter().find(|entry| {
        let ts = entry.get("timestamp").and_then(|v| to_i64(v));
        ts == Some(close_time) || ts == Some(close_time + 1)
    }) {
        Some(entry) => entry,
        None => {
            info!(
                "{} missing record for {} at close_time {}",
                label, symbol, close_time
            );
            return Err(FetchError::MatchFailure);
        }
    };

    let parse_value = |key: &str| -> Option<f64> {
        entry
            .get(key)
            .and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse::<f64>().ok()))
    };

    let long_value = match parse_value(long_key) {
        Some(value) => value,
        None => {
            info!("{} missing {} for {}", label, long_key, symbol);
            return Err(FetchError::MissingField(long_key));
        }
    };

    let short_value = match parse_value(short_key) {
        Some(value) => value,
        None => {
            info!("{} missing {} for {}", label, short_key, symbol);
            return Err(FetchError::MissingField(short_key));
        }
    };

    let ratio_value = match parse_value("longShortRatio") {
        Some(value) => value,
        None => {
            info!("{} missing longShortRatio for {}", label, symbol);
            return Err(FetchError::MissingField("longShortRatio"));
        }
    };

    let timestamp = entry
        .get("timestamp")
        .and_then(|v| to_i64(v))
        .unwrap_or(close_time);

    Ok(RatioMetrics {
        long_value,
        short_value,
        ratio_value,
        timestamp,
    })
}

struct OpenInterestHist {
    sum_open_interest: f64,
    sum_open_interest_value: f64,
    cmc_circulating_supply: f64,
    timestamp: i64,
}

async fn fetch_open_interest_hist(
    client: reqwest::Client,
    url: String,
    symbol: String,
    close_time: i64,
) -> Result<OpenInterestHist, FetchError> {
    let response = client
        .get(url.as_str())
        .query(&[
            ("symbol", symbol.as_str()),
            ("period", "5m"),
            ("limit", "2"),
        ])
        .timeout(Duration::from_secs(5))
        .send()
        .await;

    let response = match response {
        Ok(resp) => resp,
        Err(err) => {
            info!("open-interest-hist request error for {}: {}", symbol, err);
            return Err(FetchError::Request(err.to_string()));
        }
    };

    let status = response.status();
    let body = response
        .text()
        .await
        .unwrap_or_else(|_| "Unable to read response body".to_string());

    if !status.is_success() {
        if status != StatusCode::SERVICE_UNAVAILABLE && status != StatusCode::REQUEST_TIMEOUT {
            info!(
                "open-interest-hist HTTP {} for {}: {}",
                status, symbol, body
            );
        } else {
            info!(
                "open-interest-hist temporary HTTP {} for {}",
                status, symbol
            );
        }
        return Err(FetchError::Http(status));
    }

    let entries: Vec<serde_json::Value> = match serde_json::from_str(&body) {
        Ok(value) => value,
        Err(err) => {
            info!(
                "open-interest-hist JSON parse error for {}: {}",
                symbol, err
            );
            return Err(FetchError::Json(err.to_string()));
        }
    };

    if entries.is_empty() {
        info!("open-interest-hist response empty for {}", symbol);
        return Err(FetchError::EmptyResponse);
    }

    let to_i64 = |value: &serde_json::Value| -> Option<i64> {
        value
            .as_i64()
            .or_else(|| value.as_str()?.parse::<i64>().ok())
    };

    let entry = match entries.iter().find(|entry| {
        let ts = entry.get("timestamp").and_then(|v| to_i64(v));
        ts == Some(close_time) || ts == Some(close_time + 1)
    }) {
        Some(entry) => entry,
        None => {
            info!(
                "open-interest-hist missing record for {} at close_time {}",
                symbol, close_time
            );
            return Err(FetchError::MatchFailure);
        }
    };

    let parse_f64 = |key: &str| -> Option<f64> {
        entry
            .get(key)
            .and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse::<f64>().ok()))
    };

    let sum_open_interest = match parse_f64("sumOpenInterest") {
        Some(value) => value,
        None => {
            info!("open-interest-hist missing sumOpenInterest for {}", symbol);
            return Err(FetchError::MissingField("sumOpenInterest"));
        }
    };

    let sum_open_interest_value = match parse_f64("sumOpenInterestValue") {
        Some(value) => value,
        None => {
            info!(
                "open-interest-hist missing sumOpenInterestValue for {}",
                symbol
            );
            return Err(FetchError::MissingField("sumOpenInterestValue"));
        }
    };

    let cmc_circulating_supply = parse_f64("CMCCirculatingSupply").unwrap_or(0.0);

    let timestamp = entry
        .get("timestamp")
        .and_then(|v| to_i64(v))
        .unwrap_or(close_time);

    Ok(OpenInterestHist {
        sum_open_interest,
        sum_open_interest_value,
        cmc_circulating_supply,
        timestamp,
    })
}

pub struct BinanceDerivativesMetricsParser {
    symbols: HashSet<String>,
}

impl BinanceDerivativesMetricsParser {
    pub fn new(symbols_set: HashSet<String>) -> Self {
        Self {
            symbols: symbols_set,
        }
    }
}

impl Parser for BinanceDerivativesMetricsParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse Binance derivatives metrics messages (liquidations + mark price)
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Handle mark price array format: [{e: "markPriceUpdate", ...}, ...]
                if let Some(data_array) = json_value.as_array() {
                    return self.parse_mark_price_array(data_array, sender);
                }

                // Handle single liquidation event format: {e: "forceOrder", ...}
                if let Some(event_type) = json_value.get("e").and_then(|v| v.as_str()) {
                    match event_type {
                        "forceOrder" => return self.parse_liquidation_event(&json_value, sender),
                        "markPriceUpdate" => {
                            return self.parse_single_mark_price(&json_value, sender)
                        }
                        _ => return 0,
                    }
                }
            }
        }
        0
    }
}

impl BinanceDerivativesMetricsParser {
    fn parse_liquidation_event(
        &self,
        json_value: &serde_json::Value,
        sender: &broadcast::Sender<Bytes>,
    ) -> usize {
        // Parse liquidation order data
        if let Some(order_data) = json_value.get("o") {
            if let (
                Some(symbol),
                Some(side),
                Some(quantity_str),
                Some(avg_price_str),
                Some(timestamp),
            ) = (
                order_data.get("s").and_then(|v| v.as_str()),
                order_data.get("S").and_then(|v| v.as_str()),
                order_data.get("z").and_then(|v| v.as_str()), // Order Filled Accumulated Quantity
                order_data.get("ap").and_then(|v| v.as_str()), // Average Price
                order_data.get("T").and_then(|v| v.as_i64()), // Order Trade Time
            ) {
                // Check if symbol is in the allowed list (case-insensitive)
                let symbol_lower = symbol.to_lowercase();
                if !self.symbols.contains(&symbol_lower) {
                    return 0;
                }
                // Parse quantity and price
                if let (Ok(quantity), Ok(avg_price)) =
                    (quantity_str.parse::<f64>(), avg_price_str.parse::<f64>())
                {
                    // Convert Binance side to liquidation_side char
                    let liquidation_side = match side {
                        "BUY" => 'B',  // 买入强平
                        "SELL" => 'S', // 卖出强平
                        _ => return 0,
                    };

                    // Create liquidation message
                    let liquidation_msg = LiquidationMsg::create(
                        symbol.to_string(),
                        liquidation_side,
                        quantity,
                        avg_price,
                        timestamp,
                    );

                    // Send liquidation message
                    if sender.send(liquidation_msg.to_bytes()).is_ok() {
                        return 1;
                    }
                }
            }
        }
        0
    }

    fn parse_mark_price_array(
        &self,
        data_array: &Vec<serde_json::Value>,
        sender: &broadcast::Sender<Bytes>,
    ) -> usize {
        let mut total_parsed = 0;

        for item in data_array {
            total_parsed += self.parse_single_mark_price(item, sender);
        }
        total_parsed
    }

    fn parse_single_mark_price(
        &self,
        item: &serde_json::Value,
        sender: &broadcast::Sender<Bytes>,
    ) -> usize {
        // Check if this is a markPriceUpdate event
        if let Some(event_type) = item.get("e").and_then(|v| v.as_str()) {
            if event_type == "markPriceUpdate" {
                if let (
                    Some(symbol),
                    Some(mark_price_str),
                    Some(index_price_str),
                    Some(funding_rate_str),
                    Some(event_time),
                    Some(next_funding_time),
                ) = (
                    item.get("s").and_then(|v| v.as_str()),
                    item.get("p").and_then(|v| v.as_str()),
                    item.get("i").and_then(|v| v.as_str()),
                    item.get("r").and_then(|v| v.as_str()),
                    item.get("E").and_then(|v| v.as_i64()),
                    item.get("T").and_then(|v| v.as_i64()),
                ) {
                    // Check if symbol is in the allowed list (case-insensitive)
                    let symbol_lower = symbol.to_lowercase();
                    if !self.symbols.contains(&symbol_lower) {
                        return 0;
                    }
                    // Parse price values
                    if let (Ok(mark_price), Ok(index_price), Ok(funding_rate)) = (
                        mark_price_str.parse::<f64>(),
                        index_price_str.parse::<f64>(),
                        funding_rate_str.parse::<f64>(),
                    ) {
                        let mut parsed_count = 0;

                        // Create and send MarkPriceMsg
                        let mark_price_msg =
                            MarkPriceMsg::create(symbol.to_string(), mark_price, event_time);
                        if sender.send(mark_price_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }

                        // Create and send IndexPriceMsg
                        let index_price_msg =
                            IndexPriceMsg::create(symbol.to_string(), index_price, event_time);
                        if sender.send(index_price_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }

                        // Create and send FundingRateMsg
                        let funding_rate_msg = FundingRateMsg::create(
                            symbol.to_string(),
                            funding_rate,
                            next_funding_time,
                            event_time,
                        );
                        if sender.send(funding_rate_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }

                        return parsed_count;
                    }
                }
            }
        }
        0
    }
}

pub struct BinanceSnapshotParser;

impl BinanceSnapshotParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BinanceSnapshotParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        // 解析币安快照消息
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                return self.parse_snapshot_event(&json_value, sender);
            }
        }
        0
    }
}

// 公共函数：解析订单簿层级数据
fn parse_order_book_levels(
    bids_array: &Vec<serde_json::Value>,
    asks_array: &Vec<serde_json::Value>,
    inc_msg: &mut IncMsg,
) {
    // 解析bids
    for (i, bid_item) in bids_array.iter().enumerate() {
        if let Some(bid_array) = bid_item.as_array() {
            if bid_array.len() >= 2 {
                if let (Some(price_str), Some(amount_str)) =
                    (bid_array[0].as_str(), bid_array[1].as_str())
                {
                    let level = Level::new(price_str, amount_str);
                    inc_msg.set_bid_level(i, level);
                }
            }
        }
    }

    // 解析asks
    for (i, ask_item) in asks_array.iter().enumerate() {
        if let Some(ask_array) = ask_item.as_array() {
            if ask_array.len() >= 2 {
                if let (Some(price_str), Some(amount_str)) =
                    (ask_array[0].as_str(), ask_array[1].as_str())
                {
                    let level = Level::new(price_str, amount_str);
                    inc_msg.set_ask_level(i, level);
                }
            }
        }
    }
}

impl BinanceSnapshotParser {
    fn parse_snapshot_event(
        &self,
        json_value: &serde_json::Value,
        sender: &broadcast::Sender<Bytes>,
    ) -> usize {
        // 从快照数据中提取信息
        if let (Some(symbol), Some(last_update_id), Some(bids_array), Some(asks_array)) = (
            json_value.get("s").and_then(|v| v.as_str()),
            json_value.get("lastUpdateId").and_then(|v| v.as_i64()),
            json_value.get("bids").and_then(|v| v.as_array()),
            json_value.get("asks").and_then(|v| v.as_array()),
        ) {
            let bids_count = bids_array.len() as u32;
            let asks_count = asks_array.len() as u32;

            // 创建快照消息，对于快照消息，first_update_id = last_update_id + 1
            let mut inc_msg = IncMsg::create(
                symbol.to_string(),
                last_update_id + 1, // first_update_id
                last_update_id + 1, // final_update_id（对于快照相同）
                0,                  // timestamp（快照没有实际时间戳）
                true,               // is_snapshot = true
                bids_count,
                asks_count,
            );

            // 使用公共函数解析订单簿层级
            parse_order_book_levels(bids_array, asks_array, &mut inc_msg);

            // 发送快照消息
            if sender.send(inc_msg.to_bytes()).is_ok() {
                return 1;
            }
        }
        0
    }
}

pub struct BinanceIncParser {
    is_futures: bool,
}

impl BinanceIncParser {
    pub fn new(is_futures: bool) -> Self {
        Self { is_futures }
    }
}

impl Parser for BinanceIncParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        // 解析币安增量消息
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 检查是否是增量更新事件
                if let Some(event_type) = json_value.get("e").and_then(|v| v.as_str()) {
                    if event_type == "depthUpdate" {
                        return self.parse_inc_event(&json_value, sender);
                    }
                }
            }
        }
        0
    }
}

impl BinanceIncParser {
    fn parse_inc_event(
        &self,
        json_value: &serde_json::Value,
        sender: &broadcast::Sender<Bytes>,
    ) -> usize {
        // 币安现货用E字段，币安合约用T字段
        // 判断json是否包含T字段，如果有，timestamp用T，否则用E
        let timestamp_field = if json_value.get("T").is_some() {
            "T"
        } else {
            "E"
        };
        // 从增量数据中提取信息
        if let (
            Some(symbol),
            Some(first_update_id),
            Some(final_update_id),
            Some(timestamp),
            Some(bids_array),
            Some(asks_array),
        ) = (
            json_value.get("s").and_then(|v| v.as_str()),
            json_value.get("U").and_then(|v| v.as_i64()), // first update id
            json_value.get("u").and_then(|v| v.as_i64()), // final update id
            json_value.get(timestamp_field).and_then(|v| v.as_i64()), // timestamp
            json_value.get("b").and_then(|v| v.as_array()), // bids
            json_value.get("a").and_then(|v| v.as_array()), // asks
        ) {
            let mut parsed_count = 0;
            let symbol_string = symbol.to_string();

            let prev_update_id = if self.is_futures {
                match json_value.get("pu").and_then(|v| v.as_i64()) {
                    Some(value) => value,
                    None => {
                        error!(
                            "Missing 'pu' field in futures depthUpdate for symbol {}",
                            symbol
                        );
                        return 0;
                    }
                }
            } else {
                0
            };

            let seq_msg = BinanceIncSeqNoMsg::create(
                symbol_string.clone(),
                prev_update_id,
                final_update_id,
                first_update_id,
                timestamp,
            );
            if sender.send(seq_msg.to_bytes()).is_ok() {
                parsed_count += 1;
            }

            let bids_count = bids_array.len() as u32;
            let asks_count = asks_array.len() as u32;

            // 创建增量消息
            let mut inc_msg = IncMsg::create(
                symbol_string,
                first_update_id,
                final_update_id,
                timestamp,
                false, // is_snapshot = false
                bids_count,
                asks_count,
            );

            // 使用公共函数解析订单簿层级
            parse_order_book_levels(bids_array, asks_array, &mut inc_msg);

            // 发送增量消息
            if sender.send(inc_msg.to_bytes()).is_ok() {
                parsed_count += 1;
            }

            return parsed_count;
        }
        0
    }
}

pub struct BinanceTradeParser;

impl BinanceTradeParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BinanceTradeParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse Binance trade message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Check if this is a trade event
                if let Some(event_type) = json_value.get("e").and_then(|v| v.as_str()) {
                    if event_type == "trade" {
                        return self.parse_trade_event(&json_value, sender);
                    }
                }
            }
        }
        0
    }
}

impl BinanceTradeParser {
    fn parse_trade_event(
        &self,
        json_value: &serde_json::Value,
        sender: &broadcast::Sender<Bytes>,
    ) -> usize {
        // Extract trade data from Binance trade message
        if let (
            Some(symbol),
            Some(trade_id),
            Some(price_str),
            Some(qty_str),
            Some(trade_time),
            Some(is_maker),
        ) = (
            json_value.get("s").and_then(|v| v.as_str()),  // 交易对
            json_value.get("t").and_then(|v| v.as_i64()),  // 交易ID
            json_value.get("p").and_then(|v| v.as_str()),  // 成交价格
            json_value.get("q").and_then(|v| v.as_str()),  // 成交数量
            json_value.get("T").and_then(|v| v.as_i64()),  // 事件时间
            json_value.get("m").and_then(|v| v.as_bool()), // 买方是否是做市方
        ) {
            // Parse price and quantity
            if let (Ok(price), Ok(amount)) = (price_str.parse::<f64>(), qty_str.parse::<f64>()) {
                // Filter out zero values - 币安有时候price和amount会是0，过滤掉不发送
                if price <= 0.0 || amount <= 0.0 {
                    return 0;
                }

                // Determine side: 买方是否是做市方，'S'表示卖出，'B'表示买入
                // 如果买方是做市方(true)，那么这是一个主动卖出单，标记为'S'
                // 如果买方不是做市方(false)，那么这是一个主动买入单，标记为'B'
                let side = if is_maker { 'S' } else { 'B' };

                // Create trade message
                let trade_msg = TradeMsg::create(
                    symbol.to_string(),
                    trade_id,
                    trade_time,
                    side,
                    price,
                    amount,
                );

                // Send trade message
                if sender.send(trade_msg.to_bytes()).is_ok() {
                    return 1;
                }
            }
        }
        0
    }
}

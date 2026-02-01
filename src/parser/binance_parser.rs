use crate::mkt_msg::{
    BinanceIncSeqNoMsg, FundingRateMsg, IncMsg, IndexPriceMsg, KlineMsg, Level, LiquidationMsg,
    MarkPriceMsg, SignalMsg, SignalSource, TradeMsg,
};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use log::{error, info};
use std::collections::HashSet;
use tokio::sync::broadcast;

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

pub struct BinanceKlineParser;

impl BinanceKlineParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BinanceKlineParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        // Parse Binance kline message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 从顶层s字段直接获取symbol
                if let Some(symbol) = json_value.get("s").and_then(|v| v.as_str()) {
                    let event_time = json_value.get("E").and_then(|v| v.as_i64()).unwrap_or(0);
                    // 获取k对象中的K线数据
                    if let Some(kline_obj) = json_value.get("k") {
                        // 获取x字段判断是否已关闭（用于日志）
                        let is_closed = kline_obj
                            .get("x")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);

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
                            let _close_time = kline_obj
                                .get("T")
                                .and_then(|v| v.as_i64())
                                .unwrap_or(timestamp + 1);
                            // 只为BTCUSDT的closed K线打印OHLCV数据
                            if is_closed && symbol.to_lowercase() == "btcusdt" {
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
                                let event_time = if event_time == 0 {
                                    timestamp
                                } else {
                                    event_time
                                };

                                let mut kline_msg = KlineMsg::create(
                                    symbol.to_string(),
                                    open,
                                    high,
                                    low,
                                    close,
                                    volume,
                                    turnover,
                                    timestamp,
                                    event_time,
                                );

                                // 设置币安专属字段
                                kline_msg.set_binance_fields(
                                    trade_num,
                                    taker_buy_vol,
                                    taker_buy_quote_vol,
                                );

                                // 发送K线消息（无论是否 closed）
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
                Some(price_str),
                Some(timestamp),
            ) = (
                order_data.get("s").and_then(|v| v.as_str()),
                order_data.get("S").and_then(|v| v.as_str()),
                order_data.get("z").and_then(|v| v.as_str()), // Order Filled Accumulated Quantity
                order_data.get("p").and_then(|v| v.as_str()), // Price
                order_data.get("T").and_then(|v| v.as_i64()), // Order Trade Time
            ) {
                // Check if symbol is in the allowed list (case-insensitive)
                let symbol_lower = symbol.to_lowercase();
                if !self.symbols.contains(&symbol_lower) {
                    return 0;
                }
                // Parse quantity and price
                if let (Ok(quantity), Ok(price)) =
                    (quantity_str.parse::<f64>(), price_str.parse::<f64>())
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
                        price,
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

pub struct BinanceSbeIncParser;

impl BinanceSbeIncParser {
    pub fn new() -> Self {
        Self
    }

    fn parse_depth_diff(&self, msg: &[u8], sender: &broadcast::Sender<Bytes>) -> usize {
        let header = match read_sbe_header(msg) {
            Some(h) => h,
            None => return 0,
        };
        if header.template_id != 10003 {
            return 0;
        }

        let base = header.body_offset;
        if msg.len() < base + header.block_length {
            return 0;
        }

        // Use eventTime (field id=1) for depth diff timestamp.
        let event_time = match read_i64_le(msg, base) {
            Some(v) => v,
            None => return 0,
        };
        let first_update_id = match read_i64_le(msg, base + 8) {
            Some(v) => v,
            None => return 0,
        };
        let last_update_id = match read_i64_le(msg, base + 16) {
            Some(v) => v,
            None => return 0,
        };
        let price_exponent = match read_i8(msg, base + 24) {
            Some(v) => v,
            None => return 0,
        };
        let qty_exponent = match read_i8(msg, base + 25) {
            Some(v) => v,
            None => return 0,
        };

        let mut offset = base + header.block_length;
        let (bids, next_offset) =
            match read_group_levels(msg, offset, price_exponent, qty_exponent) {
                Some(v) => v,
                None => return 0,
            };
        offset = next_offset;
        let (asks, next_offset) =
            match read_group_levels(msg, offset, price_exponent, qty_exponent) {
                Some(v) => v,
                None => return 0,
            };
        offset = next_offset;

        let symbol = match read_var_string8(msg, offset) {
            Some((s, _)) => s.to_uppercase(),
            None => return 0,
        };

        let timestamp = event_time / 1000;
        let mut parsed_count = 0;

        let seq_msg = BinanceIncSeqNoMsg::create(
            symbol.clone(),
            0,
            last_update_id,
            first_update_id,
            timestamp,
        );
        if sender.send(seq_msg.to_bytes()).is_ok() {
            parsed_count += 1;
        }

        let bids_count = bids.len() as u32;
        let asks_count = asks.len() as u32;
        let mut inc_msg = IncMsg::create(
            symbol,
            first_update_id,
            last_update_id,
            timestamp,
            false,
            bids_count,
            asks_count,
        );
        parse_order_book_levels_from_pairs(&bids, &asks, &mut inc_msg);
        if sender.send(inc_msg.to_bytes()).is_ok() {
            parsed_count += 1;
        }

        parsed_count
    }
}

impl Parser for BinanceSbeIncParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        if msg.is_empty() || msg[0] == b'{' || msg[0] == b'[' {
            return 0;
        }
        self.parse_depth_diff(&msg, sender)
    }
}

pub struct BinanceSbeTradeParser;

impl BinanceSbeTradeParser {
    pub fn new() -> Self {
        Self
    }

    fn parse_trades(&self, msg: &[u8], sender: &broadcast::Sender<Bytes>) -> usize {
        let header = match read_sbe_header(msg) {
            Some(h) => h,
            None => return 0,
        };
        if header.template_id != 10000 {
            return 0;
        }

        let base = header.body_offset;
        if msg.len() < base + header.block_length {
            return 0;
        }

        let event_time = match read_i64_le(msg, base) {
            Some(v) => v,
            None => return 0,
        };
        let transact_time = match read_i64_le(msg, base + 8) {
            Some(v) => v,
            None => return 0,
        };
        let price_exponent = match read_i8(msg, base + 16) {
            Some(v) => v,
            None => return 0,
        };
        let qty_exponent = match read_i8(msg, base + 17) {
            Some(v) => v,
            None => return 0,
        };

        let mut offset = base + header.block_length;
        if msg.len() < offset + 6 {
            return 0;
        }
        let block_length = match read_u16_le(msg, offset) {
            Some(v) => v as usize,
            None => return 0,
        };
        let num_in_group = match read_u32_le(msg, offset + 2) {
            Some(v) => v as usize,
            None => return 0,
        };
        offset += 6;

        let mut trades = Vec::with_capacity(num_in_group);
        for _ in 0..num_in_group {
            if msg.len() < offset + block_length || block_length < 25 {
                break;
            }
            let trade_id = match read_i64_le(msg, offset) {
                Some(v) => v,
                None => break,
            };
            let price = match read_i64_le(msg, offset + 8) {
                Some(v) => v,
                None => break,
            };
            let qty = match read_i64_le(msg, offset + 16) {
                Some(v) => v,
                None => break,
            };
            let is_buyer_maker = msg.get(offset + 24).copied().unwrap_or(0) != 0;
            trades.push((trade_id, price, qty, is_buyer_maker));
            offset += block_length;
        }

        let symbol = match read_var_string8(msg, offset) {
            Some((s, _)) => s.to_uppercase(),
            None => return 0,
        };

        let _event_time = event_time; // keep for potential diagnostics
        let timestamp = transact_time / 1000;
        let mut parsed_count = 0;

        for (trade_id, price, qty, is_buyer_maker) in trades {
            let price = scale_mantissa(price, price_exponent);
            let amount = scale_mantissa(qty, qty_exponent);
            if price <= 0.0 || amount <= 0.0 {
                continue;
            }
            let side = if is_buyer_maker { 'S' } else { 'B' };
            let trade_msg = TradeMsg::create(
                symbol.clone(),
                trade_id,
                timestamp,
                side,
                price,
                amount,
            );
            if sender.send(trade_msg.to_bytes()).is_ok() {
                parsed_count += 1;
            }
        }

        parsed_count
    }
}

impl Parser for BinanceSbeTradeParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        if msg.is_empty() || msg[0] == b'{' || msg[0] == b'[' {
            return 0;
        }
        self.parse_trades(&msg, sender)
    }
}

#[derive(Debug, Clone, Copy)]
struct SbeHeader {
    block_length: usize,
    template_id: u16,
    body_offset: usize,
}

fn read_sbe_header(msg: &[u8]) -> Option<SbeHeader> {
    if msg.len() < 8 {
        return None;
    }
    let block_length = read_u16_le(msg, 0)? as usize;
    let template_id = read_u16_le(msg, 2)?;
    Some(SbeHeader {
        block_length,
        template_id,
        body_offset: 8,
    })
}

fn read_u16_le(msg: &[u8], offset: usize) -> Option<u16> {
    if msg.len() < offset + 2 {
        return None;
    }
    Some(u16::from_le_bytes([msg[offset], msg[offset + 1]]))
}

fn read_u32_le(msg: &[u8], offset: usize) -> Option<u32> {
    if msg.len() < offset + 4 {
        return None;
    }
    Some(u32::from_le_bytes([
        msg[offset],
        msg[offset + 1],
        msg[offset + 2],
        msg[offset + 3],
    ]))
}

fn read_i64_le(msg: &[u8], offset: usize) -> Option<i64> {
    if msg.len() < offset + 8 {
        return None;
    }
    Some(i64::from_le_bytes([
        msg[offset],
        msg[offset + 1],
        msg[offset + 2],
        msg[offset + 3],
        msg[offset + 4],
        msg[offset + 5],
        msg[offset + 6],
        msg[offset + 7],
    ]))
}

fn read_i8(msg: &[u8], offset: usize) -> Option<i8> {
    msg.get(offset).map(|v| *v as i8)
}

fn scale_mantissa(mantissa: i64, exponent: i8) -> f64 {
    let factor = 10_f64.powi(exponent as i32);
    (mantissa as f64) * factor
}

fn read_group_levels(
    msg: &[u8],
    offset: usize,
    price_exponent: i8,
    qty_exponent: i8,
) -> Option<(Vec<(f64, f64)>, usize)> {
    if msg.len() < offset + 4 {
        return None;
    }
    let block_length = read_u16_le(msg, offset)? as usize;
    let num_in_group = read_u16_le(msg, offset + 2)? as usize;
    let mut pos = offset + 4;
    let mut levels = Vec::with_capacity(num_in_group);

    for _ in 0..num_in_group {
        if msg.len() < pos + block_length || block_length < 16 {
            break;
        }
        let price = read_i64_le(msg, pos)?;
        let qty = read_i64_le(msg, pos + 8)?;
        levels.push((scale_mantissa(price, price_exponent), scale_mantissa(qty, qty_exponent)));
        pos += block_length;
    }

    Some((levels, pos))
}

fn read_var_string8(msg: &[u8], offset: usize) -> Option<(String, usize)> {
    let len = msg.get(offset).copied()? as usize;
    let start = offset + 1;
    if msg.len() < start + len {
        return None;
    }
    let data = &msg[start..start + len];
    let s = std::str::from_utf8(data).ok()?.to_string();
    Some((s, start + len))
}

fn parse_order_book_levels_from_pairs(
    bids: &[(f64, f64)],
    asks: &[(f64, f64)],
    inc_msg: &mut IncMsg,
) {
    for (i, (price, amount)) in bids.iter().enumerate() {
        inc_msg.set_bid_level(i, Level::from_values(*price, *amount));
    }
    for (i, (price, amount)) in asks.iter().enumerate() {
        inc_msg.set_ask_level(i, Level::from_values(*price, *amount));
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

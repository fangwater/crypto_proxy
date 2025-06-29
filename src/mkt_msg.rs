use bytes::{Bytes, BufMut, BytesMut};

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum MktMsgType {
    TradeInfo = 1001,
    ParseredTradeInfo = 1002,
    OrderBookSnapshot = 1003,
    OrderBookInc = 1004,
    SymbolAdd = 1005,
    SymbolDel = 1006,
    TpReset = 2001,
    Error = 2002,
}

#[allow(dead_code)]
pub struct MktMsg {
    pub msg_type: MktMsgType,
    pub msg_length: u32,
    pub data: Bytes
}   

#[allow(dead_code)]
impl MktMsg {
    /// 从bytes创建消息
    pub fn create(msg_type: MktMsgType, data: Bytes) -> Self {
        Self {
            msg_type,
            msg_length : data.len() as u32,
            data,
        }
    }

    /// 创建SymbolDel消息
    pub fn symbol_del(symbol: impl AsRef<str>) -> Self {
        Self::create(MktMsgType::SymbolDel, Bytes::from(symbol.as_ref().to_string()))
    }

    /// 创建错误消息
    pub fn error(msg: impl AsRef<str>) -> Self {
        Self::create(MktMsgType::Error, Bytes::from(msg.as_ref().to_string()))
    }

    /// 创建TP Reset消息
    pub fn tp_reset() -> Self {
        Self::create(MktMsgType::TpReset, Bytes::new())
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8 + self.data.len());
        buf.put_u32_le(self.msg_type as u32);
        buf.put_u32_le(self.msg_length as u32);
        buf.put(self.data.clone());
        buf.freeze()
    }
}
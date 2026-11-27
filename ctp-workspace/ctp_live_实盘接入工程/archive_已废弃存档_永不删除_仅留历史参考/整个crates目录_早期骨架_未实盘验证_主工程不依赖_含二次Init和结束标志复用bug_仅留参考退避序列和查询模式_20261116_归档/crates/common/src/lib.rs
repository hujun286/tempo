//! 公共模型：行情快照、报警、报单请求、配置加载。
//! 英文术语首次出现均附中文解释。

use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use serde::{Deserialize, Serialize};

/// 合约代码。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InstrumentId(pub String);

impl InstrumentId {
    pub fn as_str(&self) -> &str { &self.0 }
    pub fn new(s: impl Into<String>) -> Self { Self(s.into()) }
}

/// 交易所。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Exchange { Sse, Szse, Shfe, Ine, Dce, Czce, Cffex, Gfex }

impl Exchange {
    pub fn as_ctp_str(self) -> &'static str {
        match self {
            Exchange::Sse => "SSE",
            Exchange::Szse => "SZSE",
            Exchange::Shfe => "SHFE",
            Exchange::Ine => "INE",
            Exchange::Dce => "DCE",
            Exchange::Czce => "CZCE",
            Exchange::Cffex => "CFFEX",
            Exchange::Gfex => "GFEX",
        }
    }
}

/// 行情快照（tick，逐笔行情）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tick {
    pub instrument: InstrumentId,
    pub update_time: String,      // HH:MM:SS
    pub update_millisec: u32,     // 毫秒
    pub last_price: f64,
    pub bid_price1: f64,          // 买一价
    pub ask_price1: f64,          // 卖一价
    pub volume: i64,
    pub open_interest: f64,
    #[serde(skip)]
    pub received_at: Option<Instant>,
}

impl Tick {
    /// 无报价过滤：集合竞价/涨跌停/未撮合时买卖价可能是极大值（接近 1e308），
    /// 统一归零，避免下游误算。
    pub fn sanitize(&mut self) {
        const HUGE: f64 = 1e30;
        if !self.bid_price1.is_finite() || self.bid_price1.abs() > HUGE { self.bid_price1 = 0.0; }
        if !self.ask_price1.is_finite() || self.ask_price1.abs() > HUGE { self.ask_price1 = 0.0; }
        if !self.last_price.is_finite() || self.last_price.abs() > HUGE { self.last_price = 0.0; }
    }

    /// 去重键：更新时间 + 毫秒。
    pub fn dedup_key(&self) -> (&str, u32) {
        (&self.update_time, self.update_millisec)
    }
}

/// 报警。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub level: AlertLevel,
    pub source: String,
    pub message: String,
    pub at: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum AlertLevel { Info, Warn, Error, Critical }

/// 买卖方向。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction { Buy, Sell }

/// 开平标志。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OffsetFlag { Open, Close, CloseToday, CloseYesterday }

/// 报单价格类型。本轮只允许限价。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderPriceType { Limit, Market }

/// 报单请求。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub instrument: InstrumentId,
    pub direction: Direction,
    pub offset: OffsetFlag,
    pub price: f64,
    pub volume: i32,
    pub price_type: OrderPriceType,
}

/// 撤单请求。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelRequest {
    pub instrument: InstrumentId,
    pub order_ref: String,
    pub front_id: i32,
    pub session_id: i32,
}

/// 硬保险配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardLimits {
    pub max_lots_per_order: i32,
    pub max_premium_per_lot: f64,
    pub max_orders_per_minute: usize,
    pub allow_market_order: bool,
}

/// 交易配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingConfig {
    pub broker_id: String,
    pub user_id: String,
    pub password: String,
    pub app_id: String,
    pub auth_code: String,
    pub primary_front_ip: String,
    pub td_port: u16,
    pub flow_dir: String,
    pub settlement_confirm_on_login: bool,
    pub hard_limits: HardLimits,
}

/// 行情配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketDataConfig {
    pub md_port: u16,
    pub td_port: u16,
    pub trade_reserved_ip: String,
    pub member_ips: Vec<String>,
    pub flow_dir: String,
    pub contracts_file: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Secrets {
    pub network: MarketDataConfig,
    pub trading_account: TradingAccountSecret,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradingAccountSecret {
    pub broker_id: String,
    pub user_id: String,
    pub password: String,
    pub app_id: String,
    pub auth_code: String,
}

/// 加载 `config/secrets.toml`。文件不存在直接报错，提示复制示例文件。
pub fn load_secrets(path: &Path) -> anyhow::Result<Secrets> {
    let text = std::fs::read_to_string(path).map_err(|e| {
        anyhow::anyhow!(
            "读取 {} 失败（{}）。请复制 config/secrets.example.toml 为 config/secrets.toml 并填值。",
            path.display(), e
        )
    })?;
    Ok(toml::from_str(&text)?)
}

/// 按 member（成员）分片后的合约表。
pub type ShardMap = HashMap<usize, Vec<InstrumentId>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_sanitize_kills_huge_prices() {
        let mut t = Tick {
            instrument: InstrumentId::new("pp2701"),
            update_time: "09:00:00".into(),
            update_millisec: 500,
            last_price: 1e308,
            bid_price1: 1e308,
            ask_price1: 1e308,
            volume: 0,
            open_interest: 0.0,
            received_at: None,
        };
        t.sanitize();
        assert_eq!(t.bid_price1, 0.0);
        assert_eq!(t.ask_price1, 0.0);
        assert_eq!(t.last_price, 0.0);
    }

    #[test]
    fn tick_sanitize_keeps_normal_prices() {
        let mut t = Tick {
            instrument: InstrumentId::new("pp2701"),
            update_time: "09:00:00".into(),
            update_millisec: 500,
            last_price: 1.0,
            bid_price1: 0.9,
            ask_price1: 1.1,
            volume: 10,
            open_interest: 100.0,
            received_at: None,
        };
        t.sanitize();
        assert_eq!(t.bid_price1, 0.9);
        assert_eq!(t.ask_price1, 1.1);
    }
}

//! 硬保险（hard limits）：报单和撤单前的硬编码兜底。
//!
//! 设计原则：
//!   1. 参数从 config/secrets.toml 的 [trading.hard_limits] 读，不硬编码。
//!   2. 报单和撤单合并计数，滑动 60 秒窗口。
//!   3. 风控引擎（P5）未接入前，这是唯一的报单闸门。
//!   4. 任何 check 失败，报单必须直接拒绝，不允许降级放行。

use std::collections::VecDeque;
use std::path::Path;
use std::time::{Duration, Instant};

use serde::Deserialize;

/// secrets.toml 里 [trading.hard_limits] 段的字段。
#[derive(Debug, Clone, Deserialize)]
pub struct HardLimitsCfg {
    pub max_lots_per_order: i32,
    pub max_premium_per_lot: f64,
    pub max_orders_per_minute: usize,
    pub allow_market_order: bool,
}

#[derive(Debug, PartialEq)]
pub enum Reject {
    /// 手数超限
    LotsTooLarge { requested: i32, max: i32 },
    /// 权利金单价超限，或价格非法
    PriceInvalid { requested: f64, max: f64 },
    /// 禁止市价单
    MarketOrderForbidden,
    /// 频率超限
    RateLimited { window_count: usize, max: usize },
}

impl std::error::Error for Reject {}

impl std::fmt::Display for Reject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LotsTooLarge { requested, max } => {
                write!(f, "手数超限: 请求 {} 手，上限 {} 手", requested, max)
            }
            Self::PriceInvalid { requested, max } => {
                write!(f, "价格非法或超限: 请求 {}，上限 {} 元/张", requested, max)
            }
            Self::MarketOrderForbidden => write!(f, "禁止市价单，只允许限价单"),
            Self::RateLimited { window_count, max } => write!(
                f,
                "频率超限: 60 秒内 {} 次，上限 {} 次（报单+撤单合并）",
                window_count, max
            ),
        }
    }
}

pub struct Guard {
    cfg: HardLimitsCfg,
    recent: VecDeque<Instant>,
}

impl Guard {
    pub fn new(cfg: HardLimitsCfg) -> Self {
        Self {
            cfg,
            recent: VecDeque::new(),
        }
    }

    /// 从 secrets.toml 读取 hard_limits 段。
    pub fn load_from_secrets(path: &Path) -> anyhow::Result<Self> {
        #[derive(Deserialize)]
        struct Root {
            trading: T,
        }
        #[derive(Deserialize)]
        struct T {
            hard_limits: HardLimitsCfg,
        }
        let text = std::fs::read_to_string(path)?;
        let root: Root = toml::from_str(&text)?;
        Ok(Self::new(root.trading.hard_limits))
    }

    pub fn config(&self) -> &HardLimitsCfg {
        &self.cfg
    }

    /// 报单前检查。通过后必须调用 record() 记账。
    pub fn check_insert(&mut self, volume: i32, price: f64, is_market: bool) -> Result<(), Reject> {
        self.check_insert_at(volume, price, is_market, Instant::now())
    }

    /// 撤单前检查。撤单也计入合并频率。
    pub fn check_cancel(&mut self) -> Result<(), Reject> {
        self.check_cancel_at(Instant::now())
    }

    pub fn check_insert_at(
        &mut self,
        volume: i32,
        price: f64,
        is_market: bool,
        now: Instant,
    ) -> Result<(), Reject> {
        if is_market && !self.cfg.allow_market_order {
            return Err(Reject::MarketOrderForbidden);
        }
        if volume > self.cfg.max_lots_per_order {
            return Err(Reject::LotsTooLarge {
                requested: volume,
                max: self.cfg.max_lots_per_order,
            });
        }
        if !price.is_finite() || price <= 0.0 || price > self.cfg.max_premium_per_lot {
            return Err(Reject::PriceInvalid {
                requested: price,
                max: self.cfg.max_premium_per_lot,
            });
        }
        self.check_rate_at(now)
    }

    pub fn check_cancel_at(&mut self, now: Instant) -> Result<(), Reject> {
        self.check_rate_at(now)
    }

    fn check_rate_at(&mut self, now: Instant) -> Result<(), Reject> {
        self.evict(now);
        if self.recent.len() >= self.cfg.max_orders_per_minute {
            return Err(Reject::RateLimited {
                window_count: self.recent.len(),
                max: self.cfg.max_orders_per_minute,
            });
        }
        Ok(())
    }

    /// 真正发送成功后记账。
    pub fn record(&mut self) {
        self.record_at(Instant::now())
    }
    pub fn record_at(&mut self, now: Instant) {
        self.evict(now);
        self.recent.push_back(now);
    }

    fn evict(&mut self, now: Instant) {
        let window = Duration::from_secs(60);
        while let Some(front) = self.recent.front() {
            if now.duration_since(*front) >= window {
                self.recent.pop_front();
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> HardLimitsCfg {
        HardLimitsCfg {
            max_lots_per_order: 1,
            max_premium_per_lot: 5.0,
            max_orders_per_minute: 3,
            allow_market_order: false,
        }
    }

    #[test]
    fn rejects_two_lots() {
        let mut g = Guard::new(cfg());
        assert!(matches!(
            g.check_insert(2, 1.0, false).unwrap_err(),
            Reject::LotsTooLarge {
                requested: 2,
                max: 1
            }
        ));
    }

    #[test]
    fn rejects_price_above_5() {
        let mut g = Guard::new(cfg());
        assert!(matches!(
            g.check_insert(1, 5.0001, false).unwrap_err(),
            Reject::PriceInvalid { .. }
        ));
    }

    #[test]
    fn accepts_price_exactly_5() {
        let mut g = Guard::new(cfg());
        assert!(g.check_insert(1, 5.0, false).is_ok());
    }

    #[test]
    fn rejects_market_order() {
        let mut g = Guard::new(cfg());
        assert_eq!(
            g.check_insert(1, 1.0, true).unwrap_err(),
            Reject::MarketOrderForbidden
        );
    }

    #[test]
    fn rejects_nan_negative() {
        let mut g = Guard::new(cfg());
        assert!(matches!(
            g.check_insert(1, f64::NAN, false).unwrap_err(),
            Reject::PriceInvalid { .. }
        ));
        assert!(matches!(
            g.check_insert(1, -1.0, false).unwrap_err(),
            Reject::PriceInvalid { .. }
        ));
        assert!(matches!(
            g.check_insert(1, 0.0, false).unwrap_err(),
            Reject::PriceInvalid { .. }
        ));
    }

    #[test]
    fn rate_3_per_min() {
        let mut g = Guard::new(cfg());
        let t0 = Instant::now();
        for i in 0..3 {
            let now = t0 + Duration::from_secs(i);
            assert!(g.check_insert_at(1, 1.0, false, now).is_ok());
            g.record_at(now);
        }
        assert!(matches!(
            g.check_insert_at(1, 1.0, false, t0 + Duration::from_secs(3))
                .unwrap_err(),
            Reject::RateLimited {
                window_count: 3,
                max: 3
            }
        ));
    }

    #[test]
    fn cancel_counts() {
        let mut g = Guard::new(cfg());
        let t0 = Instant::now();
        g.check_insert_at(1, 1.0, false, t0).unwrap();
        g.record_at(t0);
        g.check_cancel_at(t0 + Duration::from_secs(1)).unwrap();
        g.record_at(t0 + Duration::from_secs(1));
        g.check_cancel_at(t0 + Duration::from_secs(2)).unwrap();
        g.record_at(t0 + Duration::from_secs(2));
        assert!(matches!(
            g.check_cancel_at(t0 + Duration::from_secs(3)).unwrap_err(),
            Reject::RateLimited { .. }
        ));
    }

    #[test]
    fn window_slides_after_60s() {
        let mut g = Guard::new(cfg());
        let t0 = Instant::now();
        for i in 0..3 {
            let now = t0 + Duration::from_secs(i);
            g.check_insert_at(1, 1.0, false, now).unwrap();
            g.record_at(now);
        }
        assert!(
            g.check_insert_at(1, 1.0, false, t0 + Duration::from_secs(61))
                .is_ok()
        );
    }
}

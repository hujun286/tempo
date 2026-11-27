//! 硬保险（hard limits，硬编码兜底）。
//! 报单和撤单合并计数，滑动 60 秒窗口。
//! 这轮不依赖行情，只查手数、权利金单价、频率、限价单。

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use common_model::{HardLimits, OrderPriceType, OrderRequest};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LimitReject {
    LotsTooLarge { requested: i32, max: i32 },
    PremiumTooHigh { requested: f64, max: f64 },
    MarketOrderForbidden,
    RateLimited { window_count: usize, max: usize },
}

pub struct HardLimitGuard {
    cfg: HardLimits,
    /// 最近 60 秒内的报单 + 撤单时间点。
    recent: VecDeque<Instant>,
}

impl HardLimitGuard {
    pub fn new(cfg: HardLimits) -> Self {
        Self { cfg, recent: VecDeque::new() }
    }

    pub fn config(&self) -> &HardLimits { &self.cfg }

    /// 报单前检查。通过后必须调用 `record_sent` 记账。
    pub fn check_insert(&mut self, req: &OrderRequest) -> Result<(), LimitReject> {
        self.check_insert_at(req, Instant::now())
    }

    /// 可注入时间，便于单测。
    pub fn check_insert_at(&mut self, req: &OrderRequest, now: Instant) -> Result<(), LimitReject> {
        if !self.cfg.allow_market_order && req.price_type == OrderPriceType::Market {
            return Err(LimitReject::MarketOrderForbidden);
        }
        if req.volume > self.cfg.max_lots_per_order {
            return Err(LimitReject::LotsTooLarge {
                requested: req.volume,
                max: self.cfg.max_lots_per_order,
            });
        }
        // 权利金单价上限。价格必须是有限正数。
        if !req.price.is_finite() || req.price < 0.0 || req.price > self.cfg.max_premium_per_lot {
            return Err(LimitReject::PremiumTooHigh {
                requested: req.price,
                max: self.cfg.max_premium_per_lot,
            });
        }
        self.check_rate_at(now)
    }

    /// 撤单前检查：只查频率，撤单也计入合并计数。
    pub fn check_cancel(&mut self) -> Result<(), LimitReject> {
        self.check_cancel_at(Instant::now())
    }

    pub fn check_cancel_at(&mut self, now: Instant) -> Result<(), LimitReject> {
        self.check_rate_at(now)
    }

    fn check_rate_at(&mut self, now: Instant) -> Result<(), LimitReject> {
        self.evict_old(now);
        if self.recent.len() >= self.cfg.max_orders_per_minute {
            return Err(LimitReject::RateLimited {
                window_count: self.recent.len(),
                max: self.cfg.max_orders_per_minute,
            });
        }
        Ok(())
    }

    /// 真正发送成功后记账。
    pub fn record_sent(&mut self) {
        self.record_sent_at(Instant::now())
    }

    pub fn record_sent_at(&mut self, now: Instant) {
        self.evict_old(now);
        self.recent.push_back(now);
    }

    fn evict_old(&mut self, now: Instant) {
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
    use common_model::{Direction, InstrumentId, OffsetFlag};

    fn limits() -> HardLimits {
        HardLimits {
            max_lots_per_order: 1,
            max_premium_per_lot: 5.0,
            max_orders_per_minute: 3,
            allow_market_order: false,
        }
    }

    fn req(price: f64, volume: i32, pt: OrderPriceType) -> OrderRequest {
        OrderRequest {
            instrument: InstrumentId::new("pp2701"),
            direction: Direction::Buy,
            offset: OffsetFlag::Open,
            price,
            volume,
            price_type: pt,
        }
    }

    #[test]
    fn rejects_two_lots() {
        let mut g = HardLimitGuard::new(limits());
        let e = g.check_insert(&req(1.0, 2, OrderPriceType::Limit)).unwrap_err();
        assert_eq!(e, LimitReject::LotsTooLarge { requested: 2, max: 1 });
    }

    #[test]
    fn rejects_price_above_5() {
        let mut g = HardLimitGuard::new(limits());
        let e = g.check_insert(&req(5.0001, 1, OrderPriceType::Limit)).unwrap_err();
        assert!(matches!(e, LimitReject::PremiumTooHigh { .. }));
    }

    #[test]
    fn accepts_price_exactly_5() {
        let mut g = HardLimitGuard::new(limits());
        assert!(g.check_insert(&req(5.0, 1, OrderPriceType::Limit)).is_ok());
    }

    #[test]
    fn rejects_market_order() {
        let mut g = HardLimitGuard::new(limits());
        let e = g.check_insert(&req(1.0, 1, OrderPriceType::Market)).unwrap_err();
        assert_eq!(e, LimitReject::MarketOrderForbidden);
    }

    #[test]
    fn rejects_nan_and_negative_price() {
        let mut g = HardLimitGuard::new(limits());
        assert!(matches!(
            g.check_insert(&req(f64::NAN, 1, OrderPriceType::Limit)).unwrap_err(),
            LimitReject::PremiumTooHigh { .. }
        ));
        assert!(matches!(
            g.check_insert(&req(-1.0, 1, OrderPriceType::Limit)).unwrap_err(),
            LimitReject::PremiumTooHigh { .. }
        ));
    }

    #[test]
    fn rate_limit_3_per_minute() {
        let mut g = HardLimitGuard::new(limits());
        let t0 = Instant::now();
        for i in 0..3 {
            let now = t0 + Duration::from_secs(i);
            assert!(g.check_insert_at(&req(1.0, 1, OrderPriceType::Limit), now).is_ok());
            g.record_sent_at(now);
        }
        let e = g.check_insert_at(&req(1.0, 1, OrderPriceType::Limit), t0 + Duration::from_secs(3)).unwrap_err();
        assert_eq!(e, LimitReject::RateLimited { window_count: 3, max: 3 });
    }

    #[test]
    fn cancel_counts_toward_rate() {
        let mut g = HardLimitGuard::new(limits());
        let t0 = Instant::now();
        // 一笔报单 + 两笔撤单 = 3
        g.check_insert_at(&req(1.0, 1, OrderPriceType::Limit), t0).unwrap();
        g.record_sent_at(t0);
        g.check_cancel_at(t0 + Duration::from_secs(1)).unwrap();
        g.record_sent_at(t0 + Duration::from_secs(1));
        g.check_cancel_at(t0 + Duration::from_secs(2)).unwrap();
        g.record_sent_at(t0 + Duration::from_secs(2));
        // 第 4 次应被拒
        assert!(matches!(
            g.check_cancel_at(t0 + Duration::from_secs(3)).unwrap_err(),
            LimitReject::RateLimited { .. }
        ));
    }

    #[test]
    fn window_slides_after_60s() {
        let mut g = HardLimitGuard::new(limits());
        let t0 = Instant::now();
        for i in 0..3 {
            let now = t0 + Duration::from_secs(i);
            g.check_insert_at(&req(1.0, 1, OrderPriceType::Limit), now).unwrap();
            g.record_sent_at(now);
        }
        // 61 秒后旧记录全部滑出窗口
        let t1 = t0 + Duration::from_secs(61);
        assert!(g.check_insert_at(&req(1.0, 1, OrderPriceType::Limit), t1).is_ok());
    }
}

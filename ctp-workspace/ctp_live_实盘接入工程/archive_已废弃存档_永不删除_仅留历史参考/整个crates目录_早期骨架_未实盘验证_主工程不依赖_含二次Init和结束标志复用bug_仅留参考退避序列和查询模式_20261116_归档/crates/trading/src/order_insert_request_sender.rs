//! 报单发送。硬保险通过后才调用。

use common_model::{OrderRequest, TradingConfig};
use crate::adapter::TdAdapter;

/// order_ref（报单引用）由调用方生成，保证唯一。
pub fn send(api: &dyn TdAdapter, req: &OrderRequest, order_ref: &str) -> String {
    api.req_order_insert(req, order_ref);
    order_ref.to_string()
}

pub fn make_order_ref(cfg: &TradingConfig, seq: u64) -> String {
    format!("{}_{:08}", cfg.user_id, seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::mock_adapter::MockTdAdapter;
    use common_model::{Direction, InstrumentId, OffsetFlag, OrderPriceType};

    #[test]
    fn send_records_order() {
        let (api, _tx, _rx) = MockTdAdapter::new();
        let req = OrderRequest {
            instrument: InstrumentId::new("pp2701"),
            direction: Direction::Buy,
            offset: OffsetFlag::Open,
            price: 1.0,
            volume: 1,
            price_type: OrderPriceType::Limit,
        };
        let r = send(&api, &req, "u_00000001");
        assert_eq!(r, "u_00000001");
        assert_eq!(api.last_order().unwrap().instrument.as_str(), "pp2701");
    }
}

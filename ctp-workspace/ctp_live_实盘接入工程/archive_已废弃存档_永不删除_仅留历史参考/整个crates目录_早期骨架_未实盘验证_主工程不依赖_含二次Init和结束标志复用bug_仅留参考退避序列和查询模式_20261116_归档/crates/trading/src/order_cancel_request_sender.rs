//! 撤单发送。保留 SHFE（上期所）本地会话定位：OrderSysID 留空，
//! 用 FrontID + SessionID + OrderRef 定位。

use common_model::CancelRequest;
use crate::adapter::TdAdapter;

pub fn send(api: &dyn TdAdapter, req: &CancelRequest) {
    api.req_order_action(req);
}

/// 按 SHFE 规则构造撤单请求：sys_id 留空。
pub fn make_cancel_shfe(
    instrument: common_model::InstrumentId,
    order_ref: String,
    front_id: i32,
    session_id: i32,
) -> CancelRequest {
    CancelRequest { instrument, order_ref, front_id, session_id }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::mock_adapter::MockTdAdapter;
    use common_model::InstrumentId;

    #[test]
    fn send_cancel() {
        let (api, _tx, _rx) = MockTdAdapter::new();
        let req = make_cancel_shfe(InstrumentId::new("pp2701"), "u_1".into(), 22, 11);
        send(&api, &req);
        assert_eq!(api.last_cancel().unwrap().order_ref, "u_1");
    }
}

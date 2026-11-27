//! 交易适配接口。上层逻辑只依赖本接口，ctp2rs 细节在 ctp2rs_backend.rs。

use crossbeam_channel::Sender;
use common_model::OrderRequest;

/// 交易事件。
#[derive(Debug, Clone)]
pub enum TdEvent {
    FrontConnected,
    FrontDisconnected { reason: i32 },
    Authenticated { error_id: i32, error_msg: String },
    LoggedIn { error_id: i32, error_msg: String, session_id: i32, front_id: i32 },
    SettlementConfirmed { error_id: i32, error_msg: String },
    OrderAccepted { order_ref: String, sys_id: String },
    OrderRejected { order_ref: String, error_id: i32, error_msg: String },
    CancelConfirmed { order_ref: String, sys_id: String },
    CancelRejected { order_ref: String, error_id: i32, error_msg: String },
    QueryAccount { available: f64 },
    QueryPosition { instrument: String, position: i32 },
    QueryInstrument { instrument: String, exchange: String, product: String },
    QueryInstrumentEnd,
    RspError { error_id: i32, error_msg: String },
}

pub trait TdAdapter: Send + Sync {
    fn register_front(&self, front: &str);
    fn register_spi(&self, tx: Sender<TdEvent>);
    fn init(&self);
    fn req_authenticate(&self, app_id: &str, auth_code: &str);
    fn req_user_login(&self, broker: &str, user: &str, pwd: &str);
    fn req_settlement_confirm(&self);
    fn req_order_insert(&self, req: &OrderRequest, order_ref: &str);
    fn req_order_action(&self, req: &common_model::CancelRequest);
    fn req_qry_trading_account(&self);
    fn req_qry_investor_position(&self);
    fn req_qry_instrument(&self);
    fn release(&self);
}

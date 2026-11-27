//! 行情适配接口。ctp2rs 细节隔离在 ctp2rs_backend.rs。

use crossbeam_channel::Sender;
use common_model::Tick;

#[derive(Debug, Clone)]
pub enum MdEvent {
    FrontConnected,
    FrontDisconnected { reason: i32 },
    Authenticated { error_id: i32, error_msg: String },
    LoggedIn { error_id: i32, error_msg: String },
    Subscribed { instrument: String, error_id: i32 },
    Tick(Tick),
    RspError { error_id: i32, error_msg: String },
}

pub trait MdAdapter: Send + Sync {
    fn register_front(&self, front: &str);
    fn register_spi(&self, tx: Sender<MdEvent>);
    fn init(&self);
    fn req_authenticate(&self, app_id: &str, auth_code: &str);
    fn req_user_login(&self, broker: &str, user: &str, pwd: &str);
    fn subscribe(&self, instruments: &[String]);
    fn release(&self);
}

//! 交易会话建立：认证 → 登录 → 结算确认。
//! 每一步都在固定超时内等待对应回调，失败即返回带阶段名的错误。

use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError};

use common_model::TradingConfig;

use crate::adapter::{TdAdapter, TdEvent};

/// 会话定位信息。
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub front_id: i32,
    pub session_id: i32,
}

#[derive(Debug)]
pub enum OpenSessionError {
    Timeout(&'static str),
    Rejected { stage: &'static str, error_id: i32, error_msg: String },
}

impl std::fmt::Display for OpenSessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout(s) => write!(f, "等待 {} 超时", s),
            Self::Rejected { stage, error_id, error_msg } =>
                write!(f, "{} 被拒: ErrorID={} ErrorMsg={}", stage, error_id, error_msg),
        }
    }
}
impl std::error::Error for OpenSessionError {}

const STAGE_TIMEOUT: Duration = Duration::from_secs(15);

pub fn open_session(
    api: &dyn TdAdapter,
    rx: &Receiver<TdEvent>,
    cfg: &TradingConfig,
) -> Result<SessionInfo, OpenSessionError> {
    // 1. 认证
    api.req_authenticate(&cfg.app_id, &cfg.auth_code);
    match recv_until(rx, "认证")? {
        TdEvent::Authenticated { error_id: 0, .. } => {}
        TdEvent::Authenticated { error_id, error_msg } =>
            return Err(OpenSessionError::Rejected { stage: "认证", error_id, error_msg }),
        other => return Err(OpenSessionError::Timeout(leak(format!("认证，收到 {:?}", other)))),
    }

    // 2. 登录
    api.req_user_login(&cfg.broker_id, &cfg.user_id, &cfg.password);
    let (front_id, session_id) = match recv_until(rx, "登录")? {
        TdEvent::LoggedIn { error_id: 0, session_id, front_id, .. } => (front_id, session_id),
        TdEvent::LoggedIn { error_id, error_msg, .. } =>
            return Err(OpenSessionError::Rejected { stage: "登录", error_id, error_msg }),
        other => return Err(OpenSessionError::Timeout(leak(format!("登录，收到 {:?}", other)))),
    };

    // 3. 结算确认
    if cfg.settlement_confirm_on_login {
        api.req_settlement_confirm();
        match recv_until(rx, "结算确认")? {
            TdEvent::SettlementConfirmed { error_id: 0, .. } => {}
            TdEvent::SettlementConfirmed { error_id, error_msg } =>
                return Err(OpenSessionError::Rejected { stage: "结算确认", error_id, error_msg }),
            other => return Err(OpenSessionError::Timeout(leak(format!("结算确认，收到 {:?}", other)))),
        }
    }

    Ok(SessionInfo { front_id, session_id })
}

fn recv_until(rx: &Receiver<TdEvent>, stage: &'static str) -> Result<TdEvent, OpenSessionError> {
    match rx.recv_timeout(STAGE_TIMEOUT) {
        Ok(ev) => Ok(ev),
        Err(RecvTimeoutError::Timeout) => Err(OpenSessionError::Timeout(stage)),
        Err(RecvTimeoutError::Disconnected) => Err(OpenSessionError::Timeout(stage)),
    }
}

/// 把动态字符串转成 &'static str 仅用于错误展示。生命周期泄漏极小，可接受。
fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::mock_adapter::MockTdAdapter;

    fn cfg() -> TradingConfig {
        TradingConfig {
            broker_id: "0000".into(),
            user_id: "u".into(),
            password: "p".into(),
            app_id: "a".into(),
            auth_code: "c".into(),
            primary_front_ip: "1.2.3.4".into(),
            td_port: 61205,
            flow_dir: "x".into(),
            settlement_confirm_on_login: true,
            hard_limits: common_model::HardLimits {
                max_lots_per_order: 1,
                max_premium_per_lot: 5.0,
                max_orders_per_minute: 3,
                allow_market_order: false,
            },
        }
    }

    #[test]
    fn open_session_success() {
        let (api, tx, rx) = MockTdAdapter::new();
        std::thread::spawn(move || {
            tx.send(TdEvent::Authenticated { error_id: 0, error_msg: String::new() }).unwrap();
            tx.send(TdEvent::LoggedIn { error_id: 0, error_msg: String::new(), session_id: 11, front_id: 22 }).unwrap();
            tx.send(TdEvent::SettlementConfirmed { error_id: 0, error_msg: String::new() }).unwrap();
        });
        let s = open_session(&api, &rx, &cfg()).unwrap();
        assert_eq!(s.session_id, 11);
        assert_eq!(s.front_id, 22);
    }

    #[test]
    fn open_session_auth_rejected() {
        let (api, tx, rx) = MockTdAdapter::new();
        std::thread::spawn(move || {
            tx.send(TdEvent::Authenticated { error_id: 63, error_msg: "auth fail".into() }).unwrap();
        });
        let e = open_session(&api, &rx, &cfg()).unwrap_err();
        assert!(matches!(e, OpenSessionError::Rejected { stage: "认证", error_id: 63, .. }));
    }
}

//! 认证 / 登录 / 结算确认三件套。
//!
//! CTP 交易前置的连接生命周期：
//!   OnFrontConnected
//!     → ReqAuthenticate          → OnRspAuthenticate
//!     → ReqUserLogin             → OnRspUserLogin
//!     → ReqSettlementInfoConfirm → OnRspSettlementInfoConfirm
//!
//! 每天首次登录必须做结算确认，否则报单 ErrorID=42。
//!
//! 本模块只负责"把会话打开"，不负责查资金/持仓/合约，那些在 query 模块。

use std::os::raw::c_char;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

use ctp2rs::v1alpha1::*;

use crate::config::{appid, authcode, broker, password, user};

use super::ctp_event_definitions_all_variants::Event;

/// 登录成功后返回的会话信息。
///
/// front_id / session_id 撤单必须传（`CThostFtdcInputOrderActionField` 的
/// FrontID / SessionID 字段），max_order_ref 用于生成本地报单引用避免重复。
#[derive(Debug, Clone, Copy)]
pub struct SessionInfo {
    pub front_id: i32,
    pub session_id: i32,
    pub max_order_ref: i32,
}

/// C 字符串写入定长 buf，末尾补 0。CTP 结构体字段都是 `[c_char; N]`。
pub fn copy_str(buf: &mut [c_char], s: &str) {
    let bytes = s.as_bytes();
    let n = bytes.len().min(buf.len().saturating_sub(1));
    for i in 0..n {
        buf[i] = bytes[i] as c_char;
    }
    if n < buf.len() {
        buf[n] = 0;
    }
}

/// 在 rx 上等待满足 pred 的事件，超时返回 None。
///
/// 收到的不匹配事件会被打印后丢弃——查询/报单流程共用同一个 channel，
/// 期间可能夹杂 RtnOrder / HeartBeatWarning 等无关事件。
pub fn wait_event<F: Fn(&Event) -> bool>(
    rx: &Receiver<Event>,
    timeout: Duration,
    pred: F,
) -> Option<Event> {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match rx.recv_timeout(remaining) {
            Ok(ev) => {
                println!("[EVENT] {:?}", ev);
                if pred(&ev) {
                    return Some(ev);
                }
            }
            Err(_) => return None,
        }
    }
}

/// 认证 + 登录 + 结算确认。
///
/// 调用前提：OnFrontConnected 已经收到（调用方负责等）。
///
/// 返回 SessionInfo，调用方拿 front_id / session_id 去报单或撤单。
pub fn open_session(api: &TraderApi, rx: &Receiver<Event>) -> Result<SessionInfo, String> {
    // ── 1. 认证 ──
    let mut auth = CThostFtdcReqAuthenticateField::default();
    copy_str(&mut auth.BrokerID, broker());
    copy_str(&mut auth.UserID, user());
    copy_str(&mut auth.AppID, appid());
    copy_str(&mut auth.AuthCode, authcode());
    println!("[AUTH REQ] ret={}", api.req_authenticate(&mut auth, 1));
    match wait_event(rx, Duration::from_secs(10), |e| matches!(e, Event::AuthRsp { .. })) {
        Some(Event::AuthRsp { err: None }) => println!("[AUTH SUCCESS]"),
        Some(Event::AuthRsp { err: Some((id, m)) }) => {
            return Err(format!("认证: {} {}", id, m))
        }
        _ => return Err("认证超时".into()),
    }

    // ── 2. 登录 ──
    let mut login = CThostFtdcReqUserLoginField::default();
    copy_str(&mut login.BrokerID, broker());
    copy_str(&mut login.UserID, user());
    copy_str(&mut login.Password, password());
    println!("[LOGIN REQ] ret={}", api.req_user_login(&mut login, 2));
    let session = match wait_event(rx, Duration::from_secs(15), |e| {
        matches!(e, Event::LoginRsp { .. })
    }) {
        Some(Event::LoginRsp {
            err: None,
            front_id,
            session_id,
            trading_day,
            max_order_ref,
        }) => {
            println!(
                "[LOGIN SUCCESS] TradingDay={} FrontID={} SessionID={} MaxOrderRef={}",
                trading_day, front_id, session_id, max_order_ref
            );
            SessionInfo { front_id, session_id, max_order_ref }
        }
        Some(Event::LoginRsp { err: Some((id, m)), .. }) => {
            return Err(format!("登录: {} {}", id, m))
        }
        _ => return Err("登录超时".into()),
    };

    // ── 3. 结算确认 ──
    let mut settle = CThostFtdcSettlementInfoConfirmField::default();
    copy_str(&mut settle.BrokerID, broker());
    copy_str(&mut settle.InvestorID, user());
    println!("[SETTLE REQ] ret={}", api.req_settlement_info_confirm(&mut settle, 3));
    match wait_event(rx, Duration::from_secs(10), |e| {
        matches!(e, Event::SettlementConfirmRsp { .. })
    }) {
        Some(Event::SettlementConfirmRsp { err: None }) => println!("[SETTLE SUCCESS]"),
        Some(Event::SettlementConfirmRsp { err: Some((id, m)) }) => {
            return Err(format!("结算: {} {}", id, m))
        }
        _ => return Err("结算确认超时".into()),
    }

    Ok(session)
}

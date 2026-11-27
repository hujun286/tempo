//! member 事件 / tick 统一结构。
use ctp2rs::ffi::WrapToString;
use ctp2rs::v1alpha1::*;

// ============================================================
// member 事件 / tick 统一结构（照抄 v1.1）
// ============================================================

pub(crate) enum MemberEvent {
    Connected(usize),
    Disconnected(usize, i32),
    Login(usize, Option<(i32, String)>),
    /// ★v1.5 方案B：Ready 事件已删除——登录+订阅由 member 线程在 Connected 分支里直接执行并记账，
    ///   事件通道里不再有"中转确认"这一环（少一跳少一处可能丢）。
    Tick {
        member_idx: usize,
        instrument: String,
        last_price: f64,
        volume: u64,
        turnover: f64,
        open_interest: f64,
        trading_day: String,
        /// 🔴3 单调性校验用的交易所侧时间（D025）："HH:MM:SS" 与毫秒部分。
        update_time: String,
        update_millisec: i32,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct TickEvent {
    pub(crate) member_idx: usize,
    pub(crate) instrument: String,
    pub(crate) last_price: f64,
    pub(crate) volume: u64,
    pub(crate) turnover: f64,
    pub(crate) open_interest: f64,
    pub(crate) trading_day: String,
    /// 🔴3 透传字段（D025）：单调性校验在 member 线程完成，这里只是透传，
    ///   供排查/未来下游使用。当前无人读，标 dead_code 压制。
    #[allow(dead_code)]
    pub(crate) update_time: String,
    #[allow(dead_code)]
    pub(crate) update_millisec: i32,
}

pub(crate) fn info_pair(i: Option<&CThostFtdcRspInfoField>) -> Option<(i32, String)> {
    match i {
        Some(x) if x.ErrorID != 0 => Some((x.ErrorID, x.ErrorMsg.to_string())),
        _ => None,
    }
}

//! member 运行统计 + 锁中毒恢复工具。
use std::time::Instant;

#[derive(Default, Clone)]
pub struct MemberStats {
    pub(crate) connected: bool,
    pub(crate) logged_in: bool,
    pub(crate) login_fail: u64,
    pub(crate) disconnects: u64,
    pub(crate) ready_rounds: u64,
    pub(crate) last_disconnect_reason: i32,
    pub(crate) last_disconnect_at: Option<Instant>,
    pub(crate) ticks: u64,
    pub(crate) last_tick_at: Option<Instant>,
    /// 🔴3 交易所时间回拨笔数（观测计数，不影响数据流）
    pub(crate) tick_out_of_order: u64,
    /// 方案B 主动 teardown→重建 次数（连接超时 / 断线超窗 / 连续登录失败三类收敛于此）
    pub(crate) rebuilds: u64,
}

pub fn 锁或恢复<T>(锁: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    锁.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

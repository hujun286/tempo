//! CTP 会话：ApiHolder / 登录订阅 / ★v1.5 方案B 事件驱动重连状态机 / 🔴3 tick 单调性校验。
use crate::config::{
    CONNECT_TIMEOUT_MS, DISCONNECT_REBUILD_MS, LOGIN_FAIL_REBUILD, LOGIN_SETTLE_MS,
    RECONNECT_BASE_MS, RECONNECT_MAX_MS, SUBSCRIBE_BATCH, SUBSCRIBE_DELAY_MS, 回拨告警节流,
};
use crate::events::{MemberEvent, TickEvent};
use crate::spi::MemberSpi;
use crate::stats::{MemberStats, 锁或恢复};
use ctp2rs::ffi::AssignFromString;
use ctp2rs::v1alpha1::*;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ============================================================
// 启动 member（★v1.5 方案B：事件驱动重连状态机，替代 api.join() 阻塞版）
// ============================================================

pub(crate) struct ApiHolder {
    api: Option<MdApi>,
    dynlib: PathBuf,
    flow_dir: PathBuf,
}

impl ApiHolder {
    fn new(dynlib: PathBuf, flow_dir: PathBuf) -> Self {
        Self {
            api: None,
            dynlib,
            flow_dir,
        }
    }

    fn spawn_new(
        &mut self,
        member_idx: usize,
        front: &str,
        tx: &mpsc::SyncSender<MemberEvent>,
    ) -> bool {
        self.teardown();
        std::fs::create_dir_all(&self.flow_dir).ok();
        // 【规则15 零 unwrap】路径来自工作区根+ASCII相对路径，必是合法 UTF-8；
        //   用 to_string_lossy 兜底(永不失败)，且【绑定到本地 let】保证 &str 活过 FFI 调用，无悬垂。
        let dynlib_s = self.dynlib.to_string_lossy().into_owned();
        let flow_s = self.flow_dir.to_string_lossy().into_owned();
        let api = MdApi::create_api(&dynlib_s, &flow_s, false, false);
        let spi = Box::new(MemberSpi {
            member_idx,
            tx: tx.clone(),
        });
        let spi_ptr = Box::into_raw(spi) as *mut dyn MdSpi;
        api.register_spi(spi_ptr);
        api.register_front(front);
        api.init();
        self.api = Some(api);
        true
    }

    fn teardown(&mut self) {
        if let Some(api) = self.api.take() {
            api.release();
        }
    }
}

fn login_and_subscribe(
    api: &MdApi,
    broker_id: &str,
    user_id: &str,
    password: &str,
    instruments: &[String],
    tag: &str,
) -> bool {
    let mut login = CThostFtdcReqUserLoginField::default();
    login.BrokerID.assign_from_str(broker_id);
    login.UserID.assign_from_str(user_id);
    login.Password.assign_from_str(password);
    let ret = api.req_user_login(&mut login, 1);
    tracing::info!("{} req_user_login ret={}", tag, ret);
    if ret != 0 {
        return false;
    }
    std::thread::sleep(Duration::from_millis(LOGIN_SETTLE_MS));

    let total = instruments.len();
    let mut done = 0;
    for batch in instruments.chunks(SUBSCRIBE_BATCH) {
        let r = api.subscribe_market_data(batch);
        done += batch.len();
        tracing::info!("{} 订阅 {}/{} ret={}", tag, done, total, r);
        std::thread::sleep(Duration::from_millis(SUBSCRIBE_DELAY_MS));
    }
    tracing::info!("{} 订阅完成 {}", tag, done);
    true
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_member(
    member_idx: usize,
    front: String,
    flow_dir: PathBuf,
    dynlib: PathBuf,
    broker_id: String,
    user_id: String,
    password: String,
    instruments: Vec<String>,
    evt_tx: mpsc::SyncSender<MemberEvent>,
    evt_rx: mpsc::Receiver<MemberEvent>,
    tick_tx: mpsc::Sender<TickEvent>,
    stats: Arc<Mutex<Vec<MemberStats>>>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let tag = format!("[MD#{}]", member_idx);
        tracing::info!(
            "{} 启动 front={} 合约数={}（方案B：事件驱动重连，无 join 阻塞）",
            tag,
            front,
            instruments.len()
        );
        let mut holder = ApiHolder::new(dynlib, flow_dir);
        let mut backoff_ms = RECONNECT_BASE_MS;
        let mut 重建轮数: u64 = 0;
        let mut 登录失败连击: u64 = 0;
        let mut 在线 = false;
        let mut 断线时刻: Option<Instant> = None;
        // 🔴3 单调性状态：合约 → (交易日, 当日总毫秒, 上次告警时刻)
        let mut 单调状态: HashMap<String, (String, i64, Option<Instant>)> = HashMap::new();

        if !holder.spawn_new(member_idx, &front, &evt_tx) {
            tracing::error!("{} 建 MdApi 失败，{}ms 后由守护定时器重试", tag, backoff_ms);
        }
        let mut 建立时刻 = Instant::now(); // 每次 spawn_new 后重置

        loop {
            if stop.load(Ordering::Relaxed) {
                holder.teardown();
                tracing::info!("{} 收到退出信号，线程退出", tag);
                break;
            }
            match evt_rx.recv_timeout(Duration::from_millis(1000)) {
                Ok(MemberEvent::Connected(idx)) => {
                    在线 = true;
                    断线时刻 = None;
                    登录失败连击 = 0;
                    {
                        let mut st = 锁或恢复(&stats);
                        st[idx].connected = true;
                        st[idx].logged_in = false;
                    }
                    tracing::error!("[MD#{}] 前置已连接", idx);
                    // ★方案B 核心：登录由 Connected 事件直接驱动。CTP 断线自动重连成功
                    //   ⇒ OnFrontConnected 必回调 ⇒ 这里必然重新登录+订阅 ——
                    //   "已连 16 已登录 0" 在结构上不可能再出现（D016 病灶已除）。
                    if let Some(api) = holder.api.as_ref()
                        && login_and_subscribe(
                            api,
                            &broker_id,
                            &user_id,
                            &password,
                            &instruments,
                            &tag,
                        )
                    {
                        let mut st = 锁或恢复(&stats);
                        st[idx].logged_in = true;
                        st[idx].ready_rounds += 1;
                        let rounds = st[idx].ready_rounds;
                        drop(st);
                        tracing::error!("[MD#{}] 登录+订阅完成（第 {} 轮）", idx, rounds);
                        backoff_ms = RECONNECT_BASE_MS;
                    } else if holder.api.is_some() {
                        登录失败连击 += 1;
                        tracing::error!("[MD#{}] 登录请求失败（连续 {} 次）", idx, 登录失败连击);
                    }
                    if 登录失败连击 >= LOGIN_FAIL_REBUILD {
                        // 连续登录失败：teardown 重建，防死循环打柜台
                        重建轮数 += 1;
                        {
                            let mut st = 锁或恢复(&stats);
                            st[idx].rebuilds += 1;
                            st[idx].connected = false;
                            st[idx].logged_in = false;
                        }
                        tracing::error!(
                            "{} ⚙ 连续登录失败达 {} 次，主动重建（第 {} 轮）",
                            tag,
                            LOGIN_FAIL_REBUILD,
                            重建轮数
                        );
                        holder.teardown();
                        sleep_or_stop(Duration::from_millis(backoff_ms), &stop);
                        backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
                        if holder.spawn_new(member_idx, &front, &evt_tx) {
                            建立时刻 = Instant::now();
                        }
                        在线 = false;
                        断线时刻 = None;
                    }
                }
                Ok(MemberEvent::Login(idx, info)) => match info {
                    None => {
                        let mut st = 锁或恢复(&stats);
                        st[idx].logged_in = true;
                    }
                    Some((id, msg)) => {
                        登录失败连击 += 1;
                        {
                            let mut st = 锁或恢复(&stats);
                            st[idx].login_fail += 1;
                            st[idx].logged_in = false;
                        }
                        tracing::error!(
                            "[MD#{}] 登录失败 ErrorID={} {}（连续 {} 次）",
                            idx,
                            id,
                            msg,
                            登录失败连击
                        );
                        if 登录失败连击 >= LOGIN_FAIL_REBUILD {
                            重建轮数 += 1;
                            {
                                let mut st = 锁或恢复(&stats);
                                st[idx].rebuilds += 1;
                                st[idx].connected = false;
                            }
                            tracing::error!(
                                "{} ⚙ 连续登录失败达 {} 次，主动重建（第 {} 轮）",
                                tag,
                                LOGIN_FAIL_REBUILD,
                                重建轮数
                            );
                            holder.teardown();
                            sleep_or_stop(Duration::from_millis(backoff_ms), &stop);
                            backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
                            if holder.spawn_new(member_idx, &front, &evt_tx) {
                                建立时刻 = Instant::now();
                            }
                            在线 = false;
                            断线时刻 = None;
                        }
                    }
                },
                Ok(MemberEvent::Disconnected(idx, reason)) => {
                    在线 = false;
                    断线时刻 = Some(Instant::now());
                    let mut st = 锁或恢复(&stats);
                    st[idx].connected = false;
                    st[idx].logged_in = false;
                    st[idx].last_disconnect_reason = reason;
                    st[idx].last_disconnect_at = Some(Instant::now());
                    st[idx].disconnects += 1;
                    let 累计 = st[idx].disconnects;
                    drop(st);
                    tracing::error!(
                        "[MD#{}] 前置断开 reason={}（累计 {} 次；CTP 自动重连中，{} 秒未回连则主动重建）",
                        idx,
                        reason,
                        累计,
                        DISCONNECT_REBUILD_MS / 1000
                    );
                }
                Ok(MemberEvent::Tick {
                    member_idx,
                    instrument,
                    last_price,
                    volume,
                    turnover,
                    open_interest,
                    trading_day,
                    update_time,
                    update_millisec,
                }) => {
                    {
                        let mut st = 锁或恢复(&stats);
                        st[member_idx].ticks += 1;
                        st[member_idx].last_tick_at = Some(Instant::now());
                        // 🔴3 观测型校验：回拨只计数+节流告警，tick 照常转发（D025）
                        if 校验tick单调性(
                            &mut 单调状态,
                            &instrument,
                            &trading_day,
                            &update_time,
                            update_millisec,
                            &tag,
                        ) {
                            st[member_idx].tick_out_of_order += 1;
                        }
                    }
                    let _ = tick_tx.send(TickEvent {
                        member_idx,
                        instrument,
                        last_price,
                        volume,
                        turnover,
                        open_interest,
                        trading_day,
                        update_time,
                        update_millisec,
                    });
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // ── 守护定时器（Timeout 分支）：三类异常全部收敛到 teardown→重建 ──
                    if !在线
                        && 断线时刻.is_none()
                        && 建立时刻.elapsed() >= Duration::from_millis(CONNECT_TIMEOUT_MS)
                    {
                        重建轮数 += 1;
                        {
                            let mut st = 锁或恢复(&stats);
                            st[member_idx].rebuilds += 1;
                        }
                        tracing::error!(
                            "{} ⚙ 连接 {} 秒未成功（CONNECT_TIMEOUT_MS={}），主动重建（第 {} 轮）",
                            tag,
                            CONNECT_TIMEOUT_MS / 1000,
                            CONNECT_TIMEOUT_MS / 1000,
                            重建轮数
                        );
                        holder.teardown();
                        sleep_or_stop(Duration::from_millis(backoff_ms), &stop);
                        backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
                        if holder.spawn_new(member_idx, &front, &evt_tx) {
                            建立时刻 = Instant::now();
                        }
                    } else if let Some(td) = 断线时刻
                        && td.elapsed() >= Duration::from_millis(DISCONNECT_REBUILD_MS)
                    {
                        重建轮数 += 1;
                        {
                            let mut st = 锁或恢复(&stats);
                            st[member_idx].rebuilds += 1;
                        }
                        tracing::error!(
                            "{} ⚙ 断线 {} 秒未回连（DISCONNECT_REBUILD_MS={}，CTP 自动重连未生效），主动重建（第 {} 轮）",
                            tag,
                            DISCONNECT_REBUILD_MS / 1000,
                            DISCONNECT_REBUILD_MS / 1000,
                            重建轮数
                        );
                        holder.teardown();
                        sleep_or_stop(Duration::from_millis(backoff_ms), &stop);
                        backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
                        if holder.spawn_new(member_idx, &front, &evt_tx) {
                            建立时刻 = Instant::now();
                        }
                        断线时刻 = None;
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    // 发送端全部丢弃才会走到这（member 自己持有 tx，正常不会发生）：保守退出
                    holder.teardown();
                    tracing::info!("{} 事件通道关闭，线程退出", tag);
                    break;
                }
            }
        }
    })
}

/// 🔴3 tick 单调性校验（观测型，不干预数据流，D025）。
/// 比对交易所侧 (TradingDay, UpdateTime+UpdateMillisec)：
///   同交易日内时间倒退 / 交易日倒退 → 返回 true（调用方计数+节流告警）；
///   新交易日（更大）→ 正常轮转，重置基线不误报；
///   空时间戳 / 解析失败 → 跳过（观测组件绝不因自身问题干预数据流）。
fn 校验tick单调性(
    状态: &mut HashMap<String, (String, i64, Option<Instant>)>,
    合约: &str,
    交易日: &str,
    时间: &str,
    毫秒: i32,
    tag: &str,
) -> bool {
    if 交易日.is_empty() || 时间.is_empty() {
        return false; // 空时间戳跳过校验（不误报）
    }
    let Some(总ms) = 解析时分秒毫(时间, 毫秒) else {
        return false; // 时间串解析不了 → 跳过，不打日志刷屏
    };
    match 状态.get_mut(合约) {
        None => {
            状态.insert(合约.to_string(), (交易日.to_string(), 总ms, None));
            false
        }
        Some((旧日, 旧ms, 上次告警)) => {
            if 交易日 > 旧日.as_str() {
                // 新交易日正常轮转：重置基线，不误报回退
                *旧日 = 交易日.to_string();
                *旧ms = 总ms;
                *上次告警 = None;
                return false;
            }
            let 回拨 = 交易日 < 旧日.as_str() || 总ms < *旧ms;
            if 回拨 {
                let 需告警 = match 上次告警 {
                    None => true,
                    Some(t) => t.elapsed() >= 回拨告警节流,
                };
                if 需告警 {
                    tracing::error!(
                        "{} ⚠ 交易所时间回拨: {} 上一笔 {}@{}ms → 本笔 {}@{}ms（tick 照常采集转发，仅观测告警）",
                        tag,
                        合约,
                        旧日,
                        旧ms,
                        交易日,
                        总ms
                    );
                    *上次告警 = Some(Instant::now());
                }
                true
            } else {
                *旧ms = 总ms;
                false
            }
        }
    }
}

/// "HH:MM:SS" + 毫秒部分 → 当日总毫秒；解析失败返回 None。
fn 解析时分秒毫(时间: &str, 毫秒: i32) -> Option<i64> {
    let p: Vec<&str> = 时间.split(':').collect();
    if p.len() != 3 {
        return None;
    }
    let h: i64 = p[0].trim().parse().ok()?;
    let m: i64 = p[1].trim().parse().ok()?;
    let s: i64 = p[2].trim().parse().ok()?;
    if !(0..24).contains(&h) || !(0..60).contains(&m) || !(0..61).contains(&s) {
        return None;
    }
    Some(h * 3_600_000 + m * 60_000 + s * 1_000 + i64::from(毫秒.max(0)))
}

pub(crate) fn sleep_or_stop(dur: Duration, stop: &AtomicBool) {
    let step = Duration::from_millis(100);
    let mut left = dur;
    while left > Duration::ZERO {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let s = if left < step { left } else { step };
        std::thread::sleep(s);
        left = left.saturating_sub(s);
    }
}

// ★v1.5 方案B：原 spawn_listener 已删除——stats 记账 + tick 转发职责并入 member 线程
//   （事件通道私有后无需中转线程，见决策日志 D024 实现要点 3）。

#[cfg(test)]
mod 测试 {
    use super::*;

    fn 校验状态() -> HashMap<String, (String, i64, Option<Instant>)> {
        HashMap::new()
    }

    #[test]
    fn 首个tick不报回拨() {
        let mut st = 校验状态();
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:00", 0, "[T]"
        ));
    }

    #[test]
    fn 空时间戳跳过校验() {
        let mut st = 校验状态();
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "", 0, "[T]"
        ));
        assert!(!校验tick单调性(
            &mut st, "IF2401", "", "09:30:00", 0, "[T]"
        ));
        // 空时间戳不进入状态表（下一笔正常起算）
        assert!(st.is_empty());
    }

    #[test]
    fn 正常递增不报回拨() {
        let mut st = 校验状态();
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:00", 0, "[T]"
        ));
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:01", 0, "[T]"
        ));
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:01", 500, "[T]"
        ));
    }

    #[test]
    fn 秒级回拨被检测() {
        let mut st = 校验状态();
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:05", 0, "[T]"
        ));
        assert!(校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:04", 0, "[T]"
        ));
    }

    #[test]
    fn 毫秒级回拨被检测() {
        let mut st = 校验状态();
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:05", 500, "[T]"
        ));
        assert!(校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:05", 400, "[T]"
        ));
    }

    #[test]
    fn 新交易日不误报回退() {
        let mut st = 校验状态();
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "15:00:00", 0, "[T]"
        ));
        // 次日 09:00 "时分秒"更小但交易日更大 → 正常轮转，不误报
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261125", "09:00:00", 0, "[T]"
        ));
    }

    #[test]
    fn 回退到更早交易日被检测() {
        let mut st = 校验状态();
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261125", "09:00:00", 0, "[T]"
        ));
        assert!(校验tick单调性(
            &mut st, "IF2401", "20261124", "10:00:00", 0, "[T]"
        ));
    }

    #[test]
    fn 不同合约互不干扰() {
        let mut st = 校验状态();
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:05", 0, "[T]"
        ));
        // 另一合约从更早时间起算，不影响 IF2401 的基线
        assert!(!校验tick单调性(
            &mut st, "rb2501", "20261124", "09:29:00", 0, "[T]"
        ));
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:06", 0, "[T]"
        ));
    }
}

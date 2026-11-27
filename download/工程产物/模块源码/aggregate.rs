//! 聚合：tick → 分钟K线（收盘即送落盘，不攒整天）。
use crate::config::{MINUTE_MS, REPORT_INTERVAL_SEC, 分片落盘秒};
use crate::events::TickEvent;
use chrono::{Local, LocalResult, TimeZone};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

// ============================================================
// 聚合：tick → 分钟K线
//   ★本版改动点 1：不再往"整天 cache"里攒，收盘即把 bar 发给落盘线程。
// ============================================================

/// 一根收盘的分钟K线。派生 Serialize/Deserialize 是为了能逐行写成/读回 jsonl 分片。
/// 这里只存 CTP 给的【当日累计】volume/turnover；单分钟增量 volume_1m/turnover_1m 留到
/// 【合并时】按同合约时间相邻两根做差得到（合并时能看全一天，算增量最简单、也最好测）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MinuteBar {
    pub(crate) instrument_id: String,
    pub(crate) bar_time_ms: i64,
    pub(crate) trading_day: String,
    pub(crate) open: f64,
    pub(crate) high: f64,
    pub(crate) low: f64,
    pub(crate) close: f64,
    pub(crate) volume: u64,
    pub(crate) turnover: f64,
    pub(crate) open_interest: f64,
    /// ★单分钟增量 = 本根累计 - 上一根累计（同合约）。采集端不填，默认 0；【合并时】算好再写进成品。
    /// serde(default)：老 jsonl 没这两列也能正常反序列化（向前兼容）。
    #[serde(default)]
    pub(crate) volume_1m: u64,
    #[serde(default)]
    pub(crate) turnover_1m: f64,
    /// true = 这根是合并时补出来的空分钟（无 tick），OHLC 前收、量额 0。
    pub(crate) is_gap: bool,
}

struct Bucket {
    pub(crate) bar_ms: i64,
    pub(crate) open: f64,
    pub(crate) high: f64,
    pub(crate) low: f64,
    pub(crate) close: f64,
    pub(crate) volume: u64,
    pub(crate) turnover: f64,
    pub(crate) open_interest: f64,
    pub(crate) trading_day: String,
}

// 【规则15 零 unwrap + 锁中毒处理】
// 为什么不用 .lock().unwrap()：持锁线程若 panic，锁会被标记"中毒(poisoned)"，
// 之后 unwrap 会连带 panic，把"一个线程挂了"放大成"全体采集停摆"。
// 这里"中毒也照常取回数据"——统计计数丢失一点无所谓，采集不能停。

fn floor_minute(ms: i64) -> i64 {
    ms - ms.rem_euclid(MINUTE_MS)
}

pub(crate) fn ms_to_date(ms: i64) -> String {
    // 【规则15 零 unwrap】timestamp_millis_opt 返回 LocalResult 三态：
    //   Single  = 唯一本地时刻（中国无夏令时，99.999% 走这里）
    //   Ambiguous = 夏令时回拨重叠的那小时，取较早那个(dt)即可，日期一样
    //   None    = 毫秒越界，理论上 bar 时间不会出现；兜底按 UTC 取日期(同毫秒 UTC 必合法)
    // 任何分支都不 unwrap —— 就算真遇上 None 也只打 warn + 兜底，绝不 panic 拖垮采集。
    match Local.timestamp_millis_opt(ms) {
        LocalResult::Single(dt) | LocalResult::Ambiguous(dt, _) => dt.format("%Y%m%d").to_string(),
        LocalResult::None => {
            tracing::warn!("ms_to_date: {} 非本地合法时刻，兜底按 UTC 取日期", ms);
            chrono::Utc
                .timestamp_millis_opt(ms)
                .single()
                .map(|dt| dt.format("%Y%m%d").to_string())
                .unwrap_or_else(|| "19700101".to_string())
        }
    }
}

/// 聚合线程：和 v1.1 一样维护"每个活跃合约一个桶"，tick 进来更新桶，分钟切换时收盘。
/// 唯一区别：收盘的 bar 不再进 cache，而是【立刻 send 给落盘线程】。
pub(crate) fn run_aggregator(
    tick_rx: mpsc::Receiver<TickEvent>,
    bar_tx: mpsc::SyncSender<MinuteBar>,
    stop: Arc<AtomicBool>,
) {
    let mut buckets: HashMap<String, Bucket> = HashMap::new();
    let mut total_done = 0u64; // 今日已收盘并送出的 bar 数（用于日志）
    let mut per_member_ticks: HashMap<usize, u64> = HashMap::new();
    let mut last_report = Instant::now();
    let mut last_tick_at = Instant::now();

    loop {
        match tick_rx.recv_timeout(Duration::from_millis(200)) {
            Ok(t) => {
                *per_member_ticks.entry(t.member_idx).or_default() += 1;
                last_tick_at = Instant::now();
                // 无效价过滤（与 v1.1 一致）：<=0 或超大视为脏数据丢弃。
                if t.last_price <= 0.0 || t.last_price >= 1e30 {
                    continue;
                }
                // 用收包本机时间取整到分钟（与 v1.1 一致，规避盘前重演污染）。
                let bar_ms = floor_minute(Local::now().timestamp_millis());
                close_if_switching(
                    &t.instrument,
                    bar_ms,
                    &mut buckets,
                    &mut total_done,
                    &bar_tx,
                );
                upsert_bucket(&t, bar_ms, &mut buckets);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }

        // 兜底巡检：冷门合约长时间无 tick 时，靠它把停在旧分钟的桶收盘（与 v1.1 一致）。
        if last_tick_at.elapsed() >= Duration::from_secs(60)
            || last_report.elapsed() >= Duration::from_secs(REPORT_INTERVAL_SEC)
        {
            let now_ms = floor_minute(Local::now().timestamp_millis());
            let stale: Vec<String> = buckets
                .iter()
                .filter_map(|(k, b)| {
                    if b.bar_ms != now_ms {
                        Some(k.clone())
                    } else {
                        None
                    }
                })
                .collect();
            for inst in stale {
                close_bucket(&inst, &mut buckets, &mut total_done, &bar_tx);
            }
        }

        if last_report.elapsed() >= Duration::from_secs(REPORT_INTERVAL_SEC) {
            let mut dist: Vec<(usize, u64)> =
                per_member_ticks.iter().map(|(&k, &v)| (k, v)).collect();
            dist.sort_unstable_by_key(|&(k, _)| k);
            let dist_str: String = dist
                .iter()
                .map(|(k, v)| format!("MD#{}:{}", k, v))
                .collect::<Vec<_>>()
                .join(" ");
            tracing::info!(
                "[AGG] 已落盘 {} 根 | 活跃桶 {} | 内存常驻桶 {}（不再攒整天，内存恒定）| 各member tick [{}]",
                total_done,
                buckets.len(),
                buckets.len(),
                dist_str
            );
            last_report = Instant::now();
        }

        if stop.load(Ordering::Relaxed) {
            tracing::info!(
                "[AGG] 收到退出信号，开始收尾：把活跃桶全部收盘送落盘（当天已在分片里，几乎秒完）"
            );
            break;
        }
    }

    // 收尾：把所有还开着的桶收盘送出，然后 drop(bar_tx) 通知落盘线程"没数据了，可以 fsync 收尾退出"。
    let drain: Vec<(String, Bucket)> = buckets.drain().collect();
    for (inst, b) in drain {
        send_bar(
            MinuteBar {
                instrument_id: inst,
                bar_time_ms: b.bar_ms,
                trading_day: b.trading_day,
                open: b.open,
                high: b.high,
                low: b.low,
                close: b.close,
                volume: b.volume,
                turnover: b.turnover,
                open_interest: b.open_interest,
                volume_1m: 0, // 采集端不算增量，留到合并
                turnover_1m: 0.0,
                is_gap: false,
            },
            &mut total_done,
            &bar_tx,
        );
    }
    drop(bar_tx);
    tracing::info!(
        "[AGG] 聚合线程退出 | 共收盘 {} 根（已全部追加进当天分片，崩溃最多丢最后 {} 秒）",
        total_done,
        分片落盘秒
    );
}

fn close_if_switching(
    inst: &str,
    bar_ms: i64,
    buckets: &mut HashMap<String, Bucket>,
    total_done: &mut u64,
    bar_tx: &mpsc::SyncSender<MinuteBar>,
) {
    let switch = matches!(buckets.get(inst), Some(b) if b.bar_ms != bar_ms);
    if switch {
        close_bucket(inst, buckets, total_done, bar_tx);
    }
}

/// 收盘一个合约的当前桶：取出桶 → 组装 MinuteBar → 立刻发给落盘线程。
/// 对比 v1.1：这里原本是 push 进 cache 等退出才写；现在当场就送走，内存里不留整天。
fn close_bucket(
    inst: &str,
    buckets: &mut HashMap<String, Bucket>,
    total_done: &mut u64,
    bar_tx: &mpsc::SyncSender<MinuteBar>,
) {
    if let Some(b) = buckets.remove(inst) {
        send_bar(
            MinuteBar {
                instrument_id: inst.to_string(),
                bar_time_ms: b.bar_ms,
                trading_day: b.trading_day,
                open: b.open,
                high: b.high,
                low: b.low,
                close: b.close,
                volume: b.volume,
                turnover: b.turnover,
                open_interest: b.open_interest,
                volume_1m: 0,
                turnover_1m: 0.0,
                is_gap: false,
            },
            total_done,
            bar_tx,
        );
    }
}

/// 把一根 bar 送入落盘队列。队列满会【阻塞等】而不是丢——宁可采集慢一拍，也不静默丢行情。
fn send_bar(bar: MinuteBar, total_done: &mut u64, bar_tx: &mpsc::SyncSender<MinuteBar>) {
    // send 在队列满时阻塞（背压）；对端断开（落盘线程已退）时返回 Err，打日志不 panic。
    if bar_tx.send(bar).is_ok() {
        *total_done += 1;
    } else {
        tracing::error!("[AGG] 落盘线程已退出，bar 无处可送（本根丢弃，属异常需排查）");
    }
}

fn upsert_bucket(t: &TickEvent, bar_ms: i64, buckets: &mut HashMap<String, Bucket>) {
    match buckets.get_mut(&t.instrument) {
        Some(b) => {
            if t.last_price > b.high {
                b.high = t.last_price;
            }
            if t.last_price < b.low {
                b.low = t.last_price;
            }
            b.close = t.last_price;
            // 累计量/额/持仓直接用最新值覆盖（CTP 给的是当日累计）。
            // 重连后柜台若重发更小值（极少见），取 max 防止回退。
            if t.volume > b.volume {
                b.volume = t.volume;
            }
            if t.turnover > b.turnover {
                b.turnover = t.turnover;
            }
            b.open_interest = t.open_interest;
            if !t.trading_day.is_empty() {
                b.trading_day = t.trading_day.clone();
            }
        }
        None => {
            buckets.insert(
                t.instrument.clone(),
                Bucket {
                    bar_ms,
                    open: t.last_price,
                    high: t.last_price,
                    low: t.last_price,
                    close: t.last_price,
                    volume: t.volume,
                    turnover: t.turnover,
                    open_interest: t.open_interest,
                    trading_day: t.trading_day.clone(),
                },
            );
        }
    }
}

#[cfg(test)]
mod 测试 {
    use super::*;

    #[test]
    fn floor分钟对齐() {
        assert_eq!(floor_minute(61_234), 60_000);
        assert_eq!(floor_minute(0), 0);
        assert_eq!(floor_minute(119_999), 60_000);
    }
}

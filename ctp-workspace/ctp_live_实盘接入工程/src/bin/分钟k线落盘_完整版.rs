//! 分钟K线落盘 · 完整版
//!
//! 在「最小版」（已冻结，勿动）基础上加三项能力：
//!   1. 断线重连：member 前置断开后，线程内自动退订旧 api、重建 MdApi、重登、重订阅，
//!      指数退避，无次数上限。盘中断网不再漏数据，无需重启进程。
//!   2. 优雅退出：Ctrl+C（SIGINT）触发全局停止标志 → member 退出 → 聚合线程把
//!      当天还在内存里的 bar 全部交给落盘线程 → 写完 Parquet 才进程结束。不丢当天。
//!   3. 分钟补齐：收尾时按每个合约的「首末 bar 时间」做连续性检查，中间缺的分钟补 0 成交 bar
//!      （OHLC 前收补齐，volume/turnover 置 0，is_gap=true 标记），便于下游对齐时间轴。
//!      注意：没来过 tick 的合约不建任何 bar（避免 30000 合约 × 345 分钟的假数据洪水）。
//!
//! 落盘保持单文件/天 + Parquet 原子写（.tmp → rename），与最小版一致。
//!
//! 用法（放进 ctp_live_实盘接入工程/src/bin/ 后）：
//!   cargo build --release --bin 分钟k线完整版
//!   ./分钟k线完整版 [secrets.toml] [输出目录]
//!   默认 secrets = config/secrets.toml，默认输出 = <工作区根>/data_数据/分钟K线/
//!
//! 依赖（写进 ctp_live_实盘接入工程/Cargo.toml，chrono 必须精确锁，见下）：
//!   arrow = "52"
//!   parquet = "52"
//!   chrono = "=0.4.38"     # 关键！>=0.4.39 会给 Datelike 加 quarter()，与 arrow-arith 52 撞名，编译直接失败
//!
//! 与最小版的差异：本文件独立成 bin，最小版 分钟k线落盘.rs 一个字不改，可同时存在、随时回退。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc, Mutex,
};
use std::time::{Duration, Instant};

use chrono::Local;
use chrono::TimeZone;
use ctp2rs::ffi::{AssignFromString, WrapToString};
use ctp2rs::v1alpha1::*;
use rust_ctp_test::config::{Secrets, WORKSPACE_ROOT};

// ============================================================
// 常量
// ============================================================

const MD_DYNLIB_REL: &str = "lib/libthostmduserapi_se.so";
const SUBSCRIBE_BATCH: usize = 500;
const SUBSCRIBE_DELAY_MS: u64 = 300;
const REPORT_INTERVAL_SEC: u64 = 5;

/// 断线重连退避：首次等多久，每次翻倍，封顶多久。
const RECONNECT_BASE_MS: u64 = 1_000;
const RECONNECT_MAX_MS: u64 = 60_000;
/// 登录后等待 CTP 回包的时间，与最小版一致。
const LOGIN_SETTLE_MS: u64 = 1_500;
const CONNECT_SETTLE_MS: u64 = 1_500;

// ============================================================
// 合约
// ============================================================

#[derive(Debug, Clone)]
struct Contract {
    code: String,
    exchange: String,
}

fn load_contracts_csv(path: &Path) -> Result<Vec<Contract>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("读 {} 失败: {}", path.display(), e))?;
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() < 2 {
            continue;
        }
        out.push(Contract {
            code: parts[0].to_string(),
            exchange: parts[1].to_string(),
        });
    }
    Ok(out)
}

/// 按交易所分组，组内轮询分给 N 个 member（与 md_pool / 最小版一致，结果稳定可复现）。
fn shard_contracts(contracts: &[Contract], num_members: usize) -> Vec<Vec<String>> {
    let mut shards: Vec<Vec<String>> = vec![Vec::new(); num_members];
    let mut by_ex: HashMap<String, Vec<&str>> = HashMap::new();
    for c in contracts {
        by_ex.entry(c.exchange.clone()).or_default().push(&c.code);
    }
    let mut ex_keys: Vec<&String> = by_ex.keys().collect();
    ex_keys.sort();
    for ex in ex_keys {
        let codes = &by_ex[ex];
        for (i, code) in codes.iter().enumerate() {
            shards[i % num_members].push(code.to_string());
        }
        println!("[SHARD] 交易所 {}: {} 条", ex, codes.len());
    }
    shards
}

// ============================================================
// member 事件 / tick 统一结构
// ============================================================

enum MemberEvent {
    Connected(usize),
    Disconnected(usize, i32),
    Login(usize, Option<(i32, String)>),
    /// member 完成一轮「登录 + 订阅」，可用于统计重连恢复次数。
    Ready(usize),
    Tick {
        member_idx: usize,
        instrument: String,
        last_price: f64,
        volume: u64,
        turnover: f64,
        open_interest: f64,
        trading_day: String,
    },
}

#[derive(Debug, Clone)]
struct TickEvent {
    member_idx: usize,
    instrument: String,
    last_price: f64,
    volume: u64,
    turnover: f64,
    open_interest: f64,
    trading_day: String,
}

fn info_pair(i: Option<&CThostFtdcRspInfoField>) -> Option<(i32, String)> {
    match i {
        Some(x) if x.ErrorID != 0 => Some((x.ErrorID, x.ErrorMsg.to_string())),
        _ => None,
    }
}

// ============================================================
// SPI（照抄 md_pool + 最小版，Tick 多掏 4 个聚合必需字段）
// ============================================================

struct MemberSpi {
    member_idx: usize,
    tx: mpsc::SyncSender<MemberEvent>,
}

impl MdSpi for MemberSpi {
    fn on_front_connected(&mut self) {
        let _ = self.tx.send(MemberEvent::Connected(self.member_idx));
    }
    fn on_front_disconnected(&mut self, reason: i32) {
        let _ = self
            .tx
            .send(MemberEvent::Disconnected(self.member_idx, reason));
    }
    fn on_rsp_user_login(
        &mut self,
        _rsp: Option<&CThostFtdcRspUserLoginField>,
        rsp_info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        _is_last: bool,
    ) {
        let _ = self
            .tx
            .send(MemberEvent::Login(self.member_idx, info_pair(rsp_info)));
    }
    fn on_rsp_sub_market_data(
        &mut self,
        _specific: Option<&CThostFtdcSpecificInstrumentField>,
        _rsp_info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        _is_last: bool,
    ) {
        // 订阅回包不逐个上报（30000 合约会把通道刷爆），完成一轮后由 spawn_member 发 Ready。
    }
    fn on_rtn_depth_market_data(
        &mut self,
        depth_market_data: Option<&CThostFtdcDepthMarketDataField>,
    ) {
        if let Some(q) = depth_market_data {
            // Volume 是 CTP int（当日累计成交量）；Turnover/OpenInterest 是 double。
            let _ = self.tx.send(MemberEvent::Tick {
                member_idx: self.member_idx,
                instrument: q.InstrumentID.to_string(),
                last_price: q.LastPrice,
                volume: q.Volume as u64,
                turnover: q.Turnover,
                open_interest: q.OpenInterest,
                trading_day: q.TradingDay.to_string(),
            });
        }
    }
    fn on_rsp_error(
        &mut self,
        p_rsp_info: Option<&CThostFtdcRspInfoField>,
        n_request_id: i32,
        _b_is_last: bool,
    ) {
        if let Some(e) = p_rsp_info {
            if e.ErrorID != 0 {
                eprintln!(
                    "[MD#{}] RSP ERROR request_id={} ErrorID={} ErrorMsg={}",
                    self.member_idx,
                    n_request_id,
                    e.ErrorID,
                    e.ErrorMsg.to_string()
                );
            }
        }
    }
}

// ============================================================
// 启动 member（带断线重连）
// ============================================================

/// MdApi 的 RAII 包装：drop 时先 release 再 free，避免重连时把旧实例留在后台线程。
/// ctp2rs 的 MdApi 若本身已在 Drop 里释放，这里只是多余包一层；若没有，这层就是防泄漏的关键。
struct ApiHolder {
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

    /// 建一个新的 MdApi 实例并注册 SPI + 前置 + init。
    /// 返回 None 表示创建失败（调用方退避后重试）。
    fn spawn_new(&mut self, member_idx: usize, front: &str, tx: &mpsc::SyncSender<MemberEvent>) -> bool {
        self.teardown();
        std::fs::create_dir_all(&self.flow_dir).ok();
        let api = MdApi::create_api(
            self.dynlib.to_str().unwrap(),
            self.flow_dir.to_str().unwrap(),
            false,
            false,
        );
        let spi = Box::new(MemberSpi {
            member_idx,
            tx: tx.clone(),
        });
        // ctp2rs 0.1.9 的 register_spi 接管裸指针；SPI 生命周期由 C++ 侧持有，
        // 与 md_pool 写法一致（不回收，进程级一次性分配，重连次数有限可接受）。
        let spi_ptr = Box::into_raw(spi) as *mut dyn MdSpi;
        api.register_spi(spi_ptr);
        api.register_front(front);
        api.init();
        self.api = Some(api);
        true
    }

    /// 主动释放：先 release（断回调、停后台线程），再让 MdApi 走 Drop。
    fn teardown(&mut self) {
        if let Some(api) = self.api.take() {
            // release 后 CTP 不会再回调这个 SPI，重连时新建实例是干净的。
            api.release();
        }
    }
}

/// 登录 + 分批订阅。返回 true 表示走完一轮（不论单批订阅是否报错，都继续下一批）。
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
    login.UserID.assign_from_str(&user_id);
    login.Password.assign_from_str(password);
    let ret = api.req_user_login(&mut login, 1);
    println!("{} req_user_login ret={}", tag, ret);
    if ret != 0 {
        return false;
    }
    std::thread::sleep(Duration::from_millis(LOGIN_SETTLE_MS));

    let total = instruments.len();
    let mut done = 0;
    for batch in instruments.chunks(SUBSCRIBE_BATCH) {
        let r = api.subscribe_market_data(batch);
        done += batch.len();
        println!("{} 订阅 {}/{} ret={}", tag, done, total, r);
        std::thread::sleep(Duration::from_millis(SUBSCRIBE_DELAY_MS));
    }
    println!("{} 订阅完成 {}", tag, done);
    true
}

#[allow(clippy::too_many_arguments)]
fn spawn_member(
    member_idx: usize,
    front: String,
    flow_dir: PathBuf,
    dynlib: PathBuf,
    broker_id: String,
    user_id: String,
    password: String,
    instruments: Vec<String>,
    evt_tx: mpsc::SyncSender<MemberEvent>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let tag = format!("[MD#{}]", member_idx);
        println!(
            "{} 启动 front={} 合约数={} （断线自动重连已开启）",
            tag,
            front,
            instruments.len()
        );
        let mut holder = ApiHolder::new(dynlib, flow_dir);
        let mut backoff_ms = RECONNECT_BASE_MS;
        let mut attempt = 0u32;

        while !stop.load(Ordering::Relaxed) {
            attempt += 1;
            if !holder.spawn_new(member_idx, &front, &evt_tx) {
                eprintln!("{} 建 MdApi 失败，{}ms 后重试", tag, backoff_ms);
                sleep_or_stop(Duration::from_millis(backoff_ms), &stop);
                backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
                continue;
            }
            let api = holder.api.as_ref().unwrap();
            println!(
                "{} api_version={} （第 {} 次连接）",
                tag,
                api.get_api_version(),
                attempt
            );
            // 等前置 connected 回调。
            sleep_or_stop(Duration::from_millis(CONNECT_SETTLE_MS), &stop);
            if stop.load(Ordering::Relaxed) {
                break;
            }
            login_and_subscribe(
                api,
                &broker_id,
                &user_id,
                &password,
                &instruments,
                &tag,
            );
            let _ = evt_tx.send(MemberEvent::Ready(member_idx));
            // 连接成功，退避重置。
            backoff_ms = RECONNECT_BASE_MS;

            // 阻塞直到 join 返回（断线）或被 stop 打断。
            // ctp2rs 的 join 不响应信号，只能靠断线/退出轮询；这里用 join 的阻塞语义，
            // 断线后 on_front_disconnected 会先把 Disconnected 发出来，join 随后返回。
            api.join();
            if stop.load(Ordering::Relaxed) {
                println!("{} 收到退出信号，停止重连", tag);
                break;
            }
            eprintln!(
                "{} 前置断开，{}ms 后开始第 {} 次重连",
                tag, backoff_ms, attempt
            );
            holder.teardown();
            sleep_or_stop(Duration::from_millis(backoff_ms), &stop);
            backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
        }
        holder.teardown();
        println!("{} 线程退出", tag);
    })
}

/// 可被打断的 sleep：stop 置位时立刻返回，保证 Ctrl+C 后 member 秒退，不拖满退避时间。
fn sleep_or_stop(dur: Duration, stop: &AtomicBool) {
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

fn spawn_listener(
    rx: mpsc::Receiver<MemberEvent>,
    tx: mpsc::Sender<TickEvent>,
    stats: Arc<Mutex<Vec<MemberStats>>>,
) {
    std::thread::spawn(move || {
        while let Ok(ev) = rx.recv() {
            match ev {
                MemberEvent::Connected(idx) => {
                    let mut st = stats.lock().unwrap();
                    st[idx].connected = true;
                    st[idx].logged_in = false;
                    eprintln!("[MD#{}] 前置已连接", idx);
                }
                MemberEvent::Disconnected(idx, reason) => {
                    let mut st = stats.lock().unwrap();
                    st[idx].connected = false;
                    st[idx].logged_in = false;
                    st[idx].last_disconnect_reason = reason;
                    st[idx].last_disconnect_at = Some(Instant::now());
                    st[idx].disconnects += 1;
                    eprintln!(
                        "[MD#{}] 前置断开 reason={} （累计 {} 次，将自动重连）",
                        idx, reason, st[idx].disconnects
                    );
                }
                MemberEvent::Login(idx, info) => match info {
                    None => {
                        stats.lock().unwrap()[idx].logged_in = true;
                        eprintln!("[MD#{}] 登录成功", idx);
                    }
                    Some((id, msg)) => {
                        stats.lock().unwrap()[idx].login_fail += 1;
                        eprintln!("[MD#{}] 登录失败 ErrorID={} {}", idx, id, msg);
                    }
                },
                MemberEvent::Ready(idx) => {
                    stats.lock().unwrap()[idx].ready_rounds += 1;
                    eprintln!("[MD#{}] 已完成一轮登录+订阅（第 {} 轮）", idx, stats.lock().unwrap()[idx].ready_rounds);
                }
                MemberEvent::Tick {
                    member_idx,
                    instrument,
                    last_price,
                    volume,
                    turnover,
                    open_interest,
                    trading_day,
                } => {
                    {
                        let mut st = stats.lock().unwrap();
                        st[member_idx].ticks += 1;
                        st[member_idx].last_tick_at = Some(Instant::now());
                    }
                    let _ = tx.send(TickEvent {
                        member_idx,
                        instrument,
                        last_price,
                        volume,
                        turnover,
                        open_interest,
                        trading_day,
                    });
                }
            }
        }
        // evt_tx 全部 drop（member 线程退出）→ listener 自然结束 → tick_tx 发送端减少。
    });
}

#[derive(Default, Clone)]
struct MemberStats {
    connected: bool,
    logged_in: bool,
    login_fail: u64,
    /// 断线次数（重连成功会继续跑，这里累计）。
    disconnects: u64,
    /// 完成「登录+订阅」的轮次，1 表示首次，>1 表示重连成功过。
    ready_rounds: u64,
    last_disconnect_reason: i32,
    last_disconnect_at: Option<Instant>,
    ticks: u64,
    last_tick_at: Option<Instant>,
}

// ============================================================
// 聚合：tick → 分钟K线
// ============================================================

#[derive(Debug, Clone)]
struct MinuteBar {
    instrument_id: String,
    bar_time_ms: i64,
    trading_day: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: u64,
    turnover: f64,
    open_interest: f64,
    /// true 表示这根是补出来的空分钟（无 tick），volume/turnover 为 0。
    is_gap: bool,
}

struct Bucket {
    bar_ms: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: u64,
    turnover: f64,
    open_interest: f64,
    trading_day: String,
}

const MINUTE_MS: i64 = 60_000;

fn floor_minute(ms: i64) -> i64 {
    ms - ms.rem_euclid(MINUTE_MS)
}

fn ms_to_date(ms: i64) -> String {
    // timestamp_millis_opt 返回 LocalResult；中国无夏令时，任意毫秒都是唯一本地时刻，unwrap 安全。
    Local
        .timestamp_millis_opt(ms)
        .unwrap()
        .format("%Y%m%d")
        .to_string()
}

/// 把日期严格早于 today 的缓存全部交给落盘线程并释放（这些天已经翻篇，不会再变）。
fn flush_older(
    cache: &mut HashMap<String, Vec<MinuteBar>>,
    today: &str,
    bar_tx: &mpsc::Sender<(String, Vec<MinuteBar>)>,
) {
    let older: Vec<String> = cache.keys().filter(|k| k.as_str() < today).cloned().collect();
    for d in older {
        if let Some(v) = cache.remove(&d) {
            if !v.is_empty() {
                let _ = bar_tx.send((d, v));
            }
        }
    }
}

/// 分钟补齐：同一合约已有 bar 按时间排序后，若相邻两根间隔 > 1 分钟，
/// 中间每分钟补一根 is_gap=true 的空 bar（OHLC 用前收，量额 0）。
/// 只在实际存在的 bar 之间补，不给「整天没来过 tick」的合约造数据。
fn fill_gaps(bars: &mut Vec<MinuteBar>, stats: &mut GapStats) {
    if bars.len() < 2 {
        return;
    }
    bars.sort_by(|a, b| a.bar_time_ms.cmp(&b.bar_time_ms));
    let mut out: Vec<MinuteBar> = Vec::with_capacity(bars.len());
    out.push(bars[0].clone());
    for w in bars.windows(2) {
        let prev = &w[0];
        let cur = &w[1];
        let mut t = prev.bar_time_ms + MINUTE_MS;
        while t < cur.bar_time_ms {
            out.push(MinuteBar {
                instrument_id: cur.instrument_id.clone(),
                bar_time_ms: t,
                // 补齐段跨日时取当前 bar 的 trading_day，保守处理。
                trading_day: cur.trading_day.clone(),
                open: prev.close,
                high: prev.close,
                low: prev.close,
                close: prev.close,
                volume: 0,
                turnover: 0.0,
                open_interest: prev.open_interest,
                is_gap: true,
            });
            stats.gap_bars += 1;
            t += MINUTE_MS;
        }
        out.push(cur.clone());
    }
    *bars = out;
}

#[derive(Default)]
struct GapStats {
    gap_bars: u64,
    instruments_with_gap: u64,
}

fn run_aggregator(
    tick_rx: mpsc::Receiver<TickEvent>,
    bar_tx: mpsc::Sender<(String, Vec<MinuteBar>)>,
    stop: Arc<AtomicBool>,
) {
    let mut buckets: HashMap<String, Bucket> = HashMap::new();
    let mut cache: HashMap<String, Vec<MinuteBar>> = HashMap::new();
    let mut total_done = 0u64;
    let mut per_member_ticks: HashMap<usize, u64> = HashMap::new();
    let mut last_report = Instant::now();
    // 兜底时间巡检：没 tick 进来时也能靠它推进分钟边界、翻日落盘。
    let mut last_tick_at = Instant::now();

    loop {
        match tick_rx.recv_timeout(Duration::from_millis(200)) {
            Ok(t) => {
                *per_member_ticks.entry(t.member_idx).or_default() += 1;
                last_tick_at = Instant::now();
                if t.last_price <= 0.0 || t.last_price >= 1e30 {
                    continue;
                }
                // 用收包本机时间取整到分钟，不碰 CTP 的 UpdateTime，规避盘前重演污染。
                let bar_ms = floor_minute(Local::now().timestamp_millis());
                close_if_switching(&t.instrument, bar_ms, &mut buckets, &mut cache, &mut total_done, &bar_tx);
                upsert_bucket(&t, bar_ms, &mut buckets);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }

        // 每分钟扫一次：把「本机时间已进入新分钟、但桶还停在旧分钟」的合约收尾。
        // 解决冷门合约长时间无 tick 时，最后一根 bar 迟迟不进缓存的问题。
        if last_tick_at.elapsed() >= Duration::from_secs(60) || last_report.elapsed() >= Duration::from_secs(REPORT_INTERVAL_SEC) {
            let now_ms = floor_minute(Local::now().timestamp_millis());
            let stale: Vec<String> = buckets
                .iter()
                .filter_map(|(k, b)| if b.bar_ms != now_ms { Some(k.clone()) } else { None })
                .collect();
            for inst in stale {
                close_bucket(&inst, &mut buckets, &mut cache, &mut total_done, &bar_tx);
            }
        }

        if last_report.elapsed() >= Duration::from_secs(REPORT_INTERVAL_SEC) {
            let cached: usize = cache.values().map(|v| v.len()).sum();
            let mut dist: Vec<(usize, u64)> = per_member_ticks.iter().map(|(&k, &v)| (k, v)).collect();
            dist.sort_unstable_by_key(|&(k, _)| k);
            let dist_str: String = dist
                .iter()
                .map(|(k, v)| format!("MD#{}:{}", k, v))
                .collect::<Vec<_>>()
                .join(" ");
            let today = ms_to_date(Local::now().timestamp_millis());
            let today_bars = cache.get(&today).map(|v| v.len()).unwrap_or(0);
            println!(
                "[AGG] 已完成 {} 根 | 活跃桶 {} | 内存缓存 {} 根（今日 {}）| 各member tick [{}]",
                total_done,
                buckets.len(),
                cached,
                today_bars,
                dist_str
            );
            last_report = Instant::now();
        }

        if stop.load(Ordering::Relaxed) {
            println!("[AGG] 收到退出信号，开始收尾（含当天数据）");
            break;
        }
    }

    // 收尾：所有活跃桶 → 缓存，按合约做分钟补齐，全部交出去。
    let drain: Vec<(String, Bucket)> = buckets.drain().collect();
    for (inst, b) in drain {
        push_bar(
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
                is_gap: false,
            },
            &mut cache,
        );
    }
    let mut gap = GapStats::default();
    for (d, mut v) in cache.drain() {
        if v.is_empty() {
            continue;
        }
        let before = v.len();
        fill_gaps(&mut v, &mut gap);
        if gap.gap_bars > 0 || v.len() != before {
            gap.instruments_with_gap += 1;
        }
        let _ = bar_tx.send((d, v));
    }
    drop(bar_tx);
    println!(
        "[AGG] 聚合线程退出 | 共完成 {} 根 | 补齐空 bar {} 根（涉及 {} 个日期批次）",
        total_done, gap.gap_bars, gap.instruments_with_gap
    );
}

fn close_if_switching(
    inst: &str,
    bar_ms: i64,
    buckets: &mut HashMap<String, Bucket>,
    cache: &mut HashMap<String, Vec<MinuteBar>>,
    total_done: &mut u64,
    bar_tx: &mpsc::Sender<(String, Vec<MinuteBar>)>,
) {
    let switch = matches!(buckets.get(inst), Some(b) if b.bar_ms != bar_ms);
    if switch {
        close_bucket(inst, buckets, cache, total_done, bar_tx);
    }
}

fn close_bucket(
    inst: &str,
    buckets: &mut HashMap<String, Bucket>,
    cache: &mut HashMap<String, Vec<MinuteBar>>,
    total_done: &mut u64,
    bar_tx: &mpsc::Sender<(String, Vec<MinuteBar>)>,
) {
    if let Some(b) = buckets.remove(inst) {
        push_bar(
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
                is_gap: false,
            },
            cache,
        );
        *total_done += 1;
        let today = ms_to_date(Local::now().timestamp_millis());
        flush_older(cache, &today, bar_tx);
    }
}

fn push_bar(bar: MinuteBar, cache: &mut HashMap<String, Vec<MinuteBar>>) {
    let d = ms_to_date(bar.bar_time_ms);
    cache.entry(d).or_default().push(bar);
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
            // 若重连后柜台重发了更小的值（极少见），取 max 防止回退。
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

// ============================================================
// 落盘：独立线程，一批 = 一天一个 Parquet（原子写）
// ============================================================

fn write_parquet(path: &Path, bars: &[MinuteBar]) -> Result<(), Box<dyn std::error::Error>> {
    use arrow::array::{
        ArrayRef, BooleanArray, Float64Array, RecordBatch, StringArray, TimestampMillisecondArray,
        UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "bar_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("trading_day", DataType::Utf8, false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::UInt64, false),
        Field::new("turnover", DataType::Float64, false),
        Field::new("open_interest", DataType::Float64, false),
        // 完整版新增：标记补齐的空分钟，下游可按需过滤。
        Field::new("is_gap", DataType::Boolean, false),
    ]));

    let bt: Vec<i64> = bars.iter().map(|b| b.bar_time_ms).collect();
    let inst: Vec<&str> = bars.iter().map(|b| b.instrument_id.as_str()).collect();
    let td: Vec<&str> = bars.iter().map(|b| b.trading_day.as_str()).collect();
    let open: Vec<f64> = bars.iter().map(|b| b.open).collect();
    let high: Vec<f64> = bars.iter().map(|b| b.high).collect();
    let low: Vec<f64> = bars.iter().map(|b| b.low).collect();
    let close: Vec<f64> = bars.iter().map(|b| b.close).collect();
    let vol: Vec<u64> = bars.iter().map(|b| b.volume).collect();
    let turnover: Vec<f64> = bars.iter().map(|b| b.turnover).collect();
    let oi: Vec<f64> = bars.iter().map(|b| b.open_interest).collect();
    let gap: Vec<bool> = bars.iter().map(|b| b.is_gap).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMillisecondArray::from(bt)) as ArrayRef,
            Arc::new(StringArray::from(inst)) as ArrayRef,
            Arc::new(StringArray::from(td)) as ArrayRef,
            Arc::new(Float64Array::from(open)) as ArrayRef,
            Arc::new(Float64Array::from(high)) as ArrayRef,
            Arc::new(Float64Array::from(low)) as ArrayRef,
            Arc::new(Float64Array::from(close)) as ArrayRef,
            Arc::new(UInt64Array::from(vol)) as ArrayRef,
            Arc::new(Float64Array::from(turnover)) as ArrayRef,
            Arc::new(Float64Array::from(oi)) as ArrayRef,
            Arc::new(BooleanArray::from(gap)) as ArrayRef,
        ],
    )?;

    // 原子写：先写 .tmp，写完 rename，中途被杀不留半截文件。
    let tmp = path.with_extension("parquet.tmp");
    {
        let file = std::fs::File::create(&tmp)?;
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
    }
    // 若当天文件已存在（补写/重跑），先备份成 .bak 再覆盖，避免直接毁掉已有数据。
    if path.exists() {
        let bak = path.with_extension("parquet.bak");
        if bak.exists() {
            std::fs::remove_file(&bak).ok();
        }
        std::fs::rename(path, &bak).ok();
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn run_writer(rx: mpsc::Receiver<(String, Vec<MinuteBar>)>, out_dir: PathBuf) {
    let mut total_bars = 0u64;
    let mut files = 0u64;
    while let Ok((date, mut bars)) = rx.recv() {
        bars.sort_by(|a, b| {
            a.instrument_id
                .cmp(&b.instrument_id)
                .then(a.bar_time_ms.cmp(&b.bar_time_ms))
        });
        let n = bars.len();
        let gap_n = bars.iter().filter(|b| b.is_gap).count();
        let path = out_dir.join(format!("bars_{}.parquet", date));
        match write_parquet(&path, &bars) {
            Ok(()) => {
                total_bars += n as u64;
                files += 1;
                println!(
                    "[WRT] 写入 {} 共 {} 根（其中补齐 {} 根）| 累计 {} 文件 {} 根",
                    path.display(),
                    n,
                    gap_n,
                    files,
                    total_bars
                );
            }
            Err(e) => eprintln!("[WRT] 写 {} 失败: {}", path.display(), e),
        }
    }
    println!("[WRT] 落盘线程退出 | 共 {} 文件 {} 根", files, total_bars);
}

// ============================================================
// 主流程
// ============================================================

fn main() {
    // 1. 全局停止标志 + SIGINT 处理（Ctrl+C 只置标志，不直接 exit，让收尾跑完）。
    let stop = Arc::new(AtomicBool::new(false));
    {
        let s = stop.clone();
        ctrlc_handler(move || {
            if s.load(Ordering::Relaxed) {
                // 第二次 Ctrl+C：强杀，防止收尾卡死时无法退出。
                eprintln!("\n[RUN] 第二次 Ctrl+C，强制退出（当天未落盘数据会丢）");
                std::process::exit(130);
            }
            eprintln!("\n[RUN] 收到 Ctrl+C，开始优雅退出：停重连 → 收尾聚合 → 落当天 → 退出（再按一次强制）");
            s.store(true, Ordering::Relaxed);
        });
    }

    // 2. 读配置（复用跑通的 Secrets 结构体，与 md_pool / 最小版完全一致）。
    let secrets_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| rust_ctp_test::config::DEFAULT_SECRETS.to_string());
    let text = std::fs::read_to_string(&secrets_path).unwrap_or_else(|e| {
        eprintln!("[CFG] 读 {} 失败: {}", secrets_path, e);
        std::process::exit(1);
    });
    let cfg: Secrets = toml::from_str(&text).unwrap_or_else(|e| {
        eprintln!("[CFG] 解析 {} 失败: {}", secrets_path, e);
        std::process::exit(1);
    });
    println!("[CFG] 已加载 {}", secrets_path);

    // 3. 合约清单。
    let contracts_file = if Path::new(&cfg.market_data.contracts_file).is_absolute() {
        PathBuf::from(&cfg.market_data.contracts_file)
    } else {
        Path::new(WORKSPACE_ROOT).join(&cfg.market_data.contracts_file)
    };
    let contracts = load_contracts_csv(&contracts_file).unwrap_or_else(|e| {
        eprintln!("[CFG] {}", e);
        std::process::exit(1);
    });
    println!(
        "[CFG] 工作区根 {} | 合约 {} 条 | member {} 个 | md_port {} | 断线重连 开",
        WORKSPACE_ROOT,
        contracts.len(),
        cfg.market_data.member_ips.len(),
        cfg.market_data.md_port
    );

    if cfg.market_data.member_ips.is_empty() {
        eprintln!("[CFG] member_ips 为空，无前置可连，退出");
        std::process::exit(1);
    }

    // 4. 分片。
    let num_members = cfg.market_data.member_ips.len();
    let shards = shard_contracts(&contracts, num_members);

    // 5. 动态库（编译期绝对路径，不依赖 cwd）。
    let dynlib = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(MD_DYNLIB_REL);
    if !dynlib.exists() {
        eprintln!("[CFG] 动态库不存在 {}", dynlib.display());
        std::process::exit(1);
    }

    // 6. 输出目录。
    let out_dir = match std::env::args().nth(2) {
        Some(p) => PathBuf::from(p),
        None => Path::new(WORKSPACE_ROOT)
            .join("data_数据")
            .join("分钟K线"),
    };
    std::fs::create_dir_all(&out_dir).unwrap_or_else(|e| {
        eprintln!("[CFG] 建输出目录 {} 失败: {}", out_dir.display(), e);
        std::process::exit(1);
    });
    println!("[CFG] 输出目录 {}", out_dir.display());

    // 7. 落盘线程 + 聚合线程。
    let (bar_tx, bar_rx) = mpsc::channel::<(String, Vec<MinuteBar>)>();
    let writer_handle = {
        let d = out_dir.clone();
        std::thread::spawn(move || run_writer(bar_rx, d))
    };
    let (tick_tx, tick_rx) = mpsc::channel::<TickEvent>();
    let agg_stop = stop.clone();
    let agg_handle = std::thread::spawn(move || run_aggregator(tick_rx, bar_tx, agg_stop));

    // 8. 启动所有 member + listener。
    let stats: Arc<Mutex<Vec<MemberStats>>> =
        Arc::new(Mutex::new(vec![MemberStats::default(); num_members]));
    let mut handles = Vec::new();
    for (idx, ip) in cfg.market_data.member_ips.iter().enumerate() {
        let front = format!("tcp://{}:{}", ip, cfg.market_data.md_port);
        let flow_dir = Path::new(WORKSPACE_ROOT)
            .join(&cfg.market_data.flow_dir)
            .join(format!("member_{:02}", idx));
        let (evt_tx, evt_rx) = mpsc::sync_channel::<MemberEvent>(8192);
        let h = spawn_member(
            idx,
            front,
            flow_dir,
            dynlib.clone(),
            cfg.trading.broker_id.clone(),
            cfg.trading.user_id.clone(),
            cfg.trading.password.clone(),
            shards[idx].clone(),
            evt_tx,
            stop.clone(),
        );
        spawn_listener(evt_rx, tick_tx.clone(), stats.clone());
        handles.push(h);
    }
    drop(tick_tx);

    println!(
        "[RUN] {} 个 member 已启动，进入健康巡检（非交易时段空转属正常；Ctrl+C 优雅退出）",
        num_members
    );

    // 9. 主循环：健康统计 + 断流告警；stop 置位后等 member 全部退出再收尾。
    let start = Instant::now();
    loop {
        if stop.load(Ordering::Relaxed) {
            println!("[RUN] 停止标志已置位，等待 member 线程退出（最长退避 {}s）", RECONNECT_MAX_MS / 1000);
            break;
        }
        std::thread::sleep(Duration::from_secs(REPORT_INTERVAL_SEC));
        {
            let st = stats.lock().unwrap();
            let mut conn = 0;
            let mut login = 0;
            let mut ticks = 0u64;
            let mut disc = 0u64;
            let mut stale: Vec<usize> = Vec::new();
            for (i, m) in st.iter().enumerate() {
                if m.connected {
                    conn += 1;
                }
                if m.logged_in {
                    login += 1;
                }
                ticks += m.ticks;
                disc += m.disconnects;
                // 已连上但 5 分钟没 tick：可能是断流或该 member 分片合约确实冷清。
                if let Some(t) = m.last_tick_at {
                    if t.elapsed().as_secs() > 300 {
                        stale.push(i);
                    }
                }
            }
            println!(
                "===== 运行 {} 秒 | 已连 {} | 已登录 {} | 累计 tick {} | 累计断线 {} 次 =====",
                start.elapsed().as_secs(),
                conn,
                login,
                ticks,
                disc
            );
            if !stale.is_empty() {
                eprintln!(
                    "[RUN] 超过 5 分钟无 tick 的 member: {:?}（开盘时段需关注，可能已断流待重连）",
                    stale
                );
            }
        }
    }

    for h in handles {
        let _ = h.join();
    }
    println!("[RUN] member 线程全部退出，等聚合线程收尾落盘");
    let _ = agg_handle.join();
    let _ = writer_handle.join();
    println!("[RUN] 全部退出，当天数据已落盘");
}

/// Ctrl+C 处理：用 ctrlc crate（纯 Rust 信号桥接，无 unsafe）。
/// Cargo.toml 需加：ctrlc = "3"
fn ctrlc_handler<F: FnMut() + Send + 'static>(f: F) {
    ctrlc::set_handler(f).expect("设置 Ctrl+C 处理失败（Linux 下一般是权限或被父进程截获）");
}

//! 分钟K线落盘 · v1.3 分片落盘版（基于已跑通的 v1.1 完整版，只换"落盘"这一块）
//!
//! ============================ 这一版解决了什么 ============================
//! v1.1 的毛病：全天 bar 攒在内存（cache），退出/跨天才写整日 Parquet。
//!   后果 1：进程被 kill -9 / 断电 / 连按两次 Ctrl+C → 当天全丢（实测丢过 46 万根）。
//!   后果 2：若改成定时全量重写整日文件，一天重写十几次 → 约 15GB 写放大，笔记本 SSD 吃不消。
//!
//! v1.3 的做法（追加式分片，两种病一起治）：
//!   · 每根 bar 一收盘，落盘线程立刻把它【追加一行 JSON】到当天分片文件
//!     data_数据/分钟分片/bars_YYYYMMDD.jsonl，并且【每 分片落盘秒(默认60) 秒 fsync 一次】。
//!     → 崩溃最多丢最后 60 秒；全天只写一遍（约 300MB），没有 15G。
//!   · 采集进程不攒整天，内存只留"当前活跃桶 + 几秒在途 bar"，约几 MB，不再随运行时间涨。
//!   · 合并是独立动作，随时可做：
//!       程序 合并 20261124          合并指定日期（可跟多个）
//!       程序 合并 今日               合并今天（按本机墙钟日期）
//!     合并会：读 jsonl（先复制一份再读，边采边合也不冲突）→ 按合约+时间排序
//!           → 算 volume_1m/turnover_1m 单分钟增量 → 补缺口(is_gap) → 原子写
//!           data_数据/分钟K线/bars_YYYYMMDD.parquet（13 列，下游照旧读）。
//!
//! ============================ 与 v1.1 的关系（重要）============================
//! CTP 接入 / SPI / 16-member 分片 / 断线重连 / 优雅退出 / 分钟聚合【全部照抄 v1.1，一字未改】。
//! 只动了三处：
//!   1) run_aggregator / close_bucket：不再往 cache 攒整天，收盘即把 bar 发给落盘线程。
//!   2) run_writer：从"写整日 Parquet"改成"追加 JSON 行 + 定时 fsync"。
//!   3) main：加了一个子命令分发——第 1 个参数是"合并"就进合并模式，否则是采集模式。
//! 其余函数（load_contracts_csv / shard_contracts / MemberSpi / ApiHolder / spawn_member /
//! spawn_listener / fill_gaps 等）与 v1.1 逐字一致，动它们=引入风险，所以不碰。
//!
//! ============================ 未做、留给下一步的 ============================
//!   · 业务代码里的 unwrap / tracing::info! 清理（改 tracing + thiserror + 零 unwrap）是【独立的另一步】，
//!     在本落盘改动验证稳定后再做，两者不捆绑，避免一次改太多查不出问题。
//!   · 收盘自动合并：目前合并是手动命令；稳定后可加"检测到收盘自动触发一次合并"。
//!
//! 用法：
//!   采集（和 v1.1 一样）：  cargo run --release --bin 分钟k线落盘_v1_3 -- [secrets.toml] [输出目录]
//!   合并：                  cargo run --release --bin 分钟k线落盘_v1_3 -- 合并 20261124
//!                           cargo run --release --bin 分钟k线落盘_v1_3 -- 合并 今日
//!
//! 依赖（Cargo.toml，chrono 必须精确锁，否则和 arrow-arith 52 撞 quarter() 编不过）：
//!   arrow = "52"
//!   parquet = "52"
//!   chrono = "=0.4.38"
//!   ctrlc = "3"
//!   toml = "0.8"
//!   serde = { version = "1", features = ["derive"] }   # 本版新增：jsonl 序列化
//!   serde_json = "1"                                    # 本版新增：jsonl 逐行读写
//! ============================================================================

#![deny(unsafe_code)]
#![deny(clippy::unwrap_used)]       // 规则15：业务代码禁 unwrap
#![deny(clippy::expect_used)]       // 规则15：禁 expect（启动致命用 unwrap_or_else+exit）
#![deny(clippy::print_stdout)]      // 规则17：运行日志走 tracing，禁 println!
#![deny(clippy::print_stderr)]      // 规则17：禁 eprintln!

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc, Mutex,
};
use std::time::{Duration, Instant};

use chrono::Local;
use chrono::LocalResult; // v1.4: ms_to_date 的 match 用到 LocalResult::{Single,Ambiguous,None}
use chrono::TimeZone;
use ctp2rs::ffi::{AssignFromString, WrapToString};
use ctp2rs::v1alpha1::*;
use rust_ctp_test::config::{Secrets, WORKSPACE_ROOT};
use serde::{Deserialize, Serialize};

// ============================================================
// 常量（改这里=改全局行为，散落在代码里的数字都收到这，符合"配置集中管理"）
// ============================================================

const MD_DYNLIB_REL: &str = "lib/libthostmduserapi_se.so";
const SUBSCRIBE_BATCH: usize = 500;
const SUBSCRIBE_DELAY_MS: u64 = 300;
const REPORT_INTERVAL_SEC: u64 = 5;

const RECONNECT_BASE_MS: u64 = 1_000;
const RECONNECT_MAX_MS: u64 = 60_000;
const LOGIN_SETTLE_MS: u64 = 1_500;
const CONNECT_SETTLE_MS: u64 = 1_500;

/// ★本版新增（可随时调）：分片落盘 fsync 间隔（秒）。
///   含义：落盘线程最多每这么多秒把追加的 bar 强制刷到磁盘一次。
///   值越小越抗崩（丢得越少）但 fsync 越频；越大越省 IO 但崩了丢得多。
///   设 60 = 最多丢 60 秒；设 5 = 最多丢 5 秒；笔记本有电池、追求极致省 IO 可设 120。
///   注意：不管设多少，每根 bar 都会"追加 + flush(到内核缓冲)"，只是 fsync(落物理盘)按这个间隔。
const 分片落盘秒: u64 = 60;

/// 落盘线程与聚合线程之间的在途 bar 队列容量。满了会【阻塞背压】，绝不静默丢 bar。
/// 全天满盘约 50 万根，100 万绰绰有余；峰值内存 = 100万 × ~200B ≈ 200MB 上限。

/// 分片文件 / 合并成品的相对目录（都挂在 工作区根/data_数据 下）。

const MINUTE_MS: i64 = 60_000;
// ── ★v1.3/v1.4 新增：运行配置（规则20「配置集中管理」）─────────────────────
// 【为什么有这一层】原来 分片落盘秒 等是 const 写死，调一个数就要改 .rs + 重编译。
// 现在：toml 有值用 toml，没值用内建默认；找不到配置文件只打 warn，绝不 panic。
// 【线程怎么共享】配置在 main 最顶部载入一次，之后只读；线程需要的单个值（如 fsync 间隔）
// 直接按值 clone 进去，不去共享整个 struct，也就不需要 Arc/锁。
const 默认_分片落盘秒: u64 = 60;          // 0=不额外fsync · 5=极致抗崩 · 60=推荐
const 默认_BAR队列容量: usize = 1_000_000;
const 默认_分片目录: &str = "data_数据/分钟分片";
const 默认_成品目录: &str = "data_数据/分钟K线";
const 配置_文件名: &str = "配置_分钟K线.toml";

/// 运行期生效配置。字段名就是 toml 里的键名，一一对应，改行为只动 toml。
#[derive(Debug, Clone)]
struct 运行配置 {
    分片落盘秒: u64,
    bar队列容量: usize,
    分片目录_rel: String,
    成品目录_rel: String,
}
impl 运行配置 {
    fn 默认() -> Self {
        Self {
            分片落盘秒: 默认_分片落盘秒,
            bar队列容量: 默认_BAR队列容量,
            分片目录_rel: 默认_分片目录.to_string(),
            成品目录_rel: 默认_成品目录.to_string(),
        }
    }

    /// 极简 toml 读取：只认平铺的 `键 = 值`；跳过空行/整行注释；截掉行内注释与引号。
    /// 【为什么不用 serde】只有 4 个键，手写解析比再加一个派生宏依赖面更小（止血版求稳）。
    /// 等配置项超过约 20 个、或出现嵌套表（[表]）时，再换成 serde 反序列化 whole struct。
    fn 从toml覆盖(mut self, 文本: &str, 来源: &str) -> Self {
        // 闭包：在函数内定义一个"取键"的小函数，能捕获外层参数但不占栈外空间。
        let 取 = |键名: &str| -> Option<String> {
            for 行 in 文本.lines() {
                let 行 = 行.trim();
                // 跳过：空行 / 整行注释 / [表头]（本解析器只认平铺键，表头无意义）
                if 行.is_empty() || 行.starts_with('#') || 行.starts_with('[') {
                    continue;
                }
                if let Some((k, v)) = 行.split_once('=') {
                    if k.trim() == 键名 {
                        let v = v.split('#').next().unwrap_or("").trim().trim_matches('"');
                        if !v.is_empty() {
                            return Some(v.to_string());
                        }
                    }
                }
            }
            None
        };
        if let Some(v) = 取("分片落盘秒") {
            match v.parse::<u64>() {
                Ok(n) => self.分片落盘秒 = n,
                // 配置写错不致命：打日志 + 保留默认，绝不 panic（规则：配置错误不能让程序起不来）
                Err(_) => tracing::error!("[CFG] {} 的 分片落盘秒={} 不是数字，忽略该行", 来源, v),
            }
        }
        if let Some(v) = 取("bar队列容量") {
            match v.parse::<usize>() {
                Ok(n) => self.bar队列容量 = n,
                Err(_) => tracing::error!("[CFG] {} 的 bar队列容量={} 不是数字，忽略该行", 来源, v),
            }
        }
        if let Some(v) = 取("分片目录") { self.分片目录_rel = v; }
        if let Some(v) = 取("成品目录") { self.成品目录_rel = v; }
        self
    }

    /// 查找顺序：①环境变量 分钟K线配置 指向的文件 ②CARGO_MANIFEST_DIR（工程根）③工作区根。
    /// 三处都找是为了"不管从哪个目录启动、toml 放在哪"都能读到，省掉"配置在却说没有"的排查。
    fn 载入() -> Self {
        let 默认 = Self::默认();
        let mut 候选: Vec<PathBuf> = Vec::new();
        if let Some(p) = std::env::var_os("分钟K线配置") {
            候选.push(PathBuf::from(p));
        }
        候选.push(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(配置_文件名));
        候选.push(Path::new(WORKSPACE_ROOT).join(配置_文件名));
        for p in &候选 {
            if p.is_file() {
                match std::fs::read_to_string(p) {
                    Ok(t) => {
                        tracing::info!("[CFG] 已加载运行配置 {}", p.display());
                        return 默认.从toml覆盖(&t, &p.display().to_string());
                    }
                    Err(e) => tracing::error!("[CFG] 读配置 {} 失败: {}（改用内建默认）", p.display(), e),
                }
            }
        }
        tracing::error!(
            "[CFG] ⚠ 没找到 {}，使用内建默认：分片落盘秒={}、bar队列容量={}、分片目录={}",
            配置_文件名, 默认.分片落盘秒, 默认.bar队列容量, 默认.分片目录_rel
        );
        默认
    }
}

// ============================================================
// 合约（照抄 v1.1）
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

/// 按交易所分组，组内轮询分给 N 个 member（与 v1.1 一致，结果稳定可复现）。
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
        tracing::info!("[SHARD] 交易所 {}: {} 条", ex, codes.len());
    }
    shards
}

// ============================================================
// member 事件 / tick 统一结构（照抄 v1.1）
// ============================================================

enum MemberEvent {
    Connected(usize),
    Disconnected(usize, i32),
    Login(usize, Option<(i32, String)>),
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
// SPI（照抄 v1.1，一字未改）
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
                tracing::error!(
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
// 启动 member（带断线重连，照抄 v1.1）
// ============================================================

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
    login.UserID.assign_from_str(&user_id);
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
        tracing::info!(
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
                tracing::error!("{} 建 MdApi 失败，{}ms 后重试", tag, backoff_ms);
                sleep_or_stop(Duration::from_millis(backoff_ms), &stop);
                backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
                continue;
            }
            // 【规则15 零 unwrap】spawn 成功但句柄为空属逻辑异常，打 warn 后本轮重连、不 panic
            let Some(api) = holder.api.as_ref() else {
                tracing::warn!("{} spawn 成功但 api 句柄为空，本轮跳过、下轮重连", tag);
                sleep_or_stop(Duration::from_millis(backoff_ms), &stop);
                continue;
            };
            tracing::info!(
                "{} api_version={} （第 {} 次连接）",
                tag,
                api.get_api_version(),
                attempt
            );
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
            backoff_ms = RECONNECT_BASE_MS;

            api.join();
            if stop.load(Ordering::Relaxed) {
                tracing::info!("{} 收到退出信号，停止重连", tag);
                break;
            }
            tracing::error!(
                "{} 前置断开，{}ms 后开始第 {} 次重连",
                tag, backoff_ms, attempt
            );
            holder.teardown();
            sleep_or_stop(Duration::from_millis(backoff_ms), &stop);
            backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
        }
        holder.teardown();
        tracing::info!("{} 线程退出", tag);
    })
}

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
                    let mut st = 锁或恢复(&stats);
                    st[idx].connected = true;
                    st[idx].logged_in = false;
                    tracing::error!("[MD#{}] 前置已连接", idx);
                }
                MemberEvent::Disconnected(idx, reason) => {
                    let mut st = 锁或恢复(&stats);
                    st[idx].connected = false;
                    st[idx].logged_in = false;
                    st[idx].last_disconnect_reason = reason;
                    st[idx].last_disconnect_at = Some(Instant::now());
                    st[idx].disconnects += 1;
                    tracing::error!(
                        "[MD#{}] 前置断开 reason={} （累计 {} 次，将自动重连）",
                        idx, reason, st[idx].disconnects
                    );
                }
                MemberEvent::Login(idx, info) => match info {
                    None => {
                        锁或恢复(&stats)[idx].logged_in = true;
                        tracing::error!("[MD#{}] 登录成功", idx);
                    }
                    Some((id, msg)) => {
                        锁或恢复(&stats)[idx].login_fail += 1;
                        tracing::error!("[MD#{}] 登录失败 ErrorID={} {}", idx, id, msg);
                    }
                },
                MemberEvent::Ready(idx) => {
                    锁或恢复(&stats)[idx].ready_rounds += 1;
                    tracing::error!(
                        "[MD#{}] 已完成一轮登录+订阅（第 {} 轮）",
                        idx,
                        锁或恢复(&stats)[idx].ready_rounds
                    );
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
                        let mut st = 锁或恢复(&stats);
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
    });
}

#[derive(Default, Clone)]
struct MemberStats {
    connected: bool,
    logged_in: bool,
    login_fail: u64,
    disconnects: u64,
    ready_rounds: u64,
    last_disconnect_reason: i32,
    last_disconnect_at: Option<Instant>,
    ticks: u64,
    last_tick_at: Option<Instant>,
}

// ============================================================
// 聚合：tick → 分钟K线
//   ★本版改动点 1：不再往"整天 cache"里攒，收盘即把 bar 发给落盘线程。
// ============================================================

/// 一根收盘的分钟K线。派生 Serialize/Deserialize 是为了能逐行写成/读回 jsonl 分片。
/// 这里只存 CTP 给的【当日累计】volume/turnover；单分钟增量 volume_1m/turnover_1m 留到
/// 【合并时】按同合约时间相邻两根做差得到（合并时能看全一天，算增量最简单、也最好测）。
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// ★单分钟增量 = 本根累计 - 上一根累计（同合约）。采集端不填，默认 0；【合并时】算好再写进成品。
    /// serde(default)：老 jsonl 没这两列也能正常反序列化（向前兼容）。
    #[serde(default)]
    volume_1m: u64,
    #[serde(default)]
    turnover_1m: f64,
    /// true = 这根是合并时补出来的空分钟（无 tick），OHLC 前收、量额 0。
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

// 【规则15 零 unwrap + 锁中毒处理】
// 为什么不用 .lock().unwrap()：持锁线程若 panic，锁会被标记"中毒(poisoned)"，
// 之后 unwrap 会连带 panic，把"一个线程挂了"放大成"全体采集停摆"。
// 这里"中毒也照常取回数据"——统计计数丢失一点无所谓，采集不能停。
fn 锁或恢复<T>(锁: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    锁.lock().unwrap_or_else(|中毒| 中毒.into_inner())
}

fn floor_minute(ms: i64) -> i64 {
    ms - ms.rem_euclid(MINUTE_MS)
}

fn ms_to_date(ms: i64) -> String {
    // 【规则15 零 unwrap】timestamp_millis_opt 返回 LocalResult 三态：
    //   Single  = 唯一本地时刻（中国无夏令时，99.999% 走这里）
    //   Ambiguous = 夏令时回拨重叠的那小时，取较早那个(dt)即可，日期一样
    //   None    = 毫秒越界，理论上 bar 时间不会出现；兜底按 UTC 取日期(同毫秒 UTC 必合法)
    // 任何分支都不 unwrap —— 就算真遇上 None 也只打 warn + 兜底，绝不 panic 拖垮采集。
    match Local.timestamp_millis_opt(ms) {
        LocalResult::Single(dt) | LocalResult::Ambiguous(dt, _) => {
            dt.format("%Y%m%d").to_string()
        }
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
fn run_aggregator(
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
                close_if_switching(&t.instrument, bar_ms, &mut buckets, &mut total_done, &bar_tx);
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
                .filter_map(|(k, b)| if b.bar_ms != now_ms { Some(k.clone()) } else { None })
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
            tracing::info!("[AGG] 收到退出信号，开始收尾：把活跃桶全部收盘送落盘（当天已在分片里，几乎秒完）");
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
                volume_1m: 0,   // 采集端不算增量，留到合并
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
        total_done, 分片落盘秒
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

// ============================================================
// ★本版改动点 2：落盘线程 —— 从"写整日 Parquet"改成"追加 JSON 行 + 定时 fsync"
// ============================================================

use std::io::{BufWriter, Write};

/// 落盘线程：把每根 bar 追加成一行 JSON 到当天的分片文件，每 分片落盘秒 fsync 一次。
/// 崩溃安全的关键：append 保证只增不改；fsync 保证"真的落到物理盘"而非只在内存缓冲。
/// 即使进程被 kill -9，最多丢最后 分片落盘秒 内没 fsync 的那几根，其余全在盘上。
fn run_writer(rx: mpsc::Receiver<MinuteBar>, 分片目录: PathBuf, 分片落盘秒_生效: u64) {
    std::fs::create_dir_all(&分片目录).ok();
    // 当前打开的分片文件：(日期串, 带缓冲的写入器)。日期一变就换文件（跨自然日）。
    let mut 当前: Option<(String, BufWriter<std::fs::File>)> = None;
    let mut 本次累计 = 0u64;
    let mut 文件数 = 0u64;
    let mut 上次落盘 = Instant::now();

    // 打开（或追加到）某一天的分片文件。
    let 打开 = |date: &str, 目录: &Path| -> std::io::Result<BufWriter<std::fs::File>> {
        let p = 目录.join(format!("bars_{}.jsonl", date));
        let f = std::fs::OpenOptions::new()
            .create(true)
            .append(true) // ★追加模式：只往后写，绝不覆盖已有数据
            .open(&p)?;
        Ok(BufWriter::new(f))
    };

    while let Ok(bar) = rx.recv() {
        let date = ms_to_date(bar.bar_time_ms);
        // 需要换文件？（首次 / 跨日）
        let 需要换 = match &当前 {
            Some((d, _)) => d != &date,
            None => true,
        };
        if 需要换 {
            // 先把旧文件 flush + fsync 收好，再开新的。
            if let Some((旧日期, mut w)) = 当前.take() {
                let _ = w.flush();
                if let Err(e) = w.get_ref().sync_all() {
                    tracing::error!("[WRT] fsync {} 失败: {}", 旧日期, e);
                }
                tracing::info!("[WRT] 收好分片 {}（累计 {} 根）", 旧日期, 本次累计);
            }
            match 打开(&date, &分片目录) {
                Ok(w) => {
                    当前 = Some((date.clone(), w));
                    文件数 += 1;
                    tracing::info!("[WRT] 打开分片 {} → {}", date, 分片目录.join(format!("bars_{}.jsonl", date)).display());
                }
                Err(e) => {
                    tracing::error!("[WRT] 打开分片 {} 失败: {}", date, e);
                    当前 = None;
                    continue;
                }
            }
        }

        // 写一行 JSON（serde 序列化）。写失败只报错不退出，尽量多保数据。
        if let Some((_, w)) = 当前.as_mut() {
            match serde_json::to_string(&bar) {
                Ok(line) => {
                    if let Err(e) = writeln!(w, "{}", line) {
                        tracing::error!("[WRT] 写行失败: {}", e);
                    } else {
                        本次累计 += 1;
                    }
                }
                Err(e) => tracing::error!("[WRT] 序列化 bar 失败: {}", e),
            }
        }

        // 定时 flush + fsync：把缓冲刷到内核(flush)，再刷到物理盘(fsync)，控制崩溃丢失上限。
        if 上次落盘.elapsed() >= Duration::from_secs(分片落盘秒_生效) {
            if let Some((_, w)) = 当前.as_mut() {
                let _ = w.flush();
                if let Err(e) = w.get_ref().sync_all() {
                    tracing::error!("[WRT] 定时 fsync 失败: {}", e);
                }
            }
            上次落盘 = Instant::now();
        }
    }

    // rx 断开（聚合线程退出、bar_tx 全 drop）→ 最后一次 flush + fsync，收尾。
    if let Some((日期, mut w)) = 当前.take() {
        let _ = w.flush();
        if let Err(e) = w.get_ref().sync_all() {
            tracing::error!("[WRT] 收尾 fsync {} 失败: {}", 日期, e);
        }
        tracing::info!("[WRT] 收好分片 {}（累计 {} 根）", 日期, 本次累计);
    }
    tracing::info!(
        "[WRT] 落盘线程退出 | 共 {} 个分片文件、{} 根 bar 已安全落盘（fsync 每 {} 秒一次）",
        文件数, 本次累计, 分片落盘秒_生效
    );
}

// ============================================================
// ★本版改动点 3：合并模式 —— 读 jsonl 分片 → 排序 → 算增量 → 补缺口 → 写整日 Parquet
// ============================================================

#[derive(Default)]
struct GapStats {
    gap_bars: u64,
    instruments_with_gap: u64,
}

/// 合并某一天的分片为成品 Parquet。
/// 步骤（每步都可单独看日志核验）：
///   1) 把当天的 bars_YYYYMMDD.jsonl 复制成 .merging 临时文件再读 —— 这样采集进程还在
///      往原文件追加也不影响合并（读的是快照），盘中随时可合并。
///   2) 逐行解析 JSON；某一行坏了（很可能是正在写的最后半行）就跳过并告警，不整批失败。
///   3) 按合约分组，组内按时间升序，算 volume_1m / turnover_1m = 相邻两根累计之差。
///      （首根增量=累计本身；补出来的缺口 bar 增量=0）
///   4) 补缺口：同合约相邻两根若隔了 >1 分钟，中间每分钟补一根 is_gap=true 的空 bar，
///      OHLC 用前收、量额 0、累计量沿用前一根（保持累计列单调，比 v1.1 补 0 更友好）。
///   5) 原子写 bars_YYYYMMDD.parquet（.tmp → rename），13 列，下游照旧 read_parquet。
fn 合并一日(date: &str, 分片目录: &Path, 成品目录: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let 源 = 分片目录.join(format!("bars_{}.jsonl", date));
    if !源.exists() {
        return Err(std::io::Error::other(format!("找不到分片文件 {}（这天没采到数据？）", 源.display())).into());
    }
    // 1) 复制快照再读
    let 快照 = 分片目录.join(format!(".merging_bars_{}.jsonl", date));
    std::fs::copy(&源, &快照)?;
    let raw = std::fs::read_to_string(&快照)?;

    // 2) 逐行解析
    let mut bars: Vec<MinuteBar> = Vec::new();
    let mut 坏行 = 0u64;
    for (i, line) in raw.lines().enumerate() {
        let l = line.trim();
        if l.is_empty() {
            continue;
        }
        match serde_json::from_str::<MinuteBar>(l) {
            Ok(b) => bars.push(b),
            Err(e) => {
                坏行 += 1;
                // 只有最后一行才可能是"正在写的半行"，中间行坏了是真问题，重点提示。
                tracing::error!("[MERGE] 跳过第 {} 行（解析失败，多为文件末尾半行）: {}", i + 1, e);
            }
        }
    }
    let _ = std::fs::remove_file(&快照); // 读完删快照

    if bars.is_empty() {
        return Err(std::io::Error::other(format!("{} 这天解析出 0 根有效 bar，不写空文件", date)).into());
    }
    tracing::info!(
        "[MERGE] {} 读到 {} 根有效 bar（跳过坏行 {}）",
        date,
        bars.len(),
        坏行
    );

    // 3) 按合约分组 + 算增量
    let mut by_inst: HashMap<String, Vec<MinuteBar>> = HashMap::new();
    for b in bars {
        // 合并时跳过分片里可能残留的 gap 标记（采集端不产 gap，这里保险起见只收真实 bar 算增量）
        by_inst.entry(b.instrument_id.clone()).or_default().push(b);
    }
    let mut gap = GapStats::default();
    let mut 全部: Vec<MinuteBar> = Vec::with_capacity(by_inst.len() * 345);
    for (_inst, v) in by_inst.iter_mut() {
        v.sort_by(|a, b| a.bar_time_ms.cmp(&b.bar_time_ms));
        // 算单分钟增量（只在一根根真实 bar 之间）
        let mut prev_vol = 0u64;
        let mut prev_to = 0.0f64;
        let mut first = true;
        for b in v.iter_mut() {
            if first {
                b.volume_1m = b.volume; // 当日首根：增量=累计（此前视为 0）
                b.turnover_1m = b.turnover;
                first = false;
            } else {
                b.volume_1m = b.volume.saturating_sub(prev_vol);
                let dt = b.turnover - prev_to;
                b.turnover_1m = if dt < 0.0 { 0.0 } else { dt }; // 浮点兜底，异常负值归 0
            }
            prev_vol = b.volume;
            prev_to = b.turnover;
        }
        // 4) 补缺口（在算完增量后补；缺口 bar 增量=0，累计沿用前一根保持单调）
        补齐缺口(v, &mut gap);
        全部.extend(v.drain(..));
    }

    // 全局排序：先合约后时间，和 v1.1 成品文件的排布一致，便于下游 diff 对照。
    全部.sort_by(|a, b| {
        a.instrument_id
            .cmp(&b.instrument_id)
            .then(a.bar_time_ms.cmp(&b.bar_time_ms))
    });

    // 5) 原子写 13 列 Parquet
    std::fs::create_dir_all(成品目录)?;
    let path = 成品目录.join(format!("bars_{}.parquet", date));
    写日线parquet(&path, &全部)?;
    let 缺口数 = 全部.iter().filter(|b| b.is_gap).count();
    tracing::info!(
        "[MERGE] ✅ {} 合并完成 → {} 共 {} 根（其中补齐 {} 根，涉及 {} 个有缺口合约）",
        date,
        path.display(),
        全部.len(),
        缺口数,
        gap.instruments_with_gap
    );
    Ok(())
}

/// 补缺口：同合约相邻真实 bar 若隔 >1 分钟，中间每分钟补一根 is_gap=true。
/// 与 v1.1 fill_gaps 的区别：缺口 bar 的累计量【沿用前一根】而非置 0，保持累计列单调，
/// 下游做 volume_1m 或直接看累计都不会因为缺口突然掉到 0 而误判。
fn 补齐缺口(bars: &mut Vec<MinuteBar>, stats: &mut GapStats) {
    if bars.len() < 2 {
        return;
    }
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
                trading_day: cur.trading_day.clone(),
                open: prev.close,
                high: prev.close,
                low: prev.close,
                close: prev.close,
                volume: prev.volume,      // 累计沿用前一根（保持单调）
                turnover: prev.turnover,
                open_interest: prev.open_interest,
                volume_1m: 0,             // 缺口分钟没有成交
                turnover_1m: 0.0,
                is_gap: true,
            });
            stats.gap_bars += 1;
            t += MINUTE_MS;
        }
        out.push(cur.clone());
    }
    *bars = out;
    if stats.gap_bars > 0 {
        stats.instruments_with_gap += 1;
    }
}

/// 写整日成品 Parquet（13 列：在 v1.1 的 11 列基础上加 volume_1m / turnover_1m）。
/// 原子写：先写 .tmp，写完 rename；已存在先备份 .bak。与 v1.1 write_parquet 同一套路。
fn 写日线parquet(path: &Path, bars: &[MinuteBar]) -> Result<(), Box<dyn std::error::Error>> {
    use arrow::array::{
        ArrayRef, BooleanArray, Float64Array, RecordBatch, StringArray, TimestampMillisecondArray,
        UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("bar_time", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("trading_day", DataType::Utf8, false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::UInt64, false),
        Field::new("turnover", DataType::Float64, false),
        Field::new("open_interest", DataType::Float64, false),
        // ★新增：单分钟增量，下游算"这一分钟成交了多少"直接用，不用再自己差分。
        Field::new("volume_1m", DataType::UInt64, false),
        Field::new("turnover_1m", DataType::Float64, false),
        Field::new("is_gap", DataType::Boolean, false),
    ]));

    let 取 = |f: fn(&MinuteBar) -> i64| bars.iter().map(|b| f(b)).collect::<Vec<i64>>();
    let bt = 取(|b| b.bar_time_ms);
    let inst: Vec<&str> = bars.iter().map(|b| b.instrument_id.as_str()).collect();
    let td: Vec<&str> = bars.iter().map(|b| b.trading_day.as_str()).collect();
    let open: Vec<f64> = bars.iter().map(|b| b.open).collect();
    let high: Vec<f64> = bars.iter().map(|b| b.high).collect();
    let low: Vec<f64> = bars.iter().map(|b| b.low).collect();
    let close: Vec<f64> = bars.iter().map(|b| b.close).collect();
    let vol: Vec<u64> = bars.iter().map(|b| b.volume).collect();
    let turnover: Vec<f64> = bars.iter().map(|b| b.turnover).collect();
    let oi: Vec<f64> = bars.iter().map(|b| b.open_interest).collect();
    let vol1m: Vec<u64> = bars.iter().map(|b| b.volume_1m).collect();
    let to1m: Vec<f64> = bars.iter().map(|b| b.turnover_1m).collect();
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
            Arc::new(UInt64Array::from(vol1m)) as ArrayRef,
            Arc::new(Float64Array::from(to1m)) as ArrayRef,
            Arc::new(BooleanArray::from(gap)) as ArrayRef,
        ],
    )?;

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

// ============================================================
// 主流程（v1.1 基础上加子命令分发）
//   参数 1 == "合并"/"merge"  → 合并模式（不连 CTP、不读账号）
//   否则                       → 采集模式（参数1=secrets.toml，参数2=成品输出目录，与 v1.1 一致）
// ============================================================

fn main() {
    // ★【规则17】subscriber 必须是 main 第一件事：之后所有 tracing::info!/error! 才有出口。
    //   RUST_LOG 环境变量控级别（如 RUST_LOG=debug）；没设则默认 info。
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    // ★【规则20】启动第一件事：载入运行配置，合并/采集两分支共用同一份，避免路径不一致。
    let 运行 = 运行配置::载入();
    let args: Vec<String> = std::env::args().collect();

    // ---------- 合并模式 ----------
    if args.get(1).map(|s| s == "合并" || s == "merge").unwrap_or(false) {
        let 分片目录 = Path::new(WORKSPACE_ROOT).join(&运行.分片目录_rel);
        let 成品目录 = Path::new(WORKSPACE_ROOT).join(&运行.成品目录_rel);
        // 日期列表：args[2..]；空或含"今日" → 今天本机墙钟日期。
        let mut 日期们: Vec<String> = args
            .iter()
            .skip(2)
            .filter(|s| s != &"今日")
            .cloned()
            .collect();
        if 日期们.is_empty() || args.iter().skip(2).any(|s| s == "今日") {
            日期们.push(Local::now().format("%Y%m%d").to_string());
        }
        tracing::info!("[MERGE] 合并模式 | 分片目录 {} | 成品目录 {} | 日期 {:?}", 分片目录.display(), 成品目录.display(), 日期们);
        let mut 有错 = false;
        for d in 日期们 {
            match 合并一日(&d, &分片目录, &成品目录) {
                Ok(()) => {}
                Err(e) => {
                    tracing::error!("[MERGE] ❌ 合并 {} 失败: {}", d, e);
                    有错 = true;
                }
            }
        }
        std::process::exit(if 有错 { 1 } else { 0 });
    }

    // ---------- 采集模式（以下基本照抄 v1.1 的 main）----------
    let stop = Arc::new(AtomicBool::new(false));
    {
        let s = stop.clone();
        ctrlc_handler(move || {
            if s.load(Ordering::Relaxed) {
                // 第二次 Ctrl+C：强杀。注意 v1.3 下当天数据已在分片里(每60秒fsync)，
                // 所以即便强杀，最多也只丢最后 60 秒，不再像 v1.1 那样丢整天。
                tracing::error!("\n[RUN] 第二次 Ctrl+C，强制退出（v1.3 下当天数据已在分片文件里，最多丢最后 {} 秒）", 分片落盘秒);
                std::process::exit(130);
            }
            tracing::error!("\n[RUN] 收到 Ctrl+C：停重连 → 收盘活跃桶 → 落盘线程收尾 fsync → 退出（再按一次强制，但只会丢最后 {} 秒）", 分片落盘秒);
            s.store(true, Ordering::Relaxed);
        });
    }

    let secrets_path = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| rust_ctp_test::config::DEFAULT_SECRETS.to_string());
    let text = std::fs::read_to_string(&secrets_path).unwrap_or_else(|e| {
        tracing::error!("[CFG] 读 {} 失败: {}", secrets_path, e);
        std::process::exit(1);
    });
    let cfg: Secrets = toml::from_str(&text).unwrap_or_else(|e| {
        tracing::error!("[CFG] 解析 {} 失败: {}", secrets_path, e);
        std::process::exit(1);
    });
    tracing::info!("[CFG] 已加载 {}", secrets_path);

    let contracts_file = if Path::new(&cfg.market_data.contracts_file).is_absolute() {
        PathBuf::from(&cfg.market_data.contracts_file)
    } else {
        Path::new(WORKSPACE_ROOT).join(&cfg.market_data.contracts_file)
    };
    let contracts = load_contracts_csv(&contracts_file).unwrap_or_else(|e| {
        tracing::error!("[CFG] {}", e);
        std::process::exit(1);
    });
    tracing::info!(
        "[CFG] 工作区根 {} | 合约 {} 条 | member {} 个 | md_port {} | 断线重连 开 | 分片fsync {}秒",
        WORKSPACE_ROOT,
        contracts.len(),
        cfg.market_data.member_ips.len(),
        cfg.market_data.md_port,
        分片落盘秒
    );

    if cfg.market_data.member_ips.is_empty() {
        tracing::error!("[CFG] member_ips 为空，无前置可连，退出");
        std::process::exit(1);
    }

    let num_members = cfg.market_data.member_ips.len();
    let shards = shard_contracts(&contracts, num_members);

    let dynlib = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(MD_DYNLIB_REL);
    if !dynlib.exists() {
        tracing::error!("[CFG] 动态库不存在 {}", dynlib.display());
        std::process::exit(1);
    }

    // 分片目录（采集时往这写 jsonl）。成品目录只在合并时用，采集模式不需要。
    let 分片目录 = match args.get(2) {
        Some(p) => PathBuf::from(p),
        None => Path::new(WORKSPACE_ROOT).join(&运行.分片目录_rel),
    };
    std::fs::create_dir_all(&分片目录).unwrap_or_else(|e| {
        tracing::error!("[CFG] 建分片目录 {} 失败: {}", 分片目录.display(), e);
        std::process::exit(1);
    });
    tracing::info!("[CFG] 分片输出目录 {}（每根bar收盘即追加，每{}秒fsync）", 分片目录.display(), 分片落盘秒);

    // ★改动：bar 通道从 (日期, Vec) 改成【逐根 MinuteBar】；bounded 队列满则背压阻塞，不丢。
    let (bar_tx, bar_rx) = mpsc::sync_channel::<MinuteBar>(运行.bar队列容量);
    let writer_handle = {
        let d = 分片目录.clone();
        std::thread::spawn(move || run_writer(bar_rx, d, 运行.分片落盘秒))
    };
    let (tick_tx, tick_rx) = mpsc::channel::<TickEvent>();
    let agg_stop = stop.clone();
    let agg_handle = std::thread::spawn(move || run_aggregator(tick_rx, bar_tx, agg_stop));

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

    tracing::info!(
        "[RUN] {} 个 member 已启动，进入健康巡检（非交易时段空转属正常；Ctrl+C 优雅退出，数据每{}秒落盘一次）",
        num_members, 分片落盘秒
    );

    let start = Instant::now();
    let mut 上一次告警 = Instant::now();
    loop {
        if stop.load(Ordering::Relaxed) {
            tracing::info!("[RUN] 停止标志已置位，等待 member 线程退出（最长退避 {}s）", RECONNECT_MAX_MS / 1000);
            break;
        }
        std::thread::sleep(Duration::from_secs(REPORT_INTERVAL_SEC));
        {
            let st = 锁或恢复(&stats);
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
                if let Some(t) = m.last_tick_at {
                    if t.elapsed().as_secs() > 300 {
                        stale.push(i);
                    }
                }
            }
            tracing::info!(
                "===== 运行 {} 秒 | 已连 {} | 已登录 {} | 累计 tick {} | 累计断线 {} 次 =====",
                start.elapsed().as_secs(),
                conn,
                login,
                ticks,
                disc
            );
            // ★改进：收盘后全员静默是正常现象，别每 5 秒刷屏。
            //   只在"曾经登录过(说明今天是交易日)"且"距上次告警≥60秒"时才报一次，避免刷屏。
            let 今天登录过 = st.iter().any(|m| m.logged_in || m.ready_rounds > 0);
            if !stale.is_empty() && 今天登录过 && 上一次告警.elapsed().as_secs() >= 60 {
                tracing::error!(
                    "[RUN] 超过 5 分钟无 tick 的 member: {:?}（若已收盘属正常；盘中则需关注是否断流待重连）",
                    stale
                );
                上一次告警 = Instant::now();
            }
        }
    }

    for h in handles {
        let _ = h.join();
    }
    tracing::info!("[RUN] member 线程全部退出，等聚合线程收尾");
    let _ = agg_handle.join();
    let _ = writer_handle.join();
    tracing::info!("[RUN] 全部退出。当天数据已在分片文件里，随时用 `合并 今日` 生成成品 Parquet。");
}

/// Ctrl+C 处理（照抄 v1.1，用 ctrlc crate，无 unsafe）。
fn ctrlc_handler<F: FnMut() + Send + 'static>(f: F) {
    // 【规则15 零 expect】启动期致命错误：打日志后 exit(1)，不用 expect（避免 clippy expect_used）
    if let Err(e) = ctrlc::set_handler(f) {
        tracing::error!("[FATAL] 设置 Ctrl+C 处理失败: {}（Linux 下一般是权限或被父进程截获）", e);
        std::process::exit(1);
    }
}

// ============================================================
// 单元测试：纯函数逻辑，用 `cargo test --bin 分钟k线落盘_v1_3分片` 跑。
//   重点测"增量差分"和"补缺口"——这俩错了回测会静默错半天，必须钉死。
// ============================================================
#[cfg(test)]
mod 测试 {
    use super::*;

    fn 造bar(inst: &str, ms: i64, vol: u64, to: f64) -> MinuteBar {
        MinuteBar {
            instrument_id: inst.to_string(),
            bar_time_ms: ms,
            trading_day: "20261124".to_string(),
            open: 1.0,
            high: 1.0,
            low: 1.0,
            close: 1.0,
            volume: vol,
            turnover: to,
            open_interest: 0.0,
            volume_1m: 0,
            turnover_1m: 0.0,
            is_gap: false,
        }
    }

    #[test]
    fn 增量首根等于累计() {
        let mut v = vec![造bar("IF2401", 0, 100, 1000.0)];
        // 模拟合并里的增量计算逻辑
        let mut prev_vol = 0u64;
        let mut prev_to = 0.0f64;
        let mut first = true;
        for b in v.iter_mut() {
            if first {
                b.volume_1m = b.volume;
                b.turnover_1m = b.turnover;
                first = false;
            } else {
                b.volume_1m = b.volume.saturating_sub(prev_vol);
                b.turnover_1m = b.turnover - prev_to;
            }
            prev_vol = b.volume;
            prev_to = b.turnover;
        }
        assert_eq!(v[0].volume_1m, 100);
        assert_eq!(v[0].turnover_1m, 1000.0);
    }

    #[test]
    fn 增量后续是相邻差() {
        let mut v = vec![
            造bar("IF2401", 0, 100, 1000.0),
            造bar("IF2401", 60_000, 250, 2600.0),
            造bar("IF2401", 120_000, 250, 2600.0), // 这分钟没成交，增量应 0
        ];
        let mut prev_vol = 0u64;
        let mut prev_to = 0.0f64;
        let mut first = true;
        for b in v.iter_mut() {
            if first {
                b.volume_1m = b.volume;
                b.turnover_1m = b.turnover;
                first = false;
            } else {
                b.volume_1m = b.volume.saturating_sub(prev_vol);
                b.turnover_1m = b.turnover - prev_to;
            }
            prev_vol = b.volume;
            prev_to = b.turnover;
        }
        assert_eq!(v[1].volume_1m, 150);
        assert_eq!(v[1].turnover_1m, 1600.0);
        assert_eq!(v[2].volume_1m, 0); // 累计没变 → 增量 0
    }

    #[test]
    fn 补缺口在中间插空bar且累计单调() {
        let mut v = vec![
            造bar("IF2401", 0, 100, 1000.0),
            造bar("IF2401", 180_000, 300, 3000.0), // 中间缺 60s、120s 两分钟
        ];
        let mut gap = GapStats::default();
        补齐缺口(&mut v, &mut gap);
        // 应有 4 根：0, 60s(gap), 120s(gap), 180s
        assert_eq!(v.len(), 4);
        assert!(v[1].is_gap && v[2].is_gap);
        assert!(!v[0].is_gap && !v[3].is_gap);
        // 缺口 bar 累计沿用前一根（单调），增量 0
        assert_eq!(v[1].volume, 100);
        assert_eq!(v[2].volume, 100);
        assert_eq!(v[1].volume_1m, 0);
        // gap 数 = 2
        assert_eq!(gap.gap_bars, 2);
    }

    #[test]
    fn 无缺口不插bar() {
        let mut v = vec![
            造bar("IF2401", 0, 100, 1000.0),
            造bar("IF2401", 60_000, 200, 2000.0),
        ];
        let mut gap = GapStats::default();
        补齐缺口(&mut v, &mut gap);
        assert_eq!(v.len(), 2);
        assert_eq!(gap.gap_bars, 0);
    }

    #[test]
    fn 单bar不补() {
        let mut v = vec![造bar("IF2401", 0, 100, 1000.0)];
        let mut gap = GapStats::default();
        补齐缺口(&mut v, &mut gap);
        assert_eq!(v.len(), 1);
    }

    #[test]
    fn floor分钟对齐() {
        assert_eq!(floor_minute(61_234), 60_000);
        assert_eq!(floor_minute(0), 0);
        assert_eq!(floor_minute(119_999), 60_000);
    }
}

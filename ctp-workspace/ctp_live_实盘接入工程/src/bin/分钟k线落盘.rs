//! 分钟K线落盘 · 最小版
//!
//! 只做一件事：16 个 member 分片订阅全市场 tick，聚合成 1 分钟 K 线，
//! 先存内存，按日交给独立落盘线程写成 Parquet。
//!
//! 刻意删掉的非核心逻辑：自动重连、断线补K线、非盘中过滤、优雅退出、去重统计。
//!
//! 用法（放进 ctp_live_实盘接入工程/src/bin/ 后）：
//!   cargo build --release --bin 分钟k线落盘
//!   ./分钟k线落盘 [secrets.toml] [输出目录]
//!   默认 secrets = config/secrets.toml，默认输出 = <工作区根>/data_数据/分钟K线/
//!
//! 依赖（写进 ctp_live_实盘接入工程/Cargo.toml，其余 crate 已具备）：
//!   arrow = "52"
//!   parquet = "52"
//!   chrono = "0.4"

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use chrono::Local;
use chrono::TimeZone;
use ctp2rs::ffi::{AssignFromString, WrapToString};
use ctp2rs::v1alpha1::*;
use rust_ctp_test::config::{Secrets, WORKSPACE_ROOT};

// ============================================================
// 常量（与 md_pool 对齐）
// ============================================================

const MD_DYNLIB_REL: &str = "lib/libthostmduserapi_se.so";
const SUBSCRIBE_BATCH: usize = 500;
const SUBSCRIBE_DELAY_MS: u64 = 300;
const REPORT_INTERVAL_SEC: u64 = 5;

// ============================================================
// 合约
// ============================================================

#[derive(Debug, Clone)]
struct Contract {
    code: String,
    exchange: String,
}

fn load_contracts_csv(path: &Path) -> Result<Vec<Contract>, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("读 {} 失败: {}", path.display(), e))?;
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

/// 按交易所分组，组内轮询分给 N 个 member（与 md_pool 一致，结果稳定可复现）。
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

// 相比 md_pool，Tick 多带 4 个字段：volume/turnover/open_interest/trading_day，
// 这几个是聚分钟K线必须的（md_pool 只做统计没采）。
enum MemberEvent {
    Connected(usize),
    Disconnected(usize, i32),
    Login(usize, Option<(i32, String)>),
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
// SPI（照抄 md_pool，仅 Tick 多掏 4 个字段）
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
// 启动 member / listener（照抄 md_pool，不做自动重连）
// ============================================================

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
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let tag = format!("[MD#{}]", member_idx);
        println!("{} 启动 front={} 合约数={}", tag, front, instruments.len());
        std::fs::create_dir_all(&flow_dir).ok();

        let mdapi = MdApi::create_api(
            dynlib.to_str().unwrap(),
            flow_dir.to_str().unwrap(),
            false,
            false,
        );
        println!("{} api_version={}", tag, mdapi.get_api_version());

        let spi = Box::new(MemberSpi {
            member_idx,
            tx: evt_tx.clone(),
        });
        let spi_ptr = Box::into_raw(spi) as *mut dyn MdSpi;
        mdapi.register_spi(spi_ptr);
        mdapi.register_front(&front);
        mdapi.init();

        std::thread::sleep(Duration::from_millis(1500));

        let mut login = CThostFtdcReqUserLoginField::default();
        login.BrokerID.assign_from_str(&broker_id);
        login.UserID.assign_from_str(&user_id);
        login.Password.assign_from_str(&password);
        let ret = mdapi.req_user_login(&mut login, 1);
        println!("{} req_user_login ret={}", tag, ret);

        std::thread::sleep(Duration::from_millis(1500));

        let total = instruments.len();
        let mut done = 0;
        for batch in instruments.chunks(SUBSCRIBE_BATCH) {
            let ret = mdapi.subscribe_market_data(batch);
            done += batch.len();
            println!("{} 订阅 {}/{} ret={}", tag, done, total, ret);
            std::thread::sleep(Duration::from_millis(SUBSCRIBE_DELAY_MS));
        }
        println!("{} 订阅完成 {}", tag, done);

        // 阻塞直到断线；断线即线程结束（最小版不重连）。
        mdapi.join();
        println!("{} 前置断开，线程退出", tag);
    })
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
                    eprintln!("[MD#{}] 前置已连接", idx);
                }
                MemberEvent::Disconnected(idx, reason) => {
                    let mut st = stats.lock().unwrap();
                    st[idx].connected = false;
                    st[idx].last_disconnect_reason = reason;
                    eprintln!("[MD#{}] 前置断开 reason={}", idx, reason);
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
    });
}

#[derive(Default, Clone)]
struct MemberStats {
    connected: bool,
    logged_in: bool,
    login_fail: u64,
    last_disconnect_reason: i32,
    ticks: u64,
    last_tick_at: Option<Instant>,
}

// ============================================================
// 聚合：tick → 分钟K线，先攒内存
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
    let older: Vec<String> = cache
        .keys()
        .filter(|k| k.as_str() < today)
        .cloned()
        .collect();
    for d in older {
        if let Some(v) = cache.remove(&d) {
            if !v.is_empty() {
                let _ = bar_tx.send((d, v));
            }
        }
    }
}

fn run_aggregator(
    tick_rx: mpsc::Receiver<TickEvent>,
    bar_tx: mpsc::Sender<(String, Vec<MinuteBar>)>,
) {
    let mut buckets: HashMap<String, Bucket> = HashMap::new();
    let mut cache: HashMap<String, Vec<MinuteBar>> = HashMap::new();
    let mut total_done = 0u64;
    let mut per_member_ticks: HashMap<usize, u64> = HashMap::new(); // 各 member 喂入的 tick 数，排障用
    let mut last_report = Instant::now();

    loop {
        match tick_rx.recv_timeout(Duration::from_millis(200)) {
            Ok(t) => {
                *per_member_ticks.entry(t.member_idx).or_default() += 1;
                // 无效价格直接丢（≤0 或 CTP 极大值占位）。
                if t.last_price <= 0.0 || t.last_price >= 1e30 {
                    continue;
                }
                // 用收包本机时间取整到分钟，不碰 CTP 的 UpdateTime，规避盘前重演污染。
                let now = Local::now();
                let bar_ms = now.timestamp_millis();
                let bar_ms = bar_ms - bar_ms.rem_euclid(60_000);

                let minute_switch = match buckets.get(&t.instrument) {
                    Some(b) => b.bar_ms != bar_ms,
                    None => false,
                };

                if minute_switch {
                    // 旧桶收尾 → 进内存缓存 → 翻篇的天落盘。
                    if let Some((inst, b)) = buckets.remove_entry(&t.instrument) {
                        let bar = MinuteBar {
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
                        };
                        let d = ms_to_date(bar.bar_time_ms);
                        cache.entry(d.clone()).or_default().push(bar);
                        total_done += 1;
                        flush_older(&mut cache, &d, &bar_tx);
                    }
                }

                match buckets.get_mut(&t.instrument) {
                    Some(b) => {
                        // 同一分钟内：更新 OHLC + 用最新累计值覆盖。
                        if t.last_price > b.high {
                            b.high = t.last_price;
                        }
                        if t.last_price < b.low {
                            b.low = t.last_price;
                        }
                        b.close = t.last_price;
                        b.volume = t.volume;
                        b.turnover = t.turnover;
                        b.open_interest = t.open_interest;
                        if !t.trading_day.is_empty() {
                            b.trading_day = t.trading_day.clone();
                        }
                    }
                    None => {
                        // 首现：建桶。
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
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }

        if last_report.elapsed() >= Duration::from_secs(REPORT_INTERVAL_SEC) {
            let cached: usize = cache.values().map(|v| v.len()).sum();
            // 各 member tick 数按 idx 升序打印，一眼看出哪个 member 断流/掉队。
            let mut dist: Vec<(usize, u64)> =
                per_member_ticks.iter().map(|(&k, &v)| (k, v)).collect();
            dist.sort_unstable_by_key(|&(k, _)| k);
            let dist_str: String = dist
                .iter()
                .map(|(k, v)| format!("MD#{}:{}", k, v))
                .collect::<Vec<_>>()
                .join(" ");
            println!(
                "[AGG] 已完成 {} 根 | 活跃桶 {} | 内存缓存 {} 根 | 各member tick [{}]",
                total_done,
                buckets.len(),
                cached,
                dist_str
            );
            last_report = Instant::now();
        }
    }

    // 通道断了（所有 member 退出）：把剩余活跃桶 + 缓存全部交出去。
    for (inst, b) in buckets {
        let bar = MinuteBar {
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
        };
        let d = ms_to_date(bar.bar_time_ms);
        cache.entry(d).or_default().push(bar);
    }
    for (d, v) in cache {
        if !v.is_empty() {
            let _ = bar_tx.send((d, v));
        }
    }
    drop(bar_tx);
    println!("[AGG] 聚合线程退出，全部数据已交给落盘线程");
}

// ============================================================
// 落盘：独立线程，收一批写一批 Parquet（原子写）
// ============================================================

fn write_parquet(path: &Path, bars: &[MinuteBar]) -> Result<(), Box<dyn std::error::Error>> {
    use arrow::array::{
        ArrayRef, Float64Array, RecordBatch, StringArray, TimestampMillisecondArray, UInt64Array,
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
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn run_writer(rx: mpsc::Receiver<(String, Vec<MinuteBar>)>, out_dir: PathBuf) {
    while let Ok((date, mut bars)) = rx.recv() {
        bars.sort_by(|a, b| {
            a.instrument_id
                .cmp(&b.instrument_id)
                .then(a.bar_time_ms.cmp(&b.bar_time_ms))
        });
        let path = out_dir.join(format!("bars_{}.parquet", date));
        match write_parquet(&path, &bars) {
            Ok(()) => println!("[WRT] 写入 {} 共 {} 根", path.display(), bars.len()),
            Err(e) => eprintln!("[WRT] 写 {} 失败: {}", path.display(), e),
        }
    }
    println!("[WRT] 落盘线程退出");
}

// ============================================================
// 主流程
// ============================================================

fn main() {
    // 1. 读配置（复用跑通的 Secrets 结构体，和 md_pool 完全一致）。
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

    // 2. 合约清单。
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
        "[CFG] 工作区根 {} | 合约 {} 条 | member {} 个 | md_port {}",
        WORKSPACE_ROOT,
        contracts.len(),
        cfg.market_data.member_ips.len(),
        cfg.market_data.md_port
    );

    if cfg.market_data.member_ips.is_empty() {
        eprintln!("[CFG] member_ips 为空，无前置可连，退出");
        std::process::exit(1);
    }

    // 3. 分片。
    let num_members = cfg.market_data.member_ips.len();
    let shards = shard_contracts(&contracts, num_members);

    // 4. 动态库（编译期绝对路径，不依赖 cwd，与 md_pool 一致）。
    let dynlib = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(MD_DYNLIB_REL);
    if !dynlib.exists() {
        eprintln!("[CFG] 动态库不存在 {}", dynlib.display());
        std::process::exit(1);
    }

    // 5. 输出目录。
    let out_dir = match std::env::args().nth(2) {
        Some(p) => PathBuf::from(p),
        None => Path::new(WORKSPACE_ROOT).join("data_数据").join("分钟K线"),
    };
    std::fs::create_dir_all(&out_dir).unwrap_or_else(|e| {
        eprintln!("[CFG] 建输出目录 {} 失败: {}", out_dir.display(), e);
        std::process::exit(1);
    });
    println!("[CFG] 输出目录 {}", out_dir.display());

    // 6. 落盘线程 + 聚合线程。
    let (bar_tx, bar_rx) = mpsc::channel::<(String, Vec<MinuteBar>)>();
    let writer_handle = {
        let d = out_dir.clone();
        std::thread::spawn(move || run_writer(bar_rx, d))
    };
    let (tick_tx, tick_rx) = mpsc::channel::<TickEvent>();
    let agg_handle = std::thread::spawn(move || run_aggregator(tick_rx, bar_tx));

    // 7. 启动所有 member + listener。
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
        );
        spawn_listener(evt_rx, tick_tx.clone(), stats.clone());
        handles.push(h);
    }
    // 主线程不再发 tick，drop 自己的发送端；所有 listener 退出后聚合线程才会断流。
    drop(tick_tx);

    println!(
        "[RUN] {} 个 member 已启动，进入统计循环（非交易时段空转属正常）",
        num_members
    );

    // 8. 主循环：只打印健康统计，member 全退出后收尾。
    let start = Instant::now();
    loop {
        std::thread::sleep(Duration::from_secs(REPORT_INTERVAL_SEC));
        {
            let st = stats.lock().unwrap();
            let mut conn = 0;
            let mut login = 0;
            let mut ticks = 0u64;
            for m in st.iter() {
                if m.connected {
                    conn += 1;
                }
                if m.logged_in {
                    login += 1;
                }
                ticks += m.ticks;
            }
            println!(
                "===== 运行 {} 秒 | 已连 {} | 已登录 {} | 累计 tick {} =====",
                start.elapsed().as_secs(),
                conn,
                login,
                ticks
            );
        }
        if handles.iter().all(|h| h.is_finished()) {
            println!("[RUN] 所有 member 已退出，开始收尾");
            break;
        }
    }

    for h in handles {
        let _ = h.join();
    }
    // tick_tx 已 drop + listener 随 member 退出 → 聚合线程断流落盘后退出。
    let _ = agg_handle.join();
    // 聚合线程 drop bar_tx → 落盘线程写完退出。
    let _ = writer_handle.join();
    println!("[RUN] 全部退出");
}

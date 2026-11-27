//! 多 member（行情成员）行情连接池。
//!
//! 架构：
//!   1. 16 个 member（行情成员），每个 member 独立一个 MdApi（行情接口）实例，互不影响。
//!   2. 每个 member 连接不同的行情前置 IP，端口相同（:61213）。
//!   3. 合约按交易所分组，组内轮询分给不同 member，保证每个 member 都覆盖所有交易所。
//!   4. 所有 member 的 tick（逐笔行情）汇聚到一个通道，主线程按 InstrumentID 去重统计。
//!   5. 不做主备切换，某个 member 断开不影响其他 member，压力由剩余 member 分摊。
//!
//! 参数来源：
//!   - 所有 IP、端口、账户、密码从 config/secrets.toml 读取，本文件不含任何真实值。
//!   - 商品合约清单从 [market_data].contracts_file 读取（CSV 格式，首行 # 注释）。
//!   - ETF 期权清单可选，通过命令行第一个参数指定（一行一个代码）。
//!
//! 运行方式：
//!   ./md_pool                      只订商品合约
//!   ./md_pool etf_contracts.txt    商品合约 + ETF 期权
//!
//! 注意：
//!   - 本程序不需要 sudo，行情登录不触发看穿式采集。
//!   - Ctrl+C 退出即可，CTP 后台线程不会自动退出。
//!   - 每个 member 的 flow_dir 独立，避免 .con 会话流文件冲突。

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use ctp2rs::ffi::{AssignFromString, WrapToString};
use ctp2rs::v1alpha1::*;
use rust_ctp_test::config::{Secrets, WORKSPACE_ROOT};

// ============================================================
// 常量
// ============================================================

/// 行情动态库相对本工程根目录的路径。
const MD_DYNLIB_REL: &str = "lib/libthostmduserapi_se.so";

/// 单批订阅数量。CTP 柜台有订阅频率限制，每批 500 实测稳定。
const SUBSCRIBE_BATCH: usize = 500;

/// 每批订阅之间的间隔（毫秒）。
const SUBSCRIBE_DELAY_MS: u64 = 300;

/// 统计打印间隔（秒）。
const REPORT_INTERVAL_SEC: u64 = 5;

// ============================================================
// 配置结构
// ============================================================

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

fn load_etf_txt(path: &Path) -> Result<Vec<Contract>, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("读 {} 失败: {}", path.display(), e))?;
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let code = line.split(',').next().unwrap_or(line).trim();
        if code.is_empty() {
            continue;
        }
        // ETF 期权交易所判定：
        //   第一位 1 → SSE（沪市）
        //   第一位 9 → SZSE（深市）
        //   其他 → 打印警告跳过，人工核对清单后再加入
        if code.len() != 8 || !code.as_bytes().iter().all(|b| b.is_ascii_digit()) {
            eprintln!("[ETF] 跳过非 8 位纯数字代码：{}", code);
            continue;
        }
        let exchange = match code.as_bytes()[0] {
            b'1' => "SSE",
            b'9' => "SZSE",
            other => {
                eprintln!(
                    "[ETF] 未知交易所代码 {}，第一位='{}'，当前只认 1→SSE / 9→SZSE，请人工核对",
                    code, other as char
                );
                continue;
            }
        };
        out.push(Contract {
            code: code.to_string(),
            exchange: exchange.to_string(),
        });
    }
    Ok(out)
}

/// 分片：按交易所分组，组内轮询分给 N 个 member。
fn shard_contracts(contracts: &[Contract], num_members: usize) -> Vec<Vec<String>> {
    let mut shards: Vec<Vec<String>> = vec![Vec::new(); num_members];
    let mut by_ex: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    for c in contracts {
        by_ex.entry(c.exchange.clone()).or_default().push(&c.code);
    }
    for (ex, codes) in &by_ex {
        for (i, code) in codes.iter().enumerate() {
            shards[i % num_members].push(code.to_string());
        }
        println!("[SHARD] 交易所 {}: {} 条", ex, codes.len());
    }
    shards
}

// ============================================================
// member 事件
// ============================================================

// 每个 member 的所有事件都在 listener 线程里被消费，转成健康状态。
#[derive(Debug)]
enum MemberEvent {
    Connected(usize),
    Disconnected(usize, i32),
    Login(usize, Option<(i32, String)>),
    SubRsp(usize, Option<(i32, String)>, String),
    Tick {
        member_idx: usize,
        instrument: String,
        update_time: String,
        update_millisec: u32,
        last_price: f64,
        bid_price1: f64,
        ask_price1: f64,
    },
}

fn info_pair(i: Option<&CThostFtdcRspInfoField>) -> Option<(i32, String)> {
    match i {
        Some(x) if x.ErrorID != 0 => Some((x.ErrorID, x.ErrorMsg.to_string())),
        _ => None,
    }
}

// ============================================================
// SPI
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
        _rsp_user_login: Option<&CThostFtdcRspUserLoginField>,
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
        specific_instrument: Option<&CThostFtdcSpecificInstrumentField>,
        rsp_info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        _is_last: bool,
    ) {
        let code = specific_instrument
            .map(|s| s.InstrumentID.to_string())
            .unwrap_or_default();
        let _ = self.tx.send(MemberEvent::SubRsp(
            self.member_idx,
            info_pair(rsp_info),
            code,
        ));
    }
    fn on_rtn_depth_market_data(
        &mut self,
        depth_market_data: Option<&CThostFtdcDepthMarketDataField>,
    ) {
        if let Some(q) = depth_market_data {
            let _ = self.tx.send(MemberEvent::Tick {
                member_idx: self.member_idx,
                instrument: q.InstrumentID.to_string(),
                update_time: q.UpdateTime.to_string(),
                update_millisec: q.UpdateMillisec as u32,
                last_price: q.LastPrice,
                bid_price1: q.BidPrice1,
                ask_price1: q.AskPrice1,
            });
        }
    }
    fn on_rsp_error(
        &mut self,
        p_rsp_info: Option<&CThostFtdcRspInfoField>,
        n_request_id: i32,
        _b_is_last: bool,
    ) {
        if let Some(e) = p_rsp_info
            && e.ErrorID != 0
        {
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

// ============================================================
// tick 统一结构
// ============================================================

// TickEvent 的每个字段都在主循环里被读：member_idx 用于每 member 覆盖面，
// 价格字段用于报价质量统计。
#[derive(Debug)]
struct TickEvent {
    member_idx: usize,
    instrument: String,
    update_time: String,
    update_millisec: u32,
    last_price: f64,
    bid_price1: f64,
    ask_price1: f64,
}

// ============================================================
// 启动 member
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

        mdapi.join();
    })
}

/// listener 线程：把一个 member 的事件转发到主 tick 通道。
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
                    eprintln!("[MD#{}] 前置断开 reason={}", idx, reason);
                }
                MemberEvent::Login(idx, info) => {
                    let mut st = stats.lock().unwrap();
                    match info {
                        None => {
                            st[idx].logged_in = true;
                            eprintln!("[MD#{}] 登录成功", idx);
                        }
                        Some((id, msg)) => {
                            st[idx].login_fail += 1;
                            eprintln!("[MD#{}] 登录失败 ErrorID={} {}", idx, id, msg);
                        }
                    }
                }
                MemberEvent::SubRsp(idx, info, _code) => {
                    let mut s = stats.lock().unwrap();
                    match info {
                        None => s[idx].sub_ok += 1,
                        Some((id, msg)) => {
                            s[idx].sub_fail += 1;
                            if s[idx].sub_fail <= 3 {
                                eprintln!("[MD#{}] sub_fail ErrorID={} {}", idx, id, msg);
                            }
                        }
                    }
                }
                MemberEvent::Tick {
                    member_idx,
                    instrument,
                    update_time,
                    update_millisec,
                    last_price,
                    bid_price1,
                    ask_price1,
                } => {
                    {
                        let mut st = stats.lock().unwrap();
                        st[member_idx].ticks += 1;
                        st[member_idx].last_tick_at = Some(Instant::now());
                        // 粗过滤：买卖价都为正且不是 CTP 极大值占位。
                        // 精确 sanitize 留给快照层，这里只为健康统计。
                        if bid_price1 > 0.0
                            && bid_price1 < 1e30
                            && ask_price1 > 0.0
                            && ask_price1 < 1e30
                        {
                            st[member_idx].valid_quotes += 1;
                        }
                    }
                    let _ = tx.send(TickEvent {
                        member_idx,
                        instrument,
                        update_time,
                        update_millisec,
                        last_price,
                        bid_price1,
                        ask_price1,
                    });
                }
            }
        }
    });
}

// ============================================================
// 主流程
// ============================================================

/// 每个 member 的健康状态。listener 线程写，主循环定时读。
#[derive(Default, Clone)]
struct MemberStats {
    // ---- 连接与登录 ----
    connected: bool,
    logged_in: bool,
    login_fail: u64,
    last_disconnect_reason: i32,
    last_disconnect_at: Option<Instant>,
    // ---- 订阅 ----
    sub_ok: u64,
    sub_fail: u64,
    // ---- 行情 ----
    ticks: u64,
    valid_quotes: u64, // 买卖价都为正的 tick 数
    last_tick_at: Option<Instant>,
}

fn main() {
    // 1. 读配置
    let secrets_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| rust_ctp_test::config::DEFAULT_SECRETS.to_string());
    let etf_path = std::env::args().nth(2);

    let text = std::fs::read_to_string(&secrets_path).unwrap_or_else(|e| {
        eprintln!("[CFG] 读 {} 失败: {}", secrets_path, e);
        std::process::exit(1);
    });
    let cfg: Secrets = toml::from_str(&text).unwrap_or_else(|e| {
        eprintln!("[CFG] 解析失败: {}", e);
        std::process::exit(1);
    });
    println!("[CFG] 已加载 {}", secrets_path);

    // 2. 读商品合约
    let contracts_file = if Path::new(&cfg.market_data.contracts_file).is_absolute() {
        PathBuf::from(&cfg.market_data.contracts_file)
    } else {
        Path::new(WORKSPACE_ROOT).join(&cfg.market_data.contracts_file)
    };
    let mut contracts = load_contracts_csv(&contracts_file).unwrap_or_else(|e| {
        eprintln!("[CFG] {}", e);
        std::process::exit(1);
    });
    println!("[CFG] 商品合约 {} 条", contracts.len());

    // 3. 可选 ETF 清单
    if let Some(p) = &etf_path {
        match load_etf_txt(Path::new(p)) {
            Ok(etf) => {
                println!("[CFG] ETF 期权 {} 条，来自 {}", etf.len(), p);
                contracts.extend(etf);
            }
            Err(e) => eprintln!("[CFG] ETF 清单读取失败: {}", e),
        }
    }

    // 4. 分片
    let num_members = cfg.market_data.member_ips.len();
    let shards = shard_contracts(&contracts, num_members);

    // 5. 动态库路径
    // 编译期展开为绝对路径，不依赖运行时 cwd。
    // CARGO_MANIFEST_DIR 是编译时环境变量，std::env::var 运行时取不到，原代码永远 fallback 到 current_dir。
    let dynlib = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(MD_DYNLIB_REL);
    if !dynlib.exists() {
        eprintln!("[CFG] 动态库不存在 {}", dynlib.display());
        std::process::exit(1);
    }

    // 6. 全局 tick 通道
    let (tick_tx, tick_rx) = mpsc::channel::<TickEvent>();

    // 7. 启动所有 member
    // 先建好共享统计，spawn_listener 和主循环都要用它
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

    // 8. 主循环：去重统计
    let mut latest: HashMap<String, (String, u32)> = HashMap::new();
    // 每个 member 收到的唯一合约集合
    let mut per_member_unique: Vec<std::collections::HashSet<String>> =
        vec![std::collections::HashSet::new(); num_members];
    // 最新价有效的合约集合
    let mut valid_price_set: std::collections::HashSet<String> = std::collections::HashSet::new();
    // 双边报价合约集合
    let mut two_sided_set: std::collections::HashSet<String> = std::collections::HashSet::new();
    let start = Instant::now();
    let mut last_report = Instant::now();
    let mut total_unique = 0u64;

    loop {
        match tick_rx.recv_timeout(Duration::from_millis(200)) {
            Ok(t) => {
                let key = t.instrument.clone();
                let v = (t.update_time.clone(), t.update_millisec);
                let is_new = !matches!(latest.get(&key), Some(old) if old == &v);
                if is_new {
                    // 真正消费 TickEvent 的所有字段
                    per_member_unique[t.member_idx].insert(key.clone());
                    if t.last_price > 0.0 && t.last_price < 1e30 {
                        valid_price_set.insert(key.clone());
                    }
                    if t.bid_price1 > 0.0
                        && t.bid_price1 < 1e30
                        && t.ask_price1 > 0.0
                        && t.ask_price1 < 1e30
                    {
                        two_sided_set.insert(key.clone());
                    }
                    latest.insert(key, v);
                    total_unique += 1;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }

        if last_report.elapsed() >= Duration::from_secs(REPORT_INTERVAL_SEC) {
            let elapsed = start.elapsed().as_secs();
            println!(
                "===== 运行 {} 秒 | 唯一个股 {} | 最新价有效 {} | 双边报价 {} =====",
                elapsed,
                total_unique,
                valid_price_set.len(),
                two_sided_set.len()
            );
            {
                let st = stats.lock().unwrap();
                for (i, m) in st.iter().enumerate() {
                    let last_ago = m
                        .last_tick_at
                        .map(|t| t.elapsed().as_secs())
                        .unwrap_or(9999);
                    let disc_ago = m
                        .last_disconnect_at
                        .map(|t| format!("{}s前", t.elapsed().as_secs()))
                        .unwrap_or_else(|| "-".to_string());
                    println!(
                        "  MD#{:02} conn={} login={} login_fail={} sub_ok={} sub_fail={} ticks={} valid={} unique={} last_tick_ago={}s disc=({} {})",
                        i,
                        m.connected,
                        m.logged_in,
                        m.login_fail,
                        m.sub_ok,
                        m.sub_fail,
                        m.ticks,
                        m.valid_quotes,
                        per_member_unique[i].len(),
                        last_ago,
                        m.last_disconnect_reason,
                        disc_ago
                    );
                }
            }
            last_report = Instant::now();
        }
    }
}

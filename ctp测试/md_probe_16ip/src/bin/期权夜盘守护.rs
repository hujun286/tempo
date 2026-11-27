#![allow(
    clippy::print_stdout,
    reason = "采集器的职责就是输出心跳与结果，本程序即报告输出器"
)]
#![allow(clippy::print_stderr, reason = "采集错误必须走 stderr 提示用户")]
//! 期权夜盘采集器（单窗口制）：au/ag 期权+标的期货 tick 采集，一次运行采一个窗口
//!
//! 架构背景（2026-11-27 夜盘实测结论）：沙盒环境任何后台进程活不过一次会话，
//! 故本程序设计为"单窗口制"——一次运行 连接→登录→订阅→采 tick 落盘→写存在清单→退出，
//! 由 cron 定时任务每 30 分钟拉起一次，跨沙盒回收持续采集整个夜盘（21:00-02:30）。
//!
//! 与一次性探针（md_probe_期权.rs）的差异：
//! 1. 窗口时长由命令行参数控制（默认 480 秒），窗口内断线自动重连续采；
//! 2. 宽档位网格订阅：订阅确认（ErrorID==0）即存在性权威判定——用户"边界探测"思路的
//!    工程化落地：网格全量候选一次订阅窗口就把"存在清单"建出来，CTP 对不存在的
//!    档位直接回错误码，不用逐个试探行情回包（无行情≠不存在）；
//! 3. 存在清单跨窗口合并（读旧文件→并新确认→重写），逐 tick 追加 JSONL 落盘。
//!
//! 用法: cargo run --release --bin 期权夜盘守护 -- [窗口秒数]
//! 不使用真实账号（BrokerID 7090 假账号 md_probe_guest），不触碰实盘 v1.4 会话。

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ctp2rs::ffi::{AssignFromString, WrapToString};
use ctp2rs::v1alpha1::{
    CThostFtdcDepthMarketDataField, CThostFtdcReqUserLoginField, CThostFtdcRspInfoField,
    CThostFtdcRspUserLoginField, CThostFtdcSpecificInstrumentField, MdApi, MdSpi,
};

const MD_DYNLIB: &str = "lib/libthostmduserapi_se.so";
const BROKER: &str = "7090";
const USER: &str = "md_probe_guest"; // 假账号：只测协议链路，不碰实盘账号
const PASS: &str = "md_probe_guest";
const DEFAULT_WINDOW_SECS: u64 = 480; // 单窗口默认 8 分钟（cron 每 30 分钟拉起）
const HEARTBEAT_SECS: u64 = 120; // 窗口内心跳间隔
const RECONNECT_WAIT_SECS: u64 = 3;

/// 黄金 au2702 期权候选网格：行权价 700~844 步长 8（黄金行权价间隔 8 元/克）
/// 2026-11-27 13:30 探针实测 752/768/800 有真实回包，主力价 783 → 平值档必在其中
const AU_STRIKES: [u32; 19] = [
    700, 708, 716, 724, 732, 740, 748, 756, 764, 772, 780, 788, 796, 804, 812, 820, 828, 836, 844,
];
/// 白银 ag2702 期权候选网格：行权价 9000~11000 步长 100（白银行权价间隔 100 元/千克）
/// 13:30 探针实测 9800 有真实回包，主力价 10445 → 平值 10400/10500 应存在
const AG_STRIKES: [u32; 21] = [
    9000, 9100, 9200, 9300, 9400, 9500, 9600, 9700, 9800, 9900, 10000, 10100, 10200, 10300, 10400,
    10500, 10600, 10700, 10800, 10900, 11000,
];

/// 生成订阅清单：标的期货 + 黄金/白银全网格 C/P
fn 生成订阅清单() -> Vec<String> {
    let mut v = vec![
        "au2702".to_string(),
        "ag2702".to_string(), // 标的期货：IV 计算需要标的实时价
    ];
    for k in AU_STRIKES {
        v.push(format!("au2702C{k}"));
        v.push(format!("au2702P{k}"));
    }
    for k in AG_STRIKES {
        v.push(format!("ag2702C{k}"));
        v.push(format!("ag2702P{k}"));
    }
    v
}

/// 期权合约名解析（SHFE 无 '-' 格式，与 md_probe_期权.rs 同一套规则）
/// 返回（方向 CALL/PUT，行权价，标的合约）；期货名解析不出方向返回 None
fn 解析期权合约(名: &str) -> Option<(String, f64, String)> {
    let mut 品种 = String::new();
    let mut 断字符: Option<char> = None;
    let mut chars = 名.chars();
    for c in chars.by_ref() {
        if c.is_ascii_alphabetic() {
            品种.push(c);
        } else {
            断字符 = Some(c); // for 循环 next() 已消费该字符，必须接回剩余流
            break;
        }
    }
    let 剩余: String = 断字符.into_iter().chain(chars).collect(); // 形如 "2702C768"
    let 月份: String = 剩余.chars().take_while(char::is_ascii_digit).collect();
    let 方向与行权价: String = 剩余.chars().skip_while(char::is_ascii_digit).collect();
    let mut 方向行权价 = 方向与行权价.chars();
    let 方向 = match 方向行权价.next()? {
        'C' => "CALL",
        'P' => "PUT",
        _ => return None,
    };
    let 行权价: f64 = 方向行权价.collect::<String>().parse().ok()?;
    if 品种.is_empty() || 月份.len() != 4 {
        return None;
    }
    Some((方向.to_string(), 行权价, format!("{品种}{月份}")))
}

/// CTP 无效价过滤：NaN / DBL_MAX / 负值 统一写 null，避免 JSON 出现 1.79e308 垃圾值
fn 价(p: f64) -> String {
    if p.is_finite() && p > 0.0 && p < 1.0e300 {
        format!("{p}")
    } else {
        "null".to_string()
    }
}

/// 当前北京时间完整格式 "YYYY-MM-DD HH:MM:SS"（文档口径统一北京时间）
fn bjt_now() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let s = secs + 8 * 3600;
    let days = s / 86400;
    let tod = s % 86400;
    let (h, m, sec) = (tod / 3600, (tod % 3600) / 60, tod % 60);
    let (y, mo, d) = civil_from_days(days as i64);
    format!("{y:04}-{mo:02}-{d:02} {h:02}:{m:02}:{sec:02}")
}

/// 当前北京时间 unix 秒（仅用于窗口计时，不含日期语义）
fn bjt_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() + 8 * 3600)
}

const fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 146_096);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (y, m, d)
}

#[derive(Debug)]
enum Evt {
    Connected,
    Disconnected(i32),
    Login(Option<CThostFtdcRspUserLoginField>, Option<CThostFtdcRspInfoField>),
    SubRsp(Option<CThostFtdcRspInfoField>, String),
    Tick(Box<CThostFtdcDepthMarketDataField>),
}

struct 采集Spi {
    tx: mpsc::SyncSender<Evt>,
}

impl MdSpi for 采集Spi {
    fn on_front_connected(&mut self) {
        let _ = self.tx.send(Evt::Connected);
    }
    fn on_front_disconnected(&mut self, reason: i32) {
        let _ = self.tx.send(Evt::Disconnected(reason));
    }
    fn on_rsp_user_login(
        &mut self,
        rsp_user_login: Option<&CThostFtdcRspUserLoginField>,
        rsp_info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        _is_last: bool,
    ) {
        let _ = self.tx.send(Evt::Login(rsp_user_login.copied(), rsp_info.copied()));
    }
    fn on_rsp_sub_market_data(
        &mut self,
        specific_instrument: Option<&CThostFtdcSpecificInstrumentField>,
        rsp_info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        _is_last: bool,
    ) {
        let code = specific_instrument.map_or_default(|s| s.InstrumentID.to_string());
        let _ = self.tx.send(Evt::SubRsp(rsp_info.copied(), code));
    }
    fn on_rtn_depth_market_data(&mut self, data: Option<&CThostFtdcDepthMarketDataField>) {
        if let Some(q) = data {
            let _ = self.tx.send(Evt::Tick(Box::new(*q)));
        }
    }
}

/// 窗口统计（跨重连会话累计，窗口结束写 JSON）
#[derive(Default)]
struct 窗口统计 {
    连接次数: AtomicU64,
    断线次数: AtomicU64,
    总tick: AtomicU64,
    落盘行: AtomicU64,
}

fn main() {
    let 窗口秒: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_WINDOW_SECS);
    let 开始 = bjt_now();
    println!("== 期权夜盘采集器启动 {开始}，窗口 {窗口秒}s ==");
    let 清单 = 生成订阅清单();
    println!(
        "订阅 {} 条：标的期货 2 + au 网格 {}×2(700-844/8) + ag 网格 {}×2(9000-11000/100)",
        清单.len(),
        AU_STRIKES.len(),
        AG_STRIKES.len()
    );

    let 存盘目录 = PathBuf::from("/home/z/my-project/ctp测试/期权探针_输出");
    let _ = fs::create_dir_all(&存盘目录);
    let 统计 = Arc::new(窗口统计::default());
    // 存在性判定表：跨窗口合并——先读旧清单文件，本窗口订阅确认再并入重写
    let 存在表: Arc<std::sync::Mutex<HashMap<String, bool>>> =
        Arc::new(std::sync::Mutex::new(读存在清单(&存盘目录)));

    let 窗口截止 = Instant::now() + Duration::from_secs(窗口秒);
    let mut 会话序 = 0u64;
    let mut 结束原因 = String::from("窗口到期");

    while Instant::now() < 窗口截止 {
        会话序 += 1;
        println!("-- 会话 #{会话序} 启动 {} --", bjt_now());
        结束原因 = 采集一轮(&清单, &存盘目录, &统计, &存在表, 窗口截止);
        if Instant::now() >= 窗口截止 {
            break;
        }
        println!("-- 会话 #{会话序} 提前结束（{结束原因}），{RECONNECT_WAIT_SECS}s 后重连 --");
        统计.断线次数.fetch_add(1, Ordering::Relaxed);
        thread::sleep(Duration::from_secs(RECONNECT_WAIT_SECS));
    }

    // 收尾：合并写存在清单 + 窗口结果 JSON（追加到 jsonl 供 cron 链读取）
    写存在清单(&存盘目录, &存在表);
    let 表 = 存在表.lock().map(|t| t.clone()).unwrap_or_default();
    let 存在数 = 表.values().filter(|v| **v).count();
    let 不存在数 = 表.values().filter(|v| !**v).count();
    let 总tick = 统计.总tick.load(Ordering::Relaxed);
    let 落盘行 = 统计.落盘行.load(Ordering::Relaxed);
    println!();
    println!("== 窗口结束 {}（原因:{结束原因}）==", bjt_now());
    println!(
        "汇总: 总tick {总tick}, 落盘 {落盘行} 行, 连接 {} 次, 断线 {} 次",
        统计.连接次数.load(Ordering::Relaxed),
        统计.断线次数.load(Ordering::Relaxed),
    );
    println!("存在清单累计: {存在数} 条确认存在, {不存在数} 条确认不存在（文件: 存在合约清单_夜盘实存.txt）");
    if 总tick > 0 {
        // 抓一条成交样例
        for tag in ["夜盘tick_57.jsonl"] {
            if let Ok(text) = fs::read_to_string(存盘目录.join(tag)) {
                if let Some(line) = text
                    .lines()
                    .rev()
                    .find(|l| l.contains("\"成交量\":") && !l.contains("\"成交量\":0}"))
                {
                    println!("末段真实成交样例: {line}");
                }
            }
        }
    }
    let json = format!(
        "{{\"time\":\"{}\",\"窗口秒\":{窗口秒},\"结束原因\":\"{结束原因}\",\"会话数\":{会话序},\"总tick\":{总tick},\"落盘行\":{落盘行},\"连接次数\":{},\"断线次数\":{},\"存在累计\":{存在数},\"不存在累计\":{不存在数}}}",
        bjt_now(),
        统计.连接次数.load(Ordering::Relaxed),
        统计.断线次数.load(Ordering::Relaxed),
    );
    if let Ok(mut f) = OpenOptions::new()
        .create(true)
        .append(true)
        .open("/home/z/my-project/download/夜盘期权守护_结果.jsonl")
    {
        let _ = writeln!(f, "{json}");
    }
    println!("结果已追加 download/夜盘期权守护_结果.jsonl");
}

/// 读旧存在清单（跨窗口合并的基础）；格式兼容：期权行 7 列逗号分隔，期货行跳过
fn 读存在清单(存盘目录: &PathBuf) -> HashMap<String, bool> {
    let mut m = HashMap::new();
    if let Ok(text) = fs::read_to_string(存盘目录.join("存在合约清单_夜盘实存.txt")) {
        for line in text.lines() {
            if line.starts_with('#') || line.trim().is_empty() {
                continue;
            }
            if let Some(名) = line.split(',').next() {
                if !名.is_empty() {
                    m.insert(名.to_string(), true); // 旧清单里的都是上轮确认存在的
                }
            }
        }
    }
    m
}

/// 写存在清单（全量重写：期权 7 列口径 + 注释头）
fn 写存在清单(存盘目录: &PathBuf, 存在表: &Arc<std::sync::Mutex<HashMap<String, bool>>>) {
    let Ok(表) = 存在表.lock() else { return };
    let mut 存在: Vec<&String> = 表.iter().filter(|(_, v)| **v).map(|(k, _)| k).collect();
    存在.sort();
    let mut 不存在: Vec<&String> = 表.iter().filter(|(_, v)| !**v).map(|(k, _)| k).collect();
    不存在.sort();
    let mut 文本 = String::from(
        "# InstrumentID,ExchangeID,ProductID,OptionsType,ProductClass,VolumeMultiple,ExpireDate\n",
    );
    文本.push_str("# 来源：期权夜盘采集器 订阅确认法（CTP ErrorID==0 = 交易所确认存在）2026-11-27 夜盘\n");
    文本.push_str("# ExpireDate/乘数列为待核补字段（行情 API 无合约静态查询），订阅存在性已实盘验证\n");
    for k in &存在 {
        let Some((方向, _, _)) = 解析期权合约(k) else { continue };
        let 品种 = k.chars().take_while(char::is_ascii_alphabetic).collect::<String>();
        let 期权类型 = if 方向 == "CALL" { "49" } else { "50" };
        let 乘数 = if 品种 == "au" { "1000" } else { "15" };
        文本.push_str(&format!("{k},SHFE,{品种},{期权类型},54,{乘数},\n"));
    }
    let _ = fs::write(存盘目录.join("存在合约清单_夜盘实存.txt"), 文本);
    let _ = fs::write(
        存盘目录.join("确认不存在_夜盘.txt"),
        format!("# CTP 订阅明确回错的合约（交易所确认不存在）\n{}", 不存在.iter().map(|s| s.as_str()).collect::<Vec<_>>().join("\n")),
    );
}

/// 一轮采集会话：连接 → 登录 → 订阅 → 采 tick 落盘；返回结束原因
fn 采集一轮(
    清单: &[String],
    存盘目录: &PathBuf,
    统计: &Arc<窗口统计>,
    存在表: &Arc<std::sync::Mutex<HashMap<String, bool>>>,
    窗口截止: Instant,
) -> String {
    // 每会话独立 flow 目录，避免 CTP 流文件跨会话重演回放旧 tick（假 bar 拦截口径）
    let 会话tag = format!("守护{}", bjt_unix() % 1_000_000);
    let flow_str = format!("./flow/{会话tag}");
    let _ = fs::create_dir_all(&flow_str);
    let 存盘路径: PathBuf = 存盘目录.join("夜盘tick_57.jsonl");

    let mdapi = MdApi::create_api(MD_DYNLIB, &flow_str, false, false);
    let (tx, rx) = mpsc::sync_channel(16_384);
    let spi = Box::new(采集Spi { tx });
    let spi_ptr = Box::into_raw(spi) as *mut dyn MdSpi;
    mdapi.register_spi(spi_ptr);
    mdapi.register_front("tcp://101.231.185.57:61213"); // 单前置轻量采集
    mdapi.init();

    let t0 = Instant::now();
    let mut last_heartbeat = Instant::now();

    // 阶段1：连接
    let mut connected = false;
    while Instant::now() < t0 + Duration::from_secs(15) {
        if Instant::now() >= 窗口截止 {
            return "窗口到期".into();
        }
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(Evt::Connected) => {
                connected = true;
                break;
            }
            Ok(Evt::Disconnected(n)) => return format!("连前断线 reason={n}"),
            Ok(_) => {}
            Err(_) => break,
        }
    }
    if !connected {
        return "15s 未连接".into();
    }
    统计.连接次数.fetch_add(1, Ordering::Relaxed);

    // 阶段2：假账号登录
    let mut req = CThostFtdcReqUserLoginField::default();
    req.BrokerID.assign_from_str(BROKER);
    req.UserID.assign_from_str(USER);
    req.Password.assign_from_str(PASS);
    let _ = mdapi.req_user_login(&mut req, 1);
    let mut login_ok = false;
    while Instant::now() < t0 + Duration::from_secs(30) {
        if Instant::now() >= 窗口截止 {
            return "窗口到期".into();
        }
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(Evt::Login(_, info)) => {
                let err_id = info.map_or(-1, |e| e.ErrorID);
                login_ok = err_id == 0;
                if !login_ok {
                    return format!("登录被拒 err={err_id}");
                }
                break;
            }
            Ok(Evt::Disconnected(n)) => return format!("登录前断线 reason={n}"),
            Ok(_) => {}
            Err(_) => break,
        }
    }
    if !login_ok {
        return "30s 未登录成功".into();
    }
    println!("   [{}] 登录成功，订阅 {} 条", bjt_now(), 清单.len());

    // 阶段3：分批订阅（每批 60 条防单包过大）
    for 批 in 清单.chunks(60) {
        let refs: Vec<&str> = 批.iter().map(String::as_str).collect();
        let _ = mdapi.subscribe_market_data(&refs);
    }

    // 阶段4：窗口主循环
    let 存盘文件 = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&存盘路径)
        .ok();
    let Some(文件) = 存盘文件 else {
        return "存盘文件打开失败".into();
    };
    let mut writer = BufWriter::new(文件);

    while Instant::now() < 窗口截止 {
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(Evt::Tick(q)) => {
                统计.总tick.fetch_add(1, Ordering::Relaxed);
                let code = q.InstrumentID.to_string();
                let (方向, 行权价, 标的) =
                    解析期权合约(&code).unwrap_or_else(|| ("FUTURES".into(), 0.0, code.clone()));
                let 行 = format!(
                    "{{\"合约\":\"{code}\",\"最新价\":{},\"买一\":{},\"买一量\":{},\"卖一\":{},\"卖一量\":{},\"成交量\":{},\"持仓量\":{},\"行权价\":{行权价},\"方向\":\"{方向}\",\"标的\":\"{标的}\",\"交易所时间\":\"{}\",\"毫秒\":{},\"动作日\":\"{}\"}}",
                    价(q.LastPrice),
                    价(q.BidPrice1),
                    q.BidVolume1,
                    价(q.AskPrice1),
                    q.AskVolume1,
                    q.Volume,
                    q.OpenInterest,
                    q.UpdateTime.to_string(),
                    q.UpdateMillisec,
                    q.ActionDay.to_string(),
                );
                if writeln!(writer, "{行}").is_ok() {
                    统计.落盘行.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(Evt::SubRsp(info, code)) => {
                // 订阅确认 Some(ErrorID==0)=存在；ErrorID!=0=CTP 明确告知不存在
                let err_id = info.map_or(-1, |e| e.ErrorID);
                if let Ok(mut t) = 存在表.lock() {
                    t.insert(code, err_id == 0);
                }
            }
            Ok(Evt::Disconnected(n)) => {
                let _ = writer.flush();
                return format!("断线 reason={n}");
            }
            Ok(_) => {}
            Err(_) => {}
        }
        if last_heartbeat.elapsed() >= Duration::from_secs(HEARTBEAT_SECS) {
            last_heartbeat = Instant::now();
            let _ = writer.flush();
            let 总 = 统计.总tick.load(Ordering::Relaxed);
            let 盘 = 统计.落盘行.load(Ordering::Relaxed);
            let 存在数 = 存在表
                .lock()
                .map(|t| t.values().filter(|v| **v).count())
                .unwrap_or(0);
            println!(
                "   [{}] 心跳: 窗口tick {总}, 落盘 {盘}, 存在确认 {存在数}, 速率 {:.1} tick/s",
                bjt_now(),
                (总 as f64) / t0.elapsed().as_secs_f64().max(1.0),
            );
        }
    }
    let _ = writer.flush();
    "窗口到期".to_string()
}

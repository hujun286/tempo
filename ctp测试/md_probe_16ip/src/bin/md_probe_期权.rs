#![allow(
    clippy::print_stdout,
    reason = "期权探针的职责就是输出测试结果，本程序即报告输出器"
)]
#![allow(clippy::print_stderr, reason = "探针错误必须走 stderr 提示用户")]
//! 期权行情探针：连接 + 登录 + 订阅期权 + 逐 tick 数据存盘 四合一验证
//!
//! 与期货探针（md_probe_16ip.rs）的差异：
//! 1. 订阅对象全部是期权合约（黄金/白银 SHFE 平值与虚值 + 中金所股指期权 IO）；
//! 2. CTP 行情快照不含期权专属字段（行权价/期权类型在合约静态结构 InstrumentField 里，
//!    行情 API 无查询接口），故期权语义由合约名携带并在本程序解析；
//! 3. 每个 tick 追加一行 JSONL 到存盘文件（数据存盘验证），
//!    判定标准：存盘行数 == 收到的 tick 数 == 0 缺口。
//!
//! 不使用真实账号（BrokerID 7090 假账号 md_probe_guest），不触碰实盘 v1.4 会话。

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use ctp2rs::ffi::{AssignFromString, WrapToString};
use ctp2rs::v1alpha1::{
    CThostFtdcDepthMarketDataField, CThostFtdcReqUserLoginField, CThostFtdcRspInfoField,
    CThostFtdcRspUserLoginField, CThostFtdcSpecificInstrumentField, MdApi, MdSpi,
};

const MD_DYNLIB: &str = "lib/libthostmduserapi_se.so";
const BROKER: &str = "7090";
const USER: &str = "md_probe_guest"; // 假账号：只测协议链路，不碰实盘账号
const PASS: &str = "md_probe_guest";
const WINDOW_SECS: u64 = 90; // 期权 tick 采样窗（90 秒，覆盖至少 1 个分钟边界）

/// 期权订阅清单：黄金平值 C/P + 黄金虚值 C/P + 白银平值 C/P + 中金所股指期权平值 C/P
const OPT_CONTRACTS: [&str; 8] = [
    "au2702C768",
    "au2702P768",
    "au2702C800",
    "au2702P752",
    "ag2702C9800",
    "ag2702P9800",
    "IO2612-C-4400",
    "IO2612-P-4400",
];

/// 探针 IP 只取 4 个（两地分布）：期权合约少、tick 稀疏，4 前置足够验证
const PROBE_IPS: [&str; 4] = [
    "101.231.185.57",
    "101.231.185.60",
    "112.65.132.105",
    "140.206.102.76",
];

#[derive(Debug)]
enum Evt {
    Connected,
    Disconnected(i32),
    Login(
        Option<CThostFtdcRspUserLoginField>,
        Option<CThostFtdcRspInfoField>,
    ),
    SubRsp(Option<CThostFtdcRspInfoField>, String),
    Tick(Box<CThostFtdcDepthMarketDataField>), // 期权 tick：整包传递（落盘需要多字段）
    Err(i32, String),
}

struct ProbeSpi {
    tx: mpsc::SyncSender<Evt>,
}

impl MdSpi for ProbeSpi {
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
        let _ = self
            .tx
            .send(Evt::Login(rsp_user_login.copied(), rsp_info.copied()));
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
    fn on_rsp_error(
        &mut self,
        p_rsp_info: Option<&CThostFtdcRspInfoField>,
        n_request_id: i32,
        _b_is_last: bool,
    ) {
        if let Some(e) = p_rsp_info {
            let _ = self.tx.send(Evt::Err(
                n_request_id,
                format!("{} {}", e.ErrorID, e.ErrorMsg.to_string()),
            ));
        }
    }
}

/// 期权合约名解析（CTP 行情快照无期权字段，语义由合约名携带）。
/// 返回（方向 CALL/PUT，行权价，标的合约）。
/// 两种命名规则：
///   SHFE/INE: `au2702C768` —— 品种(字母) + 月份(4位数字) + 方向(C/P) + 行权价
///   CFFEX:    `IO2612-C-4400` —— 品种 + 月份 + '-' + 方向 + '-' + 行权价
fn 解析期权合约(名: &str) -> Option<(String, f64, String)> {
    if 名.contains('-') {
        // CFFEX 格式：按 '-' 分三段
        let 段: Vec<&str> = 名.split('-').collect();
        if 段.len() != 3 {
            return None;
        }
        let 品种月份 = 段.first()?.to_string();
        let 方向 = match 段.get(1)? {
            &"C" => "CALL",
            &"P" => "PUT",
            _ => return None,
        };
        let 行权价: f64 = 段.get(2)?.parse().ok()?;
        Some((方向.to_string(), 行权价, 品种月份))
    } else {
        // SHFE 格式：字母段(品种) + 数字段(月份) + C/P + 行权价
        // 一次收集全部剩余字符再分段，避免逐段消费时"方向字符被月份循环吃掉"的边界 bug
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
        let 月份: String = 剩余.chars().take_while(|c| c.is_ascii_digit()).collect();
        let 方向与行权价: String =
            剩余.chars().skip_while(|c| c.is_ascii_digit()).collect();
        let mut 方向行权价 = 方向与行权价.chars();
        let 方向 = match 方向行权价.next() {
            Some('C') => "CALL",
            Some('P') => "PUT",
            _ => return None,
        };
        let 行权价段: String = 方向行权价.collect();
        let 行权价: f64 = 行权价段.parse().ok()?;
        if 品种.is_empty() || 月份.len() != 4 {
            return None;
        }
        let 标的 = format!("{品种}{月份}");
        Some((方向.to_string(), 行权价, 标的))
    }
}

/// CTP 无效价过滤：NaN / DBL_MAX / 负值 统一写 null，避免 JSON 出现 1.79e308 垃圾值
fn 价(p: f64) -> String {
    if p.is_finite() && p > 0.0 && p < 1.0e300 {
        format!("{p}")
    } else {
        "null".to_string()
    }
}

#[derive(Default)]
struct Result1 {
    ip: String,
    connected: bool,
    connect_ms: u64,
    login_ret: i32,
    login_err_id: i32,
    login_err_msg: String,
    trading_day: String,
    sub_ok: usize,
    sub_err: usize,
    ticks: u32,
    落盘行数: u64,
    last_tick: String,
    样例行: String,
    flow_err: Option<String>,
}

fn probe_one(ip: &str) -> Result1 {
    let mut r = Result1 {
        ip: ip.to_string(),
        ..Default::default()
    };
    let tag = ip.split('.').next_back().unwrap_or("x");
    let flow: PathBuf = format!("./flow/ip_{tag}").into();
    let _ = fs::create_dir_all(&flow);
    let 存盘目录 = "/home/z/my-project/ctp测试/期权探针_输出";
    let _ = fs::create_dir_all(存盘目录);
    let 存盘路径: PathBuf = format!("{存盘目录}/期权tick_{tag}.jsonl").into();

    let Some(flow_str) = flow.to_str() else {
        r.flow_err = Some("flow 路径含非法 UTF-8".into());
        return r;
    };
    let mdapi = MdApi::create_api(MD_DYNLIB, flow_str, false, false);
    let (tx, rx) = mpsc::sync_channel(4096);
    let spi = Box::new(ProbeSpi { tx });
    let spi_ptr = Box::into_raw(spi) as *mut dyn MdSpi;
    mdapi.register_spi(spi_ptr);
    mdapi.register_front(&format!("tcp://{ip}:61213"));
    let t0 = Instant::now();
    mdapi.init();

    // 阶段1：等连接
    let mut connected = false;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(Evt::Connected) => {
                connected = true;
                break;
            }
            Ok(Evt::Disconnected(n)) => {
                r.flow_err = Some(format!("断线 reason={n}"));
                return r;
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
    if !connected {
        r.flow_err = Some(
            r.flow_err
                .unwrap_or_else(|| "10s 未收到 on_front_connected".into()),
        );
        return r;
    }
    r.connected = true;
    r.connect_ms = t0.elapsed().as_millis() as u64;

    // 阶段2：假账号登录
    let mut req = CThostFtdcReqUserLoginField::default();
    req.BrokerID.assign_from_str(BROKER);
    req.UserID.assign_from_str(USER);
    req.Password.assign_from_str(PASS);
    r.login_ret = mdapi.req_user_login(&mut req, 1);

    let mut login_ok = false;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(Evt::Login(login, info)) => match info {
                None => {
                    login_ok = true;
                }
                Some(e) => {
                    r.login_err_id = e.ErrorID;
                    r.login_err_msg = e.ErrorMsg.to_string();
                    login_ok = e.ErrorID == 0;
                },
            },
            Ok(Evt::Err(_, msg)) => {
                r.login_err_msg = format!("RspError {msg}");
                break;
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
    if !login_ok {
        return r;
    }

    // 阶段3：订阅期权合约
    let v: Vec<&str> = OPT_CONTRACTS.to_vec();
    let _ = mdapi.subscribe_market_data(&v);

    // 阶段4：tick 采样 + 逐笔落盘
    let 存盘文件 = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&存盘路径)
        .ok();
    let mut writer = 存盘文件.map(BufWriter::new);
    let deadline = Instant::now() + Duration::from_secs(WINDOW_SECS);
    while Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(Evt::Tick(q)) => {
                r.ticks += 1;
                let code = q.InstrumentID.to_string();
                let (方向, 行权价, 标的) =
                    解析期权合约(&code).unwrap_or_else(|| ("UNKNOWN".into(), 0.0, code.clone()));
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
                r.last_tick = format!("{code}={}@{}", 价(q.LastPrice), q.UpdateTime.to_string());
                if r.样例行.is_empty() && q.Volume > 0 {
                    r.样例行 = 行.clone();
                }
                if let Some(w) = writer.as_mut() {
                    if writeln!(w, "{行}").is_ok() {
                        r.落盘行数 += 1;
                    }
                } else {
                    r.flow_err = Some("存盘文件打开失败".into());
                }
            }
            Ok(Evt::SubRsp(info, code)) => match info {
                None => r.sub_ok += 1,
                Some(e) => {
                    r.sub_err += 1;
                    if e.ErrorID != 0 {
                        r.flow_err = Some(format!(
                            "订阅 {code}: {} {}",
                            e.ErrorID,
                            e.ErrorMsg.to_string()
                        ));
                    }
                }
            },
            Ok(Evt::Err(_, msg)) => {
                r.flow_err = Some(format!("订阅窗错误 {msg}"));
            }
            Ok(Evt::Disconnected(n)) => {
                r.flow_err = Some(format!("采样窗断线 reason={n}"));
                break;
            }
            Ok(_) => {}
            Err(_) => {}
        }
    }
    // BufWriter flush（落盘完整性判定前提）
    if let Some(w) = writer.as_mut() {
        let _ = w.flush();
    }
    r
}

fn main() {
    println!("== 期权行情探针（连接+订阅+数据存盘三合一） {} ==", chrono_like_now());
    println!("-- 合约名解析自测 --");
    for c in OPT_CONTRACTS.iter() {
        println!("  {c} -> {:?}", 解析期权合约(c));
    }
    println!("--");
    println!(
        "so=lib/libthostmduserapi_se.so(CTP 6.7.7 同实盘v1.4)  Broker={BROKER}  假账号={USER}  采样窗={WINDOW_SECS}s"
    );
    println!("订阅期权 {OPT_CONTRACTS:?}");
    println!("探针 IP {PROBE_IPS:?}（期权合约少，4 前置足够）");
    println!();

    let t0 = Instant::now();
    let handles: Vec<_> = PROBE_IPS
        .iter()
        .map(|ip| {
            let ip = ip.to_string();
            thread::spawn(move || probe_one(&ip))
        })
        .collect();
    let mut results = Vec::new();
    for h in handles {
        match h.join() {
            Ok(r) => results.push(r),
            Err(e) => println!("线程崩溃: {e:?}"),
        }
    }
    results.sort_by(|a, b| a.ip.cmp(&b.ip));

    println!("IP              连接  耗时   登录     订阅   ticks  落盘    最后期权行情");
    for r in &results {
        let login_txt = if r.login_ret == 0 && r.login_err_id == 0 {
            "OK"
        } else {
            "拒"
        };
        println!(
            "{ip} {flag} {ms:>4}ms {login_txt:<5} sub={ok}/{tot} {ticks:>5}  {disk:>5}  {last}",
            ip = r.ip,
            flag = if r.connected { "✅" } else { "❌" },
            ms = r.connect_ms,
            ok = r.sub_ok,
            tot = r.sub_ok + r.sub_err,
            ticks = r.ticks,
            disk = r.落盘行数,
            last = r.last_tick,
        );
    }
    let ok_conn = results.iter().filter(|r| r.connected).count();
    let ok_tick = results.iter().filter(|r| r.ticks > 0).count();
    let 落盘总数: u64 = results.iter().map(|r| r.落盘行数).sum();
    let tick总数: u32 = results.iter().map(|r| r.ticks).sum();
    let 存盘一致 = results.iter().all(|r| r.落盘行数 == u64::from(r.ticks));
    println!();
    println!(
        "汇总: 连接 {ok_conn}/4, 期权tick {ok_tick}/4, 总tick {tick总数}, 落盘 {落盘总数} 行, 存盘完整性 {}",
        if 存盘一致 { "✅(行数==tick数)" } else { "❌(有缺口)" }
    );

    // 非空文件里抓一条真实样例行给报告
    let 样盘 = "/home/z/my-project/ctp测试/期权探针_输出";
    let mut 样例 = String::new();
    for tag in PROBE_IPS.iter() {
        let tag = tag.split('.').next_back().unwrap_or("x");
        if let Ok(text) = fs::read_to_string(format!("{样盘}/期权tick_{tag}.jsonl")) {
            if let Some(line) = text.lines().find(|l| l.contains("\"成交量\":") && !l.contains("\"成交量\":0")) {
                样例 = line.to_string();
                break;
            }
        }
    }
    if !样例.is_empty() {
        println!("真实期权tick样例:");
        println!("  {样例}");
    }

    let json = format!(
        "{{\"time\":\"{}\",\"窗口秒\":{WINDOW_SECS},\"合约\":{:?},\"summary\":{{\"connected\":{ok_conn},\"got_ticks\":{ok_tick},\"总tick\":{tick总数},\"落盘行\":{落盘总数},\"存盘完整\":{}}}}}",
        chrono_like_now(),
        OPT_CONTRACTS,
        存盘一致
    );
    let _ = fs::write(
        "/home/z/my-project/download/测试期权_连接与存盘_结果.json",
        json,
    );
    println!("结果已存 download/测试期权_连接与存盘_结果.json + ctp测试/期权探针_输出/期权tick_*.jsonl");
}

fn chrono_like_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let s = secs + 8 * 3600;
    let days = s / 86400;
    let tod = s % 86400;
    let (h, m, sec) = (tod / 3600, (tod % 3600) / 60, tod % 60);
    let (y, mo, d) = civil_from_days(days as i64);
    format!("{y:04}-{mo:02}-{d:02} {h:02}:{m:02}:{sec:02}")
}

const fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

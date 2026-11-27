#![allow(
    clippy::print_stdout,
    reason = "行情探针的职责就是输出测试结果，本程序即报告输出器"
)]
#![allow(clippy::print_stderr, reason = "探针错误必须走 stderr 提示用户")]
//! 16 个行情前置 IP 真实协议层测试（2026-11-26 13:00 股指期货交易时段）
//!
//! `流程：register_front` -> init -> `on_front_connected` -> `req_user_login（假账号，BrokerID` 7090）
//!       -> `on_rsp_user_login` -> `subscribe_market_data(IF/IM/IC)` -> `on_rtn_depth_market_data` 计数
//! 不使用真实账号，不触碰 v1.4 实盘会话（行情前置天然支持多会话，各自独立）。
//! 运行时 so 与实盘 v1.4 完全一致（CTP 6.7.7 `libthostmduserapi_se.so`）。
//!
//! 输出：每 IP 连接/登录/订阅/行情 汇总表 + JSON 存档。

use std::fs;
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
const WINDOW_SECS: u64 = 60; // tick 采样窗
const CONTRACTS: [&str; 4] = ["IF2612", "IF2703", "IM2612", "IC2612"];

const MEMBER_IPS: [&str; 16] = [
    "101.231.185.57",
    "101.231.185.58",
    "101.231.185.59",
    "101.231.185.60",
    "180.166.83.72",
    "180.166.83.73",
    "180.166.83.74",
    "180.166.83.75",
    "140.206.102.76",
    "140.206.102.77",
    "140.206.102.78",
    "112.65.132.105",
    "112.65.132.21",
    "112.65.132.22",
    "112.65.132.23",
    "112.65.132.25",
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
    Tick(String, f64, String), // 合约, 最新价, 交易所时间
    Err(i32, String),          // request_id, 错误串
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
            let t = format!(
                "{}:{}:{}",
                q.UpdateTime.to_string(),
                q.UpdateMillisec,
                q.ActionDay.to_string()
            );
            let _ = self
                .tx
                .send(Evt::Tick(q.InstrumentID.to_string(), q.LastPrice, t));
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

#[derive(Default)]
struct Result1 {
    ip: String,
    connected: bool,
    connect_ms: u64,
    login_ret: i32,    // req_user_login 返回值
    login_err_id: i32, // 回报 ErrorID（0=成功）
    login_err_msg: String,
    trading_day: String,
    sub_ok: usize, // 订阅成功合约数
    sub_err: usize,
    ticks: u32,
    last_tick: String, // 合约=价格@时间
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

    // 阶段1：等连接（10s）
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
            Ok(other) => {
                r.flow_err = Some(format!("连接前异常事件 {other:?}"));
            }
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

    // 阶段2：假账号登录（协议层验证）
    let mut req = CThostFtdcReqUserLoginField::default();
    req.BrokerID.assign_from_str(BROKER);
    req.UserID.assign_from_str(USER);
    req.Password.assign_from_str(PASS);
    r.login_ret = mdapi.req_user_login(&mut req, 1);

    let mut login_ok = false;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(Evt::Login(login, info)) => {
                match info {
                    None => {
                        login_ok = true;
                        r.login_err_id = 0;
                    }
                    Some(e) => {
                        r.login_err_id = e.ErrorID;
                        r.login_err_msg = e.ErrorMsg.to_string();
                        login_ok = e.ErrorID == 0;
                    }
                }
                if let Some(l) = login {
                    r.trading_day = l.TradingDay.to_string();
                }
                break;
            }
            Ok(Evt::Err(_, msg)) => {
                r.login_err_msg = format!("RspError {msg}");
                break;
            }
            Ok(other) => {
                r.flow_err = Some(format!("登录期异常事件 {other:?}"));
            }
            Err(_) => break,
        }
    }
    if !login_ok {
        // 登录被拒也保留已连接事实（协议层已验证），不再订阅
        return r;
    }

    // 阶段3：订阅
    let v: Vec<&str> = CONTRACTS.to_vec();
    let _ = mdapi.subscribe_market_data(&v);

    // 阶段4：tick 采样窗
    let deadline = Instant::now() + Duration::from_secs(WINDOW_SECS);
    while Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(Evt::Tick(code, last, t)) => {
                r.ticks += 1;
                r.last_tick = format!("{code}={last}@{t}");
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
    r
}

fn main() {
    println!("== 16 IP 行情前置真实协议层测试  {} ==", chrono_like_now());
    println!(
        "so=lib/libthostmduserapi_se.so (与实盘v1.4同版本)  Broker={BROKER}  假账号={USER}  采样窗={WINDOW_SECS}s  合约={CONTRACTS:?}"
    );
    println!("(不触碰实盘 v1.4 会话：行情前置多会话天然独立)");
    println!();

    let t0 = Instant::now();
    let handles: Vec<_> = MEMBER_IPS
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

    println!("IP              连接  耗时   登录ret  ErrID  交易日      订阅    ticks  最后行情");
    for r in &results {
        let login_txt = if r.login_err_id == 0 && r.login_ret == 0 && r.trading_day.is_empty() {
            "未收到回报".to_string()
        } else if r.login_err_id == 0 && !r.trading_day.is_empty() {
            "OK".to_string()
        } else {
            format!("E{} {}", r.login_err_id, r.login_err_msg)
        };
        println!(
            "{ip} {flag} {ms:>4}ms login[ret={ret} {txt}] sub={ok}/{tot} ticks={ticks:>4} last={last}",
            ip = r.ip,
            flag = if r.connected { "✅" } else { "❌" },
            ms = r.connect_ms,
            ret = r.login_ret,
            txt = login_txt,
            ok = r.sub_ok,
            tot = r.sub_ok + r.sub_err,
            ticks = r.ticks,
            last = r.last_tick,
        );
    }
    let ok_conn = results.iter().filter(|r| r.connected).count();
    let ok_tick = results.iter().filter(|r| r.ticks > 0).count();
    println!();
    println!(
        "汇总: 连接 {ok_conn}/16, 收到真实tick {ok_tick}/16, 总耗时 {}s",
        t0.elapsed().as_secs()
    );

    let json = serde_json_like(&results);
    let out = format!(
        "{{\"time\":\"{}\",\"so\":\"CTP 6.7.7 (同实盘v1.4)\",\"broker\":\"{BROKER}\",\"user\":\"{USER}\",\"window_secs\":{WINDOW_SECS},\"results\":{json},\"summary\":{{\"connected\":{ok_conn},\"got_ticks\":{ok_tick}}}}}",
        chrono_like_now()
    );
    let _ = fs::write("/home/z/my-project/download/测试16IP_协议层_结果.json", out);
    println!("结果已存 download/测试16IP_协议层_结果.json");
}

fn chrono_like_now() -> String {
    // 避免引入 chrono：用 std 拿系统时间戳再转北京时间由调用方粗略展示
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    // 北京时间 = UTC+8
    let s = secs + 8 * 3600;
    let days = s / 86400;
    let tod = s % 86400;
    let (h, m, sec) = (tod / 3600, (tod % 3600) / 60, tod % 60);
    // 2026-01-01 是第 20454 天 (自1970)。粗算月日（够用即可）
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

// 最小 JSON 序列化（避免额外依赖）
fn serde_json_like(results: &[Result1]) -> String {
    let items: Vec<String> = results
        .iter()
        .map(|r| {
            format!(
                "{{\"ip\":\"{}\",\"connected\":{},\"connect_ms\":{},\"login_ret\":{},\"login_err_id\":{},\"login_err_msg\":\"{}\",\"trading_day\":\"{}\",\"sub_ok\":{},\"sub_err\":{},\"ticks\":{},\"last_tick\":\"{}\",\"flow_err\":\"{}\"}}",
                r.ip, r.connected, r.connect_ms, r.login_ret, r.login_err_id,
                esc(&r.login_err_msg), esc(&r.trading_day), r.sub_ok, r.sub_err,
                r.ticks, esc(&r.last_tick), esc(r.flow_err.as_deref().unwrap_or(""))
            )
        })
        .collect();
    format!("[{}]", items.join(","))
}

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

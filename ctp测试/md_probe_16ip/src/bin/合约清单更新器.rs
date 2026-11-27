#![allow(
    clippy::print_stdout,
    reason = "合约更新器的职责就是输出验证报告，本程序即报告输出器"
)]
#![allow(clippy::print_stderr, reason = "更新器错误必须走 stderr 提示用户")]
//! 合约清单每日更新器 v1（订阅验证法）
//!
//! 背景：现有 contracts_live.txt（29433 条，2026-11-2x 生成）会随时间过期——
//! 新合约上市、到期合约摘牌、期权新行权价档位生成。CTP 无合约查询的免费无账号路径
//! （TTS/SimNow 均需注册；交易所官网 HTTP 全线 JS 反爬），而行情订阅回报本身就是
//! 合约存在性的权威判定源：订阅确认（ErrorID=0）= 合约存活，订阅错误回报 = 摘牌。
//!
//! 三阶段流程：
//!   A. 存量验证：旧清单 29433 条按 16 前置分片，每片分批订阅（CTP 单次≤500 限制，
//!      实际 300/批），收 on_rsp_sub_market_data 回报分类：存活/摘牌/无回报。
//!   B. 档位推导：对 SHFE 期权品种，从清单统计行权价间隔，按当前期货价（订阅期货
//!      合约收 tick 的 LastPrice）推算平值±2 档新候选，再订阅验证候选存在性。
//!   C. diff 报告：新增/摘牌统计 + 生成新版清单文件（人工确认后替换正式清单）。
//!
//! 判定规则（对无回报的保守处理）：无回报合约不删——保留在"待验证"类，宁可误留。

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use ctp2rs::ffi::{AssignFromString, WrapToString};
use ctp2rs::v1alpha1::{
    CThostFtdcDepthMarketDataField, CThostFtdcReqUserLoginField, CThostFtdcRspInfoField,
    CThostFtdcSpecificInstrumentField, MdApi, MdSpi,
};

const MD_DYNLIB: &str = "lib/libthostmduserapi_se.so";
const BROKER: &str = "7090";
const USER: &str = "md_probe_guest";
const PASS: &str = "md_probe_guest";
const 清单路径: &str =
    "/home/z/my-project/ctp-workspace/行情数据采集/data_数据/contracts_合约清单/contracts_live.txt";
const 输出目录: &str = "/home/z/my-project/ctp测试/合约更新器_输出";

/// 验证用前置（4 个即可，两地分布；29433 条 ÷ 4 ≈ 7360 条/IP，300/批 × 25 批）
const VERIFY_IPS: [&str; 4] = [
    "101.231.185.57",
    "101.231.185.60",
    "112.65.132.105",
    "140.206.102.76",
];

/// 每批订阅数量（CTP 单次订阅上限约 500，留余量）
const BATCH_SIZE: usize = 300;
/// 每批等待回报的间隔（订阅回报是异步流，批间给回报留时间）
const BATCH_GAP_MS: u64 = 150;

#[derive(Debug)]
enum Evt {
    Connected,
    Disconnected(i32),
    Login(Option<CThostFtdcRspInfoField>),
    SubRsp(Option<CThostFtdcRspInfoField>, String),
    Tick(Box<CThostFtdcDepthMarketDataField>),
}

struct VerifySpi {
    tx: mpsc::SyncSender<Evt>,
}

impl MdSpi for VerifySpi {
    fn on_front_connected(&mut self) {
        let _ = self.tx.send(Evt::Connected);
    }
    fn on_front_disconnected(&mut self, reason: i32) {
        let _ = self.tx.send(Evt::Disconnected(reason));
    }
    fn on_rsp_user_login(
        &mut self,
        _rsp_user_login: Option<&ctp2rs::v1alpha1::CThostFtdcRspUserLoginField>,
        rsp_info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        _is_last: bool,
    ) {
        let _ = self.tx.send(Evt::Login(rsp_info.copied()));
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

/// 单前置验证结果
#[derive(Default)]
struct VerifyResult {
    ip: String,
    connected: bool,
    存活: BTreeSet<String>,
    摘牌: BTreeMap<String, i32>, // 合约 -> 错误码
    期货价: BTreeMap<String, f64>,
    flow_err: Option<String>,
}

/// 分片订阅验证一个前置上的合约子集
fn verify_slice(ip: &str, 合约集: &[String], 需要期货价: bool) -> VerifyResult {
    let mut r = VerifyResult {
        ip: ip.to_string(),
        ..Default::default()
    };
    let tag = ip.split('.').next_back().unwrap_or("x");
    let flow: PathBuf = format!("./flow/ip_{tag}").into();
    let _ = fs::create_dir_all(&flow);
    let Some(flow_str) = flow.to_str() else {
        r.flow_err = Some("flow 路径非法".into());
        return r;
    };
    let mdapi = MdApi::create_api(MD_DYNLIB, flow_str, false, false);
    let (tx, rx) = mpsc::sync_channel(8192);
    let spi = Box::new(VerifySpi { tx });
    let spi_ptr = Box::into_raw(spi) as *mut dyn MdSpi;
    mdapi.register_spi(spi_ptr);
    mdapi.register_front(&format!("tcp://{ip}:61213"));
    mdapi.init();

    // 连接 + 登录（总超时 15s）
    let mut ok = false;
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(Evt::Connected) => {
                let mut req = CThostFtdcReqUserLoginField::default();
                req.BrokerID.assign_from_str(BROKER);
                req.UserID.assign_from_str(USER);
                req.Password.assign_from_str(PASS);
                let _ = mdapi.req_user_login(&mut req, 1);
            }
            Ok(Evt::Login(info)) => {
                if info.map_or(true, |e| e.ErrorID == 0) {
                    ok = true;
                }
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
    if !ok {
        r.flow_err = Some("15s 内未完成连接登录".into());
        return r;
    }
    r.connected = true;

    // 分批订阅：每批订完等一小段收回报，再下一批
    for batch in 合约集.chunks(BATCH_SIZE) {
        let refs: Vec<&str> = batch.iter().map(String::as_str).collect();
        let _ = mdapi.subscribe_market_data(&refs);
        // 批间收回报（500ms 窗口里收到的 SubRsp/Tick 全部记账）
        let gap_deadline = Instant::now() + Duration::from_millis(BATCH_GAP_MS);
        while Instant::now() < gap_deadline {
            match rx.try_recv() {
                Ok(Evt::SubRsp(info, code)) => {
                    // CTP 订阅确认：Some(ErrorID=0) 即成功（不是 None）；ErrorID!=0 才是摘牌/无效
                    let err_id = info.map_or(0, |e| e.ErrorID);
                    if err_id == 0 {
                        r.存活.insert(code);
                    } else {
                        r.摘牌.insert(code, err_id);
                    }
                }
                Ok(Evt::Tick(q)) => {
                    if 需要期货价 {
                        let code = q.InstrumentID.to_string();
                        let p = q.LastPrice;
                        if p.is_finite() && p > 0.0 && p < 1.0e300 {
                            r.期货价.entry(code).or_insert(p);
                        }
                    }
                }
                Ok(_) => {}
                Err(mpsc::TryRecvError::Empty) => thread::sleep(Duration::from_millis(5)),
                Err(mpsc::TryRecvError::Disconnected) => break,
            }
        }
    }
    // 订阅完全部批次后再给回报流留最后一段收尾时间（订阅回报可能晚到）
    let tail_deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < tail_deadline {
        match rx.try_recv() {
            Ok(Evt::SubRsp(info, code)) => {
                // CTP 订阅确认：Some(ErrorID=0) 即成功（不是 None）；ErrorID!=0 才是摘牌/无效
                let err_id = info.map_or(0, |e| e.ErrorID);
                if err_id == 0 {
                    r.存活.insert(code);
                } else {
                    r.摘牌.insert(code, err_id);
                }
            },
            Ok(Evt::Tick(q)) => {
                if 需要期货价 {
                    let code = q.InstrumentID.to_string();
                    let p = q.LastPrice;
                    if p.is_finite() && p > 0.0 && p < 1.0e300 {
                        r.期货价.entry(code).or_insert(p);
                    }
                }
            }
            Ok(_) => {}
            Err(mpsc::TryRecvError::Empty) => thread::sleep(Duration::from_millis(5)),
            Err(mpsc::TryRecvError::Disconnected) => break,
        }
    }
    r
}

/// 读清单（跳过表头 ExchangeID 行）
fn 读清单() -> Vec<(String, String)> {
    // (合约, 交易所)
    let mut out = Vec::new();
    if let Ok(f) = fs::File::open(清单路径) {
        for line in BufRead::lines(BufReader::new(f)).map_while(Result::ok) {
            let mut parts = line.split(',');
            if let (Some(合约), Some(所)) = (parts.next(), parts.next()) {
                if 合约 == "ExchangeID" || 合约.is_empty() {
                    continue;
                }
                out.push((合约.to_string(), 所.to_string()));
            }
        }
    }
    out
}

/// SHFE/INE 期权合约解析：au2702C768 -> (品种=au, 月份=2702, CALL, 768)
/// 返回 None 表示不是该格式（含 CFFEX 横杠格式与其他类型）
fn 解析期权(名: &str) -> Option<(String, String, bool, f64)> {
    if 名.contains('-') {
        return None;
    }
    let mut 品种 = String::new();
    let mut 断字符: Option<char> = None;
    let mut chars = 名.chars();
    for c in chars.by_ref() {
        if c.is_ascii_alphabetic() {
            品种.push(c);
        } else {
            断字符 = Some(c);
            break;
        }
    }
    let 剩余: String = 断字符.into_iter().chain(chars).collect();
    let 月份: String = 剩余.chars().take_while(|c| c.is_ascii_digit()).collect();
    let 余下: String = 剩余.chars().skip_while(|c| c.is_ascii_digit()).collect();
    let mut it = 余下.chars();
    let 是call = match it.next() {
        Some('C') => true,
        Some('P') => false,
        _ => return None,
    };
    let 价段: String = it.collect();
    let 行权价: f64 = 价段.parse().ok()?;
    if 品种.is_empty() || 月份.len() != 4 {
        return None;
    }
    Some((品种, 月份, 是call, 行权价))
}

/// 档位推导：对某品种某月份，从现有行权价集合与当前期货价推导 ±2 档新候选
fn 推导新档(现有行权价: &BTreeSet<i64>, 期货价: f64) -> Vec<f64> {
    let 档位: Vec<i64> = 现有行权价.iter().copied().collect();
    if 档位.len() < 2 {
        return Vec::new();
    }
    // 间隔取档位相邻差的中位数（简单实现：排序后取中间一对的差）
    let mut diffs = Vec::new();
    for w in 档位.windows(2) {
        if let [a, b] = w {
            if b > a {
                diffs.push(b - a);
            }
        }
    }
    diffs.sort_unstable();
    let 中位间隔 = *diffs.get(diffs.len() / 2).unwrap_or(&8).max(&1);
    // 平值：距期货价最近的现有档位，向两侧各扩 2 档
    let 平值 = 档位
        .iter()
        .copied()
        .min_by(|a, b| {
            let da = (i64::from(*a) as f64 - 期货价).abs();
            let db = (i64::from(*b) as f64 - 期货价).abs();
            da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
        })
        .unwrap_or(0);
    let mut 候选 = Vec::new();
    for k in 1..=2i64 {
        let 上 = 平值 + 中位间隔 * k;
        let 下 = 平值 - 中位间隔 * k;
        if !现有行权价.contains(&上) && 上 > 0 {
            候选.push(上 as f64);
        }
        if !现有行权价.contains(&下) && 下 > 0 {
            候选.push(下 as f64);
        }
    }
    候选
}

fn main() {
    println!("== 合约清单每日更新器 v1（订阅验证法） {} ==", chrono_like_now());
    let _ = fs::create_dir_all(输出目录);
    let 清单 = 读清单();
    if 清单.is_empty() {
        eprintln!("清单读取失败或为空: {清单路径}");
        return;
    }
    println!(
        "旧清单 {} 条；验证前置 {VERIFY_IPS:?}；批 {} 条/批",
        清单.len(),
        BATCH_SIZE
    );

    // 阶段A+B：16 前置分 4 片并行——每片先订阅期货合约收价，再全量验证
    // 分片策略：清单按索引对 4 取模分片
    let mut 片: [Vec<String>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    for (i, (合约, _)) in 清单.iter().enumerate() {
        片[i % 4].push(合约.clone());
    }
    let handles: Vec<_> = VERIFY_IPS
        .iter()
        .zip(片.iter())
        .enumerate()
        .map(|(i, (ip, set))| {
            let ip = ip.to_string();
            let set = set.clone();
            thread::spawn(move || verify_slice(&ip, &set, true))
        })
        .collect();
    let mut results = Vec::new();
    for h in handles {
        match h.join() {
            Ok(r) => results.push(r),
            Err(e) => println!("线程崩溃: {e:?}"),
        }
    }

    // 汇总
    let mut 全存活: BTreeSet<String> = BTreeSet::new();
    let mut 全摘牌: BTreeMap<String, i32> = BTreeMap::new();
    let mut 期货价: BTreeMap<String, f64> = BTreeMap::new();
    let mut 无回报 = 清单
        .iter()
        .map(|(c, _)| c.clone())
        .collect::<BTreeSet<String>>();
    let mut 已验证: BTreeSet<String> = BTreeSet::new();
    for r in &results {
        println!(
            "前置 {}: 连接{} 存活{} 摘牌{} 期货价样本{}",
            r.ip,
            if r.connected { "✅" } else { "❌" },
            r.存活.len(),
            r.摘牌.len(),
            r.期货价.len()
        );
        for c in r.存活.iter() {
            全存活.insert(c.clone());
            无回报.remove(c);
        }
        for (c, e) in r.摘牌.iter() {
            全摘牌.insert(c.clone(), *e);
            无回报.remove(c);
        }
        for (c, p) in r.期货价.iter() {
            期货价.insert(c.clone(), *p);
        }
    }
    for c in 全存活.iter().chain(全摘牌.keys()) {
        已验证.insert(c.clone());
    }

    let 存活数 = 全存活.len();
    let 摘牌数 = 全摘牌.len();
    let 无回报数 = 无回报.len();
    println!();
    println!(
        "阶段A 汇总: 存活 {存活数}, 摘牌 {摘牌数}, 无回报(保守保留) {无回报数}, 已验证 {}/{}",
        已验证.len(),
        清单.len()
    );

    // 阶段B：期权档位推导（用 SHFE 期货价样本，验证 au/ag 平值扩展档）
    let mut 新候选: Vec<String> = Vec::new();
    let mut 品种档位: BTreeMap<(String, String, bool), BTreeSet<i64>> = BTreeMap::new();
    for (合约, 所) in &清单 {
        if 所 == "SHFE" {
            if let Some((品种, 月份, 是call, 价)) = 解析期权(合约) {
                品种档位
                    .entry((品种, 月份, 是call))
                    .or_default()
                    .insert(价 as i64);
            }
        }
    }
    // au/ag 当前价：从期货价 map 找 au2702/ag2702（主力月）——找不到就用现有档位中位数近似
    for (品种, 月份) in [("au", "2702"), ("ag", "2702")] {
        let 期货标的 = format!("{品种}{月份}");
        let 价 = 期货价.get(&期货标的).copied().unwrap_or(0.0);
        if 价 <= 0.0 {
            println!("阶段B: {期货标的} 无实时价，跳过档位推导");
            continue;
        }
        for 是call in [true, false] {
            if let Some(档位) = 品种档位.get(&(品种.to_string(), 月份.to_string(), 是call)) {
                let 候选 = 推导新档(档位, 价);
                for c in 候选 {
                    let 名 = format!("{品种}{月份}{}{}", if 是call { "C" } else { "P" }, c as i64);
                    新候选.push(名);
                }
            }
        }
        println!("阶段B: {期货标的} 价={价} → 候选 {:?}", {
            let mut cs = Vec::new();
            for 是call in [true, false] {
                if let Some(档位) = 品种档位.get(&(品种.to_string(), 月份.to_string(), 是call)) {
                    for c in 推导新档(档位, 价) {
                        cs.push(format!(
                            "{品种}{月份}{}{}",
                            if 是call { "C" } else { "P" },
                            c as i64
                        ));
                    }
                }
            }
            cs
        });
    }

    // 候选验证：单前置批量订阅候选
    if !新候选.is_empty() {
        let r = verify_slice(
            VERIFY_IPS.first().copied().unwrap_or("101.231.185.57"),
            &新候选.iter().map(String::from).collect::<Vec<_>>(),
            false,
        );
        let 候选存活: Vec<&String> = r.存活.iter().collect();
        println!(
            "阶段B 验证: 候选 {} 个 → 存活 {} 个 {:?}",
            新候选.len(),
            候选存活.len(),
            候选存活
        );
        for c in 候选存活 {
            全存活.insert(c.clone());
        }
    }

    // 阶段C：输出 diff 报告与新清单
    let 报告路径 = format!("{输出目录}/更新报告_{}.json", chrono_date_compact());
    let mut 报告 = String::new();
    报告.push_str(&format!(
        "{{\"时间\":\"{}\",\"旧清单\":{},\"存活\":{},\"摘牌\":{},\"无回报\":{},\"新档候选存活\":{:?},\"期货价样本\":{:?}}}",
        chrono_like_now(),
        清单.len(),
        存活数,
        摘牌数,
        无回报数,
        新候选.len(),
        {
            let mut v = Vec::new();
            for (k, p) in 期货价.iter().take(10) {
                v.push(format!("{k}={p}"));
            }
            v
        }
    ));
    let _ = fs::write(&报告路径, 报告);
    println!("报告已存 {报告路径}");

    // 新清单：存活 + 无回报（保守保留），剔除摘牌；候选存活追加（交易所列补 SHFE）
    let 新清单路径 = format!("{输出目录}/contracts_{}.txt", chrono_date_compact());
    let f = fs::File::create(&新清单路径);
    if let Ok(file) = f {
        let mut w = BufWriter::new(file);
        let _ = writeln!(w, "ExchangeID");
        let mut 写入数 = 0usize;
        for (合约, 所) in &清单 {
            if 全摘牌.contains_key(合约) {
                continue;
            }
            let _ = writeln!(w, "{合约},{所}");
            写入数 += 1;
        }
        for c in &全存活 {
            if 解析期权(c).is_some() && !清单.iter().any(|(o, _)| o == c) {
                let _ = writeln!(w, "{c},SHFE");
                写入数 += 1;
            }
        }
        let _ = w.flush();
        println!("新清单已存 {新清单路径}（{写入数} 条）");
    }
    println!("提示: 新清单需人工核对 diff 后再替换正式 contracts_live.txt（更新器不直接覆盖）");
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

fn chrono_date_compact() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let (y, mo, d) = civil_from_days(((secs + 8 * 3600) / 86400) as i64);
    format!("{y:04}{mo:02}{d:02}")
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

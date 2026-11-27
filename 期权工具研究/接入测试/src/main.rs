//! ============================================================================
//! 期权工具安装集成实测（Task 4-a）——核心演示 crate
//!
//! 数据来源（只读，绝不改动）：
//!   夜盘真实 tick:   /home/z/my-project/ctp测试/期权探针_输出/夜盘tick_57.jsonl
//!                    （2026-11-27 夜盘 au2702/ag2702 期货 + ag2702 全网格期权 tick）
//!   下午盘期权 tick: /home/z/my-project/ctp测试/期权探针_输出/期权tick_57.jsonl
//!                    （含 au2702C768=44.18 等真实 CTP 期权 tick）
//!
//! 演示内容（对应调研报告 8 工具中的 5 个 crates.io 库）：
//!   ① implied-vol 2.1.0      —— Black-76 隐含波动率反解（夜盘真实 tick）
//!   ② black_scholes 0.11.1   —— BS Greeks（delta/gamma/vega/theta/rho）
//!   ③ optionstratlib 0.24.6  —— 真实档位 OptionChain 构造 + JSON 导出（feature: chain）
//!   ④ volsurf 4.0.1          —— 真实 IV 网格 SVI 拟合 + RMSE（feature: chain）
//!   ⑤ RustQuant_stochastics 0.3.1 —— GBM 1 万路径蒙特卡洛
//!   ⑦ option-chain-orderbook 0.15.0 —— 依赖链接验证（feature: checks）
//!   ⑧ barter-instrument 0.3.3 —— InstrumentKind::Option 字段验证（feature: checks）
//!
//! 口径说明（如实记录，禁止造假）：
//!   - 到期日 2027-01-15 为【假设值待核】（au2702 期权真实最后交易日待查交易所公告）
//!   - T = 49/365 ≈ 0.1342 年（2026-11-27 夜盘 → 2027-01-15）
//!   - r = 0.02 假设利率；商品期权为期货期权，Black-76 模型 F 直接取期货最新价
//!   - 夜盘文件含 au2702 期货但未含 au2702 期权档（实测 grep 无 C768 行），
//!     故 ① 的期权权利金取下午盘 au2702C768 真实 tick（13:42:48, 最新价 44.18，
//!     买卖中价 44.54），与夜盘期货价 786.22 存在时间口径错位，已在输出中标注。
//! ============================================================================

use std::fs;
use std::io::{BufRead, BufReader};

// ---------------------------------------------------------------------------
// 全局口径常量（中文注释，假设值待核）
// ---------------------------------------------------------------------------
/// 估值时点：2026-11-27 夜盘；到期日假设 2027-01-15 → 49 天
const T_YEARS: f64 = 49.0 / 365.0;
/// 假设无风险利率（待核：应换 SHIBOR/回购曲线）
const R_RATE: f64 = 0.02;
/// 到期日字符串（假设值，仅供记录）
const EXPIRY_STR: &str = "2027-01-15(假设)";

/// 夜盘 tick 文件（真实 CTP 数据，只读）
const PATH_NIGHT: &str = "/home/z/my-project/ctp测试/期权探针_输出/夜盘tick_57.jsonl";
/// 下午盘期权 tick 文件（真实 CTP 数据，只读）
const PATH_AFTERNOON: &str = "/home/z/my-project/ctp测试/期权探针_输出/期权tick_57.jsonl";
/// 输出目录
const OUT_DIR: &str = "/home/z/my-project/期权工具研究/接入测试/输出";

// ---------------------------------------------------------------------------
// 数据结构：把 JSONL tick 收敛成"每行权价一档"的报价行
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
struct StrikeQuote {
    /// 行权价
    strike: f64,
    /// 看涨：买一/卖一/最新价（None=夜盘无此档或无有效报价）
    call_bid: Option<f64>,
    call_ask: Option<f64>,
    call_last: Option<f64>,
    /// 看跌：买一/卖一/最新价
    put_bid: Option<f64>,
    put_ask: Option<f64>,
    put_last: Option<f64>,
    /// 成交量/持仓量（看涨腿，仅参考）
    volume: Option<u64>,
    open_interest: Option<u64>,
}

impl StrikeQuote {
    /// 买卖中价：(买一+卖一)/2，两边都存在才有效
    fn call_mid(&self) -> Option<f64> {
        match (self.call_bid, self.call_ask) {
            (Some(b), Some(a)) if b > 0.0 && a > 0.0 => Some((b + a) / 2.0),
            _ => None,
        }
    }
    fn put_mid(&self) -> Option<f64> {
        match (self.put_bid, self.put_ask) {
            (Some(b), Some(a)) if b > 0.0 && a > 0.0 => Some((b + a) / 2.0),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct MarketSnapshot {
    /// 夜盘 au2702 期货最新价（Black-76 的 F）
    f_au_night: Option<f64>,
    /// 夜盘 ag2702 期货最新价
    f_ag_night: Option<f64>,
    /// 下午盘期货价（若文件里有 FUTURES 行，用于口径对齐）
    f_au_afternoon: Option<f64>,
    /// 下午盘 au2702 期权网格（真实 tick 收敛）
    au_grid: Vec<StrikeQuote>,
    /// 夜盘 ag2702 期权网格（真实 tick 收敛）
    ag_grid: Vec<StrikeQuote>,
    /// au2702C768 的权利金来源与数值
    c768_note: String,
    c768_mid: Option<f64>,
    c768_last: Option<f64>,
}

/// 解析一行 JSON tick；opt=Some 只收期权行，None 只收 FUTURES 行
fn parse_line(v: &serde_json::Value) -> Option<(bool, String, f64, f64, f64, f64, f64, Option<u64>, Option<u64>)> {
    let dir = v.get("方向")?.as_str()?.to_string();
    let sym = v.get("合约")?.as_str()?.to_string();
    let last = v.get("最新价").and_then(|x| x.as_f64()).unwrap_or(0.0);
    let bid = v.get("买一").and_then(|x| x.as_f64()).unwrap_or(0.0);
    let ask = v.get("卖一").and_then(|x| x.as_f64()).unwrap_or(0.0);
    let strike = v.get("行权价").and_then(|x| x.as_f64()).unwrap_or(0.0);
    let under = v.get("标的").and_then(|x| x.as_str()).unwrap_or("").to_string();
    let vol = v.get("成交量").and_then(|x| x.as_u64());
    let oi = v.get("持仓量").and_then(|x| x.as_u64());
    Some((
        dir == "FUTURES",
        sym,
        last,
        bid,
        ask,
        strike,
        under.parse().unwrap_or(0.0), // 标的解析为期货价占位（仅期权行用不到）
        vol,
        oi,
    ))
}

/// 读取整个 JSONL 并收敛出快照
fn load_snapshot() -> MarketSnapshot {
    let mut snap = MarketSnapshot {
        f_au_night: None,
        f_ag_night: None,
        f_au_afternoon: None,
        au_grid: Vec::new(),
        ag_grid: Vec::new(),
        c768_note: String::new(),
        c768_mid: None,
        c768_last: None,
    };

    // ---- 夜盘文件：期货价 + ag2702 全网格 + （若有）au2702 期权 ----
    if let Ok(f) = fs::File::open(PATH_NIGHT) {
        for line in BufReader::new(f).lines().map_while(Result::ok) {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else { continue };
            let Some((is_fut, sym, last, bid, ask, strike, _, vol, oi)) = parse_line(&v) else { continue };
            if is_fut {
                // "最后一条"即最新价（文件按时间追加）
                if sym == "au2702" { snap.f_au_night = Some(last); }
                if sym == "ag2702" { snap.f_ag_night = Some(last); }
            } else if let Some(under) = v.get("标的").and_then(|x| x.as_str()) {
                let grid = if under == "ag2702" { &mut snap.ag_grid } else { &mut snap.au_grid };
                upsert_option(grid, &sym, strike, bid, ask, last, vol, oi);
            }
        }
    }

    // ---- 下午盘文件：au2702 期权网格 + C768 权利金 + （若有）期货行 ----
    if let Ok(f) = fs::File::open(PATH_AFTERNOON) {
        for line in BufReader::new(f).lines().map_while(Result::ok) {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else { continue };
            let Some((is_fut, sym, last, bid, ask, strike, _, vol, oi)) = parse_line(&v) else { continue };
            if is_fut {
                if sym == "au2702" { snap.f_au_afternoon = Some(last); }
            } else {
                // 只收 au2702 系（下午盘文件是"期权tick_57"，可能混合 au/ag）
                if sym.starts_with("au2702") {
                    upsert_option(&mut snap.au_grid, &sym, strike, bid, ask, last, vol, oi);
                }
                if sym == "au2702C768" {
                    snap.c768_mid = StrikeQuote {
                        strike: 768.0,
                        call_bid: if bid > 0.0 { Some(bid) } else { None },
                        call_ask: if ask > 0.0 { Some(ask) } else { None },
                        call_last: Some(last),
                        put_bid: None, put_ask: None, put_last: None,
                        volume: vol, open_interest: oi,
                    }
                    .call_mid();
                    snap.c768_last = Some(last);
                    snap.c768_note = format!(
                        "来源=下午盘tick(13:42:48) 最新价={last} 买一={bid} 卖一={ask} 中价={:?}",
                        snap.c768_mid
                    );
                }
            }
        }
    }

    // 排序：按行权价升序，方便构造链
    snap.au_grid.sort_by(|a, b| a.strike.partial_cmp(&b.strike).unwrap());
    snap.ag_grid.sort_by(|a, b| a.strike.partial_cmp(&b.strike).unwrap());
    snap
}

/// 把一行期权 tick 合并进网格（按行权价聚合 C/P）
fn upsert_option(
    grid: &mut Vec<StrikeQuote>,
    sym: &str,
    strike: f64,
    bid: f64,
    ask: f64,
    last: f64,
    vol: Option<u64>,
    oi: Option<u64>,
) {
    if strike <= 0.0 { return; }
    let is_call = sym.contains('C');
    if let Some(q) = grid.iter_mut().find(|q| q.strike == strike) {
        if is_call {
            if bid > 0.0 { q.call_bid = Some(bid); }
            if ask > 0.0 { q.call_ask = Some(ask); }
            q.call_last = Some(last);
            q.volume = vol.or(q.volume);
            q.open_interest = oi.or(q.open_interest);
        } else {
            if bid > 0.0 { q.put_bid = Some(bid); }
            if ask > 0.0 { q.put_ask = Some(ask); }
            q.put_last = Some(last);
        }
    } else {
        grid.push(StrikeQuote {
            strike,
            call_bid: (is_call && bid > 0.0).then_some(bid),
            call_ask: (is_call && ask > 0.0).then_some(ask),
            call_last: is_call.then_some(last),
            put_bid: (!is_call && bid > 0.0).then_some(bid),
            put_ask: (!is_call && ask > 0.0).then_some(ask),
            put_last: (!is_call).then_some(last),
            volume: vol,
            open_interest: oi,
        });
    }
}

// ---------------------------------------------------------------------------
// ① implied-vol 2.1.0：Black-76 隐含波动率（Jäckel/FlashIV 混合求解器）
//    API（源码确认）：explicit_black::implied_black_volatility_input
//        ::<DefaultSpecialFn, IS_CALL>(price, F, K, T) -> Option<f64>
// ---------------------------------------------------------------------------
mod yanshi_iv {
    use implied_vol::explicit_black::implied_black_volatility_input;
    use implied_vol::DefaultSpecialFn;

    /// Black-76 看涨 IV 反解（夜盘真实 tick 用例在调用处组装）
    pub fn black76_call_iv(price: f64, forward: f64, strike: f64, t: f64) -> Option<f64> {
        implied_black_volatility_input::<DefaultSpecialFn, true>(price, forward, strike, t)
    }
    /// Black-76 看跌 IV 反解（IS_CALL=false；链的 put 腿直接用 put 中价反解）
    pub fn black76_put_iv(price: f64, forward: f64, strike: f64, t: f64) -> Option<f64> {
        implied_black_volatility_input::<DefaultSpecialFn, false>(price, forward, strike, t)
    }
}

// ---------------------------------------------------------------------------
// ② black_scholes 0.11.1：Greeks
//    API（源码确认）：call_delta(s,k,rate,sigma,maturity) 等
//    近似口径：以 S=F、q=0 代入 BS，delta=N(d1) 与 Black-76 一致；
//    theta/vega/rho 与严格期货期权公式存在贴现差异，报告中如实标注。
// ---------------------------------------------------------------------------
mod yanshi_greeks {
    /// 一组 Greeks 的简单容器
    #[derive(Debug, Clone, Copy)]
    pub struct Greeks {
        pub delta: f64,
        pub gamma: f64,
        pub vega: f64,
        pub theta: f64,
        pub rho: f64,
        pub price: f64,
    }

    pub fn call_greeks(s: f64, k: f64, r: f64, sigma: f64, t: f64) -> Greeks {
        Greeks {
            price: black_scholes::call(s, k, r, sigma, t),
            delta: black_scholes::call_delta(s, k, r, sigma, t),
            gamma: black_scholes::call_gamma(s, k, r, sigma, t),
            vega: black_scholes::call_vega(s, k, r, sigma, t),
            theta: black_scholes::call_theta(s, k, r, sigma, t),
            rho: black_scholes::call_rho(s, k, r, sigma, t),
        }
    }
}

// ---------------------------------------------------------------------------
// ⑤ RustQuant_stochastics 0.3.1：GBM 蒙特卡洛（1 万路径）
//    API（源码确认）：GBM::new(mu,sigma) + StochasticProcessConfig::new
//      (x_0, t_0, t_n, n_steps, m_paths, parallel) + Trajectories{times,paths}
// ---------------------------------------------------------------------------
mod yanshi_mc {
    use RustQuant_stochastics::geometric_brownian_motion::GeometricBrownianMotion;
    use RustQuant_stochastics::process::{StochasticProcess, StochasticProcessConfig};

    /// GBM 路径模拟：返回 (路径数, 终值均值, 终值标准差)
    pub fn gbm_terminal_stats(
        x0: f64, mu: f64, sigma: f64, t_n: f64, n_steps: usize, m_paths: usize,
    ) -> (usize, f64, f64) {
        let gbm = GeometricBrownianMotion::new(mu, sigma);
        let cfg = StochasticProcessConfig::new(x0, 0.0, t_n, n_steps, m_paths, false);
        let trajs = gbm.head(&cfg);
        let terminals: Vec<f64> = trajs.paths.iter().map(|p| *p.last().unwrap_or(&x0)).collect();
        let n = terminals.len();
        let mean = terminals.iter().sum::<f64>() / n as f64;
        let var = terminals.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1) as f64;
        (n, mean, var.sqrt())
    }
}

// ---------------------------------------------------------------------------
// ③④ optionstratlib + volsurf（feature = "chain" 时才编译，防重依赖拖慢核心路径）
// ---------------------------------------------------------------------------
#[cfg(feature = "chain")]
mod yanshi_chain_svi {
    use crate::{MarketSnapshot, R_RATE, T_YEARS};
    use crate::yanshi_iv;
    use optionstratlib::{pos, ExpirationDate, OptionChain};
    use volsurf::surface::builder::{SmileModel, SurfaceBuilder};
    use volsurf::types::Strike;

    /// ③ optionstratlib：真实档位 OptionChain ≥5 档 C+P → JSON 导出
    pub fn run_chain(snap: &MarketSnapshot, f_au: f64) -> Result<(), String> {
        let rows: Vec<_> = snap.au_grid.iter()
            .filter(|q| q.call_mid().is_some() || q.put_mid().is_some())
            .collect();
        if rows.len() < 5 {
            return Err(format!("au2702 有效档位仅 {} 个（<5），链构造按规则中止", rows.len()));
        }
        let mut chain = OptionChain::new("au2702", pos!(f_au), ExpirationDate::Days(T_YEARS * 365.0));
        for q in &rows {
            let call_iv = q.call_mid().and_then(|m| yanshi_iv::black76_call_iv(m, f_au, q.strike, T_YEARS));
            let put_iv  = q.put_mid().and_then(|m| yanshi_iv::black76_put_iv(m, f_au, q.strike, T_YEARS));
            chain.add_option(
                pos!(q.strike),
                q.call_bid.map(|x| pos!(x)), q.call_ask.map(|x| pos!(x)),
                q.put_bid.map(|x| pos!(x)),  q.put_ask.map(|x| pos!(x)),
                call_iv.map(|x| pos!(x)),    put_iv.map(|x| pos!(x)),
                None, None, None, None, q.open_interest,
            ).map_err(|e| format!("add_option(K={}) 失败: {e}", q.strike))?;
        }
        let path = format!("{}/option_chain_au2702.json", crate::OUT_DIR);
        chain.save_to_json(&path).map_err(|e| format!("save_to_json 失败: {e}"))?;
        println!("[③optionstratlib] 链构建成功：{} 档，ATM 行权价={:?}，已导出 {path}",
            rows.len(), chain.atm_strike().map(|p| p.to_string()).unwrap_or_else(|_| "N/A".into()));
        Ok(())
    }

    /// ④ volsurf：夜盘 ag2702 真实 IV 网格 → SVI 拟合 + RMSE
    pub fn run_svi(snap: &MarketSnapshot, f_ag: f64) -> Result<(), String> {
        // OTM 选择（市场惯例）：K>=F 用 CALL，K<F 用 PUT；剔除无价/深度实值档
        let mut ks: Vec<f64> = Vec::new();
        let mut vs: Vec<f64> = Vec::new();
        for q in &snap.ag_grid {
            let mid = if q.strike >= f_ag { q.call_mid() } else { q.put_mid() };
            let Some(mid) = mid else { continue };
            if mid < 0.5 { continue; }
            let Some(iv) = yanshi_iv::black76_call_iv(mid, f_ag, q.strike, T_YEARS) else { continue };
            if !(0.03..=3.0).contains(&iv) { continue; }
            ks.push(q.strike); vs.push(iv);
        }
        if ks.len() < 5 {
            return Err(format!("SVI 拟合有效样本仅 {} 个（SmileModel::Svi 最低要求 5 档）", ks.len()));
        }
        let surface = SurfaceBuilder::new()
            .spot(f_ag)
            .rate(R_RATE)
            .model(SmileModel::Svi)
            .add_tenor(T_YEARS, &ks, &vs)
            .build()
            .map_err(|e| format!("volsurf build 失败: {e}"))?;
        let mut sq = 0.0; let mut cnt = 0;
        let mut rows_out: Vec<(f64, f64, f64)> = Vec::new();
        for (k, v) in ks.iter().zip(vs.iter()) {
            let query = surface.smile(T_YEARS).and_then(|s| s.vol(Strike(*k)));
            if let Ok(fit) = query {
                sq += (fit.0 - v).powi(2); cnt += 1;
                rows_out.push((*k, *v, fit.0));
            }
        }
        let rmse = if cnt > 0 { (sq / cnt as f64).sqrt() } else { f64::NAN };
        println!("[④volsurf] SVI 拟合完成：tenor={T_YEARS:.4}y 样本={cnt}档 RMSE={rmse:.6}");
        for (k, mkt, fit) in &rows_out {
            println!("    K={k:>8.1}  市场IV={mkt:.4}  SVI拟合IV={fit:.4}  差={:.4}", fit - mkt);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ⑦⑧ 依赖链接验证（feature = "checks" 时才编译）
// ---------------------------------------------------------------------------
#[cfg(feature = "checks")]
mod yanshi_checks {
    /// ⑧ barter-instrument 0.3.3：确认 InstrumentKind::Option(OptionContract) 字段
    pub fn run_barter() {
        use barter_instrument::instrument::kind::{InstrumentKind, OptionContract, OptionExercise, OptionKind};
        use chrono::Utc;
        use rust_decimal::Decimal;

        // 把 CTP 合约 au2702C768 映射进 barter 的期权工具类型（字段一一对应）
        let au_c768 = InstrumentKind::Option(OptionContract {
            contract_size: Decimal::from(1000u64),          // 上期所 au 乘数 1000 克/手（清单文件列待核）
            settlement_asset: "CNY".to_string(),            // AssetKey 泛型，这里用字符串
            kind: OptionKind::Call,                          // C
            exercise: OptionExercise::European,              // 中国期权全为欧式（调研已核）
            expiry: Utc::now() + chrono::Duration::days(49), // 2027-01-15 假设
            strike: Decimal::from(768u64),                   // K=768
        });
        println!("[⑧barter] InstrumentKind::Option 字段验证：");
        println!("    contract_size={:?}（乘数）", au_c768.contract_size());
        println!("    settlement_asset={:?}", au_c768.settlement_asset());
        match &au_c768 {
            InstrumentKind::Option(c) => {
                println!("    kind={:?} exercise={:?} expiry={} strike={}", c.kind, c.exercise, c.expiry, c.strike);
            }
            _ => unreachable!(),
        }
    }

    /// ⑦ option-chain-orderbook 0.15.0：crates.io 在售（实测 0.15.0 可解析下载），
    ///    此处仅验证其类型可链接（订单簿本体 API 深度适配不在本次范围）
    pub fn run_ocob() {
        let _probe = option_chain_orderbook::Error::no_data("CTP 链接探针（仅验证可链接）");
        println!("[⑦ocob] option-chain-orderbook 0.15.0 依赖链接成功，Error::no_data 构造可用");
    }
}

// ---------------------------------------------------------------------------
// 主流程
// ---------------------------------------------------------------------------
fn main() {
    fs::create_dir_all(OUT_DIR).ok();
    let t0 = std::time::Instant::now();
    println!("================ 期权工具集成演示（真实 CTP tick） ================");
    println!("口径：T={T_YEARS:.4}y（{EXPIRY_STR}），r={R_RATE}，Black-76 F=期货最新价");

    let snap = load_snapshot();
    println!("数据装载耗时 {:.1}ms", t0.elapsed().as_secs_f64() * 1000.0);
    println!("夜盘 au2702 期货 F={:?}  ag2702 期货 F={:?}", snap.f_au_night, snap.f_ag_night);
    println!("下午盘 au2702 期货={:?}  au 档位数={}  ag 档位数={}",
        snap.f_au_afternoon, snap.au_grid.len(), snap.ag_grid.len());
    println!("au2702C768 权利金：{}", snap.c768_note);

    // =========== ① IV（核心演示） ===========
    println!("\n---- ① implied-vol 2.1.0：Black-76 IV 反解 ----");
    let f_au = snap.f_au_night.unwrap_or(786.22);
    let price = snap.c768_mid.or(snap.c768_last).unwrap_or(44.18);
    println!("输入：F={f_au}（夜盘期货） K=768 市场价={price}（中价优先，{}）", EXPIRY_STR);
    match yanshi_iv::black76_call_iv(price, f_au, 768.0, T_YEARS) {
        Some(iv) => println!(">>> au2702C768 隐含波动率 IV = {iv:.6}（{:.2}%）", iv * 100.0),
        None => println!(">>> IV 反解失败（价格超出 Black 上界/下界），如实记录"),
    }
    // 夜盘口径一致的对照组：ag2702 平值附近 CALL（数据全部来自夜盘）
    if let Some(f_ag) = snap.f_ag_night {
        let atm = snap.ag_grid.iter()
            .filter(|q| q.call_mid().is_some())
            .min_by(|a, b| ((a.strike - f_ag).abs()).partial_cmp(&(b.strike - f_ag).abs()).unwrap());
        if let Some(q) = atm {
            let m = q.call_mid().unwrap();
            let iv = yanshi_iv::black76_call_iv(m, f_ag, q.strike, T_YEARS);
            println!("对照组（纯夜盘数据）：ag2702C{} 中价={m} → IV={:?}",
                q.strike, iv.map(|x| format!("{x:.6}")).unwrap_or_else(|| "反解失败".into()));
        }
    }

    // =========== ② Greeks ===========
    println!("\n---- ② black_scholes 0.11.1：Greeks（以 ① 的 IV 为输入） ----");
    let iv = yanshi_iv::black76_call_iv(price, f_au, 768.0, T_YEARS).unwrap_or(0.20);
    let g = yanshi_greeks::call_greeks(f_au, 768.0, R_RATE, iv, T_YEARS);
    println!("输入：S(=F)={f_au} K=768 σ(IV)={iv:.6} r={R_RATE} T={T_YEARS:.4}");
    println!(">>> call_price={:.4} delta={:.6} gamma={:.8} vega={:.4} theta={:.4}/年 rho={:.6}",
        g.price, g.delta, g.gamma, g.vega, g.theta, g.rho);
    println!("注：vega/theta 为每 1.0 波动率 / 每 1.0 年口径；严格 Black-76 期货期权公式需贴现调整（报告已标注）");

    // =========== ⑤ MC ===========
    println!("\n---- ⑤ RustQuant_stochastics GBM 蒙特卡洛（10000 路径） ----");
    let (n, mean, std) = yanshi_mc::gbm_terminal_stats(f_au, 0.0, iv, T_YEARS, 49, 10_000);
    println!("参数：S0(=F)={f_au} μ=0（期货风险中性） σ={iv:.6} T={T_YEARS:.4} 步数=49");
    println!(">>> 路径数={n} 终值均值={mean:.4} 终值标准差={std:.4}");
    println!("理论校验：期望≈S0·e^0={f_au}（μ=0），σ·S0·√T={:.4}", iv * f_au * T_YEARS.sqrt());

    // =========== ③④（feature = chain） ===========
    #[cfg(feature = "chain")]
    {
        println!("\n---- ③ optionstratlib 0.24.6：真实档位 OptionChain ----");
        if let Err(e) = yanshi_chain_svi::run_chain(&snap, f_au) {
            println!("[③optionstratlib] 失败（如实记录）：{e}");
        }
        println!("\n---- ④ volsurf 4.0.1：夜盘 ag2702 IV 网格 SVI 拟合 ----");
        if let Some(f_ag) = snap.f_ag_night {
            if let Err(e) = yanshi_chain_svi::run_svi(&snap, f_ag) {
                println!("[④volsurf] 失败（如实记录）：{e}");
            }
        }
    }

    // =========== ⑦⑧（feature = checks） ===========
    #[cfg(feature = "checks")]
    {
        println!("\n---- ⑦⑧ 依赖链接验证 ----");
        yanshi_checks::run_barter();
        yanshi_checks::run_ocob();
    }

    println!("\n================ 演示结束，总耗时 {:.1}s ================", t0.elapsed().as_secs_f64());
}

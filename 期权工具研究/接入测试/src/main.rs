#![allow(clippy::print_stdout, reason = "本程序即测试报告输出器")]
//! 期权工具接入测试——用今晚（2026-11-27）黄金白银夜盘的真实 CTP tick
//! 驱动全球 Rust 期权生态库，验证"选定的工具能不能直接吃我们的行情"。
//!
//! 数据源：ctp测试/期权探针_输出/夜盘tick_57.jsonl（逐 tick JSONL，假账号探测，不碰实盘）
//! 演示内容：
//!   ① implied-vol   —— Black-76 隐含波动率反解（夜盘真实价）
//!   ② black_scholes  —— delta/gamma/vega/theta（同一期权）
//!   ③ black_scholes.call_iv —— 交叉验证（两个库各自反解 IV 应一致）
//!   ★ 实测发现：ag2702 期权夜盘有行情（80–1600 tick/窗），au2702 期权夜盘零行情
//!     （同一前置同一会话，au 期货正常 1960 tick，下午盘 au2702C768 有 152 行证明
//!     合约本身正常）——故本演示改用白银 ag2702C10500；黄金期权夜盘行情缺失原因
//!     待核（以交易所公告为准），已记入工作日志
//! 输入参数口径（全部写在输出里供核对）：
//!   标的期货 ag2702 的最新价 = 远期价 F（期货即远期）
//!   T = 至 2027-01-15 的年化时间（SHFE ag2702 期权到期日按
//!       "标的合约月前一个月的第三个星期五"推算，属假设值待交易所公告核）
//!   r = 0.02（无风险利率假设值）
use std::fs;

/// 从夜盘 tick 文件提取指定合约的最新一条 tick（返回 最新价/成交量/时间）
fn 取最新tick(路径: &str, 合约: &str) -> Option<(f64, u32, String)> {
    let 文本 = fs::read_to_string(路径).ok()?;
    文本
        .lines()
        .rev() // 倒序找最新
        .find(|l| l.contains(&format!("\"合约\":\"{合约}\"")))
        .and_then(|l| {
            // 轻量字段提取（不引 serde，保持依赖极轻）
            let 取数 = |键: &str| -> Option<f64> {
                let p = format!("\"{键}\":");
                let i = l.find(&p)? + p.len();
                let 结尾 = l[i..]
                    .find(|c: char| c == ',' || c == '}')
                    .map(|e| i + e)
                    .unwrap_or(l.len());
                l[i..结尾].trim_matches('"').parse::<f64>().ok()
            };
            let 时间段 = |键: &str| -> Option<String> {
                let p = format!("\"{键}\":\"");
                let i = l.find(&p)? + p.len();
                let 结尾 = l[i..].find('"')? + i;
                Some(l[i..结尾].to_string())
            };
            Some((取数("最新价")?, 取数("成交量")? as u32, 时间段("交易所时间")?))
        })
}

fn main() {
    let tick路径 = "/home/z/my-project/ctp测试/期权探针_输出/夜盘tick_57.jsonl";
    let r = 0.02_f64;
    // T：2026-11-27 21:00 → 2027-01-15 15:00 ≈ 48.75 天
    let t = 48.75 / 365.0;

    println!("== 期权工具接入测试（数据：2026-11-27 黄金白银夜盘真实 CTP tick）==");
    println!("参数口径: T={t:.6}年(至2027-01-15,假设值待核)  r={r}(假设值)");
    println!();

    // ── 取真实行情（实测发现：au 期权夜盘零行情，改用白银 ag2702C10500）──
    let Some((期权价, 期权量, 期权时间)) = 取最新tick(tick路径, "ag2702C10500") else {
        println!("❌ 夜盘文件里没有 ag2702C10500 的 tick");
        return;
    };
    let Some((期货价, _期货量, _期货时间)) = 取最新tick(tick路径, "ag2702") else {
        println!("❌ 夜盘文件里没有 ag2702 期货 tick");
        return;
    };
    println!("真实行情（夜盘 {期权时间}）:");
    println!("  ag2702C10500 最新价={期权价} 量={期权量}");
    println!("  ag2702(标的期货) 最新价={期货价}");
    let k = 10500.0;
    println!();

    // ── 演示① implied-vol：Black-76 IV 反解 ─────────────────────
    // 期货期权的正确模型是 Black-76：市场价 P = e^{-rT}·BS(F,K,T,σ)
    // implied-vol 构建器吃的是"未贴现 BS 价"，所以先做贴现还原
    use implied_vol::{DefaultSpecialFn, ImpliedBlackVolatility};
    let 贴现因子 = (-r * t).exp();
    let bs等效价 = 期权价 / 贴现因子;
    println!("演示① implied-vol（Black-76 IV 反解, 贴现还原后 BS 价={bs等效价:.4}）:");
    let iv = ImpliedBlackVolatility::builder()
        .option_price(bs等效价)
        .forward(期货价)
        .strike(k)
        .expiry(t)
        .is_call(true)
        .build()
        .and_then(|b| b.calculate::<DefaultSpecialFn>());
    match iv {
        Some(sigma) => println!("  ✅ 隐含波动率 σ = {:.4} ({:.2}%/年)", sigma, sigma * 100.0),
        None => println!("  ❌ 反解失败（价格不在 BS 值域）"),
    }

    // ── 演示② black_scholes：Greeks（spot=贴现远期 等效换算）────
    use black_scholes::{call, call_delta, call_gamma, call_iv, call_theta, call_vega};
    // BS 世界 s = F·e^{-rT}（持有成本等效），σ 用演示①的结果
    let Some(sigma) = iv else { return };
    let s等效 = 期货价 * 贴现因子;
    println!();
    println!("演示② black_scholes（Greeks, s等效={s等效:.2} σ={sigma:.4}）:");
    let 定价 = call(s等效, k, r, sigma, t);
    let delta = call_delta(s等效, k, r, sigma, t);
    let gamma = call_gamma(s等效, k, r, sigma, t);
    let vega = call_vega(s等效, k, r, sigma, t);
    let theta = call_theta(s等效, k, r, sigma, t);
    println!("  理论价={定价:.4}（真实夜盘价 {期权价}，偏差 {:.2}）", 定价 * 贴现因子 - 期权价);
    println!("  delta={delta:.4}  gamma={:.6}  vega={:.4}(每1vol点)  theta={:.4}(每日)", gamma, vega, theta);

    // ── 演示③ 交叉验证：black_scholes.call_iv ───────────────────
    println!();
    println!("演示③ IV 交叉验证（black_scholes.call_iv, spot口径）:");
    match call_iv(期权价, s等效, k, r, t) {
        Ok(σ2) => println!("  ✅ call_iv σ2={σ2:.4}（与演示①差 {:.6}）", (σ2 - sigma).abs()),
        Err(e) => println!("  ❌ 反解失败 err={e}"),
    }

    // ── 演示④ volsurf：ag2702 全档位 IV 网格 → SVI 拟合 ─────────
    // 用夜盘真实 tick 逐档算 IV（演示①同法），再喂 SviSmile::calibrate（Zeliade 准显式）
    use volsurf::SmileSection; // .vol(K) 估值方法来自该 trait，必须在作用域
    println!();
    println!("演示④ volsurf SVI 拟合（ag2702 夜盘真实 IV 网格）:");
    let 档位: Vec<u32> = (96..=110).map(|i| i * 100).collect(); // 9600..11000 步长 100
    let mut 网格: Vec<(f64, f64)> = Vec::new();
    for strike in 档位 {
        let Some((价, _, _)) = 取最新tick(tick路径, &format!("ag2702C{strike}")) else { continue };
        let bs价 = 价 / 贴现因子;
        if let Some(σ) = ImpliedBlackVolatility::builder()
            .option_price(bs价)
            .forward(期货价)
            .strike(f64::from(strike))
            .expiry(t)
            .is_call(true)
            .build()
            .and_then(|b| b.calculate::<DefaultSpecialFn>())
        {
            网格.push((f64::from(strike), σ));
        }
    }
    println!("  真实 IV 网格 {} 档:", 网格.len());
    for (strike, σ) in &网格 {
        println!("    K={strike:>6}  IV={:.2}%", σ * 100.0);
    }
    let mut svi_rho = -0.4_f64; // 默认偏斜假设值；SVI 拟合成功则被真实值覆盖
    if 网格.len() >= 5 {
        match volsurf::smile::SviSmile::calibrate(期货价, t, &网格) {
            Ok(svi) => {
                svi_rho = svi.rho();
                println!(
                    "  ✅ SVI 参数: a={:.6} b={:.6} rho={:.4} m={:.2} sigma={:.4}",
                    svi.a(), svi.b(), svi.rho(), svi.m(), svi.sigma()
                );
                let rmse: f64 = 网格
                    .iter()
                    .map(|(strike, 市场σ)| {
                        let 模型σ = svi.vol(volsurf::types::Strike(*strike)).map(|v| v.0).unwrap_or(f64::NAN);
                        (模型σ - 市场σ).powi(2)
                    })
                    .sum::<f64>()
                    / 网格.len() as f64;
                println!("  拟合 RMSE={:.6} ({:.2}% 波动率)", rmse.sqrt(), rmse.sqrt() * 100.0);
            }
            Err(e) => println!("  ❌ SVI 拟合失败: {e}"),
        }
    } else {
        println!("  ⚠️ 有效档位不足 5，跳过拟合（volsurf 要求 ≥5 点）");
    }

    // ── 演示⑤ options_pricing_engine（tfrmma）：Heston 蒙特卡洛 ──
    // 参数口径：v0/theta 取演示①真实 ATM IV 平方（夜盘实测 60.56%）；
    // rho 取演示④ SVI 拟合的 rho=-0.4068（真实数据驱动）；
    // kappa=2.0 / sigma=0.35 为文献常见量级假设（未做完整 Heston 校准，如实注明）。
    // Feller 条件 2κθ>σ² 先验证再跑（该库作者注释专门提醒）。
    println!();
    println!("演示⑤ Heston 蒙特卡洛（options_pricing_engine, 夜盘真实参数驱动）:");
    let v = sigma; // 0.6056
    let hp = options_pricing_engine::types::HestonParams {
        v0: v * v,
        kappa: 2.0,
        theta: v * v,
        sigma: 0.35,
        rho: svi_rho, // 演示④ SVI 拟合值
    };
    println!(
        "  参数: v0={:.4} kappa=2.0 theta={:.4} sigma=0.35 rho={:.4}  Feller={}",
        hp.v0,
        hp.theta,
        hp.rho,
        hp.feller_ok()
    );
    let cfg = options_pricing_engine::mc::McConfig {
        n_paths: 10_000,
        n_steps: 126,
        seed: 42,
        antithetic: true,
        scheme: options_pricing_engine::mc::VarianceScheme::FullTruncationEuler,
    };
    let mc = options_pricing_engine::mc::mc_heston(
        期货价,
        t,
        r,
        0.0,
        &hp,
        options_pricing_engine::mc::Payoff::European {
            strike: k,
            opt_type: options_pricing_engine::types::OptionType::Call,
        },
        &cfg,
    );
    println!(
        "  MC 1万路径 Heston 价={:.2}±{:.2}（夜盘真实市场价 {期权价}）",
        mc.price, mc.std_error
    );
}

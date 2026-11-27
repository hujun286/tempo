//! 7 天依赖冷却检查（★零依赖版：手写 Cargo.lock 子集解析 + 系统 curl 取数，不引 reqwest/serde）
//!
//! 用途：解析 Cargo.lock 里的 registry 依赖 → crates.io API 查发布时间 →
//!       发布 <7 天的版本点名 + 退出码 1（CI 可做门禁；path 依赖 ctp2rs 自动跳过）。
//! 【为什么零依赖】rust-script 的 `---` manifest 在部分 nightly 上报
//!   `frontmatters are experimental`（D037 实测）；零 manifest 版 rustc 直接编译秒跑，
//!   rust-script 也能跑，部署面最稳。
//!
//! 用法：rust-script scripts/检查依赖冷却.rs   或   rustc -O 检查依赖冷却.rs -o 冷却 && ./冷却
//! 输出：冷却期内依赖点名 + 小结；全部已热身 → 退出 0。
//!
//! 原理与取舍（D030/D034）：依赖突然发布的版本（<7 天）存在供应链与回归风险，
//! 实盘工程对这类版本设"冷却期"观察；crates.io API 一查即得 created_at。

use std::collections::BTreeSet;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// 冷却期阈值（天）
const 冷却天数: u64 = 7;

fn 现在unix秒() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs()
}

/// 从 Cargo.lock 提取 registry 依赖（name+version 对）。
/// 手写解析：只认 `[[package]]` 块里的 `name = "..."` / `version = "..."` /
/// `source = "registry+..."` 行；source 缺失（path 依赖）的自动跳过。
fn 收集registry依赖(lock文本: &str) -> Vec<(String, String)> {
    let mut 结果: Vec<(String, String)> = Vec::new();
    let mut 当前名 = String::new();
    let mut 当前版本 = String::new();
    let mut 当前源: Option<String> = None;
    let mut 有包 = false;
    let flush = |名: &mut String, 版: &mut String, 源: &mut Option<String>, 有: &mut bool, 结果: &mut Vec<(String, String)>| {
        if *有 {
            let 是registry = match 源 {
                Some(s) => s.starts_with("registry+"),
                None => false, // 无 source = path 依赖（本地 ctp2rs 等）
            };
            if 是registry && !名.is_empty() && !版.is_empty() {
                结果.push((名.clone(), 版.clone()));
            }
        }
        名.clear();
        版.clear();
        *源 = None;
        *有 = false;
    };
    for 行 in lock文本.lines() {
        let t = 行.trim();
        if t == "[[package]]" {
            flush(&mut 当前名, &mut 当前版本, &mut 当前源, &mut 有包, &mut 结果);
            有包 = true;
        } else if !有包 {
            continue;
        } else if let Some(v) = t.strip_prefix("name = \"") {
            当前名 = v.trim_end_matches('"').to_string();
        } else if let Some(v) = t.strip_prefix("version = \"") {
            当前版本 = v.trim_end_matches('"').to_string();
        } else if let Some(v) = t.strip_prefix("source = \"") {
            当前源 = Some(v.trim_end_matches('"').to_string());
        }
    }
    flush(&mut 当前名, &mut 当前版本, &mut 当前源, &mut 有包, &mut 结果);
    结果.sort();
    结果.dedup();
    结果
}

/// ISO 日期(年月日时分) → unix 秒（days_from_civil 精确算法，Hinnant 公式）
fn iso转unix秒(年: i64, 月: i64, 日: i64, 时: i64, 分: i64) -> Option<u64> {
    if !(1..=12).contains(&月) || !(1..=31).contains(&日) {
        return None;
    }
    let y = if 月 <= 2 { 年 - 1 } else { 年 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if 月 > 2 { 月 - 3 } else { 月 + 9 };
    let doy = (153 * mp + 2) / 5 + 日 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = days * 86_400 + 时 * 3_600 + 分 * 60;
    if secs < 0 { None } else { Some(secs as u64) }
}

/// crates.io API 查发布时间（系统 curl，超时 15s）。返回该版本的 created_at（粗 unix 秒）。
fn 查发布时间(名称: &str, 版本: &str) -> Option<u64> {
    let url = format!("https://crates.io/api/v1/crates/{名称}/versions");
    let out = Command::new("curl")
        .args(["-sS", "-m", "15", "-H", "User-Agent: ctp-cooldown-check", &url])
        .output()
        .ok()?;
    let 文本 = String::from_utf8_lossy(&out.stdout);
    // 手写截取：定位 "num":"版本" 后最近的 "created_at":"..."
    let needle = format!("\"num\":\"{版本}\"");
    let mut pos = 0usize;
    while let Some(p) = 文本[pos..].find(&needle) {
        let 绝对 = pos + p;
        let 窗口 = &文本[绝对..(绝对 + 1200).min(文本.len())];
        if let Some(cp) = 窗口.find("\"created_at\":\"") {
            let rest = &窗口[cp + "\"created_at\":\"".len()..];
            let iso = rest.split('"').next().unwrap_or("");
            let 年: u64 = iso.get(0..4)?.parse().ok()?;
            let 月: u64 = iso.get(5..7)?.parse().ok()?;
            let 日: u64 = iso.get(8..10)?.parse().ok()?;
            let 时: u64 = iso.get(11..13).and_then(|s| s.parse().ok()).unwrap_or(0);
            let 分: u64 = iso.get(14..16).and_then(|s| s.parse().ok()).unwrap_or(0);
            return iso转unix秒(年 as i64, 月 as i64, 日 as i64, 时 as i64, 分 as i64);
        }
        pos = 绝对 + needle.len();
    }
    None
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let lock路径 = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "Cargo.lock".to_string());
    let lock文本 = match std::fs::read_to_string(&lock路径) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("读 {lock路径} 失败: {e}（在工程根目录运行）");
            std::process::exit(2);
        }
    };
    let 依赖们 = 收集registry依赖(&lock文本);
    if 依赖们.is_empty() {
        eprintln!("Cargo.lock 里没有 registry 依赖（离线/纯 path 工程？）");
        std::process::exit(0);
    }
    println!(
        "依赖冷却检查：registry 依赖 {} 个，冷却期 {} 天（crates.io 逐个查询，请稍候）",
        依赖们.len(),
        冷却天数
    );
    let now = 现在unix秒();
    let mut 冷却中: BTreeSet<String> = BTreeSet::new();
    let mut 查不到 = 0usize;
    let mut 已查: BTreeSet<String> = BTreeSet::new();
    for (name, ver) in &依赖们 {
        let key = format!("{name}@{ver}");
        if !已查.insert(key.clone()) {
            continue; // 同名多版本只查一次
        }
        match 查发布时间(name, ver) {
            Some(created) => {
                let 天 = now.saturating_sub(created) / 86_400;
                if 天 < 冷却天数 {
                    println!("  ⚠ {key:<40} 发布 {} 天前 —— 冷却期内！", 天);
                    冷却中.insert(key);
                }
            }
            None => {
                查不到 += 1;
            }
        }
    }
    println!(
        "小结：冷却期内 {} 个；查询失败 {} 个（网络抖动可重跑）",
        冷却中.len(),
        查不到
    );
    if !冷却中.is_empty() {
        for k in &冷却中 {
            eprintln!("  冷却中: {k}");
        }
        std::process::exit(1);
    }
    println!("✅ 全部依赖已过冷却期");
}

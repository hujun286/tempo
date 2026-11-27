#!/usr/bin/env rust-script
//! 危险代码检查器（rust-script · 零依赖版）· 替代已丢失的 检查_unwrap与打印.py
//!
//! 检查类别（比旧 .py 覆盖面更宽）：
//!   [A] .unwrap()          — panic 风险（Result/Option 直接解包）
//!   [B] .expect(           — panic 风险（带信息解包）
//!   [C] panic!/unreachable!/todo!/unimplemented! — 显式 panic 家族
//!   [D] unsafe             — FFI 不安全块/函数（审计项）
//!   [E] println!/print!/eprintln!/eprint!/dbg!   — 生产代码应只用 tracing
//!   [F] as 数值强转        — 静默截断/溢出风险（u64→u32 这类）
//!   [G] static mut         — 全局可变静态（数据竞争温床）
//!   [H] #[allow(           — lint 豁免审计项
//!   [I] 字面量索引 [123]   — 越界 panic 风险（审计项）
//!   [J] 浮点字面量 == 比较 — 精度陷阱（审计项）
//!
//! 零依赖：不用 regex，std 手写匹配（rustc 可直接编译运行，rust-script 亦可秒跑）。
//!
//! 用法：
//!   rust-script 检查危险代码.rs <文件或目录>...      # 报告模式（退出码恒 0）
//!   rust-script 检查危险代码.rs --strict <文件>      # 门禁模式（生产区有危险 → 退出码 1）
//!   rust-script 检查危险代码.rs --json <文件>        # JSON 输出（供 CI 消费）
//!
//! 测试区（#[cfg(test)] / mod 测试 之后）里的 unwrap/expect/panic 不算生产危险，单列统计。

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
struct 命中 {
    类别: &'static str,
    描述: &'static str,
    行号: usize,
    列: usize,
    片段: String,
    测试区: bool,
}

fn is_word(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// 词边界安全查找：s 中找 pat，且匹配处两侧不粘着词字符（仿 \b）
fn find_word(line: &str, pat: &str, from: usize) -> Option<usize> {
    let bytes = line.as_bytes();
    let p = pat.as_bytes();
    let mut i = from;
    while i + p.len() <= bytes.len() {
        if &bytes[i..i + p.len()] == p {
            let 前 = if i == 0 { ' ' } else { bytes[i - 1] as char };
            let 后 = if i + p.len() >= bytes.len() {
                ' '
            } else {
                bytes[i + p.len()] as char
            };
            if !is_word(前) && !is_word(后) {
                return Some(i);
            }
        }
        // 按 char 边界前进
        i += line[i..].chars().next().map(|c| c.len_utf8()).unwrap_or(1);
    }
    None
}

/// 简单子串查找（无词边界需求）
fn find_plain(line: &str, pat: &str, from: usize) -> Option<usize> {
    line[from..].find(pat).map(|k| k + from)
}

/// [F] as 数值强转：扫 " as " 后的标识符是否数值类型
fn scan_as_cast(line: &str) -> Vec<usize> {
    const NUM_TYPES: [&str; 14] = [
        "u8", "u16", "u32", "u64", "usize", "i8", "i16", "i32", "i64", "isize", "f32", "f64",
        "u128", "i128",
    ];
    let mut out = Vec::new();
    let mut i = 0;
    while let Some(pos) = find_word(line, "as", i) {
        // " as " 后面跳过空格取标识符
        let rest = &line[pos + 2..];
        let t = rest.trim_start();
        let lead = rest.len() - t.len();
        let ident: String = t.chars().take_while(|c| is_word(*c)).collect();
        if NUM_TYPES.contains(&ident.as_str()) && lead > 0 {
            out.push(pos);
        }
        i = pos + 2;
    }
    out
}

/// [I] 字面量索引 [123]
fn scan_lit_index(line: &str) -> Vec<usize> {
    let b = line.as_bytes();
    let mut out = Vec::new();
    for i in 0..b.len() {
        if b[i] == b'[' {
            let mut j = i + 1;
            while j < b.len() && b[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 1 && j < b.len() && b[j] == b']' {
                // 排除数组字面量 [123, ...] 与字符串下标场景外的误报：仅当下一字符非 ','
                let after = if j + 1 < b.len() { b[j + 1] as char } else { ' ' };
                if after != ',' {
                    out.push(i);
                }
            }
        }
    }
    out
}

/// [J] 浮点字面量 == / != 比较
fn scan_float_eq(line: &str) -> Vec<usize> {
    let mut out = Vec::new();
    for op in ["==", "!="] {
        let mut i = 0;
        while let Some(pos) = find_plain(line, op, i) {
            // 左右两侧查 浮点字面量（数字.数字）
            let left = line[..pos].trim_end();
            let right = line[pos + op.len()..].trim_start();
            let has_float = |s: &str| {
                let b = s.as_bytes();
                b.iter()
                    .enumerate()
                    .any(|(k, c)| *c == b'.' && k > 0 && b[k - 1].is_ascii_digit())
            };
            if (has_float(left) && right.starts_with(|c: char| c.is_ascii_digit()))
                || (left.ends_with(|c: char| c.is_ascii_digit()) && has_float(right))
            {
                out.push(pos);
            }
            i = pos + op.len();
        }
    }
    out
}

/// 该行是否处于注释里（整行 // 注释）
fn 是注释行(行: &str) -> bool {
    let t = 行.trim_start();
    t.starts_with("//")
}

/// 是否为 lint deny/allow 声明行（inner attribute）——其中的宏字样不是真实调用
fn 是lint声明行(行: &str) -> bool {
    let t = 行.trim_start();
    t.starts_with("#![") && (t.contains("clippy::") || t.contains("::print_") || t.contains("deny("))
}

fn 扫描文件(path: &Path) -> Vec<命中> {
    let 内容 = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[跳过] {} 无法读取: {e}", path.display());
            return Vec::new();
        }
    };
    let 行数总 = 内容.lines().count();
    let mut 测试区起点 = 行数总 + 1;
    for (i, 行) in 内容.lines().enumerate() {
        let t = 行.trim_start();
        if t.starts_with("#[cfg(test)]") || t.starts_with("mod 测试") {
            测试区起点 = i + 1;
            break;
        }
    }

    let mut hits = Vec::new();
    for (idx, 行) in 内容.lines().enumerate() {
        let 行号 = idx + 1;
        if 是注释行(行) || 是lint声明行(行) {
            continue;
        }
        let 测试区 = 行号 >= 测试区起点;
        let mut 报 = |类别: &'static str, 描述: &'static str, col: usize| {
            let 片段 = 行.trim();
            hits.push(命中 {
                类别,
                描述,
                行号,
                列: col + 1,
                片段: if 片段.chars().count() > 120 {
                    let cut: String = 片段.chars().take(120).collect();
                    format!("{cut}…")
                } else {
                    片段.to_string()
                },
                测试区,
            });
        };

        // A/B：.unwrap() / .expect(
        let mut i = 0;
        while let Some(p) = find_plain(行, ".unwrap()", i) {
            报("A", "unwrap() panic 风险", p);
            i = p + 9;
        }
        i = 0;
        while let Some(p) = find_plain(行, ".expect(", i) {
            报("B", "expect( panic 风险", p);
            i = p + 8;
        }
        // C：panic 家族
        for (mac, name) in [
            ("panic!", "panic! 宏"),
            ("unreachable!", "unreachable! 宏"),
            ("todo!", "todo! 宏"),
            ("unimplemented!", "unimplemented! 宏"),
        ] {
            i = 0;
            while let Some(p) = find_word(行, mac, i) {
                报("C", name, p);
                i = p + mac.len();
            }
        }
        // D：unsafe
        i = 0;
        while let Some(p) = find_word(行, "unsafe", i) {
            报("D", "unsafe 块/函数", p);
            i = p + 6;
        }
        // E：打印宏
        for (mac, name) in [
            ("println!", "println! 打印宏"),
            ("print!", "print! 打印宏"),
            ("eprintln!", "eprintln! 打印宏"),
            ("eprint!", "eprint! 打印宏"),
            ("dbg!", "dbg! 调试宏"),
        ] {
            i = 0;
            while let Some(p) = find_word(行, mac, i) {
                报("E", name, p);
                i = p + mac.len();
            }
        }
        // F：as 数值强转
        for p in scan_as_cast(行) {
            报("F", "as 数值强转(截断风险)", p);
        }
        // G：static mut
        i = 0;
        while let Some(p) = find_word(行, "static", i) {
            if find_plain(行, "mut", p + 6)
                .map(|m| 行[p + 6..m].trim().is_empty())
                .unwrap_or(false)
            {
                报("G", "static mut 全局可变", p);
            }
            i = p + 6;
        }
        // H：#[allow(
        i = 0;
        while let Some(p) = find_plain(行, "#[allow(", i) {
            报("H", "#[allow( lint豁免", p);
            i = p + 8;
        }
        // I：字面量索引
        for p in scan_lit_index(行) {
            报("I", "字面量索引(越界风险)", p);
        }
        // J：浮点 ==
        for p in scan_float_eq(行) {
            报("J", "浮点字面量==比较(精度陷阱)", p);
        }
    }
    hits
}

fn 收集目标(arg: &str) -> Vec<PathBuf> {
    let p = Path::new(arg);
    if p.is_file() {
        return vec![p.to_path_buf()];
    }
    if p.is_dir() {
        let mut out = Vec::new();
        收集递归(p, &mut out);
        return out;
    }
    eprintln!("[警告] 目标不存在: {arg}");
    Vec::new()
}

fn 收集递归(dir: &Path, out: &mut Vec<PathBuf>) {
    let skip = ["target", ".git", "archive_已废弃存档_永不删除_仅留历史参考"];
    if let Ok(rd) = fs::read_dir(dir) {
        for ent in rd.flatten() {
            let p = ent.path();
            let name = p
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            if p.is_dir() {
                if !skip.contains(&name.as_str()) {
                    收集递归(&p, out);
                }
            } else if name.ends_with(".rs") {
                out.push(p);
            }
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let mut strict = false;
    let mut json = false;
    let mut targets = Vec::new();
    for a in &args {
        match a.as_str() {
            "--strict" => strict = true,
            "--json" => json = true,
            other => targets.push(other.to_string()),
        }
    }
    if targets.is_empty() {
        eprintln!("用法: rust-script 检查危险代码.rs [--strict] [--json] <文件或目录>...");
        std::process::exit(2);
    }

    let mut files = Vec::new();
    for t in &targets {
        files.extend(收集目标(t));
    }
    files.sort();

    let mut 全部命中 = Vec::new();
    for f in &files {
        for h in 扫描文件(f) {
            全部命中.push((f.clone(), h));
        }
    }

    if json {
        let mut s = String::from("[");
        for (i, (f, h)) in 全部命中.iter().enumerate() {
            if i > 0 {
                s.push(',');
            }
            let 片段转义: String = h
                .片段
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', " ");
            s.push_str(&format!(
                r#"{{"file":"{}","cat":"{}","desc":"{}","line":{},"col":{},"test_zone":{},"code":"{}"}}"#,
                f.display(),
                h.类别,
                h.描述,
                h.行号,
                h.列,
                h.测试区,
                片段转义
            ));
        }
        s.push(']');
        println!("{s}");
        return;
    }

    println!("════════ 危险代码检查报告 ════════");
    for f in &files {
        let hits: Vec<&命中> = 全部命中
            .iter()
            .filter(|(p, _)| p == f)
            .map(|(_, h)| h)
            .collect();
        let 行数 = fs::read_to_string(f)
            .map(|s| s.lines().count())
            .unwrap_or(0);
        println!(
            "文件: {} （{} 行） 命中 {} 处",
            f.display(),
            行数,
            hits.len()
        );
        for (区名, want_test) in [("生产区", false), ("测试区(允许)", true)] {
            let 组: Vec<&&命中> =
                hits.iter().filter(|h| h.测试区 == want_test).collect();
            if 组.is_empty() {
                continue;
            }
            println!("  ── {区名} ──");
            for h in 组 {
                println!(
                    "  [{}] {}:{}  {}  │ {}",
                    h.类别, h.行号, h.列, h.描述, h.片段
                );
            }
        }
        println!();
    }

    println!("════════ 汇总 ════════");
    let 规则: [(&str, &str); 10] = [
        ("A", "unwrap() panic 风险"),
        ("B", "expect( panic 风险"),
        ("C", "panic 家族宏"),
        ("D", "unsafe 块/函数"),
        ("E", "打印宏(应只用tracing)"),
        ("F", "as 数值强转(截断风险)"),
        ("G", "static mut 全局可变"),
        ("H", "#[allow( lint豁免"),
        ("I", "字面量索引(越界风险)"),
        ("J", "浮点字面量==比较(精度陷阱)"),
    ];
    for (类别, 描述) in 规则 {
        let 生: Vec<&命中> = 全部命中
            .iter()
            .map(|(_, h)| h)
            .filter(|h| h.类别 == 类别 && !h.测试区)
            .collect();
        let 测: Vec<&命中> = 全部命中
            .iter()
            .map(|(_, h)| h)
            .filter(|h| h.类别 == 类别 && h.测试区)
            .collect();
        if 生.is_empty() && 测.is_empty() {
            continue;
        }
        let 行列表: Vec<usize> = 生.iter().map(|h| h.行号).collect();
        let 生行 = if 行列表.len() > 14 {
            let 前: Vec<usize> = 行列表[..14].to_vec();
            format!("行 {前:?} …共{}处", 行列表.len())
        } else {
            format!("行 {行列表:?}")
        };
        println!(
            "  [{类别}] {描述:<28} 生产 {} 处 | 测试 {} 处   {生行}",
            生.len(),
            测.len()
        );
    }
    let 生总 = 全部命中.iter().filter(|(_, h)| !h.测试区).count();
    println!("  生产区命中合计: {生总} 处");

    if strict && 生总 > 0 {
        std::process::exit(1);
    }
}

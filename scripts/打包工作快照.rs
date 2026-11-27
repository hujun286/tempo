#!/usr/bin/env rust-script
---
---
//! 打包工作快照（rust-script 版，替代已废弃的 打包工作快照.sh）· 用户规矩：所有脚本只用 rust-script
//!
//! 功能与 .sh 版一致：把当前工作产物打成时间戳工作包 + git commit（D015 机制）。
//! 用法：rust-script 打包工作快照.rs "本次汇报的一句话说明"

use std::process::Command;

fn main() {
    let 说明 = std::env::args().nth(1).unwrap_or_else(|| "常规汇报".into());
    let root = "/home/z/my-project";
    let ts = 时间戳();
    let pkg = format!("{root}/download/工作包_{ts}.tar.gz");

    // 打包清单：报告 + 决策日志 + 补丁 + scripts + eng 关键文件（排除 target 与旧工作包）
    let 清单 = [
        format!("{root}/download"),
        format!("{root}/scripts"),
        format!("{root}/eng/src/bin/分钟k线落盘_v1_4"),
        format!("{root}/eng/Cargo.toml"),
        format!("{root}/eng/justfile"),
        format!("{root}/eng/deny.toml"),
        format!("{root}/eng/.github"),
        format!("{root}/eng/scripts"),
    ];
    let mut cmd = Command::new("tar");
    cmd.arg("--exclude=download/工作包_*")
        .arg("--exclude=download/github_tempo")
        .arg("--exclude=*.tar.gz")
        .arg("-czf")
        .arg(&pkg);
    for c in &清单 {
        cmd.arg(c);
    }
    let out = cmd.output().expect("tar 启动失败");
    if !out.status.success() {
        eprintln!("tar 警告: {}", String::from_utf8_lossy(&out.stderr));
    }
    println!("工作包: {pkg}");

    // git 存档点（--allow-empty 保证每次汇报都有 commit）
    run(&["git", "-C", root, "add", "-A"]);
    let msg = format!("快照 {ts}：{说明} （含工作包 {ts}）");
    let _ = run_with(&["git", "-C", root, "commit", "--allow-empty", "-m", &msg, "-q"]);
    run(&["git", "-C", root, "log", "--oneline", "-3"]);
}

fn run(args: &[&str]) {
    let out = Command::new(args[0])
        .args(&args[1..])
        .output()
        .expect("命令启动失败");
    println!("{}", String::from_utf8_lossy(&out.stdout).trim());
    if !out.status.success() {
        println!("{}", String::from_utf8_lossy(&out.stderr).trim());
    }
}

fn run_with(args: &[&str]) -> bool {
    Command::new(args[0])
        .args(&args[1..])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn 时间戳() -> String {
    // 本机时区时间戳（与 bash date +%Y%m%d_%H%M 一致）——用 date 命令拿，免引 chrono
    let out = Command::new("date")
        .arg("+%Y%m%d_%H%M")
        .output()
        .expect("date 失败");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

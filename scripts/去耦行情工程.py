#!/usr/bin/env python3
"""行情数据采集/src 去铁索连舟：rust_ctp_test::config 外部依赖 → 本地化。
- config.rs：WORKSPACE_ROOT 从跨工程 const 改为本地函数（env CTP_WORKSPACE_ROOT 优先，兜底 "."）
- config.rs：补 Secrets/MarketData/Trading/HardLimits/DEFAULT_SECRETS（自 ctp_live config.rs 移植）
- main.rs：use rust_ctp_test::config → use crate::config；WORKSPACE_ROOT 调用点加 ()
"""
from pathlib import Path
import re

SRC = Path("/home/z/my-project/ctp-workspace/行情数据采集/src")

# ---------- config.rs ----------
cfg = SRC / "config.rs"
t = cfg.read_text(encoding="utf-8")
assert t.lstrip().startswith("//! 运行配置与全局常量"), "config.rs 头部异常"

旧头 = """//! 运行配置与全局常量（★v1.5 拆模块：行为零变化，只是搬家）。
use rust_ctp_test::config::WORKSPACE_ROOT;
use std::path::{Path, PathBuf};
use std::time::Duration;"""

新头 = '''//! 运行配置与全局常量（★v1.5 拆模块：行为零变化，只是搬家）。
//! ★第3棒修正（2026-11-26，D025 配套）：移除跨工程依赖 rust_ctp_test::config，
//!   WORKSPACE_ROOT 本地化——环境变量 CTP_WORKSPACE_ROOT 优先，兜底 "."（工程根启动），
//!   Secrets 结构自 ctp_live_实盘接入工程/src/config.rs 移植（字段一一对应）。
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

/// 工作区根：环境变量 CTP_WORKSPACE_ROOT 优先，兜底 "."（实盘习惯=从工程根启动进程）。
/// 【为什么不再是 const】上一棒硬编码 "/home/xx/projects/ctp-workspace" 属于铁索连舟
/// （换机器必改代码）；现在改环境变量即可，代码零改动。
pub(crate) fn WORKSPACE_ROOT() -> &'static str {
    static 根: OnceLock<String> = OnceLock::new();
    根.get_or_init(|| std::env::var("CTP_WORKSPACE_ROOT").unwrap_or_else(|_| ".".into()))
}

// ── Secrets 结构（自 ctp_live_实盘接入工程/src/config.rs 移植，字段一字未改）──
pub(crate) const DEFAULT_SECRETS: &str = "./config/secrets.toml";

#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct Secrets {
    pub(crate) market_data: MarketData,
    pub(crate) trading: Trading,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct MarketData {
    pub(crate) md_port: u16,
    pub(crate) td_port: u16,
    pub(crate) trade_reserved_ip: String,
    pub(crate) member_ips: Vec<String>,
    pub(crate) flow_dir: String,
    pub(crate) contracts_file: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct Trading {
    pub(crate) broker_id: String,
    pub(crate) user_id: String,
    pub(crate) password: String,
    pub(crate) app_id: String,
    pub(crate) auth_code: String,
    pub(crate) primary_front_ip: String,
    pub(crate) flow_dir: String,
    pub(crate) settlement_confirm_on_login: bool,
    pub(crate) hard_limits: HardLimits,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct HardLimits {
    pub(crate) max_lots_per_order: i32,
    pub(crate) max_premium_per_lot: f64,
    pub(crate) max_orders_per_minute: usize,
    pub(crate) allow_market_order: bool,
}

impl Secrets {
    pub(crate) fn load(path: &Path) -> anyhow::Result<Self> {
        Ok(toml::from_str(&std::fs::read_to_string(path)?)?)
    }
}'''

assert 旧头 in t, "config.rs 旧头部未匹配"
t = t.replace(旧头, 新头)
cfg.write_text(t, encoding="utf-8")
print("config.rs: 外部依赖移除 + Secrets 本地化完成")

# ---------- main.rs ----------
main = SRC / "main.rs"
m = main.read_text(encoding="utf-8")

旧use = "use rust_ctp_test::config::{Secrets, WORKSPACE_ROOT};"
新use = "use crate::config::{Secrets, WORKSPACE_ROOT};"
assert 旧use in m, "main.rs 外部 use 未匹配"
m = m.replace(旧use, 新use)

# 使用点：Path::new(WORKSPACE_ROOT) → Path::new(WORKSPACE_ROOT())
n1 = m.count("Path::new(WORKSPACE_ROOT)")
m = m.replace("Path::new(WORKSPACE_ROOT)", "Path::new(WORKSPACE_ROOT())")
# 裸引用（188 行 WORKSPACE_ROOT,）
m = m.replace("        WORKSPACE_ROOT,", "        WORKSPACE_ROOT(),")
main.write_text(m, encoding="utf-8")
print(f"main.rs: use 行改 crate 内 + {n1} 处 Path::new 调用点 + 裸引用点 已更新")

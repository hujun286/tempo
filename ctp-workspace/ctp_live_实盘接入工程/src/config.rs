//! 全项目唯一配置定义。所有 bin 通过 `rust_ctp_test::config::*` 复用，
//! 改 secrets.toml 字段只改这里，所有 bin 自动同步。

use serde::Deserialize;

pub const WORKSPACE_ROOT: &str = "/home/xx/projects/ctp-workspace";
pub const DEFAULT_SECRETS: &str = "/home/xx/projects/ctp-workspace/config/secrets.toml";

#[derive(Debug, Clone, Deserialize)]
pub struct Secrets {
    pub market_data: MarketData,
    pub trading: Trading,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarketData {
    pub md_port: u16,
    pub td_port: u16,
    pub trade_reserved_ip: String,
    pub member_ips: Vec<String>,
    pub flow_dir: String,
    pub contracts_file: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Trading {
    pub broker_id: String,
    pub user_id: String,
    pub password: String,
    pub app_id: String,
    pub auth_code: String,
    pub primary_front_ip: String,
    pub flow_dir: String,
    pub settlement_confirm_on_login: bool,
    pub hard_limits: HardLimits,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HardLimits {
    pub max_lots_per_order: i32,
    pub max_premium_per_lot: f64,
    pub max_orders_per_minute: usize,
    pub allow_market_order: bool,
}

impl Secrets {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        Ok(toml::from_str(&std::fs::read_to_string(path)?)?)
    }
}

/// 把 secrets.toml 里的相对路径拼到工作区根。
pub fn resolve_workspace_path(rel: &str) -> std::path::PathBuf {
    let p = std::path::Path::new(rel);
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::path::Path::new(WORKSPACE_ROOT).join(rel)
    }
}

// ============================================================
// 全局配置单例 + accessors（所有 bin 共享）
// ============================================================

use std::sync::OnceLock;

static CFG: OnceLock<Secrets> = OnceLock::new();

pub fn global() -> &'static Secrets {
    CFG.get_or_init(|| {
        let path =
            std::env::var("CTP_SECRETS_PATH").unwrap_or_else(|_| DEFAULT_SECRETS.to_string());
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("读取 {} 失败: {}", path, e));
        toml::from_str(&text).unwrap_or_else(|e| panic!("解析 {} 失败: {}", path, e))
    })
}

pub fn broker() -> &'static str {
    &global().trading.broker_id
}
pub fn user() -> &'static str {
    &global().trading.user_id
}
pub fn password() -> &'static str {
    &global().trading.password
}
pub fn appid() -> &'static str {
    &global().trading.app_id
}
pub fn authcode() -> &'static str {
    &global().trading.auth_code
}

static FRONT_URL: OnceLock<String> = OnceLock::new();
pub fn front_url() -> &'static str {
    FRONT_URL.get_or_init(|| {
        format!(
            "tcp://{}:{}",
            global().trading.primary_front_ip,
            global().market_data.td_port
        )
    })
}

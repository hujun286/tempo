//! 运行配置与全局常量（★v1.5 拆模块：行为零变化，只是搬家）。
//! ★第3棒修正（2026-11-26，D025 配套）：移除跨工程依赖 `rust_ctp_test::config`，
//!   `WORKSPACE_ROOT` 本地化——环境变量 `CTP_WORKSPACE_ROOT` 优先，兜底 "."（工程根启动），
//!   Secrets 结构自 `ctp_live_实盘接入工程/src/config.rs` 移植（字段一一对应）。
// 宪法：nonstandard_style 豁免（带理由）——工程全局中文标识符；WORKSPACE_ROOT 保持
// 大写常量风格与 ctp_live 原命名一致，避免下游调用点二次改名。
#![allow(
    nonstandard_style,
    reason = "工程全局中文标识符与 UP_SNAKE 常量风格，与 ctp_live 原工程命名对齐"
)]
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

/// 工作区根：环境变量 `CTP_WORKSPACE_ROOT` 优先，兜底 "."（实盘习惯=从工程根启动进程）。
/// 【为什么不再是 const】上一棒硬编码 "/home/xx/projects/ctp-workspace" 属于铁索连舟
/// （换机器必改代码）；现在改环境变量即可，代码零改动。
pub fn WORKSPACE_ROOT() -> &'static str {
    static 根: OnceLock<String> = OnceLock::new();
    根.get_or_init(|| std::env::var("CTP_WORKSPACE_ROOT").unwrap_or_else(|_| ".".into()))
}

// ── Secrets 结构（自 ctp_live_实盘接入工程/src/config.rs 移植，字段一字未改）──
pub const DEFAULT_SECRETS: &str = "./config/secrets.toml";

#[derive(Debug, Clone, serde::Deserialize)]
pub struct Secrets {
    pub(crate) market_data: MarketData,
    pub(crate) trading: Trading,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[allow(
    dead_code,
    reason = "字段与 secrets.toml 一一对应；td_port/trade_reserved_ip 由交易工程读取"
)]
pub struct MarketData {
    pub(crate) md_port: u16,
    pub(crate) td_port: u16,
    pub(crate) trade_reserved_ip: String,
    pub(crate) member_ips: Vec<String>,
    pub(crate) flow_dir: String,
    pub(crate) contracts_file: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[allow(
    dead_code,
    reason = "Trading 段与 secrets.toml 全字段一一对应；行情工程暂只读 broker_id，保留完整结构防配置漂移（交易工程共用同一份 toml）"
)]
pub struct Trading {
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
#[allow(dead_code, reason = "硬限风控字段由交易工程读取；行情工程仅随结构携带")]
pub struct HardLimits {
    pub(crate) max_lots_per_order: i32,
    pub(crate) max_premium_per_lot: f64,
    pub(crate) max_orders_per_minute: usize,
    pub(crate) allow_market_order: bool,
}

// ============================================================
// 常量（改这里=改全局行为，散落在代码里的数字都收到这，符合"配置集中管理"）
// ============================================================

pub const MD_DYNLIB_REL: &str = "lib/libthostmduserapi_se.so";
pub const SUBSCRIBE_BATCH: usize = 500;
pub const SUBSCRIBE_DELAY_MS: u64 = 300;
pub const REPORT_INTERVAL_SEC: u64 = 5;

pub const RECONNECT_BASE_MS: u64 = 1_000;
pub const RECONNECT_MAX_MS: u64 = 60_000;
pub const LOGIN_SETTLE_MS: u64 = 1_500;

// ★v1.5 方案B 守护常量（D024）：三类异常全部收敛到 teardown→重建，阈值关系由
//   单测「方案b守护常量下限」钉死——改任何一个数之前先跑那个测试想清楚。
pub const CONNECT_TIMEOUT_MS: u64 = 15_000; // spawn_new 后 15 秒无 Connected → 主动重建
pub const DISCONNECT_REBUILD_MS: u64 = 60_000; // 断线后 60 秒未回连（CTP 自动重连失效）→ 主动重建
pub const LOGIN_FAIL_REBUILD: u64 = 3; // 连续 3 次登录失败 → 主动重建（防死循环打柜台）
pub const 回拨告警节流: Duration = Duration::from_secs(300); // 🔴3 同合约回拨告警 5 分钟一条，不刷屏

/// ★本版新增（可随时调）：分片落盘 fsync 间隔（秒）。
///   含义：落盘线程最多每这么多秒把追加的 bar 强制刷到磁盘一次。
///   值越小越抗崩（丢得越少）但 fsync 越频；越大越省 IO 但崩了丢得多。
///   设 60 = 最多丢 60 秒；设 5 = 最多丢 5 秒；笔记本有电池、追求极致省 IO 可设 120。
///   注意：不管设多少，每根 bar 都会"追加 + flush(到内核缓冲)"，只是 fsync(落物理盘)按这个间隔。
pub const 分片落盘秒: u64 = 60;

// 落盘线程与聚合线程之间的在途 bar 队列容量。满了会【阻塞背压】，绝不静默丢 bar。
// 全天满盘约 50 万根，100 万绰绰有余；峰值内存 = 100万 × ~200B ≈ 200MB 上限。

// 分片文件 / 合并成品的相对目录（都挂在 工作区根/data_数据 下）。

pub const MINUTE_MS: i64 = 60_000;

/// ★v2.1 时戳偏离容忍度（分钟）：交易所戳(UpdateTime)与本机到达时刻之差超过它 → 判为登录重演/陈旧快照并丢弃。
/// 机理：CTP 登录/重连时前置会重发每只合约最后一帧快照，夜盘已收品种的重发快照交易所戳停在收盘时刻，
/// 与本机到达时刻差可达数十分钟；真行情的戳与本机时钟偏差亚秒级，容忍 2 分钟已极宽松。
pub const 时戳容忍分钟: i32 = 2;
// ── ★v1.3/v1.4 新增：运行配置（规则20「配置集中管理」）─────────────────────
// 【为什么有这一层】原来 分片落盘秒 等是 const 写死，调一个数就要改 .rs + 重编译。
// 现在：toml 有值用 toml，没值用内建默认；找不到配置文件只打 warn，绝不 panic。
// 【线程怎么共享】配置在 main 最顶部载入一次，之后只读；线程需要的单个值（如 fsync 间隔）
// 直接按值 clone 进去，不去共享整个 struct，也就不需要 Arc/锁。
pub const 默认_分片落盘秒: u64 = 60; // 0=不额外fsync · 5=极致抗崩 · 60=推荐
pub const 默认_BAR队列容量: usize = 1_000_000;
pub const 默认_分片目录: &str = "data_数据/分钟分片";
pub const 默认_成品目录: &str = "data_数据/分钟K线";
pub const 配置_文件名: &str = "配置_分钟K线.toml";
/// 全市场粗过滤表默认值：金属 01:00 / 贵金属原油 02:30 / 中金所无夜盘 → 并集最宽口径。
pub const 默认_交易时段: [(i32, i32); 4] = [(900, 1015), (1030, 1130), (1330, 1515), (2100, 230)];

/// 时段表判定：HHMM 闭区间；止<起 = 跨午夜（2100→230：t≥2100 或 t≤230）。
#[allow(
    clippy::redundant_pub_crate,
    reason = "聚合模块经 crate::config:: 跨模块调用，private 不可见，pub(crate) 是必要可见性"
)]
pub(crate) fn 在交易时段内(hhmm: i32, 表: &[(i32, i32)]) -> bool {
    表.iter().any(|(起, 止)| {
        if 止 >= 起 {
            hhmm >= *起 && hhmm <= *止
        } else {
            hhmm >= *起 || hhmm <= *止
        }
    })
}

/// ★中文键兼容层：toml 1.x 解析器只认 ASCII 裸键，本工程配置键全是中文 ——
/// 预处理把非 ASCII 裸键统一加引号（引号键是合法 TOML，语义完全不变），
/// 这样用户的 `配置_分钟K线.toml`（[落盘]、快照间隔秒…）一个字都不用改。
#[allow(
    clippy::indexing_slicing,
    reason = "字节切片边界由 find()('=')/trim_start()/首尾字符判定保证，且行内不含非ASCII键时不切；纯文本预处理非热路径"
)]
fn 引号化中文键(文本: &str) -> String {
    let mut 出: Vec<String> = Vec::new();
    for 原行 in 文本.lines() {
        let 行 = 原行.trim_start();
        // 缩进 = 原行开头到首个非空白前的部分（字节下标来自 trim_start，边界安全）
        let 缩进 = &原行[..原行.len() - 行.len()];
        let 段头 = 行.starts_with('[');
        let 注释 = 行.starts_with('#');
        let 新行 = if 注释 {
            None
        } else if 段头 {
            处理段头(行, 缩进)
        } else {
            处理键值(行, 缩进)
        };
        if let Some(新) = 新行 {
            出.push(新);
        } else {
            出.push(原行.to_string());
        }
    }
    出.join("\n")
}

/// 键值行：非 ASCII 裸键 → 引号键（`快照间隔秒 = 0` → `"快照间隔秒" = 0`）。
#[allow(
    clippy::indexing_slicing,
    reason = "切片下标来自 find('=')，为 char 边界安全的字节下标"
)]
fn 处理键值(行: &str, 缩进: &str) -> Option<String> {
    let 键尾 = 行.find('=')?;
    let 键 = 行[..键尾].trim();
    let 已引号 =
        (键.starts_with('"') && 键.ends_with('"')) || (键.starts_with('\'') && 键.ends_with('\''));
    let 纯ascii裸键 = 键
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-');
    if 已引号 || 纯ascii裸键 {
        return None;
    }
    Some(format!("{缩进}\"{键}\" ={}", &行[键尾 + 1..]))
}

/// 段头行：`[落盘]` → `["落盘"]`；数组行/收尾/嵌套一律不动。
#[allow(
    clippy::indexing_slicing,
    reason = "首尾字符判定后去首尾各一字节（'['与']' 均单字节），边界安全"
)]
fn 处理段头(行: &str, 缩进: &str) -> Option<String> {
    if !行.ends_with(']') {
        return None;
    }
    let 内 = 行[1..行.len() - 1].trim();
    let 已引号 = 内.starts_with('"') || 内.starts_with('\'');
    // 只引真正的段头：排除数组收尾 `]`、数组行 `[900, 1015],`、嵌套 `[[…]]`
    let 是段头名 =
        !内.is_empty() && !内.contains(',') && !内.starts_with('[') && !内.ends_with(',');
    if 已引号 || !是段头名 {
        return None;
    }
    Some(format!("{缩进}[\"{内}\"]"))
}

/// 布尔开关宽容反序列化：true/false 之外也认 0/1 与 "0"/"false"/"off"
/// （兼容 v1.5 老配置 `归档压缩 = 0` 的写法）。
fn 开关宽容<'de, D>(d: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(serde::Deserialize)]
    #[serde(untagged)]
    enum 开关 {
        布(bool),
        数(i64),
        字(String),
    }
    Ok(match serde::Deserialize::deserialize(d)? {
        开关::布(b) => Some(b),
        开关::数(0) => Some(false),
        开关::数(_) => Some(true),
        开关::字(s) => Some(!matches!(s.as_str(), "0" | "false" | "off")),
    })
}

/// toml 的 [落盘] 段（v2.0 形态）。全部 Option：没写的键一律走内建默认。
#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct 段落盘 {
    快照间隔秒: Option<u64>,
    #[serde(deserialize_with = "开关宽容")]
    #[allow(
        dead_code,
        reason = "v2.0 配置占位：原子写恒强制 true（已在载入时日志声明），字段留给 v1.6 接入写路径时消费"
    )]
    原子写: Option<bool>,
    #[serde(deserialize_with = "开关宽容")]
    同日合并: Option<bool>,
    #[serde(deserialize_with = "开关宽容")]
    双写成交量增量: Option<bool>,
    #[serde(deserialize_with = "开关宽容")]
    过滤盘前重演: Option<bool>,
    输出目录: Option<String>,
    // v1.5 平铺键在 [落盘] 段内也认（有人手滑写进来）：
    分片落盘秒: Option<u64>,
    bar队列容量: Option<usize>,
    分片目录: Option<String>,
    成品目录: Option<String>,
    #[serde(deserialize_with = "开关宽容")]
    归档压缩: Option<bool>,
}

/// toml 的 [交易时段] 段：时段 = [[900,1015], ...]（跨午夜 止<起）。
#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct 段交易时段 {
    时段: Option<Vec<(i32, i32)>>,
}

/// 根层。v2.0 键在 [落盘]/[交易时段] 段；v1.5 老配置是根层平铺键 —— 两种形态同时兼容。
#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct 外层配置 {
    落盘: 段落盘,
    交易时段: 段交易时段,
    分片落盘秒: Option<u64>,
    bar队列容量: Option<usize>,
    分片目录: Option<String>,
    成品目录: Option<String>,
    #[serde(deserialize_with = "开关宽容")]
    归档压缩: Option<bool>,
}

/// 运行期生效配置。v2.0 唯一真源：`配置_分钟K线.toml`。
#[derive(Debug, Clone)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "运行开关本就是布尔语义（归档/同日合并/双写/盘前过滤/原子写），改枚举反而降低可读性"
)]
pub struct 运行配置 {
    pub(crate) 分片落盘秒: u64,
    pub(crate) bar队列容量: usize,
    pub(crate) 分片目录_rel: String,
    pub(crate) 成品目录_rel: String,
    /// ★v1.5 归档压缩（D028）：合并成功后把"非今日"且已静默的 jsonl 压成 .jsonl.zst。
    /// 默认开；toml 里写 `归档压缩 = 0` 关闭。
    pub(crate) 归档压缩: bool,
    /// ★v2.0 同日合并（D039 补实装）：成品 parquet 已存在时读回旧 bar 与 jsonl 合并去重再写。
    /// 去重键 (合约, bar时间)，真实 bar 优先于补的 gap bar。治 11-24 被覆盖事故。
    pub(crate) 同日合并: bool,
    /// ★v2.0 双写成交量增量：`volume_1m`/`turnover_1m` 增量列（13 列）。关掉 → 11 列。
    pub(crate) 双写成交量增量: bool,
    /// ★v2.0 过滤盘前重演：非交易时段 且 当日累计成交量=0 的 tick 直接丢
    /// （治 h5i-db 00:57 volume=0 结算行情污染；冷门合约盘中零成交本来也产不出有意义K线）。
    pub(crate) 过滤盘前重演: bool,
    /// ★v2.0 定时快照间隔（秒）。0=关闭（当前选定：jsonl+fsync 已兜底，最多丢 ≤分片落盘秒）。
    /// >0 时本版本打 INFO 说明由 jsonl 兜底、快照通道 v1.6 启用（不静默吞配置）。
    pub(crate) 快照间隔秒: u64,
    /// ★v2.0 原子写（.tmp→fsync→rename）。恒 true：false 不接受（半截 parquet 下游读报错）。
    #[allow(
        dead_code,
        reason = "v2.0 配置占位：原子写恒强制 true（已在载入时日志声明），字段留给 v1.6 接入写路径时消费"
    )]
    pub(crate) 原子写: bool,
    /// 全市场粗过滤表（HHMM 闭区间；跨午夜 止<起）。用于盘前重演判定。
    pub(crate) 交易时段: Vec<(i32, i32)>,
}
impl 运行配置 {
    pub(crate) fn 默认() -> Self {
        Self {
            分片落盘秒: 默认_分片落盘秒,
            bar队列容量: 默认_BAR队列容量,
            分片目录_rel: 默认_分片目录.to_string(),
            成品目录_rel: 默认_成品目录.to_string(),
            归档压缩: true,
            同日合并: true,
            双写成交量增量: true,
            过滤盘前重演: true,
            快照间隔秒: 0,
            原子写: true,
            交易时段: 默认_交易时段.to_vec(),
        }
    }

    /// serde+toml 解析（★v2.0 升级：嵌套 [表] 终于能用；键名=字段名，一一对应）。
    /// 【兼容】v2.0 [落盘] 段 与 v1.5 根层平铺键 同时认，[落盘] 段优先。
    /// 没写的键一律内建默认；解析失败也只用默认，绝不 panic（配置错误不能让程序起不来）。
    fn 从toml(文本: &str, 来源: &str) -> Self {
        let mut c = Self::默认();
        let 外: 外层配置 = match toml::from_str(&引号化中文键(文本)) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("[CFG] {} 解析失败: {e}（改用内建默认）", 来源);
                return c;
            }
        };
        let 落 = &外.落盘;
        // 取值优先级：[落盘] 段 → 根层平铺 → 内建默认。
        let 取数 = |段值: Option<u64>, 根值: Option<u64>| 段值.or(根值);
        let 取数usize = |段值: Option<usize>, 根值: Option<usize>| 段值.or(根值);
        let 取文 = |段值: Option<&String>, 根值: Option<&String>| 段值.or(根值).cloned();

        if let Some(v) = 取数(落.分片落盘秒, 外.分片落盘秒) {
            c.分片落盘秒 = v;
        }
        if let Some(v) = 取数usize(落.bar队列容量, 外.bar队列容量) {
            c.bar队列容量 = v;
        }
        if let Some(v) = 取文(落.分片目录.as_ref(), 外.分片目录.as_ref()) {
            c.分片目录_rel = v;
        }
        // 成品目录：v2.0 叫 输出目录，v1.5 叫 成品目录 —— 两个键都认（输出目录优先）。
        if let Some(v) = 落
            .输出目录
            .as_ref()
            .or(落.成品目录.as_ref())
            .or(外.成品目录.as_ref())
        {
            c.成品目录_rel.clone_from(v); // 借用复用已有 String 缓冲（clone_from）
        }
        if let Some(v) = 落.归档压缩.or(外.归档压缩) {
            c.归档压缩 = v;
        }
        if let Some(v) = 落.同日合并 {
            c.同日合并 = v;
        }
        if let Some(v) = 落.双写成交量增量 {
            c.双写成交量增量 = v;
        }
        if let Some(v) = 落.过滤盘前重演 {
            c.过滤盘前重演 = v;
        }
        if let Some(v) = 落.快照间隔秒 {
            if v > 0 && v < 5 {
                // 配置写错不致命：打日志 + 保留默认（规则：配置错误不能让程序起不来）
                tracing::error!("[CFG] 快照间隔秒={v} 小于最小允许值 5（写太频没意义），按 0 关闭");
            } else {
                c.快照间隔秒 = v;
            }
        }
        if 落.原子写 == Some(false) {
            tracing::error!(
                "[CFG] 原子写 = false 不接受（会留半截 parquet，DuckDB 读报错），强制 true"
            );
        }
        if let Some(v) = 外.交易时段.时段 {
            if v.is_empty() || !v.iter().all(|(起, 止)| *起 > 0 && *止 > 0 && *起 != *止) {
                tracing::error!("[CFG] 交易时段.时段 为空或有非法项（起止为0或相同），保留默认表");
            } else {
                c.交易时段 = v;
            }
        }
        c
    }

    /// 查找顺序：①环境变量 分钟K线配置 指向的文件 `②CARGO_MANIFEST_DIR（工程根）③工作区根`。
    /// 三处都找是为了"不管从哪个目录启动、toml 放在哪"都能读到，省掉"配置在却说没有"的排查。
    pub(crate) fn 载入() -> Self {
        let mut 候选: Vec<PathBuf> = Vec::new();
        if let Some(p) = std::env::var_os("分钟K线配置") {
            候选.push(PathBuf::from(p));
        }
        候选.push(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(配置_文件名));
        候选.push(Path::new(WORKSPACE_ROOT()).join(配置_文件名));
        for p in &候选 {
            if p.is_file() {
                match std::fs::read_to_string(p) {
                    Ok(t) => {
                        tracing::info!("[CFG] 已加载运行配置 {}", p.display());
                        let c = Self::从toml(&t, &p.display().to_string());
                        if c.快照间隔秒 > 0 {
                            tracing::info!(
                                "[CFG] 快照间隔秒={} 已收到：本版本数据安全由 jsonl+fsync 兜底（最多丢≤{}s），定时快照通道 v1.6 启用，此期间不生效",
                                c.快照间隔秒,
                                c.分片落盘秒
                            );
                        }
                        return c;
                    }
                    Err(e) => {
                        tracing::error!("[CFG] 读配置 {} 失败: {}（改用内建默认）", p.display(), e);
                    }
                }
            }
        }
        tracing::error!(
            "[CFG] ⚠ 没找到 {}，使用内建默认：分片落盘秒={}、bar队列容量={}、分片目录={}",
            配置_文件名,
            默认_分片落盘秒,
            默认_BAR队列容量,
            默认_分片目录
        );
        Self::默认()
    }
}

#[cfg(test)]
mod 测试 {
    use super::*;

    #[test]
    #[expect(
        clippy::assertions_on_constants,
        reason = "常量钉死测试：本测试对象就是编译期常量的约束关系，断言恒真是有意为之；保留运行期形态以钉住文档化约束，改常量前先想清楚这条测试"
    )]
    fn 方案b守护常量下限() {
        // ① 连接超时给慢网络留足余地，但不超过断线重建窗口
        assert!(CONNECT_TIMEOUT_MS >= 10_000);
        assert!(CONNECT_TIMEOUT_MS <= DISCONNECT_REBUILD_MS);
        // ② 断线重建窗口覆盖 CTP 自动重连的合理时长，又不能无限等
        assert!(DISCONNECT_REBUILD_MS >= CONNECT_TIMEOUT_MS);
        assert!(DISCONNECT_REBUILD_MS <= 120_000);
        // ③ 登录失败重建：太敏感被柜台风控打爆，太迟钝又死循环
        assert!(LOGIN_FAIL_REBUILD >= 2 && LOGIN_FAIL_REBUILD <= 10);
        // ④ 退避序列上界不超过断线重建窗口
        assert!(RECONNECT_BASE_MS >= 500);
        assert!(RECONNECT_MAX_MS <= DISCONNECT_REBUILD_MS);
        // ⑤ 回拨告警节流足够长（不刷屏），又不至于掩盖持续回拨
        assert!(回拨告警节流 >= Duration::from_secs(60));
        // ⑥ settle 时间必须小于连接超时（否则连接中永远先触发超时）
        assert!(LOGIN_SETTLE_MS < CONNECT_TIMEOUT_MS);
    }

    #[test]
    fn v2_形态_段配置解析() {
        let 文 = r#"
[落盘]
快照间隔秒 = 0
原子写 = true
同日合并 = true
双写成交量增量 = true
过滤盘前重演 = true
输出目录 = "data_数据/分钟K线/"
[交易时段]
时段 = [
  [900, 1015],
  [1330, 1515],
  [2100, 230],
]
"#;
        let c = 运行配置::从toml(文, "测试");
        assert!(c.同日合并 && c.双写成交量增量 && c.过滤盘前重演 && c.原子写);
        assert_eq!(c.快照间隔秒, 0);
        assert_eq!(c.成品目录_rel, "data_数据/分钟K线/");
        assert_eq!(c.交易时段, vec![(900, 1015), (1330, 1515), (2100, 230)]);
        // 其余键走内建默认
        assert_eq!(c.分片落盘秒, 默认_分片落盘秒);
    }

    #[test]
    fn v1_5平铺兼容() {
        let 文 = r#"
分片落盘秒 = 5
bar队列容量 = 500
成品目录 = "data_数据/成品"
归档压缩 = 0
"#;
        let c = 运行配置::从toml(文, "测试");
        assert_eq!(c.分片落盘秒, 5);
        assert_eq!(c.bar队列容量, 500);
        assert_eq!(c.成品目录_rel, "data_数据/成品");
        assert!(!c.归档压缩);
        // v2.0 专属键没写 → 默认
        assert!(c.同日合并);
    }

    #[test]
    fn 段优先于根平铺() {
        let 文 = "分片落盘秒 = 5\n\n[落盘]\n分片落盘秒 = 30\n";
        let c = 运行配置::从toml(文, "测试");
        assert_eq!(c.分片落盘秒, 30);
    }

    #[test]
    fn 快照间隔_小于5拒绝() {
        let 文 = "[落盘]\n快照间隔秒 = 3\n";
        let c = 运行配置::从toml(文, "测试");
        assert_eq!(c.快照间隔秒, 0, "3<5 应按0关闭");
    }

    #[test]
    fn 跨午夜时段判定() {
        assert!(在交易时段内(2130, &默认_交易时段), "夜盘中段");
        assert!(在交易时段内(150, &默认_交易时段), "跨午夜凌晨2:50");
        assert!(!在交易时段内(1200, &默认_交易时段), "午休");
        assert!(!在交易时段内(400, &默认_交易时段), "凌晨4点收盘后");
        assert!(在交易时段内(1015, &默认_交易时段), "闭区间右端");
    }

    #[test]
    fn 非法时段保留默认() {
        let 文 = "[交易时段]\n时段 = [[0, 0], [900, 900]]\n";
        let c = 运行配置::从toml(文, "测试");
        assert_eq!(c.交易时段, 默认_交易时段.to_vec());
    }

    #[test]
    fn 多行时段数组与收尾括号() {
        // 模拟用户实机配置的真实形态：多行数组 + 行首收尾 `]` + 行内注释含 '='
        let 文 = "[交易时段]\n时段 = [\n  [900, 1015],\n  [1330, 1515],\n  [2100, 230],   # 230 = 次日 02:30，止<起=跨午夜\n]\n";
        let c = 运行配置::从toml(文, "测试");
        assert_eq!(c.交易时段, vec![(900, 1015), (1330, 1515), (2100, 230)]);
    }

    #[test]
    fn 引号化_不动数组和注释() {
        let 原文 = "# 快照间隔秒 = 53 注释里带=不动\n[落盘]\n快照间隔秒 = 0\n时段 = [\n  [900, 1015],\n]\n";
        let 化 = 引号化中文键(原文);
        assert!(化.contains("[\"落盘\"]"));
        assert!(化.contains("\"快照间隔秒\" = 0"));
        assert!(化.contains("# 快照间隔秒 = 53 注释里带=不动"));
        assert!(化.contains("  [900, 1015],"));
        assert!(化.contains("\n]"));
    }
}

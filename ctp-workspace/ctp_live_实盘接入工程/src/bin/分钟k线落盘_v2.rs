// =============================================================================
//  分钟K线落盘 v2.0.0（单文件·全中文命名）
// -----------------------------------------------------------------------------
//  为什么是“单文件”：
//    v1.1（分钟k线落盘_完整版.rs）就是单文件，741 行，实盘跑出 65 万根，稳。
//    v2 最初被我拆成 8 个 .rs 模块，结果踩了 Rust 的硬规则：
//      `mod config;` 必须去找一个 exactly 叫 config.rs 的文件，
//      单文件无跨文件模块，不存在“文件名 / mod 声明 / 模块路径”三处对齐问题，
//      错一处就 E0433/E0425 连环报错。中文文件名又叠了跨平台乱码问题。
//    结论：对“不熟 Rust 的团队”，单文件 > 多模块。没有 mod、没有跨文件路径，
//    grep 任何一个符号都能立刻找到，这也正是 v1.1 一直没出这个错的原因。
//    Rust 原生完全支持 UTF-8：中文文件名、中文函数名、中文变量名、中文注释
//    全部合法，不需要装任何“中文包/UTF-8 包”。v1.1 能跑就是铁证。
//
//  这一版相对 v1.1 新增/改进（都是你点过头的）：
//    1) volume / turnover 双写：volume 忠实存 CTP 当日累计，另加 volume_1m
//       （同合约同交易日差分增量），turnover / turnover_1m 同理。回测开箱即用，
//       不会再踩“拿累计当单分钟量用、结果严重偏大”的坑。
//    2) 以交易所时间为准（可配 以交易所时间分桶，默认开）：bar 的分钟归属用
//       CTP 的 TradingDay + UpdateTime 拼出来，完全不碰本机时钟/时区，
//       换到伦敦/北京/任何时区、电脑时间偏一点都不影响 OHLC 与分拣。
//       v1.1 用的是“收包本机时间”（Local::now），本机时钟准(NTP)时等价，
//       但严格说依赖了电脑时钟；v2 默认走交易所时间，更贴你的要求。
//    3) 快照（可配 快照间隔秒，默认 0 = 全天攒内存、只有 Ctrl+C 落一次盘）：
//       笔记本模式，断电有电池兜底。哪天想要“崩了最多丢 N 秒”，把那个数
//       改成 53（秒）即可，代码一个字不用动，并自动启用“重启读回当天快照”。
//    4) 同日合并写：夜盘 00:00-02:27 与日盘 09:00-15:00 墙钟同日，落盘前先
//       读旧文件、按 (合约, bar时间) 去重合并再原子写，不再被拆成 .bak。
//    5) 丢包计数：通道满时 try_send 失败计数，周期日志里报，峰值丢行情不再静默。
//    6) 原子写：先写 .tmp → fsync → rename，进程被杀不留半截文件。
//    7) Ctrl+C 两次语义：第 1 次优雅退出（停重连→收尾→落当天→自动退进程），
//       只有“卡住不退”时才按第 2 次强杀(exit 130)。正常情况下按一次就够。
//    8) 断线重连：member 线程 api.join() 返回即退避重连(1→2→4…封顶60s)，
//       重登、重订阅，永不退出线程。
//
//  仍留到 v2.1 的（本版不做，避免一次改太多没法定位问题）：
//    - 断线>60s 的逐分钟补平盘 K 线（is_gap 列已就位，本版恒写 false，不造假bar）；
//    - 3:1 教学注释大扩写、.agent/ 四份文档、deny.toml/clippy.toml 策略文件；
//    - 合约清单按交易日自动刷新（现在仍启动时一次性分片）。
//
//  编译（chrono 必须精确锁 =0.4.38，>=0.4.39 会给 Datelike 加 quarter()，
//        与 arrow-arith 52 撞名，编不过 —— 这是本项目最贵的一条经验）：
//    Cargo.toml [dependencies] 里：
//      chrono      = "=0.4.38"
//      ctp2rs      = "0.1.9"
//      arrow       = "52"
//      parquet     = "52"
//      serde       = { version = "1", features = ["derive"] }
//      toml        = "0.8"
//      thiserror   = "1"
//      tracing     = "0.1"
//      tracing-subscriber = { version = "0.3", features = ["env-filter"] }
//    cargo build --release --bin 分钟k线落盘_v2
//    cargo test  --bin 分钟k线落盘_v2        # 跑时间/合并的纯函数单测
//    cargo run   --release --bin 分钟k线落盘_v2 -- /path/to/secrets.toml
// =============================================================================

// ── crate 级属性 ───────────────────────────────────────────────────────────────
// deny(unsafe_code)：本 crate 一个 unsafe 块都不许有。FFI 的 unsafe 全被 ctp2rs
// 封在它自己内部，我们调用 register_spi / create_api 都是“安全函数”，所以这条
// 能开。Box::into_raw 也是安全函数（只是把 Box 变成裸指针交给 C++ 侧持有）。
#![deny(unsafe_code)]

// ── 标准库 ────────────────────────────────────────────────────────────────────
use std::collections::HashMap;
use std::fs;
use std::io::Write;                                   // 给文件 .write_all / .flush 用
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

// ── 第三方 ────────────────────────────────────────────────────────────────────
// chrono：时间处理。Timelike 提供 .hour()/.minute()/.second()；TimeZone 提供
//   .from_local_datetime()/.timestamp_millis_opt()。
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime, TimeZone, Timelike, Utc};
use serde::Deserialize;
use thiserror::Error;
use tracing::{info, warn};

// arrow / parquet：列式落盘。这些 use 与 v1.1 完全一致（v1.1 已验证可编）。
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, RecordBatch, StringArray,
    TimestampMillisecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

// 复用主实盘 crate 里的配置结构体与工作区根常量（Secrets 读账号/前置 IP）。
// 注意：rust_ctp_test 是本 bin 所属的 crate，bin 可以 use 自己 crate 的 lib，
// v1.1 就是这么干的、能编能跑，所以这里照抄。WORKSPACE_ROOT 是编译期定下的
// 工作区根，所有相对路径都挂它下面，全工程无一处硬编码绝对路径。
use rust_ctp_test::config::{Secrets, WORKSPACE_ROOT};

// =============================================================================
//  第 1 段：错误类型（thiserror 派生，业务代码统一 Result<T, 错误>，禁用 unwrap）
// -----------------------------------------------------------------------------
//  规则 15/16：业务代码禁 unwrap/expect；库/内部用 thiserror 定义错误枚举，
//  每个变体带中文 #[error] 文案，? 运算符自动转换，日志直接可读。
// =============================================================================
#[derive(Debug, Error)]
pub enum 错误 {
    #[error("配置错误: {0}")]
    配置(String),
    #[error("合约清单错误: {0}")]
    合约清单(String),
    #[error("落盘错误: {0}")]
    落盘(String),
    #[error("时间处理错误: {0}")]
    时间(String),
    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),
    #[error("Parquet 错误: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Arrow 错误: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Toml 解析错误: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("通道已断开")]
    通道,
    #[error("锁中毒(某线程持锁时 panic): {0}")]
    锁中毒(String),
}

// 一个小便利：把 Mutex 加锁失败统一转成“锁中毒”错误，省得每处都写 map_err。
// Mutex::lock 返回 Result<Guard, PoisonError>；PoisonError 说明别的线程持锁 panic 了，
// 数据可能不一致，这里选择报错而非吞掉（宁可退也不写脏数据）。
fn 加锁<T>(m: &Mutex<T>, 场景: &str) -> Result<std::sync::MutexGuard<'_, T>, 错误> {
    m.lock().map_err(|_| 错误::锁中毒(场景.to_string()))
}

// =============================================================================
//  第 2 段：配置（集中管理，唯一真源 = 配置_分钟K线_v2.toml）
// -----------------------------------------------------------------------------
//  规则 20：运行配置全进结构体+文件，源码里不留魔法常量。每个字段 serde default，
//  缺文件也能用默认值跑起来。改行为改 toml，不改代码。
// =============================================================================
#[derive(Debug, Clone, Deserialize)]
pub struct 配置 {
    #[serde(default)]
    pub 落盘: 落盘配置,
    #[serde(default)]
    pub 交易时段: 交易时段配置,
    // —— 运行参数（也可放 toml 顶层） ——
    // 以交易所时间分桶：true=用 CTP 的 TradingDay+UpdateTime 拼分钟(推荐)；
    //                   false=退回 v1.1 的“收包本机时间”行为。
    #[serde(default = "默认真")]
    pub 以交易所时间分桶: bool,
    // 过滤盘前重演：丢掉“最新价=0”的 bar（盘前重演/结算行情通常量为0），防污染。
    #[serde(default = "默认真")]
    pub 过滤盘前重演: bool,
    // 快照间隔秒：0=全天攒内存、只有 Ctrl+C 落盘（笔记本模式，默认）；
    //             0 且 >4 会被 校验并修正 掰到 ≥5；设 53 即每 53 秒原子快照一次。
    #[serde(default)]
    pub 快照间隔秒: u64,
    // 统计打印周期（秒）。
    #[serde(default = "默认统计周期")]
    pub 统计周期秒: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct 落盘配置 {
    // 输出目录（相对路径挂 WORKSPACE_ROOT 下）。
    #[serde(default = "默认输出目录")]
    pub 输出目录: String,
    // 双写成交量增量：true=额外落 volume_1m / turnover_1m 两列(推荐)。
    #[serde(default = "默认真")]
    pub 双写成交量增量: bool,
    // 原子写：先写 .tmp→fsync→rename。关掉则直接写目标(有半截文件风险)。
    #[serde(default = "默认真")]
    pub 原子写: bool,
    // 同日合并：目标文件已存在时先读旧 bar 合并去重再写(治夜盘+日盘同日拆散)。
    #[serde(default = "默认真")]
    pub 同日合并: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct 交易时段配置 {
    // 全市场粗过滤时段 (起始HHMM, 结束HHMM)。v2 默认“全推全收”不过滤
    // (非盘中 CTP 本就不推行情)，这里保留供精确到品种时扩展。
    #[serde(default = "默认时段")]
    pub 时段: Vec<(u32, u32)>,
}

fn 默认真() -> bool { true }
fn 默认输出目录() -> String { "data_数据/分钟K线/".to_string() }
fn 默认统计周期() -> u64 { 5 }
fn 默认时段() -> Vec<(u32, u32)> {
    vec![(900, 1015), (1030, 1130), (1330, 1515), (2100, 2300)]
}

impl 配置 {
    /// 载入或默认：优先读 toml（路径由环境变量 分钟K线配置 指定，否则
    /// <工作区根>/配置_分钟K线_v2.toml）；文件缺失/解析失败都退回默认值，
    /// 保证“配置坏了也能先跑起来”，但会 warn 提醒。
    pub fn 载入或默认() -> Self {
        let 路径 = 配置文件路径();
        let mut 自身 = if 路径.exists() {
            match fs::read_to_string(&路径) {
                Ok(s) => match toml::from_str::<配置>(&s) {
                    Ok(c) => {
                        info!("[CFG] 已加载配置 {}", 路径.display());
                        c
                    }
                    Err(e) => {
                        warn!("[CFG] 配置解析失败({})，用默认值: {}", 路径.display(), e);
                        配置::默认()
                    }
                },
                Err(e) => {
                    warn!("[CFG] 配置读取失败({}): {}", 路径.display(), e);
                    配置::默认()
                }
            }
        } else {
            warn!("[CFG] 未见配置文件 {}，用默认值", 路径.display());
            配置::默认()
        };
        自身.校验并修正();
        自身
    }

    fn 默认() -> Self {
        // serde 的 default 只能调 Default::default；这里手工给一遍，和字段 default 对齐。
        Self {
            落盘: 落盘配置 {
                输出目录: 默认输出目录(),
                双写成交量增量: true,
                原子写: true,
                同日合并: true,
            },
            交易时段: 交易时段配置 { 时段: 默认时段() },
            以交易所时间分桶: true,
            过滤盘前重演: true,
            快照间隔秒: 0,
            统计周期秒: 默认统计周期(),
        }
    }

    /// 校验并修正：把自相矛盾/危险的值掰回安全区间，并 warn。
    fn 校验并修正(&mut self) {
        // 快照 1~4 秒太密(写放大大、没意义)，掰到 5 秒。
        if self.快照间隔秒 > 0 && self.快照间隔秒 < 5 {
            warn!("[CFG] 快照间隔 {}s 过小，掰成 5s", self.快照间隔秒);
            self.快照间隔秒 = 5;
        }
        if self.统计周期秒 == 0 {
            self.统计周期秒 = 5;
        }
        if self.落盘.输出目录.trim().is_empty() {
            self.落盘.输出目录 = 默认输出目录();
        }
    }
}

/// 配置文件路径：环境变量 分钟K线配置 优先，否则 <工作区根>/配置_分钟K线_v2.toml。
fn 配置文件路径() -> PathBuf {
    if let Ok(p) = std::env::var("分钟K线配置") {
        return PathBuf::from(p);
    }
    Path::new(WORKSPACE_ROOT).join("配置_分钟K线_v2.toml")
}

// =============================================================================
//  第 3 段：时间处理（以交易所时间为准的核心）
// -----------------------------------------------------------------------------
//  关键概念，务必分清，否则回测会错得莫名其妙：
//   • bar_time（本程序存的 Timestamp）：绝对时刻(UTC epoch 毫秒)，列里标 +00:00。
//     它回答“这一分钟在宇宙时间轴的哪一刻”，与时区无关，伦敦机/北京机算出来一样。
//   • 交易日 trading_day：CTP 柜台给的 YYYYMMDD，是“这一波行情属于哪个交易日”。
//     夜盘 21:00 起的行情，trading_day 是“下一个”自然日。这是切交易日取数的唯一
//     权威键——下游一律 WHERE trading_day='YYYYMMDD'，绝不能按文件名取。
//   • 归档日期(文件名 bars_YYYYMMDD)：用 bar_time 的 UTC 日历日。它和 trading_day
//     在夜盘会差一天，所以“某交易日完整分钟线”可能落在两个文件里(夜盘被午夜劈开)，
//     这正是必须按 trading_day 而非文件名查的原因。
// =============================================================================

/// 北京时区固定偏移 UTC+8。中国不调夏令时，用 FixedOffset 即可，不必引入 chrono-tz
/// （少一个依赖、少一类编译坑）。注意：即便你的机器在伦敦(GMT)，这里也写死 +8，
/// 因为 CTP 给的 UpdateTime 是北京时间，我们要按北京日历把它还原成绝对时刻。
fn 北京固定偏移() -> FixedOffset {
    // east_opt 理论上不会失败(8 小时在合法范围内)，unwrap 仅在“常量级必然成立”处用。
    FixedOffset::east_opt(8 * 3600).expect("UTC+8 是合法偏移")
}

/// 分钟取整：把任意绝对毫秒向下取到所在分钟的起点(秒和毫秒清零)。
/// 纯 Unix 时间戳运算，与时区无关 —— 分钟边界对任何整点偏移的时区都对齐。
fn 分钟取整(时间戳毫秒: i64) -> i64 {
    const 一分钟毫秒: i64 = 60_000;
    时间戳毫秒 - 时间戳毫秒.rem_euclid(一分钟毫秒)
}

/// 【核心】由 CTP 的 TradingDay + UpdateTime + UpdateMillisec 还原 bar 的绝对时刻。
/// 这一步完全不碰本机时钟/时区，真正“以交易所数据时间为准”。
///
/// 算法（CTP 语义）：交易日覆盖“前一自然日 21:00 ~ 当日 15:00”这一整段。
///   - 若 UpdateTime 的“时” ≥ 20（夜盘段）：墙钟日期 = 交易日 − 1 天；
///   - 否则（日盘 / 凌晨 00:00~02:30）：墙钟日期 = 交易日。
/// 再用“北京墙钟(日期 + HH:MM:SS.sss)”转 UTC 绝对毫秒。
fn 交易所时间转绝对毫秒(交易日: &str, 更新时间: &str, 更新毫秒: u32) -> Result<i64, 错误> {
    let 交易日日期 = NaiveDate::parse_from_str(交易日, "%Y%m%d")
        .map_err(|e| 错误::时间(format!("交易日解析失败[{交易日}]: {e}")))?;

    // 更新时间形如 "HH:MM:SS"，按 ':' 切三段。CTP 保证格式，但防御性校验。
    let 段: Vec<&str> = 更新时间.split(':').collect();
    if 段.len() != 3 {
        return Err(错误::时间(format!("更新时间格式异常[{更新时间}]，期望 HH:MM:SS")));
    }
    let 解析段 = |s: &str| -> Result<u32, 错误> {
        s.parse().map_err(|_| 错误::时间(format!("时间段非数字[{s}]")))
    };
    let 时 = 解析段(段[0])?;
    let 分 = 解析段(段[1])?;
    let 秒 = 解析段(段[2])?;
    if 时 > 23 || 分 > 59 || 秒 > 59 {
        return Err(错误::时间(format!("时间越界[{更新时间}]")));
    }
    let 毫秒 = 更新毫秒.min(999); // CTP 偶发给 999+ 的脏值，钳一下
    let 夜盘 = 时 >= 20;
    // 墙钟日期：夜盘往前退一天；pred_opt 对“公元元年”才返回 None，正常取不到，兜底用原值。
    let 墙钟日期 = if 夜盘 { 交易日日期.pred_opt().unwrap_or(交易日日期) } else { 交易日日期 };
    let 时刻 = NaiveTime::from_hms_milli_opt(时, 分, 秒, 毫秒)
        .ok_or_else(|| 错误::时间(format!("非法时刻 {时}:{分}:{秒}.{毫秒}")))?;
    let 本地naive = 墙钟日期.and_time(时刻); // 北京墙钟的“无时区”表达
    // 按北京时区解释成绝对时刻。from_local_datetime 返回 LocalResult(可能歧义/不存在)，
    // .single() 取其唯一解；北京无夏令时，必然唯一，取不到才报错。
    let 绝对 = 北京固定偏移()
        .from_local_datetime(&本地naive)
        .single()
        .ok_or_else(|| 错误::时间(format!("北京墙钟转绝对失败: {本地naive}")))?;
    // timestamp_millis_opt 避免用已 deprecated 的 timestamp_millis（0.4.38 起有警告）。
    Ok(绝对.timestamp_millis_opt().single().unwrap_or(0))
}

/// 绝对毫秒 → 归档日期串 YYYYMMDD（按 UTC 日历日）。用于文件名与“跨天”判定。
fn 绝对毫秒转归档日期串(绝对毫秒: i64) -> String {
    Utc.timestamp_millis_opt(绝对毫秒)
        .single()
        .map(|dt| dt.format("%Y%m%d").to_string())
        .unwrap_or_else(|| "19700101".to_string())
}

/// 当前 UTC 的归档日期串（给“文件命名/跨天巡检”用；注意不是交易日！）。
fn 当前归档日期() -> String {
    // Utc::now() 取的是绝对时刻，与时区无关；这里只关心“此刻 UTC 是几号”。
    绝对毫秒转归档日期串(Utc::now().timestamp_millis_opt().single().unwrap_or(0))
}

// —— 时间函数的单元测试（纯函数，最该测；cargo test 会跑） ——
// 这些用例把“夜盘翻日 / 凌晨归日 / 日盘”三个最容易错的边界钉死。
#[cfg(test)]
mod 时间测试 {
    use super::*;

    // 小工具：把(交易日, 时间, 毫秒)直接算成归档日期串，便于断言。
    fn 归档(交易日: &str, 时间: &str, 毫秒: u32) -> String {
        let 绝对 = 交易所时间转绝对毫秒(交易日, 时间, 毫秒).expect("解析应成功");
        绝对毫秒转归档日期串(绝对)
    }

    #[test]
    fn 夜盘21点归前一自然日() {
        // 交易日 11-24 的夜盘 21:30 = 墙钟 11-23 21:30 北京 = UTC 11-23 13:30
        assert_eq!(归档("20261124", "21:30:00", 0), "20261123");
    }
    #[test]
    fn 夜盘凌晨仍属交易日当天() {
        // 02:00 < 20 点，走“日盘分支”，墙钟日 = 交易日 11-24 02:00 北京
        // = UTC 11-23 18:00 → 归档日 11-23（UTC 日期前滚，交易日才是权威键）
        assert_eq!(归档("20261124", "02:00:00", 0), "20261123");
    }
    #[test]
    fn 日盘上午归当天() {
        // 09:00 北京 = UTC 01:00 同日
        assert_eq!(归档("20261124", "09:00:00", 0), "20261124");
    }
    #[test]
    fn 日盘10点27归当天() {
        // 对应用户实盘那条：北京 10:27 = UTC 02:27，归档 11-24
        assert_eq!(归档("20261124", "10:27:00", 0), "20261124");
    }
    #[test]
    fn 分钟取整正确() {
        assert_eq!(分钟取整(123_456_789), 123_456_000);
        assert_eq!(分钟取整(120_000_000), 120_000_000); // 整分不变
        assert_eq!(分钟取整(-1), -60_000);              // 负数也向下
    }
    #[test]
    fn 毫秒钳位不越界() {
        // 更新毫秒给脏值 1500，应钳到 999，不 panic
        let r = 交易所时间转绝对毫秒("20261124", "09:00:00", 1500);
        assert!(r.is_ok());
    }
    #[test]
    fn 非法格式报错而非panic() {
        assert!(交易所时间转绝对毫秒("2026-11-24", "9点", 0).is_err());
        assert!(交易所时间转绝对毫秒("不是日期", "09:00:00", 0).is_err());
    }
}

// =============================================================================
//  第 4 段：聚合（tick → 分钟桶 → 分钟Bar）
// -----------------------------------------------------------------------------
//  tick 是“流水”：CTP 每 0.5 秒推一批，每条进来更新对应合约的“当前分钟桶”，
//  处理完就丢，绝不囤 tick。内存里只驻留两样东西：
//    • 活跃桶：每个活跃合约一个“正在攒的这一分钟”，分钟一切换就收盘进缓存；
//    • 缓存  ：已收盘、等落盘的成品 bar（按归档日期分桶）。
//  所以内存只随“活跃合约数 × 运行分钟数”涨，跟 tick 总量无关。
// =============================================================================

/// 一根 tick 事件（从 CTP 深度行情摘出来的最小集）。
#[derive(Debug, Clone)]
pub struct Tick事件 {
    pub member下标: usize,   // 来自第几个行情连接(0~15)，用于分 member 统计
    pub 合约: String,
    pub 更新时间: String,    // CTP UpdateTime "HH:MM:SS"，交易所时间
    pub 更新毫秒: u32,       // CTP UpdateMillisec
    pub 最新价: f64,
    pub 成交量累计: u64,     // CTP Volume —— 当日累计，非单分钟增量
    pub 成交额累计: f64,     // CTP Turnover —— 当日累计
    pub 持仓量: f64,
    pub 交易日: String,      // CTP TradingDay —— 交易所权威交易日 YYYYMMDD
}

/// 喂一根 tick 给聚合器的结果（便于上层计数/诊断，不强制使用）。
#[derive(Debug)]
pub enum 喂入结果 {
    收下,
    丢弃原因(&'static str),
}

/// 一根完工的分钟 K 线。
#[derive(Debug, Clone)]
pub struct 分钟Bar {
    pub 合约: String,
    pub bar时间毫秒: i64,    // 绝对时刻(UTC epoch ms)，列里标 +00:00
    pub trading_day: String, // 交易所交易日(权威分拣键)
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64,         // 忠实 CTP 当日累计(该分钟最后一笔时的累计值)
    pub turnover: f64,       // 忠实 CTP 当日累计
    pub open_interest: f64,
    pub 成交量增量: u64,      // 本根 volume − 上一根 volume(同合约同交易日)；首根=累计本身
    pub 成交额增量: f64,      // 同上，turnover 差分
    pub is_gap: bool,        // 是否“补齐的空 bar”。本版恒 false(不造假bar)，v2.1 断线补线时置 true
}

/// 正在攒的当前分钟桶。
#[derive(Debug, Clone)]
struct 分钟桶 {
    bar时间毫秒: i64,
    trading_day: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: u64,          // 桶内取“最新 tick 的累计量”(累计值单调增，直接覆盖即可)
    turnover: f64,
    open_interest: f64,
}

impl 分钟桶 {
    fn 新建(bar起点: i64, trading_day: &str, 价: f64) -> Self {
        Self {
            bar时间毫秒: bar起点,
            trading_day: trading_day.to_string(),
            open: 价, high: 价, low: 价, close: 价,
            volume: 0, turnover: 0.0, open_interest: 0.0,
        }
    }
    /// 同分钟内更新 OHLC 与量额持仓。量额是“当日累计”，直接取最新值覆盖(桶内最大即最后)。
    fn 更新(&mut self, 价: f64, 量: u64, 额: f64, 仓: f64) {
        if 价 > self.high { self.high = 价; }
        if 价 < self.low { self.low = 价; }
        self.close = 价;
        if 量 >= self.volume { self.volume = 量; }
        if 额 >= self.turnover { self.turnover = 额; }
        if 仓 > 0.0 { self.open_interest = 仓; } // 持仓也可能瞬变 0，取最近非零
    }
    /// 收盘：算好相对上一根的差分增量，产出成品 bar。
    fn 收盘(self, 上一量: Option<(u64, f64)>) -> 分钟Bar {
        let (增量量, 增量额) = match 上一量 {
            Some((上量, 上额)) => {
                // 差分：本根累计 − 上一根累计。saturating_sub 防柜台重发小值导致下溢。
                (self.volume.saturating_sub(上量), (self.turnover - 上额).max(0.0))
            }
            // 交易日首根：没有上一根，增量=累计本身(从 0 涨到当前累计)。
            None => (self.volume, self.turnover),
        };
        分钟Bar {
            合约: String::new(), // 由调用方(收盘入缓存)填，桶本身不存合约省一份内存
            bar时间毫秒: self.bar时间毫秒,
            trading_day: self.trading_day,
            open: self.open, high: self.high, low: self.low, close: self.close,
            volume: self.volume, turnover: self.turnover, open_interest: self.open_interest,
            成交量增量: 增量量, 成交额增量: 增量额,
            is_gap: false,
        }
    }
}

/// 聚合器：单线程持有，无锁竞争(所有 tick 汇到一个聚合线程)。
pub struct 聚合器 {
    活跃桶: HashMap<String, 分钟桶>,
    缓存: HashMap<String, Vec<分钟Bar>>,        // key = 归档日期串 YYYYMMDD
    上一累计: HashMap<String, (String, u64, f64)>, // 合约 → (交易日, 上根volume, 上根turnover)
    各member计数: Vec<u64>,
    已完成根数: u64,
    丢弃计数: u64,
    上次交易所毫秒: i64,   // 以交易所时间分桶时的单调游标，用于识别“重演/陈旧”tick
    // —— 运行开关(从配置拷进来，避免聚合热路径反查配置) ——
    以交易所时间分桶: bool,
    过滤盘前重演: bool,
}

impl 聚合器 {
    fn 新建(member数: usize, 以交易所时间分桶: bool, 过滤盘前重演: bool) -> Self {
        Self {
            活跃桶: HashMap::with_capacity(20000),
            缓存: HashMap::new(),
            上一累计: HashMap::with_capacity(20000),
            各member计数: vec![0u64; member数],
            已完成根数: 0,
            丢弃计数: 0,
            上次交易所毫秒: 0,
            以交易所时间分桶,
            过滤盘前重演,
        }
    }

    /// 喂一根 tick。返回收下 / 丢弃原因。
    pub fn 喂一个tick(&mut self, t: &Tick事件) -> 喂入结果 {
        // (1) 无效价过滤：<=0 或异常大(NaN 比较会走 else 分支也被拦)。NaN 用总比较兜。
        if !(t.最新价 > 0.0) || t.最新价 >= 1e30 {
            self.丢弃计数 += 1;
            return 喂入结果::丢弃原因("无效价");
        }
        if t.member下标 < self.各member计数.len() {
            self.各member计数[t.member下标] += 1;
        }

        // (2) 定 bar 的绝对分钟起点：交易所时间 or 本机收包时间(由开关决定)。
        let bar起点 = if self.以交易所时间分桶 {
            match 交易所时间转绝对毫秒(&t.交易日, &t.更新时间, t.更新毫秒) {
                Ok(绝对) => {
                    // 单调性闸门：识破“盘前重演/陈旧流水”。若本 tick 时间比上次接受的
                    // 还早 1 分钟以上，判为重演，丢。正常盘中时间单调不减，不受影响。
                    if 绝对 + 60_000 < self.上次交易所毫秒 {
                        self.丢弃计数 += 1;
                        return 喂入结果::丢弃原因("重演/陈旧");
                    }
                    if 绝对 > self.上次交易所毫秒 { self.上次交易所毫秒 = 绝对; }
                    分钟取整(绝对)
                }
                Err(_) => {
                    self.丢弃计数 += 1;
                    return 喂入结果::丢弃原因("时间解析失败");
                }
            }
        } else {
            // 退回 v1.1 行为：用“收包本机时间”取整。Local 知道本机时区，
            // timestamp_millis_opt 给出绝对 epoch ms，与 v1.1 等价。
            let 现在 = chrono::Local::now().timestamp_millis_opt().single().unwrap_or(0);
            分钟取整(现在)
        };

        // (3) 盘前重演过滤：量为 0 的 bar(盘前重演/结算行情常见)丢弃，防污染。
        //     注意：极冷门真·零成交分钟也会被丢，但量=0 本就产不出有效K线，无影响。
        if self.过滤盘前重演 && t.成交量累计 == 0 {
            self.丢弃计数 += 1;
            return 喂入结果::丢弃原因("量0(疑似重演)");
        }

        // (4) 桶的“首现/切换/同分钟”三态处理。
        //     用 Entry API 一次查表完成，避免多次 hash 查找。
        use std::collections::hash_map::Entry;
        match self.活跃桶.entry(t.合约.clone()) {
            Entry::Vacant(空位) => {
                // 首现：建新桶，OHLC 全 = 最新价。
                let mut 桶 = 分钟桶::新建(bar起点, &t.交易日, t.最新价);
                桶.更新(t.最新价, t.成交量累计, t.成交额累计, t.持仓量);
                空位.insert(桶);
            }
            Entry::Occupied(mut 占) => {
                if 占.get().bar时间毫秒 == bar起点 {
                    // 同一分钟：就地更新桶。
                    占.get_mut().更新(t.最新价, t.成交量累计, t.成交额累计, t.持仓量);
                } else {
                    // 分钟切换：先把旧桶收盘进缓存，再建新桶。
                    // ★ 这里踩过 v1.1 骨架的 E0382 坑：OccupiedEntry::remove(self)
                    //   会“消费”占，之后不能再 占.insert()。正确写法：先 remove 拿走旧桶
                    //   (占 到此失效)，再用 self.活跃桶.insert 重新插新桶，两步分开。
                    let 旧桶 = 占.remove();       // 消费 占，对 活跃桶 的可变借用到此结束
                    let 合约 = t.合约.clone();
                    self.收盘入缓存(合约.clone(), 旧桶);
                    let mut 新桶 = 分钟桶::新建(bar起点, &t.交易日, t.最新价);
                    新桶.更新(t.最新价, t.成交量累计, t.成交额累计, t.持仓量);
                    self.活跃桶.insert(合约, 新桶);
                }
            }
        }
        喂入结果::收下
    }

    /// 把收盘的桶算成成品 bar 塞进缓存，并记 上一累计(供下一根差分)。
    fn 收盘入缓存(&mut self, 合约: String, 桶: 分钟桶) {
        let 交易日日期 = 桶.trading_day.clone();
        let 上一 = self.上一累计.get(&合约).and_then(|(上日, 上量, 上额)| {
            if 上日 == &交易日日期 { Some((*上量, *上额)) } else { None }
        });
        let mut bar = 桶.收盘(上一);
        bar.合约 = 合约.clone();
        // 更新“上一累计”为本次收盘值(同交易日才有效，跨日已在上面置 None，这里重置)
        self.上一累计.insert(合约.clone(), (交易日日期.clone(), bar.volume, bar.turnover));
        let 日期串 = 绝对毫秒转归档日期串(bar.bar时间毫秒);
        self.缓存.entry(日期串).or_insert_with(Vec::new).push(bar);
        self.已完成根数 += 1;
    }

    /// 收尾所有活跃桶(优雅退出/收盘时调用)：把“正在攒的这一分钟”全部收盘进缓存，
    /// 让当天数据一根不落地进缓存，随后由落盘统一写盘。返回收盘根数。
    pub fn 收尾所有活跃桶(&mut self) -> u64 {
        let 键: Vec<String> = self.活跃桶.keys().cloned().collect();
        let n = 键.len() as u64;
        for 合约 in 键 {
            if let Some(桶) = self.活跃桶.remove(&合约) {
                self.收盘入缓存(合约, 桶);
            }
        }
        n
    }

    /// 取某归档日期的全部 bar(快照/落盘用，返回引用，避免拷贝)。
    pub fn 取日期bars(&self, 日期: &str) -> Option<&[分钟Bar]> {
        self.缓存.get(日期).map(|v| v.as_slice())
    }
    /// 所有归档日期(落盘/快照遍历用)。
    pub fn 所有日期(&self) -> Vec<String> {
        let mut v: Vec<String> = self.缓存.keys().cloned().collect();
        v.sort();
        v
    }
    /// 全量 bar(快照写“当天全量”用)。
    pub fn 全量bars(&self) -> Vec<分钟Bar> {
        let mut v: Vec<分钟Bar> = self.缓存.values().flat_map(|x| x.iter().cloned()).collect();
        v.sort_by(|a, b| a.合约.cmp(&b.合约).then(a.bar时间毫秒.cmp(&b.bar时间毫秒)));
        v
    }
    /// 某归档日期根数(状态日志用)。
    pub fn 日期根数(&self, 日期: &str) -> usize {
        self.缓存.get(日期).map(|v| v.len()).unwrap_or(0)
    }
    pub fn done计数(&self) -> u64 { self.已完成根数 }
    pub fn 丢弃计数(&self) -> u64 { self.丢弃计数 }
    pub fn 活跃桶数(&self) -> usize { self.活跃桶.len() }
    /// 各 member tick 分布(排障用：哪个 member 长期 0 就是那条前置/IP 有问题)。
    pub fn 各member分布(&self) -> Vec<(usize, u64)> {
        self.各member计数.iter().enumerate().map(|(i, &c)| (i, c)).collect()
    }
}

// =============================================================================
//  第 5 段：Parquet 落盘（13 列 schema + 原子写 + 同日合并）
// -----------------------------------------------------------------------------
//  列顺序(下游按此读)：bar_time, instrument_id, trading_day,
//    open, high, low, close, volume, turnover, open_interest,
//    volume_1m, turnover_1m, is_gap
//  volume/turnover 忠实 CTP 当日累计；volume_1m/turnover_1m 是差分增量(开箱即用)。
// =============================================================================

/// 13 列 schema。与 v1.1 风格一致；bar_time 用 Timestamp(Millisecond, +00:00)。
fn 表结构() -> Schema {
    Schema::new(vec![
        Field::new("bar_time", DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())), false),
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("trading_day", DataType::Utf8, false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::UInt64, false),
        Field::new("turnover", DataType::Float64, false),
        Field::new("open_interest", DataType::Float64, false),
        Field::new("volume_1m", DataType::UInt64, false),
        Field::new("turnover_1m", DataType::Float64, false),
        Field::new("is_gap", DataType::Boolean, false),
    ])
}

/// 落盘参数(从 配置.落盘 拷出，落盘器持有)。
#[derive(Debug, Clone)]
pub struct 落盘选项 {
    pub 输出目录: String,
    pub 双写增量: bool,
    pub 原子写: bool,
    pub 同日合并: bool,
}

/// 落盘器：无状态(只持选项)，clone 很便宜，快照线程/主线程各拿一份。
#[derive(Debug, Clone)]
pub struct 落盘器 { 选项: 落盘选项 }

impl 落盘器 {
    pub fn 新建(选项: 落盘选项) -> Self { Self { 选项 } }

    /// 目标文件路径：<输出目录>/bars_<归档日期>.parquet (相对路径挂工作区根)。
    fn 目标路径(&self, 归档日期: &str) -> Result<PathBuf, 错误> {
        let 目录 = if Path::new(&self.选项.输出目录).is_absolute() {
            PathBuf::from(&self.选项.输出目录)
        } else {
            Path::new(WORKSPACE_ROOT).join(&self.选项.输出目录)
        };
        fs::create_dir_all(&目录)?; // 幂等，目录已存在不报错
        Ok(目录.join(format!("bars_{归档日期}.parquet")))
    }

    /// 落“某归档日期”的全部 bar：可选同日合并 → 原子写。返回写入根数。
    pub fn 落当天(&self, 归档日期: &str, bars: &[分钟Bar]) -> Result<u64, 错误> {
        if bars.is_empty() {
            info!("[WRT] {归档日期} 无 bar，跳过");
            return Ok(0);
        }
        let 目标 = self.目标路径(归档日期)?;
        // 同日合并：目标已存在 → 读旧 bar 拼一起，按 (合约,bar时间) 去重。
        let 最终 = if self.选项.同日合并 && 目标.exists() {
            match self.读文件(&目标) {
                Ok(旧) => {
                    let 合并后 = 合并去重(旧, bars.to_vec());
                    info!("[WRT] 同日合并：旧 {} 根 + 新 {} 根 → 去重后 {} 根",
                          旧.len(), bars.len(), 合并后.len());
                    合并后
                }
                Err(e) => {
                    // 旧文件读不动(损坏/版本不合)不阻断，退化为“备份后覆盖”。
                    warn!("[WRT] 读旧文件失败({})，备份后覆盖: {e}", 目标.display());
                    let 备份 = 目标.with_extension("parquet.bak");
                    let _ = fs::rename(&目标, &备份);
                    bars.to_vec()
                }
            }
        } else if 目标.exists() && !self.选项.同日合并 {
            let 备份 = 目标.with_extension("parquet.bak");
            let _ = fs::rename(&目标, &备份);
            bars.to_vec()
        } else {
            bars.to_vec()
        };
        self.写文件原子(&目标, &最终)?;
        info!("[WRT] 写入 {} 共 {} 根", 目标.display(), 最终.len());
        Ok(最终.len() as u64)
    }

    /// 原子写：写 <目标>.tmp → flush → fsync → rename 覆盖目标。
    /// rename 在同一文件系统内是原子操作，进程被杀最多留个 .tmp，绝不留半截目标文件。
    fn 写文件原子(&self, 目标: &Path, bars: &[分钟Bar]) -> Result<(), 错误> {
        let 批次 = 造批次(bars, self.选项.双写增量)?;
        let schema = 表结构();
        let 临时 = 目标.with_extension("parquet.tmp");
        {
            let 文件 = fs::File::create(&临时)?;
            let 属性 = WriterProperties::builder()
                .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
                .build();
            let mut 写入器 = ArrowWriter::try_new(文件, Arc::new(schema), Some(属性))?;
            写入器.write(&批次)?;
            写入器.close()?; // close 会 flush 完 footer，之后再 fsync
        }
        // fsync：确保数据真落到磁盘介质，而非只在 OS page cache(断电才不丢)。
        if self.选项.原子写 {
            let f = fs::File::open(&临时)?;
            f.sync_all().ok(); // 失败不致命(有些 FS 不支持)，退化为 rename
        }
        fs::rename(&临时, 目标)?;
        Ok(())
    }

    /// 读一个 parquet 文件回 Vec<分钟Bar>(同日合并/快照恢复用)。
    fn 读文件(&self, 路径: &Path) -> Result<Vec<分钟Bar>, 错误> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let 文件 = fs::File::open(路径)?;
        let 构建器 = ParquetRecordBatchReaderBuilder::try_new(文件)?;
        let 读器 = 构建器.build()?;
        let mut 出 = Vec::new();
        for 批 in 读器 {
            let 批 = 批?;
            出.extend(批次转bars(&批)?);
        }
        Ok(出)
    }

    /// 写快照：文件名固定 bars_快照_<日期>.parquet(供重启恢复)。与归档同格式。
    pub fn 写快照(&self, 归档日期: &str, bars: &[分钟Bar]) -> Result<u64, 错误> {
        let 目录 = if Path::new(&self.选项.输出目录).is_absolute() {
            PathBuf::from(&self.选项.输出目录)
        } else {
            Path::new(WORKSPACE_ROOT).join(&self.选项.输出目录)
        };
        fs::create_dir_all(&目录)?;
        let 目标 = 目录.join(format!("bars_快照_{归档日期}.parquet"));
        self.写文件原子(&目标, bars)?;
        Ok(bars.len() as u64)
    }
    pub fn 读快照(&self, 归档日期: &str) -> Result<Vec<分钟Bar>, 错误> {
        let 目录 = if Path::new(&self.选项.输出目录).is_absolute() {
            PathBuf::from(&self.选项.输出目录)
        } else {
            Path::new(WORKSPACE_ROOT).join(&self.选项.输出目录)
        };
        self.读文件(&目录.join(format!("bars_快照_{归档日期}.parquet")))
    }
}

/// 把 bar 切片做成 RecordBatch(Arrow 列存)。双写增量列可选。
fn 造批次(bars: &[分钟Bar], 双写增量: bool) -> Result<RecordBatch, 错误> {
    let n = bars.len();
    let mut bt = Vec::with_capacity(n);
    let mut inst = Vec::with_capacity(n);
    let mut td = Vec::with_capacity(n);
    let mut o = Vec::with_capacity(n);
    let mut h = Vec::with_capacity(n);
    let mut l = Vec::with_capacity(n);
    let mut c = Vec::with_capacity(n);
    let mut vol = Vec::with_capacity(n);
    let mut tur = Vec::with_capacity(n);
    let mut oi = Vec::with_capacity(n);
    let mut vol1 = Vec::with_capacity(n);
    let mut tur1 = Vec::with_capacity(n);
    let mut gap = Vec::with_capacity(n);
    for b in bars {
        bt.push(b.bar时间毫秒);
        inst.push(b.合约.as_str());
        td.push(b.trading_day.as_str());
        o.push(b.open); h.push(b.high); l.push(b.low); c.push(b.close);
        vol.push(b.volume); tur.push(b.turnover); oi.push(b.open_interest);
        vol1.push(b.成交量增量); tur1.push(b.成交额增量);
        gap.push(b.is_gap);
    }
    let bt_arr: ArrayRef = Arc::new(TimestampMillisecondArray::from(bt)
        .with_timezone("+00:00"));
    let inst_arr: ArrayRef = Arc::new(StringArray::from(inst));
    let td_arr: ArrayRef = Arc::new(StringArray::from(td));
    let o_arr: ArrayRef = Arc::new(Float64Array::from(o));
    let h_arr: ArrayRef = Arc::new(Float64Array::from(h));
    let l_arr: ArrayRef = Arc::new(Float64Array::from(l));
    let c_arr: ArrayRef = Arc::new(Float64Array::from(c));
    let vol_arr: ArrayRef = Arc::new(UInt64Array::from(vol));
    let tur_arr: ArrayRef = Arc::new(Float64Array::from(tur));
    let oi_arr: ArrayRef = Arc::new(Float64Array::from(oi));
    let vol1_arr: ArrayRef = Arc::new(UInt64Array::from(vol1));
    let tur1_arr: ArrayRef = Arc::new(Float64Array::from(tur1));
    let gap_arr: ArrayRef = Arc::new(BooleanArray::from(gap));

    let schema = 表结构();
    let cols: Vec<ArrayRef> = if 双写增量 {
        vec![bt_arr, inst_arr, td_arr, o_arr, h_arr, l_arr, c_arr,
             vol_arr, tur_arr, oi_arr, vol1_arr, tur1_arr, gap_arr]
    } else {
        // 不双写时也要给这两列(否则列数≠schema)，填 0 占位，保持 schema 稳定。
        let z_v: ArrayRef = Arc::new(UInt64Array::from(vec![0u64; n]));
        let z_t: ArrayRef = Arc::new(Float64Array::from(vec![0.0f64; n]));
        vec![bt_arr, inst_arr, td_arr, o_arr, h_arr, l_arr, c_arr,
             vol_arr, tur_arr, oi_arr, z_v, z_t, gap_arr]
    };
    RecordBatch::try_new(Arc::new(schema), cols).map_err(|e| 错误::Arrow(e))
}

/// RecordBatch → Vec<分钟Bar>(读文件用)。列顺序与 造批次 严格一致。
fn 批次转bars(批: &RecordBatch) -> Result<Vec<分钟Bar>, 错误> {
    // 列顺序必须与 造批次() 完全一致；每列都用 as_any().downcast_ref 取出强类型数组。
    use arrow::array::{UInt64Array as U64A, Float64Array as F64A,
                       StringArray as SA, BooleanArray as BA,
                       TimestampMillisecondArray as TSA};
    let bt = 批.column(0).as_any().downcast_ref::<TSA>().ok_or_else(|| 错误::落盘("bar_time 列错 列错".into()))?;
    let inst = 批.column(1).as_any().downcast_ref::<SA>().ok_or_else(|| 错误::落盘("instrument_id 列错 列错".into()))?;
    let td = 批.column(2).as_any().downcast_ref::<SA>().ok_or_else(|| 错误::落盘("trading_day 列错 列错".into()))?;
    let o = 批.column(3).as_any().downcast_ref::<...>().ok_or_else(|| 错误::落盘("open 列错".into()))?;
    let h = 批.column(4).as_any().downcast_ref::<...>().ok_or_else(|| 错误::落盘("high 列错".into()))?;
    let l = 批.column(5).as_any().downcast_ref::<...>().ok_or_else(|| 错误::落盘("low 列错".into()))?;
    let cc = 批.column(6).as_any().downcast_ref::<...>().ok_or_else(|| 错误::落盘("close 列错".into()))?;
    let vol = 批.column(7).as_any().downcast_ref::<...>().ok_or_else(|| 错误::落盘("volume 列错".into()))?;
    let tur = 批.column(8).as_any().downcast_ref::<...>().ok_or_else(|| 错误::落盘("turnover 列错".into()))?;
    let oi = 批.column(9).as_any().downcast_ref::<...>().ok_or_else(|| 错误::落盘("open_interest 列错".into()))?;
    let vol1 = 批.column(10).as_any().downcast_ref::<...>().ok_or_else(|| 错误::落盘("volume_1m 列错".into()))?;
    let tur1 = 批.column(11).as_any().downcast_ref::<...>().ok_or_else(|| 错误::落盘("turnover_1m 列错".into()))?;
    let gap = 批.column(12).as_any().downcast_ref::<...>().ok_or_else(|| 错误::落盘("is_gap 列错".into()))?;
    // 时区标记：Arrow 的 timestamp 带 timezone，values() 给的是绝对 ms 的 i64，直接取。
    let mut 出 = Vec::with_capacity(bt.len());
    for i in 0..bt.len() {
        出.push(分钟Bar {
            合约: inst.value(i).to_string(),
            bar时间毫秒: bt.value(i),   // i64 绝对 ms
            trading_day: td.value(i).to_string(),
            open: o.value(i), high: h.value(i), low: l.value(i), close: cc.value(i),
            volume: vol.value(i), turnover: tur.value(i), open_interest: oi.value(i),
            成交量增量: vol1.value(i), 成交额增量: tur1.value(i),
            is_gap: gap.value(i),
        });
    }
    Ok(出)
}

/// 合并去重：以 (合约, bar时间毫秒) 为主键；同键时“非 gap 优先”，都同 gap 态取 volume 更大者。
/// 用于“同日夜盘段+日盘段”合并、“快照+新增”合并。
fn 合并去重(旧: Vec<分钟Bar>, 新: &[分钟Bar]) -> Vec<分钟Bar> {
    let mut 表: HashMap<(String, i64), 分钟Bar> = HashMap::with_capacity(旧.len() + 新.len());
    for b in 旧.into_iter().chain(新.iter().cloned()) {
        let 键 = (b.合约.clone(), b.bar时间毫秒);
        表.entry(键)
            .and_modify(|存在| {
                // 选“更好”的那根：非 gap 胜 gap；同态取累计量更大(信息更全)者。
                let 新更好 = (!b.is_gap && 存在.is_gap)
                    || (b.is_gap == 存在.is_gap && b.volume >= 存在.volume);
                if 新更好 { *存在 = b.clone(); }
            })
            .or_insert(b);
    }
    let mut 出: Vec<分钟Bar> = 表.into_values().collect();
    // 稳定排序：先按合约、再按时间，保证落盘顺序确定(便于 diff/对账)。
    出.sort_by(|a, b| a.合约.cmp(&b.合约).then(a.bar时间毫秒.cmp(&b.bar时间毫秒)));
    出
}

#[cfg(test)]
mod 落盘测试 {
    use super::*;

    fn 造bar(合约: &str, 时间: i64, 日: &str, 量累计: u64, 量增量: u64, gap: bool) -> 分钟Bar {
        分钟Bar {
            合约: 合约.to_string(), bar时间毫秒: 时间, trading_day: 日.to_string(),
            open: 1.0, high: 2.0, low: 0.5, close: 1.5,
            volume: 量累计, turnover: 量累计 as f64 * 10.0, open_interest: 100.0,
            成交量增量: 量增量, 成交额增量: 量增量 as f64 * 10.0, is_gap: gap,
        }
    }

    #[test]
    fn 合并去重_同键取新且非gap优先() {
        let 旧 = vec![造bar("IF2401", 1000, "20261124", 100, 100, false),
                     造bar("IF2401", 2000, "20261124", 200, 100, false)];
        // 新：1000 这根被“补成 gap 版”，应被旧的真实 bar 压过；2000 量更大应胜出；3000 新增。
        let 新 = vec![造bar("IF2401", 1000, "20261124", 100, 0, true),
                     造bar("IF2401", 2000, "20261124", 250, 50, false),
                     造bar("IF2401", 3000, "20261124", 300, 50, false)];
        let 合 = 合并去重(旧, &新);
        assert_eq!(合.len(), 3, "应去重成 3 根");
        let m: HashMap<i64, &分钟Bar> = 合.iter().map(|b| (b.bar时间毫秒, b)).collect();
        assert!(!m[&1000].is_gap, "真实 bar 应压过 gap 补齐 bar");
        assert_eq!(m[&2000].volume, 250, "同键同态取累计量更大者");
        assert!(m.contains_key(&3000));
    }

    #[test]
    fn 聚合_差分增量首根等于累计后续做差() {
        let mut 聚 = 聚合器::新建(1, true, true);
        let t1 = Tick事件 { member下标: 0, 合约: "rb2401".into(), 更新时间: "21:01:00".into(),
            更新毫秒: 0, 最新价: 3600.0, 成交量累计: 100, 成交额累计: 1000.0, 持仓量: 10000.0,
            交易日: "20261124".into() };
        assert!(matches!(聚.喂一个tick(&t1), 喂入结果::收下));
        // 同一分钟内再来一笔，量涨到 160
        let mut t2 = t1.clone(); t2.成交量累计 = 160; t2.成交额累计 = 1600.0; t2.最新价 = 3602.0;
        聚.喂一个tick(&t2);
        // 切到下一分钟，量 200
        let mut t3 = t2.clone(); t3.更新时间 = "21:02:00".into(); t3.成交量累计 = 200; t3.成交额累计 = 2000.0;
        聚.喂一个tick(&t3);
        聚.收尾所有活跃桶();
        let 全部 = 聚.全量bars();
        assert_eq!(全部.len(), 2, "应收盘 2 根");
        assert_eq!(全部[0].volume, 160, "首根累计量=该分钟末值");
        assert_eq!(全部[0].成交量增量, 160, "首根增量=累计本身");
        assert_eq!(全部[1].volume, 200);
        assert_eq!(全部[1].成交量增量, 40, "第二根增量=200-160");
    }

    #[test]
    fn 聚合_无效价与量0被丢() {
        let mut 聚 = 聚合器::新建(1, true, true);
        let 坏 = Tick事件 { member下标: 0, 合约: "x".into(), 更新时间: "21:01:00".into(),
            更新毫秒: 0, 最新价: 0.0, 成交量累计: 10, 成交额累计: 1.0, 持仓量: 0.0,
            交易日: "20261124".into() };
        assert!(matches!(聚.喂一个tick(&坏), 喂入结果::丢弃原因(_)));
        let 量零 = Tick事件 { 最新价: 10.0, 成交量累计: 0, ..坏.clone() };
        assert!(matches!(聚.喂一个tick(&量零), 喂入结果::丢弃原因(_)));
        assert_eq!(聚.丢弃计数(), 2);
    }
}

// =============================================================================
//  第 6 段：快照（可关；快照间隔秒=0 时整段不启动）
// -----------------------------------------------------------------------------
//  快照和恢复是“同一套机制的两面”，别做两遍：
//    · 启动：若当天快照文件在 → 读进缓存(崩溃恢复)；
//    · 运行：每 N 秒把“当天全量”原子写回快照；
//    · 退出/跨天：正式落归档名，并删/留快照都行(同日合并会兜住)。
//  快照间隔秒=0（默认笔记本模式）时：不启线程、不读快照、只在 Ctrl+C 落归档。
// =============================================================================

/// 启动时尝试恢复当天快照。返回恢复根数(0 表示无快照或快照关闭)。
pub fn 恢复当天快照(聚合: &mut 聚合器, 落盘器: &落盘器, 快照间隔秒: u64, 归档日期: &str) -> u64 {
    if 快照间隔秒 == 0 { return 0; } // 快照关闭，不恢复
    match 落盘器.读快照(归档日期) {
        Ok(bars) if !bars.is_empty() => {
            let n = bars.len() as u64;
            // 灌回聚合器的缓存 + 活跃桶的“上一累计”，让它接着攒不产生错误增量。
            for b in bars {
                聚合.缓存.entry(归档日期.to_string()).or_default().push(b.clone());
                // 用每根 bar 的累计量刷新“上一累计”，只保留最后一根（同合约）。
                聚合.上一累计.insert(b.合约.clone(),
                    (b.trading_day.clone(), b.volume, b.turnover));
            }
            聚合.已完成根数 = 聚合.缓存.values().map(|v| v.len()).sum::<usize>() as u64;
            info!("[SNAP] 恢复当天快照 {n} 根（{归档日期}），接着攒");
            n
        }
        Ok(_) => 0,
        Err(e) => {
            warn!("[SNAP] 读快照失败(忽略，从空开始): {e}");
            0
        }
    }
}

/// 起快照线程。快照间隔秒=0 时返回 None(不起线程 = 纯内存模式)。
/// 线程里“只在短临界区内拷 bars，写文件在锁外”，避免长时间占锁卡住聚合线程。
pub fn 起快照线程(
    快照间隔秒: u64,
    聚合: Arc<Mutex<聚合器>>,
    落盘器: 落盘器,
    退出: Arc<AtomicBool>,
    归档日期取法: Arc<dyn Fn() -> String + Send + Sync>,
) -> Option<JoinHandle<Result<u64, 错误>>> {
    if 快照间隔秒 == 0 {
        info!("[SNAP] 快照间隔=0，纯内存模式(仅 Ctrl+C 落盘)，不启快照线程");
        return None;
    }
    let 句柄 = thread::spawn(move || {
        let mut 累计写 = 0u64;
        while !退出.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(快照间隔秒));
            if 退出.load(Ordering::Relaxed) { break; }
            let 日期 = 归档日期取法();
            // 短临界区：只拷“当天 bars”，立刻放锁。
            let bars = {
                let 聚 = 加锁(&聚合, "快照取bars")?;
                聚.取日期bars(&日期).unwrap_or(&[]).to_vec()
            };
            if bars.is_empty() { continue; }
            match 落盘器.写快照(&日期, &bars) {
                Ok(n) => { 累计写 += n; info!("[SNAP] 快照 {日期} 写 {n} 根(累计 {累计写})"); }
                Err(e) => warn!("[SNAP] 快照写失败(下轮重试): {e}"),
            }
        }
        info!("[SNAP] 快照线程退出 | 累计写 {累计写} 根");
        Ok(累计写)
    });
    Some(句柄)
}

// =============================================================================
//  第 7 段：行情接入（ctp2rs MdApi，照抄 v1.1 验证过的调用方式）
// -----------------------------------------------------------------------------
//  架构(v1.1 验证过、最简)：每个 member 一个线程，线程内建 MdApi、注册 SPI、
//  连前置、登录、分批订阅，然后 api.join() 阻塞到“断线”；join 返回即退避重连，
//  永不退出线程。SPI 的 on_rtn_depth_market_data 直接把 tick 塞进共享通道，
//  不再搞 MemberEvent/listener 那一层(v2 骨架里那层 Receiver::clone 根本编不过，
//  std 的 mpsc::Receiver 不是 Clone)。
// =============================================================================

// —— 常量(真正的“魔法数”集中在此，改这里即可) ——
const MD_DYNLIB_REL: &str = "lib/libthostmduserapi_se.so"; // 行情动态库(相对工作区根)
const SUBSCRIBE_BATCH: usize = 500;     // 每批订阅合约数(CTP 单帧有上限，分批规避)
const SUBSCRIBE_DELAY_MS: u64 = 300;    // 批间间隔(ms)，别把柜台打急了
const CHANNEL容量: usize = 1_000_000;   // tick 通道容量；满则 try_send 失败→计丢包
const 重连起始秒: u64 = 1;
const 重连封顶秒: u64 = 60;
const RECONNECT_RESET_MS: i64 = 0;      // (保留占位；交易日已改用每笔 q.TradingDay)

/// 合约清单里的一行：合约代码 + 交易所。
#[derive(Debug, Clone)]
pub struct 合约 { pub code: String, pub exchange: String }

/// 载入合约清单 CSV：每行 "合约代码,交易所"。空行/注释行(#开头)跳过。
pub fn 载入合约清单(路径: &Path) -> Result<Vec<合约>, 错误> {
    let s = fs::read_to_string(路径)
        .map_err(|e| 错误::合约清单(format!("读 {} 失败: {e}", 路径.display())))?;
    let mut 出 = Vec::new();
    for (行号, 行) in s.lines().enumerate() {
        let 行 = 行.trim();
        if 行.is_empty() || 行.starts_with('#') { continue; }
        let 段: Vec<&str> = 行.split(',').collect();
        if 段.len() < 2 {
            warn!("[CFG] 合约清单第 {} 行格式跳过: {行}", 行号 + 1);
            continue;
        }
        出.push(合约 { code: 段[0].trim().to_string(), exchange: 段[1].trim().to_string() });
    }
    if 出.is_empty() {
        return Err(错误::合约清单(format!("{} 无有效合约", 路径.display())));
    }
    Ok(出)
}

/// 分片：先按交易所分组，组内轮询 i%N 分给第 i 个 member，让各 member 各交易所合约均衡。
pub fn 分片合约(合约s: &[合约], 成员数: usize) -> Vec<Vec<String>> {
    assert!(成员数 > 0, "成员数必须>0"); // 这里 assert 可接受：调用方由 member_ips 长度保证，属于“程序不变量”
    let mut 组: HashMap<String, Vec<String>> = HashMap::new();
    for c in 合约s {
        组.entry(c.exchange.clone()).or_default().push(c.code.clone());
    }
    let mut 分片 = vec![Vec::<String>::new(); 成员数];
    for (_, 列) in 组.iter() {
        for (i, code) in 列.iter().enumerate() {
            分片[i % 成员数].push(code.clone());
        }
    }
    分片
}

/// 动态库路径：<工作区根>/lib/libthostmduserapi_se.so。
pub fn 动态库路径() -> PathBuf {
    // 【经验】动态库在 crate 目录的 lib/ 下(rust_ctp_test/lib/...)，不在工作区根。
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(MD_DYNLIB_REL)
}

// —— 下面进入 ctp2rs FFI 层 ——
// 说明：以下 use 的模块路径/类型名照抄 v1.1(已编译运行验证)。若你机器上的 ctp2rs
// 版本对不上(报错 E0432/E0433 use of unresolved)，把报错整段发我，按 ctp2rs 0.1.9
// 的实际路径微调即可，业务逻辑一行不用动。
// 【经验】ctp2rs 0.1.9 的正确路径是 `ctp2rs::v1alpha1::*`（不是 api::md / api::structs，
// 那是早期猜错、导致 E0433 的写法）。v1.1 已编译运行验证：
//   use ctp2rs::ffi::{AssignFromString, WrapToString};  // 两个扩展 trait，必须 use 才能调方法
//   use ctp2rs::v1alpha1::*;                            // MdApi / MdSpi / 所有 CThostFtdc* 结构
// AssignFromString → 给 CTP 定长字符数组赋值(.assign_from_str)
// WrapToString     → CTP 字符数组转 Rust String(.to_string())，否则拿不到 InstrumentID
use ctp2rs::ffi::{AssignFromString, WrapToString};
use ctp2rs::v1alpha1::*;
use ctp2rs::v1alpha1::{
    CThostFtdcDepthMarketDataField as 深度行情,
    CThostFtdcReqUserLoginField as 登录请求,
    CThostFtdcRspUserLoginField as 登录响应,
    CThostFtdcRspInfoField as 应答信息,
};

/// CTP 账号/连接信息(从 Secrets 拷出，避免 SPI 持有整个 Secrets)。
#[derive(Debug, Clone)]
pub struct 连接信息 {
    pub broker_id: String,
    pub user_id: String,
    pub password: String,
}

/// SPI 回调实现：把 CTP 回调翻译成 tick 塞进通道。
struct 行情Spi {
    member下标: usize,
    tick_tx: mpsc::SyncSender<Tick事件>,
    连接: 连接信息,
    丢弃计数: Arc<AtomicU64>,   // 通道满时累加，周期日志读
}

impl MdSpi for 行情Spi {
    fn on_front_connected(&mut self) {
        info!("[MD#{}] 前置已连接", self.member下标);
    }
    fn on_front_disconnected(&mut self, 原因: i32) {
        // 这里只记录；真正的重连由“跑一轮member 的 join 返回 → 退避重来”驱动。
        warn!("[MD#{}] 前置断开(reason={})，将由重连循环重建", self.member下标, 原因);
    }
    // 【关键】trait 里 rsp/rsp_info/specific 都是 Option<&T>(CTP 有时不回包)，
    // 签名写成 &T(非 Option) 会 E0053“方法签名不符”。照抄 v1.1：参数收 Option，内部 let-else 拆。
    fn on_rsp_user_login(
        &mut self,
        _rsp: Option<&登录响应>,
        信息: Option<&应答信息>,
        请求id: i32,
        是否最后: bool,
    ) {
        let Some(信) = 信息 else { return; };
        if 信.ErrorID == 0 {
            info!("[MD#{}] 登录成功 (req={}, last={})", self.member下标, 请求id, 是否最后);
        } else {
            warn!("[MD#{}] 登录失败 ErrorID={} Msg={}", self.member下标, 信.ErrorID, 信.ErrorMsg.to_string());
        }
    }
    // 订阅回执：第一个参数是“具体合约”结构(本程序用不上)，第二个才是 rsp_info。
    fn on_rsp_sub_market_data(
        &mut self,
        _specific: Option<&CThostFtdcSpecificInstrumentField>,
        信息: Option<&应答信息>,
        请求id: i32,
        是否最后: bool,
    ) {
        if let Some(信) = 信息 {
            if 信.ErrorID != 0 {
                warn!("[MD#{}] 订阅回执 ErrorID={} Msg={} (req={}, last={})",
                      self.member下标, 信.ErrorID, 信.ErrorMsg.to_string(), 请求id, 是否最后);
            }
        }
        // 成功不逐条刷(3 万合约会刷屏)；总数在“订阅完成”那句统一报。
    }
    fn on_rtn_depth_market_data(&mut self, q: Option<&深度行情>) {
        let Some(q) = q else { return; };
        // 【字段类型】Volume 是 CTP int(i32)→需 `as u64`；OpenInterest 在 v1.1 实证是 f64，
        // 直接赋(再写 `as f64` 会被 clippy 判“无用转换”，-D warnings 下报错)。
        let t = Tick事件 {
            member下标: self.member下标,
            合约: q.InstrumentID.to_string(),
            更新时间: q.UpdateTime.to_string(),      // "HH:MM:SS"，交易所时间
            更新毫秒: q.UpdateMillisec as u32,
            最新价: q.LastPrice,
            成交量累计: q.Volume as u64,
            成交额累计: q.Turnover,
            持仓量: q.OpenInterest,
            交易日: q.TradingDay.to_string(),        // 交易所权威交易日
        };
        // try_send：通道满就丢、计数，绝不阻塞行情线程(阻塞会拖垮整个 member)。
        if let Err(e) = self.tick_tx.try_send(t) {
            let 已丢 = self.丢弃计数.fetch_add(1, Ordering::Relaxed) + 1;
            // 不用 is_multiple_of(整数方法 1.87 才稳定，老工具链会 E0599)；% 恒可用
            if 已丢 % 10_000 == 0 {
                warn!("[MD#{}] 通道满，累计丢弃 {} 条 ({e})", self.member下标, 已丢);
            }
        }
    }
    fn on_rsp_error(&mut self, 信息: Option<&应答信息>, 请求id: i32, 是否最后: bool) {
        let Some(信) = 信息 else { return; };
        if 信.ErrorID == 0 { return; }   // ErrorID=0 不是错误，v1.1 同款过滤
        warn!("[MD#{}] 通用错误 ErrorID={} Msg={} req={} last={}",
              self.member下标, 信.ErrorID, 信.ErrorMsg.to_string(), 请求id, 是否最后);
    }
    // 其余回调(报价询价/ForQuote 等)本程序不用，MdSpi 若给了默认实现就不写。
    // 若 ctp2rs 的 MdSpi 要求全部实现，编译器会报“not all trait items implemented”，
    // 把缺哪些发我，补空实现即可。
}

/// 跑“一轮”member：建 api → 注册 SPI → 连 → 登录 → 订阅 → join 阻塞到断线。
/// 返回后由上层退避重连。合约为空则本 member 空转(仍建连，便于统一处理)。
fn 跑一轮member(
    dynlib: &Path,
    ip: &str,
    端口: u16,
    idx: usize,
    合约s: &[String],
    连接: &连接信息,
    tick_tx: &mpsc::SyncSender<Tick事件>,
    丢弃计数: &Arc<AtomicU64>,
    退出: &Arc<AtomicBool>,
    flow根: &Path,
) {
    let front = format!("tcp://{ip}:{端口}");
    // 每个 member 独立 flow 目录(CTP 自己维护持久化文件，互不干扰)。
    let flow = flow根.join(format!("md_member_{idx}"));
    if let Err(e) = fs::create_dir_all(&flow) {
        warn!("[MD#{idx}] 建 flow 目录失败: {e}");
        return;
    }
    let flow串 = match flow.to_str() {
        Some(s) => s.to_string(),
        None => { warn!("[MD#{idx}] flow 路径非 UTF-8: {}", flow.display()); return; }
    };

    // ① 建 SPI(把 tick_tx/计数 clone 进去，交裸指针给 C++ 侧持有)
    let spi = Box::new(行情Spi {
        member下标: idx,
        tick_tx: tick_tx.clone(),
        连接: 连接.clone(),
        丢弃计数: 丢弃计数.clone(),
    });
    // 【经验】register_spi 要的是 trait 对象胖指针 `*mut dyn MdSpi`，
    // 所以必须 `as *mut dyn MdSpi` 转一次(裸结构指针→trait 对象指针，v1.1 实证写法)。
    // 漏转会报“expected *mut dyn MdSpi, found *mut 行情Spi”。SPI 由 C++ 侧持有，不回收。
    let spi_ptr = Box::into_raw(spi) as *mut dyn MdSpi;

    // 【经验】create_api 是 4 个参数：(动态库路径, flow目录, bool, bool)，
    // 两个 bool 照 v1.1 传 false/false(是否用 FENS/私有流之类，默认关)。
    // 只传 2 个会 E0061“this function takes 4 arguments”。
    let api = MdApi::create_api(
        dynlib.to_str().unwrap_or(""), // create_api 要 &str；路径异常就传空串让它报加载失败
        &flow串,
        false,
        false,
    );
    api.register_spi(spi_ptr);
    api.register_front(&front);
    api.init();
    // 打一行 api 版本，确认动态库真的加载成功(get_api_version 是 v1.1 验证过的静态方法)。
    info!("[MD#{idx}] api_version={} front={front}", MdApi::get_api_version());

    // ③ 登录(带重连感知：退出标志一旦置位，立刻放弃本轮)
    if 退出.load(Ordering::Relaxed) { api.release(); return; }
    let mut 登录 = 登录请求::default();
    登录.BrokerID.assign_from_str(&连接.broker_id);
    登录.UserID.assign_from_str(&连接.user_id);
    登录.Password.assign_from_str(&连接.password);
    api.req_user_login(&mut 登录, 1);
    thread::sleep(Duration::from_millis(1500)); // 等登录回执(CTP 异步)

    // ④ 分批订阅
    for (批号, 块) in 合约s.chunks(SUBSCRIBE_BATCH).enumerate() {
        if 退出.load(Ordering::Relaxed) { break; }
        let 批: Vec<String> = 块.to_vec();
        api.subscribe_market_data(&批);
        if 批号 + 1 < 合约s.len().div_ceil(SUBSCRIBE_BATCH) {
            thread::sleep(Duration::from_millis(SUBSCRIBE_DELAY_MS));
        }
    }
    info!("[MD#{idx}] 订阅完成 {} 只（front={front}）", 合约s.len());

    // ⑤ 阻塞到断线。join 返回 = 连接断了 → 本轮结束，上层退避重连。
    api.join();
    // ⑥ 先 release(断回调/停后台线程)再 drop，v1.1 验证过的释放顺序；
    //    release 后 CTP 不再回调旧 SPI，重连新建实例是干净的。
    api.release();
}

/// 启动全部 member 线程，返回 (句柄表, 全局丢包计数)。
/// 每个线程内部“退避重连循环”，永不因断线退出，只认 退出 标志。
pub fn 启动全部member(
    密钥: &Secrets,
    分片s: Vec<Vec<String>>,
    tick_tx: mpsc::SyncSender<Tick事件>,
    退出: Arc<AtomicBool>,
) -> (Vec<JoinHandle<()>>, Arc<AtomicU64>) {
    let 成员数 = 分片s.len();
    let 丢包 = Arc::new(AtomicU64::new(0));
    let 连接 = 连接信息 {
        broker_id: 密钥.trading.broker_id.clone(),
        user_id: 密钥.trading.user_id.clone(),
        password: 密钥.trading.password.clone(),
    };
    let flow根 = Path::new(WORKSPACE_ROOT).join(&密钥.market_data.flow_dir);
    let dynlib = 动态库路径();
    let 端口 = 密钥.market_data.md_port;

    let mut 句柄 = Vec::with_capacity(成员数);
    for (idx, ip) in 密钥.market_data.member_ips.iter().enumerate() {
        let 合约s = 分片s[idx].clone();
        let 连接 = 连接.clone();
        let tx = tick_tx.clone();
        let 丢包 = 丢包.clone();
        let 退出 = 退出.clone();
        let ip = ip.clone();
        let flow根 = flow根.clone();
        let dynlib = dynlib.clone();
        let h = thread::spawn(move || {
            let mut 退避 = 重连起始秒;
            while !退出.load(Ordering::Relaxed) {
                let 开始 = std::time::Instant::now();
                跑一轮member(&dynlib, &ip, 端口, idx, &合约s, &连接, &tx, &丢包, &退出, &flow根);
                if 退出.load(Ordering::Relaxed) { break; }
                // 若一轮“秒退”(登录失败/连接秒断)，退避加长；跑够 30s 说明是正常收盘断线，重置退避。
                if 开始.elapsed() < Duration::from_secs(30) {
                    warn!("[MD#{idx}] 本轮秒退，{退避}s 后重连");
                } else {
                    退避 = 重连起始秒; // 跑了一会儿才断，视为正常，重置
                }
                // 可中断 sleep：每 100ms 看一次退出标志，保证 Ctrl+C 能快速收尾。
                for _ in 0..(退避 * 10) {
                    if 退出.load(Ordering::Relaxed) { break; }
                    thread::sleep(Duration::from_millis(100));
                }
                退避 = (退避 * 2).min(重连封顶秒);
            }
            info!("[MD#{idx}] member 线程退出");
        });
        句柄.push(h);
    }
    (句柄, 丢包)
}

// =============================================================================
//  第 8 段：主流程
// =============================================================================

/// 注册 Ctrl+C：第 1 次置退出标志(优雅收尾→落盘→自动退进程)；
/// 第 2 次直接强杀 exit(130)。正常只按一次，看到“落盘完成+提示符回来”就已经退了。
fn 装信号处理(退出: Arc<AtomicBool>) {
    // ctrlc crate 若未引入，可用 signal-hook 替代；这里用最朴素的 ctrlc 风格：
    // 为减少依赖，直接用 std 不便(无信号 API)，故依赖 ctrlc = "3"。
    let 退出2 = 退出.clone();
    let mut 次数 = 0u8;
    let _ = ctrlc::set_handler(move || {
        次数 += 1;
        if 次数 == 1 {
            warn!("\n[RUN] 收到 Ctrl+C(第1次)：优雅退出——停重连 → 收尾聚合 → 落当天 → 自动退");
            退出2.store(true, Ordering::SeqCst);
        } else {
            eprintln!("\n[RUN] 第2次 Ctrl+C：强制退出(当天未落盘部分将丢失!)");
            std::process::exit(130);
        }
    });
}

fn main() {
    // 日志初始化：只在这里(bin 入口)装 subscriber；库/模块内只 tracing 埋点不初始化。
    // 中文 target 名在 EnvFilter directive 里解析有风险，这里直接用“默认 info 级别”，
    // 需要调级别用环境变量 RUST_LOG=debug 覆盖(from_default_env 优先)。
    let 过滤 = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    // try_init：重复初始化不 panic(自动重启场景友好)，失败忽略。
    let _ = tracing_subscriber::fmt().with_env_filter(过滤).try_init();

    if let Err(e) = 主流程() {
        // 顶层兜底：打印错误链后非零退出，交给外面(systemd/看门狗)拉起。
        eprintln!("[FATAL] 主流程出错: {e}");
        std::process::exit(1);
    }
    info!("[RUN] 正常退出");
}

fn 主流程() -> Result<(), 错误> {
    info!("========== 分钟K线落盘 v2.0.0 启动 ==========");
    let 退出 = Arc::new(AtomicBool::new(false));
    装信号处理(退出.clone());

    // (1) 配置 + 密钥
    let 配置 = 配置::载入或默认();
    let 密钥路径 = std::env::args().nth(1).map(PathBuf::from).unwrap_or_else(|| {
        Path::new(WORKSPACE_ROOT).join("config/secrets.toml")
    });
    let 密钥s = fs::read_to_string(&密钥路径)
        .map_err(|e| 错误::配置(format!("读密钥 {} 失败: {e}", 密钥路径.display())))?;
    let 密钥: Secrets = toml::from_str(&密钥s)?;
    info!("[CFG] 密钥已加载 {} | 商品前置 {} 个 | 以交易所时间分桶={} | 快照间隔={}s",
          密钥路径.display(), 密钥.market_data.member_ips.len(),
          配置.以交易所时间分桶, 配置.快照间隔秒);

    // (2) 合约清单
    let 合约文件 = if Path::new(&密钥.market_data.contracts_file).is_absolute() {
        PathBuf::from(&密钥.market_data.contracts_file)
    } else {
        Path::new(WORKSPACE_ROOT).join(&密钥.market_data.contracts_file)
    };
    let 合约s = 载入合约清单(&合约文件)?;
    let 成员数 = 密钥.market_data.member_ips.len();
    if 成员数 == 0 {
        return Err(错误::配置("member_ips 为空".into()));
    }
    let 分片s = 分片合约(&合约s, 成员数);
    let 总数: usize = 分片s.iter().map(|v| v.len()).sum();
    info!("[CFG] 合约 {} 条 → 分 {成员数} 片(各片 {} 条)，与源清单一致的校验: {}",
          合约s.len(), 总数, 总数 == 合约s.len());

    // (3) 通道 + 聚合器 + 落盘器
    let (tick_tx, tick_rx) = mpsc::sync_channel::<Tick事件>(CHANNEL容量);
    let mut 聚合 = 聚合器::新建(成员数, 配置.以交易所时间分桶, 配置.过滤盘前重演);
    let 落盘器 = 落盘器::新建(落盘选项 {
        输出目录: 配置.落盘.输出目录.clone(),
        双写增量: 配置.落盘.双写增量,
        原子写: 配置.落盘.原子写,
        同日合并: 配置.落盘.同日合并,
    });
    let 当前日期 = 当前归档日期();
    // 快照恢复(快照关闭时直接返回 0，不读盘)
    恢复当天快照(&mut 聚合, &落盘器, 配置.快照间隔秒, &当前日期);
    let 聚合 = Arc::new(Mutex::new(聚合)); // 此后经 Arc+Mutex 共享给聚合线程/快照/主线程

    // (4) 起 member 线程
    let (成员句柄s, 丢包计数) = 启动全部member(&密钥, 分片s, tick_tx, 退出.clone());
    info!("[RUN] 已起 {成员数} 个行情 member");

    // (5) 起聚合线程(独占 tick_rx)
    let 聚合2 = 聚合.clone();
    let 退出聚合 = 退出.clone();
    let 聚合线程 = thread::spawn(move || {
        let mut 上次日志 = std::time::Instant::now();
        loop {
            // 带超时收，便于周期检查退出标志与“冷门合约分钟边界巡检”。
            match tick_rx.recv_timeout(Duration::from_millis(200)) {
                Ok(t) => {
                    let _ = 加锁(&聚合2, "聚合喂tick").map(|mut g| { g.喂一个tick(&t); });
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
            // 每分钟巡检：把“本机已进入新分钟、桶还停在旧分钟”的合约收盘，
            // 解决冷门合约长时间无 tick 时最后一根迟迟不进缓存的问题(v1.1 经验)。
            if 上次日志.elapsed() >= Duration::from_secs(60) {
                上次日志 = std::time::Instant::now();
                let 现在 = chrono::Local::now().timestamp_millis_opt().single().unwrap_or(0);
                let 现在分钟 = 分钟取整(现在);
                if let Ok(mut g) = 加锁(&聚合2, "聚合巡检") {
                    // 找出桶时间 != 当前分钟的合约，逐个收盘
                    let 陈旧: Vec<String> = g.活跃桶键().into_iter()
                        .filter(|k| g.桶时间(k) != Some(现在分钟)).collect();
                    for 合约 in 陈旧 { g.收盘一个(&合约); }
                }
            }
            if 退出聚合.load(Ordering::Relaxed) {
                // 收尾：活跃桶全部收盘进缓存，然后退出聚合线程(落盘由主线程统一做)。
                if let Ok(mut g) = 加锁(&聚合2, "聚合收尾") {
                    let n = g.收尾所有活跃桶();
                    info!("[AGG] 收尾活跃桶 {n} 根进缓存");
                }
                break;
            }
        }
        info!("[AGG] 聚合线程退出 | 共完成 {} 根 | 丢弃 {} 根",
              加锁(&聚合2, "聚合退出统计").map(|g| g.done计数()).unwrap_or(0),
              加锁(&聚合2, "聚合退出统计2").map(|g| g.丢弃计数()).unwrap_or(0));
    });

    // (6) 起快照线程(快照间隔=0 时返回 None，纯内存模式)
    let 归档日期取法: Arc<dyn Fn() -> String + Send + Sync> = Arc::new(当前归档日期);
    let 快照句柄 = 起快照线程(配置.快照间隔秒, 聚合.clone(), 落盘器.clone(),
                       退出.clone(), 归档日期取法.clone());

    // (7) 主循环：周期打统计 + 检测跨天落“昨天” + 检测退出
    let mut 当前归档 = 当前日期.clone();
    let mut 上次统计 = std::time::Instant::now();
    let mut 已落盘日期: std::collections::HashSet<String> = std::collections::HashSet::new();
    // 若快照恢复过，把当天标记为“已恢复”(不重复落)，但跨天后照常落旧日。
    let 运行起点 = std::time::Instant::now();
    while !退出.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_secs(1));
        // 跨天：日期变了 → 把“昨天”正式落盘(从聚合缓存取)
        let 现在日期 = 当前归档日期();
        if 现在日期 != 当前归档 {
            let 旧 = 当前归档.clone();
            info!("[RUN] 跨天 {旧} → {现在日期}，落盘前一天");
            let bars = 加锁(&聚合, "跨天取bars").map(|g| g.取日期bars(&旧).unwrap_or(&[]).to_vec())
                .unwrap_or_default();
            match 落盘器.落当天(&旧, &bars) {
                Ok(n) => { info!("[WRT] 跨天落盘 {旧} 共 {n} 根"); 已落盘日期.insert(旧.clone()); }
                Err(e) => warn!("[WRT] 跨天落盘失败(下轮退出时重试): {e}"),
            }
            当前归档 = 现在日期;
        }
        // 周期统计
        if 上次统计.elapsed() >= Duration::from_secs(配置.统计周期秒) {
            上次统计 = std::time::Instant::now();
            if let Ok(g) = 加锁(&聚合, "统计") {
                let 今日 = g.日期根数(&当前归档);
                let 分布: String = g.各member分布().iter()
                    .map(|(i, c)| format!("MD#{i}:{c}")).collect::<Vec<_>>().join(" ");
                let 丢 = 丢包计数.load(Ordering::Relaxed);
                info!("===== 运行 {}s | 活跃桶 {} | 已完成 {} 根 | 今日 {} 根 | 丢弃 {} | 通道丢包 {} | 各member tick [{}] =====",
                      运行起点.elapsed().as_secs(), g.活跃桶数(), g.done计数(), 今日,
                      g.丢弃计数(), 丢, 分布);
            }
        }
    }

    // (8) 退出流程：等 member / 聚合 / 快照 依次收尾，再把当天正式落盘。
    warn!("[RUN] 退出流程开始：先落当天，再等线程收尾…");
    // 聚合线程看到退出标志会先“收尾活跃桶”再退，我们等它退完，保证缓存是完整的。
    let _ = 聚合线程.join();
    if let Some(h) = 快照句柄 {
        // 快照线程同样看退出标志自行结束；它和我们要不要“再落一次归档”不冲突
        // (落归档走 落当天→bars_日期.parquet；快照是 bars_快照_日期.parquet，两个文件)
        let _ = h.join();
    }
    // 落“当前归档日”全部 bars(同日合并会把它和可能已存在的快照/半段合并)
    for 日期 in 聚合.lock().map(|g| g.所有日期()).unwrap_or_default() {
        if 已落盘日期.contains(&日期) { continue; } // 跨天已落的跳过
        let bars = 加锁(&聚合, "退出取bars").map(|g| g.取日期bars(&日期).unwrap_or(&[]).to_vec())
            .unwrap_or_default();
        match 落盘器.落当天(&日期, &bars) {
            Ok(n) => info!("[WRT] 退出落盘 {日期} 共 {n} 根"),
            Err(e) => warn!("[WRT] 退出落盘 {日期} 失败: {e}（数据仍在快照文件里，重启可恢复）"),
        }
    }
    // 通知 member 线程退出并等它们(它们看 退出 标志会跳出重连循环)
    for h in 成员句柄s { let _ = h.join(); }
    info!("[RUN] 全部线程已收尾。退出。(再按一次 Ctrl+C 才会强杀，正常无需)");
    Ok(())
}

// =============================================================================
//  附录：给“聚合巡检”补的两个小方法(放在 impl 外也能，Rust 允许同 crate 多 impl 块)
// =============================================================================
impl 聚合器 {
    /// 列出所有活跃桶的合约键(巡检用)。
    fn 活跃桶键(&self) -> Vec<String> { self.活跃桶.keys().cloned().collect() }
    /// 取某合约当前桶的 bar 时间(不存在返回 None)。
    fn 桶时间(&self, 合约: &str) -> Option<i64> { self.活跃桶.get(合约).map(|b| b.bar时间毫秒) }
    /// 强制收盘某合约的当前桶(巡检推进边界用)。
    fn 收盘一个(&mut self, 合约: &str) {
        if let Some(桶) = self.活跃桶.remove(合约) {
            let 合约名 = 合约.to_string();
            self.收盘入缓存(合约名, 桶);
        }
    }
}

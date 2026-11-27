//! 运行配置与全局常量（★v1.5 拆模块：行为零变化，只是搬家）。
use rust_ctp_test::config::WORKSPACE_ROOT;
use std::path::{Path, PathBuf};
use std::time::Duration;

// ============================================================
// 常量（改这里=改全局行为，散落在代码里的数字都收到这，符合"配置集中管理"）
// ============================================================

pub(crate) const MD_DYNLIB_REL: &str = "lib/libthostmduserapi_se.so";
pub(crate) const SUBSCRIBE_BATCH: usize = 500;
pub(crate) const SUBSCRIBE_DELAY_MS: u64 = 300;
pub(crate) const REPORT_INTERVAL_SEC: u64 = 5;

pub(crate) const RECONNECT_BASE_MS: u64 = 1_000;
pub(crate) const RECONNECT_MAX_MS: u64 = 60_000;
pub(crate) const LOGIN_SETTLE_MS: u64 = 1_500;

// ★v1.5 方案B 守护常量（D024）：三类异常全部收敛到 teardown→重建，阈值关系由
//   单测「方案b守护常量下限」钉死——改任何一个数之前先跑那个测试想清楚。
pub(crate) const CONNECT_TIMEOUT_MS: u64 = 15_000; // spawn_new 后 15 秒无 Connected → 主动重建
pub(crate) const DISCONNECT_REBUILD_MS: u64 = 60_000; // 断线后 60 秒未回连（CTP 自动重连失效）→ 主动重建
pub(crate) const LOGIN_FAIL_REBUILD: u64 = 3; // 连续 3 次登录失败 → 主动重建（防死循环打柜台）
pub(crate) const 回拨告警节流: Duration = Duration::from_secs(300); // 🔴3 同合约回拨告警 5 分钟一条，不刷屏

/// ★本版新增（可随时调）：分片落盘 fsync 间隔（秒）。
///   含义：落盘线程最多每这么多秒把追加的 bar 强制刷到磁盘一次。
///   值越小越抗崩（丢得越少）但 fsync 越频；越大越省 IO 但崩了丢得多。
///   设 60 = 最多丢 60 秒；设 5 = 最多丢 5 秒；笔记本有电池、追求极致省 IO 可设 120。
///   注意：不管设多少，每根 bar 都会"追加 + flush(到内核缓冲)"，只是 fsync(落物理盘)按这个间隔。
pub(crate) const 分片落盘秒: u64 = 60;

// 落盘线程与聚合线程之间的在途 bar 队列容量。满了会【阻塞背压】，绝不静默丢 bar。
// 全天满盘约 50 万根，100 万绰绰有余；峰值内存 = 100万 × ~200B ≈ 200MB 上限。

// 分片文件 / 合并成品的相对目录（都挂在 工作区根/data_数据 下）。

pub(crate) const MINUTE_MS: i64 = 60_000;
// ── ★v1.3/v1.4 新增：运行配置（规则20「配置集中管理」）─────────────────────
// 【为什么有这一层】原来 分片落盘秒 等是 const 写死，调一个数就要改 .rs + 重编译。
// 现在：toml 有值用 toml，没值用内建默认；找不到配置文件只打 warn，绝不 panic。
// 【线程怎么共享】配置在 main 最顶部载入一次，之后只读；线程需要的单个值（如 fsync 间隔）
// 直接按值 clone 进去，不去共享整个 struct，也就不需要 Arc/锁。
pub(crate) const 默认_分片落盘秒: u64 = 60; // 0=不额外fsync · 5=极致抗崩 · 60=推荐
pub(crate) const 默认_BAR队列容量: usize = 1_000_000;
pub(crate) const 默认_分片目录: &str = "data_数据/分钟分片";
pub(crate) const 默认_成品目录: &str = "data_数据/分钟K线";
pub(crate) const 配置_文件名: &str = "配置_分钟K线.toml";

/// 运行期生效配置。字段名就是 toml 里的键名，一一对应，改行为只动 toml。
#[derive(Debug, Clone)]
pub(crate) struct 运行配置 {
    pub(crate) 分片落盘秒: u64,
    pub(crate) bar队列容量: usize,
    pub(crate) 分片目录_rel: String,
    pub(crate) 成品目录_rel: String,
    /// ★v1.5 归档压缩（D028）：合并成功后把"非今日"且已静默的 jsonl 压成 .jsonl.zst。
    /// 默认开；toml 里写 `归档压缩 = 0` 关闭。
    pub(crate) 归档压缩: bool,
}
impl 运行配置 {
    pub(crate) fn 默认() -> Self {
        Self {
            分片落盘秒: 默认_分片落盘秒,
            bar队列容量: 默认_BAR队列容量,
            分片目录_rel: 默认_分片目录.to_string(),
            成品目录_rel: 默认_成品目录.to_string(),
            归档压缩: true,
        }
    }

    /// 极简 toml 读取：只认平铺的 `键 = 值`；跳过空行/整行注释；截掉行内注释与引号。
    /// 【为什么不用 serde】只有 4 个键，手写解析比再加一个派生宏依赖面更小（止血版求稳）。
    /// 等配置项超过约 20 个、或出现嵌套表（[表]）时，再换成 serde 反序列化 whole struct。
    fn 从toml覆盖(mut self, 文本: &str, 来源: &str) -> Self {
        // 闭包：在函数内定义一个"取键"的小函数，能捕获外层参数但不占栈外空间。
        let 取 = |键名: &str| -> Option<String> {
            for 行 in 文本.lines() {
                let 行 = 行.trim();
                // 跳过：空行 / 整行注释 / [表头]（本解析器只认平铺键，表头无意义）
                if 行.is_empty() || 行.starts_with('#') || 行.starts_with('[') {
                    continue;
                }
                if let Some((k, v)) = 行.split_once('=')
                    && k.trim() == 键名
                {
                    let v = v.split('#').next().unwrap_or("").trim().trim_matches('"');
                    if !v.is_empty() {
                        return Some(v.to_string());
                    }
                }
            }
            None
        };
        if let Some(v) = 取("分片落盘秒") {
            match v.parse::<u64>() {
                Ok(n) => self.分片落盘秒 = n,
                // 配置写错不致命：打日志 + 保留默认，绝不 panic（规则：配置错误不能让程序起不来）
                Err(_) => tracing::error!("[CFG] {} 的 分片落盘秒={} 不是数字，忽略该行", 来源, v),
            }
        }
        if let Some(v) = 取("bar队列容量") {
            match v.parse::<usize>() {
                Ok(n) => self.bar队列容量 = n,
                Err(_) => tracing::error!("[CFG] {} 的 bar队列容量={} 不是数字，忽略该行", 来源, v),
            }
        }
        if let Some(v) = 取("分片目录") {
            self.分片目录_rel = v;
        }
        if let Some(v) = 取("成品目录") {
            self.成品目录_rel = v;
        }
        if let Some(v) = 取("归档压缩") {
            let 开 = v != "0" && v != "false" && v != "off";
            self.归档压缩 = 开;
        }
        self
    }

    /// 查找顺序：①环境变量 分钟K线配置 指向的文件 ②CARGO_MANIFEST_DIR（工程根）③工作区根。
    /// 三处都找是为了"不管从哪个目录启动、toml 放在哪"都能读到，省掉"配置在却说没有"的排查。
    pub(crate) fn 载入() -> Self {
        let 默认 = Self::默认();
        let mut 候选: Vec<PathBuf> = Vec::new();
        if let Some(p) = std::env::var_os("分钟K线配置") {
            候选.push(PathBuf::from(p));
        }
        候选.push(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(配置_文件名));
        候选.push(Path::new(WORKSPACE_ROOT).join(配置_文件名));
        for p in &候选 {
            if p.is_file() {
                match std::fs::read_to_string(p) {
                    Ok(t) => {
                        tracing::info!("[CFG] 已加载运行配置 {}", p.display());
                        return 默认.从toml覆盖(&t, &p.display().to_string());
                    }
                    Err(e) => {
                        tracing::error!("[CFG] 读配置 {} 失败: {}（改用内建默认）", p.display(), e)
                    }
                }
            }
        }
        tracing::error!(
            "[CFG] ⚠ 没找到 {}，使用内建默认：分片落盘秒={}、bar队列容量={}、分片目录={}",
            配置_文件名,
            默认.分片落盘秒,
            默认.bar队列容量,
            默认.分片目录_rel
        );
        默认
    }
}

#[cfg(test)]
mod 测试 {
    use super::*;

    #[test]
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
}

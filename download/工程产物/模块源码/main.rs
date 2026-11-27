//! 分钟K线落盘 · v1.3 分片落盘版（基于已跑通的 v1.1 完整版，只换"落盘"这一块）
//!
//! ============================ 这一版解决了什么 ============================
//! v1.1 的毛病：全天 bar 攒在内存（cache），退出/跨天才写整日 Parquet。
//!   后果 1：进程被 kill -9 / 断电 / 连按两次 Ctrl+C → 当天全丢（实测丢过 46 万根）。
//!   后果 2：若改成定时全量重写整日文件，一天重写十几次 → 约 15GB 写放大，笔记本 SSD 吃不消。
//!
//! v1.3 的做法（追加式分片，两种病一起治）：
//!   · 每根 bar 一收盘，落盘线程立刻把它【追加一行 JSON】到当天分片文件
//!     data_数据/分钟分片/bars_YYYYMMDD.jsonl，并且【每 分片落盘秒(默认60) 秒 fsync 一次】。
//!     → 崩溃最多丢最后 60 秒；全天只写一遍（约 300MB），没有 15G。
//!   · 采集进程不攒整天，内存只留"当前活跃桶 + 几秒在途 bar"，约几 MB，不再随运行时间涨。
//!   · 合并是独立动作，随时可做：
//!       程序 合并 20261124          合并指定日期（可跟多个）
//!       程序 合并 今日               合并今天（按本机墙钟日期）
//!     合并会：读 jsonl（先复制一份再读，边采边合也不冲突）→ 按合约+时间排序
//!           → 算 volume_1m/turnover_1m 单分钟增量 → 补缺口(is_gap) → 原子写
//!           data_数据/分钟K线/bars_YYYYMMDD.parquet（13 列，下游照旧读）。
//!
//! ============================ 与 v1.1 的关系（重要）============================
//! CTP 接入 / SPI / 16-member 分片 / 断线重连 / 优雅退出 / 分钟聚合【全部照抄 v1.1，一字未改】。
//! 只动了三处：
//!   1) run_aggregator / close_bucket：不再往 cache 攒整天，收盘即把 bar 发给落盘线程。
//!   2) run_writer：从"写整日 Parquet"改成"追加 JSON 行 + 定时 fsync"。
//!   3) main：加了一个子命令分发——第 1 个参数是"合并"就进合并模式，否则是采集模式。
//!
//! 其余函数（load_contracts_csv / shard_contracts / MemberSpi / ApiHolder / spawn_member /
//! spawn_listener / fill_gaps 等）与 v1.1 逐字一致，动它们=引入风险，所以不碰。
//!
//! ============================ 未做、留给下一步的 ============================
//!   · 业务代码里的 unwrap / tracing::info! 清理（改 tracing + thiserror + 零 unwrap）是【独立的另一步】，
//!     在本落盘改动验证稳定后再做，两者不捆绑，避免一次改太多查不出问题。
//!   · 收盘自动合并：目前合并是手动命令；稳定后可加"检测到收盘自动触发一次合并"。
//!
//! 用法：
//!   采集（和 v1.1 一样）：  cargo run --release --bin 分钟k线落盘_v1_3 -- [secrets.toml] [输出目录]
//!   合并：                  cargo run --release --bin 分钟k线落盘_v1_3 -- 合并 20261124
//!                           cargo run --release --bin 分钟k线落盘_v1_3 -- 合并 今日
//!
//! 依赖（Cargo.toml，chrono 必须精确锁，否则和 arrow-arith 52 撞 quarter() 编不过）：
//!   arrow = "52"
//!   parquet = "52"
//!   chrono = "=0.4.38"
//!   ctrlc = "3"
//!   toml = "0.8"
//!   serde = { version = "1", features = ["derive"] }   # 本版新增：jsonl 序列化
//!   serde_json = "1"                                    # 本版新增：jsonl 逐行读写
//! ============================================================================

use chrono::Local;
use rust_ctp_test::config::{Secrets, WORKSPACE_ROOT};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

mod aggregate;
mod config;
mod contracts;
mod events;
mod merge;
mod session;
mod spi;
mod stats;
mod writer;

use crate::aggregate::{MinuteBar, run_aggregator};
use crate::config::{
    MD_DYNLIB_REL, RECONNECT_MAX_MS, REPORT_INTERVAL_SEC, 分片落盘秒, 运行配置
};
use crate::contracts::{load_contracts_csv, shard_contracts};
use crate::events::{MemberEvent, TickEvent};
use crate::merge::{合并一日, 归档非今日分片};
use crate::session::spawn_member;
use crate::stats::{MemberStats, 锁或恢复};
use crate::writer::run_writer;

// ============================================================
// 主流程（v1.1 基础上加子命令分发）
//   参数 1 == "合并"/"merge"  → 合并模式（不连 CTP、不读账号）
//   否则                       → 采集模式（参数1=secrets.toml，参数2=成品输出目录，与 v1.1 一致）
// ============================================================

fn main() {
    // ★【规则17】subscriber 必须是 main 第一件事：之后所有 tracing::info!/error! 才有出口。
    //   RUST_LOG 环境变量控级别（如 RUST_LOG=debug）；没设则默认 info。
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    // ★【规则20】启动第一件事：载入运行配置，合并/采集两分支共用同一份，避免路径不一致。
    let 运行 = 运行配置::载入();
    let args: Vec<String> = std::env::args().collect();

    // ---------- 合并模式 ----------
    if args
        .get(1)
        .map(|s| s == "合并" || s == "merge")
        .unwrap_or(false)
    {
        let 分片目录 = Path::new(WORKSPACE_ROOT).join(&运行.分片目录_rel);
        let 成品目录 = Path::new(WORKSPACE_ROOT).join(&运行.成品目录_rel);
        // 日期列表：args[2..]；空或含"今日" → 今天本机墙钟日期。
        let mut 日期们: Vec<String> = args
            .iter()
            .skip(2)
            .filter(|s| s != &"今日")
            .cloned()
            .collect();
        if 日期们.is_empty() || args.iter().skip(2).any(|s| s == "今日") {
            日期们.push(Local::now().format("%Y%m%d").to_string());
        }
        tracing::info!(
            "[MERGE] 合并模式 | 分片目录 {} | 成品目录 {} | 日期 {:?}",
            分片目录.display(),
            成品目录.display(),
            日期们
        );
        let mut 有错 = false;
        for d in 日期们 {
            match 合并一日(&d, &分片目录, &成品目录) {
                Ok(()) => {}
                Err(e) => {
                    tracing::error!("[MERGE] ❌ 合并 {} 失败: {}", d, e);
                    有错 = true;
                }
            }
        }
        // ★v1.5：全部合并成功后自动归档历史分片（D028）——今日文件绝不动；
        //   想关掉：配置_分钟K线.toml 里加一行 `归档压缩 = 0`
        if !有错 && 运行.归档压缩 {
            let 今日 = Local::now().format("%Y%m%d").to_string();
            归档非今日分片(&分片目录, &今日, 600);
        }
        std::process::exit(if 有错 { 1 } else { 0 });
    }

    // ---------- 采集模式（以下基本照抄 v1.1 的 main）----------
    let stop = Arc::new(AtomicBool::new(false));
    {
        let s = stop.clone();
        ctrlc_handler(move || {
            if s.load(Ordering::Relaxed) {
                // 第二次 Ctrl+C：强杀。注意 v1.3 下当天数据已在分片里(每60秒fsync)，
                // 所以即便强杀，最多也只丢最后 60 秒，不再像 v1.1 那样丢整天。
                tracing::error!(
                    "\n[RUN] 第二次 Ctrl+C，强制退出（v1.3 下当天数据已在分片文件里，最多丢最后 {} 秒）",
                    分片落盘秒
                );
                std::process::exit(130);
            }
            tracing::error!(
                "\n[RUN] 收到 Ctrl+C：停重连 → 收盘活跃桶 → 落盘线程收尾 fsync → 退出（再按一次强制，但只会丢最后 {} 秒）",
                分片落盘秒
            );
            s.store(true, Ordering::Relaxed);
        });
    }

    let secrets_path = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| rust_ctp_test::config::DEFAULT_SECRETS.to_string());
    let text = std::fs::read_to_string(&secrets_path).unwrap_or_else(|e| {
        tracing::error!("[CFG] 读 {} 失败: {}", secrets_path, e);
        std::process::exit(1);
    });
    let cfg: Secrets = toml::from_str(&text).unwrap_or_else(|e| {
        tracing::error!("[CFG] 解析 {} 失败: {}", secrets_path, e);
        std::process::exit(1);
    });
    tracing::info!("[CFG] 已加载 {}", secrets_path);

    let contracts_file = if Path::new(&cfg.market_data.contracts_file).is_absolute() {
        PathBuf::from(&cfg.market_data.contracts_file)
    } else {
        Path::new(WORKSPACE_ROOT).join(&cfg.market_data.contracts_file)
    };
    let contracts = load_contracts_csv(&contracts_file).unwrap_or_else(|e| {
        tracing::error!("[CFG] {}", e);
        std::process::exit(1);
    });
    tracing::info!(
        "[CFG] 工作区根 {} | 合约 {} 条 | member {} 个 | md_port {} | 断线重连 开 | 分片fsync {}秒",
        WORKSPACE_ROOT,
        contracts.len(),
        cfg.market_data.member_ips.len(),
        cfg.market_data.md_port,
        分片落盘秒
    );

    if cfg.market_data.member_ips.is_empty() {
        tracing::error!("[CFG] member_ips 为空，无前置可连，退出");
        std::process::exit(1);
    }

    let num_members = cfg.market_data.member_ips.len();
    let shards = shard_contracts(&contracts, num_members);

    let dynlib = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(MD_DYNLIB_REL);
    if !dynlib.exists() {
        tracing::error!("[CFG] 动态库不存在 {}", dynlib.display());
        std::process::exit(1);
    }

    // 分片目录（采集时往这写 jsonl）。成品目录只在合并时用，采集模式不需要。
    let 分片目录 = match args.get(2) {
        Some(p) => PathBuf::from(p),
        None => Path::new(WORKSPACE_ROOT).join(&运行.分片目录_rel),
    };
    std::fs::create_dir_all(&分片目录).unwrap_or_else(|e| {
        tracing::error!("[CFG] 建分片目录 {} 失败: {}", 分片目录.display(), e);
        std::process::exit(1);
    });
    tracing::info!(
        "[CFG] 分片输出目录 {}（每根bar收盘即追加，每{}秒fsync）",
        分片目录.display(),
        分片落盘秒
    );

    // ★改动：bar 通道从 (日期, Vec) 改成【逐根 MinuteBar】；bounded 队列满则背压阻塞，不丢。
    let (bar_tx, bar_rx) = mpsc::sync_channel::<MinuteBar>(运行.bar队列容量);
    let writer_handle = {
        let d = 分片目录.clone();
        std::thread::spawn(move || run_writer(bar_rx, d, 运行.分片落盘秒))
    };
    let (tick_tx, tick_rx) = mpsc::channel::<TickEvent>();
    let agg_stop = stop.clone();
    let agg_handle = std::thread::spawn(move || run_aggregator(tick_rx, bar_tx, agg_stop));

    let stats: Arc<Mutex<Vec<MemberStats>>> =
        Arc::new(Mutex::new(vec![MemberStats::default(); num_members]));
    let mut handles = Vec::new();
    for (idx, ip) in cfg.market_data.member_ips.iter().enumerate() {
        let front = format!("tcp://{}:{}", ip, cfg.market_data.md_port);
        let flow_dir = Path::new(WORKSPACE_ROOT)
            .join(&cfg.market_data.flow_dir)
            .join(format!("member_{:02}", idx));
        // ★方案B：每个 member 私有事件通道，member 线程自己消费事件+记账+转发 tick
        //   （原 spawn_listener 已删，职责并入 spawn_member，见 D024）。
        let (evt_tx, evt_rx) = mpsc::sync_channel::<MemberEvent>(8192);
        let h = spawn_member(
            idx,
            front,
            flow_dir,
            dynlib.clone(),
            cfg.trading.broker_id.clone(),
            cfg.trading.user_id.clone(),
            cfg.trading.password.clone(),
            shards[idx].clone(),
            evt_tx,
            evt_rx,
            tick_tx.clone(),
            stats.clone(),
            stop.clone(),
        );
        handles.push(h);
    }
    drop(tick_tx);

    tracing::info!(
        "[RUN] {} 个 member 已启动，进入健康巡检（非交易时段空转属正常；Ctrl+C 优雅退出，数据每{}秒落盘一次）",
        num_members,
        分片落盘秒
    );

    let start = Instant::now();
    let mut 上一次告警 = Instant::now();
    loop {
        if stop.load(Ordering::Relaxed) {
            tracing::info!(
                "[RUN] 停止标志已置位，等待 member 线程退出（最长退避 {}s）",
                RECONNECT_MAX_MS / 1000
            );
            break;
        }
        std::thread::sleep(Duration::from_secs(REPORT_INTERVAL_SEC));
        {
            let st = 锁或恢复(&stats);
            let mut conn = 0;
            let mut login = 0;
            let mut ticks = 0u64;
            let mut disc = 0u64;
            let mut 回拨 = 0u64;
            let mut 重建 = 0u64;
            let mut stale: Vec<usize> = Vec::new();
            for (i, m) in st.iter().enumerate() {
                if m.connected {
                    conn += 1;
                }
                if m.logged_in {
                    login += 1;
                }
                ticks += m.ticks;
                disc += m.disconnects;
                回拨 += m.tick_out_of_order;
                重建 += m.rebuilds;
                if let Some(t) = m.last_tick_at
                    && t.elapsed().as_secs() > 300
                {
                    stale.push(i);
                }
            }
            tracing::info!(
                "===== 运行 {} 秒 | 已连 {} | 已登录 {} | 累计 tick {} | 累计断线 {} 次 | 交易所时间回拨 {} | 主动重建 {} =====",
                start.elapsed().as_secs(),
                conn,
                login,
                ticks,
                disc,
                回拨,
                重建
            );
            // ★改进：收盘后全员静默是正常现象，别每 5 秒刷屏。
            //   只在"曾经登录过(说明今天是交易日)"且"距上次告警≥60秒"时才报一次，避免刷屏。
            let 今天登录过 = st.iter().any(|m| m.logged_in || m.ready_rounds > 0);
            if !stale.is_empty() && 今天登录过 && 上一次告警.elapsed().as_secs() >= 60 {
                tracing::error!(
                    "[RUN] 超过 5 分钟无 tick 的 member: {:?}（若已收盘属正常；盘中则需关注是否断流待重连）",
                    stale
                );
                上一次告警 = Instant::now();
            }
        }
    }

    for h in handles {
        let _ = h.join();
    }
    tracing::info!("[RUN] member 线程全部退出，等聚合线程收尾");
    let _ = agg_handle.join();
    let _ = writer_handle.join();
    tracing::info!("[RUN] 全部退出。当天数据已在分片文件里，随时用 `合并 今日` 生成成品 Parquet。");
}

/// Ctrl+C 处理（照抄 v1.1，用 ctrlc crate，无 unsafe）。
pub(crate) fn ctrlc_handler<F: FnMut() + Send + 'static>(f: F) {
    // 【规则15 零 expect】启动期致命错误：打日志后 exit(1)，不用 expect（避免 clippy expect_used）
    if let Err(e) = ctrlc::set_handler(f) {
        tracing::error!(
            "[FATAL] 设置 Ctrl+C 处理失败: {}（Linux 下一般是权限或被父进程截获）",
            e
        );
        std::process::exit(1);
    }
}

// ╔══════════════════════════════════════════════════════════════════╗
// ║ 【文件头教学导读】aggregate.rs —— 把「秒级心跳」捏成「分钟蜡烛」             ║
// ╚══════════════════════════════════════════════════════════════════╝
//
// 交易所每秒推来若干笔 tick（最新价、累计量……），但做量化的人要的是分钟K线
//（一根蜡烛：这分钟开/高/低/收、成交了多少）。本文件就是那个「捏蜡烛」的：
//   · 每个还在活跃的合约有一个「桶（Bucket）」：tick 进来不断更新桶里的最高/最低/最新价；
//   · 一到下一分钟（或退出时兜底巡检），把桶收【盘】——组装成 MinuteBar
//     立刻塞给落盘线程（writer.rs），内存里不攒整天数据。
//
// 【它在整条链路里的位置】CTP连接 → tick → 分钟K线 → jsonl分片 → Parquet合并。
// 本文件正是「tick → 分钟K线」这一环，全工程的心脏。
//
// 【本文件用到的 Rust 核心概念】
//   · struct + #[derive(Serialize, Deserialize)]：一行注解让结构体可变 JSON；
//   · serde(rename/alias/default)：字段改名/兼容老文件/缺省值（数据兼容三板斧）；
//   · Option 与 ?：解析失败返回 None，绝不 panic（宁可漏放不误杀）；
//   · HashMap：桶仓库 + 计数器；entry().or_default() 惯用法；
//   · mpsc::SyncSender 有界管道与背压（满了阻塞，不丢数据）；
//   · 迭代器链 filter_map/collect、windows、sort_unstable_by_key；
//   · #[cfg(test)] 单元测试（含跨午夜环形最短差这类边界用例）。
//
// 【建议学习时长】45 到 60 分钟。本文件是全工程最值得精读的核心。

//! 聚合：tick → 分钟K线（收盘即送落盘，不攒整天）。
use crate::config::{MINUTE_MS, REPORT_INTERVAL_SEC, 分片落盘秒, 在交易时段内, 时戳容忍分钟};
use crate::events::TickEvent;
use chrono::{Local, LocalResult, TimeZone, Timelike};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

// ============================================================
// 聚合：tick → 分钟K线
//   ★本版改动点 1：不再往"整天 cache"里攒，收盘即把 bar 发给落盘线程。
// ============================================================

// ════════════════ Rust 概念教学 ①：Option——「可能没有答案」的返回类型 ════════════════
// 判定重演 返回 Option<bool>，三个出口对应三种人生：
//   · Some(true)  = 保证金都被重演快照骗过去了：这是重演，丢！
//   · Some(false) = 时间戳正常：真行情，留！
//   · None        = 时间戳缺失/格式怪异：无法判断，宁可不丢（宁可漏放不误杀）。
//
// 为什么不用一个 bool 打天下？bool 表达不了「不知道」——硬把「不知道」塞成
// true 或 false 都会误杀真行情。Option 是 Rust 对「可能不存在」的官方表达，
// 调用方必须显式处理 None（不处理编译不过），隐患被消灭在编译期。
//
// 函数体里的 ? 是 Option 的专属捷径：update_time.get(0..2)? 表示「切片失败就
// 整个函数立刻返回 None」。一串 ? 连起来读起来像顺流而下，中途任何一步失败
// 都自动靠岸（返回 None），全程无 if 嵌套。
//
// 【与 Result 的区别】Result<Ok, Err> 带「失败原因」，适合要向上报告的 IO；
// Option 只区分「有/没有」，适合本例这种「判不出来就别管」的场景。
// ════════════════════════════════════════════════════════════════
/// ★v2.1 时戳偏离判定：交易所戳(UpdateTime) 与本机到达时刻相差超过容忍度 → 判为重演/陈旧快照。
/// 机理：CTP 重连/登录时，前置会重发每只合约的【最后一帧快照】——夜盘已收的品种（如铜 01:00 收盘）
/// 在 01:41 重连时被重发的快照，其交易所戳仍是收盘时刻（00:59），与本机到达时刻 01:41 相差 42 分钟。
/// 真行情的交易所戳与本机到达时刻偏差通常在亚秒级（交易所时钟同步），容忍 2 分钟已极宽松。
/// 跨午夜：比较用当日分钟数，差值取环形最短（1440 取模）。
/// 返回 None = 无法判定（update_time 缺失/格式异常），不丢（宁可漏放不误杀）。
fn 判定重演(update_time: &str, 到达_hhmm: i32, 容忍分钟: i32) -> Option<bool> {
    // "HH:MM:SS" 取 HH 与 MM。解析失败返回 None（不丢）。
    let b = update_time.as_bytes(); // 行内微注释：as_bytes 借出底层字节切片（不复制）——只看 ASCII 字符时按字节处理最快
    if b.len() < 5 {
        return None;
    }
    #[allow(
        clippy::indexing_slicing,
        reason = "b[2] 前已用 b.len() >= 5 保证索引合法；时间戳为 ASCII，字节读取安全"
    )]
    let 分隔符 = b[2]; // 行内微注释：b[2] 直接下标（本处已被上面 len>=5 担保安全）；b':' 是「字节字面量」写法，b 前缀把字符变成 u8
    if 分隔符 != b':' {
        return None;
    }
    let hh = update_time.get(0..2)?.parse::<i32>().ok()?; // 行内微注释：get(0..2) 是「安全切片」（越界返回 None 而不是 panic）；parse 把 "01" 变数字 1；ok() 把 Result 变 Option 接着 ?
    let mm = update_time.get(3..5)?.parse::<i32>().ok()?; // 行内微注释：一整行是三次「可能失败」的接力：切片 → 解析 → 验证，全用 ? 短路
    if !(0..24).contains(&hh) || !(0..60).contains(&mm) {
        return None;
    }
    let 戳分 = hh * 60 + mm;
    let 到分 = (到达_hhmm / 100) * 60 + (到达_hhmm % 100);
    let 环形差 = (戳分 - 到分).abs();
    let 环形差 = 环形差.min(24 * 60 - 环形差); // 跨午夜（如 23:59 vs 00:01）取环形最短
    Some(环形差 > 容忍分钟)
}

// ════════════════ Rust 概念教学 ②：derive(Serialize, Deserialize) 与 serde 三件套 ════════════════
// #[derive(Debug, Clone, Serialize, Deserialize)] 一行给 MinuteBar 装上四个能力：
//   · Debug：能 {:?} 打印（排查用）；
//   · Clone：能 .clone() 深拷贝（补缺口时要复制前一根 bar）；
//   · Serialize：能「序列化」成 JSON 行（writer.rs 写 jsonl 分片用）；
//   · Deserialize：能从 JSON 行「反序列化」回来（merge.rs 读回分片用）。
// 序列化类比「把家具拆平打包成纸箱」，反序列化是「按说明书把家具复原」；
// serde 是全 Rust 生态最流行的打包师傅，代码全部在编译期生成，没有运行时反射。
//
// 【字段上的三个 serde 属性（数据兼容三板斧）】
//   · rename = "bar_time"：JSON 里写的外名叫 bar_time，Rust 字段名叫 bar_time_ms——
//     「外号与学名不同」时用它对齐，本工程靠它统一了 jsonl 与 Parquet 的列名口径；
//   · alias = "bar_time_ms"：读回时也认老外号。老分片文件里存的还是旧名，
//     加了 alias 新代码照样能读老文件（向后兼容神器）；
//   · default：老文件里根本没有 volume_1m 这一列时，反序列化自动填 0——
//     缺列不报错。三招合起来：字段改名不炸、老文件能读、缺列能补。
//
// 【pub(crate)】字段在 crate 内可自由读写（agg/writer/merge 都要用），
// 但发布给外人时不可见——和 stats.rs 讲的一样，是「内部公开」。
// ════════════════════════════════════════════════════════════════
/// 一根收盘的分钟K线。派生 Serialize/Deserialize 是为了能逐行写成/读回 jsonl 分片。
/// 这里只存 CTP 给的【当日累计】volume/turnover；单分钟增量 `volume_1m/turnover_1m` 留到
/// 【合并时】按同合约时间相邻两根做差得到（合并时能看全一天，算增量最简单、也最好测）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinuteBar {
    pub(crate) instrument_id: String, // 合约代码（拥有所有权的 String，因为 bar 要离开聚合线程独立生活）
    /// ★列名口径与成品 Parquet 统一为 `bar_time`（此前 jsonl 写 bar_time_ms、成品写 bar_time，
    ///   下游按列名读会被坑）。alias 兼容 20261127 之前的老分片文件（bar_time_ms）。
    #[serde(rename = "bar_time", alias = "bar_time_ms")] // 行内微注释：rename 统一外号 + alias 兼容老文件，数据兼容双保险
    pub(crate) bar_time_ms: i64, // 分钟对齐后的毫秒时间戳（i64：64 位有符号整数，毫秒级时间戳不会溢出）
    pub(crate) trading_day: String,
    pub(crate) open: f64,
    pub(crate) high: f64,
    pub(crate) low: f64,
    pub(crate) close: f64,
    pub(crate) volume: u64,
    pub(crate) turnover: f64,
    pub(crate) open_interest: f64,
    /// ★单分钟增量 = 本根累计 - 上一根累计（同合约）。采集端不填，默认 0；【合并时】算好再写进成品。
    /// serde(default)：老 jsonl 没这两列也能正常反序列化（向前兼容）。
    #[serde(default)]
    pub(crate) volume_1m: u64, // 单分钟成交量增量（合并时才算，采集端恒 0）
    #[serde(default)]
    pub(crate) turnover_1m: f64, // 单分钟成交额增量（同上）
    /// true = 这根是合并时补出来的空分钟（无 tick），OHLC 前收、量额 0。
    pub(crate) is_gap: bool,
}

// Bucket（桶）与 MinuteBar 的分工：桶是「施工中」的可变累积器（只在聚合线程内存里），
// MinuteBar 是「竣工交付」的定稿（要跨线程、要落盘）。两个结构体长得像但职责不同——
// 不要为了省事共用一个：交付物多一个字段就多一份出错面，各管各的最稳。
struct Bucket {
    pub(crate) bar_ms: i64,
    pub(crate) open: f64,
    pub(crate) high: f64,
    pub(crate) low: f64,
    pub(crate) close: f64,
    pub(crate) volume: u64,
    pub(crate) turnover: f64,
    pub(crate) open_interest: f64,
    pub(crate) trading_day: String,
}

// 【规则15 零 unwrap + 锁中毒处理】
// 为什么不用 .lock().unwrap()：持锁线程若 panic，锁会被标记"中毒(poisoned)"，
// 之后 unwrap 会连带 panic，把"一个线程挂了"放大成"全体采集停摆"。
// 这里"中毒也照常取回数据"——统计计数丢失一点无所谓，采集不能停。

// ════════════════ Rust 概念教学 ③：const fn 与 rem_euclid ════════════════
// const fn floor_minute：const 前缀声明「这个函数可以在编译期执行」——
// 传给它编译期常量时，结果直接算进二进制里；运行期调用也没问题。收益不大
// 但免费。函数体一行：ms 减去「ms 对 60000 的非负余数」，把任意毫秒数向下
// 对齐到整分钟（例如 61_234 → 60_000）。
//
// 【rem_euclid vs %】Rust 的 % 对负数会返回负余数（-1 % 60 = -1），而
// rem_euclid 永远返回非负（-1.rem_euclid(60) = 59）——「欧几里得余数」。
// 时间戳对齐要的就是「永远向下取整」，用 rem_euclid 才不会在负数上翻车。
// 这是一个「同一种运算，两种语义，Rust 分成两个方法」的典型例子：
// 语言宁可名字长一点，也不让语义藏在符号里。
// ════════════════════════════════════════════════════════════════
const fn floor_minute(ms: i64) -> i64 {
    ms - ms.rem_euclid(MINUTE_MS)
}

pub fn ms_to_date(ms: i64) -> String {
    // 【规则15 零 unwrap】timestamp_millis_opt 返回 LocalResult 三态：
    //   Single  = 唯一本地时刻（中国无夏令时，99.999% 走这里）
    //   Ambiguous = 夏令时回拨重叠的那小时，取较早那个(dt)即可，日期一样
    //   None    = 毫秒越界，理论上 bar 时间不会出现；兜底按 UTC 取日期(同毫秒 UTC 必合法)
    // 任何分支都不 unwrap —— 就算真遇上 None 也只打 warn + 兜底，绝不 panic 拖垮采集。
    match Local.timestamp_millis_opt(ms) {
        LocalResult::Single(dt) | LocalResult::Ambiguous(dt, _) => dt.format("%Y%m%d").to_string(),
        LocalResult::None => {
            tracing::warn!("ms_to_date: {} 非本地合法时刻，兜底按 UTC 取日期", ms);
            chrono::Utc.timestamp_millis_opt(ms).single().map_or_else(
                || "19700101".to_string(),
                |dt| dt.format("%Y%m%d").to_string(),
            )
        }
    }
}

/// 聚合线程：和 v1.1 一样维护"每个活跃合约一个桶"，tick 进来更新桶，分钟切换时收盘。
/// 唯一区别：收盘的 bar 不再进 cache，而是【立刻 send 给落盘线程】。
#[expect(
    clippy::too_many_lines,
    reason = "聚合状态机（桶维护/收盘/兜底巡检/上报一体），v1.4 实盘形态，拆分需跨闭包传 buckets，回归风险大于收益；v1.6 SplitRS 一并处理"
)]
// ════════════════ Rust 概念教学 ④：mpsc 管道与 SyncSender 背压 ════════════════
// 聚合线程和落盘线程之间的「传送带」就是 mpsc 管道（多生产者、单消费者）。
//   · mpsc::Receiver<TickEvent>：收 tick 的接收端（借用 &，函数结束时自动归还）；
//   · mpsc::SyncSender<MinuteBar>：发 bar 的发送端——注意类型是 SyncSender
//     （有界同步通道），对应 main.rs 里的 mpsc::sync_channel(容量)。
//
// 【背压（backpressure）是什么】普通 channel 无界，生产太快会吃光内存；
// sync_channel 有容量上限，满了之后 send 会【阻塞】直到消费者腾出空位——
// 数据不丢、内存有顶，代价是生产者慢一拍。本工程宁可采集慢一拍，
// 也绝不静默丢行情，所以选有界通道。
//
// 【参数为什么一半借用一半拥有】&Receiver、&AtomicBool、&[(i32, i32)] 都是
// 「借用」：数据的主人（main.rs）活得比本线程久，借来用就行，不必复制。
// bar_tx 则【按值拿走所有权】：管道发送端唯一、用完要由聚合线程 drop
//（drop 时机就是「通道关闭」信号），必须拥有它。
// 这就是所有权的日常权衡：不拿走就用借用，要「负责到底」就按值接。
// ════════════════════════════════════════════════════════════════
pub fn run_aggregator(
    tick_rx: &mpsc::Receiver<TickEvent>, // 收 tick 的接收端（借用）
    bar_tx: mpsc::SyncSender<MinuteBar>, // 发 bar 的发送端（拿走所有权：退出时要 drop 它通知落盘线程收工）
    stop: &AtomicBool, // 全局停止旗标（原子布尔，借用）
    盘前过滤: bool, // 是否开启盘前重演过滤（配置开关）
    交易时段: &[(i32, i32)], // 交易时段表（借用；(i32, i32) = (起始HHMM, 结束HHMM)）
) {
    // ════════════════ Rust 概念教学 ⑤：HashMap——桶仓库与计数器 ════════════════
// HashMap<K, V> 是「按钥匙取物」的储物柜：本函数用两把——
//   · buckets: HashMap<String, Bucket>：合约代码 → 桶（每个活跃合约一个）；
//   · per_member_ticks: HashMap<usize, u64>：member 编号 → 累计 tick 数（统计日志用）。
// 三个高频方法（后面代码马上全用到）：
//   · get_mut(&k) → Option<&mut V>：拿到「可变借用」，能改值（更新桶）；
//   · insert(k, v)：放入或整个替换；remove(&k)：取走并删除（收盘时拿走桶）。
// 计数器的标准写法是 entry 惯用法（下一行就是）:
//   *map.entry(钥匙).or_default() += 1 —— 没有就先放个零值，然后自增 1。
// 【与 Vec 的选择题】要「按下标取」用 Vec，要「按名字取」用 HashMap。
// 合约代码是不连续的字符串，当然选 HashMap。
    let mut buckets: HashMap<String, Bucket> = HashMap::new();
    let mut total_done = 0u64; // 今日已收盘并送出的 bar 数（用于日志）
    let mut per_member_ticks: HashMap<usize, u64> = HashMap::new();
    let mut 盘前丢弃 = 0u64; // ★v2.0 过滤盘前重演：被丢的 tick 计数
    let mut 重演丢弃 = 0u64; // ★v2.1 时戳偏离丢弃：登录重演快照（带量陈旧 tick）计数
    let mut last_report = Instant::now();
    let mut last_tick_at = Instant::now();

    loop {
        match tick_rx.recv_timeout(Duration::from_millis(200)) {
            Ok(t) => {
                *per_member_ticks.entry(t.member_idx).or_default() += 1; // 行内微注释：entry 惯用法计数——无该项则先插 0，再 +1；* 解引用才能改到值
                last_tick_at = Instant::now();
                // 无效价过滤（与 v1.1 一致）：<=0 或超大视为脏数据丢弃。
                if t.last_price <= 0.0 || t.last_price >= 1e30 {
                    continue; // 行内微注释：continue 跳过本轮循环——脏数据直接扔，不影响其他逻辑
                }
                let now = Local::now();
                // ★v2.0 过滤盘前重演：非交易时段 且 当日累计成交量=0 的 tick 直接丢。
                // 治 h5i-db 里 00:57 volume=0 的结算/重演污染（配置_分钟K线.toml 过滤盘前重演）。
                let hhmm = i32::try_from(now.hour()).unwrap_or(0) * 100
                    + i32::try_from(now.minute()).unwrap_or(0); // 行内微注释：try_from 换类型可能失败（usize→i32 理论越界）→ 失败兜底 0，不用 panic 版 unwrap
                if 盘前过滤 && t.volume == 0 && !在交易时段内(hhmm, 交易时段) {
                    盘前丢弃 += 1;
                    continue;
                }
                // ★v2.1 过滤登录重演快照（带量的陈旧 tick）：交易所戳与本机到达时刻偏离超容忍度 → 丢。
                // 实测场景：01:41 重连时铜（夜盘 01:00 已收）被重发的快照被当成真行情产了假 bar。
                // 无法判定（update_time 缺失/格式异常）时不丢，宁可漏放不误杀。
                if 盘前过滤
                    && let Some(true) = 判定重演(&t.update_time, hhmm, 时戳容忍分钟)
                {
                    重演丢弃 += 1;
                    continue;
                }
                // 用收包本机时间取整到分钟（与 v1.1 一致，规避盘前重演污染）。
                let bar_ms = floor_minute(now.timestamp_millis()); // 行内微注释：timestamp_millis 是「自 1970 年的毫秒数」，再 floor 对齐到整分钟
                close_if_switching(
                    &t.instrument,
                    bar_ms,
                    &mut buckets,
                    &mut total_done,
                    &bar_tx,
                );
                upsert_bucket(&t, bar_ms, &mut buckets); // 行内微注释：&t 借用 tick（读它的字段），&mut buckets 借出桶仓库（要写入）
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }

        // 兜底巡检：冷门合约长时间无 tick 时，靠它把停在旧分钟的桶收盘（与 v1.1 一致）。
        if last_tick_at.elapsed() >= Duration::from_secs(60)
            || last_report.elapsed() >= Duration::from_secs(REPORT_INTERVAL_SEC)
        {
            let now_ms = floor_minute(Local::now().timestamp_millis());
            // ════════════════ Rust 概念教学 ⑥：迭代器链与 filter_map ════════════════
            // 下面这段「挑出过期的桶名」是迭代器链的教科书：
            //   buckets.iter()           逐个借出 (&钥匙, &桶)；
            //   .filter_map(|…| …)      过滤加变换二合一：闭包返回 None 的被扔掉，
            //                            返回 Some(x) 的把 x 留下（这里留下桶名副本）；
            //   .collect()               把流水线产物收进 Vec<String>。
            //
            // 【为什么要先收集 stale 再收盘，不能边遍历边 remove？】因为 HashMap 的
            // 借用规则：iter() 期间整张表都被借走了，此时调 close_bucket（要 &mut）
            // 编译器直接拒绝。先 clone 出「要删的名字列表」，遍历结束后再动手——
            // 这是 Rust 里「先收集、再修改」的经典套路，也是新手必踩的坑。
            //
            // 【collect 的类型魔法】collect 能收成 Vec、HashMap、String 等各种容器，
            // 目标类型由变量类型标注决定（这里写了 Vec<String>）——「由容器决定收集」。
            // ════════════════════════════════════════════════════════════════
            let stale: Vec<String> = buckets
                .iter()
                .filter_map(|(k, b)| {
                    if b.bar_ms == now_ms {
                        None // 还是当前分钟：新鲜，不收
                    } else {
                        Some(k.clone()) // 停在旧分钟：过期，记下名字（clone 因为等会要离开借用）
                    }
                })
                .collect();
            for inst in stale {
                close_bucket(&inst, &mut buckets, &mut total_done, &bar_tx);
            }
        }

        if last_report.elapsed() >= Duration::from_secs(REPORT_INTERVAL_SEC) {
            let mut dist: Vec<(usize, u64)> =
                per_member_ticks.iter().map(|(&k, &v)| (k, v)).collect(); // 行内微注释：map 把 (&钥匙, &值) 复制成 (钥匙, 值) 元组——模式 |(&k, &v)| 顺手解引用
            dist.sort_unstable_by_key(|&(k, _)| k); // 行内微注释：按 member 编号排序；unstable 版更快（不保相等元素原序，计数排序无所谓）
            let dist_str: String = dist
                .iter()
                .map(|(k, v)| format!("MD#{k}:{v}"))
                .collect::<Vec<_>>() // 行内微注释：collect::<Vec<_>>() 的 ::<…> 写法叫「涡轮鱼」，就地指定收集成 Vec 而不改外面的变量类型
                .join(" ");
            tracing::info!(
                "[AGG] 已落盘 {} 根 | 活跃桶 {} | 内存常驻桶 {}（不再攒整天，内存恒定）| 盘前重演丢弃 {} | 时戳偏离丢弃 {} | 各member tick [{}]",
                total_done,
                buckets.len(),
                buckets.len(),
                盘前丢弃,
                重演丢弃,
                dist_str
            );
            last_report = Instant::now();
        }

        if stop.load(Ordering::Relaxed) {
            tracing::info!(
                "[AGG] 收到退出信号，开始收尾：把活跃桶全部收盘送落盘（当天已在分片里，几乎秒完）"
            );
            break;
        }
    }

    // 收尾：把所有还开着的桶收盘送出，然后 drop(bar_tx) 通知落盘线程"没数据了，可以 fsync 收尾退出"。
    // drain() 把整张表「连钥匙带值」逐项取走（取完表为空）——比 iter+remove 高效且合法。
    let drain: Vec<(String, Bucket)> = buckets.drain().collect();
    for (inst, b) in drain {
        send_bar(
            MinuteBar {
                instrument_id: inst,
                bar_time_ms: b.bar_ms,
                trading_day: b.trading_day,
                open: b.open,
                high: b.high,
                low: b.low,
                close: b.close,
                volume: b.volume,
                turnover: b.turnover,
                open_interest: b.open_interest,
                volume_1m: 0, // 采集端不算增量，留到合并
                turnover_1m: 0.0,
                is_gap: false,
            },
            &mut total_done,
            &bar_tx,
        );
    }
    drop(bar_tx);
    tracing::info!(
        "[AGG] 聚合线程退出 | 共收盘 {} 根（已全部追加进当天分片，崩溃最多丢最后 {} 秒）",
        total_done,
        分片落盘秒
    );
}

fn close_if_switching(
    inst: &str,
    bar_ms: i64,
    buckets: &mut HashMap<String, Bucket>,
    total_done: &mut u64,
    bar_tx: &mpsc::SyncSender<MinuteBar>,
) {
    let switch = matches!(buckets.get(inst), Some(b) if b.bar_ms != bar_ms);
    if switch {
        close_bucket(inst, buckets, total_done, bar_tx);
    }
}

/// 收盘一个合约的当前桶：取出桶 → 组装 `MinuteBar` → 立刻发给落盘线程。
/// 对比 v1.1：这里原本是 push 进 cache 等退出才写；现在当场就送走，内存里不留整天。
fn close_bucket(
    inst: &str,
    buckets: &mut HashMap<String, Bucket>,
    total_done: &mut u64,
    bar_tx: &mpsc::SyncSender<MinuteBar>,
) {
    if let Some(b) = buckets.remove(inst) {
        send_bar(
            MinuteBar {
                instrument_id: inst.to_string(),
                bar_time_ms: b.bar_ms,
                trading_day: b.trading_day,
                open: b.open,
                high: b.high,
                low: b.low,
                close: b.close,
                volume: b.volume,
                turnover: b.turnover,
                open_interest: b.open_interest,
                volume_1m: 0,
                turnover_1m: 0.0,
                is_gap: false,
            },
            total_done,
            bar_tx,
        );
    }
}

/// 把一根 bar 送入落盘队列。队列满会【阻塞等】而不是丢——宁可采集慢一拍，也不静默丢行情。
fn send_bar(bar: MinuteBar, total_done: &mut u64, bar_tx: &mpsc::SyncSender<MinuteBar>) {
    // send 在队列满时阻塞（背压）；对端断开（落盘线程已退）时返回 Err，打日志不 panic。
    // 行内微注释：bar 按值传进来（所有权交给 send）——塞进管道后数据就属于管道了，这是所有权的「交接」。
    if bar_tx.send(bar).is_ok() {
        *total_done += 1; // 行内微注释：total_done 是 &mut u64 可变借用，*解引用后才能自增
    } else {
        tracing::error!("[AGG] 落盘线程已退出，bar 无处可送（本根丢弃，属异常需排查）");
    }
}

// ════════════════ Rust 概念教学 ⑦：get_mut 与「有则改、无则建」 ════════════════
// upsert_bucket（update+insert 的合成词）处理两种情况：桶在（更新）或不在（新建）。
//   · buckets.get_mut(&k) 返回 Option<&mut Bucket>：拿到【可变借用】才能改桶内字段；
//   · match 拆包后 Some(b) 分支直接 b.high = … 写入——可变借用期间这张表
//     属于你，别人动不了（编译器担保）；
//   · None 分支用 buckets.insert(新桶) 建桶。
// 注意 insert 的 key 是 t.instrument.clone()：HashMap 要【拥有】钥匙；
// 而上面我们只是借用 t，所以必须复制一份 String——「借用看着省事，
// 放进容器时还是要交所有权」，这是新手最常见的 Rust 心理落差。
// ════════════════════════════════════════════════════════════════
fn upsert_bucket(t: &TickEvent, bar_ms: i64, buckets: &mut HashMap<String, Bucket>) {
    match buckets.get_mut(&t.instrument) {
        Some(b) => {
            if t.last_price > b.high {
                b.high = t.last_price;
            }
            if t.last_price < b.low {
                b.low = t.last_price;
            }
            b.close = t.last_price;
            // 累计量/额/持仓直接用最新值覆盖（CTP 给的是当日累计）。
            // 重连后柜台若重发更小值（极少见），取 max 防止回退。
            if t.volume > b.volume {
                b.volume = t.volume;
            }
            if t.turnover > b.turnover {
                b.turnover = t.turnover;
            }
            b.open_interest = t.open_interest;
            if !t.trading_day.is_empty() {
                b.trading_day.clone_from(&t.trading_day); // 行内微注释：clone_from 等价于 = x.clone()，但复用已有 String 的内存不重新分配——微优化
            }
        }
        None => {
            buckets.insert(
                t.instrument.clone(), // 行内微注释：钥匙必须拥有所有权，所以 clone；值（下面的 Bucket）直接现场构造
                Bucket {
                    bar_ms,
                    open: t.last_price,
                    high: t.last_price,
                    low: t.last_price,
                    close: t.last_price,
                    volume: t.volume,
                    turnover: t.turnover,
                    open_interest: t.open_interest,
                    trading_day: t.trading_day.clone(),
                },
            );
        }
    }
}

// ════════════════ Rust 概念教学 ⑧：#[cfg(test)] 单元测试写法 ════════════════
// #[cfg(test)] 读作「仅在编译目标含 test 时编译这一段」——cargo test 才编它，
// cargo build 直接无视。测试模块通常叫 mod 测试（惯例名 tests），里面的
// use super::*; 表示「把父模块（本文件）的东西都引进来」，否则看不见 判定重演。
//
// 每个测试函数上标 #[test]；断言用宏：
//   · assert!(表达式)              表达式必须为真；
//   · assert_eq!(a, b)             两者必须相等（失败会打印两边的值）；
//   · assert_eq!(a, b, "说明")     失败时的自定义说明。
// 测试失败只是该测试挂掉，其他测试继续跑——和 panic 生产环境的行为完全不同。
//
// 本文件测试的亮点是「边界值思维」：01:41 重连重演 00:59 的快照、23:59 vs
// 00:01 的跨午夜环形最短差、格式坏串的 None 兜底——把业务上最疼的场景
// 钉死成代码，谁改坏一眼看出。写测试就学这个思路：正常例一两根即可，
// 边界例与异常例才是测试的价值所在。
// ════════════════════════════════════════════════════════════════
#[cfg(test)]
mod 测试 {
    use super::*;

    #[test]
    fn floor分钟对齐() {
        assert_eq!(floor_minute(61_234), 60_000);
        assert_eq!(floor_minute(0), 0);
        assert_eq!(floor_minute(119_999), 60_000);
    }

    #[test]
    fn 重演判定_真行情不丢() {
        // 真行情：交易所戳与本机到达时刻差 < 2 分钟 → 不判重演
        assert_eq!(判定重演("01:33:05", 133, 2), Some(false));
        assert_eq!(判定重演("21:00:00", 2100, 2), Some(false));
        assert_eq!(判定重演("21:01:30", 2100, 2), Some(false), "差1分30秒=90秒边界内");
        assert_eq!(判定重演("21:03:00", 2100, 2), Some(true), "差3分钟超容忍");
    }

    #[test]
    fn 重演判定_登录重演快照必丢() {
        // 实测场景复现：01:41 重连，铜（夜盘 01:00 收盘）重发快照交易所戳 00:59 → 偏离 42 分钟 → 丢
        assert_eq!(判定重演("00:59:59", 141, 2), Some(true));
        // 盘前重演：20:58 连上，白天天盘结算快照戳 15:00 → 丢
        assert_eq!(判定重演("15:00:00", 2058, 2), Some(true));
        // 中金所重演：凌晨连上，股指戳 15:15 → 丢
        assert_eq!(判定重演("15:15:00", 141, 2), Some(true));
    }

    #[test]
    fn 重演判定_跨午夜环形取最短() {
        // 23:59 的戳 vs 00:01 到达：线性差 1438 分，环形差 2 分 → 不丢
        assert_eq!(判定重演("23:59:00", 1, 2), Some(false));
        // 00:01 戳 vs 23:59 到达同理
        assert_eq!(判定重演("00:01:00", 2359, 2), Some(false));
        // 环形差 4 分 → 丢
        assert_eq!(判定重演("23:57:00", 1, 2), Some(true));
    }

    #[test]
    fn 重演判定_无法判定不误杀() {
        // update_time 缺失/格式异常 → None，调用方不丢
        assert_eq!(判定重演("", 133, 2), None);
        assert_eq!(判定重演("abc", 133, 2), None);
        assert_eq!(判定重演("0133:05", 133, 2), None, "分隔符位置不对");
        assert_eq!(判定重演("99:99:99", 133, 2), None, "小时/分钟越界");
    }
}

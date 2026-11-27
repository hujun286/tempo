// ╔══════════════════════════════════════════════════════════════════╗
// ║ 【文件头教学导读】writer.rs —— 落盘线程：「只增不改」的记账本             ║
// ╚══════════════════════════════════════════════════════════════════╝
//
// 每根分钟K线收完盘，聚合线程会把它塞进管道；落盘线程（就是本文件的
// run_writer）守在管道另一头，把每根 bar 追加成一【行 JSON】写进当天的
// 分片文件 data_数据/分钟分片/bars_YYYYMMDD.jsonl，并每 60 秒 fsync 一次。
//
// 为什么这么设计（防丢两件套）：
//   · append（追加）：只往后写，绝不回头改——就算中途断电，写好的部分完好；
//   · fsync（强制落盘）：flush 只是「交给操作系统内存缓冲」，fsync 才是
//     「盯着操作系统真写到物理盘上」。两者都做，崩溃最多丢 60 秒。
//
// 【它在整条链路里的位置】CTP连接 → tick → 分钟K线 → jsonl分片 → Parquet合并。
// 本文件就是「jsonl分片」那一环：把内存里的 bar 变成磁盘上的行。
//
// 【本文件用到的 Rust 核心概念】
//   · Option<(String, BufWriter<File>)>：用 Option 持有「当前打开的文件」状态；
//   · take()：把 Option 里的值拿出来、原位留 None（所有权转移的常见手法）；
//   · BufWriter + Write trait：缓冲写入与 trait 方法（writeln! 宏）；
//   · serde_json::to_string：结构体 → JSON 字符串（serde 序列化）；
//   · 闭包（打开）：被当「局部小函数」用的匿名函数；
//   · while let Ok(bar) = rx.recv()：管道收货的标准循环（通道关闭自动收尾）。
//
// 【建议学习时长】20 到 30 分钟。

//! 落盘线程：逐根 bar 追加 jsonl + 定时 fsync。
use crate::aggregate::{MinuteBar, ms_to_date}; // bar 结构体 + 毫秒转日期的工具函数
use std::io::{BufWriter, Write}; // BufWriter 带缓冲的写入器；Write 是「能写」的能力接口（trait）
use std::path::Path;
use std::sync::mpsc;
use std::time::{Duration, Instant};

// ============================================================
// ★本版改动点 2：落盘线程 —— 从"写整日 Parquet"改成"追加 JSON 行 + 定时 fsync"
// ============================================================

/// 落盘线程：把每根 bar 追加成一行 JSON 到当天的分片文件，每 分片落盘秒 fsync 一次。
/// 崩溃安全的关键：append 保证只增不改；fsync 保证"真的落到物理盘"而非只在内存缓冲。
/// 即使进程被 kill -9，最多丢最后 分片落盘秒 内没 fsync 的那几根，其余全在盘上。
// 行内微注释：三个参数——rx 是管道接收端（借用）；分片目录是写往哪里；分片落盘秒_生效
// 是 fsync 间隔（main 已按值拷出）。前两个 & 都是「借用」：只借不拿，主线程才是主人。
pub fn run_writer(
    rx: &mpsc::Receiver<MinuteBar>, 分片目录: &Path, 分片落盘秒_生效: u64
) {
    std::fs::create_dir_all(分片目录).ok(); // 目录不存在就建；ok() 把 Result 变 Option 丢弃——建不了后面打开文件会再报错，不在此处惊慌
    // ════════════════ Rust 概念教学 ①：用 Option 持有「当前状态」 ════════════════
    // `当前: Option<(String, BufWriter<File>)>` 是本函数的状态核心：
    //   · None  = 还没开过文件 / 刚才打开失败了；
    //   · Some((日期串, 写入器)) = 正在往这个日期的分片文件写。
    //
    // 【为什么要 Option 包一层】Rust 没有「空指针」，表达「还没有文件」只能用
    // Option。每次用时都要显式拆包（match / if let），编译器强迫你处理「没开文件
    // 就想写」的路径——这类 bug 在 C/Go 里是运行时才炸，在 Rust 是编译期拦截。
    //
    // 【元组 (A, B)】元组是「打包箱」：把两个值捆在一起传递，不必为它专门定义
    // 结构体。这里 (日期, 写入器) 天生一对，捆着走最自然。
    //
    // 【BufWriter<File> 套娃】File 是「文件句柄」，BufWriter 在外面套一层内存
    // 缓冲：小笔写入先攒在缓冲里，攒多了才一次性交给操作系统——就像挤牙膏
    // 先攒一管再挤，减少系统调用次数。BufWriter 在被 drop 时会自动 flush 一次
    //（但崩溃时缓冲里没 flush 的数据就丢了，所以还需要定时 fsync 兜底）。
    // ════════════════════════════════════════════════════════════════
    // 当前打开的分片文件：(日期串, 带缓冲的写入器)。日期一变就换文件（跨自然日）。
    let mut 当前: Option<(String, BufWriter<std::fs::File>)> = None;
    let mut 本次累计 = 0u64; // 本次运行已写多少根 bar（日志用）
    let mut 文件数 = 0u64; // 本次运行开了几个分片文件
    let mut 上次落盘 = Instant::now(); // 上次 fsync 的时刻（计时器）

    // ════════════════ Rust 概念教学 ②：闭包当「局部小函数」用 ════════════════
    // `打开` 是一个闭包（匿名函数），绑定到变量名，之后像函数一样调用：
    // 打开(&date, 分片目录)。它比 fn 更灵活——可以捕获外部变量；它比写在
    // 外面的 fn 更贴近使用点，一眼看到「这个辅助逻辑只在这里用」。
    //
    // 【签名三件套】|date: &str, 目录: &Path| -> std::io::Result<BufWriter<…>>
    //   · 两条竖线是参数列表；
    //   · 返回值是 io::Result：文件打开可能失败（权限/磁盘满），失败原样上交；
    //   · 闭包体最后一条表达式就是返回值（没有分号的 Ok(BufWriter::new(f))）。
    //
    // 【OpenOptions 链式配置】create(true).append(true)：文件不存在就建、
    // 写入位置挪到文件末尾（追加模式）。std::fs::OpenOptions 用「建造者模式」：
    // 一串方法链最后 .open() 收口——Rust 标准库到处是这种风格。
    // ════════════════════════════════════════════════════════════════
    // 打开（或追加到）某一天的分片文件。
    let 打开 = |date: &str, 目录: &Path| -> std::io::Result<BufWriter<std::fs::File>> {
        let p = 目录.join(format!("bars_{date}.jsonl")); // 拼出文件名：bars_20261127.jsonl（format 里的 {date} 是内联插值）
        let f = std::fs::OpenOptions::new()
            .create(true)
            .append(true) // ★追加模式：只往后写，绝不覆盖已有数据
            .open(&p)?; // ? 打开失败（权限/磁盘）就把 Err 交给调用方
        Ok(BufWriter::new(f)) // 给文件句柄套上缓冲层
    };

    // ════════════════ Rust 概念教学 ③：while let 与「通道断开即收工」 ════════════════
    // `while let Ok(bar) = rx.recv()` 读作：只要还能收到东西就一直循环。
    // rx.recv() 返回 Result<MinuteBar, RecvError>：
    //   · Ok(bar)   = 收到一根 bar；
    //   · Err(…)    = 管道断了。什么时候断？【所有发送端都被 drop】。
    //     聚合线程退出前 drop(bar_tx)，落盘线程的 recv() 立刻返回 Err，
    //     while let 不匹配 → 循环自然结束 → 走到下面的收尾逻辑。
    // 这就是 Rust 线程协作的优雅之处：不需要「通知落盘线程退出」的额外消息，
    // 「发送端全部消失」本身就是结束信号（所有权系统的天然副产品）。
    // ════════════════════════════════════════════════════════════════
    while let Ok(bar) = rx.recv() {
        let date = ms_to_date(bar.bar_time_ms); // 从 bar 的毫秒时间戳算出日期串（决定写进哪个分片文件）
        // 需要换文件？（首次 / 跨日）
        let 需要换 = match &当前 {
            Some((d, _)) => d != &date, // 已经开着别的日期的文件 → 要换
            None => true, // 手上没文件 → 要开
        }; // 行内微注释：match 借用 &当前（只借不改），元组解构 (d, _) 用 _ 忽略写入器
        if 需要换 {
            // 先把旧文件 flush + fsync 收好，再开新的。
            // ════════════════ Rust 概念教学 ④：take() 与所有权交接 ════════════════
            // 当前.take() 做两件事：把 Some 里的值【完整拿走】交给左侧，
            // 同时把 当前 置回 None。这就是 Option 的「取货并清空」手法——
            // 因为 Rust 的值不能「边借边还」（不能既把写入器交出去又被 当前 持有），
            // take() 是标准解法。
            //
            // if let Some((旧日期, mut w)) = …：拿到手再逐件用——旧日期用来打日志，
            // w 用来 flush；flush 之后 w.get_ref() 借出内部的 File 调 sync_all()
            //（fsync）。get_ref() 是 BufWriter 的「透视窗」：不动缓冲层、直接看底层文件。
            // ════════════════════════════════════════════════════════════════
            if let Some((旧日期, mut w)) = 当前.take() {
                let _ = w.flush(); // flush 失败也无法补救，忽略（随后 fsync 还有一道防线）
                if let Err(e) = w.get_ref().sync_all() {
                    tracing::error!("[WRT] fsync {} 失败: {}", 旧日期, e); // fsync 失败要喊出来：这是「数据可能没真落盘」的信号
                }
                tracing::info!("[WRT] 收好分片 {}（累计 {} 根）", 旧日期, 本次累计);
            }
            match 打开(&date, 分片目录) {
                Ok(w) => {
                    当前 = Some((date.clone(), w)); // 日期串 clone 一份存起来（原 date 后面还要用）
                    文件数 += 1;
                    tracing::info!(
                        "[WRT] 打开分片 {} → {}",
                        date,
                        分片目录.join(format!("bars_{date}.jsonl")).display() // display() 把 Path 转成可打印的东西
                    );
                }
                Err(e) => {
                    tracing::error!("[WRT] 打开分片 {} 失败: {}", date, e);
                    当前 = None; // 打不开就把状态清空：下一根 bar 再试
                    continue; // 行内微注释：这根 bar 丢了但要活下来——落盘不能因为一次打不开文件就整线程崩溃
                }
            }
        }

        // 写一行 JSON（serde 序列化）。写失败只报错不退出，尽量多保数据。
        // ════════════════ Rust 概念教学 ⑤：serde 序列化——结构体变 JSON ════════════════
        // serde_json::to_string(&bar) 把 MinuteBar 一口气变成一行 JSON 文本：
        //   {"bar_time":1732680000000,"instrument_id":"IF2401",…}
        // 能这样做靠的是 MinuteBar 头上的 #[derive(Serialize)]（见 aggregate.rs）：
        // derive 在编译期替你生成「逐字段怎么写进 JSON」的代码，零运行时反射。
        //
        // 【返回 Result】序列化也可能失败（极少见，如浮点 NaN），所以返回
        // Result<String>——这里 match 三层拆开：序列化成功 → 写行；写行成功 →
        // 计数加一；任何一步失败 → 只打日志不 panic，尽量多保数据。
        //
        // 【写行用 writeln!(w, "{line}")】writeln! 是宏（编译期展开的代码模板），
        // 比 w.write_all 手动拼 \n 方便；宏名带 ! 是它和普通函数的显眼区别。
        // ════════════════════════════════════════════════════════════════
        if let Some((_, w)) = 当前.as_mut() {
            match serde_json::to_string(&bar) {
                Ok(line) => {
                    if let Err(e) = writeln!(w, "{line}") {
                        tracing::error!("[WRT] 写行失败: {}", e);
                    } else {
                        本次累计 += 1;
                    }
                }
                Err(e) => tracing::error!("[WRT] 序列化 bar 失败: {}", e),
            }
        } // 行内微注释：as_mut() 借出 Option 里的内容做【可变借用】（要往里写），而不是 take 走所有权——文件还得继续用

        // 定时 flush + fsync：把缓冲刷到内核(flush)，再刷到物理盘(fsync)，控制崩溃丢失上限。
        if 上次落盘.elapsed() >= Duration::from_secs(分片落盘秒_生效) {
            if let Some((_, w)) = 当前.as_mut() {
                let _ = w.flush(); // 第一步：BufWriter 的用户态缓冲 → 操作系统内核缓冲
                if let Err(e) = w.get_ref().sync_all() {
                    tracing::error!("[WRT] 定时 fsync 失败: {}", e); // 第二步：内核缓冲 → 物理盘；这一步才是「崩溃也不丢」的保证
                }
            }
            上次落盘 = Instant::now(); // 重置计时器
        }
    }

    // rx 断开（聚合线程退出、bar_tx 全 drop）→ 最后一次 flush + fsync，收尾。
    if let Some((日期, mut w)) = 当前.take() {
        let _ = w.flush();
        if let Err(e) = w.get_ref().sync_all() {
            tracing::error!("[WRT] 收尾 fsync {} 失败: {}", 日期, e);
        }
        tracing::info!("[WRT] 收好分片 {}（累计 {} 根）", 日期, 本次累计);
    }
    tracing::info!(
        "[WRT] 落盘线程退出 | 共 {} 个分片文件、{} 根 bar 已安全落盘（fsync 每 {} 秒一次）",
        文件数,
        本次累计,
        分片落盘秒_生效
    );
}

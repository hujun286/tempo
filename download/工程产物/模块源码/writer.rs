//! 落盘线程：逐根 bar 追加 jsonl + 定时 fsync。
use crate::aggregate::{MinuteBar, ms_to_date};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::{Duration, Instant};

// ============================================================
// ★本版改动点 2：落盘线程 —— 从"写整日 Parquet"改成"追加 JSON 行 + 定时 fsync"
// ============================================================

/// 落盘线程：把每根 bar 追加成一行 JSON 到当天的分片文件，每 分片落盘秒 fsync 一次。
/// 崩溃安全的关键：append 保证只增不改；fsync 保证"真的落到物理盘"而非只在内存缓冲。
/// 即使进程被 kill -9，最多丢最后 分片落盘秒 内没 fsync 的那几根，其余全在盘上。
pub(crate) fn run_writer(
    rx: mpsc::Receiver<MinuteBar>, 分片目录: PathBuf, 分片落盘秒_生效: u64
) {
    std::fs::create_dir_all(&分片目录).ok();
    // 当前打开的分片文件：(日期串, 带缓冲的写入器)。日期一变就换文件（跨自然日）。
    let mut 当前: Option<(String, BufWriter<std::fs::File>)> = None;
    let mut 本次累计 = 0u64;
    let mut 文件数 = 0u64;
    let mut 上次落盘 = Instant::now();

    // 打开（或追加到）某一天的分片文件。
    let 打开 = |date: &str, 目录: &Path| -> std::io::Result<BufWriter<std::fs::File>> {
        let p = 目录.join(format!("bars_{}.jsonl", date));
        let f = std::fs::OpenOptions::new()
            .create(true)
            .append(true) // ★追加模式：只往后写，绝不覆盖已有数据
            .open(&p)?;
        Ok(BufWriter::new(f))
    };

    while let Ok(bar) = rx.recv() {
        let date = ms_to_date(bar.bar_time_ms);
        // 需要换文件？（首次 / 跨日）
        let 需要换 = match &当前 {
            Some((d, _)) => d != &date,
            None => true,
        };
        if 需要换 {
            // 先把旧文件 flush + fsync 收好，再开新的。
            if let Some((旧日期, mut w)) = 当前.take() {
                let _ = w.flush();
                if let Err(e) = w.get_ref().sync_all() {
                    tracing::error!("[WRT] fsync {} 失败: {}", 旧日期, e);
                }
                tracing::info!("[WRT] 收好分片 {}（累计 {} 根）", 旧日期, 本次累计);
            }
            match 打开(&date, &分片目录) {
                Ok(w) => {
                    当前 = Some((date.clone(), w));
                    文件数 += 1;
                    tracing::info!(
                        "[WRT] 打开分片 {} → {}",
                        date,
                        分片目录.join(format!("bars_{}.jsonl", date)).display()
                    );
                }
                Err(e) => {
                    tracing::error!("[WRT] 打开分片 {} 失败: {}", date, e);
                    当前 = None;
                    continue;
                }
            }
        }

        // 写一行 JSON（serde 序列化）。写失败只报错不退出，尽量多保数据。
        if let Some((_, w)) = 当前.as_mut() {
            match serde_json::to_string(&bar) {
                Ok(line) => {
                    if let Err(e) = writeln!(w, "{}", line) {
                        tracing::error!("[WRT] 写行失败: {}", e);
                    } else {
                        本次累计 += 1;
                    }
                }
                Err(e) => tracing::error!("[WRT] 序列化 bar 失败: {}", e),
            }
        }

        // 定时 flush + fsync：把缓冲刷到内核(flush)，再刷到物理盘(fsync)，控制崩溃丢失上限。
        if 上次落盘.elapsed() >= Duration::from_secs(分片落盘秒_生效) {
            if let Some((_, w)) = 当前.as_mut() {
                let _ = w.flush();
                if let Err(e) = w.get_ref().sync_all() {
                    tracing::error!("[WRT] 定时 fsync 失败: {}", e);
                }
            }
            上次落盘 = Instant::now();
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

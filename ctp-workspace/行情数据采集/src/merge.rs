// ── 宪法豁免（带理由）：clippy::indexing_slicing ─────────────────────────
// 本文件是离线合并器（非实盘热路径）：输入为自有 jsonl 分片格式（格式由 writer
// 生成，字段边界自洽），窗口排序/增量计算/补缺口测试的下标均由构造保证。
// 沿用「v1.4 与 v1.1 逐字一致，改动=引入风险」工程原则，整文件显式豁免。
#![allow(
    clippy::indexing_slicing,
    reason = "离线合并器+自有格式边界自洽；v1.4 逐字一致原则，测试与主体均由构造保证不越界"
)]
//! 合并：jsonl 分片 → 排序 → 算增量 → 补缺口 → 13 列 Parquet（含 ★v1.5 归档压缩）。
use crate::aggregate::MinuteBar;
use crate::config::MINUTE_MS;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::Path;
use std::time::Duration;

// ============================================================
// ★本版改动点 3：合并模式 —— 读 jsonl 分片 → 排序 → 算增量 → 补缺口 → 写整日 Parquet
// ============================================================

#[derive(Default)]
struct GapStats {
    gap_bars: u64,
    instruments_with_gap: u64,
}

/// 合并某一天的分片为成品 Parquet。
/// 步骤（每步都可单独看日志核验）：
///   1) 把当天的 `bars_YYYYMMDD.jsonl` 复制成 .merging 临时文件再读 —— 这样采集进程还在
///      往原文件追加也不影响合并（读的是快照），盘中随时可合并。
///   2) 逐行解析 JSON；某一行坏了（很可能是正在写的最后半行）就跳过并告警，不整批失败。
///   3) 按合约分组，组内按时间升序，算 `volume_1m` / `turnover_1m` = 相邻两根累计之差。
///      （首根增量=累计本身；补出来的缺口 bar 增量=0）
///   4) 补缺口：同合约相邻两根若隔了 >1 分钟，中间每分钟补一根 `is_gap=true` 的空 bar，
///      OHLC 用前收、量额 0、累计量沿用前一根（保持累计列单调，比 v1.1 补 0 更友好）。
///   5) 原子写 `bars_YYYYMMDD.parquet`（`.tmp` → rename），13 列，下游照旧 `read_parquet`。
#[expect(
    clippy::too_many_lines,
    reason = "合并管道五步一体（读/排序/增量/补缺口/原子写），拆分需传递中间态，v1.4 逐字一致原则，拆分收益小于回归风险"
)]
pub fn 合并一日(
    date: &str,
    分片目录: &Path,
    成品目录: &Path,
    同日合并: bool,
    双写增量: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // ★v1.5：分片支持 .jsonl 与 .jsonl.zst 两种源（归档后的历史日期重新合并仍可用，D028）
    let jsonl = 分片目录.join(format!("bars_{date}.jsonl"));
    let zst = 分片目录.join(format!("bars_{date}.jsonl.zst"));
    let 源 = if jsonl.exists() {
        jsonl
    } else if zst.exists() {
        zst
    } else {
        return Err(std::io::Error::other(format!(
            "找不到分片文件 {} 或 {}（这天没采到数据？）",
            jsonl.display(),
            zst.display()
        ))
        .into());
    };
    let 是zst = 源.extension() == Some(std::ffi::OsStr::new("zst"));
    // 1) 复制快照再读（.zst 先解压写快照，同样不影响原件）
    let 快照 = 分片目录.join(format!(".merging_bars_{date}.jsonl"));
    if 是zst {
        let 压缩数据 = std::fs::read(&源)?;
        let 解开 = zstd::stream::decode_all(&压缩数据[..])?;
        std::fs::write(&快照, &解开)?;
        tracing::info!(
            "[MERGE] {} 是归档分片，已解压 {}B → 快照",
            源.display(),
            解开.len()
        );
    } else {
        std::fs::copy(&源, &快照)?;
    }
    let raw = std::fs::read_to_string(&快照)?;

    // 2) 逐行解析
    let mut bars: Vec<MinuteBar> = Vec::new();
    let mut 坏行 = 0u64;
    for (i, line) in raw.lines().enumerate() {
        let l = line.trim();
        if l.is_empty() {
            continue;
        }
        match serde_json::from_str::<MinuteBar>(l) {
            Ok(b) => bars.push(b),
            Err(e) => {
                坏行 += 1;
                // 只有最后一行才可能是"正在写的半行"，中间行坏了是真问题，重点提示。
                tracing::error!(
                    "[MERGE] 跳过第 {} 行（解析失败，多为文件末尾半行）: {}",
                    i + 1,
                    e
                );
            }
        }
    }
    let _ = std::fs::remove_file(&快照); // 读完删快照

    if bars.is_empty() {
        return Err(
            std::io::Error::other(format!("{date} 这天解析出 0 根有效 bar，不写空文件")).into(),
        );
    }
    tracing::info!(
        "[MERGE] {} 读到 {} 根有效 bar（跳过坏行 {}）",
        date,
        bars.len(),
        坏行
    );

    // ★v2.0 同日合并（D039 补实装）：成品已存在时读回旧 bar 一并合并 —— 重复合并不再丢旧数据。
    let 成品路径 = 成品目录.join(format!("bars_{date}.parquet"));
    let mut 旧根数 = 0usize;
    if 同日合并 && 成品路径.exists() {
        match 读取成品(&成品路径) {
            Ok(旧) => {
                旧根数 = 旧.len();
                bars.extend(旧);
            }
            Err(e) => {
                tracing::error!("[MERGE] 读回旧成品失败（按无旧数据继续）: {e}");
            }
        }
    }
    // ★v2.0 去重：键=(合约, bar时间)；真实 bar 优先于补的 gap bar；同级后写者胜（jsonl 在后=新数据）。
    let 去重前 = bars.len();
    let mut 最佳: HashMap<(String, i64), MinuteBar> = HashMap::new();
    for b in bars {
        match 最佳.entry((b.instrument_id.clone(), b.bar_time_ms)) {
            Entry::Occupied(mut o) => {
                if o.get().is_gap || !b.is_gap {
                    o.insert(b);
                }
            }
            Entry::Vacant(v) => {
                v.insert(b);
            }
        }
    }
    let 去重数 = 去重前 - 最佳.len();
    // 旧成品里的 gap bar 只是上一轮合并的产物，本轮按真实 bar 重新生成（增量语义一致）。
    let 旧gap = 最佳.values().filter(|b| b.is_gap).count();
    let bars: Vec<MinuteBar> = 最佳.into_values().filter(|b| !b.is_gap).collect();
    if 去重数 > 0 || 旧根数 > 0 {
        tracing::info!(
            "[MERGE] 同日合并：读回旧成品 {旧根数} 根（含 gap {旧gap}）| 去重 {去重数} 根 | 以 {} 根真实 bar 重算增量",
            bars.len()
        );
    }

    // 3) 按合约分组 + 算增量
    let mut by_inst: HashMap<String, Vec<MinuteBar>> = HashMap::new();
    for b in bars {
        // （v2.0 起上一步已把 gap 剔除；真实 bar 逐根入组）
        by_inst.entry(b.instrument_id.clone()).or_default().push(b);
    }
    let mut gap = GapStats::default();
    let mut 全部: Vec<MinuteBar> = Vec::with_capacity(by_inst.len() * 345);
    for v in by_inst.values_mut() {
        v.sort_by_key(|a| a.bar_time_ms);
        // 算单分钟增量（只在一根根真实 bar 之间）
        let mut prev_vol = 0u64;
        let mut prev_to = 0.0f64;
        let mut first = true;
        for b in v.iter_mut() {
            if first {
                b.volume_1m = b.volume; // 当日首根：增量=累计（此前视为 0）
                b.turnover_1m = b.turnover;
                first = false;
            } else {
                b.volume_1m = b.volume.saturating_sub(prev_vol);
                let dt = b.turnover - prev_to;
                b.turnover_1m = if dt < 0.0 { 0.0 } else { dt }; // 浮点兜底，异常负值归 0
            }
            prev_vol = b.volume;
            prev_to = b.turnover;
        }
        // 4) 补缺口（在算完增量后补；缺口 bar 增量=0，累计沿用前一根保持单调）
        补齐缺口(v, &mut gap);
        全部.append(v);
    }

    // 全局排序：先合约后时间，和 v1.1 成品文件的排布一致，便于下游 diff 对照。
    全部.sort_by(|a, b| {
        a.instrument_id
            .cmp(&b.instrument_id)
            .then(a.bar_time_ms.cmp(&b.bar_time_ms))
    });

    // 5) 原子写 13 列 Parquet
    std::fs::create_dir_all(成品目录)?;
    let path = 成品目录.join(format!("bars_{date}.parquet"));
    写日线parquet(&path, &全部, 双写增量)?;
    let 缺口数 = 全部.iter().filter(|b| b.is_gap).count();
    tracing::info!(
        "[MERGE] ✅ {} 合并完成 → {} 共 {} 根（其中补齐 {} 根，涉及 {} 个有缺口合约）",
        date,
        path.display(),
        全部.len(),
        缺口数,
        gap.instruments_with_gap
    );
    Ok(())
}

/// ★v1.5 归档压缩（D028）：把"非今日"且已静默的 jsonl 分片压成 .jsonl.zst，压成功才删原件。
/// 【磁盘压力根因】jsonl 中间产物一天 ~600MB 线性增长；成品 parquet 只有十几 MB；
/// 压档后分片目录常年 <100MB，合并端可直接读 .zst 重新合并历史日期。
/// 三道护栏：①非今日（今日可能还在写）②mtime 静默 ≥ 静默阈值秒（生产传 600；测试传 0 跳过）
/// ③压缩产物非零且原子写（.tmp → rename），删原件前先确认目标存在非空。
/// 返回归档文件数。
pub fn 归档非今日分片(分片目录: &Path, 今日: &str, 静默阈值秒: u64) -> usize {
    let mut 归档数 = 0usize;
    let Ok(rd) = std::fs::read_dir(分片目录) else {
        return 0;
    };
    for entry in rd.flatten() {
        let p = entry.path();
        let Some(name) = p.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        // 只处理 bars_YYYYMMDD.jsonl（恰 8 位日期），不动 .zst / .merging / 其他文件
        let Some(stem) = name.strip_suffix(".jsonl") else {
            continue;
        };
        if !stem.starts_with("bars_") || stem.len() != "bars_YYYYMMDD".len() {
            continue;
        }
        let 日期 = &stem["bars_".len()..];
        if 日期 == 今日 || !日期.chars().all(|c| c.is_ascii_digit()) {
            continue; // 护栏① 非今日；非日期文件不碰
        }
        if 静默阈值秒 > 0 {
            let Ok(meta) = entry.metadata() else { continue };
            let Ok(mtime) = meta.modified() else { continue };
            let Ok(静默了) = mtime.elapsed() else {
                continue;
            };
            if 静默了 < Duration::from_secs(静默阈值秒) {
                continue; // 护栏② 可能还在写
            }
        }
        let 目标 = 分片目录.join(format!("{name}.zst"));
        if 目标.exists() {
            continue; // 已归档过
        }
        let Ok(原) = std::fs::read(&p) else { continue };
        if 原.is_empty() {
            continue;
        }
        let Ok(压缩) = zstd::stream::encode_all(&原[..], 10) else {
            tracing::error!("[归档] {} 压缩失败，保留原件", name);
            continue;
        };
        if 压缩.is_empty() {
            tracing::error!("[归档] {} 压缩产物为空，保留原件", name);
            continue;
        }
        let tmp = 分片目录.join(format!(".{name}.zst.tmp"));
        if std::fs::write(&tmp, &压缩).is_err() {
            tracing::error!("[归档] {} 写临时文件失败，保留原件", name);
            continue;
        }
        if std::fs::rename(&tmp, &目标).is_err() {
            let _ = std::fs::remove_file(&tmp);
            tracing::error!("[归档] {} 改名失败，保留原件", name);
            continue;
        }
        // 护栏③ 目标确认存在非空，才删原件
        match std::fs::metadata(&目标) {
            Ok(m) if m.len() > 0 => {
                let 目标名 = 目标.file_name().and_then(|s| s.to_str()).unwrap_or("?");
                if std::fs::remove_file(&p).is_ok() {
                    归档数 += 1;
                    tracing::info!(
                        "[归档] {} → {}（{}B → {}B）",
                        name,
                        目标名,
                        原.len(),
                        压缩.len()
                    );
                }
            }
            _ => tracing::error!("[归档] {} 归档产物校验失败，保留原件", name),
        }
    }
    if 归档数 > 0 {
        tracing::info!(
            "[归档] 共归档 {} 个历史分片（合并端此后可直接读 .zst）",
            归档数
        );
    }
    归档数
}

/// 补缺口：同合约相邻真实 bar 若隔 >1 分钟，中间每分钟补一根 `is_gap=true`。
/// 与 v1.1 `fill_gaps` 的区别：缺口 bar 的累计量【沿用前一根】而非置 0，保持累计列单调，
/// 下游做 `volume_1m` 或直接看累计都不会因为缺口突然掉到 0 而误判。
fn 补齐缺口(bars: &mut Vec<MinuteBar>, stats: &mut GapStats) {
    if bars.len() < 2 {
        return;
    }
    let mut out: Vec<MinuteBar> = Vec::with_capacity(bars.len());
    out.push(bars[0].clone());
    for w in bars.windows(2) {
        let prev = &w[0];
        let cur = &w[1];
        let mut t = prev.bar_time_ms + MINUTE_MS;
        while t < cur.bar_time_ms {
            out.push(MinuteBar {
                instrument_id: cur.instrument_id.clone(),
                bar_time_ms: t,
                trading_day: cur.trading_day.clone(),
                open: prev.close,
                high: prev.close,
                low: prev.close,
                close: prev.close,
                volume: prev.volume, // 累计沿用前一根（保持单调）
                turnover: prev.turnover,
                open_interest: prev.open_interest,
                volume_1m: 0, // 缺口分钟没有成交
                turnover_1m: 0.0,
                is_gap: true,
            });
            stats.gap_bars += 1;
            t += MINUTE_MS;
        }
        out.push(cur.clone());
    }
    *bars = out;
    if stats.gap_bars > 0 {
        stats.instruments_with_gap += 1;
    }
}

/// ★v2.0 同日合并：读回已存在的成品 Parquet（兼容 13 列新口径与 11 列老口径）。
/// 返回逐根 bar（`volume_1m`/`turnover_1m` 缺列时默认 0，反正后续全量重算增量）。
fn 读取成品(path: &Path) -> Result<Vec<MinuteBar>, Box<dyn std::error::Error>> {
    use arrow_array::{
        BooleanArray, Float64Array, StringArray, TimestampMillisecondArray, UInt64Array,
    };
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = std::fs::File::open(path)?;
    let batches = ParquetRecordBatchReaderBuilder::try_new(file)?
        .build()?
        .collect::<Result<Vec<_>, _>>()?;
    let mut out: Vec<MinuteBar> = Vec::new();
    for batch in batches {
        let n = batch.num_rows();

        let bt = batch
            .column_by_name("bar_time")
            .and_then(|a| a.as_any().downcast_ref::<TimestampMillisecondArray>())
            .ok_or_else(|| std::io::Error::other("成品缺列 bar_time"))?;
        let inst = batch
            .column_by_name("instrument_id")
            .and_then(|a| a.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| std::io::Error::other("成品缺列 instrument_id"))?;
        let td = batch
            .column_by_name("trading_day")
            .and_then(|a| a.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| std::io::Error::other("成品缺列 trading_day"))?;
        let 取浮点 = |名: &str| -> Result<Vec<f64>, Box<dyn std::error::Error>> {
            Ok(batch
                .column_by_name(名)
                .and_then(|a| a.as_any().downcast_ref::<Float64Array>())
                .ok_or_else(|| std::io::Error::other(format!("成品缺列 {名}")))?
                .iter()
                .map(|v| v.unwrap_or(0.0))
                .collect())
        };
        let 取整数 = |名: &str| -> Result<Vec<u64>, Box<dyn std::error::Error>> {
            Ok(batch
                .column_by_name(名)
                .and_then(|a| a.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| std::io::Error::other(format!("成品缺列 {名}")))?
                .iter()
                .map(|v| v.unwrap_or(0))
                .collect())
        };
        let open = 取浮点("open")?;
        let high = 取浮点("high")?;
        let low = 取浮点("low")?;
        let close = 取浮点("close")?;
        let vol = 取整数("volume")?;
        let turnover = 取浮点("turnover")?;
        let oi = 取浮点("open_interest")?;
        let vol1m = 取整数("volume_1m").unwrap_or_else(|_| vec![0; n]);
        let to1m = 取浮点("turnover_1m").unwrap_or_else(|_| vec![0.0; n]);
        let gap = batch
            .column_by_name("is_gap")
            .and_then(|a| a.as_any().downcast_ref::<BooleanArray>())
            .ok_or_else(|| std::io::Error::other("成品缺列 is_gap"))?
            .iter()
            .map(|v| v.unwrap_or(false))
            .collect::<Vec<bool>>();
        for i in 0..n {
            out.push(MinuteBar {
                instrument_id: inst.value(i).to_string(),
                bar_time_ms: bt.value(i),
                trading_day: td.value(i).to_string(),
                open: open[i],
                high: high[i],
                low: low[i],
                close: close[i],
                volume: vol[i],
                turnover: turnover[i],
                open_interest: oi[i],
                volume_1m: vol1m[i],
                turnover_1m: to1m[i],
                is_gap: gap[i],
            });
        }
    }
    Ok(out)
}

/// 写整日成品 Parquet（双写成交量增量=true → 13 列；false → 11 列老口径，无 `volume_1m`/`turnover_1m`）。
/// 原子写：先写 .tmp，写完 rename；已存在先备份 .bak。与 v1.1 `write_parquet` 同一套路。
fn 写日线parquet(
    path: &Path,
    bars: &[MinuteBar],
    双写增量: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use arrow_array::{
        ArrayRef, BooleanArray, Float64Array, RecordBatch, StringArray, TimestampMillisecondArray,
        UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    let mut 字段 = vec![
        Field::new(
            "bar_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("trading_day", DataType::Utf8, false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::UInt64, false),
        Field::new("turnover", DataType::Float64, false),
        Field::new("open_interest", DataType::Float64, false),
    ];
    // ★v2.0 双写成交量增量：增量列可选（关掉则 11 列老口径）
    if 双写增量 {
        字段.push(Field::new("volume_1m", DataType::UInt64, false));
        字段.push(Field::new("turnover_1m", DataType::Float64, false));
    }
    字段.push(Field::new("is_gap", DataType::Boolean, false));
    let schema = Arc::new(Schema::new(字段));

    let 取 = |f: fn(&MinuteBar) -> i64| bars.iter().map(&f).collect::<Vec<i64>>();
    let bt = 取(|b| b.bar_time_ms);
    let inst: Vec<&str> = bars.iter().map(|b| b.instrument_id.as_str()).collect();
    let td: Vec<&str> = bars.iter().map(|b| b.trading_day.as_str()).collect();
    let open: Vec<f64> = bars.iter().map(|b| b.open).collect();
    let high: Vec<f64> = bars.iter().map(|b| b.high).collect();
    let low: Vec<f64> = bars.iter().map(|b| b.low).collect();
    let close: Vec<f64> = bars.iter().map(|b| b.close).collect();
    let vol: Vec<u64> = bars.iter().map(|b| b.volume).collect();
    let turnover: Vec<f64> = bars.iter().map(|b| b.turnover).collect();
    let oi: Vec<f64> = bars.iter().map(|b| b.open_interest).collect();
    let vol1m: Vec<u64> = bars.iter().map(|b| b.volume_1m).collect();
    let to1m: Vec<f64> = bars.iter().map(|b| b.turnover_1m).collect();
    let gap: Vec<bool> = bars.iter().map(|b| b.is_gap).collect();

    let mut 数组: Vec<ArrayRef> = vec![
        Arc::new(TimestampMillisecondArray::from(bt)) as ArrayRef,
        Arc::new(StringArray::from(inst)) as ArrayRef,
        Arc::new(StringArray::from(td)) as ArrayRef,
        Arc::new(Float64Array::from(open)) as ArrayRef,
        Arc::new(Float64Array::from(high)) as ArrayRef,
        Arc::new(Float64Array::from(low)) as ArrayRef,
        Arc::new(Float64Array::from(close)) as ArrayRef,
        Arc::new(UInt64Array::from(vol)) as ArrayRef,
        Arc::new(Float64Array::from(turnover)) as ArrayRef,
        Arc::new(Float64Array::from(oi)) as ArrayRef,
    ];
    if 双写增量 {
        数组.push(Arc::new(UInt64Array::from(vol1m)) as ArrayRef);
        数组.push(Arc::new(Float64Array::from(to1m)) as ArrayRef);
    }
    数组.push(Arc::new(BooleanArray::from(gap)) as ArrayRef);

    let batch = RecordBatch::try_new(schema.clone(), 数组)?;

    let tmp = path.with_extension("parquet.tmp");
    {
        let file = std::fs::File::create(&tmp)?;
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(
                parquet::basic::ZstdLevel::default(),
            ))
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
    }
    if path.exists() {
        let bak = path.with_extension("parquet.bak");
        if bak.exists() {
            std::fs::remove_file(&bak).ok();
        }
        std::fs::rename(path, &bak).ok();
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod 测试 {
    use super::*;

    fn 造bar(inst: &str, ms: i64, vol: u64, to: f64) -> MinuteBar {
        MinuteBar {
            instrument_id: inst.to_string(),
            bar_time_ms: ms,
            trading_day: "20261124".to_string(),
            open: 1.0,
            high: 1.0,
            low: 1.0,
            close: 1.0,
            volume: vol,
            turnover: to,
            open_interest: 0.0,
            volume_1m: 0,
            turnover_1m: 0.0,
            is_gap: false,
        }
    }

    #[test]
    fn 增量首根等于累计() {
        let mut v = [造bar("IF2401", 0, 100, 1000.0)];
        // 模拟合并里的增量计算逻辑
        let mut prev_vol = 0u64;
        let mut prev_to = 0.0f64;
        let mut first = true;
        for b in &mut v {
            if first {
                b.volume_1m = b.volume;
                b.turnover_1m = b.turnover;
                first = false;
            } else {
                b.volume_1m = b.volume.saturating_sub(prev_vol);
                b.turnover_1m = b.turnover - prev_to;
            }
            prev_vol = b.volume;
            prev_to = b.turnover;
        }
        assert_eq!(v[0].volume_1m, 100);
        assert_eq!(v[0].turnover_1m, 1000.0);
    }

    #[test]
    fn 增量后续是相邻差() {
        let mut v = vec![
            造bar("IF2401", 0, 100, 1000.0),
            造bar("IF2401", 60_000, 250, 2600.0),
            造bar("IF2401", 120_000, 250, 2600.0), // 这分钟没成交，增量应 0
        ];
        let mut prev_vol = 0u64;
        let mut prev_to = 0.0f64;
        let mut first = true;
        for b in &mut v {
            if first {
                b.volume_1m = b.volume;
                b.turnover_1m = b.turnover;
                first = false;
            } else {
                b.volume_1m = b.volume.saturating_sub(prev_vol);
                b.turnover_1m = b.turnover - prev_to;
            }
            prev_vol = b.volume;
            prev_to = b.turnover;
        }
        assert_eq!(v[1].volume_1m, 150);
        assert_eq!(v[1].turnover_1m, 1600.0);
        assert_eq!(v[2].volume_1m, 0); // 累计没变 → 增量 0
    }

    #[test]
    fn 补缺口在中间插空bar且累计单调() {
        let mut v = vec![
            造bar("IF2401", 0, 100, 1000.0),
            造bar("IF2401", 180_000, 300, 3000.0), // 中间缺 60s、120s 两分钟
        ];
        let mut gap = GapStats::default();
        补齐缺口(&mut v, &mut gap);
        // 应有 4 根：0, 60s(gap), 120s(gap), 180s
        assert_eq!(v.len(), 4);
        assert!(v[1].is_gap && v[2].is_gap);
        assert!(!v[0].is_gap && !v[3].is_gap);
        // 缺口 bar 累计沿用前一根（单调），增量 0
        assert_eq!(v[1].volume, 100);
        assert_eq!(v[2].volume, 100);
        assert_eq!(v[1].volume_1m, 0);
        // gap 数 = 2
        assert_eq!(gap.gap_bars, 2);
    }

    #[test]
    fn 无缺口不插bar() {
        let mut v = vec![
            造bar("IF2401", 0, 100, 1000.0),
            造bar("IF2401", 60_000, 200, 2000.0),
        ];
        let mut gap = GapStats::default();
        补齐缺口(&mut v, &mut gap);
        assert_eq!(v.len(), 2);
        assert_eq!(gap.gap_bars, 0);
    }

    #[test]
    fn 单bar不补() {
        let mut v = vec![造bar("IF2401", 0, 100, 1000.0)];
        let mut gap = GapStats::default();
        补齐缺口(&mut v, &mut gap);
        assert_eq!(v.len(), 1);
    }

    #[test]
    fn 重复时间戳不补() {
        let mut v = vec![
            造bar("IF2401", 60_000, 100, 1000.0),
            造bar("IF2401", 60_000, 120, 1200.0), // 同一分钟两根（异常输入）
        ];
        let mut gap = GapStats::default();
        补齐缺口(&mut v, &mut gap);
        assert_eq!(v.len(), 2);
        assert_eq!(gap.gap_bars, 0);
    }

    #[test]
    fn 恰好隔2分钟补1根() {
        // 间隔 n 分钟 → 中间补 n-1 根：0s 与 120s 之间恰补 1 根 60s
        let mut v = vec![
            造bar("IF2401", 0, 100, 1000.0),
            造bar("IF2401", 120_000, 300, 3000.0),
        ];
        let mut gap = GapStats::default();
        补齐缺口(&mut v, &mut gap);
        assert_eq!(v.len(), 3);
        assert!(v[1].is_gap);
        assert_eq!(v[1].bar_time_ms, 60_000);
        assert_eq!(gap.gap_bars, 1);
    }

    #[test]
    fn 跨大间隔全补齐且单调() {
        // 日盘 15:00 → 夜盘 21:00 = 361 分钟间隔（真实数据每天走的跨时段路径，D026）
        let mut v = vec![
            造bar("IF2401", 0, 100, 1000.0),
            造bar("IF2401", 361 * MINUTE_MS, 300, 3000.0),
        ];
        let mut gap = GapStats::default();
        补齐缺口(&mut v, &mut gap);
        assert_eq!(v.len(), 1 + 360 + 1);
        assert_eq!(gap.gap_bars, 360);
        // 累计量单调不回退（gap 沿用前一根）
        let mut last_vol = 0u64;
        for b in &v {
            assert!(b.volume >= last_vol);
            last_vol = b.volume;
        }
        // 时间轴严格递增
        for w in v.windows(2) {
            assert!(w[1].bar_time_ms > w[0].bar_time_ms);
        }
        // 成品 is_gap 含跨时段空分钟：下游按 is_gap 过滤（语义写进测试注释，D026）
        assert_eq!(v.iter().filter(|b| b.is_gap).count(), 360);
    }

    #[test]
    #[expect(
        clippy::panic,
        reason = "测试辅助写文件失败即测试失败，panic 是正确语义（宪法测试豁免需显式标注）"
    )]
    fn 归档压缩往返内容一致() {
        let dir = std::env::temp_dir().join(format!("归档测试_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let f = dir.join("bars_20260101.jsonl");
        let 内容 = "{\"instrument_id\":\"IF2401\"}\n{\"instrument_id\":\"rb2501\"}\n".repeat(1000);
        std::fs::write(&f, 内容.as_bytes()).unwrap_or_else(|e| panic!("写测试分片失败: {e}"));
        // 静默阈值传 0 = 跳过 mtime 护栏（测试注入；生产传 600）
        let n = 归档非今日分片(&dir, "20991231", 0);
        assert_eq!(n, 1);
        let z = dir.join("bars_20260101.jsonl.zst");
        assert!(z.exists(), "应生成 .zst 归档");
        let 原数据 = std::fs::read(&z).unwrap_or_default();
        let 解开 = zstd::stream::decode_all(&原数据[..]).unwrap_or_default();
        assert_eq!(解开, 内容.as_bytes(), "解压内容必须逐字节一致");
        assert!(!f.exists(), "压缩成功后原件应已删除");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[expect(
        clippy::panic,
        reason = "测试辅助写文件失败即测试失败，panic 是正确语义（宪法测试豁免需显式标注）"
    )]
    fn 归档拒绝今日文件() {
        let dir = std::env::temp_dir().join(format!("归档拒绝_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let 今日 = "20991231";
        let f = dir.join(format!("bars_{今日}.jsonl"));
        std::fs::write(&f, "x".repeat(500).as_bytes()).unwrap_or_else(|e| panic!("写失败: {e}"));
        let n = 归档非今日分片(&dir, 今日, 0);
        assert_eq!(n, 0);
        assert!(f.exists(), "今日文件绝不能被归档");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

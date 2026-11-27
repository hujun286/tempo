//! 成品 Parquet 读回校验（下游视角·按列名读取示范）
//!
//! 用法：cargo run --example 读回校验 -- [parquet路径]
//! 校验点：13 列列名与顺序、行数、黄金/白银样本行、缺口统计。
//!
//! 守卫说明：本 example 是独立 CLI 工具，stdout 就是它的输出接口（未接 tracing），
//! print_stdout/print_stderr 带 reason 显式豁免；文件/解析失败一律报错退出非 unwrap。

use arrow_array::{
    BooleanArray, Float64Array, StringArray, TimestampMillisecondArray, UInt64Array,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashSet;

#[allow(
    clippy::print_stdout,
    reason = "独立 CLI 校验工具，stdout 是输出接口；主程序已统一走 tracing"
)]
fn 打印(行: &str) {
    println!("{行}");
}

#[allow(
    clippy::print_stderr,
    reason = "独立 CLI 校验工具，stderr 报错并带退出码，供脚本判定"
)]
fn 出错退出(消息: &str) -> ! {
    eprintln!("❌ {消息}");
    std::process::exit(1);
}

fn 列字符串<'a>(b: &'a arrow_array::RecordBatch, 名: &str) -> &'a StringArray {
    b.column_by_name(名)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .unwrap_or_else(|| 出错退出(&format!("列 {名} 缺失或类型不符")))
}

fn 主() {
    let 路径 =
        std::env::args().nth(1).unwrap_or_else(|| "data_数据/分钟K线/bars_20261127.parquet".into());
    let 文件 = match std::fs::File::open(&路径) {
        Ok(f) => f,
        Err(e) => 出错退出(&format!("打开成品文件失败({路径}): {e}")),
    };
    let builder = match ParquetRecordBatchReaderBuilder::try_new(文件) {
        Ok(b) => b,
        Err(e) => 出错退出(&format!("构建 reader 失败: {e}")),
    };
    let schema = builder.schema().clone();
    打印("=== 元数据 ===");
    打印(&format!("列数: {}", schema.fields().len()));
    let 列名们: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    打印(&format!("列名: {列名们:?}"));
    打印(&format!("总行数: {}", builder.metadata().file_metadata().num_rows()));
    let 期望 = [
        "bar_time",
        "instrument_id",
        "trading_day",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover",
        "open_interest",
        "volume_1m",
        "turnover_1m",
        "is_gap",
    ];
    let 结构对 = 列名们 == 期望.iter().map(|s| (*s).to_string()).collect::<Vec<_>>();
    打印(&format!("13 列口径一致性: {}", if 结构对 { "完全一致" } else { "不一致(需人工看)" }));

    let reader = builder.build().unwrap_or_else(|e| 出错退出(&format!("读取失败: {e}")));
    let mut 黄金样本 = String::new();
    let mut 白银样本 = String::new();
    let mut 缺口 = 0usize;
    let mut 合约: HashSet<String> = HashSet::new();
    for batch in reader {
        let b = match batch {
            Ok(b) => b,
            Err(e) => 出错退出(&format!("批次读取失败: {e}")),
        };
        let 取列 = |名: &str| -> &arrow_array::ArrayRef {
            b.column_by_name(名).unwrap_or_else(|| 出错退出(&format!("缺列 {名}")))
        };
        let ids = 列字符串(&b, "instrument_id");
        let times = 取列("bar_time")
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap_or_else(|| 出错退出("bar_time 类型应为 Timestamp(ms)"));
        let days = 列字符串(&b, "trading_day");
        let closes = 取列("close")
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap_or_else(|| 出错退出("close 类型应为 Float64"));
        let vols = 取列("volume")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap_or_else(|| 出错退出("volume 类型应为 UInt64"));
        let gaps = 取列("is_gap")
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap_or_else(|| 出错退出("is_gap 类型应为 Boolean"));
        for i in 0..b.num_rows() {
            if gaps.value(i) {
                缺口 += 1;
            }
            合约.insert(ids.value(i).to_string());
            if ids.value(i) == "au2702" && 黄金样本.is_empty() {
                黄金样本 = format!(
                    "au2702 | trading_day={} | close={:.2} | volume={} | bar_time={}",
                    days.value(i),
                    closes.value(i),
                    vols.value(i),
                    times.value(i)
                );
            }
            if ids.value(i) == "ag2702" && 白银样本.is_empty() {
                白银样本 = format!(
                    "ag2702 | trading_day={} | close={:.2} | volume={} | bar_time={}",
                    days.value(i),
                    closes.value(i),
                    vols.value(i),
                    times.value(i)
                );
            }
        }
    }
    打印("=== 内容 ===");
    打印(&format!("黄金样本: {黄金样本}"));
    打印(&format!("白银样本: {白银样本}"));
    打印(&format!("涉及合约数: {} | is_gap=True 根数: {缺口}", 合约.len()));
}

fn main() {
    主();
}

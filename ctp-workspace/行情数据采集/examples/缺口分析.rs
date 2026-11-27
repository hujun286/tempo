//! 缺口分析：统计成品 Parquet 里 is_gap=true 的分布（哪些合约、什么时段）
//!
//! 用法：cargo run --example 缺口分析 -- [parquet路径]
//! 产出：缺口最多的合约清单、黄金白银专项、按品种归类缺口根数——用于核对补缺口逻辑。
//!
//! 守卫说明：独立 CLI 分析工具，stdout 为输出接口（print 豁免带理由）；失败即报错退出，零 unwrap/expect。

use arrow_array::{BooleanArray, StringArray, TimestampMillisecondArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::BTreeMap;

#[allow(
    clippy::print_stdout,
    reason = "独立 CLI 分析工具，stdout 是输出接口；主程序已统一走 tracing"
)]
fn 打印(行: &str) {
    println!("{行}");
}

#[allow(
    clippy::print_stderr,
    reason = "独立 CLI 分析工具，stderr 报错并带退出码，供脚本判定"
)]
fn 出错退出(消息: &str) -> ! {
    eprintln!("❌ {消息}");
    std::process::exit(1);
}

struct 合约缺口 {
    真实: usize,
    缺口: usize,
    最早: i64,
    最晚: i64,
    样本: Vec<i64>,
}

fn 主() {
    let 路径 =
        std::env::args().nth(1).unwrap_or_else(|| "data_数据/分钟K线/bars_20261127.parquet".into());
    let 文件 = match std::fs::File::open(&路径) {
        Ok(f) => f,
        Err(e) => 出错退出(&format!("打开失败({路径}): {e}")),
    };
    let reader = match ParquetRecordBatchReaderBuilder::try_new(文件)
        .map_err(|e| format!("构建失败: {e}"))
        .and_then(|b| b.build().map_err(|e| format!("读取失败: {e}")))
    {
        Ok(r) => r,
        Err(msg) => 出错退出(&msg),
    };
    // 合约 → 缺口档案
    let mut 表: BTreeMap<String, 合约缺口> = BTreeMap::new();
    for batch in reader {
        let b = match batch {
            Ok(b) => b,
            Err(e) => 出错退出(&format!("批次失败: {e}")),
        };
        let 解列 = |名: &str| -> &arrow_array::ArrayRef {
            b.column_by_name(名).unwrap_or_else(|| 出错退出(&format!("缺列 {名}")))
        };
        let ids = 解列("instrument_id")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| 出错退出("instrument_id 类型应为 Utf8"));
        let times = 解列("bar_time")
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap_or_else(|| 出错退出("bar_time 类型应为 Timestamp(ms)"));
        let gaps = 解列("is_gap")
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap_or_else(|| 出错退出("is_gap 类型应为 Boolean"));
        for i in 0..b.num_rows() {
            let id = ids.value(i).to_string();
            let t = times.value(i);
            let e = 表.entry(id).or_insert(合约缺口 {
                真实: 0,
                缺口: 0,
                最早: t,
                最晚: t,
                样本: Vec::new(),
            });
            if gaps.value(i) {
                e.缺口 += 1;
                if e.样本.len() < 3 {
                    e.样本.push(t);
                }
            } else {
                e.真实 += 1;
                if t < e.最早 {
                    e.最早 = t;
                }
                if t > e.最晚 {
                    e.最晚 = t;
                }
            }
        }
    }
    let 总合约 = 表.len();
    let 有缺口: Vec<(&String, &合约缺口)> = 表.iter().filter(|(_, v)| v.缺口 > 0).collect();
    打印(&format!("总合约 {总合约} | 有缺口合约 {}", 有缺口.len()));
    打印("=== 缺口最多的前 15 个合约 ===");
    打印(&format!(
        "{:<12} {:>6} {:>6} {:>14} {:>14} {:>8}  缺口时间样本",
        "合约", "真实", "缺口", "最早", "最晚", "跨度分"
    ));
    let mut 排序: Vec<(&String, &合约缺口)> = 有缺口.clone();
    排序.sort_by_key(|&(_, v)| std::cmp::Reverse(v.缺口));
    for (id, v) in 排序.iter().take(15) {
        打印(&format!(
            "{:<12} {:>6} {:>6} {:>14} {:>14} {:>8}  {:?}",
            id,
            v.真实,
            v.缺口,
            v.最早,
            v.最晚,
            (v.最晚 - v.最早) / 60_000,
            v.样本
        ));
    }
    打印("=== 黄金白银专项 ===");
    for id in ["au2702", "ag2702", "au2701", "ag2701"] {
        match 表.get(id) {
            Some(v) => 打印(&format!(
                "{id}: 真实 {} 根, 缺口 {} 根, 样本 {:?}",
                v.真实, v.缺口, v.样本
            )),
            None => 打印(&format!("{id}: 无数据")),
        }
    }
    打印("=== 缺口根数按品种归类（合约代码字母前缀） ===");
    let mut 品种统计: BTreeMap<String, usize> = BTreeMap::new();
    for (id, v) in &有缺口 {
        let 品种: String = id.chars().take_while(|c| c.is_alphabetic()).collect();
        *品种统计.entry(品种).or_default() += v.缺口;
    }
    for (品种, 数量) in 品种统计.iter().rev() {
        打印(&format!("  {品种}: {数量} 根缺口"));
    }
}

fn main() {
    主();
}

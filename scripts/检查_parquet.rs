// cargo-deps: parquet="58", arrow="58"
//! 检查成品 parquet：schema（列名+类型+非空）、行数、row group、压缩、首末各 2 行样例，
//! 并与 v1.4 写日线parquet 的 13 列预期逐列比对。
//! 用法：rust-script 检查_parquet.rs <parquet路径>

use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
// (Compression 类型不再单独引用)
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::PathBuf;
use std::process::exit;

/// v1.4 写日线parquet 的 13 列预期：(列名, arrow 类型名)
const 预期13列: [(&str, &str); 13] = [
    ("bar_time", "Timestamp(Millisecond, None)"),
    ("instrument_id", "Utf8"),
    ("trading_day", "Utf8"),
    ("open", "Float64"),
    ("high", "Float64"),
    ("low", "Float64"),
    ("close", "Float64"),
    ("volume", "UInt64"),
    ("turnover", "Float64"),
    ("open_interest", "Float64"),
    ("volume_1m", "UInt64"),
    ("turnover_1m", "Float64"),
    ("is_gap", "Boolean"),
];

fn 类型名(dt: &DataType) -> String {
    match dt {
        DataType::Timestamp(unit, tz) => {
            let u = format!("{unit:?}");
            match tz {
                None => format!("Timestamp({u}, None)"),
                Some(t) => format!("Timestamp({u}, {t})"),
            }
        }
        other => format!("{other:?}"),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("用法: rust-script 检查_parquet.rs <parquet路径>");
        exit(2);
    }
    let p = PathBuf::from(&args[1]);
    let 大小 = std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
    println!("文件: {}", p.display());
    println!("大小: {} 字节 ({:.1} MB)", 大小, 大小 as f64 / 1048576.0);

    // 1) 物理层：行数 / row group / 压缩
    let reader = match SerializedFileReader::try_from(File::open(&p).expect("打开失败")) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("❌ 不是合法 parquet: {e}");
            exit(1);
        }
    };
    let meta = reader.metadata();
    println!("行数: {}", meta.file_metadata().num_rows());
    println!("row group 数: {}", meta.num_row_groups());
    let mut 压缩集: Vec<String> = Vec::new();
    for rg in 0..meta.num_row_groups() {
        let g = meta.row_group(rg);
        for c in 0..g.num_columns() {
            let col = g.column(c);
            let 名 = format!("{:?}", col.compression());
            if !压缩集.contains(&名) {
                压缩集.push(名);
            }
        }
    }
    println!("压缩: {}", 压缩集.join(", "));

    // 2) arrow 层：schema 逐列比对
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).expect("打开失败"))
        .expect("构建 reader 失败");
    let schema = builder.schema().clone();
    println!("\n=== schema 实际 {} 列 | 预期 {} 列 ===", schema.fields().len(), 预期13列.len());
    let mut 差异: Vec<String> = Vec::new();
    for (i, f) in schema.fields().iter().enumerate() {
        let 实际 = 类型名(f.data_type());
        let (预期名, 预期型) = if i < 预期13列.len() {
            预期13列[i]
        } else {
            ("(多余)", "(无)")
        };
        let 空性 = if f.is_nullable() { "nullable" } else { "NOT NULL" };
        let 状态 = if f.name() == 预期名 && 实际 == 预期型 { "✓" } else { "✗" };
        println!("{state} 列{i}: {} [{实际}] {空性}  (预期: {预期名} [{预期型}])", f.name(), state = 状态);
        if 状态 == "✗" {
            差异.push(format!("列{i}: 实际 {} [{实际}] vs 预期 {预期名} [{预期型}]", f.name()));
        }
    }

    // 3) 首末样例
    let reader_batches = builder.build().expect("读取失败");
    let mut batches: Vec<_> = reader_batches.map(|b| b.expect("batch 解析失败")).collect();
    if batches.is_empty() {
        println!("\n⚠ 0 个 batch");
    } else {
        let 第一 = &batches[0];
        let 末尾 = batches.last().unwrap();
        let n = 第一.num_rows().min(2);
        println!("\n=== 首批前 {n} 行 ===");
        for r in 0..n {
            println!("-- 行 {r} --");
            for (ci, col) in 第一.columns().iter().enumerate() {
                let v = format!("{:?}", arrow::util::display::array_value_to_string(col, r));
                println!("  {}: {}", schema.field(ci).name(), v);
            }
        }
        let m = 末尾.num_rows().saturating_sub(1);
        println!("=== 末批最后 1 行 ===");
        println!("-- 行 (末) --");
        for (ci, col) in 末尾.columns().iter().enumerate() {
            let v = format!("{:?}", arrow::util::display::array_value_to_string(col, m));
            println!("  {}: {}", schema.field(ci).name(), v);
        }
        let _ = &mut batches;
    }

    // 4) 结论
    println!("\n=== 结论 ===");
    if 差异.is_empty() && schema.fields().len() == 预期13列.len() {
        println!("✅ schema 与 v1.4 预期 13 列完全一致，可作为 🔴2 补缺口合并产物的对照基准");
    } else {
        println!("❌ 有 {} 处差异：", 差异.len());
        for d in &差异 {
            println!("  - {d}");
        }
    }
}

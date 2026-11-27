//! parquet 下游按列名读取规范示例（★schema-first，D031）
//!
//! 背景（D018 实证）：成品 parquet 存在 11 列旧格式（v1.1）与 13 列新格式（v1.4+），
//! 且 is_gap 列位漂移 —— 下游按【列序】读必错位。规范：**先读列名、后读列序**。
//! 四步规范（本脚本可运行演示）：
//!   ① ParquetRecordBatchReaderBuilder 只读 schema（metadata 旁路，不加载整文件）
//!   ② 列名清单判定格式：11 列（v1.1 旧）/ 13 列（v1.4+ 新）
//!   ③ 列索引全部从 schema 动态查（零硬编码列序）
//!   ④ 缺失列（旧文件无 volume_1m/turnover_1m）按业务给默认值
//!
//! 用法（二选一）：
//!   rust-script scripts/示例_下游按列名读取.rs data_数据/分钟K线/bars_20261125.parquet
//!   （manifest 冲突时报 frontmatters 的机器，用独立 cargo 项目包装，见 D037 注记）
//!
//! 依赖：parquet 58（与主工程同版本，workspace.dependencies 已统一）

// ---
// [dependencies]
// parquet = { version = "58", default-features = false, features = ["arrow"] }
// arrow = "58"
// ---

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn 判格式(列名: &[String]) -> &'static str {
    match 列名.len() {
        11 => "11 列 = v1.1 旧格式（无 volume_1m/turnover_1m）",
        13 => "13 列 = v1.4+ 新格式",
        n => {
            let _ = n;
            "未知格式（列数既非 11 也非 13）——先人工看 schema 再接"
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let Some(path) = args.get(1) else {
        eprintln!("用法: 示例_下游按列名读取.rs <bars_YYYYMMDD.parquet 路径>");
        std::process::exit(2);
    };
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("打不开 {path}: {e}");
            std::process::exit(1);
        }
    };

    // ① 只读 schema（builder 阶段拿 metadata，不加载整文件）
    let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("读 parquet metadata 失败: {e}");
            std::process::exit(1);
        }
    };
    let schema = builder.schema().clone();
    let 行数 = builder.metadata().file_metadata().num_rows();
    let 列名: Vec<String> = schema.fields().iter().map(|f| f.name().to_string()).collect();

    println!("文件: {path}");
    println!("行数: {行数}");
    println!("schema（列名导向——先列名后列序）:");
    for (i, f) in schema.fields().iter().enumerate() {
        println!("  [{i:>2}] {:<16} {}", f.name(), f.data_type());
    }

    // ② 格式判定
    println!("格式判定: {}", 判格式(&列名));

    // ③ 列索引动态查（零硬编码列序）——下游代码照这个模式写
    let 必需列 = ["instrument_id", "bar_time", "open", "high", "low", "close", "volume"];
    for c in 必需列 {
        match schema.index_of(c) {
            Ok(_) => {}
            Err(_) => {
                eprintln!("❌ 必需列缺失: {c}（文件不是分钟K线成品？）");
                std::process::exit(1);
            }
        }
    }
    println!("必需列索引（动态查得）:");
    for c in 必需列 {
        let idx = schema.index_of(c).unwrap_or(usize::MAX);
        println!("  {c:<14} → [{idx}]");
    }

    // ④ 缺失列给默认（旧格式无增量列，业务上按"该分钟增量未知=0"处理）
    let volume_1m_ok = schema.field_with_name("volume_1m").is_ok();
    println!(
        "volume_1m 列存在: {volume_1m_ok}{}",
        if volume_1m_ok {
            ""
        } else {
            "（旧格式 → 下游默认 0 并自行差分，或只看累计列）"
        }
    );
    let is_gap_idx = schema.index_of("is_gap").ok();
    match is_gap_idx {
        Some(i) => println!("is_gap 列位: [{i}]（列位随格式漂移——务必按列名查，勿写死）"),
        None => println!("is_gap 列不存在（更老格式），下游按全真实 bar 处理"),
    }
    println!("✅ schema-first 四步演示完成：下游照此模式实现即不会错位");
}

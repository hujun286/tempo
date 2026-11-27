use parquet::file::reader::{FileReader, SerializedFileReader};
use std::env;
use std::fs::File;

fn main() {
    // 唯一改动：路径从命令行第一个参数取
    let parquet_path = env::args()
        .nth(1)
        .expect("用法: h5i-db数据库查询 <parquet文件路径>");

    let file = File::open(&parquet_path).expect("打开parquet文件失败，检查路径");
    let reader = SerializedFileReader::new(file).expect("解析parquet失败");

    println!("文件: {}", parquet_path);
    println!(
        "共 {} 行，{} 个行组",
        reader.metadata().file_metadata().num_rows(),
        reader.num_row_groups()
    );

    let iter = reader.get_row_iter(None).unwrap();
    for (i, row) in iter.enumerate().take(5) {
        println!("第{}行: {}", i + 1, row.unwrap());
    }
}

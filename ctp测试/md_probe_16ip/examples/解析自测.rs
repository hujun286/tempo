//! 期权合约名解析离线自测（不需要网络与 CTP 环境）
//! 验证 SHFE（au2702C768）与 CFFEX（IO2612-C-4400）两种命名规则

/// 与 src/bin/md_probe_期权.rs 同逻辑（改了那边要同步这里）
fn 解析期权合约(名: &str) -> Option<(String, f64, String)> {
    if 名.contains('-') {
        let 段: Vec<&str> = 名.split('-').collect();
        if 段.len() != 3 {
            return None;
        }
        let 品种月份 = 段.first()?.to_string();
        let 方向 = match 段.get(1)? {
            &"C" => "CALL",
            &"P" => "PUT",
            _ => return None,
        };
        let 行权价: f64 = 段.get(2)?.parse().ok()?;
        Some((方向.to_string(), 行权价, 品种月份))
    } else {
        let mut 品种 = String::new();
        let mut 断字符: Option<char> = None;
        let mut chars = 名.chars();
        for c in chars.by_ref() {
            if c.is_ascii_alphabetic() {
                品种.push(c);
            } else {
                断字符 = Some(c); // for 循环 next() 已消费该字符，必须接回剩余流
                break;
            }
        }
        let 剩余: String = 断字符.into_iter().chain(chars).collect(); // 形如 "2702C768"
        let 月份: String = 剩余.chars().take_while(|c| c.is_ascii_digit()).collect();
        let 方向与行权价: String = 剩余.chars().skip_while(|c| c.is_ascii_digit()).collect();
        let mut 方向行权价 = 方向与行权价.chars();
        let 方向 = match 方向行权价.next() {
            Some('C') => "CALL",
            Some('P') => "PUT",
            _ => return None,
        };
        let 行权价段: String = 方向行权价.collect();
        let 行权价: f64 = 行权价段.parse().ok()?;
        if 品种.is_empty() || 月份.len() != 4 {
            return None;
        }
        let 标的 = format!("{品种}{月份}");
        Some((方向.to_string(), 行权价, 标的))
    }
}

fn main() {
    let 样例 = [
        "au2702C768",
        "au2702P768",
        "au2702C800",
        "ag2702P9800",
        "IO2612-C-4400",
        "IO2612-P-4400",
        "mo2612-C-60000",
        "cu2506P78000",
        "畸形合约XYZ",
    ];
    println!("== 期权合约名解析自测 ==");
    for c in 样例.iter() {
        match 解析期权合约(c) {
            Some((方向, 价, 标的)) => println!("  {c:<16} -> 方向={方向:<4} 行权价={价:<8} 标的={标的}"),
            None => println!("  {c:<16} -> 解析失败(None)"),
        }
    }
}

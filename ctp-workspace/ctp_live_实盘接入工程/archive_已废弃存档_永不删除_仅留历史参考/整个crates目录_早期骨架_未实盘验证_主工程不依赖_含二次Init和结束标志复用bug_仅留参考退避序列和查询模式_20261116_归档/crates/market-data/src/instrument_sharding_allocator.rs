use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;

use common_model::{Exchange, InstrumentId};

/// 从合约代码中提取 YYMM（四位数字，紧随品种字母之后）。
/// 例：EG2706P4900 -> 2706；pp2701 -> 2701；IO2701-C-4000 -> 2701。
pub fn extract_yymm(code: &str) -> Option<u32> {
    let bytes = code.as_bytes();
    let mut i = 0;
    // 跳过前导字母
    while i < bytes.len() && bytes[i].is_ascii_alphabetic() { i += 1; }
    // 取连续 4 位数字
    if i + 4 <= bytes.len() && bytes[i..i + 4].iter().all(|b| b.is_ascii_digit()) {
        let s = std::str::from_utf8(&bytes[i..i + 4]).ok()?;
        return s.parse::<u32>().ok();
    }
    None
}

/// 只保留 YYMM >= 2701（2027 年 1 月及以后）的合约。
pub fn keep_after_2701(code: &str) -> bool {
    match extract_yymm(code) {
        Some(yymm) => yymm >= 2701,
        None => false, // 无法解析年月的一律丢弃，避免误订
    }
}

/// 按品种前缀归交易所。匹配时统一转大写。
pub fn classify_exchange(code: &str) -> Option<Exchange> {
    // 纯 8 位数字 -> 上交所 ETF 期权
    if code.len() == 8 && code.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return Some(Exchange::Sse);
    }
    let upper = code.to_ascii_uppercase();
    let prefix: String = upper.chars().take_while(|c| c.is_ascii_alphabetic()).collect();
    let p = prefix.as_str();

    // 上期所 / 上期能源
    const SHFE_INE: &[&str] = &["RB","AL","CU","AU","AG","NI","ZN","PB","SN","SC","NR","BC"];
    if SHFE_INE.contains(&p) {
        return Some(match p {
            "SC" | "NR" | "BC" => Exchange::Ine,
            _ => Exchange::Shfe,
        });
    }
    // 大商所
    const DCE: &[&str] = &["M","I","J","JM","CS","V","PP","L","PVC","EG","EB","PG","LH"];
    if DCE.contains(&p) { return Some(Exchange::Dce); }
    // 郑商所
    const CZCE: &[&str] = &["SR","CF","TA","MA","OI","RM","SA","FG","PF","PX","SH","AP","CJ","UR"];
    if CZCE.contains(&p) { return Some(Exchange::Czce); }
    // 中金所
    const CFFEX: &[&str] = &["IO","HO","MO"];
    if CFFEX.contains(&p) { return Some(Exchange::Cffex); }
    // 广期所
    const GFEX: &[&str] = &["SI","LC"];
    if GFEX.contains(&p) { return Some(Exchange::Gfex); }

    None
}

/// 读取合约文件：每行一个代码，跳过空行和 # 注释。
pub fn load_contracts(path: &Path) -> std::io::Result<Vec<InstrumentId>> {
    let content = fs::read_to_string(path)?;
    let mut out = Vec::new();
    for line in content.lines() {
        let t = line.trim();
        if t.is_empty() || t.starts_with('#') { continue; }
        // 兼容 CSV：取第一列
        let code = t.split(',').next().unwrap_or(t).trim();
        if !code.is_empty() {
            out.push(InstrumentId(code.to_string()));
        }
    }
    Ok(out)
}

/// 分片配置：每个 member（成员）负责的交易所列表。
#[derive(Debug, Clone)]
pub struct MemberSpec {
    pub member_idx: usize,
    pub exchanges: Vec<Exchange>,
}

/// 分片结果：member_idx -> 合约列表。
pub type ShardMap = HashMap<usize, Vec<InstrumentId>>;

/// 分片规则：先按交易所分组，组内轮询分给负责该交易所的 member（成员）。
/// 保证每个合约有且只有一个归属。
pub fn shard_contracts(contracts: &[InstrumentId], members: &[MemberSpec]) -> ShardMap {
    // 交易所 -> 合约列表
    let mut by_ex: BTreeMap<Exchange, Vec<&InstrumentId>> = BTreeMap::new();
    let mut unclassified: Vec<&InstrumentId> = Vec::new();
    for c in contracts {
        match classify_exchange(c.as_str()) {
            Some(ex) => by_ex.entry(ex).or_default().push(c),
            None => unclassified.push(c),
        }
    }
    if !unclassified.is_empty() {
        eprintln!("[WARN] {} 个合约无法归类交易所，已丢弃:", unclassified.len());
        for c in unclassified.iter().take(5) {
            eprintln!("  - {}", c.as_str());
        }
    }

    let mut out: ShardMap = HashMap::new();
    for (ex, list) in &by_ex {
        // 找负责该交易所的 member（成员）
        let owners: Vec<usize> = members
            .iter()
            .filter(|m| m.exchanges.contains(ex))
            .map(|m| m.member_idx)
            .collect();
        if owners.is_empty() {
            eprintln!("[WARN] 交易所 {} 无 member（成员）负责，{} 个合约丢弃", ex.as_ctp_str(), list.len());
            continue;
        }
        for (i, c) in list.iter().enumerate() {
            let m = owners[i % owners.len()];
            out.entry(m).or_default().push((*c).clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_yymm() {
        assert_eq!(extract_yymm("EG2706P4900"), Some(2706));
        assert_eq!(extract_yymm("pp2701"), Some(2701));
        assert_eq!(extract_yymm("IO2701-C-4000"), Some(2701));
        assert_eq!(extract_yymm("10008000"), None); // 纯数字 ETF 期权无 YYMM
    }

    #[test]
    fn test_filter_2701() {
        assert!(keep_after_2701("EG2706P4900"));
        assert!(keep_after_2701("pp2701"));
        assert!(!keep_after_2701("EG2605P4900"));
        assert!(!keep_after_2701("10008000"));
    }

    #[test]
    fn test_classify() {
        assert_eq!(classify_exchange("EG2706P4900"), Some(Exchange::Dce));
        assert_eq!(classify_exchange("pp2701"), Some(Exchange::Dce));
        assert_eq!(classify_exchange("rb2705"), Some(Exchange::Shfe));
        assert_eq!(classify_exchange("SR701"), Some(Exchange::Czce));
        assert_eq!(classify_exchange("IO2701-C-4000"), Some(Exchange::Cffex));
        assert_eq!(classify_exchange("10008000"), Some(Exchange::Sse));
    }

    #[test]
    fn test_shard_no_dup_no_miss() {
        let contracts: Vec<InstrumentId> = vec![
            "EG2706P4900", "EG2706P5000", "pp2701", "rb2705", "SR701",
            "IO2701-C-4000", "si2701", "10008000",
        ].into_iter().map(|s| InstrumentId(s.into())).collect();

        let members: Vec<MemberSpec> = (0..4).map(|i| MemberSpec {
            member_idx: i,
            exchanges: vec![Exchange::Sse, Exchange::Shfe, Exchange::Ine,
                            Exchange::Dce, Exchange::Czce, Exchange::Cffex, Exchange::Gfex],
        }).collect();

        let shards = shard_contracts(&contracts, &members);
        let mut all: Vec<&InstrumentId> = shards.values().flatten().collect();
        all.sort();
        let mut expected: Vec<&InstrumentId> = contracts.iter().collect();
        expected.sort();
        assert_eq!(all, expected, "每个合约必须有且只有一个归属");
    }
}

//! 合约清单加载与 member 分片。
use std::collections::HashMap;
use std::path::Path;

// ============================================================
// 合约（照抄 v1.1）
// ============================================================

#[derive(Debug, Clone)]
pub(crate) struct Contract {
    code: String,
    exchange: String,
}

pub(crate) fn load_contracts_csv(path: &Path) -> Result<Vec<Contract>, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("读 {} 失败: {}", path.display(), e))?;
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() < 2 {
            continue;
        }
        out.push(Contract {
            code: parts[0].to_string(),
            exchange: parts[1].to_string(),
        });
    }
    Ok(out)
}

/// 按交易所分组，组内轮询分给 N 个 member（与 v1.1 一致，结果稳定可复现）。
pub(crate) fn shard_contracts(contracts: &[Contract], num_members: usize) -> Vec<Vec<String>> {
    let mut shards: Vec<Vec<String>> = vec![Vec::new(); num_members];
    let mut by_ex: HashMap<String, Vec<&str>> = HashMap::new();
    for c in contracts {
        by_ex.entry(c.exchange.clone()).or_default().push(&c.code);
    }
    let mut ex_keys: Vec<&String> = by_ex.keys().collect();
    ex_keys.sort();
    for ex in ex_keys {
        let codes = &by_ex[ex];
        for (i, code) in codes.iter().enumerate() {
            shards[i % num_members].push(code.to_string());
        }
        tracing::info!("[SHARD] 交易所 {}: {} 条", ex, codes.len());
    }
    shards
}

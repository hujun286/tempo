//! 行情快照：内存 HashMap，按 InstrumentId 覆盖，按 UpdateTime+Millisec 去重。
//! 高频模式走内存直接读；低频模式由后台线程定时落盘。

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use common_model::{InstrumentId, Tick};

#[derive(Clone, Copy, Debug)]
pub enum SnapshotMode { HighFrequency, LowFrequency }

pub struct MarketSnapshot {
    map: Arc<RwLock<HashMap<InstrumentId, Tick>>>,
    mode: SnapshotMode,
}

impl MarketSnapshot {
    pub fn new(mode: SnapshotMode) -> Self {
        Self { map: Arc::new(RwLock::new(HashMap::new())), mode }
    }

    /// 更新快照。返回 true 表示该 tick 有效（非重复）。
    pub fn update(&self, tick: Tick) -> bool {
        let mut t = tick;
        t.sanitize();
        let key = t.instrument.clone();
        let dedup_key = (t.update_time.clone(), t.update_millisec);
        let mut w = self.map.write().unwrap();
        if let Some(old) = w.get(&key) {
            if old.dedup_key() == (dedup_key.0.as_str(), dedup_key.1) {
                return false; // 重复
            }
        }
        w.insert(key, t);
        true
    }

    pub fn get(&self, key: &InstrumentId) -> Option<Tick> {
        self.map.read().unwrap().get(key).cloned()
    }

    /// 快照新鲜度：最后一个 tick 距今毫秒。
    pub fn age_ms(&self, key: &InstrumentId) -> Option<u128> {
        let r = self.map.read().unwrap();
        r.get(key)
            .and_then(|t| t.received_at)
            .map(|i| Instant::now().duration_since(i).as_millis())
    }

    pub fn len(&self) -> usize { self.map.read().unwrap().len() }
    pub fn is_empty(&self) -> bool { self.len() == 0 }
    pub fn mode(&self) -> SnapshotMode { self.mode }

    /// 导出为可落盘的 JSON 行数组。
    pub fn dump_jsonl(&self) -> String {
        let r = self.map.read().unwrap();
        let mut out = String::new();
        for t in r.values() {
            if let Ok(s) = serde_json::to_string(t) {
                out.push_str(&s);
                out.push('\n');
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tick(instr: &str, time: &str, ms: u32, price: f64) -> Tick {
        Tick {
            instrument: InstrumentId::new(instr),
            update_time: time.into(),
            update_millisec: ms,
            last_price: price,
            bid_price1: price - 0.1,
            ask_price1: price + 0.1,
            volume: 1,
            open_interest: 1.0,
            received_at: Some(Instant::now()),
        }
    }

    #[test]
    fn update_overwrites_by_instrument() {
        let s = MarketSnapshot::new(SnapshotMode::HighFrequency);
        assert!(s.update(tick("pp2701", "09:00:00", 0, 1.0)));
        assert!(s.update(tick("pp2701", "09:00:01", 0, 2.0)));
        assert_eq!(s.len(), 1);
        assert_eq!(s.get(&InstrumentId::new("pp2701")).unwrap().last_price, 2.0);
    }

    #[test]
    fn duplicate_is_rejected() {
        let s = MarketSnapshot::new(SnapshotMode::HighFrequency);
        assert!(s.update(tick("pp2701", "09:00:00", 500, 1.0)));
        assert!(!s.update(tick("pp2701", "09:00:00", 500, 1.0)));
    }

    #[test]
    fn huge_prices_sanitized() {
        let s = MarketSnapshot::new(SnapshotMode::HighFrequency);
        let mut t = tick("pp2701", "09:00:00", 0, 1.0);
        t.bid_price1 = 1e308;
        s.update(t);
        assert_eq!(s.get(&InstrumentId::new("pp2701")).unwrap().bid_price1, 0.0);
    }
}

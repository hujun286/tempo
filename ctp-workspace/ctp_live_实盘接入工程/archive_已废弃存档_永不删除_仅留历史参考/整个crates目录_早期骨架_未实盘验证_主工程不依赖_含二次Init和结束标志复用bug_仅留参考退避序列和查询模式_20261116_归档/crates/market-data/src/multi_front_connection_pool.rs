//! 连接池：16 个 member 并行，各自独立连接。留 1 个 IP 给交易。

use std::sync::Arc;
use crossbeam_channel::{Receiver, Select};
use common_model::{InstrumentId, Tick};
use crate::adapter::MdAdapter;
use crate::market_data_connection_member::{spawn, MemberConfig, MemberHandle, MemberStatus};

pub struct PoolConfig {
    pub member_ips: Vec<String>,
    pub md_port: u16,
    pub broker: String,
    pub user: String,
    pub pwd: String,
    pub app_id: String,
    pub auth_code: String,
    pub need_authenticate: bool,
}

pub struct Pool {
    pub handles: Vec<MemberHandle>,
    pub tick_rx: Receiver<Tick>,
    pub status_rx: Receiver<MemberStatus>,
}

pub fn start_pool(
    cfg: PoolConfig,
    shards: common_model::ShardMap,
    api_factory: impl Fn(usize) -> Arc<dyn MdAdapter>,
) -> Pool {
    let (tick_tx, tick_rx) = crossbeam_channel::unbounded();
    let (status_tx, status_rx) = crossbeam_channel::unbounded();

    let mut handles = Vec::new();
    for (idx, ip) in cfg.member_ips.iter().enumerate() {
        let instruments: Vec<InstrumentId> = shards.get(&idx).cloned().unwrap_or_default();
        let front = format!("tcp://{}:{}", ip, cfg.md_port);
        let mcfg = MemberConfig { member_idx: idx, front, instruments };
        let api = api_factory(idx);
        let mut h = spawn(
            api, mcfg,
            cfg.broker.clone(), cfg.user.clone(), cfg.pwd.clone(),
            cfg.app_id.clone(), cfg.auth_code.clone(),
            cfg.need_authenticate,
        );
        h.member_idx = idx;

        // 把 member 的 tick/status 汇聚到池
        let ttx = tick_tx.clone();
        let stx = status_tx.clone();
        let trx = h.tick_rx.clone();
        let srx = h.status_rx.clone();
        std::thread::spawn(move || {
            let mut sel = Select::new();
            let ti = sel.recv(&trx);
            let si = sel.recv(&srx);
            loop {
                let op = sel.select();
                match op.index() {
                    x if x == ti => match trx.recv() {
                        Ok(t) => { let _ = ttx.send(t); }
                        Err(_) => break,
                    },
                    x if x == si => match srx.recv() {
                        Ok(s) => { let _ = stx.send(s); }
                        Err(_) => break,
                    },
                    _ => break,
                }
            }
        });
        handles.push(h);
    }

    Pool { handles, tick_rx, status_rx }
}

#[cfg(test)]
mod tests {
    #[test]
    fn pool_placeholder() { assert!(true); }
}

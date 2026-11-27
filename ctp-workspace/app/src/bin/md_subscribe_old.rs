//! 今晚跑：读老合约 2701 之后过滤，分片给 16 个 member，连实盘 :61213 订阅。

use std::path::Path;
use std::sync::Arc;
use common_model::{Exchange, InstrumentId};
use market_data::instrument_sharding_allocator::{
    keep_after_2701, load_contracts, shard_contracts, MemberSpec,
};
use market_data::market_data_connection_member::BATCH_SIZE;
use market_data::multi_front_connection_pool::{start_pool, PoolConfig};

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("用法: md_subscribe_old <contracts_file>");
        std::process::exit(2);
    }
    let path = Path::new(&args[1]);
    let all = load_contracts(path)?;
    println!("[MD] 读入 {} 个合约", all.len());

    let filtered: Vec<InstrumentId> = all.into_iter().filter(|c| keep_after_2701(c.as_str())).collect();
    println!("[MD] 过滤 2701 之后: {} 个", filtered.len());

    let secrets = common_model::load_secrets(Path::new("config/secrets.toml"))?;
    let md = secrets.network;

    let members: Vec<MemberSpec> = (0..md.member_ips.len()).map(|i| MemberSpec {
        member_idx: i,
        exchanges: vec![Exchange::Sse, Exchange::Shfe, Exchange::Ine,
                        Exchange::Dce, Exchange::Czce, Exchange::Cffex, Exchange::Gfex],
    }).collect();
    let shards = shard_contracts(&filtered, &members);
    println!("[MD] 分片: {} 个 member", shards.len());
    for (idx, list) in &shards {
        println!("  member#{:02}: {} 个合约", idx, list.len());
    }

    let cfg = PoolConfig {
        member_ips: md.member_ips.clone(),
        md_port: md.md_port,
        broker: secrets.trading_account.broker_id.clone(),
        user: secrets.trading_account.user_id.clone(),
        pwd: secrets.trading_account.password.clone(),
        app_id: secrets.trading_account.app_id.clone(),
        auth_code: secrets.trading_account.auth_code.clone(),
        need_authenticate: true,
    };
    let flow_md = md.flow_dir.clone();
    let pool = start_pool(cfg, shards, move |_i| {
        market_data::ctp2rs_backend::Ctp2rsMdAdapter::new(&flow_md)
    });

    let snap = Arc::new(market_data::market_snapshot::MarketSnapshot::new(
        market_data::market_snapshot::SnapshotMode::HighFrequency,
    ));
    let snap2 = snap.clone();

    // 汇聚 tick
    let tick_rx = pool.tick_rx.clone();
    std::thread::spawn(move || {
        let mut total = 0u64;
        for t in tick_rx {
            if snap2.update(t) { total += 1; }
            if total % 10000 == 0 {
                println!("[MD] tick 累计 {}，快照 {} 个合约", total, snap2.len());
            }
        }
    });

    // 每 60 秒打印状态
    std::thread::spawn(move || loop {
        std::thread::sleep(std::time::Duration::from_secs(60));
        println!("[MD] 快照合约数={}，单 member 每批 {} 个", snap.len(), BATCH_SIZE);
    });

    // 运行到手动 Ctrl-C
    loop { std::thread::sleep(std::time::Duration::from_secs(1)); }
}

//! 明天开盘前跑：交易进程连实盘 :61205，ReqQryInstrument 查全量合约落文件。

use std::path::Path;
use std::sync::Arc;
use crossbeam_channel::unbounded;
use common_model::load_secrets;
use trading::account_position_order_query::query_all_instruments;
use trading::adapter::TdAdapter;
use trading::trading_session_authentication::open_session;

fn main() -> anyhow::Result<()> {
    let secrets = load_secrets(Path::new("config/secrets.toml"))?;
    let td = secrets.trading_account;
    let md = secrets.network;
    let cfg = common_model::TradingConfig {
        broker_id: td.broker_id.clone(),
        user_id: td.user_id.clone(),
        password: td.password.clone(),
        app_id: td.app_id.clone(),
        auth_code: td.auth_code.clone(),
        primary_front_ip: md.trade_reserved_ip.clone(),
        td_port: md.td_port,
        flow_dir: "./data/flow/td".into(),
        settlement_confirm_on_login: true,
        hard_limits: common_model::HardLimits {
            max_lots_per_order: 1,
            max_premium_per_lot: 5.0,
            max_orders_per_minute: 3,
            allow_market_order: false,
        },
    };
    let api: Arc<dyn TdAdapter> = trading::ctp2rs_backend::Ctp2rsTdAdapter::new(&cfg.flow_dir);
    let (tx, rx) = unbounded();
    api.register_spi(tx);
    api.register_front(&format!("tcp://{}:{}", cfg.primary_front_ip, cfg.td_port));
    api.init();

    let _session = open_session(api.as_ref(), &rx, &cfg)?;
    println!("[TD] 登录成功，开始查询合约");
    let list = query_all_instruments(api.as_ref(), &rx)?;
    println!("[TD] 查到 {} 个合约", list.len());

    let out = Path::new("data/contracts/contracts_live.txt");
    let mut text = String::new();
    for (code, ex, prod) in &list {
        text.push_str(&format!("{},{},{}\n", code, ex, prod));
    }
    std::fs::write(out, text)?;
    println!("[TD] 已写入 {}", out.display());
    api.release();
    Ok(())
}

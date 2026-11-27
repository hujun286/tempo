//! 明天跑：查资金/持仓 → 挂不成交单 → 撤单确认。每步等待人工确认。
//! 用法：cargo run -p app --bin td_min_verify
//! 报单合约在下方常量指定，明天改成实际合约代码。

use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use crossbeam_channel::unbounded;
use common_model::{
    load_secrets, CancelRequest, Direction, InstrumentId, OffsetFlag, OrderPriceType, OrderRequest,
    TradingConfig,
};
use trading::account_position_order_query::{query_account, query_positions};
use trading::adapter::TdAdapter;
use trading::hard_limits::HardLimitGuard;
use trading::order_cancel_request_sender;
use trading::order_insert_request_sender;
use trading::trading_session_authentication::open_session;

const VERIFY_INSTRUMENT: &str = "pp2701"; // 明天改成实际合约代码
const VERIFY_PRICE: f64 = 1.0;             // 远离盘口不成交价
const VERIFY_VOLUME: i32 = 1;

fn main() -> anyhow::Result<()> {
    let secrets = load_secrets(Path::new("config/secrets.toml"))?;
    let td = secrets.trading_account;
    let md = secrets.network;
    let cfg = TradingConfig {
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
    let session = open_session(api.as_ref(), &rx, &cfg)?;
    println!("[TD] 登录成功 session={} front={}", session.session_id, session.front_id);

    // 步骤 1：查资金/持仓
    confirm("步骤1：查询资金和持仓，是否继续？")?;
    let available = query_account(api.as_ref(), &rx).map_err(|e| anyhow::anyhow!(e))?;
    println!("[TD] 可用资金: {:.2}", available);
    let positions = query_positions(api.as_ref(), &rx).map_err(|e| anyhow::anyhow!(e))?;
    println!("[TD] 持仓 {} 条: {:?}", positions.len(), positions);

    // 步骤 2：挂不成交单
    confirm("步骤2：挂 1 手远离盘口不成交限价单，是否继续？")?;
    let mut guard = HardLimitGuard::new(cfg.hard_limits.clone());
    let order = OrderRequest {
        instrument: InstrumentId::new(VERIFY_INSTRUMENT),
        direction: Direction::Buy,
        offset: OffsetFlag::Open,
        price: VERIFY_PRICE,
        volume: VERIFY_VOLUME,
        price_type: OrderPriceType::Limit,
    };
    guard.check_insert(&order).map_err(|e| anyhow::anyhow!("硬保险拒绝: {:?}", e))?;
    let order_ref = order_insert_request_sender::make_order_ref(&cfg, 1);
    order_insert_request_sender::send(api.as_ref(), &order, &order_ref);
    guard.record_sent();
    println!("[TD] 已发送报单 order_ref={}", order_ref);

    // 等回报
    wait_order_accepted(&rx, &order_ref)?;

    // 步骤 3：撤单
    confirm("步骤3：撤单，是否继续？")?;
    guard.check_cancel().map_err(|e| anyhow::anyhow!("硬保险拒绝撤单: {:?}", e))?;
    let cancel = CancelRequest {
        instrument: InstrumentId::new(VERIFY_INSTRUMENT),
        order_ref: order_ref.clone(),
        front_id: session.front_id,
        session_id: session.session_id,
    };
    order_cancel_request_sender::send(api.as_ref(), &cancel);
    guard.record_sent();
    wait_cancel_confirmed(&rx, &order_ref)?;

    println!("[TD] 最小验证完成，未做开平仓测试。");
    api.release();
    Ok(())
}

fn confirm(msg: &str) -> anyhow::Result<()> {
    print!("{} [y/N] ", msg);
    std::io::stdout().flush()?;
    let mut s = String::new();
    std::io::stdin().read_line(&mut s)?;
    if s.trim().to_lowercase() == "y" { Ok(()) } else { anyhow::bail!("人工取消") }
}

fn wait_order_accepted(rx: &crossbeam_channel::Receiver<trading::adapter::TdEvent>, order_ref: &str)
    -> anyhow::Result<()>
{
    loop {
        match rx.recv_timeout(std::time::Duration::from_secs(15)) {
            Ok(trading::adapter::TdEvent::OrderAccepted { order_ref: r, sys_id }) if r == order_ref => {
                println!("[ORDER ACCEPTED] sys_id={}", sys_id);
                return Ok(());
            }
            Ok(trading::adapter::TdEvent::OrderRejected { order_ref: r, error_id, error_msg }) if r == order_ref => {
                anyhow::bail!("报单被拒 ErrorID={} {}", error_id, error_msg);
            }
            Ok(_) => continue,
            Err(_) => anyhow::bail!("等报单回报超时"),
        }
    }
}

fn wait_cancel_confirmed(rx: &crossbeam_channel::Receiver<trading::adapter::TdEvent>, order_ref: &str)
    -> anyhow::Result<()>
{
    loop {
        match rx.recv_timeout(std::time::Duration::from_secs(15)) {
            Ok(trading::adapter::TdEvent::CancelConfirmed { order_ref: r, .. }) if r == order_ref => {
                println!("[CANCEL CONFIRMED] order_ref={}", r);
                return Ok(());
            }
            Ok(trading::adapter::TdEvent::CancelRejected { order_ref: r, error_id, error_msg }) if r == order_ref => {
                anyhow::bail!("撤单被拒 ErrorID={} {}", error_id, error_msg);
            }
            Ok(_) => continue,
            Err(_) => anyhow::bail!("等撤单回报超时"),
        }
    }
}

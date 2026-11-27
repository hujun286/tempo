use ctp2rs::v1alpha1::*;
use rust_ctp_test::config::{self, appid, authcode, broker, front_url, password, user};
use std::os::raw::c_char;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::time::{Duration, Instant};

// ============ 配置 ============
// 编译期展开为绝对路径，不依赖运行时 cwd。
// 原 "lib/..." 是相对当前工作目录，必须在 ctp_live_实盘接入工程/ 下启动才找得到。
const TD_DYNLIB: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/lib/libthosttraderapi_se.so");

// ============ 字符串工具 ============
fn copy_str(buf: &mut [c_char], s: &str) {
    let bytes = s.as_bytes();
    let n = bytes.len().min(buf.len().saturating_sub(1));
    for i in 0..n {
        buf[i] = bytes[i] as c_char;
    }
    if n < buf.len() {
        buf[n] = 0;
    }
}

fn read_str(buf: &[c_char]) -> String {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let bytes: Vec<u8> = buf[..end].iter().map(|&b| b as u8).collect();
    if bytes.is_empty() {
        return String::new();
    }
    let (decoded, _, had_errors) = encoding_rs::GB18030.decode(&bytes);
    if had_errors {
        String::from_utf8_lossy(&bytes).trim().to_string()
    } else {
        decoded.trim().to_string()
    }
}

fn rsp_err(info: Option<&CThostFtdcRspInfoField>) -> Option<(i32, String)> {
    info.and_then(|i| {
        if i.ErrorID != 0 {
            Some((i.ErrorID, read_str(&i.ErrorMsg)))
        } else {
            None
        }
    })
}

// ============ 事件 ============
#[derive(Debug)]
enum Event {
    Connected,
    Disconnected,
    AuthRsp(Option<(i32, String)>),
    LoginRsp {
        err: Option<(i32, String)>,
        front_id: i32,
        session_id: i32,
        trading_day: String,
    },
    SettleRsp(Option<(i32, String)>),
    InsertRsp(Option<(i32, String)>),
    ActionRsp(Option<(i32, String)>),
    RtnOrder {
        sys_id: String,
        status: i8,
        traded: i32,
        total: i32,
    },
    RtnTrade {
        price: f64,
        volume: i32,
    },
    ErrRtnInsert(Option<(i32, String)>),
    ErrRtnAction(Option<(i32, String)>),
}

struct MySpi {
    tx: Sender<Event>,
}

impl TraderSpi for MySpi {
    fn on_front_connected(&mut self) {
        let _ = self.tx.send(Event::Connected);
    }
    fn on_front_disconnected(&mut self, r: i32) {
        eprintln!(
            "[SPI] on_front_disconnected reason={}  <-- C++ 回调已触发",
            r
        );
        let _ = self.tx.send(Event::Disconnected);
    }
    fn on_rsp_authenticate(
        &mut self,
        _rsp: Option<&CThostFtdcRspAuthenticateField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        _last: bool,
    ) {
        let _ = self.tx.send(Event::AuthRsp(rsp_err(info)));
    }
    fn on_rsp_user_login(
        &mut self,
        rsp: Option<&CThostFtdcRspUserLoginField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        _last: bool,
    ) {
        let err = rsp_err(info);
        let (f, s, d) = rsp
            .map(|r| (r.FrontID, r.SessionID, read_str(&r.TradingDay)))
            .unwrap_or((0, 0, String::new()));
        let _ = self.tx.send(Event::LoginRsp {
            err,
            front_id: f,
            session_id: s,
            trading_day: d,
        });
    }
    fn on_rsp_settlement_info_confirm(
        &mut self,
        _rsp: Option<&CThostFtdcSettlementInfoConfirmField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        _last: bool,
    ) {
        let _ = self.tx.send(Event::SettleRsp(rsp_err(info)));
    }
    fn on_rsp_order_insert(
        &mut self,
        _o: Option<&CThostFtdcInputOrderField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        _last: bool,
    ) {
        let _ = self.tx.send(Event::InsertRsp(rsp_err(info)));
    }
    fn on_rsp_order_action(
        &mut self,
        _a: Option<&CThostFtdcInputOrderActionField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        _last: bool,
    ) {
        let _ = self.tx.send(Event::ActionRsp(rsp_err(info)));
    }
    fn on_err_rtn_order_insert(
        &mut self,
        _o: Option<&CThostFtdcInputOrderField>,
        info: Option<&CThostFtdcRspInfoField>,
    ) {
        let _ = self.tx.send(Event::ErrRtnInsert(rsp_err(info)));
    }
    fn on_err_rtn_order_action(
        &mut self,
        _a: Option<&CThostFtdcOrderActionField>,
        info: Option<&CThostFtdcRspInfoField>,
    ) {
        let _ = self.tx.send(Event::ErrRtnAction(rsp_err(info)));
    }
    fn on_rtn_order(&mut self, order: Option<&CThostFtdcOrderField>) {
        if let Some(o) = order {
            let _ = self.tx.send(Event::RtnOrder {
                sys_id: read_str(&o.OrderSysID),
                status: o.OrderStatus,
                traded: o.VolumeTraded,
                total: o.VolumeTotal,
            });
        }
    }
    fn on_rtn_trade(&mut self, trade: Option<&CThostFtdcTradeField>) {
        if let Some(t) = trade {
            let _ = self.tx.send(Event::RtnTrade {
                price: t.Price,
                volume: t.Volume,
            });
        }
    }
}

fn wait_event<F: Fn(&Event) -> bool>(
    rx: &Receiver<Event>,
    timeout: Duration,
    pred: F,
) -> Option<Event> {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match rx.recv_timeout(remaining) {
            Ok(ev) => {
                println!("[EVENT] {:?}", ev);
                if pred(&ev) {
                    return Some(ev);
                }
            }
            Err(_) => return None,
        }
    }
}

// ============ 公共会话：认证 + 登录 + 结算确认 ============
fn open_session(api: &TraderApi, rx: &Receiver<Event>) -> Result<(i32, i32), String> {
    // 认证
    let mut auth = CThostFtdcReqAuthenticateField::default();
    copy_str(&mut auth.BrokerID, broker());
    copy_str(&mut auth.UserID, user());
    copy_str(&mut auth.AppID, appid());
    copy_str(&mut auth.AuthCode, authcode());
    println!("[AUTH REQ] ret={}", api.req_authenticate(&mut auth, 1));
    match wait_event(rx, Duration::from_secs(10), |e| {
        matches!(e, Event::AuthRsp(..))
    }) {
        Some(Event::AuthRsp(None)) => println!("[AUTH SUCCESS]"),
        Some(Event::AuthRsp(Some((id, m)))) => return Err(format!("认证: {} {}", id, m)),
        _ => return Err("认证超时".into()),
    }

    // 登录
    let mut login = CThostFtdcReqUserLoginField::default();
    copy_str(&mut login.BrokerID, broker());
    copy_str(&mut login.UserID, user());
    copy_str(&mut login.Password, password());
    println!("[LOGIN REQ] ret={}", api.req_user_login(&mut login, 2));
    let (front_id, session_id) = match wait_event(rx, Duration::from_secs(15), |e| {
        matches!(e, Event::LoginRsp { .. })
    }) {
        Some(Event::LoginRsp {
            err: None,
            front_id,
            session_id,
            trading_day,
        }) => {
            println!(
                "[LOGIN SUCCESS] TradingDay={} FrontID={} SessionID={}",
                trading_day, front_id, session_id
            );
            (front_id, session_id)
        }
        Some(Event::LoginRsp {
            err: Some((id, m)), ..
        }) => return Err(format!("登录: {} {}", id, m)),
        _ => return Err("登录超时".into()),
    };

    // 结算确认（每天必须，否则报单 ErrorID=42）
    let mut settle = CThostFtdcSettlementInfoConfirmField::default();
    copy_str(&mut settle.BrokerID, broker());
    copy_str(&mut settle.InvestorID, user());
    println!(
        "[SETTLE REQ] ret={}",
        api.req_settlement_info_confirm(&mut settle, 3)
    );
    match wait_event(rx, Duration::from_secs(10), |e| {
        matches!(e, Event::SettleRsp(..))
    }) {
        Some(Event::SettleRsp(None)) => println!("[SETTLE SUCCESS]"),
        Some(Event::SettleRsp(Some((id, m)))) => return Err(format!("结算: {} {}", id, m)),
        _ => return Err("结算确认超时".into()),
    }

    Ok((front_id, session_id))
}

// ============ 2.2 基础交易 ============
fn run_login_only(api: &TraderApi, rx: &Receiver<Event>) -> Result<(), Box<dyn std::error::Error>> {
    if wait_event(rx, Duration::from_secs(10), |e| {
        matches!(e, Event::Connected)
    })
    .is_none()
    {
        return Err("10 秒内未连接".into());
    }
    println!("[LOGIN] OnFrontConnected");

    let (front_id, session_id) = open_session(api, rx)?;
    println!(
        "[LOGIN] 登录成功 FrontID={} SessionID={}",
        front_id, session_id
    );

    let mut q = CThostFtdcQryTradingAccountField::default();
    copy_str(&mut q.BrokerID, broker());
    copy_str(&mut q.InvestorID, user());
    let ret = api.req_qry_trading_account(&mut q, 10);
    println!("[LOGIN] 查资金请求 ret={}", ret);

    let mut p = CThostFtdcQryInvestorPositionField::default();
    copy_str(&mut p.BrokerID, broker());
    copy_str(&mut p.InvestorID, user());
    let ret = api.req_qry_investor_position(&mut p, 11);
    println!("[LOGIN] 查持仓请求 ret={}", ret);

    std::thread::sleep(Duration::from_secs(10));
    println!("[LOGIN] 登录+查询完成，不报单。");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 交易 flow 目录：从 secrets.toml [trading].flow_dir 读，基于 WORKSPACE_ROOT 绝对化。
    let td_flow = config::resolve_workspace_path(&config::global().trading.flow_dir);
    std::fs::create_dir_all(&td_flow)?;
    let td_flow_str = td_flow.to_str().ok_or("trading.flow_dir 非 UTF-8")?;

    let api = TraderApiBuilder::new()
        .with_dynlib(TD_DYNLIB)
        .flow_path(td_flow_str)
        .build()
        .map_err(|e| format!("build: {}", e))?;

    let (tx, rx) = channel();
    let spi_box: Box<dyn TraderSpi> = Box::new(MySpi { tx });
    let spi_ptr: *mut dyn TraderSpi = Box::into_raw(spi_box);
    api.register_spi(spi_ptr);
    api.subscribe_private_topic(THOST_TE_RESUME_TYPE::THOST_TERT_QUICK);
    api.subscribe_public_topic(THOST_TE_RESUME_TYPE::THOST_TERT_QUICK);
    api.register_front(front_url());
    api.init();

    let mode = std::env::var("CTP_TEST").unwrap_or_else(|_| "login".to_string());
    match mode.as_str() {
        "order" => run_order_with_guard(&api, &rx),
        "login" => run_login_only(&api, &rx),
        _ => Err(format!("未知 CTP_TEST={}，默认不报单。可用模式: login / order（循环报单函数已归档，实盘请用 order 模式）", mode).into()),
    }
}

// ============================================================
// 硬保险与受控单笔报单入口
// ============================================================

mod hard_limits;

/// 受硬保险保护的单笔报单入口。
///
/// 环境变量（全部必须设置，缺一不报）：
///   CTP_ORDER_INSTRUMENT 合约代码，如 pp2701
///   CTP_ORDER_PRICE      限价价格，如 1.0
///   CTP_ORDER_VOLUME     手数，如 1
///   CTP_ORDER_DIRECTION  buy 或 sell
///   CTP_ORDER_OFFSET     open / close / close_today / close_yesterday
///
/// 流程：读配置 → 登录 → 打印待报内容 → 等人工输入 y → 硬保险 check → 报单 → record → 等回报。
fn run_order_with_guard(
    api: &TraderApi,
    rx: &Receiver<Event>,
) -> Result<(), Box<dyn std::error::Error>> {
    // 1. 读订单参数
    let instrument =
        std::env::var("CTP_ORDER_INSTRUMENT").map_err(|_| "缺少环境变量 CTP_ORDER_INSTRUMENT")?;
    let price_str = std::env::var("CTP_ORDER_PRICE").map_err(|_| "缺少 CTP_ORDER_PRICE")?;
    let price: f64 = price_str.parse().map_err(|_| "CTP_ORDER_PRICE 不是数字")?;
    let volume: i32 = std::env::var("CTP_ORDER_VOLUME")
        .map_err(|_| "缺少 CTP_ORDER_VOLUME")?
        .parse()
        .map_err(|_| "CTP_ORDER_VOLUME 不是整数")?;
    let dir_str = std::env::var("CTP_ORDER_DIRECTION").map_err(|_| "缺少 CTP_ORDER_DIRECTION")?;
    let direction = match dir_str.as_str() {
        "buy" => THOST_FTDC_D_Buy,
        "sell" => THOST_FTDC_D_Sell,
        other => return Err(format!("CTP_ORDER_DIRECTION 只能是 buy/sell，收到 {}", other).into()),
    };
    let off_str = std::env::var("CTP_ORDER_OFFSET").map_err(|_| "缺少 CTP_ORDER_OFFSET")?;
    let offset = match off_str.as_str() {
        "open" => THOST_FTDC_OF_Open,
        "close" => THOST_FTDC_OF_Close,
        "close_today" => THOST_FTDC_OF_CloseToday,
        "close_yesterday" => THOST_FTDC_OF_CloseYesterday,
        other => return Err(format!("CTP_ORDER_OFFSET 非法: {}", other).into()),
    };

    // 2. 加载硬保险
    let mut guard =
        hard_limits::Guard::load_from_secrets(std::path::Path::new(config::DEFAULT_SECRETS))?;
    eprintln!(
        "[GUARD] 硬保险: 单笔 ≤{} 手, 单价 ≤{} 元/张, 每分钟报单+撤单 ≤{} 次, 市价单={}",
        guard.config().max_lots_per_order,
        guard.config().max_premium_per_lot,
        guard.config().max_orders_per_minute,
        if guard.config().allow_market_order {
            "允许"
        } else {
            "禁止"
        },
    );

    // 3. 登录
    if wait_event(rx, Duration::from_secs(10), |e| {
        matches!(e, Event::Connected)
    })
    .is_none()
    {
        return Err("10 秒内未连接".into());
    }
    let (front_id, session_id) = open_session(api, rx)?;
    eprintln!("[ORDER] 登录 FrontID={} SessionID={}", front_id, session_id);

    // 4. 打印待报内容，人工确认
    eprintln!(
        "[ORDER] 即将报单: 合约={} 方向={:?} 开平={:?} 限价={} 手数={}",
        instrument, dir_str, off_str, price, volume
    );
    eprint!("[ORDER] 输入 y 确认发送，其他任意键取消: ");
    use std::io::Write;
    std::io::stdout().flush().ok();
    let mut s = String::new();
    std::io::stdin().read_line(&mut s)?;
    if s.trim().to_lowercase() != "y" {
        return Err("用户取消报单".into());
    }

    // 5. 硬保险 check
    guard.check_insert(volume, price, false)?;
    eprintln!("[GUARD] check 通过");

    // 6. 构造并发送报单
    let mut order = CThostFtdcInputOrderField::default();
    copy_str(&mut order.BrokerID, broker());
    copy_str(&mut order.InvestorID, user());
    copy_str(&mut order.InstrumentID, &instrument);
    // ExchangeID 从环境变量 CTP_ORDER_EXCHANGE 读，不硬编码。
    // 期权和期货交易所不同，由调用者指定。
    let exchange = std::env::var("CTP_ORDER_EXCHANGE")
        .map_err(|_| "缺少环境变量 CTP_ORDER_EXCHANGE（如 SHFE/DCE/CZCE/INE/CFFEX/GFEX）")?;
    copy_str(&mut order.ExchangeID, &exchange);
    copy_str(&mut order.OrderRef, "G1");
    order.Direction = direction as i8;
    order.CombOffsetFlag[0] = offset as i8;
    order.CombHedgeFlag[0] = THOST_FTDC_HF_Speculation as i8;
    order.OrderPriceType = THOST_FTDC_OPT_LimitPrice as i8;
    order.LimitPrice = price;
    order.VolumeTotalOriginal = volume;
    order.TimeCondition = THOST_FTDC_TC_GFD as i8;
    order.VolumeCondition = THOST_FTDC_VC_AV as i8;
    order.ContingentCondition = THOST_FTDC_CC_Immediately as i8;
    order.ForceCloseReason = THOST_FTDC_FCC_NotForceClose as i8;
    order.IsAutoSuspend = 0;
    order.UserForceClose = 0;

    let ret = api.req_order_insert(&mut order, 900);
    if ret != 0 {
        return Err(format!("req_order_insert 失败 ret={}", ret).into());
    }
    guard.record();
    eprintln!(
        "[ORDER] 已发送 OrderRef=G1 FrontID={} SessionID={}",
        front_id, session_id
    );

    // 7. 等回报 30 秒，真消费所有事件字段
    let mut last_sys_id: Option<String> = None;
    let mut filled_volume: i32 = 0;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(ev) => match &ev {
                Event::InsertRsp(None) => {
                    eprintln!("[RSP-INSERT] 报单本地回报成功");
                }
                Event::InsertRsp(Some((id, m))) => {
                    eprintln!("[RSP-INSERT] 报单被拒 ErrorID={} ErrorMsg={}", id, m);
                }
                Event::RtnOrder {
                    sys_id,
                    status,
                    traded,
                    total,
                } => {
                    let desc = match *status {
                        48 => "全部成交",
                        49 => "部分成交还在队列",
                        50 => "部分成交不在队列",
                        51 => "未成交还在队列",
                        52 => "未成交不在队列",
                        53 => "已撤单",
                        97 => "未知",
                        _ => "其他",
                    };
                    eprintln!(
                        "[RTN-ORDER] sys_id={} status={}({}) traded={}/{}",
                        sys_id, status, desc, traded, total
                    );
                    if !sys_id.is_empty() {
                        last_sys_id = Some(sys_id.clone());
                    }
                }
                Event::RtnTrade { price, volume } => {
                    eprintln!("[RTN-TRADE] price={} volume={}", price, volume);
                    filled_volume += *volume;
                }
                Event::ErrRtnInsert(info) => {
                    if let Some((id, m)) = info {
                        eprintln!("[ERR-INSERT] ErrorID={} ErrorMsg={}", id, m);
                    }
                }
                Event::ErrRtnAction(info) => {
                    if let Some((id, m)) = info {
                        eprintln!("[ERR-ACTION] ErrorID={} ErrorMsg={}", id, m);
                    }
                }
                Event::ActionRsp(info) => match info {
                    None => eprintln!("[RSP-ACTION] 撤单本地回报成功"),
                    Some((id, m)) => {
                        eprintln!("[RSP-ACTION] 撤单被拒 ErrorID={} ErrorMsg={}", id, m)
                    }
                },
                _ => eprintln!("[EVENT] {:?}", ev),
            },
            Err(_) => {}
        }
    }
    eprintln!(
        "[ORDER] 30 秒回报窗口结束，成交手数={} 最后 OrderSysID={:?}",
        filled_volume, last_sys_id
    );

    // 8. 可选撤单（由环境变量 CTP_ORDER_CANCEL=1 触发）。
    // 这是 check_cancel 的真实使用场景：报单和撤单共用 60 秒合并频率闸门。
    let cancel_requested = std::env::var("CTP_ORDER_CANCEL").ok().as_deref() == Some("1");
    if cancel_requested {
        let sys_id = last_sys_id.ok_or("CTP_ORDER_CANCEL=1 但未收到有效 OrderSysID，无法撤单")?;
        eprintln!("[CANCEL] 准备撤单 sys_id={}", sys_id);

        guard
            .check_cancel()
            .map_err(|e| format!("撤单硬保险拒绝: {}", e))?;
        eprintln!("[GUARD] 撤单 check 通过");

        let mut action = CThostFtdcInputOrderActionField::default();
        copy_str(&mut action.BrokerID, broker());
        copy_str(&mut action.InvestorID, user());
        copy_str(&mut action.InstrumentID, &instrument);
        copy_str(&mut action.ExchangeID, &exchange);
        copy_str(&mut action.OrderSysID, &sys_id);
        action.ActionFlag = THOST_FTDC_AF_Delete as i8;
        action.FrontID = front_id;
        action.SessionID = session_id;
        action.OrderActionRef = 1;

        let ret = api.req_order_action(&mut action, 901);
        if ret != 0 {
            return Err(format!("req_order_action 失败 ret={}", ret).into());
        }
        guard.record();
        eprintln!("[CANCEL] 撤单已发送 ActionRef=1");

        // 再等 10 秒收撤单回报
        let deadline2 = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline2 {
            match rx.recv_timeout(Duration::from_millis(500)) {
                Ok(ev) => match &ev {
                    Event::ActionRsp(None) => {
                        eprintln!("[RSP-ACTION] 撤单成功");
                        break;
                    }
                    Event::ActionRsp(Some((id, m))) => {
                        eprintln!("[RSP-ACTION] 撤单被拒 ErrorID={} ErrorMsg={}", id, m);
                        break;
                    }
                    Event::RtnOrder { sys_id, status, .. } => {
                        eprintln!(
                            "[RTN-ORDER] sys_id={} status={}(撤单后状态)",
                            sys_id, status
                        );
                    }
                    _ => eprintln!("[EVENT] {:?}", ev),
                },
                Err(_) => {}
            }
        }
    } else {
        eprintln!("[CANCEL] 未设置 CTP_ORDER_CANCEL=1，不撤单");
    }

    Ok(())
}

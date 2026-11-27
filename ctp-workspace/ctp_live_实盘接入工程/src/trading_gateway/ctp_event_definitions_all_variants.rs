//! CTP 所有回调统一转成 Event 枚举。
//!
//! 设计要点：
//!   1. Event 只放控制流需要的字段。完整字段由 spi 回调里 eprintln 打印。
//!   2. 查询类回调带 `is_last`，多页返回时业务侧靠它判断结束。
//!   3. 命名区分 Rsp（请求响应）和 Rtn（柜台推送）。
//!   4. RtnOrder / RtnTrade 字段 2026-11-16 补全：instrument / order_ref / direction / offset 原缺失，
//!      对账和撤单匹配都要用，不能在 Event 层丢。

/// CTP 错误信息。None 表示成功。
pub type CtpErr = Option<(i32, String)>;

#[derive(Debug, Clone)]
pub enum Event {
    // ── 连接与心跳 ──
    FrontConnected,
    FrontDisconnected { reason: i32 },
    HeartBeatWarning { time_lapse: i32 },

    // ── 认证 / 登录 / 登出 ──
    AuthRsp { err: CtpErr },
    LoginRsp {
        err: CtpErr,
        front_id: i32,
        session_id: i32,
        trading_day: String,
        max_order_ref: i32,
    },
    LogoutRsp { err: CtpErr },

    // ── 结算 ──
    SettlementConfirmRsp { err: CtpErr },
    SettlementInfoRsp { err: CtpErr, content: String, is_last: bool },

    // ── 报单 / 撤单 / 成交 ──
    OrderInsertRsp { err: CtpErr },
    OrderActionRsp { err: CtpErr },
    ErrRtnOrderInsert { err: CtpErr },
    ErrRtnOrderAction { err: CtpErr },
    RtnOrder {
        instrument: String,
        order_ref: String,
        order_sys_id: String,
        status: i8,
        traded: i32,
        total: i32,
        direction: i8,
        offset: i8,
        limit_price: f64,
    },
    RtnTrade {
        instrument: String,
        trade_id: String,
        order_ref: String,
        direction: i8,
        offset: i8,
        price: f64,
        volume: i32,
    },

    // ── 查询 ──
    QryTradingAccountRsp { err: CtpErr, balance: f64, available: f64, margin: f64, is_last: bool },
    QryInvestorPositionRsp {
        err: CtpErr,
        instrument: String,
        posi_direction: i8,
        position: i32,
        yd_position: i32,
        is_last: bool,
    },
    QryInvestorPositionDetailRsp { err: CtpErr, instrument: String, is_last: bool },
    QryInstrumentRsp {
        err: CtpErr,
        instrument: String,
        exchange: String,
        product: String,
        is_last: bool,
    },
    QryOrderRsp { err: CtpErr, order_sys_id: String, instrument: String, is_last: bool },
    QryTradeRsp { err: CtpErr, trade_id: String, instrument: String, is_last: bool },
    QryInstrumentMarginRateRsp { err: CtpErr, instrument: String, is_last: bool },
    QryInstrumentCommissionRateRsp { err: CtpErr, instrument: String, is_last: bool },
    QryExchangeMarginRateRsp { err: CtpErr, exchange: String, is_last: bool },
    QryMaxOrderVolumeRsp { err: CtpErr, instrument: String, max_volume: i32, is_last: bool },
    QryTradingNoticeRsp { err: CtpErr, is_last: bool },
    QryBrokerTradingParamsRsp { err: CtpErr, is_last: bool },
    QryBrokerTradingAlgosRsp { err: CtpErr, is_last: bool },

    // ── 柜台主动推送 ──
    RtnTradingNotice { content: String },
    RtnInstrumentStatus { instrument: String, exchange: String, status: i8 },

    // ── 通用错误 ──
    RspError { err: CtpErr, request_id: i32 },
}

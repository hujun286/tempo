//! 实盘交易接口网关层。
//!
//! 按职责拆分为五个子模块，全部为 bin 私有（main.rs 顶部 `mod trading_gateway;`）。
//! 模块名刻意写长，半年后看目录即知用途。
//!
//! - ctp_event_definitions_all_variants        : 所有 CTP 回调统一转成 Event 枚举
//! - trader_spi_callback_implementations_all   : TraderSpi 全部回调实现
//! - session_authenticate_login_settlement_flow: 认证/登录/结算确认三件套
//! - query_handlers_all_inquiry_types          : 所有查询请求封装（本轮空壳）
//! - order_insert_cancel_with_hard_limits_guard: 受控单笔报单/撤单/回报消费

pub mod ctp_event_definitions_all_variants;
pub mod trader_spi_callback_implementations_all;
pub mod session_authenticate_login_settlement_flow;
pub mod query_handlers_all_inquiry_types;
pub mod order_insert_cancel_with_hard_limits_guard;

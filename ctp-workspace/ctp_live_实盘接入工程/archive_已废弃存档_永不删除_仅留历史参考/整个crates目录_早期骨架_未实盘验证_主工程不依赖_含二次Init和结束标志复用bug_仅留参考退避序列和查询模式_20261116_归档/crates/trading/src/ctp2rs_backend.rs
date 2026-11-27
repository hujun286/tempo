//! ctp2rs 真实后端。**从旧项目 trend_analyzer_ctp_test 的 ctp2rs 用法拷贝对齐。**
//! 本文件是唯一直接依赖 ctp2rs 的地方，版本升级只改这里。
//!
//! 对齐清单（逐项照旧项目改）：
//! 1. `TraderApi::new` 的参数顺序
//! 2. `register_front` / `init` / `subscribe_market_data` 的函数名
//! 3. Trait `MdSpi` / `TraderSpi` 的回调方法名和参数
//! 4. 结构体字段名：`OnRspUserLogin` 的 `SessionID`/`FrontID` 等
//!
//! 下面保留结构，函数体用 `todo!()` 占位，明天按旧项目逐条填。

use std::sync::Arc;
use common_model::{CancelRequest, OrderRequest};
use crate::adapter::{TdAdapter, TdEvent};

pub struct Ctp2rsTdAdapter {
    // 实际类型按旧项目：Arc<TraderApi<...>>
    _placeholder: (),
}

impl Ctp2rsTdAdapter {
    pub fn new(_flow_dir: &str) -> Arc<Self> {
        todo!("按旧项目 ctp2rs 版本构造 TraderApi")
    }
}

impl TdAdapter for Ctp2rsTdAdapter {
    fn register_front(&self, _front: &str) { todo!() }
    fn register_spi(&self, _tx: crossbeam_channel::Sender<TdEvent>) { todo!() }
    fn init(&self) { todo!() }
    fn req_authenticate(&self, _a: &str, _c: &str) { todo!() }
    fn req_user_login(&self, _b: &str, _u: &str, _p: &str) { todo!() }
    fn req_settlement_confirm(&self) { todo!() }
    fn req_order_insert(&self, _req: &OrderRequest, _r: &str) { todo!() }
    fn req_order_action(&self, _req: &CancelRequest) { todo!() }
    fn req_qry_trading_account(&self) { todo!() }
    fn req_qry_investor_position(&self) { todo!() }
    fn req_qry_instrument(&self) { todo!() }
    fn release(&self) { todo!() }
}

//! 行情 ctp2rs 真实后端。**从旧项目 option_monitor 的 ctp2rs 用法拷贝对齐。**
//! 对齐清单同上（MdApi 构造、Trait 回调方法名、字段名）。

use std::sync::Arc;
use crossbeam_channel::Sender;
use crate::adapter::{MdAdapter, MdEvent};

pub struct Ctp2rsMdAdapter { _p: () }

impl Ctp2rsMdAdapter {
    pub fn new(_flow_dir: &str) -> Arc<Self> { todo!("按旧项目 ctp2rs 构造 MdApi") }
}

impl MdAdapter for Ctp2rsMdAdapter {
    fn register_front(&self, _front: &str) { todo!() }
    fn register_spi(&self, _tx: Sender<MdEvent>) { todo!() }
    fn init(&self) { todo!() }
    fn req_authenticate(&self, _a: &str, _c: &str) { todo!() }
    fn req_user_login(&self, _b: &str, _u: &str, _p: &str) { todo!() }
    fn subscribe(&self, _instruments: &[String]) { todo!() }
    fn release(&self) { todo!() }
}

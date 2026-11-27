//! 测试用 mock（模拟）交易适配器，记录调用。

use std::sync::{Arc, Mutex};
use crossbeam_channel::{Sender, Receiver, unbounded};
use common_model::{CancelRequest, OrderRequest};
use crate::adapter::{TdAdapter, TdEvent};

#[derive(Default)]
struct Inner {
    orders: Vec<OrderRequest>,
    cancels: Vec<CancelRequest>,
    fronts: Vec<String>,
}

pub struct MockTdAdapter {
    inner: Arc<Mutex<Inner>>,
    tx: Mutex<Option<Sender<TdEvent>>>,
}

impl MockTdAdapter {
    pub fn new() -> (Arc<Self>, Sender<TdEvent>, Receiver<TdEvent>) {
        let (tx, rx) = unbounded();
        let api = Arc::new(Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            tx: Mutex::new(Some(tx.clone())),
        });
        (api, tx, rx)
    }

    pub fn last_order(&self) -> Option<OrderRequest> {
        self.inner.lock().unwrap().orders.last().cloned()
    }
    pub fn last_cancel(&self) -> Option<CancelRequest> {
        self.inner.lock().unwrap().cancels.last().cloned()
    }
    pub fn fronts(&self) -> Vec<String> {
        self.inner.lock().unwrap().fronts.clone()
    }
}

impl TdAdapter for MockTdAdapter {
    fn register_front(&self, front: &str) {
        self.inner.lock().unwrap().fronts.push(front.to_string());
    }
    fn register_spi(&self, _tx: Sender<TdEvent>) {}
    fn init(&self) {}
    fn req_authenticate(&self, _a: &str, _c: &str) {}
    fn req_user_login(&self, _b: &str, _u: &str, _p: &str) {}
    fn req_settlement_confirm(&self) {}
    fn req_order_insert(&self, req: &OrderRequest, _r: &str) {
        self.inner.lock().unwrap().orders.push(req.clone());
    }
    fn req_order_action(&self, req: &CancelRequest) {
        self.inner.lock().unwrap().cancels.push(req.clone());
    }
    fn req_qry_trading_account(&self) {}
    fn req_qry_investor_position(&self) {}
    fn req_qry_instrument(&self) {}
    fn release(&self) {}
}

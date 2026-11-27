use std::sync::{Arc, Mutex};
use crossbeam_channel::{Sender, unbounded};
use crate::adapter::{MdAdapter, MdEvent};

pub struct MockMdAdapter {
    subscribed: Mutex<Vec<String>>,
    fronts: Mutex<Vec<String>>,
}

impl MockMdAdapter {
    pub fn new() -> (Arc<Self>, Sender<MdEvent>) {
        let (tx, _rx) = unbounded();
        (Arc::new(Self { subscribed: Mutex::new(Vec::new()), fronts: Mutex::new(Vec::new()) }), tx)
    }
    pub fn subscribed(&self) -> Vec<String> { self.subscribed.lock().unwrap().clone() }
    pub fn fronts(&self) -> Vec<String> { self.fronts.lock().unwrap().clone() }
}

impl MdAdapter for MockMdAdapter {
    fn register_front(&self, front: &str) { self.fronts.lock().unwrap().push(front.into()); }
    fn register_spi(&self, _tx: Sender<MdEvent>) {}
    fn init(&self) {}
    fn req_authenticate(&self, _a: &str, _c: &str) {}
    fn req_user_login(&self, _b: &str, _u: &str, _p: &str) {}
    fn subscribe(&self, instruments: &[String]) {
        self.subscribed.lock().unwrap().extend(instruments.iter().cloned());
    }
    fn release(&self) {}
}

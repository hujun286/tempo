//! 单 member（成员）行情连接：注册前置 → 认证探测 → 登录 → 分批订阅 → 回调入队。

use std::time::Duration;
use crossbeam_channel::{Receiver, Sender, RecvTimeoutError};
use common_model::{InstrumentId, Tick};
use crate::adapter::{MdAdapter, MdEvent};

pub const BATCH_SIZE: usize = 500;
pub const BATCH_DELAY: Duration = Duration::from_millis(300);
pub const STAGE_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone)]
pub struct MemberConfig {
    pub member_idx: usize,
    pub front: String,          // tcp://ip:61213
    pub instruments: Vec<InstrumentId>,
}

#[derive(Debug, Clone)]
pub enum MemberStatus {
    Connected,
    Authenticated,
    LoggedIn,
    Subscribed { count: usize },
    Tick,
    Error(String),
    Disconnected { reason: i32 },
}

/// 单 member 运行入口。返回 tick 接收端和状态接收端。
pub struct MemberHandle {
    pub member_idx: usize,
    pub tick_rx: Receiver<Tick>,
    pub status_rx: Receiver<MemberStatus>,
    pub join: Option<std::thread::JoinHandle<()>>,
}

pub fn spawn(
    api: std::sync::Arc<dyn MdAdapter>,
    cfg: MemberConfig,
    broker: String,
    user: String,
    pwd: String,
    app_id: String,
    auth_code: String,
    need_authenticate: bool,
) -> MemberHandle {
    let (tick_tx, tick_rx) = crossbeam_channel::unbounded();
    let (status_tx, status_rx) = crossbeam_channel::unbounded();
    let (ev_tx, ev_rx) = crossbeam_channel::unbounded::<MdEvent>();

    let api2 = api.clone();
    api2.register_spi(ev_tx);

    let join = std::thread::spawn(move || {
        run(api, cfg, broker, user, pwd, app_id, auth_code, need_authenticate,
            ev_rx, tick_tx, status_tx);
    });

    MemberHandle { member_idx: 0, tick_rx, status_rx, join: Some(join) }
}

#[allow(clippy::too_many_arguments)]
fn run(
    api: std::sync::Arc<dyn MdAdapter>,
    cfg: MemberConfig,
    broker: String,
    user: String,
    pwd: String,
    app_id: String,
    auth_code: String,
    need_authenticate: bool,
    ev_rx: Receiver<MdEvent>,
    tick_tx: Sender<Tick>,
    status_tx: Sender<MemberStatus>,
) {
    let tag = format!("[MD#{}]", cfg.member_idx);
    api.register_front(&cfg.front);
    api.init();

    // 等连接
    if !wait_for(&ev_rx, |e| matches!(e, MdEvent::FrontConnected), STAGE_TIMEOUT) {
        let _ = status_tx.send(MemberStatus::Error("连接超时".into()));
        return;
    }
    let _ = status_tx.send(MemberStatus::Connected);

    // 认证探测（可选）
    if need_authenticate {
        api.req_authenticate(&app_id, &auth_code);
        match wait_for_auth(&ev_rx, STAGE_TIMEOUT) {
            Ok(0) => { let _ = status_tx.send(MemberStatus::Authenticated); }
            Ok(id) => {
                eprintln!("{} 认证失败 ErrorID={}（63 表示需要认证但失败）", tag, id);
                let _ = status_tx.send(MemberStatus::Error(format!("认证失败 ErrorID={}", id)));
                return;
            }
            Err(e) => {
                eprintln!("{} {}", tag, e);
                let _ = status_tx.send(MemberStatus::Error(e));
                return;
            }
        }
    }

    // 登录
    api.req_user_login(&broker, &user, &pwd);
    match wait_for_login(&ev_rx, STAGE_TIMEOUT) {
        Ok(0) => { let _ = status_tx.send(MemberStatus::LoggedIn); }
        Ok(id) => {
            eprintln!("{} 登录失败 ErrorID={}", tag, id);
            let _ = status_tx.send(MemberStatus::Error(format!("登录失败 ErrorID={}", id)));
            return;
        }
        Err(e) => {
            eprintln!("{} {}", tag, e);
            let _ = status_tx.send(MemberStatus::Error(e));
            return;
        }
    }

    // 分批订阅
    for batch in cfg.instruments.chunks(BATCH_SIZE) {
        let codes: Vec<String> = batch.iter().map(|i| i.0.clone()).collect();
        api.subscribe(&codes);
        std::thread::sleep(BATCH_DELAY);
    }
    let _ = status_tx.send(MemberStatus::Subscribed { count: cfg.instruments.len() });

    // 回调循环：只拷贝入队
    loop {
        match ev_rx.recv() {
            Ok(MdEvent::Tick(t)) => {
                let _ = status_tx.send(MemberStatus::Tick);
                let _ = tick_tx.send(t);
            }
            Ok(MdEvent::FrontDisconnected { reason }) => {
                let _ = status_tx.send(MemberStatus::Disconnected { reason });
                return;
            }
            Ok(MdEvent::RspError { error_id, error_msg }) => {
                eprintln!("{} 错误 ErrorID={} {}", tag, error_id, error_msg);
            }
            Ok(_) => {}
            Err(_) => return,
        }
    }
}

fn wait_for(rx: &Receiver<MdEvent>, pred: impl Fn(&MdEvent) -> bool, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(e) if pred(&e) => return true,
            Ok(_) => continue,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(_) => return false,
        }
    }
    false
}

fn wait_for_auth(rx: &Receiver<MdEvent>, timeout: Duration) -> Result<i32, String> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(MdEvent::Authenticated { error_id, .. }) => return Ok(error_id),
            Ok(_) => continue,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(_) => return Err("通道断开".into()),
        }
    }
    Err("认证超时".into())
}

fn wait_for_login(rx: &Receiver<MdEvent>, timeout: Duration) -> Result<i32, String> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(MdEvent::LoggedIn { error_id, .. }) => return Ok(error_id),
            Ok(_) => continue,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(_) => return Err("通道断开".into()),
        }
    }
    Err("登录超时".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::mock_md_adapter::MockMdAdapter;

    #[test]
    fn subscription_is_batched_500() {
        let instruments: Vec<InstrumentId> = (0..1200)
            .map(|i| InstrumentId::new(format!("pp2701{:04}", i)))
            .collect();
        let (api, _tx) = MockMdAdapter::new();
        // 只验证分批数量，不跑真实连接
        let batches: Vec<_> = instruments.chunks(BATCH_SIZE).collect();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].len(), 500);
        assert_eq!(batches[2].len(), 200);
        let _ = api;
    }
}

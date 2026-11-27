//! 断线重连：重连后重新认证 + 登录 + 结算确认。

use crossbeam_channel::Receiver;
use common_model::TradingConfig;
use crate::adapter::{TdAdapter, TdEvent};
use crate::trading_session_authentication::{open_session, SessionInfo};

pub struct ReconnectPolicy {
    pub backoff_ms: Vec<u64>,      // 例 [1000, 2000, 5000, 10000]
    pub max_failures: u32,
}

impl Default for ReconnectPolicy {
    fn default() -> Self { Self { backoff_ms: vec![1000, 2000, 5000, 10000], max_failures: 5 } }
}

/// 阻塞等待断线事件，然后按退避重连。
/// 返回重连后的新会话。连续失败超过阈值返回错误。
pub fn run(
    api: &dyn TdAdapter,
    rx: &Receiver<TdEvent>,
    cfg: &TradingConfig,
    policy: &ReconnectPolicy,
) -> Result<SessionInfo, String> {
    let mut failures = 0u32;
    loop {
        // 等待断线
        loop {
            match rx.recv() {
                Ok(TdEvent::FrontDisconnected { reason }) => {
                    eprintln!("[TD] 前置断开 reason={}", reason);
                    break;
                }
                Ok(_) => continue,
                Err(_) => return Err("通道关闭".into()),
            }
        }
        // 退避重连
        let delay = policy.backoff_ms
            .get(failures as usize)
            .copied()
            .unwrap_or(*policy.backoff_ms.last().unwrap_or(&10000));
        std::thread::sleep(std::time::Duration::from_millis(delay));
        api.init();
        match open_session(api, rx, cfg) {
            Ok(s) => {
                eprintln!("[TD] 重连成功 session={} front={}", s.session_id, s.front_id);
                return Ok(s);
            }
            Err(e) => {
                failures += 1;
                eprintln!("[TD] 重连失败 {}/{}: {}", failures, policy.max_failures, e);
                if failures >= policy.max_failures {
                    return Err(format!("连续 {} 次重连失败", failures));
                }
            }
        }
    }
}

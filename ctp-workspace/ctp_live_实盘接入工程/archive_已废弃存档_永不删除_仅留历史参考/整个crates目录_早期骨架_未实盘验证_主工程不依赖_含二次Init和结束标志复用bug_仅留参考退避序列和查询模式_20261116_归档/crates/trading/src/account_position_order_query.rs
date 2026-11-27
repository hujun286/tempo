//! 查询：资金、持仓、合约。

use crossbeam_channel::{Receiver, RecvTimeoutError};
use std::time::Duration;
use crate::adapter::{TdAdapter, TdEvent};

pub fn query_account(api: &dyn TdAdapter, rx: &Receiver<TdEvent>) -> Result<f64, String> {
    api.req_qry_trading_account();
    loop {
        match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(TdEvent::QueryAccount { available }) => return Ok(available),
            Ok(TdEvent::RspError { error_id, error_msg }) =>
                return Err(format!("查询资金失败 ErrorID={} {}", error_id, error_msg)),
            Ok(_) => continue,
            Err(RecvTimeoutError::Timeout) => return Err("查询资金超时".into()),
            Err(RecvTimeoutError::Disconnected) => return Err("查询资金通道断开".into()),
        }
    }
}

pub fn query_positions(api: &dyn TdAdapter, rx: &Receiver<TdEvent>)
    -> Result<Vec<(String, i32)>, String>
{
    api.req_qry_investor_position();
    let mut out = Vec::new();
    loop {
        match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(TdEvent::QueryPosition { instrument, position }) => {
                if position != 0 { out.push((instrument, position)); }
            }
            Ok(TdEvent::QueryInstrumentEnd) => return Ok(out), // 复用结束标志
            Ok(TdEvent::RspError { error_id, error_msg }) =>
                return Err(format!("查询持仓失败 ErrorID={} {}", error_id, error_msg)),
            Ok(_) => continue,
            Err(RecvTimeoutError::Timeout) => return Ok(out),
            Err(RecvTimeoutError::Disconnected) => return Err("查询持仓通道断开".into()),
        }
    }
}

pub fn query_all_instruments(api: &dyn TdAdapter, rx: &Receiver<TdEvent>)
    -> Result<Vec<(String, String, String)>, String>
{
    api.req_qry_instrument();
    let mut out = Vec::new();
    loop {
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(TdEvent::QueryInstrument { instrument, exchange, product }) =>
                out.push((instrument, exchange, product)),
            Ok(TdEvent::QueryInstrumentEnd) => return Ok(out),
            Ok(TdEvent::RspError { error_id, error_msg }) =>
                return Err(format!("查询合约失败 ErrorID={} {}", error_id, error_msg)),
            Ok(_) => continue,
            Err(RecvTimeoutError::Timeout) => return Ok(out),
            Err(RecvTimeoutError::Disconnected) => return Err("查询合约通道断开".into()),
        }
    }
}

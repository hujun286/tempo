//! 行情 SPI：CTP 回调 → 事件（照抄 v1.1，一字未改）。
use crate::events::{MemberEvent, info_pair};
use ctp2rs::ffi::WrapToString;
use ctp2rs::v1alpha1::*;
use std::sync::mpsc;

// ============================================================
// SPI（照抄 v1.1，一字未改）
// ============================================================

pub(crate) struct MemberSpi {
    pub(crate) member_idx: usize,
    pub(crate) tx: mpsc::SyncSender<MemberEvent>,
}

impl MdSpi for MemberSpi {
    fn on_front_connected(&mut self) {
        let _ = self.tx.send(MemberEvent::Connected(self.member_idx));
    }
    fn on_front_disconnected(&mut self, reason: i32) {
        let _ = self
            .tx
            .send(MemberEvent::Disconnected(self.member_idx, reason));
    }
    fn on_rsp_user_login(
        &mut self,
        _rsp: Option<&CThostFtdcRspUserLoginField>,
        rsp_info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        _is_last: bool,
    ) {
        let _ = self
            .tx
            .send(MemberEvent::Login(self.member_idx, info_pair(rsp_info)));
    }
    fn on_rsp_sub_market_data(
        &mut self,
        _specific: Option<&CThostFtdcSpecificInstrumentField>,
        _rsp_info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        _is_last: bool,
    ) {
        // 订阅回包不逐个上报（30000 合约会把通道刷爆），完成一轮后由 spawn_member 发 Ready。
    }
    fn on_rtn_depth_market_data(
        &mut self,
        depth_market_data: Option<&CThostFtdcDepthMarketDataField>,
    ) {
        if let Some(q) = depth_market_data {
            // Volume 是 CTP int（当日累计成交量）；Turnover/OpenInterest 是 double。
            let _ = self.tx.send(MemberEvent::Tick {
                member_idx: self.member_idx,
                instrument: q.InstrumentID.to_string(),
                last_price: q.LastPrice,
                volume: q.Volume as u64,
                turnover: q.Turnover,
                open_interest: q.OpenInterest,
                trading_day: q.TradingDay.to_string(),
                update_time: q.UpdateTime.to_string(),
                update_millisec: q.UpdateMillisec,
            });
        }
    }
    fn on_rsp_error(
        &mut self,
        p_rsp_info: Option<&CThostFtdcRspInfoField>,
        n_request_id: i32,
        _b_is_last: bool,
    ) {
        if let Some(e) = p_rsp_info
            && e.ErrorID != 0
        {
            tracing::error!(
                "[MD#{}] RSP ERROR request_id={} ErrorID={} ErrorMsg={}",
                self.member_idx,
                n_request_id,
                e.ErrorID,
                e.ErrorMsg.to_string()
            );
        }
    }
}

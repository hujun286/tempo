//! TraderSpi 全部回调实现（A 方案：期货 + 商品期权用到的 28 个）。
//!
//! 每个回调做两件事：
//!   1. eprintln 打印关键字段（便于实盘日志追踪）
//!   2. 往 channel 发 Event（供 session/query/order 消费）
//!
//! 字段读取统一走 read_str（GB18030 解码）和 rsp_err（错误转 Option）。

use ctp2rs::v1alpha1::*;
use std::os::raw::c_char;
use std::sync::mpsc::Sender;

use super::ctp_event_definitions_all_variants::{CtpErr, Event};

pub fn read_str(buf: &[c_char]) -> String {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let bytes: Vec<u8> = buf[..end].iter().map(|&b| b as u8).collect();
    if bytes.is_empty() {
        return String::new();
    }
    let (decoded, _, had_errors) = encoding_rs::GB18030.decode(&bytes);
    if had_errors {
        String::from_utf8_lossy(&bytes).trim().to_string()
    } else {
        decoded.trim().to_string()
    }
}

pub fn rsp_err(info: Option<&CThostFtdcRspInfoField>) -> CtpErr {
    info.and_then(|i| {
        if i.ErrorID != 0 {
            Some((i.ErrorID, read_str(&i.ErrorMsg)))
        } else {
            None
        }
    })
}

pub struct MySpi {
    pub tx: Sender<Event>,
}

impl TraderSpi for MySpi {
    // ── 连接 / 心跳 ──
    fn on_front_connected(&mut self) {
        eprintln!("[SPI] OnFrontConnected");
        let _ = self.tx.send(Event::FrontConnected);
    }
    fn on_front_disconnected(&mut self, reason: i32) {
        eprintln!("[SPI] OnFrontDisconnected reason={}", reason);
        let _ = self.tx.send(Event::FrontDisconnected { reason });
    }
    fn on_heart_beat_warning(&mut self, time_lapse: i32) {
        eprintln!("[SPI] OnHeartBeatWarning time_lapse={}s", time_lapse);
        let _ = self.tx.send(Event::HeartBeatWarning { time_lapse });
    }

    // ── 认证 / 登录 / 登出 ──
    fn on_rsp_authenticate(
        &mut self,
        _rsp: Option<&CThostFtdcRspAuthenticateField>,
        info: Option<&CThostFtdcRspInfoField>,
        req: i32,
        _last: bool,
    ) {
        let err = rsp_err(info);
        match &err {
            None => eprintln!("[SPI] OnRspAuthenticate req={} OK", req),
            Some((id, m)) => eprintln!("[SPI] OnRspAuthenticate req={} ErrorID={} {}", req, id, m),
        }
        let _ = self.tx.send(Event::AuthRsp { err });
    }

    fn on_rsp_user_login(
        &mut self,
        rsp: Option<&CThostFtdcRspUserLoginField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        _last: bool,
    ) {
        let err = rsp_err(info);
        let (f, s, d, m) = rsp
            .map(|r| (r.FrontID, r.SessionID, read_str(&r.TradingDay), read_str(&r.MaxOrderRef).parse::<i32>().unwrap_or(0)))
            .unwrap_or((0, 0, String::new(), 0));
        eprintln!("[SPI] OnRspUserLogin FrontID={} SessionID={} TradingDay={} MaxOrderRef={}", f, s, d, m);
        let _ = self.tx.send(Event::LoginRsp { err, front_id: f, session_id: s, trading_day: d, max_order_ref: m });
    }

    fn on_rsp_user_logout(
        &mut self,
        _rsp: Option<&CThostFtdcUserLogoutField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        _last: bool,
    ) {
        let err = rsp_err(info);
        eprintln!("[SPI] OnRspUserLogout err={:?}", err);
        let _ = self.tx.send(Event::LogoutRsp { err });
    }

    // ── 结算 ──
    fn on_rsp_settlement_info_confirm(
        &mut self,
        _rsp: Option<&CThostFtdcSettlementInfoConfirmField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        _last: bool,
    ) {
        let err = rsp_err(info);
        eprintln!("[SPI] OnRspSettlementInfoConfirm err={:?}", err);
        let _ = self.tx.send(Event::SettlementConfirmRsp { err });
    }

    fn on_rsp_qry_settlement_info(
        &mut self,
        rsp: Option<&CThostFtdcSettlementInfoField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        last: bool,
    ) {
        let err = rsp_err(info);
        let content = rsp.map(|r| read_str(&r.Content)).unwrap_or_default();
        eprintln!("[SPI] OnRspQrySettlementInfo is_last={} content_len={}", last, content.len());
        let _ = self.tx.send(Event::SettlementInfoRsp { err, content, is_last: last });
    }

    // ── 报单 / 撤单 / 成交 ──
    fn on_rsp_order_insert(
        &mut self,
        _o: Option<&CThostFtdcInputOrderField>,
        info: Option<&CThostFtdcRspInfoField>,
        req: i32,
        _last: bool,
    ) {
        let err = rsp_err(info);
        eprintln!("[SPI] OnRspOrderInsert req={} err={:?}", req, err);
        let _ = self.tx.send(Event::OrderInsertRsp { err });
    }

    fn on_rsp_order_action(
        &mut self,
        _a: Option<&CThostFtdcInputOrderActionField>,
        info: Option<&CThostFtdcRspInfoField>,
        req: i32,
        _last: bool,
    ) {
        let err = rsp_err(info);
        eprintln!("[SPI] OnRspOrderAction req={} err={:?}", req, err);
        let _ = self.tx.send(Event::OrderActionRsp { err });
    }

    fn on_err_rtn_order_insert(
        &mut self,
        o: Option<&CThostFtdcInputOrderField>,
        info: Option<&CThostFtdcRspInfoField>,
    ) {
        let err = rsp_err(info);
        let inst = o.map(|x| read_str(&x.InstrumentID)).unwrap_or_default();
        eprintln!("[SPI] OnErrRtnOrderInsert instrument={} err={:?}", inst, err);
        let _ = self.tx.send(Event::ErrRtnOrderInsert { err });
    }

    fn on_err_rtn_order_action(
        &mut self,
        a: Option<&CThostFtdcOrderActionField>,
        info: Option<&CThostFtdcRspInfoField>,
    ) {
        let err = rsp_err(info);
        let sys_id = a.map(|x| read_str(&x.OrderSysID)).unwrap_or_default();
        eprintln!("[SPI] OnErrRtnOrderAction sys_id={} err={:?}", sys_id, err);
        let _ = self.tx.send(Event::ErrRtnOrderAction { err });
    }

    fn on_rtn_order(&mut self, order: Option<&CThostFtdcOrderField>) {
        if let Some(o) = order {
            let instrument = read_str(&o.InstrumentID);
            let order_ref = read_str(&o.OrderRef);
            let sys_id = read_str(&o.OrderSysID);
            let status = o.OrderStatus;
            let traded = o.VolumeTraded;
            let total = o.VolumeTotal;
            let direction = o.Direction;
            let offset = o.CombOffsetFlag[0];
            let limit_price = o.LimitPrice;
            eprintln!(
                "[SPI] OnRtnOrder instrument={} ref={} sys_id={} status={} traded={}/{} dir={} off={} limit={}",
                instrument, order_ref, sys_id, status, traded, total, direction, offset, limit_price
            );
            let _ = self.tx.send(Event::RtnOrder {
                instrument, order_ref, order_sys_id: sys_id, status, traded, total, direction, offset, limit_price,
            });
        }
    }

    fn on_rtn_trade(&mut self, trade: Option<&CThostFtdcTradeField>) {
        if let Some(t) = trade {
            let instrument = read_str(&t.InstrumentID);
            let trade_id = read_str(&t.TradeID);
            let order_ref = read_str(&t.OrderRef);
            let direction = t.Direction;
            let offset = t.OffsetFlag;
            let price = t.Price;
            let volume = t.Volume;
            eprintln!(
                "[SPI] OnRtnTrade instrument={} trade_id={} ref={} dir={} off={} price={} vol={}",
                instrument, trade_id, order_ref, direction, offset, price, volume
            );
            let _ = self.tx.send(Event::RtnTrade {
                instrument, trade_id, order_ref, direction, offset, price, volume,
            });
        }
    }

    // ── 查询 ──
    fn on_rsp_qry_trading_account(
        &mut self,
        rsp: Option<&CThostFtdcTradingAccountField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        last: bool,
    ) {
        let err = rsp_err(info);
        let (balance, available, margin) = rsp
            .map(|r| (r.Balance, r.Available, r.CurrMargin))
            .unwrap_or((0.0, 0.0, 0.0));
        if let Some(r) = rsp {
            eprintln!(
                "[SPI] OnRspQryTradingAccount Balance={} Available={} CurrMargin={} CloseProfit={} PositionProfit={} Commission={} FrozenMargin={} FrozenCash={} Deposit={} Withdraw={} is_last={}",
                r.Balance, r.Available, r.CurrMargin, r.CloseProfit, r.PositionProfit,
                r.Commission, r.FrozenMargin, r.FrozenCash, r.Deposit, r.Withdraw, last
            );
        } else {
            eprintln!("[SPI] OnRspQryTradingAccount no_data is_last={}", last);
        }
        let _ = self.tx.send(Event::QryTradingAccountRsp { err, balance, available, margin, is_last: last });
    }

    fn on_rsp_qry_investor_position(
        &mut self,
        rsp: Option<&CThostFtdcInvestorPositionField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        last: bool,
    ) {
        let err = rsp_err(info);
        let (inst, dir, pos, yd) = rsp
            .map(|r| (read_str(&r.InstrumentID), r.PosiDirection, r.Position, r.YdPosition))
            .unwrap_or((String::new(), 0, 0, 0));
        if let Some(r) = rsp {
            eprintln!(
                "[SPI] OnRspQryInvestorPosition {} dir={} Position={} YdPosition={} TodayPosition={} LongFrozen={} ShortFrozen={} PositionProfit={} UseMargin={} is_last={}",
                inst, dir, r.Position, r.YdPosition, r.TodayPosition, r.LongFrozen,
                r.ShortFrozen, r.PositionProfit, r.UseMargin, last
            );
        }
        let _ = self.tx.send(Event::QryInvestorPositionRsp {
            err, instrument: inst, posi_direction: dir, position: pos, yd_position: yd, is_last: last,
        });
    }

    fn on_rsp_qry_investor_position_detail(
        &mut self,
        rsp: Option<&CThostFtdcInvestorPositionDetailField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        last: bool,
    ) {
        let err = rsp_err(info);
        let inst = rsp.map(|r| read_str(&r.InstrumentID)).unwrap_or_default();
        if let Some(r) = rsp {
            eprintln!(
                "[SPI] OnRspQryInvestorPositionDetail {} OpenDate={} TradeID={} Volume={} Price={} is_last={}",
                inst, read_str(&r.OpenDate), read_str(&r.TradeID), r.Volume, r.OpenPrice, last
            );
        }
        let _ = self.tx.send(Event::QryInvestorPositionDetailRsp { err, instrument: inst, is_last: last });
    }

    fn on_rsp_qry_instrument(
        &mut self,
        rsp: Option<&CThostFtdcInstrumentField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        last: bool,
    ) {
        let err = rsp_err(info);
        let (inst, ex, product) = rsp
            .map(|r| (read_str(&r.InstrumentID), read_str(&r.ExchangeID), read_str(&r.ProductID)))
            .unwrap_or((String::new(), String::new(), String::new()));
        let _ = self.tx.send(Event::QryInstrumentRsp {
            err, instrument: inst, exchange: ex, product, is_last: last,
        });
    }

    fn on_rsp_qry_order(
        &mut self,
        rsp: Option<&CThostFtdcOrderField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        last: bool,
    ) {
        let err = rsp_err(info);
        let (sys_id, inst) = rsp
            .map(|r| (read_str(&r.OrderSysID), read_str(&r.InstrumentID)))
            .unwrap_or((String::new(), String::new()));
        if let Some(r) = rsp {
            eprintln!(
                "[SPI] OnRspQryOrder {} sys_id={} status={} traded={}/{} limit={} is_last={}",
                inst, sys_id, r.OrderStatus, r.VolumeTraded, r.VolumeTotal, r.LimitPrice, last
            );
        }
        let _ = self.tx.send(Event::QryOrderRsp { err, order_sys_id: sys_id, instrument: inst, is_last: last });
    }

    fn on_rsp_qry_trade(
        &mut self,
        rsp: Option<&CThostFtdcTradeField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        last: bool,
    ) {
        let err = rsp_err(info);
        let (trade_id, inst) = rsp
            .map(|r| (read_str(&r.TradeID), read_str(&r.InstrumentID)))
            .unwrap_or((String::new(), String::new()));
        if let Some(r) = rsp {
            eprintln!(
                "[SPI] OnRspQryTrade {} trade_id={} price={} vol={} is_last={}",
                inst, trade_id, r.Price, r.Volume, last
            );
        }
        let _ = self.tx.send(Event::QryTradeRsp { err, trade_id, instrument: inst, is_last: last });
    }

    fn on_rsp_qry_instrument_margin_rate(
        &mut self,
        rsp: Option<&CThostFtdcInstrumentMarginRateField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        last: bool,
    ) {
        let err = rsp_err(info);
        let inst = rsp.map(|r| read_str(&r.InstrumentID)).unwrap_or_default();
        if let Some(r) = rsp {
            eprintln!(
                "[SPI] OnRspQryInstrumentMarginRate {} LongMarginRatioByMoney={} LongMarginRatioByVolume={} ShortMarginRatioByMoney={} ShortMarginRatioByVolume={} is_last={}",
                inst, r.LongMarginRatioByMoney, r.LongMarginRatioByVolume,
                r.ShortMarginRatioByMoney, r.ShortMarginRatioByVolume, last
            );
        }
        let _ = self.tx.send(Event::QryInstrumentMarginRateRsp { err, instrument: inst, is_last: last });
    }

    fn on_rsp_qry_instrument_commission_rate(
        &mut self,
        rsp: Option<&CThostFtdcInstrumentCommissionRateField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        last: bool,
    ) {
        let err = rsp_err(info);
        let inst = rsp.map(|r| read_str(&r.InstrumentID)).unwrap_or_default();
        if let Some(r) = rsp {
            eprintln!(
                "[SPI] OnRspQryInstrumentCommissionRate {} OpenRatioByMoney={} OpenRatioByVolume={} CloseRatioByMoney={} CloseRatioByVolume={} CloseTodayRatioByMoney={} CloseTodayRatioByVolume={} is_last={}",
                inst, r.OpenRatioByMoney, r.OpenRatioByVolume,
                r.CloseRatioByMoney, r.CloseRatioByVolume,
                r.CloseTodayRatioByMoney, r.CloseTodayRatioByVolume, last
            );
        }
        let _ = self.tx.send(Event::QryInstrumentCommissionRateRsp { err, instrument: inst, is_last: last });
    }

    fn on_rsp_qry_exchange_margin_rate(
        &mut self,
        rsp: Option<&CThostFtdcExchangeMarginRateField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        last: bool,
    ) {
        let err = rsp_err(info);
        let ex = rsp.map(|r| read_str(&r.ExchangeID)).unwrap_or_default();
        let _ = self.tx.send(Event::QryExchangeMarginRateRsp { err, exchange: ex, is_last: last });
    }

    fn on_rsp_qry_max_order_volume(
        &mut self,
        rsp: Option<&CThostFtdcQryMaxOrderVolumeField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        last: bool,
    ) {
        let err = rsp_err(info);
        let (inst, max) = rsp
            .map(|r| (read_str(&r.InstrumentID), r.MaxVolume))
            .unwrap_or((String::new(), 0));
        eprintln!("[SPI] OnRspQryMaxOrderVolume {} MaxVolume={} is_last={}", inst, max, last);
        let _ = self.tx.send(Event::QryMaxOrderVolumeRsp { err, instrument: inst, max_volume: max, is_last: last });
    }

    fn on_rsp_qry_trading_notice(
        &mut self,
        rsp: Option<&CThostFtdcTradingNoticeField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        last: bool,
    ) {
        let err = rsp_err(info);
        if let Some(r) = rsp {
            eprintln!("[SPI] OnRspQryTradingNotice SendTime={} Content={} is_last={}",
                read_str(&r.SendTime), read_str(&r.FieldContent), last);
        }
        let _ = self.tx.send(Event::QryTradingNoticeRsp { err, is_last: last });
    }

    fn on_rsp_qry_broker_trading_params(
        &mut self,
        rsp: Option<&CThostFtdcBrokerTradingParamsField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        last: bool,
    ) {
        let err = rsp_err(info);
        if let Some(r) = rsp {
            eprintln!("[SPI] OnRspQryBrokerTradingParams MarginPriceType={} Algorithm={} is_last={}",
                r.MarginPriceType, r.Algorithm, last);
        }
        let _ = self.tx.send(Event::QryBrokerTradingParamsRsp { err, is_last: last });
    }

    fn on_rsp_qry_broker_trading_algos(
        &mut self,
        rsp: Option<&CThostFtdcBrokerTradingAlgosField>,
        info: Option<&CThostFtdcRspInfoField>,
        _req: i32,
        last: bool,
    ) {
        let err = rsp_err(info);
        if let Some(r) = rsp {
            eprintln!("[SPI] OnRspQryBrokerTradingAlgos ExchangeID={} InstrumentID={} is_last={}",
                read_str(&r.ExchangeID), read_str(&r.InstrumentID), last);
        }
        let _ = self.tx.send(Event::QryBrokerTradingAlgosRsp { err, is_last: last });
    }

    // ── 柜台主动推送 ──
    fn on_rtn_trading_notice(&mut self, notice: Option<&CThostFtdcTradingNoticeInfoField>) {
        if let Some(n) = notice {
            let content = read_str(&n.FieldContent);
            eprintln!("[SPI] OnRtnTradingNotice SendTime={} Content={}", read_str(&n.SendTime), content);
            let _ = self.tx.send(Event::RtnTradingNotice { content });
        }
    }

    fn on_rtn_instrument_status(&mut self, status: Option<&CThostFtdcInstrumentStatusField>) {
        if let Some(s) = status {
            let inst = read_str(&s.InstrumentID);
            let ex = read_str(&s.ExchangeID);
            let st = s.InstrumentStatus;
            eprintln!("[SPI] OnRtnInstrumentStatus {} {} status={}", ex, inst, st);
            let _ = self.tx.send(Event::RtnInstrumentStatus { instrument: inst, exchange: ex, status: st });
        }
    }

    // ── 通用错误 ──
    fn on_rsp_error(
        &mut self,
        p_rsp_info: Option<&CThostFtdcRspInfoField>,
        request_id: i32,
        _is_last: bool,
    ) {
        let err = rsp_err(p_rsp_info);
        if let Some((id, m)) = &err {
            eprintln!("[SPI] OnRspError req={} ErrorID={} {}", request_id, id, m);
        }
        let _ = self.tx.send(Event::RspError { err, request_id });
    }
}

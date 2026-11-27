// ╔══════════════════════════════════════════════════════════════════╗
// ║ 【文件头教学导读】spi.rs —— CTP 世界与 Rust 世界之间的「翻译官」          ║
// ╚══════════════════════════════════════════════════════════════════╝
//
// SPI（Service Provider Interface）是 CTP 这类 C/C++ 库的经典玩法：你实现一个
// 「回调接口」交出去，库在合适的时机反过来调用你。就像你给快递站留了电话，
// 包裹一到（行情一到）快递站打你电话（on_rtn_depth_market_data 被调用）。
//
// 本文件把 C 库的回调「翻译」成我们自己定义的 MemberEvent 信封塞进管道：
//   · 连接上了        → 塞 MemberEvent::Connected；
//   · 断线了          → 塞 MemberEvent::Disconnected；
//   · 登录应答回来了  → 塞 MemberEvent::Login；
//   · 行情快照回来了  → 塞 MemberEvent::Tick{…}。
// 谁来消费这些信封？session.rs 里的 member 线程（见下一个文件）。
//
// 【它在整条链路里的位置】CTP连接 → tick → 分钟K线 → jsonl分片 → Parquet合并。
// 这是链路的第 0 环：tick 在这里第一次被「接进 Rust 世界」。
//
// 【本文件用到的 Rust 核心概念】
//   · trait 与 impl：Rust 的「接口」机制，回调接口的教科书场景；
//   · &mut self：回调方法拿可变借用（CTP 回调是串行的，改自己的信箱没问题）；
//   · Option<&T>：把 C 的「可能为空的指针」安全化为 Rust 类型；
//   · 类型转换 try_from / unwrap_or：带失败可能的转换与兜底；
//   · `let _ =` ：明知可能失败、故意忽略返回值的写法（含理由）。
//
// 【建议学习时长】15 到 20 分钟。读之前建议先看 events.rs。

//! 行情 SPI：CTP 回调 → 事件（照抄 v1.1，一字未改）。
use crate::events::{MemberEvent, info_pair}; // 从本 crate 的 events 模块引入信封定义和错误翻译函数
use ctp2rs::ffi::WrapToString; // 给 C 字符数组补 to_string 方法的 trait
use ctp2rs::v1alpha1::{
    CThostFtdcDepthMarketDataField, CThostFtdcRspInfoField, CThostFtdcRspUserLoginField,
    CThostFtdcSpecificInstrumentField, MdSpi,
}; // CTP 协议里用到的几个 C 结构体 + SPI 接口定义
use std::sync::mpsc; // mpsc = 多生产者单消费者管道，线程间传消息的标准件

// ============================================================
// SPI（照抄 v1.1，一字未改）
// ============================================================

// ════════════════ Rust 概念教学 ①：trait（特征）—— Rust 的「接口」 ════════════════
// C++ 用虚函数表做回调，Rust 用 trait。MdSpi 是 ctp2rs 库定义的接口（trait），
// 里面声明了一堆 on_xxx 方法；我们写 struct MemberSpi 再 impl MdSpi for MemberSpi
// 「认领」这个接口、填空自己关心的回调。没填的方法库里有默认实现（空操作）。
//
// 生活类比：trait 像一张「上岗证考纲」，考试科目（方法）都列好了；你交的答卷
//（impl）至少要覆盖必考科目，其余科目默认交白卷（默认实现）。
//
// 【和 C 侧怎么接上的】session.rs 里会执行 Box::into_raw 把 MemberSpi 变成裸指针
// 注册给 C 库；C 库回调时通过指针找到它。这一步的 unsafe 被 ctp2rs 封装掉了，
// 你在本文件看不到 unsafe 关键字——但要知道：trait 对象（dyn MdSpi）+ 裸指针
// 就是 C 与 Rust 对接的桥。
//
// 【为什么回调拿 &mut self】C 库保证同一时刻只调一个回调（串行），所以方法可以
// 拿可变借用改自身字段（比如 self.tx）。要是多线程同时回调，Rust 会要求你用
// 更重的同步手段（Mutex）——borrow checker（借用检查器）管得很严。
// ════════════════════════════════════════════════════════════════
pub struct MemberSpi {
    pub(crate) member_idx: usize, // 我是几号机：塞进每封信里，下游才知道行情来自哪条连接
    pub(crate) tx: mpsc::SyncSender<MemberEvent>, // 发信管道的「发送端」（SyncSender 是有界管道，满了会阻塞或报错）
}

// 给 MemberSpi 认领 MdSpi 接口：只填关心的回调，其余用库的默认实现
impl MdSpi for MemberSpi {
    fn on_front_connected(&mut self) {
        // 行内微注释：`let _ =` 表示「返回值我不要了」。send 在接收端已挂时会
        // 返回 Err，但那时整个 member 线程都在退出，报不报都无所谓——
        // 故意忽略，并写明理由（这也是 clippy 禁 let_underscore_untyped 之外的
        // 一种「显式放弃」姿势）。
        let _ = self.tx.send(MemberEvent::Connected(self.member_idx)); // 连上了：发「已连接」信
    }
    fn on_front_disconnected(&mut self, reason: i32) {
        let _ = self
            .tx
            .send(MemberEvent::Disconnected(self.member_idx, reason)); // 断线了：带上 CTP 原因码
    }
    fn on_rsp_user_login(
        &mut self,
        _rsp: Option<&CThostFtdcRspUserLoginField>, // 登录应答正文：这里不需要，下划线开头表示「参数故意不用」
        rsp_info: Option<&CThostFtdcRspInfoField>, // 错误应答：登录成败的判定依据
        _request_id: i32,
        _is_last: bool,
    ) {
        let _ = self
            .tx
            .send(MemberEvent::Login(self.member_idx, info_pair(rsp_info))); // 信里装上 info_pair 翻译好的「成功/失败+原因」
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
        depth_market_data: Option<&CThostFtdcDepthMarketDataField>, // 行情快照；None = C 库给了空指针
    ) {
        // ════════════════ Rust 概念教学 ②：Option<&T> 与 if let ════════════════
        // C 的回调参数是「指针，可能为 NULL」。Rust 绑定把它翻成 Option<&T>：
        //   · Some(引用) = 指针有效，数据在这；
        //   · None = 指针为空。编译器强制你先「拆包」再用，想都不想直接取值是
        //     编译不过的——空指针崩溃（C 里最常见的 segfault）在这里被类型系统根除。
        //
        // 拆包姿势有两种：
        //   · if let Some(q) = … { … }：只有 Some 才干活，None 直接略过（本文件选它，因为
        //     空行情没意义，静默丢弃即可）；
        //   · match … { Some(q) => …, None => … }：两种都要认真处理时用 match。
        //
        // 【&T 是「共享借用」】q 只是「借」来看，不拥有数据；回调一结束借用自动失效，
        // 借用检查器保证不会用悬垂指针。想留数据必须 .clone() 或转成自己的 String。
        // ════════════════════════════════════════════════════════════════
        if let Some(q) = depth_market_data {
            // Volume 是 CTP int（当日累计成交量）；Turnover/OpenInterest 是 double。
            // ════════════════ Rust 概念教学 ③：类型转换 try_from 与 unwrap_or ════════════════
            // q.Volume 在 C 里是 int（可正可负）；我们 Rust 侧字段是 u64（只装非负）。
            // 转换可能失败（负数），所以用 try_from 返回 Result；再 unwrap_or(0)
            // 「失败了就给 0」。两步连起来的意思：负数视为脏数据、归零。
            //
            // 对比记忆（重要）：
            //   · u64::try_from(x)：可能失败的转换 → Result，必须显式处理；
            //   · x as u64：无条件强转，负数会「回绕成巨大正数」——危险，本工程禁用；
            //   · i64::from(x)：保证不失败的转换（i32→i64 必然装得下）→ 无需处理。
            // 三条路分别对应「可能炸 / 强行转 / 绝不炸」，Rust 把差别写进了类型名。
            //
            // q.InstrumentID 是 C 定长字符数组（[i8; 31]），靠 WrapToString trait 的
            // to_string() 转成真正的 Rust String——C 数组离开回调作用域就没了，
            // 所以必须复制成拥有所有权的 String 再发出去。
            // ════════════════════════════════════════════════════════════════
            let _ = self.tx.send(MemberEvent::Tick {
                member_idx: self.member_idx, // 报上自己的编号
                instrument: q.InstrumentID.to_string(), // C 数组 → String（复制所有权）
                last_price: q.LastPrice, // f64 直接抄
                volume: u64::try_from(q.Volume).unwrap_or(0), // CTP Volume 是 i32 当日累计；负数为脏数据归 0
                turnover: q.Turnover, // 累计成交额
                open_interest: q.OpenInterest, // 持仓量
                trading_day: q.TradingDay.to_string(), // 交易日
                update_time: q.UpdateTime.to_string(), // 交易所时间戳（识别重演快照用）
                update_millisec: q.UpdateMillisec, // 毫秒部分
            }); // 行内微注释：send 失败（接收端挂了）忽略——同上，退出期无关紧要
        }
    }
    fn on_rsp_error(
        &mut self,
        p_rsp_info: Option<&CThostFtdcRspInfoField>,
        n_request_id: i32,
        _b_is_last: bool,
    ) {
        // ════════════════ Rust 概念教学 ④：if let 链与 let 链（let chains） ════════════════
        // `if let Some(e) = p_rsp_info && e.ErrorID != 0` 是 Rust 2024 纪元的新写法
        // 「let 链」：先拆包 Some，再用 && 追加条件判断，一步到位。等价的旧写法是
        // if let Some(e) = p_rsp_info { if e.ErrorID != 0 { … } } 两层嵌套。
        // 记法：&& 左边是「拆包」，右边是「对拆出来的值的判断」。
        // ════════════════════════════════════════════════════════════════
        if let Some(e) = p_rsp_info
            && e.ErrorID != 0
        {
            tracing::error!(
                "[MD#{}] RSP ERROR request_id={} ErrorID={} ErrorMsg={}",
                self.member_idx,
                n_request_id,
                e.ErrorID,
                e.ErrorMsg.to_string()
            ); // 行内微注释：错误只打日志不上报——这是协议级杂音（如重复订阅），不值得打断主流程
        }
    }
}

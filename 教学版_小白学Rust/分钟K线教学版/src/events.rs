// ╔══════════════════════════════════════════════════════════════════╗
// ║ 【文件头教学导读】events.rs —— 全工程消息的「信封样式大全」              ║
// ╚══════════════════════════════════════════════════════════════════╝
//
// 整个程序是多线程的：CTP 库的回调线程、每个 member 的事务线程、聚合线程、
// 落盘线程彼此之间不直接调用函数，而是「往管道里塞纸条」互传消息。
// 本文件定义的就是纸条的两种样式：
//   · MemberEvent：CTP 库 → member 线程 的事件（连接上、断开、登录结果、tick）；
//   · TickEvent：member 线程 → 聚合线程 的行情快照（聚合线程只关心行情本身）。
//
// 【它在整条链路里的位置】CTP连接 → tick → 分钟K线 → jsonl分片 → Parquet合并。
// 这里是链路的最前端：tick 数据的第一站「打包格式」就定义在本文件。
// spi.rs 把 CTP 回调翻译成 MemberEvent；session.rs 消费 MemberEvent 并转成
// TickEvent；aggregate.rs 消费 TickEvent 去聚合分钟K线。
//
// 【本文件用到的 Rust 核心概念】
//   · enum 枚举与「带数据的变体」：一个类型装多种消息，是 Rust 的招牌能力；
//   · struct 结构体与 pub(crate) 字段可见性；
//   · Option<&T>：表示「C 库可能给你空指针」，Rust 用 Option 把它安全化；
//   · match 模式匹配与通配 _ 分支；
//   · FFI（外部函数接口）类型：CThostFtdcRspInfoField 是 CTP 的 C 结构体在 Rust 的映射。
//
// 【建议学习时长】15 到 20 分钟。配合 spi.rs（谁造信封）一起看更好。

use ctp2rs::ffi::WrapToString; // WrapToString：ctp2rs 给 C 字符数组（如 [i8; 64]）提供的「转 &str」扩展能力
use ctp2rs::v1alpha1::CThostFtdcRspInfoField; // CTP 协议的「错误应答」结构体（C 语言定义，绑定工具转成了 Rust 结构体）

// ============================================================
// member 事件 / tick 统一结构（照抄 v1.1）
// ============================================================

// ════════════════ Rust 概念教学 ①：enum 枚举与「带数据的变体」 ════════════════
// Rust 的 enum 远比 C 的 enum 强：每个变体可以【随身携带不同的数据】。
// MemberEvent 四个变体就是四种信封：
//   · Connected(usize)                    信封里只装一个编号（第几号 member）；
//   · Disconnected(usize, i32)            装两个：编号 + 断线原因码；
//   · Login(usize, Option<(i32, String)>) 装编号 + 「登录结果，可能有也可能没有」；
//   · Tick { … }                          装一大包命名字段（struct 风格变体）。
//
// 生活类比：enum 像快递站的「包裹类型登记表」——信件、箱货、生鲜各自要求
// 不同的随附单据；快递员（match 语句）按类型分拣，绝不拿错。
//
// 【为什么这里不 derive(Serialize)】这个 enum 只在线程间传递（内存里），不落盘、
// 不上网络，所以不需要序列化；而聚合端的 MinuteBar 要写成 jsonl，就 derive 了
// Serialize/Deserialize。按需派生，不多不少。
//
// 【Option<(i32, String)> 怎么读】从里到外拆：String 是字符串，i32 是错误码，
// (i32, String) 是二者打包的元组，Option<…> 表示「可能没有」。None=登录成功，
// Some((错误码, 错误消息))=登录失败。用类型把「可能不存在」说在明面上，
// 编译器会逼你两种情况都处理——空指针异常在 Rust 里从根上杜绝。
// ════════════════════════════════════════════════════════════════
pub enum MemberEvent {
    Connected(usize), // 已连上前置。usize = member 编号（0 到 15），也就是「几号机」
    Disconnected(usize, i32), // 已断开：编号 + CTP 原因码
    Login(usize, Option<(i32, String)>), // 登录应答：None=成功，Some(错误码, 错误消息)=失败
    /// ★v1.5 方案B：Ready 事件已删除——登录+订阅由 member 线程在 Connected 分支里直接执行并记账，
    ///   事件通道里不再有"中转确认"这一环（少一跳少一处可能丢）。
    Tick {
        member_idx: usize, // 这笔行情来自哪个 member（第几号机）
        instrument: String, // 合约代码，比如 "IF2401"
        last_price: f64, // 最新价（f64 = 双精度浮点数）
        volume: u64, // 当日累计成交量（CTP 给的是累计值，不是这笔的增量）
        turnover: f64, // 当日累计成交额
        open_interest: f64, // 持仓量
        trading_day: String, // 交易日，如 "20261127"
        /// 红标3 单调性校验用的交易所侧时间（D025）："HH:MM:SS" 与毫秒部分。
        update_time: String, // 交易所时钟的时分秒（用于识别登录重演的陈旧快照）
        update_millisec: i32, // 毫秒部分
    },
}

// ════════════════ Rust 概念教学 ②：struct、derive 与字段可见性 ════════════════
// TickEvent 是「纯行情快照」，比 MemberEvent::Tick 少了事件包装、多了 derive。
//   · #[derive(Debug, Clone)]：自动生成两套代码——
//       Debug 允许 {:?} 打印（排查问题用）；Clone 允许 .clone() 复制一份。
//     这里需要 Clone 吗？严格说通道传递不需要，但保留它让测试和排查更方便。
//   · pub(crate)：字段在 crate 内可读写，外部不可见——「工程内部公开」。
//     对比 main.rs 里 enum 变体用的匿名元组：元组变体天然不涉及字段可见性。
//
// 【u64 与 usize 的区别】u64 是纯计数类型；usize 是「和指针同宽」的无符号整数，
// 专用于索引和长度（Vec 的下标就是 usize）。member_idx 用 usize 是因为它要当
// 数组下标用（stats[idx]）。
// ════════════════════════════════════════════════════════════════
#[derive(Debug, Clone)]
pub struct TickEvent {
    pub(crate) member_idx: usize, // 来源 member 编号
    pub(crate) instrument: String, // 合约代码
    pub(crate) last_price: f64, // 最新价
    pub(crate) volume: u64, // 当日累计成交量
    pub(crate) turnover: f64, // 当日累计成交额
    pub(crate) open_interest: f64, // 持仓量
    pub(crate) trading_day: String, // 交易日
    /// 红标3 透传字段（D025）：单调性校验在 member 线程完成，这里只是透传，
    ///   供排查/未来下游使用。当前无人读，标 `dead_code` 压制。
    #[allow(
        dead_code,
        reason = "D025 透传字段：单调性校验在 member 线程完成，当前无人读，留作排查/未来下游"
    )]
    pub(crate) update_time: String,
    #[allow(
        dead_code,
        reason = "D025 透传字段：毫秒精度留作排查/未来下游，当前无人读"
    )]
    pub(crate) update_millisec: i32,
}

// ════════════════ Rust 概念教学 ③：match 模式匹配与「穷尽性检查」 ════════════════
// info_pair 只有三行，却浓缩了 Rust 错误处理的典型姿势。它把 CTP 的错误应答
// 结构体「翻译」成我们自己的 Option<(错误码, 消息)>：
//   · match i { … }：对 Option 穷尽匹配。Option 只有两个变体（Some/None），
//     漏写一个编译器直接报错——这就是「穷尽性检查」，编译器替你查漏；
//   · Some(x) if x.ErrorID != 0：叫「匹配守卫」，先匹配 Some 再检查附加条件，
//     条件不满足就落到 _ 分支——比 if 嵌套清爽得多；
//   · _ => None：通配分支。这里 Some(x) 且 ErrorID==0（没错误）也归为「无事」。
//
// 【FFI 边界的类型净化】CThostFtdcRspInfoField 是 C 结构体（字段 ErrorID 是 C 的
// int，ErrorMsg 是 C 的定长字符数组）。到了这个函数，它被「净化」成纯 Rust 类型
//（i32 与 String）。原则：C 的东西不越过 FFI 边界乱跑，边界处转成安全类型。
//   · x.ErrorMsg.to_string()：C 字符数组 → Rust String。能这样写是因为
//     ctp2rs 通过 trait（WrapToString）给 C 数组补了 to_string 方法——
//     trait 可以给别人的类型「追加方法」，这是 Rust 的扩展机制。
// ════════════════════════════════════════════════════════════════
/// 把 CTP 的错误应答转成 Option<(错误码, 错误消息)>；无错返回 None。
pub fn info_pair(i: Option<&CThostFtdcRspInfoField>) -> Option<(i32, String)> {
    match i {
        Some(x) if x.ErrorID != 0 => Some((x.ErrorID, x.ErrorMsg.to_string())), // 有错：打包错误码和消息
        _ => None, // 指针为空，或错误码为 0（=没出错）：都返回 None
    }
}

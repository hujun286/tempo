// ╔══════════════════════════════════════════════════════════════════╗
// ║ 【文件头教学导读】stats.rs —— member 的「体检表」与「开锁师傅」          ║
// ╚══════════════════════════════════════════════════════════════════╝
//
// 这个文件只有二十来行，却是全工程「最好入门」的一课：它干两件事——
//   1）定义 MemberStats 结构体：给每一个行情连接（member，即一条到 CTP 前置的会话）
//      记一本「体检表」：连上了吗、登录了吗、收到多少笔行情（tick）、断了几次、
//      主动重建了几次……主线程每隔 5 秒把 16 本表汇总起来打一条健康日志。
//   2）提供 锁或恢复() 函数：从互斥锁（Mutex）里把数据安全取出来，就算锁「中毒」
//      也不慌，照常取数据。全工程凡是要读统计表的地方都调它。
//
// 【它在整条链路里的位置】CTP连接 → tick → 分钟K线 → jsonl分片 → Parquet合并。
// 本文件不在数据主干上，而是挂在旁边的「仪表盘」：session.rs（每条连接的线程）
// 往表里记账，main.rs（主线程巡检）读表打日志。数据（tick）不经过这里。
//
// 【本文件用到的 Rust 核心概念】
//   · struct 结构体 + derive(Default, Clone)：一行注解给结构体自动生成代码；
//   · pub(crate) 可见性：只在当前工程内可见，对外（crate 外）是隐私；
//   · Option<Instant>：表示「可能还没有值」（比如还没断过线，就没有断线时刻）；
//   · 泛型函数 <T>：一份代码服务所有类型；
//   · Mutex / MutexGuard / 锁中毒（poisoning）：多线程共享数据的入门必修课；
//   · unwrap_or_else：不用 unwrap() 也能拿到值的「容错写法」（本工程宪法禁 unwrap）。
//
// 【建议学习时长】10 到 15 分钟。建议它是你读的第一个文件。

//! member 运行统计 + 锁中毒恢复工具。
use std::time::Instant;

// ════════════════ Rust 概念教学 ①：derive(Default, Clone) 是什么 ════════════════
// #[derive(...)] 叫「派生注解」：让编译器替你自动实现某个 trait（trait 可理解为
// 一套「能力清单」，比如 Default 的能力是「能造一个全零初始值」）。
//   · Default：MemberStats::default() 会给出一个所有字段为零值的新实例——
//     bool 全是 false、u64 全是 0、Option 全是 None。main.rs 建表时
//     vec![MemberStats::default(); 16] 就靠它，一行造出 16 本空白体检表。
//   · Clone：允许 .clone() 深拷贝一份。本文件的表其实不 clone，但加上无害。
// 生活类比：Default 像「酒店入住时的初始房间状态」（灯全关、电视全关），
// 你不用一个个字段手写初始化，编译器按类型自动给出「出厂设置」。
//   · u64 是「无符号 64 位整数」：只能装 0 和正数，计数器用它不会出负数；
//   · i32 是「有符号 32 位整数」：能装负数，这里放 CTP 返回的断线原因码。
// ════════════════════════════════════════════════════════════════
#[derive(Default, Clone)]
pub struct MemberStats {
    // pub(crate) 读作「crate 内公开」：本工程（一个 crate）里谁都能读，
    // 但发布成库给外人用时不可见。类比「公司内部公开、对外保密」。
    // 字段全部 pub(crate) 是因为：session.rs 写表、main.rs 读表，都不在本文件里。
    pub(crate) connected: bool, // 当前是否连上前置（每个 member 一条连接）
    pub(crate) logged_in: bool, // 当前是否已登录成功
    pub(crate) login_fail: u64, // 累计登录失败次数
    pub(crate) disconnects: u64, // 累计断线次数
    pub(crate) ready_rounds: u64, // 累计「登录加订阅」完成的轮数（第几轮就绪）
    pub(crate) last_disconnect_reason: i32, // 最近一次断线的原因码（CTP 给的整数）
    pub(crate) last_disconnect_at: Option<Instant>, // 最近一次断线时刻；None=从没断过
    pub(crate) ticks: u64, // 累计收到多少笔行情快照（tick）
    pub(crate) last_tick_at: Option<Instant>, // 最近一笔 tick 的时刻；主线程靠它发现「断流」
    /// 红标3 交易所时间回拨笔数（观测计数，不影响数据流）
    pub(crate) tick_out_of_order: u64,
    /// 方案B 主动 teardown→重建 次数（连接超时 / 断线超窗 / 连续登录失败三类收敛于此）
    pub(crate) rebuilds: u64,
}

// ════════════════ Rust 概念教学 ②：Mutex、锁中毒与 unwrap_or_else ════════════════
// Mutex<T>（互斥锁）就像「洗手间门锁」：想用数据 T，先 lock() 拿钥匙（MutexGuard），
// 用完 guard 被丢弃（离开作用域）就自动还锁——Rust 用「所有权」保证不会忘还。
//
// 【锁中毒是什么】如果某个线程「拿着锁的时候 panic 了」，Mutex 会把自己标记为
// 「中毒（poisoned）」：之后别人 lock() 会返回 Err。这是 Rust 的保护机制——
// 提醒你「上一个持锁人可能把数据改坏了一半」。
//
// 【本工程为什么这样写】普通写法是 锁.lock().unwrap()，但 unwrap 遇到 Err 会
// 连带 panic：一个线程挂 → 全体跟着挂，采集全军覆没。这里用
// unwrap_or_else(PoisonError::into_inner)：「就算中毒，也把里面的数据原样给我」。
// 统计计数丢一点无所谓，采集线程活着最重要——这是有意的工程取舍。
// 生活类比：有人晕倒在浴室里，与其把整个浴室封锁（全体憋着），不如开门把人扶出去继续用。
//
// 【顺带学两个语法】
//   · <T>：泛型。这个函数对任何 T 的 Mutex 都适用，不用为每种类型写一遍；
//   · MutexGuard<'_, T> 里的 '_：匿名生命周期标注，意思是「借用有效期跟调用者走」，
//     编译器自动推算，平时不用手写——这就是「生命周期多数时候不用写」的例子。
// ════════════════════════════════════════════════════════════════
// 泛型函数：入参是「对 Mutex 的借用」（& 表示只借不拿所有权），
// 出参是「守卫」——持有守卫期间，别人 lock() 会阻塞等待。
pub fn 锁或恢复<T>(锁: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    // 行内微注释：unwrap_or_else 传一个「出错了怎么办」的处理函数（这里是
    // PoisonError::into_inner，即「把中毒锁里的真数据掏出来」）。
    // 这正是本工程宪法「禁 unwrap/expect」的标准替代写法之一。
    锁.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

// ════════════════ Rust 概念教学 ③：中文标识符（本工程的特殊约定） ════════════════
// 上面函数名「锁或恢复」是合法的 Rust 标识符！Rust 允许 Unicode 标识符，
// 但 rustfmt/clippy 的默认风格检查（nonstandard_style）会提醒「函数名应该
// 用小写蛇形」——本工程在 config.rs 顶部用 crate 级豁免声明放行了它：
//   #![allow(nonstandard_style, reason = "工程全局中文标识符……")]
// 这是团队刻意的约定：让关键业务函数（锁或恢复、在交易时段内、合并一日）
// 一眼可读。你新写函数时可以跟英文，也可以跟中文，风格别混得太乱即可。
// ════════════════════════════════════════════════════════════════

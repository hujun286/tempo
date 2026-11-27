// ╔══════════════════════════════════════════════════════════════════╗
// ║ 【文件头教学导读】session.rs —— 每条行情连接的「总管」与「守护者」           ║
// ╚══════════════════════════════════════════════════════════════════╝
//
// 全市场几万只合约靠 16 条连接（member）分摊订阅。每个 member 一个线程，
// 本文件就是线程的「主循环剧本」：
//   · ApiHolder：持有 CTP 的 MdApi 对象（Option 包着：可能还没创建/已拆掉）；
//   · 事件循环：从私有事件通道收信（连接/断开/登录/tick），按事件做该做的事；
//   · 守护定时器：收不到事件时每秒醒来巡检——连接超时、断线超窗、连续登录
//     失败三类异常统统收敛到「拆掉重建（teardown→spawn_new）」；
//   · 退避（backoff）：重建间隔从 1 秒起步翻倍，封顶 60 秒，不死缠烂打柜台。
//
// 【它在整条链路里的位置】CTP连接 → tick → 分钟K线 → jsonl分片 → Parquet合并。
// 本文件是「CTP连接」这一环的大脑：连接稳，后面才有 tick。
//
// 【本文件用到的 Rust 核心概念】
//   · Option<MdApi> 与 take()：资源「可能没有」的状态机表达；
//   · Box::into_raw 与 FFI 裸指针：把 Rust 对象交给 C 库回调（安全边界在哪）；
//   · Arc<Mutex<Vec<MemberStats>>> 跨线程记账 与 锁或恢复（锁中毒恢复）；
//   · Arc<AtomicBool> 停止旗标：一根「全员能读的灯绳」；
//   · match recv_timeout：事件驱动主循环的标准骨架；
//   · HashMap 里存元组 (String, i64, Option<Instant>)：轻量复合状态；
//   · let-else：提前返回的优雅写法。
//
// 【建议学习时长】60 分钟。全工程最长的文件，建议分三段读：ApiHolder →
// login_and_subscribe → spawn_member 主循环。

// ── 宪法豁免（带理由）：clippy::indexing_slicing ─────────────────────────
// 本文件下标访问全部位于 CTP FFI 交互与 16-member 分片分配边界：
//   · member_idx 由 shard_contracts 分片分配器生成（0..num_members，边界由构造保证）
//   · CTP 结构体字段为协议固定长度 C 数组，下标来自协议常量
// v1.4 实盘验证过且工程文档明文「与 v1.1 逐字一致，改动=引入风险」，
// 故按宪法逃生舱做整文件显式豁免，不逐处改 get() 触碰热路径。
#![allow(
    clippy::indexing_slicing,
    reason = "CTP FFI/分片分配边界由构造保证；v1.4 实盘逐字一致原则，不触碰热路径"
)]
//! CTP 会话：ApiHolder / 登录订阅 / ★v1.5 方案B 事件驱动重连状态机 / 红标3 tick 单调性校验。
use crate::config::{
    CONNECT_TIMEOUT_MS, DISCONNECT_REBUILD_MS, LOGIN_FAIL_REBUILD, LOGIN_SETTLE_MS,
    RECONNECT_BASE_MS, RECONNECT_MAX_MS, SUBSCRIBE_BATCH, SUBSCRIBE_DELAY_MS, 回拨告警节流,
};
use crate::events::{MemberEvent, TickEvent};
use crate::spi::MemberSpi;
use crate::stats::{MemberStats, 锁或恢复};
use ctp2rs::ffi::AssignFromString;
use ctp2rs::v1alpha1::{CThostFtdcReqUserLoginField, MdApi, MdSpi};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ============================================================
// 启动 member（★v1.5 方案B：事件驱动重连状态机，替代 api.join() 阻塞版）
// ============================================================

// ════════════════ Rust 概念教学 ①：Option<MdApi> 与 take()——资源持有 ════════════════
// ApiHolder 只有一个字段值得细看：api: Option<MdApi>。为什么用 Option 包？
//   · 刚 new 时还没有 api（None）；spawn_new 成功才有（Some）；
//   · 重建时要先把旧的【完整拆掉】（teardown 调 api.release() 释放 CTP 资源），
//     再造新的——「先还给库、再要新的」，不能一边占着一边换。
// take() 就是完成这个动作的利器：把 Some 里的值整个拿出来，原位留 None。
// 拿出来的 api 在 teardown 里被 release 消费掉，所有权走到尽头自动清理。
//
// 生活类比：Option 像手上的「房卡套」，take() 是把房卡真掏出来交还前台；
// 套子（Option 外壳）还在手上，但里面已经空了——下次 spawn_new 再塞新卡。
//
// 【新手的常见问句】「为什么要 Option，直接留个 MdApi 字段不行吗？」
// 不行：Rust 没有「空对象」，字段要么有值要么没值必须二选一地写进类型；
// 编译器强制你处理 None 分支（没建 api 就调 login？编译不过）——空指针
// 这类事故在类型层面被拒绝，这就是 Option 的全部意义。
// ═══════════════════════════════════════════════════════════════
pub struct ApiHolder {
    api: Option<MdApi>,
    dynlib: PathBuf,
    flow_dir: PathBuf,
}

impl ApiHolder {
    const fn new(dynlib: PathBuf, flow_dir: PathBuf) -> Self {
        Self {
            api: None,
            dynlib,
            flow_dir,
        }
    }

    fn spawn_new(
        &mut self,
        member_idx: usize,
        front: &str,
        tx: &mpsc::SyncSender<MemberEvent>,
    ) -> bool {
        self.teardown(); // 行内微注释：先拆旧的（可能没有旧的就空拆一次），保证资源不叠加
        std::fs::create_dir_all(&self.flow_dir).ok(); // 行内微注释：CTP 要求流文件目录存在；ok() 容错——建失败建 api 时还会报错，不在此处升级
        // 【规则15 零 unwrap】路径来自工作区根+ASCII相对路径，必是合法 UTF-8；
        //   用 to_string_lossy 兜底(永不失败)，且【绑定到本地 let】保证 &str 活过 FFI 调用，无悬垂。
        //   行内微注释：to_string_lossy 把路径转 String（非法字节替换成占位符，永不 panic）；
        //   「绑定到本地 let」很重要——FFI 借用必须活过整个调用，临时值会悬垂。
        let dynlib_s = self.dynlib.to_string_lossy().into_owned();
        let flow_s = self.flow_dir.to_string_lossy().into_owned();
        let api = MdApi::create_api(&dynlib_s, &flow_s, false, false); // 非 union 版 4 参（实机 .so 同 v1.4 只有 2-bool 符号；production_mode 第 5 参仅 union 版支持）
        let spi = Box::new(MemberSpi {
            member_idx,
            tx: tx.clone(),
        });
        // ════════════════ Rust 概念教学 ②：Box::into_raw 与 FFI 裸指针 ════════════════
        // 下面两行是全工程最接近「危险区」的代码，也是 FFI 的必修一课：
        //   · Box::new(MemberSpi{…})：在堆上造一个 MemberSpi（Box = 独占所有权的堆指针）；
        //   · Box::into_raw(spi)：把 Box 拆成【裸指针】交给 C 库——从此 Rust 的
        //     自动回收不管它了，就像把车钥匙交给代驾：车还是你的，但代驾在开。
        //   · api.register_spi(spi_ptr)：C 库记下指针，行情一到就通过它回调
        //     MemberSpi 的 on_xxx 方法（见 spi.rs）。
        //
        // 【谁负责释放】CTP 的 api.release() 会释放 SPI（teardown 里触发）——
        // 这类「两边约定的释放责任」在 FFI 里必须逐条写进注释，否则就是内存泄漏
        // 或二次释放的双重隐患。
        //
        // 【为什么这里没写 unsafe】按理说裸指针操作必须包 unsafe 块；但 ctp2rs
        // 这个绑定库把 unsafe 封装在 register_spi 内部了——我们调用的是【安全函数】。
        // 学到的是：FFI 的 unsafe 可以被封装收口，让上层业务代码保持安全外观；
        // 但「安全的外观」不等于「没有风险」，只是风险被集中看管了。
        //
        // 【dyn MdSpi 是什么】「动态分发的 SPI 接口对象」（trait object）：编译期
        // 不确定具体类型，运行时通过虚表找到真正的方法——和 C++ 虚函数同理。
        // ═══════════════════════════════════════════════════════════════
        let spi_ptr = Box::into_raw(spi) as *mut dyn MdSpi;
        api.register_spi(spi_ptr);
        api.register_front(front);
        api.init();
        self.api = Some(api);
        true
    }

    fn teardown(&mut self) {
        // 行内微注释：if let Some(api) = self.api.take()——先 take 走所有权再释放；
        // release() 消费 api（它自己的 drop 逻辑会连 SPI 一起清），一行完成「拆掉并置空」。
        if let Some(api) = self.api.take() {
            api.release();
        }
    }
}

fn login_and_subscribe(
    api: &MdApi,
    broker_id: &str,
    user_id: &str,
    password: &str,
    instruments: &[String],
    tag: &str,
) -> bool {
    let mut login = CThostFtdcReqUserLoginField::default();
    login.BrokerID.assign_from_str(broker_id);
    login.UserID.assign_from_str(user_id);
    login.Password.assign_from_str(password);
    let ret = api.req_user_login(&mut login, 1);
    tracing::info!("{} req_user_login ret={}", tag, ret);
    if ret != 0 {
        return false;
    }
    std::thread::sleep(Duration::from_millis(LOGIN_SETTLE_MS));

    let total = instruments.len();
    let mut done = 0;
    for batch in instruments.chunks(SUBSCRIBE_BATCH) {
        let r = api.subscribe_market_data(batch);
        done += batch.len();
        tracing::info!("{} 订阅 {}/{} ret={}", tag, done, total, r);
        std::thread::sleep(Duration::from_millis(SUBSCRIBE_DELAY_MS));
    }
    tracing::info!("{} 订阅完成 {}", tag, done);
    true
}

// ════════════════ Rust 概念教学 ③：spawn_member 的参数清单——一次「跨线程搬家」 ════════════════
// 这个函数签名长（已被 clippy::too_many_arguments 豁免），但每一项都有讲究：
//   · 字符串/PathBuf/Vec 按值传入（String、PathBuf、Vec<String>）：它们要被
//     move 进新线程。线程可能比函数活得更久，借用不安全，必须【搬家】——
//     这就是为什么 main.rs 里到处是 .clone()：拷一份给线程，原件留在主线程；
//   · evt_tx：本 member 私有事件通道的发送端（SPI 回调用它发信）；
//   · evt_rx：同一个通道的接收端（本线程自己收）——注意发送与接收两端
//     都进同一个线程的剧本：SPI 回调在 CTP 的线程里发生，信从那里塞进来；
//   · tick_tx：转发行情的全局管道（聚合线程收）；
//   · stats: Arc<Mutex<…>> 与 stop: Arc<AtomicBool>：多线程共享的「共阅文件」
//     与「共读灯绳」，Arc 让多个线程共同持有同一份数据的所有权（下面细讲）。
// 返回值 JoinHandle<()>：线程的「收据」，main 最后 join() 它等待线程退出。
// ═══════════════════════════════════════════════════════════════
#[allow(
    clippy::too_many_arguments,
    reason = "16-member spawn 签名与 CTP API 形态对齐（idx/front/flow/凭据/通道），参数从公开接口传齐"
)]
#[expect(
    clippy::too_many_lines,
    reason = "方案B事件驱动重连状态机（v1.4 实盘形态一体成型）：拆分需跨闭包传递 holder/evt_rx，拆分收益小于实盘回归风险，v1.6 SplitRS 拆模块时一并处理"
)]
pub fn spawn_member(
    member_idx: usize,
    front: String,
    flow_dir: PathBuf,
    dynlib: PathBuf,
    broker_id: String,
    user_id: String,
    password: String,
    instruments: Vec<String>,
    evt_tx: mpsc::SyncSender<MemberEvent>,
    evt_rx: mpsc::Receiver<MemberEvent>,
    tick_tx: mpsc::Sender<TickEvent>,
    stats: Arc<Mutex<Vec<MemberStats>>>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    // 行内微注释：thread::spawn(move || …) 的 move 把上面所有按值参数「搬」进闭包，
    // 闭包因此拥有它们，编译器才放心让它在另一个线程里活（Send 检查通过）。
    std::thread::spawn(move || {
        let tag = format!("[MD#{member_idx}]");
        tracing::info!(
            "{} 启动 front={} 合约数={}（方案B：事件驱动重连，无 join 阻塞）",
            tag,
            front,
            instruments.len()
        );
        // ════════════════ Rust 概念教学 ④：状态机的「变量面板」 ════════════════
// 下面一排 let 声明就是整个状态机的「仪表盘」，每个变量一个状态维度：
//   · backoff_ms：下次重建的等待毫秒数（1 秒起步，翻倍到 60 秒封顶）；
//   · 重建轮数 / 登录失败连击：计数器（u64）；
//   · 在线：当前是否已连上前置（bool）；
//   · 断线时刻: Option<Instant>：None=没断线；Some(时刻)=断线后用来算「超窗」——
//     Option 在这里又一次优雅地表达了「可能不存在的状态」；
//   · 单调状态: HashMap<String, (String, i64, Option<Instant>)>：每只合约一行
//     「(交易日, 当日总毫秒, 上次回拨告警时刻)」。元组当轻量记录用，
//     不用专门定义结构体——字段少、只在本文件用时，元组最省事；
//     Option<Instant> 里 None=从没告警过（首次回拨立即告警）。
// 这些变量全是【栈上的局部变量】，属于本线程私有——线程不共享它们，
// 也就不需要锁；要共享的（stats/stop）才用 Arc/Mutex/Atomic——「能私有不共享
// 就私有不共享」是 Rust 并发设计的第一原则（共用越少，死锁越少）。
// ═══════════════════════════════════════════════════════════════
        let mut holder = ApiHolder::new(dynlib, flow_dir);
        let mut backoff_ms = RECONNECT_BASE_MS;
        let mut 重建轮数: u64 = 0;
        let mut 登录失败连击: u64 = 0;
        let mut 在线 = false;
        let mut 断线时刻: Option<Instant> = None;
        // 红标3 单调性状态：合约 → (交易日, 当日总毫秒, 上次告警时刻)
        let mut 单调状态: HashMap<String, (String, i64, Option<Instant>)> = HashMap::new();

        if !holder.spawn_new(member_idx, &front, &evt_tx) {
            tracing::error!("{} 建 MdApi 失败，{}ms 后由守护定时器重试", tag, backoff_ms);
        }
        let mut 建立时刻 = Instant::now(); // 每次 spawn_new 后重置

        loop {
            if stop.load(Ordering::Relaxed) {
                holder.teardown();
                tracing::info!("{} 收到退出信号，线程退出", tag);
                break;
            }
            match evt_rx.recv_timeout(Duration::from_millis(1000)) {
                Ok(MemberEvent::Connected(idx)) => {
                    在线 = true;
                    断线时刻 = None;
                    登录失败连击 = 0;
                    {
                        let mut st = 锁或恢复(&stats);
                        st[idx].connected = true;
                        st[idx].logged_in = false;
                    }
                    tracing::info!("[MD#{}] 前置已连接", idx);
                    // ★方案B 核心：登录由 Connected 事件直接驱动。CTP 断线自动重连成功
                    //   ⇒ OnFrontConnected 必回调 ⇒ 这里必然重新登录+订阅 ——
                    //   "已连 16 已登录 0" 在结构上不可能再出现（D016 病灶已除）。
                    // ════════════════ Rust 概念教学 ⑤：if let 链与 Arc/Mutex 记账 ════════════════
// 下面的 if let Some(api) = holder.api.as_ref() && login_and_subscribe(…)
// 是「let 链」：先拆出 api 的引用（as_ref 只借不拿），再判断登录订阅是否成功，
// 两个条件都成立才进记账分支——把「有 api」和「登录成功」两个前提写在一行。
//
// 【Arc<Mutex<Vec<MemberStats>>> 怎么用】
//   · Arc（Atomic Reference Counting，原子引用计数）：像「多人共阅一份文件」——
//     clone 一次加一个读者，最后一人离开（计数归零）时文件才销毁；
//   · Mutex：轮到谁看谁拿锁（洗手间门锁）。两者组合 = 「多线程安全地共改一份数据」
//     的标准姿势；为什么不用全局变量？全局可变状态在 Rust 里是「最不推荐路线」，
//     Arc/Mutex 把「谁能改」显式写进类型，审查起来一目了然；
//   · 锁或恢复(&stats)：stats.rs 里那个「锁中毒也照常取数」的函数，返回
//     MutexGuard；guard 活着期间锁被持有，离开作用域自动释放——
//     所以记账块都用 { } 包起来，出括号立即还锁（锁作用域最小化）。
// ═══════════════════════════════════════════════════════════════
                    if let Some(api) = holder.api.as_ref()
                        && login_and_subscribe(
                            api,
                            &broker_id,
                            &user_id,
                            &password,
                            &instruments,
                            &tag,
                        )
                    {
                        let mut st = 锁或恢复(&stats);
                        st[idx].logged_in = true;
                        st[idx].ready_rounds += 1;
                        let rounds = st[idx].ready_rounds;
                        drop(st); // 行内微注释：显式提前释放锁——后面打日志不再占锁（significant_drop_tightening 的手动版）
                        tracing::error!("[MD#{}] 登录+订阅完成（第 {} 轮）", idx, rounds);
                        backoff_ms = RECONNECT_BASE_MS;
                    } else if holder.api.is_some() {
                        登录失败连击 += 1;
                        tracing::error!("[MD#{}] 登录请求失败（连续 {} 次）", idx, 登录失败连击);
                    }
                    if 登录失败连击 >= LOGIN_FAIL_REBUILD {
                        // 连续登录失败：teardown 重建，防死循环打柜台
                        重建轮数 += 1;
                        {
                            let mut st = 锁或恢复(&stats);
                            st[idx].rebuilds += 1;
                            st[idx].connected = false;
                            st[idx].logged_in = false;
                        }
                        tracing::error!(
                            "{} ⚙ 连续登录失败达 {} 次，主动重建（第 {} 轮）",
                            tag,
                            LOGIN_FAIL_REBUILD,
                            重建轮数
                        );
                        holder.teardown();
                        sleep_or_stop(Duration::from_millis(backoff_ms), &stop);
                        backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
                        if holder.spawn_new(member_idx, &front, &evt_tx) {
                            建立时刻 = Instant::now();
                        }
                        在线 = false;
                        断线时刻 = None;
                    }
                }
                Ok(MemberEvent::Login(idx, info)) => match info {
                    None => {
                        let mut st = 锁或恢复(&stats);
                        st[idx].logged_in = true;
                    }
                    Some((id, msg)) => {
                        登录失败连击 += 1;
                        {
                            let mut st = 锁或恢复(&stats);
                            st[idx].login_fail += 1;
                            st[idx].logged_in = false;
                        }
                        tracing::error!(
                            "[MD#{}] 登录失败 ErrorID={} {}（连续 {} 次）",
                            idx,
                            id,
                            msg,
                            登录失败连击
                        );
                        if 登录失败连击 >= LOGIN_FAIL_REBUILD {
                            重建轮数 += 1;
                            {
                                let mut st = 锁或恢复(&stats);
                                st[idx].rebuilds += 1;
                                st[idx].connected = false;
                            }
                            tracing::error!(
                                "{} ⚙ 连续登录失败达 {} 次，主动重建（第 {} 轮）",
                                tag,
                                LOGIN_FAIL_REBUILD,
                                重建轮数
                            );
                            holder.teardown();
                            sleep_or_stop(Duration::from_millis(backoff_ms), &stop);
                            backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
                            if holder.spawn_new(member_idx, &front, &evt_tx) {
                                建立时刻 = Instant::now();
                            }
                            在线 = false;
                            断线时刻 = None;
                        }
                    }
                },
                Ok(MemberEvent::Disconnected(idx, reason)) => {
                    在线 = false;
                    断线时刻 = Some(Instant::now());
                    let mut st = 锁或恢复(&stats);
                    st[idx].connected = false;
                    st[idx].logged_in = false;
                    st[idx].last_disconnect_reason = reason;
                    st[idx].last_disconnect_at = Some(Instant::now());
                    st[idx].disconnects += 1;
                    let 累计 = st[idx].disconnects;
                    drop(st);
                    tracing::error!(
                        "[MD#{}] 前置断开 reason={}（累计 {} 次；CTP 自动重连中，{} 秒未回连则主动重建）",
                        idx,
                        reason,
                        累计,
                        DISCONNECT_REBUILD_MS / 1000
                    );
                }
                Ok(MemberEvent::Tick {
                    member_idx,
                    instrument,
                    last_price,
                    volume,
                    turnover,
                    open_interest,
                    trading_day,
                    update_time,
                    update_millisec,
                }) => {
                    // ════════════════ Rust 概念教学 ⑥：struct 变体的解构与「转发即所有权交接」 ════════════════
// match 的这个分支直接把 Tick 信封【拆件】：member_idx、instrument……每个字段
// 变成独立局部变量——struct 变体也能像元组一样解构，这是模式匹配的又一红利。
// 拆出来的 instrument/trading_day/update_time 都是 String，随后被整个塞进新的
// TickEvent 发往聚合线程：没有 clone，所有权直接「过户」——旧信封拆了、
// 新信封装了原封不动的内容，零拷贝转发。这是 Rust 高效并发的日常：
// 数据在线程间「搬家」而不是「复印」。
//
// 【let _ = tick_tx.send(…)】行情转发失败（聚合线程已退）忽略——退出期
// 的最后一根 tick 无处安放也不必惊慌，打日志都不需要（进程在关灯）。
// ═══════════════════════════════════════════════════════════════
                    {
                        let mut st = 锁或恢复(&stats);
                        st[member_idx].ticks += 1;
                        st[member_idx].last_tick_at = Some(Instant::now());
                        // 红标3 观测型校验：回拨只计数+节流告警，tick 照常转发（D025）
                        if 校验tick单调性(
                            &mut 单调状态,
                            &instrument,
                            &trading_day,
                            &update_time,
                            update_millisec,
                            &tag,
                        ) {
                            st[member_idx].tick_out_of_order += 1;
                        }
                    }
                    let _ = tick_tx.send(TickEvent {
                        member_idx,
                        instrument,
                        last_price,
                        volume,
                        turnover,
                        open_interest,
                        trading_day,
                        update_time,
                        update_millisec,
                    });
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // ── 守护定时器（Timeout 分支）：三类异常全部收敛到 teardown→重建 ──
                    // ════════════════ Rust 概念教学 ⑦：recv_timeout 与守护定时器 ════════════════
// recv_timeout(1000ms)：最多等 1 秒。有信就处理（Ok 分支）；没信超时（Timeout 分支）
// 也不闲着——正好当「心跳」用，巡检三类异常；通道关闭（Disconnected）就退出。
// 一个 match 同时搞定「干活、心跳、收工」三种节奏，比「专职定时线程 + 信号量」
// 简单得多——这是事件驱动线程的常用骨架，值得背下来。
//
// 退避算法两行值得看：
//   · backoff_ms * 2：翻倍（1s→2s→4s→…）；
//   · .min(RECONNECT_MAX_MS)：封顶 60 秒。min 是 Ord 的方法，两数取小，
//     不会溢出不会越界——比手写 if 简洁；
//   · sleep_or_stop（见文件尾）：睡眠也要【可中断】——每 100ms 醒一次看
//     停止旗标，用户按 Ctrl+C 时不用等完整个退避就能退。
// ═══════════════════════════════════════════════════════════════
                    if !在线
                        && 断线时刻.is_none()
                        && 建立时刻.elapsed() >= Duration::from_millis(CONNECT_TIMEOUT_MS)
                    {
                        重建轮数 += 1;
                        {
                            let mut st = 锁或恢复(&stats);
                            st[member_idx].rebuilds += 1;
                        }
                        tracing::error!(
                            "{} ⚙ 连接 {} 秒未成功（CONNECT_TIMEOUT_MS={}），主动重建（第 {} 轮）",
                            tag,
                            CONNECT_TIMEOUT_MS / 1000,
                            CONNECT_TIMEOUT_MS / 1000,
                            重建轮数
                        );
                        holder.teardown();
                        sleep_or_stop(Duration::from_millis(backoff_ms), &stop);
                        backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
                        if holder.spawn_new(member_idx, &front, &evt_tx) {
                            建立时刻 = Instant::now();
                        }
                    } else if let Some(td) = 断线时刻
                        && td.elapsed() >= Duration::from_millis(DISCONNECT_REBUILD_MS)
                    {
                        重建轮数 += 1;
                        {
                            let mut st = 锁或恢复(&stats);
                            st[member_idx].rebuilds += 1;
                        }
                        tracing::error!(
                            "{} ⚙ 断线 {} 秒未回连（DISCONNECT_REBUILD_MS={}，CTP 自动重连未生效），主动重建（第 {} 轮）",
                            tag,
                            DISCONNECT_REBUILD_MS / 1000,
                            DISCONNECT_REBUILD_MS / 1000,
                            重建轮数
                        );
                        holder.teardown();
                        sleep_or_stop(Duration::from_millis(backoff_ms), &stop);
                        backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
                        if holder.spawn_new(member_idx, &front, &evt_tx) {
                            建立时刻 = Instant::now();
                        }
                        断线时刻 = None;
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    // 发送端全部丢弃才会走到这（member 自己持有 tx，正常不会发生）：保守退出
                    holder.teardown();
                    tracing::info!("{} 事件通道关闭，线程退出", tag);
                    break;
                }
            }
        }
    })
}

/// 红标3 tick 单调性校验（观测型，不干预数据流，D025）。
/// 比对交易所侧 (`TradingDay`, UpdateTime+UpdateMillisec)：
///   同交易日内时间倒退 / 交易日倒退 → 返回 true（调用方计数+节流告警）；
///   新交易日（更大）→ 正常轮转，重置基线不误报；
///   空时间戳 / 解析失败 → 跳过（观测组件绝不因自身问题干预数据流）。
fn 校验tick单调性(
    状态: &mut HashMap<String, (String, i64, Option<Instant>)>,
    合约: &str,
    交易日: &str,
    时间: &str,
    毫秒: i32,
    tag: &str,
) -> bool {
    if 交易日.is_empty() || 时间.is_empty() {
        return false; // 空时间戳跳过校验（不误报）
    }
    let Some(总ms) = 解析时分秒毫(时间, 毫秒) else {
        return false; // 时间串解析不了 → 跳过，不打日志刷屏
    };
    match 状态.get_mut(合约) {
        None => {
            状态.insert(合约.to_string(), (交易日.to_string(), 总ms, None));
            false
        }
        Some((旧日, 旧ms, 上次告警)) => {
            if 交易日 > 旧日.as_str() {
                // 新交易日正常轮转：重置基线，不误报回退
                *旧日 = 交易日.to_string();
                *旧ms = 总ms;
                *上次告警 = None;
                return false;
            }
            let 回拨 = 交易日 < 旧日.as_str() || 总ms < *旧ms;
            if 回拨 {
                let 需告警 = 上次告警.is_none_or(|t| t.elapsed() >= 回拨告警节流);
                if 需告警 {
                    tracing::error!(
                        "{} ⚠ 交易所时间回拨: {} 上一笔 {}@{}ms → 本笔 {}@{}ms（tick 照常采集转发，仅观测告警）",
                        tag,
                        合约,
                        旧日,
                        旧ms,
                        交易日,
                        总ms
                    );
                    *上次告警 = Some(Instant::now());
                }
                true
            } else {
                *旧ms = 总ms;
                false
            }
        }
    }
}

/// "HH:MM:SS" + 毫秒部分 → 当日总毫秒；解析失败返回 None。
fn 解析时分秒毫(时间: &str, 毫秒: i32) -> Option<i64> {
    let p: Vec<&str> = 时间.split(':').collect();
    if p.len() != 3 {
        return None;
    }
    let h: i64 = p[0].trim().parse().ok()?; // 行内微注释：parse().ok()? 双保险——解析失败转 None 直接返回，类型标注 i64 决定 parse 解析成什么
    let m: i64 = p[1].trim().parse().ok()?;
    let s: i64 = p[2].trim().parse().ok()?;
    if !(0..24).contains(&h) || !(0..60).contains(&m) || !(0..61).contains(&s) {
        return None;
    }
    Some(h * 3_600_000 + m * 60_000 + s * 1_000 + i64::from(毫秒.max(0)))
}

pub fn sleep_or_stop(dur: Duration, stop: &AtomicBool) {
    let 单步 = Duration::from_millis(100);
    let mut left = dur;
    while left > Duration::ZERO {
        if stop.load(Ordering::Relaxed) {
            return; // 行内微注释：睡眠中途也要响应停止旗标——可中断睡眠是优雅退出的前提
        }
        let 本片段 = if left < 单步 { left } else { 单步 };
        std::thread::sleep(本片段);
        left = left.saturating_sub(本片段); // 行内微注释：saturating_sub「减到 0 为止」不借位成负数（u64 没有负数，普通减法下溢会 panic）
    }
}

// ★v1.5 方案B：原 spawn_listener 已删除——stats 记账 + tick 转发职责并入 member 线程
//   （事件通道私有后无需中转线程，见决策日志 D024 实现要点 3）。

#[cfg(test)]
mod 测试 {
    use super::*;

    // 行内微注释：测试辅助函数（不带 #[test]）：给每个用例造一张空状态表，
    // 保持用例之间互不依赖（测试要能乱序跑、单跑，不能靠上一个用例留下的状态）。
    fn 校验状态() -> HashMap<String, (String, i64, Option<Instant>)> {
        HashMap::new()
    }

    #[test]
    fn 首个tick不报回拨() {
        let mut st = 校验状态();
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:00", 0, "[T]"
        ));
    }

    #[test]
    fn 空时间戳跳过校验() {
        let mut st = 校验状态();
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "", 0, "[T]"
        ));
        assert!(!校验tick单调性(
            &mut st, "IF2401", "", "09:30:00", 0, "[T]"
        ));
        // 空时间戳不进入状态表（下一笔正常起算）
        assert!(st.is_empty());
    }

    #[test]
    fn 正常递增不报回拨() {
        let mut st = 校验状态();
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:00", 0, "[T]"
        ));
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:01", 0, "[T]"
        ));
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:01", 500, "[T]"
        ));
    }

    #[test]
    fn 秒级回拨被检测() {
        let mut st = 校验状态();
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:05", 0, "[T]"
        ));
        assert!(校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:04", 0, "[T]"
        ));
    }

    #[test]
    fn 毫秒级回拨被检测() {
        let mut st = 校验状态();
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:05", 500, "[T]"
        ));
        assert!(校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:05", 400, "[T]"
        ));
    }

    #[test]
    fn 新交易日不误报回退() {
        let mut st = 校验状态();
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "15:00:00", 0, "[T]"
        ));
        // 次日 09:00 "时分秒"更小但交易日更大 → 正常轮转，不误报
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261125", "09:00:00", 0, "[T]"
        ));
    }

    #[test]
    fn 回退到更早交易日被检测() {
        let mut st = 校验状态();
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261125", "09:00:00", 0, "[T]"
        ));
        assert!(校验tick单调性(
            &mut st, "IF2401", "20261124", "10:00:00", 0, "[T]"
        ));
    }

    #[test]
    fn 不同合约互不干扰() {
        let mut st = 校验状态();
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:05", 0, "[T]"
        ));
        // 另一合约从更早时间起算，不影响 IF2401 的基线
        assert!(!校验tick单调性(
            &mut st, "rb2501", "20261124", "09:29:00", 0, "[T]"
        ));
        assert!(!校验tick单调性(
            &mut st, "IF2401", "20261124", "09:30:06", 0, "[T]"
        ));
    }
}

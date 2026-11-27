# Rust 概念速查表（本工程用到的全部概念一页通）

> 用法：按拼音/分组排布；「在哪里」列给出最典型的出现位置，用
> `rg "关键词" src/` 可直接定位。生活类比只帮你入门，别停留在类比上。

## 1. 类型与结构

| 概念 | 一句话解释 | 在本工程哪里用到 |
|---|---|---|
| struct | 把几个字段捆成一个类型 | `MemberStats`（stats.rs）、`MinuteBar`（aggregate.rs）、`Contract`（contracts.rs） |
| enum 带数据变体 | 一个类型装多种消息，每种携带不同数据 | `MemberEvent::Connected/Tick{…}`（events.rs）、`开关::{布,数,字}`（config.rs） |
| 元组 (A, B) | 临时打包两个值，不必定义结构体 | `Option<(i32, String)>`（events.rs）、`HashMap<(String,i64), MinuteBar>`（merge.rs） |
| 基本类型 u64/i32/f64/usize | u 无符号、i 有符号、f 浮点、usize 是"下标专用无符号" | 计数器 u64、CTP 原因码 i32、价格 f64、member_idx usize |
| String / &str | String 拥有可变的堆字符串；&str 是只借用的切片 | 结构体字段用 String，函数参数用 &str |
| Vec&lt;T&gt; | 可增长数组 | `Vec<Contract>`、`Vec<MinuteBar>`、`shards: Vec<Vec<String>>`（contracts.rs） |
| HashMap&lt;K, V&gt; | 按键取物的储物柜 | 桶仓库（aggregate.rs）、去重表（merge.rs）、单调状态（session.rs） |
| Option&lt;T&gt; | "可能有值"的类型，None=没有；根治空指针 | 到处都是：`Option<MdApi>`、`Option<&CTP结构体>`、`Option<Instant>` |
| Result&lt;T, E&gt; | "可能失败"且带原因；Ok/Err 两个变体 | `load_contracts_csv`、`合并一日`、所有 std::fs 调用 |

## 2. 所有权与借用（Rust 灵魂）

| 概念 | 一句话解释 | 在本工程哪里用到 |
|---|---|---|
| 所有权/移动(move) | 值默认"搬家"不复印；旧变量从此不可用 | `send(bar)` 塞进管道（aggregate.rs）、闭包 move 进线程（main.rs） |
| 共享借用 `&T` | 只读借出，可同时多人借 | `&str` 参数、`&TickEvent`、`tick_rx: &Receiver` |
| 可变借用 `&mut T` | 独占的写权限，同一时刻只能一个 | `&mut HashMap`、`get_mut` 更新桶（aggregate.rs）、`values_mut`（merge.rs） |
| clone / clone_from | 显式深拷贝；clone_from 复用缓冲更省 | `t.instrument.clone()`、`b.trading_day.clone_from(…)`（aggregate.rs） |
| 生命周期 'a | 引用的有效期标注；多数被省略规则自动推断 | `MutexGuard<'_, T>`（stats.rs）、`<'de, D>`（config.rs 开关宽容） |
| 'static | "活到程序结束" | `WORKSPACE_ROOT() -> &'static str`（config.rs）、闭包约束（main.rs） |
| drop | 值离开作用域自动清理；drop(x) 提前触发 | `drop(tick_tx)` 通知通道关闭（main.rs）、`drop(st)` 提前还锁（session.rs） |
| take() | 把 Option 里的值取出并留 None | `self.api.take()`（session.rs teardown）、`当前.take()`（writer.rs） |

## 3. 错误处理（宪法：零 unwrap/expect）

| 概念 | 一句话解释 | 在本工程哪里用到 |
|---|---|---|
| `?` 运算符 | Err 就立刻上抛、Ok 就拆包继续 | `load_contracts_csv`、`合并一日` 全流水线 |
| map_err | 换掉错误类型（为 `?` 铺路） | `read_to_string(...).map_err(\|e\| format!(…))`（contracts.rs） |
| unwrap_or / unwrap_or_else | 失败给默认值/默认闭包 | 读环境变量（config.rs）、`u64::try_from(...).unwrap_or(0)`（spi.rs） |
| ok / ok_or_else | Option 与 Result 互转 | `判定重演` 里 `parse().ok()?`；arrow 缺列 `ok_or_else`（merge.rs） |
| Box&lt;dyn Error&gt; | 万能错误箱子（trait object），省去逐类型转换 | `合并一日`/`读取成品`/`写日线parquet` 返回类型（merge.rs） |
| std::process::exit(1) | 启动期致命错误：打日志+退出码，不 panic | main.rs 载配置失败、跑合并退出码 |
| 锁中毒 PoisonError | 持锁线程 panic 后锁标记中毒；into_inner 照常取数 | `锁或恢复`（stats.rs，全工程复用） |

## 4. 并发（线程/管道/共享）

| 概念 | 一句话解释 | 在本工程哪里用到 |
|---|---|---|
| std::thread::spawn + move | 开线程；move 把数据搬进闭包（线程要求 'static） | main.rs 装配 18 个线程、session.rs spawn_member |
| JoinHandle / join | 线程收据；join 等收工 | main.rs 收尾三连 join |
| mpsc::channel | 无界管道（多生产者单消费者） | tick 通道：member 线程 → 聚合线程 |
| mpsc::sync_channel(n) | 有界管道，满则阻塞（背压），不丢数据 | 事件通道 8192、bar 通道 bar队列容量 |
| recv / recv_timeout | 收货；timeout 版兼顾心跳巡检 | 聚合 200ms、member 1000ms |
| 通道关闭信号 | 所有发送端 drop 后 recv 返回 Err | writer 循环退出、聚合线程退出（writer.rs/aggregate.rs） |
| Arc | 原子引用计数：多线程共有一份数据的所有权 | `Arc<Mutex<Vec<MemberStats>>>`、`Arc<AtomicBool>` |
| Mutex / MutexGuard | 互斥锁/持有期间守卫，出作用域自动还锁 | member 记账（session.rs）、主线程巡检（main.rs） |
| AtomicBool + Ordering::Relaxed | 原子布尔旗标；Relaxed 适合纯旗标/计数 | `stop` 停止旗标，全员能读的"灯绳" |
| Send / Sync | 类型能否跨线程搬运/共享的编译期证明 | `ctrlc_handler<F: FnMut() + Send + 'static>`（main.rs） |

## 5. FFI 与 CTP（外部函数接口）

| 概念 | 一句话解释 | 在本工程哪里用到 |
|---|---|---|
| CTP 绑定 crate (ctp2rs) | 把 C 头文件翻译成 Rust 结构体与方法 | `MdApi`、`MdSpi`、`CThostFtdc…Field` |
| trait 回调（SPI 模式） | 实现 MdSpi 接口，CTP 在事件发生时反调 | `MemberSpi`（spi.rs） |
| Box::into_raw / 裸指针 | 把 Rust 对象拆成指针交给 C 库保管 | `Box::into_raw(spi) as *mut dyn MdSpi`（session.rs） |
| unsafe 的封装收口 | 裸指针的 unsafe 藏在绑定库内部，业务层保持安全外观 | register_spi/register_front（session.rs 教学②） |
| C 数组 → String | 定长 C 字符数组经 WrapToString 转 String | `q.InstrumentID.to_string()`（spi.rs） |
| dyn trait object | 运行时动态分发的接口对象 | `*mut dyn MdSpi`、`Box<dyn Error>` |
| downcast_ref / as_any | 从泛化 trait object 向下还原成具体类型 | arrow 列还原 `Float64Array` 等（merge.rs 教学⑤） |
| CString/指针的借用期 | FFI 借用必须活过整个调用，先绑定 let 防悬垂 | `dynlib_s/flow_s` 先转 owned 再传（session.rs） |

## 6. serde 序列化（jsonl/toml）

| 概念 | 一句话解释 | 在本工程哪里用到 |
|---|---|---|
| derive(Serialize) | 结构体 → JSON（编译期生成代码） | `MinuteBar` 写 jsonl（writer.rs） |
| derive(Deserialize) | JSON/toml → 结构体 | 读分片（merge.rs）、读配置（config.rs） |
| `#[serde(rename)]` | 字段的"外号"与学名不同时对齐 | `bar_time_ms` ↔ `"bar_time"`（aggregate.rs） |
| `#[serde(alias)]` | 反序列化时兼容旧外号 | 兼容 20261127 前的老分片（aggregate.rs） |
| `#[serde(default)]` | 缺字段/缺键时给默认值不报错 | `volume_1m`（字段级）、配置结构体（容器级，config.rs） |
| `#[serde(untagged)]` | 枚举不带标签，挨个变体试装 | `开关::{布,数,字}` 宽容布尔（config.rs） |
| `deserialize_with` | 自定义解析函数 | `开关宽容`（config.rs） |
| serde_json::to_string/from_str | 单行 JSON 互转 | writer 写行 / merge 读行 |

## 7. 迭代器与闭包

| 概念 | 一句话解释 | 在本工程哪里用到 |
|---|---|---|
| 闭包 `\|x\| …` | 匿名函数，可捕获环境；当局部小函数用 | `打开`（writer.rs）、`取浮点/取整数`（merge.rs）、`取数`（config.rs） |
| Fn/FnMut/FnOnce | 闭包三兄弟：只读/可变/消费捕获 | `ctrlc_handler<F: FnMut()>`（main.rs） |
| 迭代器链 map/filter/collect | 流水线式加工，collect 按目标类型收货 | 统计汇总（main.rs）、列转置（merge.rs） |
| filter_map | 过滤+变换二合一（None 被丢掉） | 过期桶收集（aggregate.rs） |
| entry().or_default() | 查坑位+空坑给默认值，分组/计数惯用法 | 计数（aggregate.rs）、分组（merge.rs/contracts.rs） |
| Entry::Occupied/Vacant | 坑位有主/空坑两种状态可 match | 同日合并去重（merge.rs） |
| enumerate / any / windows(2) | 带序号遍历/存在性短路/滑动窗口 | 巡检统计（main.rs）、补缺口（merge.rs） |
| sort_by_key / sort_by+then | 按 key 排序 / 多级比较器排序 | bar 时间排序、成品全局排序（merge.rs） |
| 涡轮鱼 `::<Type>` | 就地指定泛型类型 | `sync_channel::<MinuteBar>`、`from_str::<MinuteBar>` |
| format! / writeln! 宏 | 格式化字符串/写一行；宏名带感叹号 | 全工程日志、writer 写行 |
| rem_euclid / saturating_sub | 非负余数 / 饱和减法（不下溢） | floor_minute（aggregate.rs）、增量计算（merge.rs） |

## 8. 属性、宏与工程守卫（本工程特色）

| 概念 | 一句话解释 | 在本工程哪里用到 |
|---|---|---|
| `#[derive(…)]` | 让编译器自动实现能力清单（Debug/Clone/…） | 全部结构体 |
| `#[cfg(test)]` + `#[test]` | 测试代码只在 cargo test 时编译 | 各文件 `mod 测试`（共 32 个测试） |
| `#[allow(lint, reason)]` | 豁免某条编译器检查，必须写理由 | 中文标识符（config.rs）、indexing_slicing（contracts/session/merge） |
| `#[expect(lint, reason)]` | 豁免且要求提醒必然出现（带保质期） | 常量钉死测试（config.rs）、测试 panic（merge.rs）、too_many_lines |
| `#![allow(…)]` 文件级 | 感叹号=作用整个 crate/文件 | contracts.rs/session.rs/merge.rs 文件头 |
| 中文标识符 | Rust 允许 Unicode 命名，本工程全局使用并豁免风格检查 | `合并一日`、`锁或恢复`、`在交易时段内`…… |
| tracing::info!/error! | 结构化日志宏；RUST_LOG 控级别 | 全工程（main.rs 初始化 subscriber） |
| let-else | 模式不匹配就发散（return/continue），卫语句神器 | 归档护栏（merge.rs）、`shards.get(idx)`（main.rs） |
| let 链 `if let … && …` | 拆包加条件一行写完（2024 纪元） | spi.rs 错误上报、main.rs 断流判定 |
| 块表达式圈作用域 | 用 `{}` 提前结束借用/锁的生命期 | main.rs 巡检统计块 |
| OnceLock | 只能写一次的惰性全局单元 | `WORKSPACE_ROOT()`（config.rs） |
| 背压（backpressure） | 有界管道满则阻塞生产者，内存有顶不丢数据 | bar 通道（aggregate.rs/writer.rs/main.rs） |
| 原子写 (.tmp→rename) | 先写临时文件再原子改名，永无半截文件 | Parquet 成品与 zst 归档（merge.rs） |
| flush 与 fsync | 冲到内核缓冲 vs 真落物理盘；崩溃丢失上限=间隔秒 | writer.rs（分片落盘秒=60） |

## 9. 十秒记忆卡（考前三行）

1. 所有权默认移动、借用要写 & / &mut、可变借用独占——"把钥匙交出去，原主别再用"。
2. Option 管"有没有"、Result 管"成没成"、`?` 是快速失败通道、unwrap 是违禁品。
3. 线程间要么"搬"（move/管道），要么"借不出去就共享"（Arc/Mutex/AtomicBool）。

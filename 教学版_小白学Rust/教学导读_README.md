# 教学导读（写给 Rust 零基础新人）

> 欢迎来到「分钟K线教学版」。这是一份可以【编译、测试、逐行读注释】的真实工程：
> 它从期货交易所的行情流（tick）出发，一路加工成分钟K线，落盘成 jsonl 分片，
> 最后合并成 13 列 Parquet 成品。每一行代码旁边都写着"为什么这么写"。
> 你不需要懂量化、也不需要懂期货，只需要会一点编程常识即可开始。

---

## 一、这个项目到底在干什么（一分钟版）

CTP 是国内期货公司的行情网关。本项目连上行情前置，订阅全市场合约，把
每一笔 tick 聚合成 1 分钟K线（开/高/低/收/量/额），追加写入当天的
`bars_YYYYMMDD.jsonl` 分片文件，并随时可用子命令把分片合并成
`bars_YYYYMMDD.parquet` 成品给下游（DuckDB 等）读。

整条数据流水线（请把它背下来，每个源码文件的导读都会引用它）：

```
CTP连接 → tick → 分钟K线 → jsonl分片 → Parquet合并
  spi.rs / session.rs   aggregate.rs    writer.rs    merge.rs
  （config.rs 供电，stats.rs 仪表，main.rs 总装）
```

程序是多线程的：16 条行情连接（member）各占一个线程，再加 1 个聚合线程、
1 个落盘线程，主线程负责巡检和 Ctrl+C。线程之间用 mpsc 管道传数据、
用 `Arc<AtomicBool>` 广播"该下班了"。

## 二、环境怎么装（一句话版 + 三步实操）

一句话：安装 Rust 工具链（rustup + nightly）、libclang（给 CTP 绑定生成
代码用）、国内 crates 镜像——本仓库的 `tools_工具箱/一键安装Rust工具链.sh`
与 `ctp-workspace` 工程已配好这一切，直接照抄即可。

实操三步（沙盒内验证过）：

```bash
# 1) 每开一个新终端先加载环境（rustc/cargo/libclang 路径全在里面）
source /home/z/my-project/scripts/env_rust.sh

# 2) 编译本教学版
cd /home/z/my-project/教学版_小白学Rust/分钟K线教学版
cargo build

# 3) 跑单元测试（应看到 32 个测试全部通过）
cargo test
```

注意：编译不需要行情动态库（.so）、不需要联网账号——教学版只练"读代码
与写代码"，不实盘运行。跑实盘是主工程（`ctp-workspace/行情数据采集/`）的事。

## 三、建议学习路线（按顺序，总时长约一周）

| 顺序 | 文件 | 内容一句话 | 预计时长 | 前置知识 |
|---|---|---|---|---|
| 0 | `Cargo.toml` | 工程身份证与购物清单 | 10 分钟 | 无 |
| 1 | `src/stats.rs` | 结构体、derive、Mutex 锁中毒 | 15 分钟 | 无（最小文件热身） |
| 2 | `src/events.rs` | 枚举与带数据的变体、FFI 类型净化 | 20 分钟 | stats.rs |
| 3 | `src/contracts.rs` | Result 错误传播、HashMap entry 惯用法 | 20 分钟 | events.rs |
| 4 | `src/spi.rs` | trait 回调、Option<&T> 治空指针 | 20 分钟 | events.rs |
| 5 | `src/writer.rs` | BufWriter/flush/fsync、serde_json 序列化 | 30 分钟 | stats/events |
| 6 | `src/aggregate.rs` | 全工程心脏：HashMap 桶、管道背压、单测 | 60 分钟 | 上前五个 |
| 7 | `src/config.rs` | 中文标识符豁免、OnceLock、serde 配置解析 | 45 分钟 | aggregate |
| 8 | `src/session.rs` | FFI 裸指针、状态机、Arc/Mutex 记账 | 60 分钟 | spi+aggregate |
| 9 | `src/merge.rs` | Box<dyn Error>、Entry 去重、Parquet 列式读写 | 75 分钟 | aggregate+writer |
| 10 | `src/main.rs` | 总装：mod 树、线程地图、泛型与闭包三兄弟 | 60 分钟 | 全部 |
| 附 | `Rust概念速查表.md` | 考前突击/忘了就查 | 随时 | — |

学习法建议：
1. 先读文件头【教学导读】块，再顺着读概念插页（`// ════ Rust 概念教学 ════`）；
2. 遇到看不懂的语法，先查速查表，再回来继续；
3. 读代码时用 `rg "概念教学" src/aggregate.rs` 这类命令快速定位所有插页。

## 四、动手练习题（5 道，由易到难）

1. 【给 MinuteBar 加一个字段】（aggregate.rs + merge.rs）
   在 `MinuteBar` 结构体加一个 `pub(crate) amount_1m: f64` 之类的字段是禁的
   （教学版不改逻辑），所以请在【自己的练习副本】里做：加字段后观察编译器
   报错清单——它会告诉你所有需要补齐这个字段的地方（这是 Rust 的"改结构体
   全网通缉"体验）。思考：为什么加字段后 32 个测试还全绿？

2. 【写一个新单测】（aggregate.rs）
   在 `#[cfg(test)] mod 测试` 里新增 `#[test] fn 重演判定_容忍度为零()`：
   验证 `判定重演("21:00:00", 2100, 0)` 的返回值，并解释为什么差 0 分钟时
   返回 `Some(false)`。跑 `cargo test 重演判定` 只跑这一个测试。

3. 【在 判定重演 加一个边界用例】（aggregate.rs）
   思考并测试：`判定重演("24:00:00", 0, 2)` 应该返回什么？（提示：小时
   越界走哪个分支？）再测 `判定重演("21:01:30", 2103, 2)`（差 90 秒，
   环形差按分钟取整后是 1 还是 2？）。把你预测的答案写成断言，跑测试验证。

4. 【给 writer.rs 换一种分片命名】（writer.rs，练习副本）
   把分片文件名从 `bars_{date}.jsonl` 改成 `k_{date}.jsonl`，然后回答：
   merge.rs 读分片的地方要不要同步改？（答：要——搜 `bars_` 会命中
   `合并一日` 与 `归档非今日分片`。体会"两个模块共享一个格式约定"的耦合。）

5. 【补齐 锁或恢复 的姊妹函数】（stats.rs，练习副本）
   写一个 `锁或丢弃<T>(&Mutex<T>) -> Option<MutexGuard<T>>`：锁中毒时返回
   None 而不是硬拿数据。想清楚：聚合统计场景该用哪个？（参考答案：本工程
   用"中毒也取回"因为统计可丢、采集不能停；但对"宁可不要不可脏数据"的
   场景，返回 None 更安全。）

做完 5 道题，你就可以挑战真练习了：把教学版某一处注释删掉、只看代码，
自己把"为什么"讲出来——讲得出来，才算学会。

## 五、常见新手疑问（FAQ）

- 为什么代码里能写中文函数名（`合并一日`、`锁或恢复`）？
  Rust 允许 Unicode 标识符；本工程在 config.rs 顶部用 `#![allow(nonstandard_style, …)]`
  豁免了风格检查。这是团队约定，见 config.rs 教学①。
- 为什么到处都是 `unwrap_or_else` 而很少 `unwrap`？
  本工程"宪法"禁 unwrap/expect：任何 panic 都可能拖垮采集。unwrap_or_else
  是"失败给默认"的容错写法，见 stats.rs 教学②。
- 编译报 `empty identifier segment`？
  Cargo 版本号只认 ASCII——本教学版版本号因此写作 `1.0.0-jiaoxue`（拼音），
  中文后缀 `1.0.0-教学` 无法编译，这是个真实的小坑。
- 注释里的 `// ════` 框线是什么？
  教学插页的视觉分隔，纯属注释，编译器完全无视。

## 六、下一步

读完教学版后，回到主工程 `ctp-workspace/行情数据采集/`：
1. 对比阅读：教学版删除注释后与主工程逐字节一致（仅 Cargo.toml 版本号不同）；
2. 跑通实盘：按主工程《交付说明_上机步骤.md》接线（需要 .so、账号、交易日）；
3. 参与开发：从"改注释、加测试"开始，再到 `检查危险代码.rs`、`deny.toml`
   里了解本工程的守卫体系。

祝学习顺利。

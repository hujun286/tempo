# 交接报告 · CTP 分钟K线落盘项目（第3棒 · 2026-11-25 · 本轮"全做"完成版）

> **给谁看**：接手本棒工作的下一个智能体 / 开发者，或回到本窗口的第3棒自己（沙盒回收后上下文全失，靠本文+决策日志恢复）。
> **读法**：先读 §3（当前状态）→ §4（本轮改造明细）→ §5（剩余待办）→ §7（红线）。逐决策推理链看 `决策日志_推理链.md`（D000-D032）。
> **最后更新**：2026-11-25 · D032 · 高优四项全清 + 大改造清单全落地 + ctp2rs 升 0.1.11-alpha1 + 拆模块完成，沙盒 20/20 全绿。
> **工作模式（用户铁律）**：任何选择点停下来问用户；一切结论以回传输出/源码/文件为准，不猜测；每个决策记入决策日志（五要素：决策/事实依据/推理链/备选弃用原因/验证方式）；**每次汇报工作时重新打时间戳工作包 + git commit**。

---

## §1 项目一句话（继承）

连期货公司 CTP 行情前置（16 条链路），收 tick 聚合成分钟 K 线，追加分片落盘 jsonl（崩溃最多丢 60 秒），合并成 13 列 Parquet 供回测/分析。改造后主力代码 `src/bin/分钟k线落盘_v1_4/`（目录版 9 模块，bin 名不变），v1.1 完整版是冻结退路。

## §2 沙盒环境（第3棒的工作机）

| 项 | 事实 |
|---|---|
| 工具链 | rustc/cargo **1.102.0-nightly (2026-11-24)** + clippy + rustfmt；`/home/z/my-project/.rustup`、`.cargo`（持久） |
| 环境入口 | `source /home/z/my-project/scripts/env_rust.sh` |
| 镜像 | **cargo 依赖改走官方 sparse index（index.crates.io）**——rsproxy 对 cargo 客户端实测抽风（30s 0 字节），官方 index 直连反而快（D030 备注）；rustup 组件仍走 rsproxy |
| 编译验证 | v1_4 bin 口径：build ✅ clippy 0 ✅ test **20/20** ✅ |
| Cranelift | `rustup component add rustc-codegen-cranelift-preview` 已装，`RUSTFLAGS="-Zcodegen-backend=cranelift" cargo build` 实测可用 |
| 中文 | LC_CTYPE=C.UTF-8 正常；git `core.quotepath false` |
| git | 沙盒主仓 `/home/z/my-project`（白名单跟踪）+ eng 内独立仓（源码存档点） |

## §3 当前状态（2026-11-25 晚，本轮全部完成）

**用户机器（生产侧）**：
- v1.4 旧版进程照常跑（debug 构建）——本轮所有改造**尚未部署到用户机器**，沙盒全绿后由用户择时切换。
- 数据：`分钟分片/` 两个 jsonl 共 577M（11-24 181M + 11-25 397M）；`分钟K线/` 13M 的 11-25 成品（13 列新格式，github tempo 仓库样本即它）。

**沙盒（工作侧）——本轮成果全清单**：
1. ★**环境重建**：沙盒曾被回收重置，工具链/工作区全部重建（D033），基线复现（12 条 warning→复现→清零）。
2. ★**清 warning 补丁已应用**（9 块 57 行，git apply 干净通过）+ **全文 cargo fmt**（D022）。
3. ★**ctp2rs 0.1.9 → 0.1.11-alpha1**：v1_4 **零改动**编译通过；升级关键收益 = Drop 悬垂指针修复（D023）。
4. ★**🔴1 方案 B 事件驱动重连完成**：`spawn_member` 重写为状态机，`api.join()` 从代码里消失，Connected 事件直接驱动登录；三重守护（15s 连接超时 / 60s 断线重建 / 3 次登录失败重建），全部指数退避（D024）。
5. ★**🔴3 tick 单调性校验完成**：纯观测型（记数+节流告警，不动数据流），状态行新增"交易所时间回拨 / 主动重建"两个指标（D025）。
6. ★**🔴2 边界单测补齐**：重复时间戳 / 隔2分钟 / 跨大间隔 361 分钟全补齐且累计单调——合并补齐语义钉死（D026）。
7. ★**chrono 评估完成**：保留但裁剪 features（default-features=false）；bar 对齐保留本机墙钟的理由入档（盘前重演污染），trading_day 字段本就是交易所时间（D027）。
8. ★**jsonl 归档压缩落地**：合并成功后自动把已停写的 jsonl 压成 `.zst`（三道护栏），合并端支持直读 .zst；一天 ~600MB → 预期 ~30MB，磁盘不再线性增长（D028）。
9. ★**SplitRS 拆模块完成**：2234 行单文件 → `main+9 模块`目录版，行为零变化（build/clippy/test 与拆分前逐位一致），测试按语义分发到各模块（D029）。
10. ★**Workspace 化**：`[workspace.dependencies]` 统一 15 个依赖 + `[workspace.lints.clippy]` 统一门禁（D030）。
11. ★**justfile 17 配方**：build-release/Cranelift/ci 一键门禁/合并/关卡/冷却/deny 全托管（D030）。
12. ★**rust-script 替换 .sh**：打包工作快照.rs 落地；env_rust.sh 保留 .sh（source 语义必需，文档说明）（D030）。
13. ★**7 天依赖冷却脚本**：检查依赖冷却.rs（读 Cargo.lock → crates.io API 查发布时间 → <7 天点名+退出码 1）（D030）。
14. ★**cargo-deny CI**：.github/workflows/ci.yml（门禁 job + 依赖治理 job），本地等价 `just ci && just 关卡 && just deny && just 冷却`（D030）。
15. ★**危险代码检查器**（替代丢失的 .py）：检查危险代码.rs **零依赖版**——A unwrap/B expect/C panic 家族/D unsafe/E 打印宏/F as 强转/G static mut/H allow/I 字面量索引/J 浮点==，10 类检测 + 测试区豁免 + --strict 门禁模式 + --json；**基线：生产区 10 处审计项（0 unwrap/0 expect/0 panic/0 unsafe/0 打印宏），测试区 28 处**（D031 复验见下）。
16. ★**parquet schema-first 示例**：示例_下游按列名读取.rs——先读 schema 列名再动态取列，11/13 列自动判定（D031）。
17. ★**release 直跑约定**：`just build-release` → `./target/release/分钟k线落盘_v1_4` 直跑（run/合并 配方）；实盘 debug→release 切换前先 SimNow/收盘验证（D032）。

## §4 本轮关键设计决策（详见决策日志对应条目）

| # | 主题 | 一句话结论 |
|---|---|---|
| D024 | 🔴1 方案 B | 登录由 Connected 事件驱动；join() 删除；三重守护定时器兜底"CTP 自动重连不可全信" |
| D025 | 🔴3 单调性 | 观测型：回拨只记数+节流告警，不干预数据流（最坏情况多几条日志，零数据风险） |
| D027 | chrono | bar 对齐保留本机墙钟（夜盘 TradingDay 与自然日错位 + 盘前重演污染两大理由）；features 裁剪瘦身 |
| D028 | jsonl 归档 | 写入路径零改动；合并后自动压缩已停写分片；写入直接写 zstd 流被明确弃用（fsync 语义破坏） |
| D029 | 拆模块 | 按作者留的分区注释切割=风险最小切法；编译+20 测试全绿=行为零变化的最强验证 |
| D032 | 运行方式 | release 二进制直跑；debug→release 切换前必须 SimNow/收盘验证一轮 |

## §5 剩余待办（按优先级）

**0. （用户侧）本轮成果上机**：
   1) 取工作包/交付包，对照 §6 文件地图拷贝 `src/bin/分钟k线落盘_v1_4/`（目录版）、`Cargo.toml`、`justfile`、`scripts/`、`.github/`；
   2) `Cargo.toml` 的 ctp2rs path 改回用户 vendor 路径（沙盒值见 §6 注）；
   3) 跑 `just ci && just 关卡`（用户机器复验）；
   4) SimNow 或收盘时段试跑新版采集（🔴1 方案 B 首次实盘验证：人为拔网线/断代理，观察"已连/已登录"恢复）；
   5) 验证通过后切换实盘进程（建议 release：`just build-release && just run`）。
   6) 首次"合并 昨日"观察归档 .zst 生成与原件删除（归档开关默认开，不想要就 配置_分钟K线.toml 加 `归档压缩 = 0`）。

**1. 🔴1 实盘验证**（方案 B 代码已完成）：人为断链压测一次，确认"断线→自动重连→自动重登录"全链路；观察守护定时器是否误触发（阈值 15s/60s 在单测钉死，如需调整改 config.rs 常量）。

**2. 巡检增强（低优，未做）**：fd 上限检查、队列水位日志、收盘自动合并——原中低优清单里本轮未动的项，登记不展开。

**3. 其他 bin 的健康度**：v2/v1_3/完整版等旧 bin 在沙盒编不过（84 errors，历史遗留）。建议后续专项：要么归档删除要么修到能编，避免永远拖着。

## §6 文件地图（本轮后）

| 想要 | 位置 |
|---|---|
| 本报告 / 决策推理链 | `download/交接报告_CTP分钟K线_第3棒.md` / `download/决策日志_推理链.md`（D000-D032） |
| ★拆模块后的源码（交付核心） | `eng/src/bin/分钟k线落盘_v1_4/`（main+config/contracts/events/spi/session/stats/aggregate/writer/merge，9 模块） |
| ★Workspace 化 Cargo.toml | `eng/Cargo.toml`（⚠ ctp2rs path 是**沙盒适配值** `/home/z/my-project/ctp2rs/ctp-dyn`；用户机器改回 `/home/xx/projects/ctp-workspace/vendor/ctp2rs-src/ctp2rs-ctp2rs-0.1.9/ctp-dyn` 或指向新版解包目录） |
| ★justfile | `eng/justfile`（17 配方） |
| ★CI workflow | `eng/.github/workflows/ci.yml` |
| ★危险代码检查器 | `scripts/检查危险代码.rs`（零依赖，rust-script 或 rustc 皆可跑） |
| ★7 天依赖冷却 | `eng/scripts/检查依赖冷却.rs` |
| ★parquet 下游读取示例 | `eng/scripts/示例_下游按列名读取.rs` |
| parquet 检查脚本（上棒） | `scripts/检查_parquet.rs` |
| 打包快照（rust-script 版） | `scripts/打包工作快照.rs`（.sh 版已废弃保留对照） |
| 清 warning 补丁（已应用，留档） | `download/v1_4_清warning补丁_20261125.patch` |
| eng 源码仓 | `eng/.git`（每步一个 commit，可回放拆分/方案 B 全过程） |
| 用户源码原始包 | `upload/ctp_src_20261125.tar.gz`（只读） |
| ctp2rs 双版本 | `ctp2rs/`（master=0.1.11-alpha1）+ `ctp2rs-0.1.9/`（worktree，与 vendor 一致） |

## §7 红线（第 3 棒修订版——"照抄区不碰"已按用户指示解除，新增防线如下）

1. 不动 Cargo.lock 里已解析的 chrono 版本（现为 0.4.45）；Cargo.toml 已裁剪 features，**别把 default-features 加回去**。
2. 不引入 tokio/async 到 分钟k线落盘 bin（v1_4 是纯 std 线程架构；tokio 只允许其他 bin 用）。
3. 不改 CTP 回调签名 / SPI 结构（ctp2rs FFI 契约）。
4. 业务代码零 `unwrap()`/`expect()`（测试区除外）——检查器 `--strict` 模式 + workspace lints 双门禁。
5. **落盘线程（writer.rs）的追加+fsync 语义不许动**——崩溃最多丢 60 秒的保证全靠它；改 jsonl 相关逻辑必须先读 D028 的护栏设计。
6. 方案 B 的守护常量（CONNECT_TIMEOUT_MS=15s / DISCONNECT_REBUILD_MS=60s / LOGIN_FAIL_REBUILD=3）改动前跑 `方案b守护常量下限` 单测，且调小会引发重建风暴。
7. 归档压缩的三道护栏（非今日 / mtime 静默 10 分钟 / 产物非零）不许删——删了会出现"采集中文件被压缩"事故。
8. **改动流程**：一次一项 → 沙盒 `just ci && just 关卡` 全绿 → 用户机器复验 → 实盘 SimNow/收盘验证 → 切换。
9. 用户机器约束：sccache 全程、不覆盖 CARGO_TARGET_DIR、不删 ~/.cargo/config.toml、不用 root 构建、**安装工具 cargo binstall 优先，失败 cargo install**。

## §8 踩坑（本棒新增）

| 坑 | 教训 |
|---|---|
| gitignore 行内注释无效 | `#` 注释必须整行；行内 `#` 被当路径字符——已用白名单模式根治 |
| MultiEdit 大块替换损文 | `st[member_idx]`→`stember_idx]`：大块文本编辑后必须立刻 grep 关键行验证 |
| struct 字段可见性 | 全局 regex 加 pub(crate) 会误伤函数参数/字面量——必须按 struct 块范围处理 |
| rsproxy 对 cargo 抽风 | index 直连 curl 快但 cargo 客户端 30s 0 字节——换官方 index.crates.io 立刻恢复；curl 命令行结论不能外推给 cargo 客户端 |
| 沙盒回收 | 工具链/工作区全丢但 download/scripts/upload 存活——产物必须落持久目录，工具链重装 ~10 分钟（rsproxy） |
| 旧 bin 编不过 | v2/v1_3 等 84 errors 是历史遗留；验证口径维持"仅 v1_4 bin" |
| cargo install 超时误判 | 日志只显示 "Updating index" 不代表没动——要看进程；进程没了且 registry 无源码=失败 |

## §9 已拍板的决策（索引）

D001 停下问+先取证 · D002 隐患优先 · D003 ~~大改造不做~~（**D021 作废，改全做**） · D006 ~~只做高优~~（**D021 升级全做**） · D008 分支b · D016 🔴1根因+方案A/B · D017 12条warning全清+fmt可用 · D021 **全做+方案B+0.1.11-alpha1+fmt+直接应用补丁+schema-first+release直跑+binstall优先** · D022-D032 本轮逐项完成记录（见决策日志）

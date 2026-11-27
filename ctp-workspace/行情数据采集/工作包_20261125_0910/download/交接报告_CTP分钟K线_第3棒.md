# 交接报告 · CTP 分钟K线落盘项目（第3棒 · 2026-11-25 接手）

> **给谁看**：接手本棒工作的下一个智能体 / 开发者，或回到本窗口的第3棒自己（沙盒回收后上下文全失，靠本文+决策日志恢复）。
> **读法**：先读 §3（当前状态）→ §4（对第2棒报告的事实修正，**有7处，必读**）→ §5（待办）→ §7（红线）。背景知识看第2棒原始报告（`交接报告_第2棒_原始.md`，已随包附带）；逐决策推理链看 `决策日志_推理链.md`。
> **最后更新**：2026-11-25 · D015 · 沙盒环境就绪、源码到手、🔴1行为证据入手、编译待vendor。
> **工作模式（用户铁律）**：任何选择点停下来问用户；一切结论以回传输出/源码/文件为准，不猜测；每个决策记入决策日志（五要素：决策/事实依据/推理链/备选弃用原因/验证方式）；**每次汇报工作时重新打时间戳工作包 + git commit**。

---

## §1 项目一句话（继承）

连期货公司 CTP 行情前置（16 条链路），收 tick 聚合成分钟 K 线，追加分片落盘 jsonl（崩溃最多丢 60 秒），合并成 13 列 Parquet 供回测/分析。当前主力 `src/bin/分钟k线落盘_v1_4.rs`（1577行，单文件，中文标识符），v1.1 完整版是冻结退路。

## §2 沙盒环境（第3棒的工作机）

| 项 | 事实 |
|---|---|
| 工具链 | rustc/cargo **1.102.0-nightly** + clippy 0.1.102 + rustfmt（与第2棒报告 §14 基线 nightly=1.102 对齐）；装在 `/home/z/my-project/.rustup`、`/home/z/my-project/.cargo`（持久） |
| 环境入口 | `source /home/z/my-project/scripts/env_rust.sh`（含 rsproxy 镜像变量） |
| 镜像 | **必须走 rsproxy.cn**（官方 static.rust-lang.org 实测 6.5KB/s 不可用；rsproxy 9.4MB/s）。cargo 依赖走 sparse 镜像（配置在 `scripts/cargo_config_沙盒.toml` 与 `~/.cargo/config.toml`，仅沙盒生效） |
| 权限 | 无 root；**不能创建 `/home/xx/`**（Permission denied）→ 用户机器的绝对路径依赖无法原样复现，见 §4-修正⑥ |
| 编译验证 | 已实测 build/clippy/test 全链路可用（nettest 试金工程） |
| 中文 | LC_CTYPE=C.UTF-8，中文文件名/输出实测正常；git 已配 `core.quotepath false` |
| git | 本地仓库在 `/home/z/my-project`（不推送），每次汇报打一次 commit |

## §3 当前状态（2026-11-25 07:5x 用户时区 UTC）

**用户机器（生产侧）**：
- v1.4 采集进程活着，**debug 构建**（`/opt/rust_cache_本地缓存/target_xx/debug/分钟k线落盘_v1_4`，pid 255768）。**是否故意用 debug 待向用户确认（不擅自改）**。
- 用户**人工重启**后 16/16 登录、累计断线 0 → 表面健康。但**重启≠修复**：🔴1（断线后登录不重发）根因还在代码里。
- 数据：`/home/xx/projects/ctp-workspace/data_数据/分钟分片/bars_20261124.jsonl`(181M)、`bars_20261125.jsonl`(397M，增长中)。成品目录只有 11-24 的两个非标准命名 parquet（`(Copy 1)` 和 `_夜盘` 后缀，来源待确认）。
- `合并 今日`（07:37 跑的那次）**没有产出**：无新 parquet、无任何 `[MERGE]` 日志行 → 判定：那次合并进程没跑到合并逻辑（嫌疑：cargo release 编译 arrow/parquet 耗时，输出没被看到）。代码读完后确认了合并的确定性输出格式，重跑即可验证，见 §5-0。
- 第2棒报告说的"合并后 jsonl 改名 `.merged_`"**在 v1.4 代码里不存在**（详见 §4-修正②），故不存在改名错位风险。

**沙盒（工作侧）**：
- 源码已到手并解包：`/home/z/my-project/workspace_ctp/ctp_live_实盘接入工程/`（含 lib/、logs/、Cargo.lock、clippy.toml、deny.toml、检查格式串.py）。
- 关卡1（`检查格式串.py`）已跑：**v1_4.rs 通过**。
- **编译被路径依赖阻塞**：`ctp2rs = { path = "/home/xx/projects/ctp-workspace/vendor/ctp2rs-src/ctp2rs-ctp2rs-0.1.9/ctp-dyn", ... }`，vendor 不在 tar 包里，等用户发（§5-1）。到手后在沙盒内把该 path 改指向沙盒位置（仅沙盒适配，原始文件不动、不回传此改动）。
- 🔴1 行为证据已入手（原来用户以为丢了，实际在 tar 包里）：`logs/完整版.out`，见 §4-修正⑤。

## §4 对第2棒报告的事实修正（以源码/文件为证，共7处）

| # | 第2棒报告说法 | 实测事实 | 证据 |
|---|---|---|---|
| ① | 红线§12.1"chrono 必须 =0.4.38" | **Cargo.lock 锁的是 0.4.45**；Cargo.toml 故意解开 `=`（注释：配合 h5i-db-core；0.4.38 只是下限）。红线的真实含义="别乱动 Cargo.lock 里已解析的 chrono 版本" | Cargo.toml L21 注释 + Cargo.lock |
| ② | §7"合并后原jsonl转.merged_留档" | **v1.4 合并不改名**：复制 `.merging_` 快照→读→删快照→原子写 parquet，原 jsonl 不动 | v1_4.rs L997-1098 |
| ③ | §2 暗示 crate 只有 K 线 bin | crate 实为**交易+行情全功能工程**：src/trading_gateway/ 5个模块、main.rs、7个 bin（含 h5i-db数据库查询.rs）；依赖含 tokio/arrow58/parquet58/h5i-db-core/encoding_rs（tokio 等被其他 bin 用，v1_4 仍是纯 std 线程） | Cargo.toml + src/ 清单 |
| ④ | §6"已登录15"是小隐患 | 实为**同一病灶的严重形态已有实录**：v1.1 在 11-24 的运行中，7655秒时 15/15 → 5秒内雪崩 → **7.5小时登录数=0**（TCP反复重连成功、登录永不重发），断线累计96次、tick冻结 | logs/完整版.out（时间线见决策日志 D012） |
| ⑤ | §10 说两道关卡脚本在交付包 | tar 包里只有 `检查格式串.py`，**`检查_unwrap与打印.py` 缺失**，待用户发 | tar 清单 |
| ⑥ | （未写）路径依赖 | `ctp2rs` 是**绝对路径依赖**指向用户机器 `/home/xx/...`；vendor/ 不随 crate 打包 → 异地编译必须先拿到 vendor 包 | Cargo.toml L15 |
| ⑦ | §6/§16 数据目录相对路径 | 实际在**工作区根** `/home/xx/projects/ctp-workspace/data_数据/`，不在 crate 目录内 | 用户回传 ls |

## §5 待办（D002 分支 b：先清 warning，再🔴1→🔴2→🔴3；一次只做一项，每项过门禁再做下一项）

**0. 合并验证补跑**（用户侧，等回传）：`cargo run --release --bin 分钟k线落盘_v1_4 -- 合并 今日 2>&1 | tee /tmp/合并输出.log; echo exit=$?; ls -lh data_数据/分钟分片/ data_数据/分钟K线/`——重点看 `[MERGE] 合并模式 | 分片目录...` 和 `✅ 合并完成` 两行；若报 `❌ 找不到分片文件` 则把整段发来（说明工作区根解析有问题）。另跑 `ps -eo pid,etime,cmd | grep -v grep | grep 分钟k线落盘_v1_4` 确认当前进程数。

**1. 等用户发 `vendor/ctp2rs-src` 打包** → 沙盒适配 path → `cargo build --release --bin 分钟k线落盘_v1_4` 全量编译 → `cargo clippy --all-targets` 拿到 warning 清单基线（第2棒说有12条，以实测为准）。

**2. 清 warning**（8条 let 链按第2棒报告用 clippy --fix 批量改，其余逐条人工；每改一批跑关卡1+沙盒 build/clippy/test）。
**3. 🔴1 治重连登录**：重连改指数退避+抖动，断线后主动重发登录。证据链已有（§4-④），动工前先读 v1_4.rs 重连段落，改动最小化（红线§12.6：聚合/落盘/CTP接入不大改；方案先问用户再动手）。
**4. 🔴2 接补齐缺口**：函数已在（v1_4.rs L1104），采集端没接。判据：某member>60s无tick。
**5. 🔴3 tick时间单调性校验**：回拨/跳变打 warn 并跳过（§5 第2棒报告的夏令时坑）。

**中低优（只登记不做，用户已拍板）**：panic hook、队列水位日志、.merged_清理策略（注意：v1.4 不产 .merged_，此项描述需重新核对）、fd上限、收盘自动合并、可观测性、cargo mutants、debug→release 确认等。

## §6 已拍板的决策（索引，详见决策日志 D001-D015）

D001 停下问+先取证 · D002 隐患优先顺序 · D003 大改造（SplitRS拆模块等）本次不做 · D004 合并验证现在跑 · D005 源码打包上传不发secrets · D006 只做高优 · D007 决策日志=沙盒md · D008 分支b判定（重启≠自愈） · D009 合并错位担忧→已由源码解除 · D010 沙盒工具链1.102就绪 · D011 vendor待发+path适配方案 · D012 🔴1证据时间线 · D013 合并代码事实 · D014 chrono事实与红线纠正 · D015 交接/打包/git机制。

## §7 红线（继承第2棒报告 §12 全部，逐条复核后的当前版本）

1. **不动 Cargo.lock 里已解析的 chrono 版本（现为 0.4.45）**——改动理由见 §4-①；Cargo.toml 的 "0.4.38" 是故意解钉的下限，**别自作主张加回 `=`**。
2. 不引入 tokio/async 到 v1_4（crate 其他 bin 已有 tokio，别扩散）。
3. 不改 CTP 回调签名 / SPI 结构（ctp2rs FFI 契约）。
4. 业务代码零 `unwrap()`/`expect()`；日志只用 tracing。
5. 不直连数据库（h5i-db 是其他 bin 的事）。
6. 聚合/落盘/CTP接入三块不大改；要改先小范围验证（5分钟采集→Ctrl+C→看jsonl→合并）。
7. **改动流程**：一次一项 → 沙盒 build/clippy/test 全绿 → 关卡脚本 → 交用户跑门禁三件套（fmt+clippy+nextest）→ 用户确认 → 下一项。
8. 用户机器约束（不适用于沙盒但别教坏）：sccache 全程、不覆盖 CARGO_TARGET_DIR、不删 ~/.cargo/config.toml、不用 root 构建、安装工具用 cargo binstall。

## §8 踩坑（本棒新增，继承见第2棒报告 §13）

| 坑 | 教训 |
|---|---|
| 官方 Rust 源 6.5KB/s | 沙盒下载必须走 rsproxy.cn（9.4MB/s），rustup-init/wget 直连会假死超时 |
| `/home/xx` 不可创建 | 沙盒无 root；路径依赖只能"vendor 包 + 本地改 path"适配 |
| 第2棒报告 vs 代码漂移 | 报告写了 `.merged_` 改名、`=0.4.38`、两道脚本齐全——实际都不成立/不齐。**结论：报告是快照不是真相，动手前一律以源码+文件为准** |
| "日志没了" | 用户以为重启前日志丢了，实际在 tar 包的 logs/完整版.out 里。**收包先翻 logs/** |
| cargo run 的输出归属 | 采集进程在前台终端刷屏时，另一终端跑的合并输出容易被忽略/贴错窗口——长命令一律 `tee` 落盘再贴 |

## §9 文件地图

| 想要 | 位置 |
|---|---|
| 本棒交接报告 | `download/交接报告_CTP分钟K线_第3棒.md`（本文件，滚动更新） |
| 决策推理链 | `download/决策日志_推理链.md`（增量追加） |
| 第2棒原始报告 | `download/交接报告_第2棒_原始.md` |
| 工作包（时间戳快照） | `download/工作包_YYYYMMDD_HHMM.tar.gz`（每次汇报重新生成） |
| 沙盒源码 | `workspace_ctp/ctp_live_实盘接入工程/`（用户的 tar 解包） |
| 沙盒环境入口 | `scripts/env_rust.sh`；cargo 镜像配置 `scripts/cargo_config_沙盒.toml` |
| 打包+提交脚本 | `scripts/打包工作快照.sh` |
| 🔴1 证据 | `workspace_ctp/ctp_live_实盘接入工程/logs/完整版.out` |

## §10 用户侧待发材料（截至本文更新）

1. **`vendor/ctp2rs-src` 打包**（编译解锁的关键）：
   ```bash
   cd /home/xx/projects/ctp-workspace
   tar --exclude='*/target' -czf /tmp/vendor_ctp2rs.tar.gz vendor
   ls -lh /tmp/vendor_ctp2rs.tar.gz
   ```
2. `检查_unwrap与打印.py`（第2棒报告 §10 的第二道关卡，tar 包里没有）。
3. §5-0 的合并补跑输出（tee 落盘版）。
4. （可选确认）chrono 红线纠正（§4-①）是否与你的记忆一致；debug 构建是否有意。

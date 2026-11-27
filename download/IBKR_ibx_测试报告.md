# IBKR ibx 接入测试报告（Task 4-b）

> 测试时间：2026-11-27（北京时间）22:03 – 22:10（对应日志 UTC 14:03 – 14:06Z）
> 测试人：子代理 4-b（IBKR 接入测试）
> 测试对象：gitee.com/zz237_admin/ibx_fork（IBX v0.9.5，HEAD 1693d1a，--depth 1）
> 凭据纪律：IBKR 模拟号经 `IB_USERNAME`/`IB_PASSWORD` 环境变量传入测试进程，**密码由会话持有不落盘**——未写入任何文件、任何日志、任何输出。
> 所有"报错原文"均为 /tmp/ibx_run.log（233 行，无凭据）与终端实录逐字摘录，未编造。

---

## 0. 结论速览

| 项 | 结果 |
|---|---|
| 克隆 | ✅ gitee **匿名**直连成功（未动用 token，未动用镜像前缀） |
| ibx 协议定性 | ✅ 非 REST / 非 websocket / 非网页会话——**直连 IB 服务器的私有二进制 socket 协议**（TLS:4001 认证 + SRP/DH + FIXCOMP 压缩），完全绕开 IB Gateway/TWS |
| 语言构成 | ✅ 核心 100% Rust（289 个 .rs）；Python 只是 PyO3 绑定层与 Jupyter 演示（106 个 .py，可整体忽略）；另有可选 C++ 兼容层 |
| Rust 构建 | ✅ `cargo build --lib --bin ibx` 成功（rustc 1.100.0，dev 1m05s，仅 4 条 dead_code 警告） |
| 模拟号登录 | ✅ **两次登录全部成功**，账号 DU1200508，经 cdc1→hdc1→**mcgw1.ibllc.com.cn（中国网关）**，SRP 认证 + 三条数据 farm 全部建立，心跳正常 |
| 延迟行情 | ❌ 0 tick——`req_market_data_type(3)` 已生效，但 AAPL 报 **10089**、随后 **354**，EURUSD 报 **10089**：该模拟账号在 API 通道无延迟行情权益（账号订阅限制，非 ibx 缺陷） |
| 网关前置假设 | 任务书"若它需要 TWS 纸面网关端口 7497"——**前提不成立**：ibx 根本不需要任何网关，沙盒无需起 IB Gateway |

---

## 1. 克隆过程实录

1. `git clone --depth 1 https://gitee.com/zz237_admin/ibx_fork.git` → **匿名直接成功**（终端输出 `Cloning into 'ibx_fork'... RESULT: CLONE_OK_GITEE_ANON`）。
2. oauth2 token 与 ghfast.top 镜像两条回退路径**均未触发**；仓库内 remote 保持匿名 URL，无需 set-url 清理。
3. 仓库规模：HEAD `1693d1a release: v0.9.5`；文件构成 `289 rs / 223 jsonl(测试场景) / 106 py / 58 h / 20 cpp / 42 md`。

## 2. ibx 实际协议定性（读 README + 源码证据）

- **协议**：IB Gateway 客户端与 IB 服务器之间的**私有二进制协议**的独立 Rust 重实现（README《How IBX Was Built》自述"通过独立分析官方 IB Gateway 客户端与 IB 服务器之间的网络流量"逆向而来，未反编译任何 IB 软件）。
- **源码证据**（目录即协议栈）：
  - `src/auth/{srp,dh,session,crypto,certs}.rs` —— **SRP/DH 大整数认证**（Cargo.toml 依赖 `num-bigint`/`hmac`/`sha1`/`sha2`/`aes`/`cbc`/`x509-cert`），即"用户名+密码"直接对 IB 认证服务器做安全远程密码握手；
  - `src/protocol/{fix,fixcomp,connection,tick_decoder,depth_decoder,ns,xyz}.rs` —— FIX 风格会话 + **FIXCOMP 压缩信封** + tick/深度解码；
  - `src/gateway.rs` 日志实证：`Connecting to auth server cdc1.ibllc.com:4001 (ssl=true)`（TLS 直连 4001 认证端口，随后路由到 trading farm `hfarm`、行情 farm `apachmds`、合约定义 farm `secdefhk`）；
  - 结论：**不是 REST、不是 websocket、不是网页会话**，是"自实现网关"——把 Java Gateway 的事在进程内做掉。
- **语言构成**：核心引擎与 API 全在 Rust（`src/engine`、`src/api`、`src/control`）；Python（`src/python/PyO3` 兼容层 + `notebooks/*.ipynb`）与 C++（`cpp/`）只是同一引擎的两种 FFI 外壳。**用户只用 Rust：Rust API `EClient/EClientConfig/Wrapper/Contract/Order` 即全部所需，Python 部分可完全忽略。**

## 3. 构建结果

```
$ source /home/z/my-project/scripts/env_rust.sh   # rustc 1.100.0 / cargo 1.100.0 / rsproxy
$ cargo build --lib --bin ibx
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 1m 05s   # 仅 4 条 dead_code 警告
$ cargo build --bin delayed_quote_test            # 本次新增的测试 bin
    Finished `dev` profile ... in 1.42s
```

无 openssl/绑定缺失问题（`native-tls` 在沙盒直接编过）。本次唯一代码改动：新增 `src/bin/delayed_quote_test.rs`（约 130 行，登录→切延迟模式→请求 AAPL/EURUSD→回调打印→75s 汇总退出）。未 push。

## 4. 登录实测逐步记录（两次运行，真实日志原文）

**第 1 步 连接与重定向**（运行 1，UTC 14:03；运行 2，UTC 14:05 同链路）：
```
INFO ibx::gateway: Connecting to auth server cdc1.ibllc.com:4001 (ssl=true)
INFO ibx::gateway: Redirected to hdc1.ibllc.com, reconnecting...
INFO ibx::gateway: Redirected to mcgw1.ibllc.com.cn, reconnecting...      ← 自动落到中国网关
INFO ibx::gateway: Starting auth for ytzqmn666
INFO ibx::gateway: Auth complete                                          ← SRP 认证通过，~0.2s
```

**第 2 步 认证信封与 farm 路由下发**（FIXCOMP 解压 41281 字节）：
```
Auth msg type=A tag=6145: "mcgw1.ibllc.com.cn/hfarm"        (trading farm)
Auth msg type=A tag=6171: "mcgw1.ibllc.com.cn/apachmds/4000"(market-data farm)
Auth msg type=A tag=8008: "mcgw1.ibllc.com.cn/secdefhk/4000"(sec-def farm)
Normal logon [cutoffVersion=None]
Auth logon: account=DU1200508 session_id=6b09754d.00000008 hb=10s scale_us_lots=true max_real_time_requests=100
Init sequence sent (101 messages, seq now 102);  Init response: 31911 bytes in 409ms
```
（DU 前缀 = IBKR 模拟/paper 账号；同一账号两次会话 00000006、00000008 均建立成功。）

**第 3 步 API 层就绪**：
```
[13543ms] Connected（登录成功，密码仅存于会话内存）
[13568ms] error req_id=-1 code=2104: Market data farm connection is OK:hfarm
[13568ms] error req_id=-1 code=2106: HMDS data farm connection is OK:apachmds
[13568ms] error req_id=-1 code=2158: Sec-def data farm connection is OK:secdefhk
[13769ms] error req_id=-1 code=2119: Market data farm is connecting:usfarm / :cashfarm
[18188ms]/[19293ms] code=2104 OK:cashfarm / OK:usfarm       ← 三类 farm 全部握手完成
（随后 75 秒 Farm TestReq 心跳 seq=2..7 全部 Ok，会话稳定）
```

> 说明：ibapi 风格里 2104/2106/2158/2119 经 `error()` 回调下发，实为**连接状态信息**而非故障。

## 5. 延迟行情结果（真实返回/报错原文）

先 `req_market_data_type(3)`（延迟冻结模式，回调确认 `market_data_type req_id=1 mode=3`），再请求两个合约：

| 合约 | req_id | 返回（原文） | 解读 |
|---|---|---|---|
| EURUSD CASH/IDEALPRO (con 12087797) | 2 | `[18489ms] error req_id=2 code=10089: Requested market data requires additional subscription for API. See link in 'Market Data Connections' dialog for more details.` | 该模拟账号的 API 通道无外汇延迟行情权益 |
| AAPL STK/SMART (con 265598) | 1 | `[19544ms] error req_id=1 code=10089: ...additional subscription for API...` → `[19544ms] market_data_type req_id=1 mode=3` → `[19544ms] error req_id=1 code=354: Requested market data is not subscribed. ... availability of delayed data.Error&BEST/STK/Top&BEST/STK/Top` | 美股 TOP 延迟行情同样未授权；`mode=3` 确认延迟开关本身已生效 |

75 秒窗口 SUMMARY：`prices=0 sizes=0 strings=0 generic=0 errors=10` —— **0 tick**。

**定性**：登录、farm 握手、心跳、延迟模式切换全部成功；行情被拒是**账号订阅权益问题（10089/354）**，不是 ibx 协议问题。沙盒网络直连 IB 服务器（4001 端口 TLS）畅通。下一步若要拿到真实 tick，需换有行情订阅的账号（或启用订阅后的 paper 账号）重测；交易链路（下单回环）同理可在 paper 账号上继续验证。

## 6. 工具接 IBKR 结论表

| 工具 | 有无 IBKR 通道 | 是否需 IB Gateway/TWS | 语言/许可 | 本次核实程度 | 对本工程（CTP Rust 采集 + Rust 策略层）适配 |
|---|---|---|---|---|---|
| **ibx v0.9.5** | ✅ 直连 IB 服务器（自实现网关协议） | **否**（实测登录成功，无 7497/4002 本地端口） | Rust 核心；AGPL-3.0 | **本次实测**：登录✅、会话/心跳✅、行情权益❌(账号侧) | 高：与 CTP 采集工程同为 Rust，可进程内组合；注意 AGPL 与"逆向协议随时可能失效"的自述风险 |
| **barter-rs** | ❌ 无 IBKR 连接器（Task 2-a 调研报告与官方 README 均未见；内置连接器为加密货币系） | — | Rust；MIT | 调研级（Task 2-a 已核 README/源码目录） | 低-中：只能自写 `MarketStream`/`ExecutionClient` 适配器；框架本身无期权数学 |
| **NautilusTrader InteractiveBrokersAdapter** | 有，但 **Python 侧**（`adapters/interactive_brokers`，基于 ibapi/ib_async 路线），且需本机起 IB Gateway/TWS | **是**（Python 适配器 + 网关进程） | Python-first；LGPL-3.0 | 调研级（上游文档/调研报告；适配器源码未逐行核对，标注待验证） | 低：Python 优先级用户已定最低，且引入网关进程与 LGPL 边界 |
| **ChainView**（joaquinbejar） | provider 能力表声明含 `ibkr`（Task 2-a 核实的 README） | 是否经 TWS/网关**未验证——待验证** | Rust（ratatui）；MIT | 调研级 | 中：其价值是 T 型报价终端 UI；正确用法是**自写 provider** 喂 CTP/Parquet 数据，绕开其 ibkr provider |

## 7. Rust 策略层优先路线建议

1. **IBKR 通道选 ibx（Rust 原生、无网关）**，不建议 NautilusTrader：登录链路本次已实证（中国网关自动重定向 + SRP + farm 全建立），与 CTP 采集工程同语言可直接进程内组网；Nautilus 是 Python 侧适配器 + 必须另起网关 + LGPL，三重不符。
2. **ibx 定位为"研究/参考通道"**：AGPL-3.0 + 逆向协议（README 自述 IB 可随时变更且无兼容承诺），不宜进闭源分发与生产关键路径；期权链能力上游 issue #38 尚 blocked（多资产期权未完成），**中国期权链数据仍以 CTP 为主源，ibx 只作海外对照研究**。
3. **待解除的两个验证缺口**（均卡在账号权益，不在代码）：a) 有订阅账号上的真实延迟 tick；b) paper 账号 LMT 挂单→order_status 回环。账号具备权限后各约 10 行 Rust 即可复测（本次的 `delayed_quote_test.rs` 就是现成骨架）。
4. 期权分析层沿用 Task 2-a 结论：optionstratlib / volsurf / implied-vol 三件套（纯 Rust、无行情源耦合）；IBKR 行情仅作 USD 标的对照数据源，非必需依赖。

## 8. 变更与留痕清单

- 克隆：`/home/z/my-project/期权工具研究/ibx_fork/`（匿名 remote，--depth 1，未 push）
- 新增：`src/bin/delayed_quote_test.rs`（本次唯一代码改动）
- 日志：`/tmp/ibx_run.log` 233 行（全程无凭据字段）
- 凭据：oauth2 token 仅出现在一次克隆**命令**中（本次未触发）；IBKR 密码由会话持有不落盘

*报告完 —— 子代理 Task 4-b，2026-11-27 北京时间。*

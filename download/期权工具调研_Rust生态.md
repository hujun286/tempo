# Rust 期权工具生态调研报告
## ——面向 CTP 分钟K线行情采集工程补齐期权能力的选型调研（Task 2-a）

> 调研时间：2026-11-27（所有 stars / 更新时间 / 下载量为本次实际抓取值）
> 调研方式：crates.io API、GitHub API / README 全文 / 源码目录、网页抓取（page_reader）与搜索引擎交叉验证
> 服务对象：现有 CTP（中金所 IO/MO/HO + SHFE/INE/DCE/CZCE/GFEX 商品期权）分钟K线 Rust 工程（Parquet 落盘 13 列：合约/交易所/交易日/bar时间/开高低收/成交量额/持仓量/增量列等），需补：定价、Greeks、IV、微笑/期限结构、买方与卖方策略分析。
> 原则：只列实际打开/抓取过的来源；未亲自打开全文的标注"（来自搜索摘要）"；不确定处标"待验证"。

---

## 0. 结论速览（Top3）

| 排名 | 工具 | 一句话理由 |
|---|---|---|
| 1 | **OptionStratLib**（joaquinbejar） | 纯 Rust 全功能期权库：BS/Black-76/二叉树/MC + 全套 Greeks + IV + SVI 曲面 + 25+ 策略（铁鹰/备兑/保护性认沽/Collar）+ SPAN 风格保证金 + 回测/可视化；MIT+Apache；高频活跃；数据接口是纯结构体/CSV/JSON，接 Parquet 改造成本低-中 |
| 2 | **volsurf**（volsurf-rs） | 波动率曲面专用库：SVI/SABR/SSVI/eSSVI + 蝶式/日历无套利检查 + Dupire 局部波动率 + Jäckel 高精度 IV；Apache-2.0；输入就是 `(到期,行权价,IV)` 数组，与我们的 IV 管线天然衔接，改造成本低 |
| 3 | **tfrmma 期权双引擎**（options-pricing-engine-rs + options-market-making-engine-rs） | 极轻依赖（6 个基础 crate）的 Black-76/BSM/Heston/Bates/Dupire 定价引擎 + "不碰网络、纯 Rust 类型进出"的期权做市大脑（曲面→风控→报价→对冲→PnL 归因）；MIT；作为第 3 依赖源与卖方/做市架构参考，Deribit 逆向合约部分需替换为线性 Black-76，改造成本中 |

辅助结论：IV 反解与高频批量 Greeks 建议用 `implied-vol`（38 万下载）或 `blackscholes`/`black76`（同作者、~35ns 单期权定价）做轻量底座；框架类（nautilus_trader / barter-rs / hftbacktest）与本工程的"纯 Rust + 分钟K线"形态均不直接匹配，详见第二节末尾对照。

---

## 一、总览表

### A. 纯 Rust 期权定价 / Greeks / IV / 波动率曲面库

| 工具（crates.io） | 定位 | 核心能力 | stars | 许可证 | 依赖重量 | 活跃度（最新动作） | 中国参数支持 | 对接成本 |
|---|---|---|---|---|---|---|---|---|
| **optionstratlib** [crates.io](https://crates.io/crates/optionstratlib) / [GitHub](https://github.com/joaquinbejar/OptionStratLib) | 全功能期权交易与策略开发库（9 个组件 crate + facade） | BS / **Black-76** / Garman-Kohlhagen / 二叉树（美式）/ MC（GBM/Heston/GARCH/rough Heston 等）/ 12 类奇异期权；Greeks 全套（ΔΓνΘρ + Vanna/Vomma/Veta/Charm/Color）；IV（Newton+二分，至 500% 年化）；SVI 切片加权校准 + 蝶式/日历静态套利检查；`ImpliedVolatilitySurface::calibrate`（从买卖报价校准）；OptionChain（CSV/JSON 导入导出、行权价生成、过滤）；25+ 策略；**SPAN 风格保证金**；概率分析/风险中性密度；回测引擎；plotly 可视化 | 271 | MIT + Apache-2.0（README 双许可；crates.io 版本字段标 MIT） | facade 默认拉全功能（含 plotly）；可按 feature 或只用组件 crate（如 `optionstratlib-pricing`）显著瘦身 | v0.24.6（2026-11-18 发版）；累计下载 11.7 万、近期 5.6 万 | 欧式+Black-76 可覆盖股指/商品期权数学；合约乘数/保证金公式/涨跌停/中国日历需自研（SPAN 风格仅是参考实现） | **低-中**：数据从 Parquet→OptionChain/结构体；f64↔Decimal 转换层 |
| **implied-vol** [crates.io](https://crates.io/crates/implied-vol) / [GitHub](https://github.com/nakashima-hikaru/implied-vol) | 高精度纯 Rust IV 与期权定价（Black/Bachelier/归一化 Black） | 隐含 Black 波动率、隐含 Normal（Bachelier）波动率（支持负远期）、欧式期权价 builders；Jäckel 风格高精度算法 | 23 | MIT + 第三方声明（含 Jäckel 代码改编，商用前核对 notices） | 极轻（纯 Rust） | v2.1.0（2026-10-03）；累计下载 37.8 万、近期 13.9 万 | 数学层通用；无中国参数 | **低**：直接以 f64 接口反解 IV，接我们的期权分钟行情 |
| **blackscholes** [crates.io](https://crates.io/crates/blackscholes) / [GitHub](https://github.com/hayden4r4/blackscholes-rust) | 高性能 BS 定价 + 一二三阶 Greeks + IV | 欧式/美式 Vanilla、Binary、Barrier；一/二/三阶 Greeks；IV：Modified Corrado-Miller 初值 + Newton，以及 Jäckel "Let's be rational"；单期权定价 ~35-40ns、1000 期权全 Greeks ~2ms | 16 | MIT | 极轻 | crates.io v0.24.0 停在 2023-09-23，仓库 2025-10-27 有推送（发版滞后，待验证是否将发新版） | 数学层通用；无中国参数 | **低** |
| **black76** [crates.io](https://crates.io/crates/black76) / [GitHub](https://github.com/hayden4r4/black76-rust) | Black-76（期货期权）定价/Greeks/IV 姊妹库 | 与 blackscholes 同 API 风格，直接面向期货期权定价（中国商品/股指期权数学底座） | 2 | MIT | 极轻 | v0.24.2（2023-07-28），其后无动作（数学稳定，停更风险见第四节） | Black-76 正是中金所/商品期权教材模型；无中国参数 | **低** |
| **black_scholes** [crates.io](https://crates.io/crates/black_scholes) / [GitHub](https://github.com/danielhstahl/black_scholes_rust) | 简洁 BS 定价 | call/put 定价、Greeks、IV | 25 | MIT（crates.io 字段；GitHub 仓库缺 LICENSE 文件，合规留痕需注意） | 极轻 | v0.11.1（2026-01-18）；下载 14.8 万 | 无 | **低** |
| **volsurf** [crates.io](https://crates.io/crates/volsurf) / [GitHub](https://github.com/volsurf-rs/volsurf) | 生产级波动率曲面库 | **SVI（Zeliade 准显式校准）/SABR（Hagan 闭式）/三次样条**；**SSVI/eSSVI** 全局曲面（eSSVI 日历无套利保证）；蝶式/日历无套利检测（`SurfaceDiagnostics`）；Dupire 局部波动率；Black/Bachelier IV（Jäckel 近机器精度）+ displaced diffusion；vega 加权、warm-start、DataFilter 剔翼部；零分配 vol 查询（≤20ns）；serde 序列化 | 43 | Apache-2.0 | 轻（rayon/tracing 为可选 feature） | v4.0.1（2026 年创建，2026-09-27 推送）；下载 1.3 万 | 曲面数学通用；输入 `(tenor, strike, iv)` 与中国市场 T 型报价天然对齐；合约乘数等无关 | **低** |
| **regit-svi** [crates.io](https://crates.io/crates/regit-svi) / [GitHub](https://github.com/org-regit-io/regit-svi)（来自搜索摘要） | 无套利 SVI/SSVI 曲面（纯 Rust） | Raw/Jump-Wings/SSVI 参数化、校准、静态套利检查 | 4 | Apache-2.0 | 轻 | 2026-08 推送；下载 7.6 万 | 无 | 低 |
| **stochastic-rs** [crates.io](https://crates.io/crates/stochastic-rs) / [GitHub](https://github.com/rust-dd/stochastic-rs) | 量化金融大而全（130+ 随机过程 + 定价/校准/曲面） | Heston 定价与校准、SVI/SSVI 曲面教程、Fourier/PDE/格点/MC 定价、GPU（CUDA/Metal）、PyO3 绑定 | 202 | MIT | 重（workspace 全家桶） | v3.0.0-rc（2026-11 活跃） | 无 | 中-高（按需取 `stochastic-rs-quant` 子 crate） |
| **RustQuant** [crates.io](https://crates.io/crates/RustQuant) / [GitHub](https://github.com/avhz/RustQuant) | 通用量化金融库（自动微分等） | 覆盖面广（期权定价模块覆盖度待验证） | 1849 | Apache-2.0 | 中-重（多子 crate） | 2026-10-04 推送 | 无 | 中 |
| **options_pricing_engine**（tfrmma）[GitHub](https://github.com/tfrmma/options-pricing-engine-rs) | 低延迟期权定价引擎 | BSM / **Black-76** / Heston / Bates / Dupire 局部波动率；解析 Greeks（含 vanna/volga）+ AD Greeks；Halley 迭代 IV；Levenberg-Marquardt 校准（单起/多起/差分进化全局）；无套利曲面修复；MC（Euler 全截断/Andersen QE） | 16 | MIT | **极轻**（num-complex/num-traits/rayon/libm/rand/realfft 共 6 个） | 2026-09-11 推送；CI 齐备 | linear 模型可直用；Deribit 逆向合约模块弃用即可 | **中**（作依赖引入成本中，作为源码级参考更佳） |
| **opengreeks**（marketcalls）[GitHub](https://github.com/marketcalls/opengreeks)（来自搜索摘要） | py_vollib 的 Rust 替代（PyO3） | Black-76/BS/BSM，二/三阶 Greek 面，与 vollib bit 级一致 | 28 | MIT | 极轻 | 2026-05 推送；Rust 侧是否可直接作 lib 依赖待验证 | 无 | 低（若走 Python 桥）/待验证 |
| 其他小项目：[gnsqd/surface](https://github.com/gnsqd/surface)（2★，Apache-2.0，SVI+CMA-ES/L-BFGS-B，2025-11 新建）、[regit-blackscholes](https://github.com/org-regit-io/regit-blackscholes)（3★，Apache-2.0，17 个 Greeks）、[siddharthqs/RustyQLib](https://github.com/siddharthqs/RustyQLib)（28★，MIT）、[klemen-r/slv](https://github.com/klemen-r/slv)（2★，MIT）、[day01/quant-opts](https://github.com/day01/quant-opts)（3★，MIT）、[option-pricing](https://crates.io/crates/option-pricing)（8.8k 下载，仓库 URL 404，疑似改名/删除，**待验证**）、[rust-options](https://crates.io/crates/rust-options)（仓库指向占位 `yourusername`，**不建议使用**） | | | | | | | | |

### B. 框架 / 做市 / 策略生态（含"接外国交易所行情"的期权工具）

| 项目 | 定位 | 核心能力 | stars | 许可证 | 数据输入接口抽象 | 对接 CTP 成本 |
|---|---|---|---|---|---|---|
| **OptionStratLib 生态**：[ChainView](https://github.com/joaquinbejar/ChainView) + [IronCondor](https://github.com/joaquinbejar/IronCondor) | T 型报价终端 + 期权策略回测器 | ChainView：ratatui 终端实时链/Greeks/曲面/盈亏图 + 回放模式，provider 适配器声明式能力表（deribit 默认/alpaca/ig/ibkr/tastytrade）；IronCondor：基于 optionstratlib + option-chain-orderbook + orderbook-rs 的**订单簿级撮合回测**、双成交模型、按 Greek 的 P&L 归因、Python 绑定 | 5 / 6 | MIT | **provider adapter 模式**：新增一个"CTP/Parquet provider"即可复用整套 UI/回放 | 中：写一个 provider + 把中国 tick 规范化进 adapter |
| **options-market-making-engine-rs**（tfrmma）[GitHub](https://github.com/tfrmma/options-market-making-engine-rs) | 期权做市"大脑"（5 crate） | vol-surface（每到期日 SVI + 蝶式/日历无套利 + 插值）→ book-risk（风控聚合、6 个 Greeks 闭式交叉验证、ForwardCurve）→ quoting-engine（**隐含波动率空间的 Avellaneda-Stoikov**、按 gamma 局部偏斜、输出映射 Mass Quote QuoteEntry、Deribit tick 表）→ hedge-orchestrator（精确 delta 对冲 + Whalley-Wilmott 不交易带 + vega 对冲成本效益选择）→ pnl-explain（二阶 Taylor ΔΓνvanna/volga 归因 + 显式 unexplained） | 6 | MIT | **最佳抽象**："This repo does not touch a wire. It consumes market data and positions through plain Rust types and produces desired quotes / hedge orders through plain Rust types."——行情输入就是普通结构体 | 中：替换 Deribit 逆向合约 Greeks→线性 Black-76；tick 表→中国交易所；外部行情槽直接喂 CTP tick 转换体 |
| **hftbacktest** [GitHub](https://github.com/nkaz001/hftbacktest) / [crates.io](https://crates.io/crates/hftbacktest) / [docs](https://hftbacktest.readthedocs.io) | HFT/做市回测+实盘（Rust+Python/Numba） | L2 MBP / L3 MBO 全订单簿重建、排队位置撮合、喂价/订单延迟模型、多资产多交易所、GLGT/APT 做市教程；实盘连接器仅 Binance Futures/Bybit | 5147 | MIT | 自有 npz/数据格式 + 自定义 feed 接口，面向逐笔深度行情 | **高**：需要 L2 深度而非我们的分钟K线；无期权链/Greeks 概念；仅作为远期做市执行引擎参考 |
| **barter-rs** [GitHub](https://github.com/barter-rs/barter-rs) / [crates.io](https://crates.io/crates/barter) | Rust 事件驱动实盘/回测生态（barter-data/instrument/execution/integration） | MarketStream / ExecutionClient 可插拔接口、Engine 状态机、批量回测、绩效指标 | 2379 | MIT | `MarketStream` trait + `InstrumentKind::Option(OptionContract)`（已核实源码：contract_size/settlement_asset/kind(C-P)/exercise(American-European-Bermudan)/expiry/strike，字段可映射中国合约） | **中-高**：自写 CTP MarketStream 适配器可行，但框架本身**无任何期权定价/Greeks/曲面能力**，需外挂 A 表库；对本工程属于"换壳"级改造 |
| **nautilus_trader** [GitHub](https://github.com/nautechsystems/nautilus_trader) / [官网](https://nautilustrader.io) | 生产级交易平台（Rust 核心 + Python API） | 已核实 `docs/concepts/options.md`：OptionContract/OptionSpread/CryptoOption 等工具类型（strike_price/option_kind/expiration_ns/underlying/**multiplier**/premium_style（含 FUTURES_STYLE，与商品期权期货式持仓语义相关）/exercise_style/settlement_type）、交易所 Greeks 流、期权链聚合、本地 Black-Scholes Greeks 计算器、Delta 中性教程 | 31848 | **LGPL-3.0** | adapter 模式（Python 为主） | **高**：Python-first + LGPL + 全平台重；纯 Rust 工程引入成本与合规成本大；建议只作架构借鉴 |
| **Hummingbot** [GitHub](https://github.com/hummingbot/hummingbot) / [官网](https://hummingbot.org) | 加密货币做市/策略机器人 | 主体 Python（GitHub language 字段核实为 Python），V2 controller 为 Python 体系 | 21411 | Apache-2.0 | Python/Cython 交易所连接器 | **不适用**：无 Rust 期权模块（其 Rust 相关组件与期权无关，深度核查待验证） |
| **PTR-25/box-spread-arbitrage** [GitHub](https://github.com/PTR-25/box-spread-arbitrage) | 静态套利扫描 + tick 级 Parquet 回测器 | box/conversion-reversal/垂直价差/蝶式四类静态无套利、多档 L2 扫单、Deribit 手续费与保证金机制、**Hive 分区 Parquet 回放**（基于 deribit-tick-collector） | 1 | MIT | tick 采集器→Parquet→回放的完整链路 | 中：架构参考价值大于直接复用（Deribit 专属逻辑多） |

### C. 中国市场数据层配套（改行情源的"最后一公里"）

| 项目 | 定位 | 关键事实 | 许可证 |
|---|---|---|---|
| [rn7s2/ctp-rs](https://github.com/rn7s2/ctp-rs) | CTP 安全惯用 Rust 绑定 | Win/Linux/macOS 三平台，2026-06-28 仍活跃；21★ | MIT |
| [Linfee/ctp-rs（ctp-sys）](https://github.com/Linfee/ctp-rs) | bindgen 直转 C++ API | 支持 6.6.5.x(linux)；2024-08 后停更 | Apache-2.0 |
| [ctp2rs](https://crates.io/crates/ctp2rs)（来自搜索摘要） | libloading 动态加载 ctp/ctp-mini/**ctp-sopt** | 免编译链接，适合快速接入 | 待验证 |
| [docs.rs/ctp_rust](https://docs.rs/ctp_rust)（来自搜索摘要） | CTP 安全绑定 | Linux/macOS，自动处理 GB18030→UTF-8 | 待验证 |
| [pseudocodes/tqsdk-rs](https://github.com/pseudocodes/tqsdk-rs) | 天勤 TQSDK 的 Rust 封装（期货**期权**行情/历史/交易） | 27★，2026-09 活跃，**无 LICENSE 文件（法律上默认不可用）**，WIP | 无 |
| [openctp/openctp](https://github.com/openctp/openctp)（来自搜索摘要） | 7x24 CTP 兼容环境/TTS，CTP 程序无缝对接 | 用于无夜盘时段的模拟联调 | 待验证 |

---

## 二、Top3 深度评估

### Top1：OptionStratLib（含 ChainView / IronCondor 生态）

**仓库**：https://github.com/joaquinbejar/OptionStratLib （v0.24.6，MIT + Apache-2.0，271★，2026-11-18 发版，11.7 万下载/近期 5.6 万）

**数据接口抽象**（这是它能低成本接我们行情的根本原因）：
- 核心对象是纯领域结构体：`Options`（strike/iv/underlying_price/expiry/style…，金额/波动率均为 `Positive`（rust_decimal 新类型）），`OptionChain`（OptionData 集合，支持 CSV/JSON 导入导出、行权价生成与过滤），`ExpirationDate::{Days, DateTime}`。
- **没有任何行情源依赖**：不绑定 websocket/交易所，喂什么算什么；`clock` feature 可关闭以支持确定性回放（对我们的历史 K 线回放是刚需）。
- facade 按 feature 拆 9 个组件 crate（core/math/pricing/market/analytics/strategies/simulation/backtest/visualization），我们可只引 `pricing`+`market`+`analytics`+`strategies`，避免 plotly 等重依赖。

**改造点清单**：
1. **类型桥**：f64 ↔ `Positive`/`Decimal`。库已提供 `f2d!`/`d2f!` 等宏与 `finite_decimal` 边界防护，写一个 `From<QuoteRow>` 转换层即可。
2. **数据补位**：我们的 13 列只有 OHLC/量额/持仓量，没有期权买卖价与 IV。方案：新增期权合约分钟采集（CTP 期权代码），每根 bar 用（买一+卖一)/2 或最新价经 `implied-vol`/`optionstratlib` IV 模块反解 → 填入 `OptionData.iv`。
3. **利率与远期**：Black-76 需要 forward；商品期权用同月期货价（我们的期货分钟K线直接可当 F），股指期权 IO/MO/HO 用指数价 + 中国利率曲线（自备 shibor/回购曲线）。
4. **到期日历**：`ExpirationDate` 需按中国规则生成（IO/MO/HO 到期月第三个周五等）；纯业务代码，自研约 1-2 天。
5. **保证金**：`span.rs` 是 SPAN 风格通用实现，**不是**上期所/中金所公式。保留它做交叉校验，另自研交易所公式（见第三节卖方部分）。
6. **涨跌停/T 型报价渲染**：库不管涨跌停；T 型报价展示可直接复用 ChainView 的 Chain/Surface/Payoff 屏（它就是 T 型报价板 + 曲面 + 策略盈亏）。

**与 13 列 Parquet 的衔接方式**：
```
Parquet(期货13列) ──► spot/forward 序列（close 列）
Parquet(期权分钟，待新增采集，同13列+买卖价扩展) ──► 按 (交易日,到期月,行权价,C/P) 分组
   ──► 反解 IV（implied-vol crate，f64 接口）
   ──► 组装 OptionChain{ OptionData{name,strike_price,spot,call/put bid/ask,iv,delta…} }
   ──► optionstratlib: 链过滤 → 策略构造(25+) → Greeks/PnL/概率 → 回测引擎
   ──► 结果落 Parquet/CSV（链与策略指标可与现有列式落盘同栈）
```
arrow-rs/parquet 读取批次 → DataFrame 风格分组（iter 多列）→ 每 bar 一次 chain 构建。分钟频率下 177 档全链 Greeks 校准在百毫秒级可完成（参照 opengreeks 对 Black-76 链 0.26ms 的量级与 volsurf ≤20ns 的 vol 查询，实际瓶颈在 IV 反解与 Decimal 精度路径）。

### Top2：volsurf（波动率微笑/期限结构专用）

**仓库**：https://github.com/volsurf-rs/volsurf （v4.0.1，Apache-2.0，43★，2026-02 创建，持续迭代）

**数据接口抽象**：
- 输入即市场快照：`SurfaceBuilder::new().spot(F).rate(r).add_tenor(T, &strikes, &vols).build()`；或直接 `SsviSurface::new(rho,eta,gamma,tenors,forwards,thetas)` / `EssviSurface::calibrate(&tenors,&forwards,&per_tenor_data)`——全部是 `(f64,f64)` 数组，**零行情源耦合**。
- 支持不规则行权价网格（不同到期日不同档位），正好匹配中国期权挂链不均的现状；`DataFilter`/`WeightingScheme` 处理翼部与流动性差的档位（我们深度不足的远月虚值档可剔除）。

**改造点清单**：
1. IV 输入由我们产出：volsurf 不管 IV 反解，需配 `implied-vol`（或 optionstratlib IV）先行反解——两库均为 f64 接口，胶水 ~50 行。
2. forward/tenor 计算：T 用（到期日-交易日）/年化（中国交易日历 243 天/245 天口径自定，全年一致即可）。
3. 期限结构输出：eSSVI 校准同时给出每 tenor 的 ATM 总方差 theta → 直接画期限结构；`SurfaceDiagnostics` 的蝶式/日历警报可作为**行情质检器**接入现有采集工程（异常报价自动标记）。
4. 与 Top1 的关系：volsurf 与 optionstratlib 的 SVI/曲面模块功能重叠；建议**曲面主用 volsurf**（数学覆盖更全、更轻、Apache-2.0），optionstratlib 的曲面作为策略库内置能力备用，避免双栈口径漂移。

**与 13 列 Parquet 的衔接方式**：Parquet 分组 → 每（交易日×到期月）一个 smile → 校准 → 把 (参数, 诊断) 作为新 Parquet 表落盘（与 13 列主表同栈）；回放时按 bar 时间索引曲面快照，即可做微笑/期限结构时序分析。

### Top3：tfrmma 双引擎（pricing + MM"大脑"）

**仓库**：https://github.com/tfrmma/options-pricing-engine-rs 、https://github.com/tfrmma/options-market-making-engine-rs （均 MIT，stars 低：16/6，单人维护但 CI/测试/文档质量高，2026-09/08 仍有推送）

**数据接口抽象**（全生态最佳）：
- MM 引擎自述："does not touch a wire. It consumes market data and positions through plain Rust types and produces desired quotes / hedge orders through plain Rust types. Wiring those to an actual exchange is somebody else's crate."——行情/持仓输入、报价/对冲输出全部是普通 Rust 类型，这正是"改一改就能接 CTP tick"的形态。
- 五段流水线（vol-surface → book-risk → quoting-engine → hedge-orchestrator → pnl-explain）+ 122 个测试 + 集成测试端到端验证，可作为我们卖方/做市模块的**架构蓝图**逐 crate 借鉴或直接依赖。

**改造点清单**：
1. `book-risk` 的 Greeks 按 Deribit 逆向（币本位）公式推导 → 整体替换为线性合约：直接用同作者 pricing engine 的 BSM/Black-76 解析 Greeks（ΔΓνΘ+vanna/volga），中国商品/股指期权都是线性欧式，数学上更简单。
2. `quoting-engine` 的 tick 取整绑定 Deribit `tick_size/tick_size_steps` → 换成中国各交易所最小变动价位表；Delibit 价格带宽 clamp 部分按其注释明确不移植。
3. `hedge-orchestrator` 的 perp delta 对冲公式 → 改为对期货（IH/IF/IM 或商品期货）对冲，Whalley-Wilmott 不交易带思路可保留（其 dollar-greek 重标定假设需按中国市场费率重算）。
4. `pnl-explain` 快照历史为内存 buffer，需外接持久化（可直接挂我们的 Parquet 落盘）。
5. `toxicity_score`（VPIN/Kyle λ）与 kappa/vol_of_vol 拟合是外部输入槽：我们有成交量额/持仓量列，可自研或后续引入其姊妹库（未核查，待验证）。

**与 13 列 Parquet 的衔接方式**：CTP tick/分钟 bar → `MarketSnapshot`（strike/expiry/bid/ask/position + forward）→ 引擎各 crate 纯函数调用；归因结果（delta/gamma/vega/vanna/volga/unexplained）按 bar 落 Parquet，与现有增量列体系并列。

### 框架对照（为什么不选 nautilus/barter/hftbacktest 进 Top3）

- **nautilus_trader**（31.8k★，LGPL-3.0）：期权对象模型最完整（含 `multiplier`、`premium_style=FUTURES_STYLE` 等，与我们需求贴合度最高），但 Python-first API + LGPL 合规边界 + 平台级重量，对"纯 Rust + 只补分析能力"的我们改造/合规成本过高。建议：只借鉴其 `options.md` 的字段设计与 Greeks 流发布模式（URL 见文末）。
- **barter-rs**（2.4k★，MIT）：数据层 `MarketStream` 抽象干净、`OptionContract` 字段可映射中国合约，但引擎内无任何期权数学；若未来要事件驱动实盘壳，可"barter 做壳 + optionstratlib/volsurf 做脑"，当前阶段改造收益不足。
- **hftbacktest**（5.1k★，MIT）：需要 L2/L3 逐笔深度与排队位置撮合，我们是分钟K线；且无期权链概念。远期若做期权做市实盘再评估。

---

## 三、买方 / 卖方策略工具建议

### 3.1 买方策略（做多波动率、保护性买入等）

| 能力 | 用哪个库的什么模块 | 缺什么我们自补 |
|---|---|---|
| 定价/Greeks 批量计算 | `optionstratlib-pricing`（BS/Black-76 + 全套 Greeks）；高频批量路径可换 `blackscholes`/`black76`（~35ns/期权）+ `implied-vol`（IV 反解）组合 | 中国利率曲线与 forward 生成（自备） |
| IV | `implied-vol`（Black/Bachelier，含负远期）；optionstratlib IV（Newton+二分兜底） | 期权行情采集与中价合成（买卖价/结算价） |
| 微笑/期限结构 | `volsurf`（eSSVI/SABR/SVI + 无套利诊断） | 按 bar 时序落曲面的 Parquet 表 |
| 策略构造与盈亏 | `optionstratlib-strategies`：Long Call/Put、Bull/Bear Call/Put Spread、Butterfly、Long/Short Straddle/Strangle、**Protective Put、Collar、PMCC**（做多波动率与保护性买入均有现成类型，含盈亏图/盈亏平衡/多断点） | 期权组合在中国交易所的组合行权/结算细节（基本不影响分析层） |
| 概率/信号 | `optionstratlib-analytics`：价格概率核、风险中性密度（从链提取）、Delta 中性分析 | 结合持仓量的流动性过滤规则（用我们的量额/持仓量列） |
| 历史波动率 | optionstratlib 波动率模块（constant/EWMA/GARCH + 不确定波动率边界） | 用我们分钟K线自算 realized vol 做交叉验证 |
| 回测 | `optionstratlib-backtest`；订单簿级成交参考 [IronCondor](https://github.com/joaquinbejar/IronCondor)（双成交模型 + Greek 归因 + 回放 bundle 可被 ChainView 渲染） | 中国手续费/滑点模型、涨跌停无法成交的处理（自研撮合过滤器） |
| 可视化 | `optionstratlib-visualization`（plotly）；终端 T 型报价用 [ChainView](https://github.com/joaquinbejar/ChainView)（需新增 CTP/Parquet provider） | provider adapter 一枚（中等工作量） |

**买方路线一句话**：optionstratlib（策略+定价+回测）+ volsurf（曲面择时）+ implied-vol（IV 底座），三者均为 MIT/Apache、纯 Rust、无行情源耦合，自补的只有"期权行情→链→IV"数据段与中国日历/费率。

### 3.2 卖方策略（备兑、覆盖性卖出、铁鹰等）

| 能力 | 用哪个库的什么模块 | 缺什么我们自补 |
|---|---|---|
| 卖方策略类型 | `optionstratlib-strategies`：**Iron Condor、Iron Butterfly、Covered Call（支持现货/期货腿）、Collar、Bull Call Ladder** 等 25+；`get_best_ratio`/`get_best_area` 策略优化器 | 中金所**组合保证金优惠**（跨式/宽跨式组合保证金的收取规则）——所有库都没有 |
| **保证金（核心缺口）** | 参考 optionstratlib `span.rs`（SPAN 风格）做框架与校验 | **自研交易所公式**：上期所/中金所期权保证金 = 权利金 + max(标的保证金系数部分 − 虚值额, 半额系数部分) 一族公式；商品期权各交易所参数不同、股指期权另含保证系数调整——全部自研，公式以交易所官网结算参数为准（本次调研未抓取官网公式页，落地前需核对） |
| 报价/做市参考 | `quoting-engine`（隐含波动率空间 AS 模型、gamma 局部偏斜、报价→Mass Quote 映射） | 中国 tick 表替换；做市义务/豁免规则不在任何库中 |
| 对冲 | `hedge-orchestrator`：精确 delta 对冲 + Whalley-Wilmott 不交易带 + vega 对冲成本效益选择 | 对冲标的换成国内期货；费率参数重标定 |
| 风险归因 | `pnl-explain`：二阶 Taylor 分解 Δ/Γ/ν/vanna/volga + 显式 unexplained_pnl | 快照持久化挂 Parquet（简单） |
| 压力测试 | `optionstratlib-simulation`：GBM/Heston/GARCH/跳跃/telegraph 路径 + 策略退出策略 | 极端行情（涨跌停锁死）场景自研 |
| 静态套利/质检 | 参考 [box-spread-arbitrage](https://github.com/PTR-25/box-spread-arbitrage) 的 box/conv-rev/垂直/蝶式扫描思路；volsurf `SurfaceDiagnostics` | 转换为国内行权价结构（等距挂档更规整，实现更容易） |
| 撮合级回测 | [IronCondor](https://github.com/joaquinbejar/IronCondor)（订单簿级成交、队列位置）+ [OrderBook-rs / option-chain-orderbook](https://github.com/joaquinbejar/OrderBook-rs)（待验证其国内适用性） | 中国撮合规则（连续竞价集合竞价、行权指派现金/实物） |

**卖方路线一句话**：策略类型与盈亏/优化用 optionstratlib 现成；**保证金是最大自研项**（交易所公式 + 组合优惠），做市与对冲用 tfrmma 架构移植；建议把"保证金计算器"作为独立模块先行，因为它同时服务回测与风控。

---

## 四、风险与坑

1. **许可证**
   - nautilus_trader 为 **LGPL-3.0**：若借鉴代码（而非仅学习设计），注意链接边界；纯 Rust 闭源工程建议进程隔离或重写。
   - `implied-vol`：crates.io 许可证字段为 "non-standard"（GitHub 识别为 NOASSERTION）——实为 **MIT + 第三方声明**（含 Jäckel "Let's be rational" 代码改编），商用前逐条核对 `LICENSE` 第三方 notices（本调研已打开该文件确认存在第三方段落，具体条款逐项核对待办）。
   - `black_scholes`（danielhstahl）：GitHub 仓库无 LICENSE 文件、crates.io 标 MIT——引用时留好 crates.io 快照证据。
   - **tqsdk-rs 无 LICENSE**：法律上默认保留所有权利，**不可直接使用**，只可读源码学思路。
   - `option-pricing`（oscar6echo）：crates.io 在售但仓库 URL 404（改名/删除，待验证）——无法溯源审计，建议不依赖。
   - `rust-options`：仓库指向占位账号 `yourusername`，**放弃**。
2. **依赖爆炸**：optionstratlib facade 默认启用全部 feature（含 plotly/async/schema），务必用 feature 门控或直接引组件 crate（README 明确每个组件默认仅 `clock`）；stochastic-rs 与 barter 同理。对照组：tfrmma 全家仅 6 个基础依赖、blackscholes/black76/implied-vol 近零依赖——高频路径优先轻库。
3. **维护停更**：`blackscholes`/`black76` crates.io 停在 2023（仓库 2025 有推送但未发版，发版计划待验证）；`black76-rust` 2★ 单人库；`gnsqd/surface`（2★，2025-11 新建）随时可能弃更；tfrmma 两库 6/16★ 单人维护——**建议对低星库"以源码 vendoring 方式引入并锁定提交"，不追浮动版本**。OptionStratLib/volsurf/implied-vol 近 1-3 个月均有发版或推送，停更风险低。
4. **数学口径**：optionstratlib 用 `rust_decimal`（高精度、较慢），blackscholes 用 f64 SIMD 快路径——两栈并行时同一链会出微小价差，回测与实盘须统一口径（建议：研究层用 Decimal，实时层用 f64，差异设容忍带）。
5. **中国市场特有缺口（任何被调研库均未覆盖，全部自研）**：
   - 交易所保证金公式（上期所/中金所等）与组合保证金优惠；
   - 期权涨跌停公式与"涨跌停无法成交"的回测约束；
   - 到期日/行权日历、行权指派（现金 vs 实物、商品期权行权后转期货持仓）；
   - T 型报价的盘口细节（我们现有 13 列不含买卖价，需扩展期权行情采集，可用 ctp-rs/ctp2rs 直连或 openctp TTS 模拟联调）；
   - 中金所 IO/MO/HO 与商品期权的合约乘数/最小变动价位/保证金系数参数表。
6. **数据模型提醒**：中国期权全为欧式（barter `OptionContract.exercise` 与 nautilus `exercise_style` 都支持 EUROPEAN，映射无障碍）；商品期权是期货期权 → 用 **Black-76**（black76/optionstratlib 均现成），股指期权 IO/MO/HO 是指数期权 → BSM+股息近似或期货合成；两类不要混用一个 pricer。
7. **搜索来源可靠性**：opengreeks/regit-svi 的部分信息来自搜索摘要，落地前需再打开仓库核对（已在表中标注）。

---

## 五、引用 URL 清单（本次实际打开/抓取）

**GitHub 仓库（API/README 全文级）**
- https://github.com/joaquinbejar/OptionStratLib （README 全文 + Cargo.toml + license 徽章）
- https://github.com/joaquinbejar/IronCondor （README 全文）
- https://github.com/joaquinbejar/ChainView （README 全文）
- https://github.com/joaquinbejar/OrderBook-rs （IronCondor README 引用，未单独打开，待验证）
- https://github.com/volsurf-rs/volsurf （README 全文）
- https://github.com/nakashima-hikaru/implied-vol （LICENSE 全文 + API）
- https://github.com/hayden4r4/blackscholes-rust （README 全文）
- https://github.com/hayden4r4/black76-rust （API）
- https://github.com/danielhstahl/black_scholes_rust （API）
- https://github.com/tfrmma/options-pricing-engine-rs （README 头部 + Cargo.toml 全文）
- https://github.com/tfrmma/options-market-making-engine-rs （README 全文）
- https://github.com/nkaz001/hftbacktest （README 全文）
- https://github.com/barter-rs/barter-rs （README 全文 + barter-instrument 源码 kind/mod.rs、kind/option.rs）
- https://github.com/nautechsystems/nautilus_trader （API + docs/concepts/options.md 全文）
- https://github.com/hummingbot/hummingbot （API：language=Python）
- https://github.com/avhz/RustQuant （API）
- https://github.com/rust-dd/stochastic-rs （README 全文）
- https://github.com/gnsqd/surface （README 全文）
- https://github.com/PTR-25/box-spread-arbitrage （README 头部）
- https://github.com/rn7s2/ctp-rs （API）
- https://github.com/Linfee/ctp-rs （API）
- https://github.com/pseudocodes/tqsdk-rs （API）
- https://github.com/org-regit-io/regit-svi 、https://github.com/org-regit-io/regit-blackscholes 、https://github.com/marketcalls/opengreeks 、https://github.com/siddharthqs/RustyQLib 、https://github.com/klemen-r/slv 、https://github.com/day01/quant-opts （GitHub API 搜索结果条目）
- https://github.com/openctp/openctp 、https://github.com/wangys96/SVI-Volatility-Surface-Calibration （来自搜索摘要，未打开全文）

**crates.io（API：版本/下载量/许可证/repository 字段）**
- https://crates.io/crates/optionstratlib 、https://crates.io/crates/implied-vol 、https://crates.io/crates/blackscholes 、https://crates.io/crates/black76 、https://crates.io/crates/black_scholes 、https://crates.io/crates/option-pricing 、https://crates.io/crates/rust-options 、https://crates.io/crates/volsurf 、https://crates.io/crates/regit-svi 、https://crates.io/crates/RustQuant 、https://crates.io/crates/stochastic-rs 、https://crates.io/crates/hftbacktest 、https://crates.io/crates/barter 、https://crates.io/crates/barter-data 、https://crates.io/crates/barter-execution 、https://crates.io/crates/ironcondor 、https://crates.io/crates/ctp2rs （来自搜索摘要）、https://crates.io/keywords/greeks 、https://crates.io/keywords/volatility-surface 、https://crates.io/keywords/option-pricing

**文档/官网**
- https://docs.rs/implied-vol （搜索摘要级）
- https://docs.rs/barter-instrument （打开过枚举页尝试，最终以 GitHub 源码为准）
- https://docs.rs/ctp_rust （搜索摘要级）
- https://hftbacktest.readthedocs.io （README 引用）
- https://nautilustrader.io （搜索摘要；期权细节以 https://github.com/nautechsystems/nautilus_trader/blob/develop/docs/concepts/options.md 打开全文为准）
- https://hummingbot.org （搜索摘要级）

---

*报告完 —— 子代理 Task 2-a，仅调研未改代码。*

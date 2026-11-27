# 宪法守卫 v1.1.0

**全局接管 cargo，让每一次编译都过同一部宪法。**

7 个中文模块合并为 1 个文件，全部承诺与行为一一对应，已通过端到端实测。

---

## 它做什么

```
cargo build / check / clippy / run / test / bench（含 b/c/t/r 别名）
        │
        ▼
┌─ 宪法体检 ─────────────────────────────┐
│ ① rustfmt --check     格式宪法         │
│ ② cargo clippy        20 条铁律 +     │
│    （项目自己的工具链）警告组 + 淘汰清单 │
│ ③ edition 落后提示                    │
└────────────┬──────────────────────────┘
             │
        通过 → 放行原命令（真编译真运行）
        违宪 → 拦截，退出码 1，原命令不执行
```

**设计原则：官方机制优先，守卫只做编排。** 工具链解析归 rustup（项目用 nightly 就真用 nightly）、语法迁移归 `cargo fix --edition`、lint 修复归 `cargo clippy --fix`、淘汰清单归 clippy 官方 `clippy.toml`。守卫本体零依赖（供应链攻击面为零，离线可编译）、固定 stable（执法者不跟浮动版）。

## 安装（三步）

```bash
cd 宪法守卫-v1.1.0
bash 工具/编译安装.sh     # 编译安装守卫 + 全局淘汰清单（自动锁定 stable 版本号）
bash 工具/全局接管.sh      # 接管全部 cargo 命令（独立目录，不碰 rustup 的 shim）
source ~/.bashrc          # 或重开终端
cargo constitutional-guard 自检   # 验证环境
```

在任何项目里跑一次 `cargo 宪法守卫 初始化`，生成 AGENTS.md / CLAUDE.md（AI 智能体开工前自动读取的规则文件，由宪法单一事实源生成）。

## 命令表

| 命令 | 作用 |
|---|---|
| `cargo build`（及 b/c/t/r/clippy/run/test/bench） | 自动拦截：先宪法体检，通过才放行 |
| `cargo 宪法守卫 修复` | 一键整改：格式化 → edition 迁移并升级 Cargo.toml → clippy --fix → 复检 |
| `cargo 宪法守卫 初始化` | 生成 AGENTS.md + CLAUDE.md（存在则跳过不覆盖） |
| `cargo 宪法守卫 体检` | 只体检，不执行原命令（pre-commit 钩子用的就是它） |
| `cargo 宪法守卫 自检` | 环境体检：cargo / rustup / clippy / rustfmt / 淘汰清单 / 接管状态 |
| `cargo 宪法守卫 规则` | 打印当前宪法全文（铁律 + 警告组 + 淘汰清单 + 逃生舱） |
| `cargo 宪法守卫 版本` | 版本号 |

未接管时把 `cargo 宪法守卫` 换成 `cargo constitutional-guard` 即可，一切功能相同。

## 三层防御（AI 智能体场景）

1. **事前引导**：AGENTS.md 告诉 AI 规则，写代码之前就知道；
2. **事中拦截**：每次编译自动体检，烂代码过不去；
3. **事后自愈**：AI 写了旧语法，`cargo 宪法守卫 修复` 用官方 `--fix` 全自动升级（实测把 `println!("{}", y)` 自动改成了 `println!("{y}"`）。

AI 想要机器可读输出：设 `CARGO_CONSTITUTION_JSON=1`，clippy 走 `--message-format=json`。

## 工具链策略（本版核心）

- **你的项目：浮动 nightly**。`rustup default nightly` 后，没 pin 工具链的项目全部走最新 nightly。nightly 偶尔有坏版本，回退一条命令：`rustup default stable`，或单项目回稳：项目根放 `rust-toolchain.toml` 写 `channel = "stable"`。
- **守卫本体：固定 stable**。编译安装脚本自动把版本号锁死在 `rust-toolchain.toml`（如 1.100.0），执法层永不跟浮动版。
- **宪法自动变严**：clippy 每版新增 lint（现共 800+ 条），项目升工具链后 `clippy::all` 自动覆盖新规则，零配置。新版淘汰的 API 由 `-D deprecated` + 全局淘汰清单自动跟上——**这就是"把淘汰语法全部禁止"的官方正解，不需要几百万条规则**。

## 测试与生产同严（明确策略）

**测试代码同样受全部铁律约束，不放宽。** 开发节奏快，测试通过即上生产，任何放宽都是漏洞。确需 panic 的用例（如 `#[should_panic]`）用 `#[expect(clippy::panic)]` 显式标注——豁免必须写在明处。

## 逃生舱（全部显式，看得见）

| 手段 | 范围 | 说明 |
|---|---|---|
| `#[allow(clippy::unwrap_used, reason = "理由")]` | 单点 | 宪法要求必须写 reason |
| `CONSTITUTION_OVERRIDE=1 cargo build` | 整次 | 红色警告放行 |
| 项目根放空文件 `宪法守卫.关闭` | 整项目 | 遗留项目救星，直接透传 |
| 项目自带 `clippy.toml` | 清单 | 守卫自动让位，尊重项目配置 |

另有兜底熔断：守卫自身若崩溃，退出码 125，接管脚本自动回退原生 cargo 并告警——**执法者死了，你的项目照常编译**。

## 已知限制（诚实清单）

- 接管脚本为 bash：支持 Linux / macOS / WSL。Windows 请在 Git Bash 下使用，或手动放置 wrapper。
- 体检在首次编译/代码变更时有开销（clippy 复用 cargo 增量缓存，重复构建几乎免费）；大项目首次约多 1.5~2 倍时间。
- `cargo doc` / `cargo install` 等不触发体检（doc 不产生 lint，install 编译的是外部项目）。
- 依赖管理命令（add/update/remove 等）直接透传，绝不干扰。
- 守卫自身豁免：本项目根带 `宪法守卫.关闭`。原因：CLI 工具的本职就是终端输出，与"生产禁打印"铁律天然冲突；unwrap/panic 类铁律守卫本体严格遵守（源码无 unwrap/expect/下标切片，已用全量铁律 clippy 实测验证）。

## 常见问题

**问：为什么不用 `~/.cargo/config.toml` 的 RUSTFLAGS 全局设 lint？**
答：RUSTFLAGS 会传给**所有依赖 crate**，第三方代码的 lint 会把你淹没。守卫把参数传给 `cargo clippy -- --`（只作用于你自己的 crate）。

**问：为什么不用官方 `[lints]` 表？**
答：它是正解，但每项目要声明 `lints.workspace = true`，管不了"全局一刀切"。守卫是它的零侵入补充：不改任何项目文件。

**问：lint 到底有多少条？**
答：clippy 约 800+ 条，rustc 内置约 400 条，合计 1200 左右。"几百万条"来自组合空间，不存在那么多 lint——数量不重要，`-D deprecated`（官方淘汰清单）+ `disallowed-*`（自选禁令）+ 版本升级自动变严，三件套已覆盖"淘汰语法全部禁止"。

**问：v1.0.1 升级到 v1.1.0 改了什么？**
答：修复 7 文件名循环错位（原项目无法编译）、补 `Path` 导入、clippy/别名拦截、非 Cargo 目录透传、指纹按项目存放、删无效缓存清理函数、版本号改读 CARGO_PKG_VERSION、删 `-A unknown_lints` 暗洞、nursery 降为警告；新增修复/初始化/体检/自检/规则命令、全局淘汰清单、AGENTS 生成、PATH 保真接管（不覆盖 rustup shim）、熔断回退、一键卸载、pre-commit 钩子、edition 自动升级、12 个单元测试。

## 文件清单

```
宪法守卫-v1.1.0/
├── main.rs              唯一源文件（7 模块内联 + 12 个单元测试，约 900 行）
├── Cargo.toml           零依赖
├── rust-toolchain.toml  守卫自身固定 stable（安装脚本自动锁版本号）
├── clippy.toml          全局淘汰清单（安装到 ~/.cargo/constitutional-guard/）
├── 宪法守卫.关闭          守卫自身豁免标记（说明见文件内）
├── README.md            本文件
└── 工具/
    ├── 编译安装.sh       编译 + 安装 + 锁版本 + 装全局清单（自动备份旧清单）
    ├── 全局接管.sh       PATH 保真接管（不碰 rustup shim）
    ├── 卸载接管.sh       一键还原
    └── 安装钩子.sh       git pre-commit（拦提交比拦编译更早）
```

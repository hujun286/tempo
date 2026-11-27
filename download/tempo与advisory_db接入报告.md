# tempo 与 advisory_db 接入报告

> **生成时间**：2026-11-27 21:52 北京时间
> **执行**：基础设施工程师（Task 4-c）
> **纪律**：GitHub PAT / Gitee token 仅出现在克隆命令里，克隆后立即 `remote set-url` 清除并复核（`git remote -v` 显示干净 URL）；本报告与所有落盘文件**零凭据**；全程**未 push** 任何远端。

---

## 任务1：tempo.git 盘点（GitHub）

**接入过程**：直连 `github.com:443` 超时（129s 后失败），带 PAT 克隆不可行；改走 ghfast.top 镜像探测发现 **tempo 是公开仓，匿名即可克隆**，PAT 未派上用场。克隆后 origin 已置为干净 URL `https://github.com/hujun286/tempo.git`。
**HEAD**：`1d6e57c`（2026-11-27 12:22 UTC = 北京时间 20:22，"Add files via upload"）
**规模**：6 个文件，工作树 22.2MB + .git 17MB ≈ **39MB**（< 100MB，按存储纪律**保留不删**）

### 盘点表

| 文件 | 大小 | 说明 |
|---|---|---|
| bars_20261125.parquet | 12.6MB | **13 列分钟K线**，8,586,131 行；**23,870 个合约**（期货+期权链，如 AP701C4800…）；trading_day=**20261125**；393 个分钟槽（日盘+夜盘全天） |
| 分钟k线落盘_v1_4.rs | 63KB | 分钟K线落盘流水线**源码 v1.4** |
| 配置_分钟K线.toml | 3.4KB | 落盘配置 |
| contracts_live.txt | 1.1MB | 实盘合约清单 |
| combined-backup-20261126_031954.tar.gz | 3.8MB | 打包备份 |
| h5i-db数据库查询 | 4.6MB | **ELF x86-64 可执行程序**（od 探测魔数 `177ELF`，非数据文件） |

### 列结构（tempo 13 列）

`bar_time`(timestamp 带时区) / `instrument_id`(string) / `trading_day`(string) / `open` `high` `low` `close`(double) / `volume`(uint64) / `turnover`(double) / `open_interest`(double) / **`volume_1m`(uint64)** / **`turnover_1m`(double)** / `is_gap`(bool)

### 与 zhipu2 参考口径对比（pyarrow 实测）

`zhipu2_容灾仓/bars_20261124_夜盘.parquet`：651,489 行，25,661 合约，trading_day=20261124，**实测 11 列**（缺 `volume_1m`/`turnover_1m`）。

→ tempo 的 20261125 全天数据与 zhipu2 手动上传的 20261124 夜盘数据**不同一天、非同源重复**，且列口径为完整 13 列。

### 结论

**tempo 值得留**：① 一份独有的 20261125 全天 13 列分钟K线；② v1.4 落盘源码与配置（对行情数据采集工程有直接参考价值）。已精选备份 `bars_20261125.parquet` + `分钟k线落盘_v1_4.rs` + `配置_分钟K线.toml` 至 `zhipu2_容灾仓/tempo上游备份_20261127/`（13MB，随 git 提交持久化）；克隆整体保留在 `tmpo上游/tempo`（39MB < 100MB，不删）。

---

## 任务2：RustSec advisory-db fork 接入 + 真实审计

**克隆**：gitee **匿名克隆成功** → `/home/z/my-project/advisory-db_fork/`（12MB；1035 个 crate advisory 目录；HEAD 2026-11-26 "Correct first patched release for RUSTSEC-2024-0362"）。
**cargo-audit**：0.23.0（官方 release musl 二进制）；`cargo audit --help` 确认参数：`-d, --db <DB>`（advisory database git repo path，默认 ~/.cargo/advisory-db），另用 `--stale` 允许跳过远端 fetch（沙盒直连 GitHub 不通，直接用 fork 本地库）。

### 审计1：ctp测试/md_probe_16ip（39 个依赖）

真实输出（`cargo audit --db /home/z/my-project/advisory-db_fork --stale`）：

```text
    Fetching advisory database from `https://github.com/RustSec/advisory-db.git`
      Loaded 1568 security advisories (from /home/z/my-project/advisory-db_fork)
    Updating crates.io index
    Scanning Cargo.lock for vulnerabilities (39 crate dependencies)
```

扫描后无任何告警输出 → **0 漏洞、0 警告，完全干净**。

### 审计2：ctp-workspace/行情数据采集（144 个依赖）

真实输出（告警仅 1 条，全录，未满 10 条）：

```text
      Loaded 1568 security advisories (from /home/z/my-project/advisory-db_fork)
    Updating crates.io index
    Scanning Cargo.lock for vulnerabilities (144 crate dependencies)
Crate:     chrono
Version:   0.4.45
Warning:   unmaintained
Title:     chrono is unmaintained
Date:      2026-09-14
ID:        RUSTSEC-2026-0543
URL:       https://rustsec.org/advisories/RUSTSEC-2026-0543
Solution:  No fixed upgrade is available!

warning: 1 allowed warning found
```

→ **0 漏洞，1 条 unmaintained 警告**（chrono 0.4.45；官方暂无 fixed upgrade，建议等 0.4.x 后续版本再升）。parquet/arrow 系依赖全部未触发告警。

---

## 任务3：难装二进制持久化

| 工具 | 结果 | 版本 | 安装方式 |
|---|---|---|---|
| sccache | ✅ | 0.20.1 | ghfast.top 镜像直拉官方 release musl 二进制（11MB 压缩包） |
| cargo-deny | ✅ | 0.22.1 | 同上（4.9MB） |
| cargo-audit | ✅ | 0.23.0 | 同上（6.2MB；资产名 `cargo-audit-x86_64-unknown-linux-musl-v0.23.0.tgz`） |
| rust-script | ✅ | 0.36.0 | `cargo install --locked`（rsproxy 源码编译，1m24s） |
| cargo-binstall | ❌ | — | rsproxy 安装脚本 404；其本体发布也在 GitHub（直连不通）。它只是安装器，上述四工具已全部就位，不影响功能 |

**持久化**：四个二进制已复制到 `tools_工具箱/bin/`（cargo-audit 15M、sccache 22M、rust-script 3.4M、cargo-deny 8.7M；musl 静态链接无 glibc 依赖）；`tools_工具箱/README.md` 已增补「bin/ 二进制清单」一节（名字/版本/用途/上次安装时间 2026-11-27 21:51），**随本次 git 提交入库，沙盒回收后免编译直接用**。

---

## 任务4：gitee.com/zz237/tempo.git 存在性

- 匿名 `git ls-remote` → gitee 要求输入用户名（= 该仓库对匿名不可见）；
- 携带用户 token（输出已脱敏）→ HTTP **403 "The token is not authorized to access this project"**（仓库不存在，或为私有且该 token 无权访问）。

**结论**：当前**无法访问、无法确认 refs**；**用户需在 gitee 手动建仓（或设为可匿名访问 / 给 token 授权）后，我们才能向 gitee 推送 tempo 备份**。

---

## 踩坑记录

1. `github.com:443` 直连全部超时（129s），PAT-URL 方案失效；ghfast.top / gh-proxy.com 探测 200 可用，mirror.ghproxy.com / ghproxy.net 探测 000 不通。
2. ghfast.top + PAT-in-URL 返回 403（镜像不接受 userinfo 形式），而**匿名克隆直接成功**——tempo 本就是公开仓，PAT 完全多余。
3. `rsproxy.cn/binstall/install.sh` 返回 404（脚本已下线/路径变更）→ binstall 路线放弃，改镜像直拉 release 二进制；cargo-audit 资产名**含版本号**（最初猜无版本名 404，通过 `expanded_assets` 页面解析出真实资产名后成功）。
4. 清华 pypi 镜像下载 pyarrow wheel 403；**阿里云 pypi 可用**，pyarrow 装入 /tmp/venv 后成功读两份 parquet schema。
5. rsproxy sparse index 路径规则为 `/index/ru/st/rust-script`（首2字/次2字/全名），写成 `st-` 拿不到 repository 字段。
6. 在行情数据采集目录跑 `cargo audit` 触发 rustup 按 `rust-toolchain.toml` 自动安装 nightly（约 1-2 分钟，正常行为）。
7. 全量 `git status`（扫 target/ 等海量未跟踪文件）导致一次命令 180s 超时；改 `--untracked-files=no` 秒回，后续 git 操作只 add 指定路径。
8. zhipu2 的 bars_20261124_夜盘.parquet 实测 11 列，与"13 列口径"（= tempo 20261125 文件）不一致——两份数据入库/合并前需做列对齐（补 volume_1m/turnover_1m）。
9. tempo 的 `h5i-db数据库查询` 是 ELF 可执行程序而非数据/文档，来源与用途待用户确认。

---

## 一页结论

- **tempo**：公开仓已克隆盘点，内容 = 20261125 全天 13 列分钟K线（8.59M 行 / 2.39 万合约）+ v1.4 落盘源码，与 zhipu2 已有的 1124 夜盘数据**不同源不重复**，精选件已入容灾仓，整仓 39MB 保留。
- **审计发现数**：md_probe_16ip = **0**；行情数据采集 = **1 条**（chrono 0.4.45 unmaintained，非漏洞），两工程均 **0 漏洞**。
- **工具安装**：cargo-audit 0.23.0 ✅ / sccache 0.20.1 ✅ / rust-script 0.36.0 ✅ / cargo-deny 0.22.1 ✅（cargo-binstall ❌ 404，不影响）。
- **gitee tempo.git**：匿名不可见 + token 403，**需用户建仓/授权后才能推备份**。
- 报告路径：`/home/z/my-project/download/tempo与advisory_db接入报告.md`

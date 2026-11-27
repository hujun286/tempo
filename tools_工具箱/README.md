# 工具箱 —— 常用软件一次装好，不再反复下载

> 由"每次都要重新找/重新装"的痛点催生。所有脚本均在本仓库实测过（2026-11-27 凌晨夜盘实测同环境）。

## 目录
- `一键安装Rust工具链.sh` —— rustup + 稳定版工具链 + cargo-binstall + cargo-deny，全部走 rsproxy 国内镜像（实测 ~9.4MB/s，直连只有 ~6KB/s）
- `cargo国内镜像配置.toml` —— 放到 `~/.cargo/config.toml`（或工程 `.cargo/config.toml`），注册表/crate下载全走 rsproxy
- `env_rust沙盒环境变量.sh` —— 沙盒/容器内 Rust 环境变量模板（RUSTUP_HOME/CARGO_HOME/LIBCLANG_PATH）
- `环境自举_libclangvenv/` —— 免 root 的 libclang 18.1.1（pip 装 ctp2rs bindgen 必需；bindgen 找不到 libclang 会编译失败）
- `CTP动态库版本对照.md` —— 实机 .so 导出符号与 ctp2rs feature 的对应关系（2026-11-27 实测结论）

## 一分钟装好（实盘机/Linux）
```bash
bash tools_工具箱/一键安装Rust工具链.sh
cp tools_工具箱/cargo国内镜像配置.toml ~/.cargo/config.toml
```

## bin/ 二进制清单

_更新时间（北京时间）：2026-11-27 21:51_。来源：cargo-binstall 不可用（rsproxy 安装脚本 404、GitHub 直连 443 超时），sccache / cargo-deny / cargo-audit 经 ghfast.top 镜像直拉 GitHub 官方 release 预编译 musl 二进制（免编译），rust-script 走 rsproxy 源码编译（1m24s）。

| 二进制 | 版本 | 用途 | 安装方式 |
|---|---|---|---|
| `cargo-audit` | cargo-audit 0.23.0 | RustSec 依赖漏洞审计：`cargo audit --db <本地advisory-db> --stale` | 镜像直拉 release |
| `sccache` | sccache 0.20.1 | Rust 编译缓存，加速重复构建（`RUSTC_WRAPPER=sccache`） | 镜像直拉 release |
| `rust-script` | rust-script 0.36.0 | 单文件 .rs 脚本直接运行 | cargo install（rsproxy 编译 84s） |
| `cargo-deny` | cargo-deny 0.22.1 | 依赖 license/漏洞/来源合规检查 | 镜像直拉 release |

备注：
- cargo-binstall 本体未装成（rsproxy 的 binstall 安装脚本 404），它只是安装器，上述四工具均已就位，不影响使用。
- bin/ 下二进制为官方 musl 静态编译，无 glibc 依赖问题；沙盒回收后从本目录直接取用，免重新下载/编译。
- 上次审计基线（2026-11-27）：md_probe_16ip 0 告警；行情数据采集 1 条 unmaintained 告警（chrono 0.4.45，RUSTSEC-2026-0543）。

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

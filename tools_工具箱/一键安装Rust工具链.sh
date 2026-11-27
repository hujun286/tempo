#!/usr/bin/env bash
# 一键安装 Rust 工具链（国内镜像版）——行情数据采集项目的全部编译依赖
# 用法: bash 一键安装Rust工具链.sh
# 实测: 2026-11-26/27 凌晨夜盘实测环境（rsproxy 镜像 ~9.4MB/s；直连 static.rust-lang.org 仅 ~6KB/s）
set -euo pipefail

echo "== [1/4] rustup（rsproxy 镜像）=="
export RUSTUP_DIST_SERVER=https://rsproxy.cn
export RUSTUP_UPDATE_ROOT=https://rsproxy.cn/rustup
if ! command -v rustup >/dev/null 2>&1; then
    curl --proto '=https' --tlsv1.2 -sSf https://rsproxy.cn/rustup-init.sh | sh -s -- -y --profile minimal
    source "$HOME/.cargo/env"
else
    echo "    rustup 已存在，跳过"
fi

echo "== [2/4] 默认工具链（stable，minimal profile）=="
rustup default stable

echo "== [3/4] cargo-binstall（预编译二进制安装器，免编译装 cargo 工具）=="
if ! command -v cargo-binstall >/dev/null 2>&1; then
    curl -L --proto '=https' --tlsv1.2 -sSf https://rsproxy.cn/binstall/install.sh 2>/dev/null \
      | sh || {
        echo "    rsproxy 无 binstall 脚本，退回官方源…"
        curl -L --proto '=https' --tlsv1.2 -sS https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | sh
    }
else
    echo "    cargo-binstall 已存在，跳过"
fi

echo "== [4/4] cargo-deny（依赖安全审计，RustSec advisory-db）=="
cargo binstall -y cargo-deny 2>/dev/null || cargo install cargo-deny --locked

echo
echo "✅ 安装完成。记得把 cargo国内镜像配置.toml 复制到 ~/.cargo/config.toml"
rustc --version && cargo --version

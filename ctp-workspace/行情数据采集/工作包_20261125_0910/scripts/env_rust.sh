# 沙盒 Rust 环境变量（每次新 shell 先 source 本文件）
export RUSTUP_HOME=/home/z/my-project/.rustup
export CARGO_HOME=/home/z/my-project/.cargo
export PATH="$CARGO_HOME/bin:$PATH"
# rsproxy 镜像（沙盒直连 static.rust-lang.org 只有 ~6KB/s，rsproxy ~9.4MB/s，实测 2026-11-25）
export RUSTUP_DIST_SERVER=https://rsproxy.cn
export RUSTUP_UPDATE_ROOT=https://rsproxy.cn/rustup

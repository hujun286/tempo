# 沙盒 Rust 环境变量（每次新 shell 先 source 本文件）
export LANG=C.UTF-8   # 中文兜底，防乱码
export RUSTUP_HOME=/home/z/my-project/.rustup
export CARGO_HOME=/home/z/my-project/.cargo
export PATH="$CARGO_HOME/bin:$PATH"
# rsproxy 镜像（沙盒直连 static.rust-lang.org 只有 ~6KB/s，rsproxy ~9.4MB/s，实测 2026-11-25）
export RUSTUP_DIST_SERVER=https://rsproxy.cn
export RUSTUP_UPDATE_ROOT=https://rsproxy.cn/rustup
# libclang（ctp2rs build.rs 的 bindgen 需要；来自 pip 包 libclang-18.1.1，免 root）
export LIBCLANG_PATH=/home/z/my-project/工具链venv/libclang-18.1.1.data/platlib/clang/native

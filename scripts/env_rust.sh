# 沙盒 Rust 环境变量（每次新 shell 先 source 本文件）
export LANG=C.UTF-8   # 中文兜底，防乱码
# 20261127 改用非隐藏目录：沙盒回收会重置到 git 跟踪态，隐藏目录/.cargo 与 gitignore 目录都会丢失；
# 非隐藏目录实测也同被回收，但配合 zhipu2 容灾仓可秒级重建（工具链安装 2 分钟内）
export RUSTUP_HOME=/home/z/my-project/rustup_home
export CARGO_HOME=/home/z/my-project/cargo_home
export PATH="$CARGO_HOME/bin:$PATH"
# rsproxy 镜像（沙盒直连 static.rust-lang.org 只有 ~6KB/s，rsproxy ~9.4MB/s，实测 2026-11-25）
export RUSTUP_DIST_SERVER=https://rsproxy.cn
export RUSTUP_UPDATE_ROOT=https://rsproxy.cn/rustup
# libclang（ctp2rs build.rs 的 bindgen 需要；来源 zhipu2 容灾仓——20261127 沙盒回收 venv 丢失后从 Gitee 拉回，容灾首次实战生效）
export LIBCLANG_PATH=/home/z/my-project/zhipu2_容灾仓/刚需二进制_CTP与编译

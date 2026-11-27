# 沙盒 Rust 环境变量（每次新 shell 先 source 本文件）
export LANG=C.UTF-8   # 中文兜底，防乱码
# 20261127 改用非隐藏目录：沙盒回收会重置到 git 跟踪态，隐藏目录/.cargo 与 gitignore 目录都会丢失；
# 非隐藏目录实测也同被回收，但配合 zhipu2 容灾仓可秒级重建（工具链安装 2 分钟内）
export RUSTUP_HOME=/home/z/my-project/rustup_home
export CARGO_HOME=/home/z/my-project/cargo_home
export PATH="$CARGO_HOME/bin:$PATH"
# 回收兜底（20261128 凌晨实证）：cargo_home（rustup 垫片+registry 缓存）会丢而 rustup_home（工具链本体）偶有幸存；
# 垫片丢失时直接把工具链本體 bin 加入 PATH，免重装 rustup（2 分钟→0 分钟）
if [ ! -x "$CARGO_HOME/bin/cargo" ] && [ -x "$RUSTUP_HOME/toolchains/stable-x86_64-unknown-linux-gnu/bin/cargo" ]; then
  export PATH="$RUSTUP_HOME/toolchains/stable-x86_64-unknown-linux-gnu/bin:$PATH"
  # 无垫片时 cargo 直接定位工具链，禁用 rustup 自动安装逻辑
  export RUSTUP_TOOLCHAIN=stable-x86_64-unknown-linux-gnu
fi
# rsproxy 镜像（沙盒直连 static.rust-lang.org 只有 ~6KB/s，rsproxy ~9.4MB/s，实测 2026-11-25）
export RUSTUP_DIST_SERVER=https://rsproxy.cn
export RUSTUP_UPDATE_ROOT=https://rsproxy.cn/rustup
# libclang（ctp2rs build.rs 的 bindgen 需要；来源 zhipu2 容灾仓——20261127 沙盒回收 venv 丢失后从 Gitee 拉回，容灾首次实战生效）
export LIBCLANG_PATH=/home/z/my-project/zhipu2_容灾仓/刚需二进制_CTP与编译
# sccache 编译缓存（20261127 夜盘定案：不强制 cargo workspace 合并，两工程共享 sccache+registry 消除重复编译；
# 二进制在 tools_工具箱/bin（git 跟踪，回收免重装），缓存目录用非隐藏路径）
export RUSTC_WRAPPER=/home/z/my-project/tools_工具箱/bin/sccache
export SCCACHE_DIR=/home/z/my-project/sccache_cache
# GitHub 国内加速前缀（ghfast.top，20261127 夜盘实测可用；克隆时拼在原始 URL 前）
export GH_FAST_PREFIX=https://ghfast.top/

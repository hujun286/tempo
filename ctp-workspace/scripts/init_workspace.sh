#!/usr/bin/env bash
set -euo pipefail

WS="/home/xx/projects/ctp-workspace"
OLD1="/mnt/oldroot/home/cc/文档/option_monitor"
OLD2="/mnt/oldroot/home/cc/文档/trend_analyzer_ctp_test_国泰君安仿真程序化外部接入测试全过程记录_从Rust依赖误判到openctp_ctp真实交易库确认_20261021"

mkdir -p "$WS"/{crates/{common/src,market-data/src/testsupport,trading/src/testsupport},app/src/bin,config,data/{contracts,flow/{md,td}},logs_运行日志,docs,scripts,targets,.cargo-home,old}

# 复制旧目录（幂等：已存在则跳过）到 old/ 下作参考
[ -d "$WS/old/option_monitor" ] || cp -a "$OLD1" "$WS/old/option_monitor"
[ -d "$WS/old/trend_analyzer" ] || cp -a "$OLD2" "$WS/old/trend_analyzer"

# 统一 CARGO_HOME（Cargo 依赖缓存目录）
export CARGO_HOME="$WS/.cargo-home"
mkdir -p "$CARGO_HOME"

# 每个项目独立 target（编译输出目录）
export CARGO_TARGET_DIR="$WS/targets/new-project"

# sccache（编译缓存工具）/ mold（链接器）自检
command -v sccache >/dev/null && echo "sccache: ok"
command -v mold    >/dev/null && echo "mold: ok"

echo "workspace ready: $WS"

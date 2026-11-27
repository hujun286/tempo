#!/usr/bin/env bash
# 打包工作快照：把当前工作产物打成时间戳工作包 + git commit
# 用法：bash /home/z/my-project/scripts/打包工作快照.sh "本次汇报的一句话说明"
# ★清单 v2（D035）：含 eng 改造产物（9 模块源码/Cargo.toml/justfile/deny.toml/CI/scripts）
# rust-script 版（打包工作快照.rs）功能同此，锁竞争时用本脚本兜底。
set -e
cd /home/z/my-project
TS=$(date +%Y%m%d_%H%M)
PKG="download/工作包_${TS}.tar.gz"
tar --exclude='download/工作包_*' --exclude='download/github_tempo' -czf "$PKG" \
    download/交接报告_CTP分钟K线_第3棒.md \
    download/决策日志_推理链.md \
    download/交付完整性清单.md \
    download/交付说明_上机步骤.md \
    download/交接报告_第2棒_原始.md \
    download/v1_4_清warning补丁_*.patch \
    scripts/ \
    eng/src/bin/分钟k线落盘_v1_4 \
    eng/Cargo.toml \
    eng/Cargo.lock \
    eng/justfile \
    eng/deny.toml \
    eng/clippy.toml \
    eng/.github \
    eng/scripts \
    2>/dev/null || true
git add -A
git commit --allow-empty -m "快照 ${TS}：${1:-常规汇报} （含工作包 $(basename "$PKG")）" -q
echo "工作包: $PKG"
echo "清单核对（文件数）："
tar -tzf "$PKG" | wc -l
git log --oneline -2

#!/usr/bin/env bash
# 打包工作快照：把当前工作产物打成时间戳工作包 + git commit
# 用法：bash /home/z/my-project/scripts/打包工作快照.sh "本次汇报的一句话说明"
set -e
cd /home/z/my-project
TS=$(date +%Y%m%d_%H%M)
PKG="download/工作包_${TS}.tar.gz"
# 打包：两份报告 + 决策日志 + scripts + cargo配置副本（排除旧工作包本身）
tar --exclude='download/工作包_*' -czf "$PKG" \
    download/交接报告_CTP分钟K线_第3棒.md \
    download/决策日志_推理链.md \
    download/交接报告_第2棒_原始.md \
    scripts/ \
    2>/dev/null || true
git add -A
git commit --allow-empty -m "快照 ${TS}：${1:-常规汇报} （含工作包 $(basename "$PKG")）" -q
echo "工作包: $PKG"
git log --oneline -3

#!/usr/bin/env bash
# 推送主仓库到 Gitee zhipu.git（第3棒）。
# 凭据只在本条命令行临时拼 URL（不写进任何文件/提交内容）；凭据文件权限 600 且已 gitignore。
set -euo pipefail
cd /home/z/my-project
[ -f .gitee_推送凭据 ] || { echo "缺少 .gitee_推送凭据（格式 user:pass）"; exit 1; }
CRED=$(tr -d '\n' < .gitee_推送凭据)
git add -A
git commit -q -m "定时/手动推送快照 $(date '+%F %T')" 2>/dev/null || true
git push "https://${CRED}@gitee.com/zz237/zhipu.git" main:main
echo "推送完成"

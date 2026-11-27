#!/usr/bin/env bash
# 推送主仓库到 Gitee zhipu.git（第3棒）。
# 凭据只在本条命令行临时拼 URL（不写进任何文件/提交内容）；凭据文件权限 600 且已 gitignore。
set -euo pipefail
cd /home/z/my-project
# 凭据三级兜底（绝不打印）：1) 凭据文件 2) .git/config 带凭据 remote（实测回收幸存）3) 报错退出
PASS=""
if [ -f .gitee_推送凭据 ]; then
  CRED=$(tr -d '\n' < .gitee_推送凭据)
  PASS="${CRED##*:}"   # 凭据文件格式 user:pass，Gitee 接口用 oauth2:pass 形式（用户名固定 oauth2）
else
  URL=$(git remote get-url origin 2>/dev/null || true)
  P="${URL##*oauth2:}"
  case "$P" in *@*) PASS="${P%%@*}";; esac
fi
[ -n "$PASS" ] || { echo "缺少 .gitee_推送凭据 且 origin remote 不带凭据"; exit 1; }
git add -A
git commit -q -m "定时/手动推送快照 $(date '+%F %T')" 2>/dev/null || true
if git push "https://oauth2:${PASS}@gitee.com/zz237/zhipu.git" main:main 2>/dev/null; then
  echo "推送完成(gitee)"
else
  # 兜底：Gitee 拒收(容量锁/等GC)时转推 GitHub tempo 备用分支，本地不丢
  echo "gitee 拒收，转推 tempo/zhipu_main 备用"
  git push tempo main:zhipu_main
  echo "推送完成(tempo备用)"
fi

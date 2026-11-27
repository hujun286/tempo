#!/usr/bin/env bash
# 宪法守卫 · 安装 git pre-commit 钩子（拦提交比拦编译更早）
# 在目标项目根目录运行本脚本
set -euo pipefail

if [ ! -d .git ]; then
  echo "当前目录不是 git 仓库根（未找到 .git）"
  exit 1
fi

if ! command -v cargo-constitutional-guard >/dev/null 2>&1; then
  echo "未找到守卫本体，请先运行 宪法守卫/工具/编译安装.sh"
  exit 1
fi

cat > .git/hooks/pre-commit <<'EOF'
#!/usr/bin/env bash
# 宪法守卫 · pre-commit（删除本文件即卸载）
if command -v cargo-constitutional-guard >/dev/null 2>&1; then
  exec cargo-constitutional-guard 体检
fi
EOF
chmod +x .git/hooks/pre-commit
echo "已安装 .git/hooks/pre-commit：提交前自动执行宪法体检，违宪直接挡下提交。"

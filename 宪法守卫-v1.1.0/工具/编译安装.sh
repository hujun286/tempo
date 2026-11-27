#!/usr/bin/env bash
# 宪法守卫 · 编译安装（守卫本体固定 stable，与被检项目的 nightly 互不影响）
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v cargo >/dev/null 2>&1; then
  echo "未找到 cargo，请先安装 rustup：https://rustup.rs"
  exit 1
fi

# 把守卫自身的工具链锁定为「当前 stable 的具体版本号」，避免浮动
STABLE="$(rustc +stable --version 2>/dev/null | awk '{print $2}')"
if [ -n "$STABLE" ]; then
cat > rust-toolchain.toml <<EOF
[toolchain]
channel = "$STABLE"
components = ["clippy", "rustfmt"]
EOF
  echo "[锁定] 守卫自身已固定 stable $STABLE"
else
  echo "[跳过] 无法确定 stable 版本号，保持 channel = \"stable\""
fi

echo "[编译] cargo install --path .（零依赖，离线可编译）"
cargo install --path . --force

# 安装全局淘汰清单（已存在则先备份，绝不静默覆盖用户改动）
CONF_DIR="$HOME/.cargo/constitutional-guard"
mkdir -p "$CONF_DIR"
if [ -f "$CONF_DIR/clippy.toml" ] && [ ! -f "$CONF_DIR/clippy.toml.备份" ]; then
  cp "$CONF_DIR/clippy.toml" "$CONF_DIR/clippy.toml.备份"
  echo "[备份] 原有清单 → clippy.toml.备份"
fi
cp clippy.toml "$CONF_DIR/clippy.toml"
echo "[清单] 全局淘汰清单 → $CONF_DIR/clippy.toml"

echo
echo "安装完成。下一步（可选但推荐）："
echo "  bash 工具/全局接管.sh    # 接管全部 cargo 命令（删除即还原）"
echo "  cargo constitutional-guard 自检"

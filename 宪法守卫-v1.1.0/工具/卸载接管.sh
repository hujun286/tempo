#!/usr/bin/env bash
# 宪法守卫 · 卸载全局接管（一键还原，不留痕迹）
set -euo pipefail

rm -rf "$HOME/.cargo/宪法守卫"

for f in "$HOME/.bashrc" "$HOME/.zshrc"; do
  if [ -f "$f" ]; then
    sed -i '\#宪法守卫/bin#d' "$f"
    # 顺带清掉成对出现过的注释行
    sed -i '/^# 宪法守卫 · 全局接管$/d' "$f"
  fi
done

echo "已还原：wrapper 已删除、shell 配置已清理。"
echo "如需彻底移除守卫本体：cargo uninstall cargo-constitutional-guard"
echo "如需移除全局淘汰清单：rm -rf ~/.cargo/constitutional-guard"

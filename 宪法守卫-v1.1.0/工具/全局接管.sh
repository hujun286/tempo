#!/usr/bin/env bash
# 宪法守卫 · 全局接管
# 关键设计：wrapper 装在独立目录 ~/.cargo/宪法守卫/bin 并置于 PATH 最前，
# 绝不覆盖 rustup 的 shim（~/.cargo/bin/cargo）——
# 透传时仍由 rustup shim 解析项目级 rust-toolchain.toml，
# 所以项目用 nightly 就真的用 nightly，工具链解析保真。
# 卸载：运行 工具/卸载接管.sh，或直接删掉 ~/.cargo/宪法守卫 目录。
set -euo pipefail

if ! command -v cargo-constitutional-guard >/dev/null 2>&1; then
  echo "未找到守卫本体，请先运行 工具/编译安装.sh"
  exit 1
fi

GUARD="$(command -v cargo-constitutional-guard)"

# 解析「真实 cargo」：跳过守卫自己的 wrapper 目录，拿到 rustup shim
REAL_CARGO="$(echo "$PATH" | tr ':' '\n' | grep -v '宪法守卫' | while IFS= read -r dir; do
  if [ -x "$dir/cargo" ]; then echo "$dir/cargo"; break; fi
done)"
if [ -z "$REAL_CARGO" ] || [ ! -x "$REAL_CARGO" ]; then
  echo "无法定位真实 cargo（rustup shim），接管中止"
  exit 1
fi

BIN_DIR="$HOME/.cargo/宪法守卫/bin"
mkdir -p "$BIN_DIR"

cat > "$BIN_DIR/cargo" <<EOF
#!/usr/bin/env bash
# 宪法守卫 · 全局接管 wrapper —— 运行 工具/卸载接管.sh 即完全还原
export CARGO_CONSTITUTION_REAL="$REAL_CARGO"
rc=0
"$GUARD" "\$@" || rc=\$?
if [ "\$rc" -eq 125 ]; then
  # 125 = 守卫自身故障（熔断），自动回退原生 cargo，绝不断你的编译
  printf '\033[31;1m宪法守卫自身故障，已自动回退原生 cargo\033[0m\n' >&2
  exec "\$CARGO_CONSTITUTION_REAL" "\$@"
fi
exit "\$rc"
EOF
chmod +x "$BIN_DIR/cargo"

# 写入 shell 配置（幂等，支持 bash 与 zsh）
RCFILE="$HOME/.bashrc"
if [ -n "${ZSH_VERSION:-}" ] || [ ! -f "$HOME/.bashrc" ] && [ -f "$HOME/.zshrc" ]; then
  RCFILE="$HOME/.zshrc"
fi
touch "$RCFILE"
if ! grep -q '宪法守卫/bin' "$RCFILE"; then
  printf '\n# 宪法守卫 · 全局接管\nexport PATH="$HOME/.cargo/宪法守卫/bin:$PATH"\n' >> "$RCFILE"
  echo "[写入] PATH 已加入 $RCFILE"
else
  echo "[就绪] $RCFILE 已包含接管 PATH"
fi

echo
echo "接管完成。执行以下命令使其立即生效（或重开终端）："
echo "  source $RCFILE"
echo "验证：cargo 自检    （然后随便找个项目 cargo build 试试）"

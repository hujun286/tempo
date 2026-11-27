#!/usr/bin/env python3
"""依赖安全审计（轻量版，零安装）：Cargo.lock 全依赖 对照 RustSec advisory-db 快照。
输出：命中 RUSTSEC 通告的依赖清单（含通告文件路径，人工核对版本区间）。
G3 完整跑通用 cargo-deny（本脚本是其离线粗筛替代，沙盒后台进程被回收装不上 cargo-deny）。
"""
import re
from pathlib import Path

LOCK = Path("/home/z/my-project/ctp-workspace/行情数据采集/src/Cargo.lock")
DB = Path("/home/z/my-project/ctp-workspace/advisory-db/crates")
OUT = Path("/home/z/my-project/download/G3_依赖安全审计_结果.md")

# 1. 解析 Cargo.lock（[[package]] name/version）
text = LOCK.read_text(encoding="utf-8")
pkgs = re.findall(r'\[\[package\]\]\nname = "([^"]+)"\nversion = "([^"]+)"', text)
print(f"Cargo.lock 依赖总数: {len(pkgs)}")

# 2. 扫 advisory-db 快照：crates/<name>/RUSTSEC-*.md
命中 = []
for name, ver in pkgs:
    d = DB / name
    if not d.is_dir():
        continue
    for adv in d.glob("RUSTSEC-*.md"):
        头 = adv.read_text(encoding="utf-8", errors="ignore")[:1200]
        标题 = re.search(r"^# (.+)$", 头, re.M)
        命中.append((name, ver, adv.name, 标题.group(1) if 标题 else ""))

# 3. 输出
lines = [
    "# G3 依赖安全审计（离线粗筛，第3棒 2026-11-26）",
    "",
    f"- 审计对象：行情数据采集/src/Cargo.lock（{len(pkgs)} 个依赖，含传递依赖）",
    "- 对照库：RustSec advisory-db 快照（1035 crate 目录，commit b9b8a3c，2026-11-26）",
    "- 判定口径：依赖 crate 名在 advisory-db 中有 RUSTSEC 通告即列出（不管版本是否命中区间）；",
    "  是否实际受影响需点开通告文件核对 patched/unaffected 版本区间。",
    "",
]
if 命中:
    lines.append(f"## 命中通告的依赖（{len(命中)} 条）")
    lines.append("")
    lines.append("| crate | lock版本 | 通告 | 标题 |")
    lines.append("|---|---|---|---|")
    for name, ver, adv, 标题 in sorted(命中):
        lines.append(f"| {name} | {ver} | {adv} | {标题[:60]} |")
else:
    lines.append("## 结果：全部依赖无 RUSTSEC 通告 ✅")

lines += ["", "## 完整审计（G3 正式）", "",
          "上机执行（deny.toml 已改相对路径 ./advisory-db，快照随工作区）：",
          "```bash",
          "cargo install cargo-deny --locked   # rsproxy 镜像下约 3 分钟",
          "cd 行情数据采集/src",
          "cargo deny check advisories bans sources",
          "```"]
OUT.write_text("\n".join(lines), encoding="utf-8")
print(f"结果已存 {OUT}")
print(f"命中 {len(命中)} 条" if 命中 else "无命中")

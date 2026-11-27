#!/usr/bin/env python3
"""校验教学版文件相对主工程源文件只增加了注释，未改动任何代码。
规则：对源文件每个「代码行」（跳过纯注释行），在教学版中按顺序寻找匹配行：
  - 完全相等，或
  - 以该行（截掉行尾 // 注释之前的部分）为前缀，且剩余部分是空白+// 开头的注释。
"""
import sys, os

SRC = "/home/z/my-project/ctp-workspace/行情数据采集/src"
DST = "/home/z/my-project/教学版_小白学Rust/分钟K线教学版/src"

def is_pure_comment(line):
    s = line.strip()
    return s.startswith("//") or s.startswith("/*") or s.startswith("*")

def code_part(line):
    i = line.find("//")
    if i == 0:
        return ""
    return line[:i] if i >= 0 else line

def check(name):
    src = open(os.path.join(SRC, name), encoding="utf-8").read().splitlines()
    dst = open(os.path.join(DST, name), encoding="utf-8").read().splitlines()
    j = 0
    problems = []
    for ln, L in enumerate(src, 1):
        if is_pure_comment(L):
            continue
        base = code_part(L).rstrip()
        if not base.strip():
            continue
        # 在 dst 中从 j 开始向后找
        found = -1
        for k in range(j, len(dst)):
            T = dst[k].rstrip()
            if T == base or (T.startswith(base) and dst[k][len(base):].strip().startswith("//")):
                found = k
                break
        if found < 0:
            problems.append(f"  未匹配 源第{ln}行: {L.strip()[:90]}")
        else:
            j = found + 1
    return problems

def main():
    files = sys.argv[1:] or ["stats.rs","events.rs","spi.rs","contracts.rs","writer.rs","aggregate.rs","config.rs","session.rs","merge.rs","main.rs"]
    ok = True
    for f in files:
        p = check(f)
        if p:
            ok = False
            print(f"[FAIL] {f}")
            print("\n".join(p))
        else:
            print(f"[OK]   {f} 代码逐行一致（仅多注释）")
    sys.exit(0 if ok else 1)

main()

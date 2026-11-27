# -*- coding: utf-8 -*-
# 静态关卡：检查 Rust 源码里 println!/format!/warn!/info!/debug!/error!/trace!/eprintln!/panic!/assert!
# 的格式串占位符个数 是否 == 传入参数个数。避免 E0061「占位符多/少参数」类错误。
# 沙箱无 cargo，这脚本就是我们的“编译器替身”之一——专抓这一类肉眼爱漏的错。
import sys, re

MACROS = ["println","format","print","eprintln","ewarn","warn","info","debug",
          "error","trace","panic","todo","unimplemented","write","writeln","assert","assert_eq"]

def strip_string_literal(s, i):
    # s[i] 是引号，返回(字符串内容, 结束下标) 处理转义与 raw 串
    if s.startswith("r#", i):
        hashes = 0; j = i+1
        while j < len(s) and s[j] == '#': hashes += 1; j += 1
        # s[j]== '"'
        end = s.find('"' + '#'*hashes, j+1)
        return s[j+1:end], end+1+hashes
    if s[i] == 'r' and i+1 < len(s) and s[i+1] == '"':
        end = s.find('"', i+2)
        return s[i+2:end], end+1
    # 普通串 "..."
    j = i+1; out = []
    while j < len(s):
        c = s[j]
        if c == '\\': out.append(s[j:j+2]); j += 2; continue
        if c == '"': return ("".join(out), j+1)
        out.append(c); j += 1
    return ("".join(out), j)

def count_placeholders(fmt):
    n = 0; i = 0
    while i < len(fmt):
        c = fmt[i]
        if c == '{':
            if i+1 < len(fmt) and fmt[i+1] == '{': i += 2; continue   # 转义 {{
            # 一个占位符，吃到匹配的 }
            j = fmt.find('}', i+1)
            if j == -1: break
            n += 1; i = j+1; continue
        if c == '}' and i+1 < len(fmt) and fmt[i+1] == '}': i += 2; continue  # 转义 }}
        i += 1
    return n

def split_top_args(argstr):
    # 按顶层逗号切分参数（忽略括号/方括号/花括号/字符串内的逗号）
    args = []; depth = 0; i = 0; cur = []; has_content = False
    while i < len(argstr):
        c = argstr[i]
        if c in '([{':
            depth += 1; cur.append(c); i += 1; continue
        if c in ')]}':
            depth -= 1; cur.append(c); i += 1; continue
        if c == '"' or (c=='r' and i+1<len(argstr) and argstr[i+1] in '"#'):
            lit, ni = strip_string_literal(argstr, i)
            cur.append('"'+lit+'"'); i = ni; has_content=True; continue
        if c == ',' and depth == 0:
            if "".join(cur).strip():
                args.append("".join(cur).strip())
            cur = []; i += 1; continue
        cur.append(c); has_content=True; i += 1
    if "".join(cur).strip():
        args.append("".join(cur).strip())
    if not has_content and not args:
        return []
    return args

def check(path):
    src = open(path, encoding="utf-8").read()
    problems = []
    for m in re.finditer(r'([A-Za-z_\u4e00-\u9fff][\w\u4e00-\u9fff]*)\s*!', src):
        name = m.group(1)
        if name not in MACROS: continue
        k = m.end()
        while k < len(src) and src[k] in ' \t\r\n': k += 1
        if k >= len(src) or src[k] != '(': continue
        depth = 0; i = k; inner_start = k+1; inner = None
        while i < len(src):
            c = src[i]
            if c == '"' or (c=='r' and i+1<len(src) and src[i+1] in '"#'):
                _, ni = strip_string_literal(src, i); i = ni; continue
            if c in '([{': depth += 1
            elif c in ')]}':
                depth -= 1
                if depth == 0:
                    inner = src[inner_start:i]; break
            i += 1
        if inner is None: continue
        stripped = inner.lstrip()
        if not stripped or stripped[0] not in '"r': continue
        fmt_start = inner.find(stripped[0])
        fmt, fmt_end = strip_string_literal(inner, fmt_start)  # fmt_end=右引号之后
        ph = count_placeholders(fmt)
        rest = inner[fmt_end:]
        # 格式串之后若无逗号 -> 0 个附加参数
        rest = rest.strip()
        if rest == '':
            nargs = 0
        elif rest.startswith(','):
            tail = rest[1:].strip()
            nargs = 0 if tail == '' else len(split_top_args(tail))
        else:
            nargs = 0
        if name in ("write","writeln"):
            continue  # 首参是 writer，单独口径，跳过以免误报
        line = src[:m.start()].count('\n')+1
        if ph != nargs:
            problems.append((line, name, ph, nargs, fmt[:60]))
    return problems

ok = True
for p in sys.argv[1:]:
    probs = check(p)
    if probs:
        ok = False
        print(f"❌ {p}: {len(probs)} 处占位符/参数不匹配")
        for line,name,ph,na,fmt in probs:
            print(f"   行{line} {name}!  占位符={ph} 参数={na}  fmt=«{fmt}»")
    else:
        print(f"✅ {p}: 全部格式串占位符与参数匹配")
sys.exit(1 if not ok else 0)

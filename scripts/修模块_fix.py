#!/usr/bin/env python3
"""拆分后的模块修复（use 注入 / 可见性 / derive / 字段 pub(crate)），配合 拆模块_split.py 使用。"""
import re, os

OUT = "/home/z/my-project/eng/src/bin/分钟k线落盘_v1_4"

def rd(name): return open(os.path.join(OUT, name)).read()
def wr(name, t): open(os.path.join(OUT, name), "w").write(t)

def add_fields_within_struct(text, struct_name, field_names):
    m = re.search(r"(?:#\[[^\]]+\]\n)*(?:pub\(crate\) )?struct " + re.escape(struct_name) + r" \{", text)
    assert m, struct_name
    start = m.end()
    depth, i = 1, start
    while depth > 0:
        if text[i] == "{": depth += 1
        elif text[i] == "}": depth -= 1
        i += 1
    block = text[start:i-1]
    for f in field_names:
        block = re.sub(r"^    " + re.escape(f) + r":", "    pub(crate) " + f + ":", block, flags=re.M)
    return text[:start] + block + text[i-1:]

# ── config.rs ──
c = rd("config.rs")
c = c.replace("use std::path::{Path, PathBuf};", "use std::path::{Path, PathBuf};\nuse std::time::Duration;", 1)
c = add_fields_within_struct(c, "运行配置", ["分片落盘秒","bar队列容量","分片目录_rel","成品目录_rel","归档压缩"])
wr("config.rs", c)

# ── stats.rs ──
s = rd("stats.rs")
if "#[derive" not in s:
    s = s.replace("pub(crate) struct MemberStats {", "#[derive(Default, Clone)]\npub(crate) struct MemberStats {", 1)
s = add_fields_within_struct(s, "MemberStats", ["connected","logged_in","login_fail","disconnects","ready_rounds","last_disconnect_reason","last_disconnect_at","ticks","last_tick_at","tick_out_of_order","rebuilds"])
wr("stats.rs", s)

# ── aggregate.rs ──
a = rd("aggregate.rs")
a = add_fields_within_struct(a, "MinuteBar", ["instrument_id","bar_time_ms","trading_day","open","high","low","close","volume","turnover","open_interest","volume_1m","turnover_1m","is_gap"])
a = add_fields_within_struct(a, "Bucket", ["bar_ms","open","high","low","close","volume","turnover","open_interest","trading_day"])
wr("aggregate.rs", a)

# ── events.rs ──
e = rd("events.rs")
e = e.replace("use ctp2rs::v1alpha1::*;", "use ctp2rs::ffi::WrapToString;\nuse ctp2rs::v1alpha1::*;", 1)
e = add_fields_within_struct(e, "TickEvent", ["member_idx","instrument","last_price","volume","turnover","open_interest","trading_day","update_time","update_millisec"])
wr("events.rs", e)

# ── spi.rs ──
p = rd("spi.rs")
p = p.replace("use ctp2rs::v1alpha1::*;", "use ctp2rs::ffi::{AssignFromString, WrapToString};\nuse ctp2rs::v1alpha1::*;", 1)
p = add_fields_within_struct(p, "MemberSpi", ["member_idx","tx"])
wr("spi.rs", p)

# ── writer.rs ──
w = rd("writer.rs")
# 段内重复的 use std::io 已由拆分头带入：删除第二次出现
lines = w.split("\n")
seen = False
out = []
for ln in lines:
    if ln.strip() == "use std::io::{BufWriter, Write};":
        if seen: continue
        seen = True
    out.append(ln)
w = "\n".join(out)
w = w.replace("use std::io::{BufWriter, Write};", "use std::io::{BufWriter, Write};\nuse std::path::Path;\nuse std::sync::mpsc;", 1)
wr("writer.rs", w)

# ── session.rs ──
se = rd("session.rs")
# 1) 删除尾部孤立 derive（原 MemberStats 的 derive 被切进 session 段尾）
se = se.replace("#[derive(Default, Clone)]\n\npub(crate) struct MemberStats", "pub(crate) struct MemberStats", 1)
se = re.sub(r"#\[derive\(Default, Clone\)\]\n(?=\n|$)", "", se)  # 兜底删孤立 derive
# 2) 段内重复 use 清理（若头/段都有）
wr("session.rs", se)

# ── main.rs ──
m = rd("main.rs")
anchor = "mod aggregate;"
new_use = """use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use chrono::Local;
use rust_ctp_test::config::{Secrets, WORKSPACE_ROOT};
use crate::aggregate::{run_aggregator, MinuteBar};
use crate::config::{MD_DYNLIB_REL, RECONNECT_MAX_MS, REPORT_INTERVAL_SEC, 运行配置, 分片落盘秒};
use crate::contracts::{load_contracts_csv, shard_contracts};
use crate::events::TickEvent;
use crate::merge::合并一日;
use crate::session::spawn_member;
use crate::stats::{锁或恢复, MemberStats};
use crate::writer::run_writer;

"""
if anchor not in m.split("\n", 60)[-1]:
    # use 段缺失（head_doc 只到 L54）
    if "use crate::session::spawn_member" not in m:
        m = m.replace(anchor, new_use + anchor, 1)
        wr("main.rs", m)
        print("main.rs use 注入")
else:
    # use 段已存在（head_doc 含原 use）→ 替换为模块导入
    old_use = re.search(r"use std::collections::HashMap;[\s\S]*?use serde::\{Deserialize, Serialize\};\n", m)
    if old_use:
        m = m[:old_use.start()] + new_use + m[old_use.end():]
        wr("main.rs", m)
        print("main.rs use 替换")

# ── merge.rs：无额外修复（头已带 use），校验一次 ──
mg = rd("merge.rs")
assert mg.count("mod 测试") == 1, f"merge.rs 测试 mod 数 {mg.count('mod 测试')}"
print("模块修复完成")

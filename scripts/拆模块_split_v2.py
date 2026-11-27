#!/usr/bin/env python3
"""v1_4 单文件拆模块 v2（本轮重建版）：按既有分区注释切割，行为零变化。
与上一棒 scripts/拆模块_split.py 同一套路，边界行号按本轮 fmt 后文件重推。
切割后：src/bin/分钟k线落盘_v1_4/main.rs + 9 个子模块。测试按语义分发到各模块。"""
import re, os

SRC = "/tmp/v14_本轮单文件.rs"
OUT = "/home/z/my-project/eng/src/bin/分钟k线落盘_v1_4"

lines = open(SRC).read().splitlines(keepends=True)
N = len(lines)

def seg(a, b):  # 1-based inclusive
    return "".join(lines[a-1:b])

# ── 校验边界行（防错位）──
checks = {
    73: "// ============================================================",
    223: "// ============================================================",
    273: "// ============================================================",
    321: "// ============================================================",
    398: "// ============================================================",
    834: "#[derive(Default, Clone)]",
    851: "// ============================================================",
    897: "fn 锁或恢复",
    1135: "// ============================================================",
    1238: "// ============================================================",
    1603: "// ============================================================",
    1875: "// ============================================================",
    1879: "#[cfg(test)]",
}
for ln, expect in checks.items():
    actual = lines[ln-1].rstrip()
    assert actual.startswith(expect[:20]), f"L{ln} 期望 {expect[:20]} 实际 {actual}"

assert lines[900].startswith("fn floor_minute"), repr(lines[900][:30])
assert lines[834].startswith("struct MemberStats"), repr(lines[834][:30])

doc_head = seg(1, 49)          # 文档注释 + #![deny] 内属性（不含 use 区）
cfg_seg = seg(73, 221)         # 常量 + 运行配置
contracts_seg = seg(223, 271)
events_seg = seg(273, 319)
spi_seg = seg(321, 396)
session_seg = seg(398, 833)    # ApiHolder + 登录订阅 + 方案B状态机 + 单调性
stats_seg = seg(834, 850)      # MemberStats
lock_fn = seg(897, 899)        # 锁或恢复
agg_a = seg(851, 896)          # 聚合段头 + MinuteBar + Bucket
agg_b = seg(900, 1133)         # floor_minute 起到聚合段尾
writer_seg = seg(1135, 1236)
merge_seg = seg(1238, 1601)    # 合并 + 归档 + 补缺口 + 写日线parquet
main_seg = seg(1603, 1873)
test_seg = seg(1875, N)        # 测试模块全文（后面按测试名分发）

# ── 测试分发：按 fn 名把测试块切出来 ──
def _extract_block(body, start):
    """从 start（{ 的索引）brace counting，返回闭合行的结束索引"""
    depth = 0
    i = start
    started = False
    while i < len(body):
        ch = body[i]
        if ch == "{":
            depth += 1
            started = True
        elif ch == "}":
            depth -= 1
            if started and depth == 0:
                j = body.find("\n", i)
                return j + 1
        i += 1
    raise AssertionError("brace 未闭合")

def extract_fn(text, names):
    """提取指定 fn 名的完整块（含前导 #[test]），返回 (拼接文本, 剩余文本)"""
    out = []
    rest = text
    for name in names:
        pat = re.compile(r"    #\[test\]\n    fn " + re.escape(name) + r"\(\) \{", re.M)
        m = pat.search(rest)
        assert m, f"找不到测试 fn {name}"
        end = _extract_block(rest, m.end() - 1)
        out.append(rest[m.start():end])
        rest = rest[: m.start()] + rest[end :]
    return "\n".join(out), rest

test_body = test_seg.split("mod 测试 {", 1)[1].rsplit("}", 1)[0]

merge_tests, t1 = extract_fn(test_body, ["增量首根等于累计", "增量后续是相邻差"])
merge_tests2, t2 = extract_fn(t1, ["补缺口在中间插空bar且累计单调", "无缺口不插bar", "单bar不补"])
agg_tests, t3 = extract_fn(t2, ["floor分钟对齐"])
sess_tests, t4 = extract_fn(t3, ["首个tick不报回拨", "空时间戳跳过校验", "正常递增不报回拨", "秒级回拨被检测",
                                   "毫秒级回拨被检测", "新交易日不误报回退", "回退到更早交易日被检测", "不同合约互不干扰"])
merge_tests3, t5 = extract_fn(t4, ["重复时间戳不补", "恰好隔2分钟补1根", "跨大间隔全补齐且单调"])
merge_tests4, t6 = extract_fn(t5, ["归档压缩往返内容一致", "归档拒绝今日文件"])
cfg_tests, t7 = extract_fn(t6, ["方案b守护常量下限"])
assert "造bar" in t7 and "校验状态" in t7
rest_helpers = t7  # 造bar + 校验状态 helpers
# 造bar helper → merge；校验状态 helper → session
def _extract_helper(body, sig_prefix):
    m = re.search(r"    fn " + sig_prefix, body)
    assert m, f"找不到 helper {sig_prefix}"
    brace = body.find("{", m.end())
    end = _extract_block(body, brace)
    s = m.start()
    pre = body[:s]
    pre_lines = pre.split("\n")
    while pre_lines and pre_lines[-1].strip().startswith("///"):
        s -= len(pre_lines[-1]) + 1
        pre_lines.pop()
    return body[s:end]

造bar_txt = _extract_helper(rest_helpers, "造bar\\(inst: &str")
校验状态_txt = _extract_helper(rest_helpers, "校验状态\\(\\)")
残留 = rest_helpers.replace(造bar_txt, "").replace(校验状态_txt, "")
assert not re.search(r"fn |assert", 残留.strip()), f"测试模块有未分发内容: {残留[:300]}"

# ── 模块可见性：给跨模块 item 加 pub(crate) ──
def pub_crate(seg_text, names):
    for n in names:
        pat = re.compile(r"^(" + re.escape(n) + r"\b)", re.M)
        seg_text = pat.sub(r"pub(crate) \1", seg_text)
    return seg_text

def pub_method(seg_text, names):
    for n in names:
        seg_text = re.sub(r"^    fn (" + re.escape(n) + r")\(", r"    pub(crate) fn \1(", seg_text, flags=re.M)
    return seg_text

cfg_seg = pub_crate(cfg_seg, ["const MD_DYNLIB_REL", "const SUBSCRIBE_BATCH", "const SUBSCRIBE_DELAY_MS",
    "const REPORT_INTERVAL_SEC", "const RECONNECT_BASE_MS", "const RECONNECT_MAX_MS", "const LOGIN_SETTLE_MS",
    "const CONNECT_SETTLE_MS", "const CONNECT_TIMEOUT_MS", "const DISCONNECT_REBUILD_MS", "const LOGIN_FAIL_REBUILD",
    "const 回拨告警节流", "const 分片落盘秒", "const MINUTE_MS", "const 默认_分片落盘秒", "const 默认_BAR队列容量",
    "const 默认_分片目录", "const 默认_成品目录", "const 配置_文件名", "struct 运行配置"])
cfg_seg = pub_method(cfg_seg, ["默认", "载入"])
contracts_seg = pub_crate(contracts_seg, ["struct Contract", "fn load_contracts_csv", "fn shard_contracts"])
events_seg = pub_crate(events_seg, ["enum MemberEvent", "struct TickEvent", "fn info_pair"])
spi_seg = pub_crate(spi_seg, ["struct MemberSpi"])
session_seg = pub_crate(session_seg, ["struct ApiHolder", "fn spawn_member", "fn sleep_or_stop"])
stats_seg = pub_crate(stats_seg, ["struct MemberStats"])
lock_fn = lock_fn.replace("fn 锁或恢复", "pub(crate) fn 锁或恢复", 1)
agg_all = agg_a + "\n" + agg_b
agg_all = pub_crate(agg_all, ["struct MinuteBar", "fn run_aggregator"])
agg_all = re.sub(r"^fn ms_to_date", "pub(crate) fn ms_to_date", agg_all, flags=re.M)
writer_seg = pub_crate(writer_seg, ["fn run_writer"])
merge_seg = pub_crate(merge_seg, ["fn 合并一日", "fn 归档非今日分片"])
main_seg = pub_crate(main_seg, ["fn ctrlc_handler"])

# ── 组装模块文件 ──
os.makedirs(OUT, exist_ok=True)

def w(name, text):
    open(os.path.join(OUT, name), "w").write(text)
    print(f"  写出 {name} ({len(text.splitlines())} 行)")

w("config.rs", f"""//! 运行配置与全局常量（★v1.5 拆模块：行为零变化，只是搬家）。
use std::path::{{Path, PathBuf}};
use std::time::{{Duration}};
use rust_ctp_test::config::WORKSPACE_ROOT;

{cfg_seg}

#[cfg(test)]
mod 测试 {{
    use super::*;

{cfg_tests}}}
""")

w("contracts.rs", f"""//! 合约清单加载与 member 分片。
use std::collections::HashMap;
use std::path::Path;

{contracts_seg}
""")

w("events.rs", f"""//! member 事件 / tick 统一结构。
use ctp2rs::v1alpha1::*;

{events_seg}
""")

w("spi.rs", f"""//! 行情 SPI：CTP 回调 → 事件（照抄 v1.1，一字未改）。
use std::sync::mpsc;
use ctp2rs::v1alpha1::*;
use crate::events::{{info_pair, MemberEvent}};

{spi_seg}
""")

w("session.rs", f"""//! CTP 会话：ApiHolder / 登录订阅 / ★v1.5 方案B 事件驱动重连状态机 / 🔴3 tick 单调性校验。
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::atomic::{{AtomicBool, Ordering}};
use std::sync::{{Arc, Mutex}};
use std::time::{{Duration, Instant}};
use ctp2rs::ffi::{{AssignFromString, WrapToString}};
use ctp2rs::v1alpha1::*;
use crate::config::{{
    CONNECT_SETTLE_MS, CONNECT_TIMEOUT_MS, DISCONNECT_REBUILD_MS, LOGIN_FAIL_REBUILD,
    LOGIN_SETTLE_MS, MD_DYNLIB_REL, RECONNECT_BASE_MS, RECONNECT_MAX_MS, SUBSCRIBE_BATCH, SUBSCRIBE_DELAY_MS,
    回拨告警节流,
}};
use crate::events::{{MemberEvent, TickEvent}};
use crate::spi::MemberSpi;
use crate::stats::{{锁或恢复, MemberStats}};

{session_seg}

#[cfg(test)]
mod 测试 {{
    use super::*;

{校验状态_txt}
{sess_tests}}}
""")

w("stats.rs", f"""//! member 运行统计 + 锁中毒恢复工具。
use std::sync::{{Mutex, MutexGuard}};
use std::time::Instant;

{stats_seg}

{lock_fn}
""")

w("aggregate.rs", f"""//! 聚合：tick → 分钟K线（收盘即送落盘，不攒整天）。
use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::atomic::{{AtomicBool, Ordering}};
use std::sync::Arc;
use std::time::{{Duration, Instant}};
use chrono::{{Local, LocalResult, TimeZone}};
use serde::{{Deserialize, Serialize}};
use crate::config::{{MINUTE_MS, REPORT_INTERVAL_SEC, 分片落盘秒}};
use crate::events::TickEvent;
use crate::stats::锁或恢复;

{agg_all}

#[cfg(test)]
mod 测试 {{
    use super::*;

{agg_tests}}}
""")

w("writer.rs", f"""//! 落盘线程：逐根 bar 追加 jsonl + 定时 fsync。
use std::io::{{BufWriter, Write}};
use std::path::PathBuf;
use std::time::{{Duration, Instant}};
use crate::aggregate::{{ms_to_date, MinuteBar}};

{writer_seg}
""")

w("merge.rs", f"""//! 合并：jsonl 分片 → 排序 → 算增量 → 补缺口 → 13 列 Parquet（含 ★v1.5 归档压缩）。
use std::collections::HashMap;
use std::io::BufReader;
use std::path::Path;
use std::time::Duration;
use crate::aggregate::MinuteBar;
use crate::config::MINUTE_MS;

{merge_seg}

#[cfg(test)]
mod 测试 {{
    use super::*;

{造bar_txt}
{merge_tests}
{merge_tests2}
{merge_tests3}
{merge_tests4}}}
""")

main_doc = doc_head.replace("#![deny(unsafe_code)]", "//! ★v1.5 拆模块版：单文件 → main + 9 模块，行为零变化（拆分说明见决策日志）。\n#![deny(unsafe_code)]", 1)
w("main.rs", f"""{main_doc}

use std::collections::HashMap;
use std::path::{{Path, PathBuf}};
use std::sync::{{Arc, Mutex}};
use std::sync::atomic::{{AtomicBool, Ordering}};
use std::sync::mpsc;
use std::time::{{Duration, Instant}};
use chrono::Local;
use rust_ctp_test::config::{{Secrets, WORKSPACE_ROOT}};

mod aggregate;
mod config;
mod contracts;
mod events;
mod merge;
mod session;
mod spi;
mod stats;
mod writer;

{main_seg}
""")

print("拆分完成，原文件行数", N)

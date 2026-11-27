#!/usr/bin/env python3
"""T4 端到端合并链路实测（离线，不连 CTP）：
1) 构造合成分片 jsonl（含：跨分钟缺口、精确重复行、双合约）
2) 调 market_data_collector 合并模式（同实机同二进制）
3) pyarrow 独立读回 parquet：13 列 schema / 去重 / 增量 / 补缺口 全校验
预期：IF2612 4 根（1 根 gap）+ IM2612 1 根 = 5 行；去重后无重复。
"""
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone

VENV_PY = "/home/z/my-project/工具链venv/bin/python"
BIN = "/home/z/my-project/ctp-workspace/行情数据采集/target/debug/market_data_collector"
WS = "/tmp/测试工作区_T4"
DATE = "20261126"

def ms(yyyy, mo, d, h, mi):
    return int(datetime(yyyy, mo, d, h, mi, tzinfo=timezone.utc).timestamp() * 1000)

def bar(inst, t, vol, to, day=DATE, gap=False):
    return {
        "instrument_id": inst, "bar_time_ms": t, "trading_day": day,
        "open": 3900.0, "high": 3910.0, "low": 3895.0, "close": 3905.0,
        "volume": vol, "turnover": to, "open_interest": 12345.0,
        "volume_1m": 0, "turnover_1m": 0.0, "is_gap": gap,
    }

def main():
    t0, t1, t2, t3 = ms(2026, 11, 26, 9, 30), ms(2026, 11, 26, 9, 31), ms(2026, 11, 26, 9, 32), ms(2026, 11, 26, 9, 33)
    lines = [
        bar("IF2612", t0, 100, 1000.0),
        bar("IF2612", t1, 250, 2600.0),
        # t2 缺失 → 合并应补 1 根 gap bar（09:32，累计量沿用前根 250）
        bar("IF2612", t3, 400, 4600.0),
        bar("IM2612", t0, 50, 800.0),
        bar("IF2612", t0, 100, 1000.0),  # 精确重复行 → 去重
    ]
    分片 = os.path.join(WS, "data_数据", "分钟分片")
    成品 = os.path.join(WS, "data_数据", "分钟K线")
    os.makedirs(分片, exist_ok=True)
    with open(os.path.join(分片, f"bars_{DATE}.jsonl"), "w") as f:
        for b in lines:
            f.write(json.dumps(b, ensure_ascii=False) + "\n")
    print(f"[T4] 合成 5 行（4 唯一 + 1 重复，含 1 分钟缺口）→ {分片}")

    env = dict(os.environ, CTP_WORKSPACE_ROOT=WS, RUST_LOG="info")
    r = subprocess.run([BIN, "合并", DATE], env=env, capture_output=True, text=True, timeout=120)
    tail = (r.stdout + r.stderr).strip().splitlines()[-3:]
    print("[T4] 第一次合并退出码:", r.returncode, "|", " / ".join(tail))
    assert r.returncode == 0, "合并失败"

    # 第二次重复合并：同日合并应读回旧成品 → 幂等（仍 5 行，无重复无去重丢数）
    r2 = subprocess.run([BIN, "合并", DATE], env=env, capture_output=True, text=True, timeout=120)
    print("[T4] 第二次合并退出码:", r2.returncode)
    assert r2.returncode == 0, "重复合并失败"

    code = f'''
import pyarrow.parquet as pq
import datetime as dt
t = pq.read_table("{成品}/bars_{DATE}.parquet")
print("列名:", t.column_names)
print("列数:", len(t.column_names), "行数:", t.num_rows)
d = t.to_pydict()
ms = [int(v.timestamp() * 1000) for v in d["bar_time"]]
rows = sorted(zip(d["instrument_id"], ms, d["volume"], d["volume_1m"], d["turnover_1m"], d["is_gap"]))
for r_ in rows: print(r_)
assert len(t.column_names) == 13, "列数不是13"
assert t.num_rows == 5, f"行数{{t.num_rows}}!=5（同日合并后应仍为5）"
m = {{(a,b): (v,v1,to1,g) for a,b,v,v1,to1,g in rows}}
assert m[("IF2612",{t0})] == (100, 100, 1000.0, False), "首根增量=累计"
assert m[("IF2612",{t1})] == (250, 150, 1600.0, False), "增量=相邻差"
assert m[("IF2612",{t2})][3] is True and m[("IF2612",{t2})][0] == 250, "gap补根:累计沿用前根"
assert m[("IF2612",{t2})][1] == 0, "gap增量=0"
assert m[("IF2612",{t3})] == (400, 150, 2000.0, False), "增量跨gap=400-250"
assert m[("IM2612",{t0})] == (50, 50, 800.0, False)
assert len(rows) == len(set((a,b) for a,b,*_ in rows)), "仍有重复"
print("T4 端到端合并链路 + 同日合并幂等 ✅ 全部断言通过")
'''
    subprocess.run([VENV_PY, "-c", code], check=True)

if __name__ == "__main__":
    shutil.rmtree(WS, ignore_errors=True)
    main()

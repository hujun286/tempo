# parquet 样本深度检验 v2（对齐源码 schema：成品首列 bar_time=Timestamp(ms)）
# 回答"JSONL 600M → Parquet 13M 是否合理"
import json, os, random
import pyarrow.parquet as pq

FILES = [
    "/home/z/my-project/upload/github_tempo/bars_20261125.parquet",
    "/home/z/my-project/upload/bars_20261124_夜盘.parquet",
]

# MinuteBar serde_json 行格式（分片 jsonl 的真实行格式，13 键固定顺序）
FIELDS = ["instrument_id","bar_time_ms","trading_day","open","high","low","close",
          "volume","turnover","open_interest","volume_1m","turnover_1m","is_gap"]

def f64(v):
    fv = float(v)
    if fv == int(fv) and abs(fv) < 1e15:
        return f"{int(fv)}.0"
    return repr(fv)

def serde_json_line(row):
    parts = []
    for f in FIELDS:
        v = row.get(f)
        if f in ("instrument_id","trading_day"):
            parts.append(f'"{f}":"{v}"')
        elif f in ("bar_time_ms","volume","volume_1m"):
            parts.append(f'"{f}":{int(v)}')
        elif f == "is_gap":
            parts.append(f'"{f}":{"true" if v else "false"}')
        else:
            parts.append(f'"{f}":{f64(v)}')
    return "{" + ",".join(parts) + "}"

for path in FILES:
    print("=" * 74)
    size = os.path.getsize(path)
    pf = pq.ParquetFile(path)
    md = pf.metadata
    print(f"文件: {os.path.basename(path)}   磁盘大小 {size:,}B ({size/1e6:.1f}MB)")
    print(f"行数={md.num_rows:,}  行组={md.num_row_groups}  列数={md.num_columns}")
    names = [pf.schema_arrow.field(i).name for i in range(md.num_columns)]
    types = [str(pf.schema_arrow.field(i).type) for i in range(md.num_columns)]
    for i,(n,t) in enumerate(zip(names,types)):
        print(f"  [{i:>2}] {n:<16} {t}")
    tbl = pf.read()
    # is_gap 统计
    gap_true = real = 0
    if "is_gap" in names:
        gv = tbl.column("is_gap").to_pylist()
        gap_true = sum(1 for x in gv if x)
        real = md.num_rows - gap_true
        print(f"is_gap=true(补齐空分钟)={gap_true:,}  真实bar={real:,}  补齐占比={gap_true/md.num_rows*100:.1f}%")
    # 合约/时间
    n_inst = len(set(tbl.column("instrument_id").to_pylist()))
    print(f"合约数={n_inst:,}")
    bt = tbl.column("bar_time").cast("int64").to_pylist() if "bar_time" in names else []
    import datetime
    if bt:
        lo, hi = min(bt), max(bt)
        utc = lambda ms: datetime.datetime.fromtimestamp(ms/1000, datetime.timezone.utc).strftime("%m-%d %H:%M")
        bj  = lambda ms: datetime.datetime.fromtimestamp(ms/1000, datetime.timezone(datetime.timedelta(hours=8))).strftime("%m-%d %H:%M")
        print(f"bar_time 范围(UTC内码): {utc(lo)} ~ {utc(hi)}   |   北京时间: {bj(lo)} ~ {bj(hi)}")
    # jsonl 预测：只用真实行（is_gap=false），分片里没有补齐行
    # 11 列旧文件缺 volume_1m/turnover_1m：jsonl 行仍会带这两个键（MinuteBar serde 全字段），默认 0/0.0
    if "is_gap" in names and "bar_time" in names:
        random.seed(7)
        real_idx = [i for i in random.sample(range(md.num_rows), min(md.num_rows,3000)) if not tbl.column("is_gap")[i].as_py()][:1500]
        tot = 0
        for i in real_idx:
            row = {"instrument_id": tbl.column("instrument_id")[i].as_py(),
                   "bar_time_ms": tbl.column("bar_time").cast("int64")[i].as_py(),
                   "trading_day": tbl.column("trading_day")[i].as_py()}
            for c in ["open","high","low","close","turnover","open_interest","turnover_1m"]:
                row[c] = tbl.column(c)[i].as_py() if c in names else 0.0
            for c in ["volume","volume_1m"]:
                row[c] = (tbl.column(c)[i].as_py() or 0) if c in names else 0
            row["is_gap"] = False
            tot += len(serde_json_line(row).encode())
        if real_idx:
            avg = tot/len(real_idx)
            pred = real*avg/1e6
            print(f"jsonl 行预测: {avg:.0f} B/行 × 真实bar {real:,} ≈ {pred:.0f} MB")
            print(f"对照: 用户实测 20261125.jsonl=397MB / 20261124.jsonl=181MB")
    # 列压缩统计（第 0 行组）+ 全文件汇总
    print("第0行组列压缩（未压缩→压缩，压缩比）:")
    su = sc = 0
    for col in range(md.num_columns):
        c = md.row_group(0).column(col)
        su += c.total_uncompressed_size; sc += c.total_compressed_size
        print(f"  {c.path_in_schema:<16} {c.total_uncompressed_size:>9,} → {c.total_compressed_size:>9,}  ({c.total_uncompressed_size/max(c.total_compressed_size,1):.1f}:1)  codec={c.compression}")
    print(f"第0行组合计: {su:,} → {sc:,} ({su/max(sc,1):.1f}:1)；整文件 {size:,}B ÷ {md.num_rows:,}行 = {size/md.num_rows:.2f} B/行")
print("=" * 74)
print("要点: 成品行数=真实bar+补齐空分钟(4000+合约×全天分钟 全展开)；")
print("      jsonl 只有真实bar；13MB parquet 承载 858 万行属于列式+ZSTD+重复值字典化正常水平。")

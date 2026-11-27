#!/usr/bin/env python3
"""16 个行情前置 IP 的 TCP 真实连通测试（只握手不登录，不影响任何已有会话）。
来源: secrets (Copy 1).toml [market_data] 段, 用户 2026-11-26 提供的 member_ips。
结果存 JSON 到 download/, 供协议层测试对照。
"""
import socket, time, json, sys
from concurrent.futures import ThreadPoolExecutor

MD_PORT = 61213
MEMBER_IPS = [
    "101.231.185.57", "101.231.185.58", "101.231.185.59", "101.231.185.60",
    "180.166.83.72", "180.166.83.73", "180.166.83.74", "180.166.83.75",
    "140.206.102.76", "140.206.102.77", "140.206.102.78", "112.65.132.105",
    "112.65.132.21", "112.65.132.22", "112.65.132.23", "112.65.132.25",
]
TIMEOUT = 5.0

def probe(ip: str) -> dict:
    r = {"ip": ip, "port": MD_PORT, "ok": False, "ms": None, "err": None, "ts": None}
    t0 = time.time()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(TIMEOUT)
    try:
        s.connect((ip, MD_PORT))
        ms = (time.time() - t0) * 1000
        r["ok"] = True
        r["ms"] = round(ms, 1)
        # CTP 前置收到连接后不会主动发数据；保持 socket 打开 0.2s 验证非即时 RST
        s.settimeout(0.5)
        try:
            data = s.recv(64)
            r["peer_data"] = data.hex() if data else "(无数据,正常)"
        except socket.timeout:
            r["peer_data"] = "(超时无推送,正常)"
        r["ts"] = time.strftime("%H:%M:%S")
    except Exception as e:
        r["err"] = f"{type(e).__name__}: {e}"
    finally:
        s.close()
    return r

def main():
    print(f"== 16 IP x TCP {MD_PORT} 真实连接测试  开始 {time.strftime('%F %T')} ==")
    with ThreadPoolExecutor(max_workers=16) as ex:
        results = list(ex.map(probe, MEMBER_IPS))
    ok = [r for r in results if r["ok"]]
    bad = [r for r in results if not r["ok"]]
    for r in results:
        flag = "✅" if r["ok"] else "❌"
        line = f"{flag} {r['ip']}:{MD_PORT}  {r['ms']}ms  {r['err'] or ''}"
        if r["ok"]:
            line += f"  {r.get('peer_data','')}"
        print(line)
    print(f"\n成功 {len(ok)}/16, 失败 {len(bad)}/16")
    if ok:
        ms_list = sorted(r["ms"] for r in ok)
        print(f"延迟: min={ms_list[0]}ms median={ms_list[len(ms_list)//2]}ms max={ms_list[-1]}ms")
    out = {
        "time": time.strftime("%F %T"),
        "port": MD_PORT,
        "timeout_s": TIMEOUT,
        "results": results,
        "summary": {"ok": len(ok), "fail": len(bad)},
    }
    path = "/home/z/my-project/download/测试16IP_TCP连接_结果.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"结果已存 {path}")
    return 0 if len(ok) == 16 else 1

if __name__ == "__main__":
    sys.exit(main())

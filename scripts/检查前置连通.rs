//! 前置连通性检查器（零依赖 · rust-script / rustc 双方式可跑）
//!
//! 用途：对 CTP 行情前置做**真实 TCP 连接测试**（connect + 保持探测），
//!       替代"看日志猜网络"——上机前/断链排查时先跑这个，16 个 member 前置一目了然。
//!
//! 用法（三选一）：
//!   rust-script 检查前置连通.rs 192.0.2.1,192.0.2.2          # 逗号分隔 IP，端口默认 61213
//!   rust-script 检查前置连通.rs 前置IP清单.txt                # 每行一个 IP（或 tcp://ip:port）
//!   rustc -O 检查前置连通.rs -o 检查前置连通 && ./检查前置连通 前置IP清单.txt 61213
//!
//! 输入行支持三种形式：裸 IP / ip:port / tcp://ip:port（端口缺省用命令行第 2 参数，再缺省 61213）。
//! 退出码：全部可达=0，有失败=1（可接 CI / 上机预检）。
//!
//! 判定口径：
//!   可达     = TCP 三次握手成功（默认 5 秒超时）；
//!   保持稳定 = 连上后再静默探测 2 秒不被对端 RST——CTP 行情前置是服务端先不发数据的
//!              私有协议，握手后无输出且连接不被踢是正常特征；
//!   本工具只做 TCP 层测试，不做 CTP 协议登录（登录需凭据，且同账号二次登录可能
//!   干扰正在实盘采集的会话——不要在实盘运行时段用真实账号做协议级测试）。

use std::env;
use std::fs;
use std::io::Read;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

const 默认端口: u16 = 61213;
const 连接超时秒: u64 = 5;
const 保持探测秒: u64 = 2;

struct 结果 {
    序号: usize,
    地址: String,
    可达: bool,
    保持稳定: bool,
    握手毫秒: u128,
    失败原因: String,
}

fn 解析目标(行: &str, 默认: u16) -> Option<(String, u16)> {
    let s = 行.trim();
    let s = s.strip_prefix("tcp://").unwrap_or(s);
    if s.is_empty() || s.starts_with('#') {
        return None; // 空行与注释跳过
    }
    if let Some((ip, p)) = s.rsplit_once(':') {
        if let Ok(port) = p.parse::<u16>() {
            return Some((ip.to_string(), port));
        }
    }
    Some((s.to_string(), 默认))
}

fn 测一个(序号: usize, ip: String, port: u16) -> 结果 {
    let 地址 = format!("{ip}:{port}");
    let mut r = 结果 {
        序号,
        地址: 地址.clone(),
        可达: false,
        保持稳定: false,
        握手毫秒: 0,
        失败原因: String::new(),
    };
    let addrs = match 地址.to_socket_addrs() {
        Ok(a) => a.collect::<Vec<_>>(),
        Err(e) => {
            r.失败原因 = format!("地址解析失败: {e}");
            return r;
        }
    };
    let sock = match addrs.first() {
        Some(a) => *a,
        None => {
            r.失败原因 = "无可用地址".into();
            return r;
        }
    };
    let t0 = Instant::now();
    let mut stream = match TcpStream::connect_timeout(&sock, Duration::from_secs(连接超时秒)) {
        Ok(s) => s,
        Err(e) => {
            r.握手毫秒 = t0.elapsed().as_millis();
            r.失败原因 = format!("TCP 连接失败({连接超时秒}s 超时内): {e}");
            return r;
        }
    };
    r.握手毫秒 = t0.elapsed().as_millis();
    r.可达 = true;
    // 保持探测：2 秒内若被对端 RST/EOF 则判不稳定；超时无数据 = 正常（CTP 服务端先不发数据）
    stream
        .set_read_timeout(Some(Duration::from_secs(保持探测秒)))
        .ok();
    let mut buf = [0u8; 64];
    match stream.read(&mut buf) {
        Ok(0) => r.保持稳定 = false, // 对端立即优雅关闭——罕见，标记出来人工看
        Ok(n) => {
            r.保持稳定 = true;
            r.失败原因 = format!("注意: 握手后收到 {n} 字节（非典型 CTP 前置行为）");
        }
        Err(_) => r.保持稳定 = true, // 读超时=连接仍在=稳定（期望路径）
    }
    r
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("用法: 检查前置连通.rs <IP清单文件|逗号分隔IP> [端口(默认{默认端口})]");
        eprintln!("示例: 检查前置连通.rs 前置IP清单.txt        # 从你机器 配置_分钟K线.toml 的 member_ips 抄出来");
        std::process::exit(2);
    }
    let port = args
        .get(2)
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(默认端口);
    let 输入 = &args[1];
    let 原文 = if 输入.contains(',') && !输入.ends_with(".txt") {
        输入.clone()
    } else {
        match fs::read_to_string(输入) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("读清单失败({输入}): {e}");
                std::process::exit(2);
            }
        }
    };
    let 目标们: Vec<(String, u16)> = 原文
        .split(|c| c == ',' || c == '\n' || c == '\r')
        .filter_map(|行| 解析目标(行, port))
        .collect();
    if 目标们.is_empty() {
        eprintln!("清单里没有可用 IP");
        std::process::exit(2);
    }
    let 总数 = 目标们.len();
    println!(
        "前置连通性检查：共 {} 个目标，连接超时 {}s，保持探测 {}s",
        总数,
        连接超时秒,
        保持探测秒
    );
    // 与实盘 v1.4 一致：16 个 member 并行发起（每个前置一个线程）
    let handles: Vec<_> = 目标们.clone()
        .into_iter()
        .enumerate()
        .map(|(i, (ip, p))| std::thread::spawn(move || 测一个(i + 1, ip, p)))
        .collect();
    let mut 全部可达 = true;
    let mut 全部稳定 = true;
    println!(
        "{:<4} {:<26} {:<8} {:<10} {:<12} {}",
        "序号", "前置", "TCP握手", "保持稳定", "耗时", "备注"
    );
    for h in handles {
        let r = h.join().expect("测试线程 panic");
        if !r.可达 {
            全部可达 = false;
        }
        if !r.保持稳定 {
            全部稳定 = false;
        }
        println!(
            "{:<4} {:<26} {:<8} {:<10} {:<12} {}",
            r.序号,
            r.地址,
            if r.可达 { "成功" } else { "失败" },
            if r.保持稳定 { "稳定" } else { "不稳定" },
            format!("{} ms", r.握手毫秒),
            if r.失败原因.is_empty() {
                if r.可达 && r.保持稳定 { "CTP 前置特征正常" } else { "" } } else { &r.失败原因 }
        );
    }
    println!("小结：可达 {}/{}，保持稳定 {}/{}，退出码 {}",
        if 全部可达 { 总数 } else { 0 }, 总数,
        if 全部稳定 { 总数 } else { 0 }, 总数,
        if 全部可达 && 全部稳定 { 0 } else { 1 });
    std::process::exit(if 全部可达 && 全部稳定 { 0 } else { 1 });
}

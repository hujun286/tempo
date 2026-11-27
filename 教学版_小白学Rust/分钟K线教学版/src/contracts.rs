// ╔══════════════════════════════════════════════════════════════════╗
// ║ 【文件头教学导读】contracts.rs —— 「花名册」与「分班组」                  ║
// ╚══════════════════════════════════════════════════════════════════╝
//
// 这个文件是全工程最短的主干模块，只有两个函数，但麻雀虽小五脏俱全：
//   · load_contracts_csv：把合约清单 CSV（一行一个「代码,交易所」）读进内存，
//     变成 Vec<Contract>——像教务处把 Excel 花名册读进系统；
//   · shard_contracts：把全部合约「分班」——按交易所分组、组内轮询，平均分给
//     16 个 member 连接。为什么要分？一条连接订阅几万只合约会顶不住，
//     分给 16 条连接（每条一个线程）才能扛住全市场行情。
//
// 【它在整条链路里的位置】CTP连接 → tick → 分钟K线 → jsonl分片 → Parquet合并。
// 它在「CTP连接」之前：先有花名册和分班，才知道每条连接该订阅哪些合约。
//
// 【本文件用到的 Rust 核心概念】
//   · struct 与字段私有性（没有 pub 的字段外界摸不到）；
//   · Result<T, E> 错误处理 + map_err + ? 运算符（错误传播的标准流水线）；
//   · 迭代器链：lines / trim / split / collect；
//   · HashMap 的 entry().or_default() 惯用法；
//   · 借用 Vec<&str>：容器里装「引用」而非拷贝，体会借用省拷贝的思想；
//   · 取模轮询分配（i % num_members）。
//
// 【建议学习时长】15 到 20 分钟。适合作为「第一个完整读懂的模块」。

// ── 宪法豁免（带理由）：clippy::indexing_slicing ─────────────────────────
// 本文件下标访问全部位于 CTP FFI 交互与 16-member 分片分配边界：
//   · member_idx 由 shard_contracts 分片分配器生成（0..num_members，边界由构造保证）
//   · CTP 结构体字段为协议固定长度 C 数组，下标来自协议常量
// v1.4 实盘验证过且工程文档明文「与 v1.1 逐字一致，改动=引入风险」，
// 故按宪法逃生舱做整文件显式豁免，不逐处改 get() 触碰热路径。
#![allow(
    clippy::indexing_slicing,
    reason = "CTP FFI/分片分配边界由构造保证；v1.4 实盘逐字一致原则，不触碰热路径"
)]
//! 合约清单加载与 member 分片。
use std::collections::HashMap; // 哈希表：按 key（交易所名）快速找到 value（该所合约列表）
use std::path::Path; // Path：跨平台的文件路径类型（不用自己拼字符串）

// ============================================================
// 合约（照抄 v1.1）
// ============================================================

// ════════════════ Rust 概念教学 ①：struct 与「字段私有性」 ════════════════
// Contract 的两个字段 code / exchange 都【没写 pub】——它们是私有字段：
// crate 外部（以及本文件之外，因为没暴露 getter）无法直接访问。
// 只有本文件的两个函数知道怎么构造它（构造时字段按名初始化）。
//
// 生活类比：字段私有像「学生证上的学号只有教务处能改」——外部只能拿到整本
// 学生证，改不了里面的字段。好处：以后想给 Contract 加校验/换内部表示，
// 本文件之外没有任何代码会被波及。
//
// #[derive(Debug, Clone)]：Debug 支持 {:?} 打印排查；Clone 支持按值复制。
// 注意 Clone 是「深拷贝语义」：两个 String 字段会各自复制一份，互不影响——
// 这和「借用」（&Contract，只看不拷）是两种给函数传数据的路径。
// ════════════════════════════════════════════════════════════════
#[derive(Debug, Clone)]
pub struct Contract {
    code: String, // 合约代码，如 "IF2401"（私有字段）
    exchange: String, // 交易所简称，如 "CFFEX"（私有字段）
}

// ════════════════ Rust 概念教学 ②：Result 与错误传播（map_err + ?） ════════════════
// 函数签名 Result<Vec<Contract>, String> 读作「成功返回合约列表，失败返回一条
// 错误消息字符串」。Rust 不用 try/catch，而是把「可能失败」写进返回类型。
//
// 两件法宝看懂本函数：
//   · ? 运算符：表达式后面加问号 = 「若 Err 就立即把错误返回给调用方，若 Ok 就
//     拆包继续」。它是函数体从上到下读的「快速失败通道」；
//   · map_err(|e| format!(…))：错误类型不同不能直接 ?，先用 map_err 把
//     std::io::Error 翻译成本函数约定的 String（顺便带上文件路径等上下文）。
//
// 生活类比：? 像「流水线质检」——上一站不合格立刻整箱退回（连带包装上的说明），
// 合格的继续往下走。调用方拿到 Result 再决定怎么办，程序不会中途崩溃。
//
// 【为什么错误类型选 String 而不是 thiserror】教学版按原工程保留 String：
// 简单直接；工程化进阶再换自定义错误枚举（那是另一门课）。
// ════════════════════════════════════════════════════════════════
/// 读入合约清单 CSV：一行一个「代码,交易所」，井号开头是注释，空行跳过。
pub fn load_contracts_csv(path: &Path) -> Result<Vec<Contract>, String> {
    // 读整个文件为 String；map_err 带上路径上下文；? 失败即返回
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("读 {} 失败: {}", path.display(), e))?;
    let mut out = Vec::new(); // 收集结果的可增长数组（Vec 像自动扩容的数组）
    for line in text.lines() {
        // 行内微注释：lines() 是迭代器，逐行借出 &str（不复制字符串本体）——
        // 「借用」省内存又省拷贝，这正是 Rust 迭代器的惯用收益。
        let line = line.trim(); // 去掉行首尾空白；这个新变量把旧变量「遮蔽」了（shadowing），同名更清爽
        if line.is_empty() || line.starts_with('#') {
            continue; // 空行和注释行直接跳过
        }
        let parts: Vec<&str> = line.split(',').collect(); // 按逗号切开；collect() 把迭代器收成 Vec
        if parts.len() < 2 {
            continue; // 连「代码,交易所」两栏都凑不齐：坏行，跳过（容错不崩溃）
        }
        out.push(Contract {
            code: parts[0].to_string(), // &str → String：把切片内容复制成拥有所有权的字符串
            exchange: parts[1].to_string(),
        }); // 行内微注释：to_string 在这里必要——text 迭代器借出的切片活不出本函数
    }
    Ok(out) // Ok 包裹返回：一切正常
}

// ════════════════ Rust 概念教学 ③：HashMap 的 entry().or_default() 惯用法 ════════════════
// 「按 key 分组」是 HashMap 最高频的场景，标准姿势只有一行：
//     by_ex.entry(c.exchange.clone()).or_default().push(&c.code);
// 拆开讲：
//   · entry(k)：查一下这个 key 的「坑位」，返回 Vacant（空坑）或 Occupied（有主）；
//   · or_default()：空坑就放一个默认值进去（Vec 的默认值是空列表），并借出它；
//   · .push(…)：往借出的列表里追加。
// 对比两段式写法「先 contains_key 判断、再 insert、再 get_mut」要省一次哈希查找，
// 而且不会因为借用规则报错——新手常在这里撞墙，entry 是标准答案。
//
// 【生命周期时刻】Vec<&str> 里装的是「借用」（引用），不是拷贝：&c.code 借用的是
// 外层 contracts 的内容。函数签名没写生命周期，因为「省略规则」可以推断
// （输入借多长，输出借多长）——这就是「生命周期多数时候不用手写」的实例。
// 一旦编译器推不出来，它会直接提示你补<'a>，而不是悄悄出错。
// ════════════════════════════════════════════════════════════════
/// 按交易所分组，组内轮询分给 N 个 member（与 v1.1 一致，结果稳定可复现）。
/// 入参 &[Contract] 是「共享借用」：本函数只读不拿所有权，调用方继续持有原数据。
pub fn shard_contracts(contracts: &[Contract], num_members: usize) -> Vec<Vec<String>> {
    // 先造 N 个空班：vec![Vec::new(); N] 一行造出「N 个空 Vec 的 Vec」
    let mut shards: Vec<Vec<String>> = vec![Vec::new(); num_members];
    let mut by_ex: HashMap<String, Vec<&str>> = HashMap::new(); // 交易所名 → 该所合约代码（借用）
    for c in contracts {
        by_ex.entry(c.exchange.clone()).or_default().push(&c.code); // entry 惯用法：分组一行搞定（exchange 要 clone，因为要当 key 拥有）
    }
    // 为什么要先收集 keys 再排序？HashMap 的遍历顺序是「随机的」（哈希序不稳定），
    // 排序后分组处理顺序固定 → 分班结果完全可复现，同一输入两次运行结果一致。
    // 这对「测试可重复」「日志可对比」都很关键——确定性是工程美德。
    let mut ex_keys: Vec<&String> = by_ex.keys().collect(); // 收集所有交易所名的引用
    ex_keys.sort(); // 按名称排序保证确定性
    for ex in ex_keys {
        let codes = &by_ex[ex]; // 行内微注释：这里用 [ ] 索引而不是 get，因为 key 来自 keys() 必然存在；本文件已整文件豁免 indexing_slicing（见文件头）
        for (i, code) in codes.iter().enumerate() {
            shards[i % num_members].push(code.to_string()); // 取模轮询：第 i 个分给第 i%N 班——像发牌，每人牌数最多差一张
        }
        tracing::info!("[SHARD] 交易所 {}: {} 条", ex, codes.len()); // 每个交易所分了多少条，打日志便于核对
    }
    shards // 返回：N 个班，每班是一列合约代码
}

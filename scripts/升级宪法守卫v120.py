#!/usr/bin/env python3
"""宪法守卫 v1.1.0 -> v1.2.0 升级脚本：按行号精准替换执行宪法体检函数 + 主流程接线。
行号依据：main.rs 中 执行宪法体检 位于 479-542（含函数尾 }），其后是 需要迁移。
"""
from pathlib import Path

F = Path("/home/z/my-project/宪法守卫-v1.1.0/main.rs")
lines = F.read_text(encoding="utf-8").splitlines(keepends=True)

# 定位函数边界（1-based 行号 -> 0-based 索引）
start = next(i for i, l in enumerate(lines) if "/// 宪法体检：格式" in l)
end = None
for i in range(start, len(lines)):
    if lines[i].strip() == "}" and any("fn 需要迁移" in lines[j] for j in range(i + 1, min(i + 4, len(lines)))):
        end = i
        break
assert end is not None, "未找到函数边界"
print(f"替换区间: {start+1} - {end+1} 行")

新函数 = '''    /// 宪法体检：格式 → Clippy 铁律 → edition 提示。返回 0=通过，1=违宪。
    /// v1.2.0：fmt 与 clippy 并行执行；强制真跑=false 时，源码与参数未变可复用上次通过结果。
    pub fn 执行宪法体检(项目根: &Path, 原参数: &[String], 强制真跑: bool) -> i32 {
        // 特性旗标影响条件编译代码，必须转发给 clippy，也参与缓存指纹
        let 特性参数: Vec<String> = 原参数
            .iter()
            .filter(|项| {
                let 项 = 项.as_str();
                项.starts_with("--features")
                    || 项 == "--all-features"
                    || 项 == "--no-default-features"
            })
            .cloned()
            .collect();
        let 特性指纹 = 特性参数.join(" ");

        // 缓存复用仅用于接管拦截场景（「宪法守卫 体检」等强制真跑模式不复用）
        if !强制真跑 && 缓存管理::体检缓存命中(项目根, &特性指纹) {
            println!(
                "{} 宪法体检通过（缓存命中：源码与参数未变，复用上次全量体检结果）",
                色::绿("[通过]")
            );
            return 0;
        }

        let 违规数 = 全量体检(项目根, &特性参数);
        if 违规数 == 0 {
            println!(
                "{} 宪法体检通过（{} 条铁律 + 警告组 + 淘汰清单）",
                色::绿("[通过]"),
                宪法规则::拒绝规则.len()
            );
            缓存管理::记录体检通过(项目根, &特性指纹);
            0
        } else {
            缓存管理::清体检缓存(项目根);
            println!(
                "{} 共 {违规数} 类违宪。修复：cargo 宪法守卫 修复；临时放行：CONSTITUTION_OVERRIDE=1",
                色::红("[拦截]")
            );
            1
        }
    }

    /// 全量真跑：rustfmt 与 clippy 并行（墙钟时间约等于两者较慢者），汇总违规数
    fn 全量体检(项目根: &Path, 特性参数: &[String]) -> i32 {
        let 根1 = 项目根.to_path_buf();
        let 特1: Vec<String> = 特性参数.to_vec();
        let 格式线程 = std::thread::spawn(move || 格式体检(&根1));
        let clippy线程 = std::thread::spawn(move || clippy体检(&根1, &特1));

        let mut 违规数 = 0;
        if !格式线程.join().unwrap_or(false) {
            违规数 += 1;
        }
        if !clippy线程.join().unwrap_or(false) {
            违规数 += 1;
        }

        // 三、edition 宪法（提示级，不拦截）
        edition提示(项目根);
        违规数
    }

    /// 格式体检：true=通过或跳过，false=违宪
    fn 格式体检(项目根: &Path) -> bool {
        if 格式可用(项目根) {
            let 通过 = Command::new(真实cargo())
                .args(["fmt", "--check"])
                .current_dir(项目根)
                .status()
                .map(|态 | 态.success())
                .unwrap_or(true);
            if !通过 {
                println!(
                    "{} 格式不符合 rustfmt 规范 —— 运行「cargo 宪法守卫 修复」一键整改",
                    色::红("[违宪]")
                );
                false
            } else {
                true
            }
        } else {
            println!(
                "{} 未检测到 rustfmt，跳过格式检查（rustup component add rustfmt）",
                色::黄("[提示]")
            );
            true
        }
    }

    /// clippy 体检：true=通过，false=违宪
    fn clippy体检(项目根: &Path, 特性参数: &[String]) -> bool {
        let 通过 = clippy命令(项目根, 特性参数, false)
            .status()
            .map(|态 | 态.success())
            .unwrap_or(false);
        if !通过 {
            println!("{} clippy 铁律违规（上方含具体条目与位置）", 色::红("[违宪]"));
            false
        } else {
            true
        }
    }
'''
lines[start:end + 1] = [新函数]
F.write_text("".join(lines), encoding="utf-8")
print("体检函数替换完成")

# —— 主流程接线：体检子命令强制真跑；接管复用缓存 ——
文本 = F.read_text(encoding="utf-8")
旧1 = 'Some("体检") => 以项目执行(|根| 检查执行::执行宪法体检(根, &[])),'
新1 = 'Some("体检") => 以项目执行(|根| 检查执行::执行宪法体检(根, &[], true)),'
assert 旧1 in 文本, "体检子命令接线点未找到"
文本 = 文本.replace(旧1, 新1)

旧2 = "let 码 = 检查执行::执行宪法体检(&根, 参数);"
新2 = "let 码 = 检查执行::执行宪法体检(&根, 参数, false);"
assert 旧2 in 文本, "接管接线点未找到"
文本 = 文本.replace(旧2, 新2)

# —— 自检增加体检缓存状态行（在逃生舱行之前插入）——
锚 = '        let 逃生舱 = std::env::var_os("CONSTITUTION_OVERRIDE").is_some();'
缓存行 = '''        let 项目 = 项目探测::项目根(std::path::Path::new("."));
        println!(
            "{} 体检缓存：{}",
            色::蓝("[缓存]"),
            缓存管理::缓存状态一句话(项目.as_deref())
        );

'''
assert 锚 in 文本, "自检接线点未找到"
文本 = 文本.replace(锚, 缓存行 + 锚)

# 主流程 use 行补 缓存管理 已存在？检查
旧use = "use crate::{缓存管理, 命令解析, 检查执行, 宪法规则, 项目探测};"
assert 旧use in 文本, "主流程 use 行未找到（应已包含 缓存管理）"

F.write_text(文本, encoding="utf-8")
print("主流程接线完成")

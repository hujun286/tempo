#![allow(nonstandard_style, reason = "本项目使用中文标识符与中文模块名")]
#![allow(
    clippy::print_stdout,
    reason = "守卫职责即向用户输出体检/拦截/修复结果，本程序本身就是日志门面"
)]
#![allow(
    clippy::print_stderr,
    reason = "熔断与致命错误必须走 stderr，这是守卫职责"
)]
//! # 宪法守卫 v1.2.0（单文件实现）
//!
//! 让每一次 `cargo build / check / clippy / run / test / bench` 都先过同一部宪法：
//! rustfmt 格式 + Clippy 严格规则组 + 官方淘汰清单，通过才放行原命令。
//!
//! 设计原则：**官方机制优先，守卫只做编排**。
//! - 工具链解析交给 rustup（项目用 nightly 就是 nightly）；
//! - 语法迁移交给 `cargo fix --edition`；
//! - lint 修复交给 `cargo clippy --fix`；
//! - 淘汰清单交给 clippy 官方 `clippy.toml`（CLIPPY_CONF_DIR）；
//! - 守卫自身零依赖、固定 stable，只负责"何时检查、检查什么、如何放行"。
//!
//! 运行形态：
//! - 全局接管后：`cargo 宪法守卫 <子命令>` 或 `cargo build`（自动拦截）；
//! - 未接管时：`cargo constitutional-guard <子命令>`。
//!
//! 退出码约定：0 = 通过；1 = 违宪拦截；125 = 守卫自身故障（接管脚本自动回退原生 cargo）。

mod 宪法规则 {
    //! 宪法条文：唯一事实源。命令行参数、AGENTS.md、规则打印全部由此生成。

    use std::path::Path;

    /// 守卫版本（来自 Cargo.toml，不再手抄）
    pub const 守卫版本: &str = env!("CARGO_PKG_VERSION");

    /// 铁律（违反即编译失败）。共 20 条。
    /// 选择标准：错误处理类（AI 最爱 unwrap）、正确性升级类（官方默认仅警告）、
    /// 现代语法强制类、治理类（豁免必须写理由）。
    pub const 拒绝规则: &[(&str, &str)] = &[
        (
            "clippy::unwrap_used",
            "错误处理不得崩溃：用 match / ok_or / let-else",
        ),
        (
            "clippy::expect_used",
            "expect 同样是潜在 panic：返回 Result",
        ),
        ("clippy::panic", "生产路径禁止 panic"),
        ("clippy::todo", "未完成代码不得混入生产"),
        ("clippy::unimplemented", "未实现代码不得混入生产"),
        ("clippy::dbg_macro", "调试语句不得残留"),
        ("clippy::print_stdout", "生产代码禁止打印：返回值或日志门面"),
        ("clippy::print_stderr", "生产代码禁止打印：返回值或日志门面"),
        ("clippy::indexing_slicing", "下标可能越界：用 get / 迭代器"),
        (
            "clippy::undocumented_unsafe_blocks",
            "unsafe 必须附 // SAFETY: 注释说明不变量",
        ),
        (
            "clippy::unwrap_in_result",
            "不得把 Option 塞进 Result 绕过错误处理",
        ),
        ("clippy::unreachable", "生产路径禁止 unreachable"),
        (
            "clippy::float_cmp",
            "浮点数禁止 == 直接比较：用容差或完全有序类型",
        ),
        ("clippy::mem_forget", "泄漏内存是代码气味：显式管理生命周期"),
        (
            "clippy::await_holding_lock",
            "await 期间持有锁会死锁：先释放或用作用域守卫",
        ),
        (
            "clippy::arc_with_non_send_sync",
            "非 Send/Sync 类型包 Arc 无意义且有数据竞争隐患",
        ),
        (
            "deprecated",
            "官方已淘汰的 API 立即报错（官方维护的淘汰清单）",
        ),
        ("wildcard_dependencies", "依赖版本禁止写 \"*\"：必须锁定"),
        (
            "clippy::uninlined_format_args",
            "必须用内联格式化 format!(\"{x}\")——强制最新语法",
        ),
        (
            "clippy::allow_attributes_without_reason",
            "豁免必须写明 reason：看得见的例外才可控",
        ),
    ];

    /// 警告组（提示但不拦截）
    pub const 警告组: &[(&str, &str)] = &[
        ("clippy::pedantic", "学究组：更严格的正确性与风格建议"),
        ("clippy::nursery", "实验组：未定稿规则，仅提示不拦截"),
        ("clippy::cargo", "Cargo 清单卫生"),
    ];

    /// 生成传给 clippy 的参数（追加在 `cargo clippy --` 之后）
    pub fn 宪法参数() -> Vec<String> {
        let mut 参数: Vec<String> = Vec::new();
        for (名称, _) in 拒绝规则 {
            参数.push("-D".to_string());
            参数.push((*名称).to_string());
        }
        for (名称, _) in 警告组 {
            参数.push("-W".to_string());
            参数.push((*名称).to_string());
        }
        参数
    }

    /// 生成 AGENTS.md / CLAUDE.md 内容（AI 智能体开工前会读取的宪法摘要）
    pub fn AGENTS内容() -> String {
        let mut 文 = String::new();
        文.push_str("# 本项目宪法\n\n");
        文.push_str(&format!(
            "> 由宪法守卫 v{守卫版本} 生成 · 单一事实源：`cargo 宪法守卫 规则`\n\n"
        ));
        文.push_str("## 工具链\n- 本项目默认 **nightly（浮动）**：`rustup default nightly`，新功能即刻可用\n");
        文.push_str("- 需要回稳时：项目根 `rust-toolchain.toml` 写 `channel = \"stable\"`，仅本项目生效\n\n");
        文.push_str("## 铁律（违反即编译失败）\n\n| lint | 理由 |\n|---|---|\n");
        for (名, 理) in 拒绝规则 {
            文.push_str(&format!("| `{名}` | {理} |\n"));
        }
        文.push_str("\n## 警告组（提示不拦截）\n");
        for (名, 理) in 警告组 {
            文.push_str(&format!("- `{名}`：{理}\n"));
        }
        文.push_str("\n## 淘汰清单（全局 clippy.toml）\n");
        文.push_str("- `lazy_static!` → `std::sync::LazyLock`（标准库）\n");
        文.push_str("- `Once::new` → `OnceLock::new`（可携带值）\n");
        文.push_str("- `hint::unreachable_unchecked` → `unreachable!()`（安全版本）\n\n");
        文.push_str("## 写完代码后\n\n```sh\ncargo 宪法守卫 修复\n# 格式化 → edition 迁移 → clippy 自动修复 → 复检，全部官方机制\n```\n\n");
        文.push_str("## 测试与生产同严\n");
        文.push_str("测试代码同样受全部铁律约束。确需 panic 的用例，用 `#[expect(clippy::panic)]` 显式标注。\n\n");
        文.push_str("## 逃生舱（必须显式）\n");
        文.push_str("- 单点豁免：`#[allow(clippy::unwrap_used, reason = \"为什么\")]`\n");
        文.push_str("- 整次放行：`CONSTITUTION_OVERRIDE=1 cargo ...`\n");
        文.push_str("- 整项目豁免：项目根放空文件 `宪法守卫.关闭`\n");
        文
    }

    /// 打印完整宪法（含全局淘汰清单安装状态）
    pub fn 打印规则() {
        println!("宪法守卫 v{守卫版本} · 现行宪法\n");
        println!("铁律（拒绝，共 {} 条）：", 拒绝规则.len());
        for (序, (名, 理)) in 拒绝规则.iter().enumerate() {
            let 号 = 序 + 1;
            println!("  {号:2}. {名}\n      {理}");
        }
        println!("\n警告组（提示不拦截）：");
        for (名, 理) in 警告组 {
            println!("  - {名}：{理}");
        }
        match 全局清单文本() {
            Some(文本) => {
                println!("\n全局淘汰清单（~/.cargo/constitutional-guard/clippy.toml）：\n{文本}")
            }
            None => println!("\n全局淘汰清单：未安装（运行 工具/编译安装.sh）"),
        }
    }

    fn 全局清单文本() -> Option<String> {
        let 家 = std::env::var_os("HOME")?;
        std::fs::read_to_string(
            Path::new(&家)
                .join(".cargo")
                .join("constitutional-guard")
                .join("clippy.toml"),
        )
        .ok()
    }
}

mod 项目探测 {
    //! 项目边界与豁免判定

    use std::path::{Path, PathBuf};

    /// 从起始目录向上找 Cargo.toml，返回项目根
    pub fn 项目根(起始: &Path) -> Option<PathBuf> {
        let 起点 = if 起始.is_absolute() {
            起始.to_path_buf()
        } else {
            match std::env::current_dir() {
                Ok(目录) => 目录,
                Err(_) => return None,
            }
        };
        let mut 目录 = 起点;
        loop {
            if 目录.join("Cargo.toml").is_file() {
                return Some(目录);
            }
            if !目录.pop() {
                return None;
            }
        }
    }

    /// 项目根存在「宪法守卫.关闭」即整项目豁免
    pub fn 是否豁免(根: &Path) -> bool {
        根.join("宪法守卫.关闭").exists()
    }

    /// 项目自带 clippy.toml / .clippy.toml 时，守卫不注入全局清单（尊重项目）
    pub fn 有自带清单(根: &Path) -> bool {
        根.join("clippy.toml").is_file() || 根.join(".clippy.toml").is_file()
    }

    /// 解析 package.edition（尽力而为，找不到返回 None）
    pub fn 项目edition(根: &Path) -> Option<String> {
        let 文本 = std::fs::read_to_string(根.join("Cargo.toml")).ok()?;
        for 行 in 文本.lines() {
            let 行 = 行.trim();
            if let Some(余) = 行.strip_prefix("edition") {
                if let Some(值) = 余.trim().strip_prefix('=') {
                    return Some(值.trim().trim_matches('"').to_string());
                }
            }
        }
        None
    }
}

mod 缓存管理 {
    //! 规则指纹 + 体检缓存：按项目存放在各自 target/宪法守卫/ 下。
    //! - 规则指纹：宪法变更时提示全量复检；
    //! - 体检缓存（v1.2.0 新增）：源码未变时复用上次体检通过，避免每次编译都全量
    //!   rustfmt+clippy（这是 v1.1.0 用户反馈"编译又慢又容易拦截"的主因）。

    use crate::宪法规则;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::path::{Path, PathBuf};
    use std::time::UNIX_EPOCH;

    fn 哈希(文本: &str) -> String {
        let mut 器 = DefaultHasher::new();
        文本.hash(&mut 器);
        format!("{:016x}", 器.finish())
    }

    /// 指纹 = 守卫版本 + 全部宪法参数
    pub fn 规则指纹() -> String {
        let 源 = 宪法规则::守卫版本.to_string() + &宪法规则::宪法参数().join(" ");
        哈希(&源)
    }

    fn 缓存目录(项目根: &Path) -> PathBuf {
        项目根.join("target").join("宪法守卫")
    }

    fn 指纹路径(项目根: &Path) -> PathBuf {
        缓存目录(项目根).join("规则指纹")
    }

    fn 体检缓存路径(项目根: &Path) -> PathBuf {
        缓存目录(项目根).join("体检缓存")
    }

    /// 返回 true = 指纹一致；false = 首次运行或宪法已更新（应提示全量复检）
    pub fn 校验并记录(项目根: &Path) -> bool {
        let 当前 = 规则指纹();
        let 路径 = 指纹路径(项目根);
        let 旧 = std::fs::read_to_string(&路径).unwrap_or_default();
        // 首次运行没有可比对象，不算"变化"
        let 一致 = 旧.trim().is_empty() || 旧.trim() == 当前;
        if let Some(父) = 路径.parent() {
            let _ = std::fs::create_dir_all(父);
            let _ = std::fs::write(&路径, &当前);
        }
        一致
    }

    /// 源码指纹：遍历项目文件（排除 target/ 与隐藏目录），
    /// 对 (相对路径, 大小, mtime纳秒) 列表做哈希。误报方向安全：
    /// mtime 变了就复检，绝不会"源码变了却复用旧结果"。
    pub fn 源码指纹(项目根: &Path) -> String {
        let mut 条目: Vec<String> = Vec::new();
        收集(项目根, 项目根, &mut 条目);
        条目.sort();
        哈希(&条目.join("\n"))
    }

    fn 收集(项目根: &Path, 目录: &Path, 条目: &mut Vec<String>) {
        let Ok(读) = std::fs::read_dir(目录) else {
            return;
        };
        for 项 in 读.flatten() {
            let 路径 = 项.path();
            let 名 = 项.file_name().to_string_lossy().to_string();
            if 路径.is_dir() {
                if 名 == "target" || 名 == ".git" || 名 == ".cargo" || 名.starts_with('.') {
                    continue;
                }
                收集(项目根, &路径, 条目);
            } else if let Ok(元) = 项.metadata() {
                let 相对 = 路径
                    .strip_prefix(项目根)
                    .map(|r| r.to_string_lossy().to_string())
                    .unwrap_or(名.clone());
                let mtime = 元
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                    .map(|d| d.as_nanos())
                    .unwrap_or(0);
                条目.push(format!("{}|{}|{}", 相对, 元.len(), mtime));
            }
        }
    }

    /// 读体检缓存。命中 = (规则,源码,特性) 三指纹全部一致且上次结果为通过。
    pub fn 体检缓存命中(项目根: &Path, 特性指纹: &str) -> bool {
        let 记录 = std::fs::read_to_string(体检缓存路径(项目根)).unwrap_or_default();
        let mut 段 = 记录.trim().split('|');
        let (规则段, 源码段, 特性段, 结果段) = match (段.next(), 段.next(), 段.next(), 段.next())
        {
            (Some(a), Some(b), Some(c), Some(d)) => (a, b, c, d),
            _ => return false,
        };
        段.next().is_none()
            && 规则段 == 规则指纹()
            && 源码段 == 源码指纹(项目根)
            && 特性段 == 特性指纹
            && 结果段 == "0"
    }

    /// 写体检缓存（只在体检通过时调用；违宪时清缓存，下次强制真跑）
    pub fn 记录体检通过(项目根: &Path, 特性指纹: &str) {
        let 内容 = format!("{}|{}|{}|0", 规则指纹(), 源码指纹(项目根), 特性指纹);
        let _ = std::fs::create_dir_all(缓存目录(项目根));
        let _ = std::fs::write(体检缓存路径(项目根), 内容);
    }

    pub fn 清体检缓存(项目根: &Path) {
        let _ = std::fs::remove_file(体检缓存路径(项目根));
    }

    /// 自检展示用：缓存当前状态一句话
    pub fn 缓存状态一句话(项目根: Option<&Path>) -> String {
        match 项目根 {
            None => "不在项目内，无缓存".to_string(),
            Some(根) => {
                let 在 = 体检缓存路径(根).is_file();
                if 在 {
                    format!(
                        "已有缓存（源码变更/参数变更时自动失效重检）：{}",
                        体检缓存路径(根).display()
                    )
                } else {
                    "暂无缓存（首次体检通过后建立）".to_string()
                }
            }
        }
    }
}

mod 命令解析 {
    //! 哪些 cargo 命令被宪法接管，哪些直接透传

    /// 触发宪法体检的命令（含 rustup 风格别名）。
    /// 依据：这些命令都会编译代码，编译必须过宪法。
    /// 注意：测试（test）与生产（build/run）同样严格，一视同仁。
    pub const 被接管命令: &[&str] = &[
        "build", "b", "check", "c", "clippy", "run", "r", "test", "t", "bench",
    ];

    /// 直接透传的命令（依赖管理、脚手架、文档、清洁等，不触发体检）
    pub const 直接透传命令: &[&str] = &[
        "add",
        "remove",
        "update",
        "new",
        "init",
        "search",
        "login",
        "logout",
        "owner",
        "publish",
        "yank",
        "unyank",
        "install",
        "uninstall",
        "doc",
        "fmt",
        "fetch",
        "clean",
        "tree",
        "vendor",
        "verify-project",
        "report",
        "metadata",
        "completions",
        "config",
        "help",
        "version",
        "coverage",
        "llvm-tools",
        "rustc",
        "bloat",
        "expand",
    ];

    /// 第一个非旗标参数即子命令
    #[allow(dead_code, reason = "命令解析辅助函数，由单元测试覆盖")]
    pub fn 首个命令(参数: &[String]) -> Option<&str> {
        参数
            .iter()
            .find(|项| !项.starts_with('-'))
            .map(|项| 项.as_str())
    }

    pub fn 是否接管(命令: &str) -> bool {
        被接管命令.contains(&命令)
    }

    pub fn 是否透传(命令: &str) -> bool {
        直接透传命令.contains(&命令)
    }

    /// 别名归一（b→build 等），用于展示
    pub fn 规范名(命令: &str) -> &'static str {
        match 命令 {
            "b" | "build" => "build",
            "c" | "check" => "check",
            "t" | "test" => "test",
            "r" | "run" => "run",
            "clippy" => "clippy",
            "bench" => "bench",
            _ => "其他",
        }
    }
}

mod 输出风格 {
    //! ANSI 输出（尊重 NO_COLOR 环境变量）

    fn 染(码: &str, 文本: &str) -> String {
        if std::env::var_os("NO_COLOR").is_some() {
            return 文本.to_string();
        }
        format!("\x1b[{码}m{文本}\x1b[0m")
    }

    pub fn 红(文本: &str) -> String {
        染("31;1", 文本)
    }
    pub fn 绿(文本: &str) -> String {
        染("32;1", 文本)
    }
    pub fn 黄(文本: &str) -> String {
        染("33;1", 文本)
    }
    pub fn 蓝(文本: &str) -> String {
        染("34;1", 文本)
    }
    pub fn 暗(文本: &str) -> String {
        染("2", 文本)
    }
}

mod 检查执行 {
    //! 宪法体检、自动修复、初始化、自检的具体执行

    use crate::宪法规则;
    use crate::输出风格 as 色;
    use crate::{缓存管理, 项目探测};
    use std::path::{Path, PathBuf};
    use std::process::Command;

    /// 解析真实 cargo：
    /// 1) 接管脚本注入的 CARGO_CONSTITUTION_REAL（优先）；
    /// 2) PATH 中第一个不在守卫目录里的 cargo（跳过自身 wrapper，防止递归）；
    /// 3) 兜底 "cargo"。
    /// 不固定单一真实路径的原因：必须保留 rustup shim 的项目级工具链解析
    /// （项目用 nightly 就自动用 nightly，这是本守卫的核心机制之一）。
    pub fn 真实cargo() -> PathBuf {
        if let Some(路径) = std::env::var_os("CARGO_CONSTITUTION_REAL") {
            if Path::new(&路径).exists() {
                return PathBuf::from(路径);
            }
        }
        if let Ok(路径串) = std::env::var("PATH") {
            for 目录 in 路径串.split(':') {
                if 目录.contains("宪法守卫") {
                    continue;
                }
                let 候选 = Path::new(目录).join("cargo");
                if 候选.is_file() {
                    return 候选;
                }
            }
        }
        PathBuf::from("cargo")
    }

    fn 全局清单目录() -> Option<PathBuf> {
        let 家 = std::env::var_os("HOME")?;
        let 目录 = Path::new(&家).join(".cargo").join("constitutional-guard");
        if 目录.join("clippy.toml").is_file() {
            Some(目录)
        } else {
            None
        }
    }

    /// 组装带宪法的 clippy 命令
    fn clippy命令(项目根: &Path, 前置参数: &[String], 修复模式: bool) -> Command {
        let mut 命令 = Command::new(真实cargo());
        命令.current_dir(项目根);
        命令.arg("clippy").arg("--all-targets").arg("--keep-going");
        for 参 in 前置参数 {
            命令.arg(参);
        }
        if 修复模式 {
            命令.arg("--fix").arg("--allow-dirty");
            if !项目根.join(".git").exists() {
                命令.arg("--allow-no-vcs");
            }
        }
        if std::env::var_os("CARGO_CONSTITUTION_JSON").is_some() && !修复模式 {
            命令.arg("--message-format=json");
        }
        命令.arg("--");
        for 参 in 宪法规则::宪法参数() {
            命令.arg(参);
        }
        // 项目自带 clippy.toml 时让位（尊重项目配置）
        if !项目探测::有自带清单(项目根) {
            if let Some(目录) = 全局清单目录() {
                命令.env("CLIPPY_CONF_DIR", 目录);
            }
        }
        命令
    }

    fn 格式可用(项目根: &Path) -> bool {
        Command::new(真实cargo())
            .args(["fmt", "--version"])
            .current_dir(项目根)
            .output()
            .map(|出| 出.status.success())
            .unwrap_or(false)
    }

    fn edition提示(项目根: &Path) {
        if let Some(版) = 项目探测::项目edition(项目根) {
            let 数 = 版.trim().parse::<u32>().unwrap_or(2024);
            if 数 < 2024 {
                println!(
                    "{} 项目 edition 为 {版}，落后于 2024；「cargo 宪法守卫 修复」可自动迁移旧写法",
                    色::黄("[提示]")
                );
            }
        }
    }

    /// 宪法体检：格式 → Clippy 铁律 → edition 提示。返回 0=通过，1=违宪。
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

    /// 全量真跑：rustfmt 与 clippy 并行（墙钟时间约等于两者较慢者），汇总违规数。
    /// 并行前先串行"预热"两个组件（--version 触发 rustup 自动下载，串行避免
    /// 两个子进程并发下载同一组件导致锁冲突——实测于 2026-11-26）。
    fn 全量体检(项目根: &Path, 特性参数: &[String]) -> i32 {
        let 预热 = Command::new(真实cargo())
            .arg("fmt")
            .arg("--version")
            .current_dir(项目根)
            .status()
            .map(|态| 态.success())
            .unwrap_or(false);
        let 预热2 = Command::new(真实cargo())
            .arg("clippy")
            .arg("--version")
            .current_dir(项目根)
            .status()
            .map(|态| 态.success())
            .unwrap_or(false);
        let _ = (预热, 预热2);

        let 根1 = 项目根.to_path_buf();
        let 根2 = 根1.clone();
        let 特1: Vec<String> = 特性参数.to_vec();
        let 格式线程 = std::thread::spawn(move || 格式体检(&根2));
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
                .map(|态| 态.success())
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
            .map(|态| 态.success())
            .unwrap_or(false);
        if !通过 {
            println!(
                "{} clippy 铁律违规（上方含具体条目与位置）",
                色::红("[违宪]")
            );
            false
        } else {
            true
        }
    }

    fn 需要迁移(项目根: &Path) -> bool {
        match 项目探测::项目edition(项目根) {
            Some(版) => 版.trim().parse::<u32>().unwrap_or(2024) < 2024,
            None => false,
        }
    }

    /// fix --edition 成功后，把 Cargo.toml 的 edition 升一级（2015→2018→2021→2024）
    fn 升级edition(项目根: &Path) {
        let 当前 = match 项目探测::项目edition(项目根) {
            Some(版) => 版,
            None => return,
        };
        let 下一版 = match 当前.trim() {
            "2015" => "2018",
            "2018" => "2021",
            _ => "2024",
        };
        let 清单 = 项目根.join("Cargo.toml");
        let 文本 = match std::fs::read_to_string(&清单) {
            Ok(t) => t,
            Err(_) => return,
        };
        let 旧行 = format!("edition = \"{当前}\"");
        let 新行 = format!("edition = \"{下一版}\"");
        if 文本.contains(&旧行) {
            let 新文本 = 文本.replacen(&旧行, &新行, 1);
            if std::fs::write(&清单, 新文本).is_ok() {
                println!(
                    "{} edition 已升级：{当前} → {下一版}（旧写法已由 cargo fix 自动迁移）",
                    色::绿("[升级]")
                );
            }
        } else {
            println!(
                "{} 未能自动改写 edition（可能是 workspace 继承写法），请手动把 edition 改为 \"{下一版}\"",
                色::黄("[注意]")
            );
        }
    }

    /// 自动修复编排：格式化 → edition 迁移 → clippy 修复 → 复检（全部官方机制）
    pub fn 执行修复(项目根: &Path) -> i32 {
        println!("{} 第 1/4 步：rustfmt 全量格式化", 色::蓝("[修复]"));
        if 格式可用(项目根) {
            let _ = Command::new(真实cargo())
                .arg("fmt")
                .current_dir(项目根)
                .status();
        } else {
            println!("{} 未安装 rustfmt，跳过", 色::黄("[提示]"));
        }

        println!(
            "{} 第 2/4 步：旧版语法迁移（cargo fix --edition）",
            色::蓝("[修复]")
        );
        if 需要迁移(项目根) {
            let 迁移成功 = Command::new(真实cargo())
                .args(["fix", "--edition", "--allow-dirty", "--allow-no-vcs"])
                .current_dir(项目根)
                .status()
                .map(|态| 态.success())
                .unwrap_or(false);
            if 迁移成功 {
                升级edition(项目根);
            } else {
                println!(
                    "{} 迁移未完全成功，edition 保持不变，可重试",
                    色::黄("[注意]")
                );
            }
        } else {
            println!("       已是 2024 或更新 edition，跳过");
        }

        println!(
            "{} 第 3/4 步：Clippy 自动修复（clippy --fix，含全局淘汰清单）",
            色::蓝("[修复]")
        );
        let _ = clippy命令(项目根, &[], true).status();

        println!("{} 第 4/4 步：复检", 色::蓝("[修复]"));
        let 码 = 执行宪法体检(项目根, &[], true);
        if 码 == 0 {
            println!("{} 修复完成，当前代码符合宪法", 色::绿("[完成]"));
        } else {
            println!(
                "{} 仍有违宪项：多为需要人工决策的规则，按报错逐条处理后再试",
                色::黄("[注意]")
            );
        }
        码
    }

    /// 生成 AGENTS.md + CLAUDE.md（事前引导层，供 AI 智能体开工前读取）
    pub fn 执行初始化(项目根: &Path) -> i32 {
        let 内容 = 宪法规则::AGENTS内容();
        for 名 in ["AGENTS.md", "CLAUDE.md"] {
            let 路径 = 项目根.join(名);
            if 路径.exists() {
                println!(
                    "{} {名} 已存在，未覆盖（删除后重跑可再生成）",
                    色::黄("[跳过]")
                );
                continue;
            }
            match std::fs::write(&路径, &内容) {
                Ok(()) => println!("{} 已生成 {名}", 色::绿("[完成]")),
                Err(错) => {
                    println!("{} 写入 {名} 失败：{错}", 色::红("[失败]"));
                    return 1;
                }
            }
        }
        println!("{} AI 智能体将在开工前自动读取以上文件", 色::蓝("[引导]"));
        0
    }

    fn 一行(标题: &str, 可用: bool, 详情: String) {
        let 记号 = if 可用 {
            色::绿("[有]")
        } else {
            色::红("[无]")
        };
        println!("{记号} {标题}：{详情}");
    }

    fn 命令版本(程序: &Path, 参数: &[&str]) -> Option<String> {
        let 出 = Command::new(程序).args(参数).output().ok()?;
        let 文 = String::from_utf8_lossy(&出.stdout).to_string();
        let 首 = 文.lines().next().unwrap_or("").to_string();
        if 首.is_empty() { None } else { Some(首) }
    }

    /// 环境自检：工具链、组件、清单、接管状态、逃生舱
    pub fn 执行自检() -> i32 {
        println!("宪法守卫 v{} · 环境自检\n", 宪法规则::守卫版本);

        let 真 = 真实cargo();
        let cargo版 = 命令版本(&真, &["--version"]);
        一行(
            "原生 cargo",
            cargo版.is_some(),
            match &cargo版 {
                Some(v) => format!("{}（{v}）", 真.display()),
                None => "未找到可用的 cargo".to_string(),
            },
        );

        let rustup在 = std::env::var("PATH").ok().and_then(|路径串| {
            路径串.split(':').find_map(|目录| {
                let 候选 = Path::new(目录).join("rustup");
                if 候选.is_file() { Some(候选) } else { None }
            })
        });
        let rustup版 = rustup在
            .as_ref()
            .and_then(|程| 命令版本(程, &["--version"]));
        一行(
            "rustup",
            rustup版.is_some(),
            match &rustup版 {
                Some(v) => v.to_string(),
                None => "未安装（非 rustup 环境不影响守卫基本功能）".to_string(),
            },
        );

        let clippy版 = 命令版本(&真, &["clippy", "--version"]);
        一行(
            "clippy 组件",
            clippy版.is_some(),
            match &clippy版 {
                Some(v) => v.to_string(),
                None => "未安装：rustup component add clippy".to_string(),
            },
        );

        let fmt版 = 命令版本(&真, &["fmt", "--version"]);
        一行(
            "rustfmt 组件",
            fmt版.is_some(),
            match &fmt版 {
                Some(v) => v.to_string(),
                None => "未安装：rustup component add rustfmt".to_string(),
            },
        );

        let audit版 = 命令版本(Path::new("cargo-audit"), &["--version"]);
        println!(
            "{} cargo-audit（可选，供应链审计）：{}",
            if audit版.is_some() {
                色::绿("[有]")
            } else {
                色::暗("[无]")
            },
            audit版.unwrap_or_else(|| "未安装：cargo install cargo-audit".to_string())
        );

        let 清单在 = 全局清单目录().is_some();
        一行(
            "全局淘汰清单",
            清单在,
            if 清单在 {
                "~/.cargo/constitutional-guard/clippy.toml".to_string()
            } else {
                "未安装：重跑 工具/编译安装.sh".to_string()
            },
        );

        if let Some(家) = std::env::var_os("HOME") {
            let 接管文件 = Path::new(&家)
                .join(".cargo")
                .join("宪法守卫")
                .join("bin")
                .join("cargo");
            let 已接管 = 接管文件.is_file();
            一行(
                "全局接管",
                已接管,
                if 已接管 {
                    "已安装 wrapper（未开新终端时 PATH 可能尚未生效）".to_string()
                } else {
                    "未接管：运行 工具/全局接管.sh".to_string()
                },
            );
        }

        let 项目 = 项目探测::项目根(std::path::Path::new("."));
        println!(
            "{} 体检缓存：{}",
            色::蓝("[缓存]"),
            缓存管理::缓存状态一句话(项目.as_deref())
        );

        let 逃生舱 = std::env::var_os("CONSTITUTION_OVERRIDE").is_some();
        println!(
            "{} 逃生舱：{}",
            if 逃生舱 {
                色::黄("[开]")
            } else {
                色::绿("[关]")
            },
            if 逃生舱 {
                "CONSTITUTION_OVERRIDE 已设置，宪法暂时旁路".to_string()
            } else {
                "未设置（正常）".to_string()
            }
        );

        0
    }
}

mod 主流程 {
    //! 参数分发：子命令、透传、接管三态

    use crate::输出风格 as 色;
    use crate::{命令解析, 宪法规则, 检查执行, 缓存管理, 项目探测};
    use std::process::Command;

    pub fn 运行() {
        let 原始: Vec<String> = std::env::args().skip(1).collect();
        let 参数 = crate::剥离守卫名(原始);

        let 首 = 参数.first().map(String::as_str);
        match 首 {
            None => 用法(),
            Some("版本") | Some("--version") | Some("-V") => {
                println!("宪法守卫 {}", 宪法规则::守卫版本);
            }
            Some("规则") => 宪法规则::打印规则(),
            Some("自检") => {
                let 码 = 检查执行::执行自检();
                std::process::exit(码);
            }
            Some("初始化") => 以项目执行(检查执行::执行初始化),
            Some("修复") => 以项目执行(检查执行::执行修复),
            Some("体检") => 以项目执行(|根| 检查执行::执行宪法体检(根, &[], true)),
            Some(首) if 命令解析::是否透传(首) => 透传(&参数),
            Some(首) if 命令解析::是否接管(首) => 接管(&参数),
            // 未知子命令：透传给原生 cargo，保持其原生报错行为一致
            Some(_) => 透传(&参数),
        }
    }

    fn 以项目执行(动作: impl Fn(&std::path::Path) -> i32) {
        match 项目探测::项目根(std::path::Path::new(".")) {
            Some(根) => {
                let 码 = 动作(&根);
                std::process::exit(码);
            }
            None => {
                println!(
                    "{} 当前不在 Cargo 项目内（未找到 Cargo.toml）",
                    色::红("[失败]")
                );
                std::process::exit(1);
            }
        }
    }

    fn 接管(参数: &[String]) {
        let 根 = 项目探测::项目根(std::path::Path::new("."));
        let Some(根) = 根 else {
            // 非 Cargo 目录：直接透传，不报错（兑现 README 承诺）
            println!("{} 当前不在 Cargo 项目内，直接透传", 色::蓝("[透传]"));
            透传(参数);
        };

        if 项目探测::是否豁免(&根) {
            println!("{} 本项目启用宪法豁免（宪法守卫.关闭）", 色::蓝("[豁免]"));
            透传(参数);
        }

        if 参数.iter().any(|项| 项.as_str() == "--fix") {
            // 用户明确要求自动修复，直接放行让官方 --fix 工作
            println!(
                "{} 检测到 --fix（自动修复模式），直接透传",
                色::蓝("[透传]")
            );
            透传(参数);
        }

        if std::env::var_os("CONSTITUTION_OVERRIDE").is_some() {
            println!(
                "{} 逃生舱生效：本次绕过宪法，请尽快整改",
                色::红("[逃生舱]")
            );
            透传(参数);
        }

        if !缓存管理::校验并记录(&根) {
            println!("{} 宪法条文已更新，本次全量复检", 色::黄("[复检]"));
        }

        let 码 = 检查执行::执行宪法体检(&根, 参数, false);
        if 码 != 0 {
            println!(
                "{} 原命令未执行。临时放行：CONSTITUTION_OVERRIDE=1 cargo {}",
                色::红("[拦截]"),
                命令解析::规范名(参数.first().map(String::as_str).unwrap_or(""))
            );
            std::process::exit(码);
        }
        透传(参数);
    }

    fn 透传(参数: &[String]) -> ! {
        let 真 = 检查执行::真实cargo();
        match Command::new(&真).args(参数).status() {
            Ok(态) => std::process::exit(态.code().unwrap_or(1)),
            Err(错) => {
                eprintln!("宪法守卫无法启动原生 cargo（{}）：{错}", 真.display());
                std::process::exit(125);
            }
        }
    }

    fn 用法() {
        println!("宪法守卫 v{} —— 全局 cargo 宪法拦截器", 宪法规则::守卫版本);
        println!();
        println!("用法（全局接管后）：");
        println!("  cargo 宪法守卫 规则        查看现行宪法（铁律 + 警告组 + 淘汰清单）");
        println!("  cargo 宪法守卫 体检        手动执行一次宪法体检");
        println!("  cargo 宪法守卫 修复        格式化 → edition 迁移 → clippy 自动修复 → 复检");
        println!("  cargo 宪法守卫 初始化      生成 AGENTS.md / CLAUDE.md（AI 事前引导）");
        println!("  cargo 宪法守卫 自检        检查工具链、组件、接管状态");
        println!("  cargo 宪法守卫 版本        显示版本");
        println!();
        println!("接管行为：build/check/clippy/run/test/bench（含 b/c/r/t 别名）先体检后执行；");
        println!("          其余命令直接透传。逃生舱：CONSTITUTION_OVERRIDE=1 或 宪法守卫.关闭");
    }
}

/// 兼容两种调用形态：接管脚本会原样转发（含「宪法守卫」动词），
/// cargo 原生子命令机制则只转发其后的参数。这里统一剥掉守卫自身名。
fn 剥离守卫名(参数: Vec<String>) -> Vec<String> {
    let mut 参数 = 参数;
    let 是守卫名 = 参数
        .first()
        .map(String::as_str)
        .is_some_and(|首| 首 == "宪法守卫" || 首 == "守卫" || 首 == "constitutional-guard");
    if 是守卫名 {
        参数.remove(0);
    }
    参数
}

fn main() {
    // 熔断：守卫自身 panic / 内部错误 → 退出码 125，
    // 全局接管脚本检测到 125 会自动回退原生 cargo（执法者死了，项目照常编译）。
    std::panic::set_hook(Box::new(|信息| {
        eprintln!("\x1b[31;1m宪法守卫内部错误：{信息}\x1b[0m");
        eprintln!("\x1b[33m已触发熔断（退出码 125），接管脚本将自动回退原生 cargo\x1b[0m");
        std::process::exit(125);
    }));
    主流程::运行();
}

#[cfg(test)]
mod 守卫自测 {
    // 注意：测试代码同样遵守宪法（无 unwrap / expect / panic / println / 索引切片）
    use crate::命令解析;
    use crate::宪法规则;

    #[test]
    fn 铁律恰好二十条() {
        assert_eq!(宪法规则::拒绝规则.len(), 20, "拒绝规则数量应为 20");
    }

    #[test]
    fn 铁律名称无重复() {
        let mut 名单: Vec<&str> = 宪法规则::拒绝规则.iter().map(|(名, _)| *名).collect();
        let 总数 = 名单.len();
        名单.sort_unstable();
        名单.dedup();
        assert_eq!(名单.len(), 总数, "铁律名称存在重复");
    }

    #[test]
    fn 宪法参数完整() {
        let 参数 = 宪法规则::宪法参数();
        let 拒绝数 = 参数.iter().filter(|项| 项.as_str() == "-D").count();
        let 警告数 = 参数.iter().filter(|项| 项.as_str() == "-W").count();
        assert_eq!(拒绝数, 20, "-D 数量应等于铁律数");
        assert_eq!(警告数, 3, "-W 数量应等于警告组数");
        assert!(
            参数.contains(&"deprecated".to_string()),
            "应包含官方淘汰清单开关"
        );
        assert!(参数.contains(&"clippy::unwrap_used".to_string()));
    }

    #[test]
    fn 接管与透传边界() {
        for 命 in [
            "build", "b", "check", "c", "clippy", "run", "r", "test", "t", "bench",
        ] {
            assert!(命令解析::是否接管(命), "{命} 应被接管");
            assert!(!命令解析::是否透传(命), "{命} 不应在透传表");
        }
        for 命 in [
            "add", "update", "remove", "new", "init", "doc", "fmt", "install", "clean", "help",
        ] {
            assert!(!命令解析::是否接管(命), "{命} 不应被接管");
            assert!(命令解析::是否透传(命), "{命} 应直接透传");
        }
        assert!(!命令解析::是否接管("宪法守卫"), "守卫自身动词不进接管表");
    }

    #[test]
    fn 别名归一() {
        assert_eq!(命令解析::规范名("b"), "build");
        assert_eq!(命令解析::规范名("c"), "check");
        assert_eq!(命令解析::规范名("t"), "test");
        assert_eq!(命令解析::规范名("r"), "run");
        assert_eq!(命令解析::规范名("build"), "build");
        assert_eq!(命令解析::规范名("clippy"), "clippy");
        assert_eq!(命令解析::规范名("bench"), "bench");
        assert_eq!(命令解析::规范名("别的"), "其他");
    }

    #[test]
    fn 首个命令跳过旗标() {
        let 带旗: Vec<String> = vec!["--release".to_string(), "build".to_string()];
        assert!(matches!(命令解析::首个命令(&带旗), Some("build")));
        let 空表: Vec<String> = Vec::new();
        assert!(matches!(命令解析::首个命令(&空表), None));
    }

    #[test]
    fn 规则指纹稳定() {
        let 一 = crate::缓存管理::规则指纹();
        let 二 = crate::缓存管理::规则指纹();
        assert_eq!(一, 二, "同一宪法必须得到同一指纹");
        assert_eq!(一.len(), 16, "指纹应为 16 位十六进制");
    }

    #[test]
    fn 项目探测与豁免() {
        use crate::项目探测;
        let 基 = std::env::temp_dir().join("宪法守卫自测目录_6b05");
        let _ = std::fs::remove_dir_all(&基);
        if std::fs::create_dir_all(&基).is_ok() {
            assert!(
                matches!(项目探测::项目edition(&基), None),
                "无清单应解析为 None"
            );
            assert!(!项目探测::是否豁免(&基));
            let _ = std::fs::write(
                基.join("Cargo.toml"),
                "[package]\nname = \"x\"\nversion = \"0\"\nedition = \"2021\"\n",
            );
            assert!(matches!(项目探测::项目edition(&基), Some(版) if 版 == "2021"));
            let _ = std::fs::write(基.join("宪法守卫.关闭"), "");
            assert!(项目探测::是否豁免(&基), "豁免文件应被发现");
            let _ = std::fs::remove_dir_all(&基);
        }
    }

    #[test]
    fn 向上寻找项目根() {
        use crate::项目探测;
        let 基 = std::env::temp_dir().join("宪法守卫自测目录_根");
        let 子 = 基.join("a").join("b");
        if std::fs::create_dir_all(&子).is_ok() {
            let _ = std::fs::write(基.join("Cargo.toml"), "");
            let 找到 = 项目探测::项目根(&子);
            assert!(
                matches!(&找到, Some(根) if 根.ends_with("宪法守卫自测目录_根")),
                "应向上找到项目根"
            );
            let _ = std::fs::remove_dir_all(&基);
        }
    }

    #[test]
    fn 剥离守卫动词() {
        let 甲 = crate::剥离守卫名(vec!["宪法守卫".to_string(), "修复".to_string()]);
        assert_eq!(甲.len(), 1);
        assert_eq!(甲.first().map(String::as_str), Some("修复"));
        let 乙 = crate::剥离守卫名(vec!["build".to_string()]);
        assert_eq!(乙.len(), 1);
        let 丙 =
            crate::剥离守卫名(vec!["constitutional-guard".to_string(), "体检".to_string()]);
        assert_eq!(丙.first().map(String::as_str), Some("体检"));
    }

    #[test]
    fn AGENTS内容覆盖关键章节() {
        let 文 = 宪法规则::AGENTS内容();
        for 关键 in [
            "铁律",
            "工具链",
            "修复",
            "测试与生产同严",
            "逃生舱",
            "LazyLock",
        ] {
            assert!(文.contains(关键), "AGENTS.md 应包含「{关键}」章节");
        }
    }

    #[test]
    fn 宪法参数成对出现() {
        let 参数 = 宪法规则::宪法参数();
        assert_eq!(
            参数.first().map(String::as_str),
            Some("-D"),
            "首参必须是 -D"
        );
        // chunk 头部必须是级别旗标，且每 chunk 恰有两项；全部用安全 API，不用下标
        let 成对 = 参数
            .chunks(2)
            .all(|对| 对.len() == 2 && 对.first().is_some_and(|级| 级 == "-D" || 级 == "-W"));
        assert!(成对, "参数必须严格按 [级别, 名称] 成对出现");
    }
}

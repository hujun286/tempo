# CTP 动态库版本对照 —— .so 导出符号 × ctp2rs feature（2026-11-27 凌晨实测）

一句话结论：**feature 选错，dlsym 必炸**。午夜实测踩的坑，这里一次说清。

## 实测环境
- 实机 .so：`lib/libthostmduserapi_se.so`，MD5 `5e6d1f8131f66d28ba3a10220a95791c`（与实盘 v1.4 完全一致）
- 导出符号：`_ZN15CThostFtdcMdApi15CreateFtdcMdApiEPKcbb`（**2-bool 版**：`(const char*, bool, bool)`）

## 对照表

| ctp2rs feature 组合 | 找的符号 | 实机 .so 结果 | 结论 |
|---|---|---|---|
| 默认（含 union） | `...PKcbbb`（3-bool，多 `is_production_mode`） | ❌ dlsym 失败 → panic | **不能用** |
| `default-features=false, features=["v1alpha1","ctp_v6_7_7"]` | `...PKcbb`（2-bool） | ✅ 全链路实测通过 | **用这个** |

## 代码侧的对应调用
- 非 union（2-bool）：`MdApi::create_api(&dynlib, &flow, false, false)` —— 4 参
- union（3-bool）：`MdApi::create_api(&dynlib, &flow, false, false, true)` —— 5 参（第 5 参=生产模式）

## 验证命令（拿到新 .so 先跑这个再编译）
```bash
nm -D lib/libthostmduserapi_se.so | grep CreateFtdcMdApi
# 末尾是 PKcbb   → 2-bool → 用非 union 组合（4 参调用）
# 末尾是 PKcbbb  → 3-bool → 才可以用默认/union 组合（5 参调用）
```

## 实测记录（全部真实行情）
- 2026-11-27 01:33 黄金银夜盘：非 union 组合 16/16 前置连接+登录，24306 tick，5146 根 bar 落盘 ✅
- 同晚用默认 union 组合：`undefined symbol: ...PKcbbb`，16 个 member 线程全 panic ❌

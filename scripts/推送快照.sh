#!/usr/bin/env bash
# 推送快照 v3 —— 两地推送（2026-11-28 用户指令定版）
#   通道A：主仓 git 历史 → GitHub hujun286/tempo 的 zhipu_main 分支（专用分支，绝不碰 tempo/main 用户自有数据！）
#   通道B：主仓工作树镜像 → Gitee zhipu2 仓库「主仓镜像/」一级大目录
#   通道C：Gitee zhipu 主推（默认关闭——容量锁待用户梳理；恢复方法：ZHIPU_ENABLED=1 bash scripts/推送快照.sh）
# 铁律：凭据只在脚本内读 .gitee_推送凭据（0600），绝不打印/不写文件/不进提交信息；推送前泄漏哨兵扫描。
set -uo pipefail
cd /home/z/my-project
BEIJING_NOW=$(TZ='Asia/Shanghai' date '+%F %T')

# ── 凭据读取（key=value 三行；只取值不回显）──
get_cred() { grep -E "^$1=" .gitee_推送凭据 2>/dev/null | head -1 | cut -d= -f2-; }
ZHIPU_PASS=$(get_cred zhipu_pass)
ZHIPU2_PASS=$(get_cred zhipu2_pass)
TEMPO_PAT=$(get_cred tempo_pat)
[ -n "$ZHIPU_PASS" ] && [ -n "$ZHIPU2_PASS" ] && [ -n "$TEMPO_PAT" ] || { echo "缺凭据(.gitee_推送凭据)"; exit 1; }

# ── 泄漏哨兵：暂存区出现任何凭据特征即中止（深刻教训防线）──
# 实现说明：特征用「GitHub PAT 形态正则」+「运行时从凭据文件取值精确匹配」，脚本内零密码常量（防哨兵自触发）
sentinel() {  # 用法: sentinel <仓库目录>；扫 staged diff
  local D="$1" hit=""
  hit=$(git -C "$D" diff --cached 2>/dev/null | grep -iE "github_pat_[A-Za-z0-9_]{20,}" | head -1 || true)
  if [ -n "$hit" ]; then echo "哨兵命中：$1 暂存区疑似 GitHub PAT 形态泄漏，中止推送"; return 1; fi
  local v
  for v in "$TEMPO_PAT" "$ZHIPU_PASS" "$ZHIPU2_PASS"; do
    [ -z "$v" ] && continue
    if git -C "$D" diff --cached 2>/dev/null | grep -qF -- "$v"; then
      echo "哨兵命中：$1 暂存区含真实凭据值，中止推送"; return 1
    fi
  done
  return 0
}

# ── 主仓提交（北京时间戳）──
git add -A
sentinel /home/z/my-project || exit 1
git commit -q -m "定时/手动推送快照 ${BEIJING_NOW}" 2>/dev/null || true
echo "本地提交就绪 ${BEIJING_NOW}"

# ── 通道A：tempo zhipu_main（专用快照分支）──
# 注：github.com 直连偶发 443 超时（20261128 凌晨实测），失败自动重试一次
if ! timeout 180 git push tempo main:zhipu_main 2>/dev/null; then
  echo "通道A首次失败，重试一次"
  timeout 180 git push tempo main:zhipu_main 2>/dev/null && echo "通道A完成(tempo zhipu_main·重试)" || echo "通道A失败(tempo不可达)——历史仍安全在本地+通道B容灾"
else
  echo "通道A完成(tempo zhipu_main)"
fi

# ── 通道B：zhipu2「主仓镜像/」一级大目录 ──
# 容灾仓缺失（回收后）→ 凭据静默重克隆；镜像=tracked+未忽略untracked（.gitignore 全套排除天然生效）
if [ ! -d zhipu2_容灾仓/.git ]; then
  echo "容灾仓缺失，静默重克隆 zhipu2 ..."
  timeout 300 git clone --quiet "https://oauth2:${ZHIPU2_PASS}@gitee.com/zz237/zhipu2.git" zhipu2_容灾仓 \
    && echo "重克隆完成" || { echo "zhipu2 重克隆失败，通道B中止"; exit 0; }
fi
git ls-files --cached --others --exclude-standard -z \
  | while IFS= read -r -d '' f; do [ -f "$f" ] && printf '%s\0' "$f"; done > /tmp/镜像清单.lst
mkdir -p zhipu2_容灾仓/主仓镜像
rsync -a --delete --from0 --files-from=/tmp/镜像清单.lst /home/z/my-project/ zhipu2_容灾仓/主仓镜像/
git -C zhipu2_容灾仓 add -A 主仓镜像
sentinel /home/z/my-project/zhipu2_容灾仓 || { git -C zhipu2_容灾仓 reset; exit 1; }
git -C zhipu2_容灾仓 commit -q -m "主仓镜像同步 ${BEIJING_NOW}（zhipu容量锁期两地推送通道B）" 2>/dev/null || true
if timeout 300 git -C zhipu2_容灾仓 push origin main 2>/dev/null; then
  echo "通道B完成(zhipu2 主仓镜像/)"
else
  echo "通道B推送失败——镜像保留本地 zhipu2_容灾仓（下次自动补推）"
fi

# ── 通道C：gitee zhipu 主推（默认关，容量锁解除后 ZHIPU_ENABLED=1 恢复）──
if [ "${ZHIPU_ENABLED:-0}" = "1" ]; then
  if timeout 180 git push "https://oauth2:${ZHIPU_PASS}@gitee.com/zz237/zhipu.git" main:main 2>/dev/null; then
    echo "通道C完成(gitee zhipu)"
  else
    echo "通道C失败(gitee zhipu 仍拒收)"
  fi
else
  echo "通道C跳过(gitee zhipu 容量锁期，ZHIPU_ENABLED=1 可恢复)"
fi
echo "两地推送流程结束 ${BEIJING_NOW}"

#!/usr/bin/env bash
# rescue-term 安装脚本：在解压出的发布包目录内以 root 执行

set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
DEST=/opt/rescue-term
CFG="$DEST/config/rescue-term.yaml"

if [ "$(id -u)" -ne 0 ]; then
  echo "请以 root 运行（unit 本身就需要 root：终端里要做 systemctl 操作）" >&2
  exit 1
fi

# 先装到临时名再 mv：服务运行中时对原路径直接 install（open+覆写）
# 会 ETXTBSY（Text file busy），mv 是 rename(2) 只换目录项，进程握着的
# 旧 inode 不受影响——升级路径必须走这条（与板上 edge 的 restart.sh 同款手法）
install -D -m 0755 "$DIR/rescue-term" "$DEST/rescue-term.new"
mv -f "$DEST/rescue-term.new" "$DEST/rescue-term"

# 已有配置不覆盖（真实 token 的 yaml 只存在于设备上）
if [ ! -f "$CFG" ]; then
  install -D -m 0600 "$DIR/config/rescue-term.example.yaml" "$CFG"
  echo "已写入示例配置 $CFG —— 请先填入 endpoint / sn / token 再启动服务"
  NEED_CFG=1
fi

install -D -m 0644 "$DIR/rescue-term.service" /etc/systemd/system/rescue-term.service
systemctl daemon-reload

# 三态收尾：
# - 正在运行（升级场景）→ restart 才会换到新二进制。enable --now 对
#   active 服务的 start 是 no-op，不重启的话跑的还是旧 inode
# - 全新安装且未填配置 → 只 enable 不 start（逼先填 token）
# - 全新安装且配置就绪 → enable --now
if systemctl is-active --quiet rescue-term; then
  systemctl restart rescue-term
  echo "服务已重启到新版本。日志: journalctl -u rescue-term -f"
elif [ "${NEED_CFG:-0}" = "1" ]; then
  systemctl enable rescue-term
  echo "配置完成后执行: systemctl start rescue-term"
else
  systemctl enable --now rescue-term
  echo "已启动。日志: journalctl -u rescue-term -f"
fi

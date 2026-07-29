#!/usr/bin/env bash
# Host CPU / memory / disk / IO — baseline before blaming Kafka.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../lib/admin_common.sh"

section "01 Host resources"

subsection "Identity & uptime"
hostname
hostname -I 2>/dev/null || true
uptime
date

subsection "CPU / memory"
nproc 2>/dev/null || true
free -h 2>/dev/null || true
if have top; then
  top -b -n1 | head -20
fi

subsection "Disk usage (root, /var, log dir)"
df -h / /var "$KAFKA_LOG_DIR" 2>/dev/null || df -h
if [[ -d "$KAFKA_LOG_DIR" ]]; then
  echo "log dir size:"
  du -sh "$KAFKA_LOG_DIR" 2>/dev/null || true
  echo "segment / dir count (top-level entries):"
  ls "$KAFKA_LOG_DIR" 2>/dev/null | wc -l || true
fi

subsection "IO snapshot (iostat if available)"
if have iostat; then
  iostat -x 1 3
else
  echo "iostat not installed; skipping"
fi

subsection "Load vs Kafka process (best effort)"
if have systemctl; then
  main_pid="$(systemctl show -p MainPID --value "$KAFKA_SYSTEMD_UNIT" 2>/dev/null || true)"
  if [[ -n "${main_pid:-}" && "$main_pid" != "0" ]]; then
    ps -p "$main_pid" -o pid,pcpu,pmem,rss,vsz,etime,cmd --no-headers 2>/dev/null || true
    if [[ -r "/proc/$main_pid/status" ]]; then
      grep -E '^(Threads|VmRSS|VmSize|FDSize):' "/proc/$main_pid/status" || true
    fi
  fi
fi

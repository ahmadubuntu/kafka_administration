#!/usr/bin/env bash
# TCP connection pressure on Kafka listeners (often explains high CPU / UI lag).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../lib/common.sh"

section "06 Connections"

if ! have ss; then
  echo "ss not available; install iproute2"
  exit 0
fi

subsection "Socket summary"
ss -s

subsection "Established counts by local port"
# ss -tn Local Address:Port is field $4 (some versions) or $3 — detect header.
ss -tn state established | awk '
NR == 1 { next }
{
  # Prefer column that looks like local addr:port (contains a port at end)
  local = ($4 ~ /:[0-9]+$/) ? $4 : $3
  # IPv6: [fe80::1]:9094
  if (local ~ /^\[/) {
    sub(/^.*\]:/, "", local)
    lp = local
  } else {
    n = split(local, a, ":")
    lp = a[n]
  }
  if (lp ~ /^[0-9]+$/) c[lp]++
}
END {
  for (p in c) printf "%8d  :%s\n", c[p], p
}' | sort -rn | head -30

subsection "Kafka listener peers (9092 / 9094) — top remote hosts"
ss -tn state established | awk '
NR == 1 { next }
{
  local  = ($4 ~ /:[0-9]+$/) ? $4 : $3
  remote = ($5 ~ /:[0-9]+$/) ? $5 : $4

  if (local ~ /^\[/) {
    tmp = local
    sub(/^.*\]:/, "", tmp)
    lp = tmp
  } else {
    n = split(local, a, ":")
    lp = a[n]
  }

  if (remote ~ /^\[/) {
    rh = remote
    sub(/^\[/, "", rh)
    sub(/\]:.*/, "", rh)
  } else {
    rh = remote
    sub(/:[0-9]+$/, "", rh)
  }

  if (lp == "9092" || lp == "9094") print lp, rh
}' | sort | uniq -c | sort -rn | head -40 | tee "$REPORT_DIR/connections_by_peer.txt"

subsection "Established on Kafka ports"
echo -n ":9094 "; ss -tn state established '( sport = :9094 )' 2>/dev/null | tail -n +2 | wc -l
echo -n ":9092 "; ss -tn state established '( sport = :9092 )' 2>/dev/null | tail -n +2 | wc -l
echo -n ":9093 "; ss -tn state established '( sport = :9093 )' 2>/dev/null | tail -n +2 | wc -l

subsection "Open files (Kafka process vs ulimit)"
main_pid=""
if have systemctl; then
  main_pid="$(systemctl show -p MainPID --value "$KAFKA_SYSTEMD_UNIT" 2>/dev/null || true)"
fi
if [[ -z "${main_pid:-}" || "$main_pid" == "0" ]]; then
  main_pid="$(pgrep -f 'kafka.Kafka' | head -1 || true)"
fi
if [[ -n "${main_pid:-}" && -r "/proc/$main_pid/limits" ]]; then
  echo "pid=$main_pid"
  grep -i 'open files' "/proc/$main_pid/limits" || true
  if [[ -d "/proc/$main_pid/fd" ]]; then
    # may need privileges; best-effort
    fd_count="$(ls "/proc/$main_pid/fd" 2>/dev/null | wc -l || echo 0)"
    echo "open_fd_count=$fd_count"
  fi
  if have curl; then
    curl -sf -m 5 \
      "${KAFKA_JOLOKIA_URL%/}/read/java.lang:type=OperatingSystem/OpenFileDescriptorCount,MaxFileDescriptorCount" \
      2>/dev/null | tee "$REPORT_DIR/open_file_descriptors.json" || true
    echo
  fi
else
  echo "Could not resolve Kafka PID for FD limits"
fi

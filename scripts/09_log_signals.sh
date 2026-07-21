#!/usr/bin/env bash
# Scan broker logs for auth denials, SASL handshake issues, WARN/ERROR.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../lib/common.sh"

section "09 Log signals ($KAFKA_LOG_DIR)"

if [[ ! -d "$KAFKA_LOG_DIR" ]]; then
  echo "WARNING: log dir not found: $KAFKA_LOG_DIR"
  exit 0
fi

subsection "Log files present"
ls -lh "$KAFKA_LOG_DIR"/*.log 2>/dev/null | head -30 || true

server_log="$KAFKA_LOG_DIR/server.log"
auth_log="$KAFKA_LOG_DIR/kafka-authorizer.log"
gc_log="$KAFKA_LOG_DIR/kafkaServer-gc.log"

subsection "Recent server WARN/ERROR (tail matched)"
if [[ -f "$server_log" ]]; then
  grep -E 'ERROR|WARN' "$server_log" | tail -40 | tee "$REPORT_DIR/server_warn_error_tail.txt" || true
else
  echo "no server.log"
fi

subsection "SASL handshake anomalies"
if [[ -f "$server_log" ]]; then
  echo "count(during SASL handshake): $(grep -c 'during SASL handshake' "$server_log" || true)"
  grep 'during SASL handshake' "$server_log" | tail -20 || true
else
  echo "no server.log"
fi

subsection "Authorizer denials (sample + recent rate)"
if [[ -f "$auth_log" ]]; then
  ls -lh "$auth_log"
  echo "total lines: $(wc -l <"$auth_log")"
  echo "recent Denied sample:"
  grep -i 'Denied' "$auth_log" | tail -20 || true
  # rough per-minute for current hour (best effort)
  hour_prefix="$(date '+%Y-%m-%d %H:')"
  echo "Denied lines this hour (prefix $hour_prefix): $(grep -c "$hour_prefix" "$auth_log" || true)"
else
  echo "no kafka-authorizer.log"
fi

subsection "GC pause sample (last 20 Pause lines)"
if [[ -f "$gc_log" ]]; then
  grep -E 'Pause Young|Pause Full|Full GC' "$gc_log" | tail -20 || true
else
  echo "no kafkaServer-gc.log (path may differ)"
fi

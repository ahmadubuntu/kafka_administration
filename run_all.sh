#!/usr/bin/env bash
# Run the full Kafka admin health suite on the current machine.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$ROOT/lib/common.sh"

STAMP="$(date +%Y%m%d_%H%M%S)"
export REPORT_DIR="${REPORT_DIR:-$ROOT/reports}/$STAMP"
mkdir -p "$REPORT_DIR"

section "Kafka admin suite"
kv "host" "$(hostname)"
kv "started" "$(timestamp)"
kv "reports" "$REPORT_DIR"
info "Color output on TTY; set NO_COLOR=1 to disable."

print_env_summary | tee "$REPORT_DIR/00_env.txt"

SCRIPTS=(
  01_host_resources.sh
  02_service_status.sh
  03_cluster_health.sh
  04_admin_ops_timing.sh
  05_broker_config.sh
  06_connections.sh
  07_jmx_exporter.sh
  08_jolokia_latency.sh
  09_log_signals.sh
  11_capacity_estimate.sh
  10_summary_hints.sh
)

total="${#SCRIPTS[@]}"
failed=0
failed_names=()
idx=0
for s in "${SCRIPTS[@]}"; do
  idx=$((idx + 1))
  path="$ROOT/scripts/$s"
  out="$REPORT_DIR/${s%.sh}.txt"
  step_banner "$idx" "$total" "$s"
  set +e
  bash "$path" 2>&1 | tee "$out"
  rc=${PIPESTATUS[0]}
  set -e
  if [[ $rc -ne 0 ]]; then
    warn "$s exited $rc (continuing)"
    echo "WARN: $s exited $rc" >>"$out"
    failed=$((failed + 1))
    failed_names+=("$s")
  else
    ok "$s finished"
  fi
done

section "Suite complete"
kv "reports" "$REPORT_DIR"
kv "failed" "$failed / $total"
if (( failed > 0 )); then
  warn "Failed: ${failed_names[*]}"
else
  ok "All scripts completed successfully"
fi
exit 0

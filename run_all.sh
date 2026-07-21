#!/usr/bin/env bash
# Run the full Kafka admin health suite on the current machine.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$ROOT/lib/common.sh"

STAMP="$(date +%Y%m%d_%H%M%S)"
export REPORT_DIR="${REPORT_DIR:-$ROOT/reports}/$STAMP"
mkdir -p "$REPORT_DIR"

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

failed=0
for s in "${SCRIPTS[@]}"; do
  path="$ROOT/scripts/$s"
  out="$REPORT_DIR/${s%.sh}.txt"
  echo
  echo ">>>> Running $s"
  set +e
  bash "$path" 2>&1 | tee "$out"
  rc=${PIPESTATUS[0]}
  set -e
  if [[ $rc -ne 0 ]]; then
    echo "WARN: $s exited $rc (continuing)" | tee -a "$out"
    failed=$((failed + 1))
  fi
done

section "Done"
echo "Reports: $REPORT_DIR"
echo "Failed scripts: $failed"
exit 0

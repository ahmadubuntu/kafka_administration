#!/usr/bin/env bash
# Deep single-broker Kafka admin diagnostics (scripts/01_…–11_…).
# Formerly named run_all.sh — the toolkit-wide orchestrator is now ./run_all.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$ROOT/lib/admin_common.sh"
# shellcheck disable=SC1091
source "$ROOT/lib/tasks.sh"

# id|prereqs|title|aliases  → script under scripts/
TASK_CATALOG=(
  "host||Host resources|host,host resources,01,01_host_resources"
  "service||Service status|service,service status,systemd,02,02_service_status"
  "cluster||Cluster health|cluster,cluster health,03,03_cluster_health"
  "admin_ops||Admin ops timing|admin ops,timing,04,04_admin_ops_timing"
  "broker_config||Broker config|broker config,05,05_broker_config"
  "connections||Connections|connections,06,06_connections"
  "jmx||JMX exporter|jmx,jmx exporter,07,07_jmx_exporter"
  "jolokia||Jolokia latency|jolokia,08,08_jolokia_latency"
  "logs||Log signals|logs,log signals,09,09_log_signals"
  "capacity||Capacity estimate|capacity,11,11_capacity_estimate"
  "summary||Summary hints|summary,hints,10,10_summary_hints"
)

declare -A TASK_SCRIPT=(
  [host]=01_host_resources.sh
  [service]=02_service_status.sh
  [cluster]=03_cluster_health.sh
  [admin_ops]=04_admin_ops_timing.sh
  [broker_config]=05_broker_config.sh
  [connections]=06_connections.sh
  [jmx]=07_jmx_exporter.sh
  [jolokia]=08_jolokia_latency.sh
  [logs]=09_log_signals.sh
  [capacity]=11_capacity_estimate.sh
  [summary]=10_summary_hints.sh
)

TASKS_LIST_EXAMPLES="  $(basename "$0") --only host,service
  $(basename "$0") --only \"Host resources\"
  $(basename "$0") --skip jolokia,jmx
  $(basename "$0") --ask-tasks"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Deep single-broker Kafka admin suite (run on a broker, or via ./run_via_ssh.sh).

Options:
  --only TASKS     Run only these steps (ids/titles)
  --skip TASKS     Skip these steps
  --ask-tasks      Interactive picker
  --list-tasks     List steps and exit
  -h, --help

Toolkit-wide runner (HA + min.isr + this suite): ./run_all.sh
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --only) ONLY_TASKS="$2"; shift 2 ;;
      --skip) SKIP_TASKS="$2"; shift 2 ;;
      --ask-tasks) ASK_TASKS=1; shift ;;
      --list-tasks) LIST_TASKS=1; shift ;;
      -h|--help) usage; exit 0 ;;
      *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
    esac
  done
}

main() {
  parse_args "$@"

  if [[ "$LIST_TASKS" == "1" ]]; then
    tasks_list
    exit 0
  fi

  STAMP="$(date +%Y%m%d_%H%M%S)"
  export REPORT_DIR="${REPORT_DIR:-$ROOT/reports}/admin_${STAMP}"
  mkdir -p "$REPORT_DIR"

  section "Kafka admin suite"
  kv "host" "$(hostname)"
  kv "started" "$(timestamp)"
  kv "reports" "$REPORT_DIR"
  info "Color output on TTY; set NO_COLOR=1 to disable."
  info "Formerly ./run_all.sh — toolkit orchestrator is now ./run_all.sh"

  tasks_select || exit $?

  print_env_summary | tee "$REPORT_DIR/00_env.txt"

  local -a selected=()
  read -r -a selected <<<"$SELECTED_TASKS"
  local total="${#selected[@]}"
  local failed=0
  local -a failed_names=()
  local idx=0 id script path out rc

  for id in "${selected[@]}"; do
    idx=$((idx + 1))
    script="${TASK_SCRIPT[$id]:-}"
    if [[ -z "$script" ]]; then
      warn "No script mapped for task '$id' — skipping"
      continue
    fi
    path="$ROOT/scripts/$script"
    out="$REPORT_DIR/${script%.sh}.txt"
    step_banner "$idx" "$total" "$script ($id)"
    set +e
    bash "$path" 2>&1 | tee "$out"
    rc=${PIPESTATUS[0]}
    set -e
    if [[ $rc -ne 0 ]]; then
      warn "$script exited $rc (continuing)"
      echo "WARN: $script exited $rc" >>"$out"
      failed=$((failed + 1))
      failed_names+=("$script")
    else
      ok "$script finished"
    fi
  done

  section "Suite complete"
  kv "reports" "$REPORT_DIR"
  kv "failed" "$failed / $total"
  if (( failed > 0 )); then
    warn "Failed: ${failed_names[*]}"
  else
    ok "All selected scripts completed successfully"
  fi
  exit 0
}

main "$@"

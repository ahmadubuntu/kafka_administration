#!/usr/bin/env bash
# Kafka HA cluster health check
# Usage: ./check_kafka_ha.sh -c config/clusters/devkafka.env [options]
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_VERSION="$(cat "${ROOT_DIR}/VERSION" 2>/dev/null | tr -d '[:space:]' || echo 0.0.0)"

# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/common.sh"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/parallel.sh"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/ssh.sh"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/connectivity.sh"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/ports_matrix.sh"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/services.sh"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/kafka_cluster.sh"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/capacity.sh"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/os_security.sh"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/report.sh"

CONFIG_FILE=""
VERBOSE=0
JSON_OUT=0
SKIP_LAG=0
USE_SUDO=1
NONINTERACTIVE=0
REPORT_FILE_OVERRIDE=""
SSH_USER_OVERRIDE="${SSH_USER_OVERRIDE:-}"
SUDO_PASSWORD_ENV="${SUDO_PASSWORD:-${SUDO_PASSWORD_ENV:-}}"

usage() {
  cat <<EOF
Usage: $(basename "$0") -c CONFIG.env [options]

Kafka HA Health Check v${SCRIPT_VERSION}

Options:
  -c, --config FILE     Cluster inventory (.env)
  -u, --user USER       SSH username (skip prompt)
  -n, --no-sudo         Do not use sudo on remotes
  --skip-lag            Skip consumer-group lag describe
  --json                Emit JSON summary at end
  -o, --report FILE     Write report to FILE (default: reports/kafkaha-*.log)
  -v, --verbose         Verbose details
  -y, --yes             Non-interactive (use defaults / env)
  -h, --help            Show help

Exit codes: 0=PASS, 1=WARN/SLOW, 2=FAIL
Deep single-broker diagnostics remain under ./scripts and ./run_all.sh
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -c|--config) CONFIG_FILE="$2"; shift 2 ;;
      -u|--user) SSH_USER_OVERRIDE="$2"; shift 2 ;;
      -n|--no-sudo) USE_SUDO=0; shift ;;
      --skip-lag) SKIP_LAG=1; shift ;;
      --json) JSON_OUT=1; shift ;;
      -o|--report) REPORT_FILE_OVERRIDE="$2"; shift 2 ;;
      -v|--verbose) VERBOSE=1; shift ;;
      -y|--yes) NONINTERACTIVE=1; shift ;;
      -h|--help) usage; exit 0 ;;
      *) loge "Unknown argument: $1"; usage; exit 2 ;;
    esac
  done
}

load_config() {
  if [[ -z "$CONFIG_FILE" ]]; then
    loge "Missing -c/--config"; usage; exit 2
  fi
  if [[ ! -f "$CONFIG_FILE" ]]; then
    if [[ -f "${ROOT_DIR}/${CONFIG_FILE}" ]]; then
      CONFIG_FILE="${ROOT_DIR}/${CONFIG_FILE}"
    else
      loge "Config not found: $CONFIG_FILE"; exit 2
    fi
  fi
  set -a
  # shellcheck disable=SC1090
  source "$CONFIG_FILE"
  set +a
}

require_local_bins() {
  local missing=0
  have_cmd ssh || { loge "Required command not found: ssh"; missing=1; }
  (( missing == 0 )) || exit 2
  have_cmd nc || have_cmd timeout || have_cmd telnet || {
    loge "Need nc, timeout, or telnet for TCP checks"; exit 2
  }
}

run_ssh_sweep() {
  section "SSH connectivity (telnet/tcp → SSH, PASS / SLOW / FAIL)"
  emit "  ${C_DIM}SLOW≥${SSH_SLOW_WARN_MS:-3000}ms; retry timeout=${SSH_RETRY_TIMEOUT_SEC:-35}s if port open but SSH fails${C_RESET}"
  local hosts=() all=()
  csv_to_array hosts "${LB_HOSTS:-}"; for h in "${hosts[@]}"; do all+=("lb:$h|$h"); done
  csv_to_array hosts "${BROKER_HOSTS:-}"; for h in "${hosts[@]}"; do all+=("broker:$h|$h"); done
  csv_to_array hosts "${CONTROLLER_HOSTS:-}"; for h in "${hosts[@]}"; do
    # skip duplicates already listed as brokers
    local skip=0 b
    for b in ${BROKER_HOSTS//,/ }; do [[ "$b" == "$h" ]] && skip=1 && break; done
    (( skip )) || all+=("controller:$h|$h")
  done

  if [[ ${#all[@]} -eq 0 ]]; then
    record_check "ssh" "hosts" "FAIL" "BROKER_HOSTS empty in inventory"
    return
  fi

  _ssh_one() {
    local spec="$1"
    local label="${spec%%|*}"
    local host="${spec#*|}"
    check_ssh_host "$host" "$label"
  }
  run_parallel_fn _ssh_one "${all[@]}"
}

main() {
  parse_args "$@"
  load_config
  require_local_bins
  init_result_store
  init_ssh_status_store
  init_parallel_store
  install_interrupt_traps
  init_report_file "${REPORT_FILE_OVERRIDE}"

  emit "${C_BOLD}Kafka HA Health Check v${SCRIPT_VERSION}${C_RESET} — ${CLUSTER_NAME:-cluster}"
  emit "Config: ${CONFIG_FILE}"
  emit "Time:   $(date -Is)"
  emit "Parallel jobs: ${PARALLEL_JOBS:-8}"

  prompt_credentials

  set +e
  run_ssh_sweep
  run_port_matrix
  run_kafka_cascade
  check_vip_owner
  check_kafka_services
  check_kafka_membership
  check_kafka_controller
  check_kafka_partition_health
  check_kafka_disk_logdirs
  check_kafka_consumer_lag
  check_kafka_config_drift
  check_kafka_capacity
  check_all_os_security
  set -e

  local rc=0
  print_summary || rc=$?
  if [[ -n "${REPORT_FILE:-}" ]]; then
    {
      echo "----------------------------------------"
      echo "Finished: $(date -Is)"
      echo "Exit: ${rc}"
    } >> "$REPORT_FILE"
  fi
  exit "$rc"
}

main "$@"

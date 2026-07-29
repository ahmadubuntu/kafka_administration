#!/usr/bin/env bash
# Run all kafka_realtime_check suites: HA check, min.insync.replicas scan, broker admin.
#
# Usage:
#   ./run_all.sh -c config/clusters/dmzkafka.env -u USER -y
#   ./run_all.sh -c CONFIG.env -u USER -y --only ha
#   ./run_all.sh -c CONFIG.env -u USER -y --only ha,min_isr
#   ./run_all.sh --only admin                    # local broker admin suite
#   ./run_all.sh --list-tasks
#   ./run_all.sh -c CONFIG.env -u USER -y --only ha -- --only os
#
# Args after "--" are forwarded to child entrypoints that accept them
# (check_kafka_ha.sh / fix_topic_min_isr.sh / run_admin_suite.sh).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_VERSION="$(cat "${ROOT}/VERSION" 2>/dev/null | tr -d '[:space:]' || echo 0.0.0)"

# Prefer HA common for emit/loge when available; tasks.sh works either way.
# shellcheck disable=SC1091
source "$ROOT/lib/common.sh"
# shellcheck disable=SC1091
source "$ROOT/lib/tasks.sh"

CONFIG_FILE=""
SSH_USER_OVERRIDE="${SSH_USER_OVERRIDE:-}"
NONINTERACTIVE=0
APPLY_ISR=0
CHILD_ARGS=()

TASK_CATALOG=(
  "ha||HA cluster health check|ha,health,check_kafka_ha,kafka ha"
  "min_isr||Topic min.insync.replicas scan|min_isr,min isr,isr,fix_topic,topics isr"
  "admin||Deep broker admin suite|admin,broker admin,admin suite,run_admin_suite"
)
TASKS_LIST_EXAMPLES="  $(basename "$0") -c CONFIG.env -u USER -y
  $(basename "$0") -c CONFIG.env -u USER -y --only ha
  $(basename "$0") -c CONFIG.env -u USER -y --only ha,min_isr
  $(basename "$0") --only admin -- --only host,service
  $(basename "$0") -c CONFIG.env -u USER -y --only ha -- --only os
  $(basename "$0") --ask-tasks"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options] [-- child-args...]

Kafka realtime check orchestrator v${SCRIPT_VERSION}

Runs selected top-level suites under this directory:
  ha       → ./check_kafka_ha.sh
  min_isr  → ./fix_topic_min_isr.sh (scan-only unless --apply)
  admin    → ./run_admin_suite.sh (on this host; use ./run_via_ssh.sh for remote)

Options:
  -c, --config FILE   Cluster inventory (required for ha / min_isr)
  -u, --user USER     SSH username (passed to ha / min_isr)
  --only TASKS        Suites to run (default: all)
  --skip TASKS        Suites to skip
  --ask-tasks         Interactive suite picker
  --list-tasks        List suites and exit
  --apply             For min_isr: pass --apply (otherwise scan-only)
  -y, --yes           Non-interactive
  -h, --help

Anything after "--" is forwarded to each selected child.
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -c|--config) CONFIG_FILE="$2"; shift 2 ;;
      -u|--user) SSH_USER_OVERRIDE="$2"; shift 2 ;;
      --only) ONLY_TASKS="$2"; shift 2 ;;
      --skip) SKIP_TASKS="$2"; shift 2 ;;
      --ask-tasks) ASK_TASKS=1; shift ;;
      --list-tasks) LIST_TASKS=1; shift ;;
      --apply) APPLY_ISR=1; shift ;;
      -y|--yes) NONINTERACTIVE=1; shift ;;
      -h|--help) usage; exit 0 ;;
      --) shift; CHILD_ARGS+=("$@"); break ;;
      *) loge "Unknown argument: $1"; usage; exit 2 ;;
    esac
  done
}

_resolve_config() {
  if [[ -z "$CONFIG_FILE" ]]; then
    return 1
  fi
  if [[ ! -f "$CONFIG_FILE" && -f "${ROOT}/${CONFIG_FILE}" ]]; then
    CONFIG_FILE="${ROOT}/${CONFIG_FILE}"
  fi
  [[ -f "$CONFIG_FILE" ]]
}

_run_suite() {
  local id="$1"
  local -a cmd=()
  case "$id" in
    ha)
      if ! _resolve_config; then
        loge "ha requires -c/--config"; return 2
      fi
      cmd=("${ROOT}/check_kafka_ha.sh" -c "$CONFIG_FILE")
      [[ -n "$SSH_USER_OVERRIDE" ]] && cmd+=(-u "$SSH_USER_OVERRIDE")
      [[ "$NONINTERACTIVE" == "1" ]] && cmd+=(-y)
      cmd+=("${CHILD_ARGS[@]}")
      ;;
    min_isr)
      if ! _resolve_config; then
        loge "min_isr requires -c/--config"; return 2
      fi
      cmd=("${ROOT}/fix_topic_min_isr.sh" -c "$CONFIG_FILE")
      [[ -n "$SSH_USER_OVERRIDE" ]] && cmd+=(-u "$SSH_USER_OVERRIDE")
      [[ "$NONINTERACTIVE" == "1" ]] && cmd+=(-y)
      [[ "$APPLY_ISR" == "1" ]] && cmd+=(--apply)
      cmd+=("${CHILD_ARGS[@]}")
      ;;
    admin)
      cmd=("${ROOT}/run_admin_suite.sh")
      cmd+=("${CHILD_ARGS[@]}")
      ;;
    *)
      loge "Unknown suite: $id"; return 2
      ;;
  esac

  section "Suite: ${id}"
  emit "Command: ${cmd[*]}"
  set +e
  "${cmd[@]}"
  local rc=$?
  set -e
  if [[ $rc -eq 0 ]]; then
    emit "${C_GREEN}Suite ${id} finished OK (rc=0)${C_RESET}"
  else
    emit "${C_YELLOW}Suite ${id} finished with rc=${rc}${C_RESET}"
  fi
  return "$rc"
}

main() {
  parse_args "$@"

  if [[ "$LIST_TASKS" == "1" ]]; then
    tasks_list
    exit 0
  fi

  emit "${C_BOLD}kafka_realtime_check orchestrator v${SCRIPT_VERSION}${C_RESET}"
  emit "Time: $(date -Is)"

  tasks_select || exit $?

  local -a selected=()
  read -r -a selected <<<"$SELECTED_TASKS"
  local id worst=0 rc
  for id in "${selected[@]}"; do
    rc=0
    _run_suite "$id" || rc=$?
    (( rc > worst )) && worst=$rc
  done

  section "Orchestrator complete"
  emit "Worst child exit: ${worst}"
  exit "$worst"
}

main "$@"

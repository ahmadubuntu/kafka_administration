#!/usr/bin/env bash
# Batch topic admin: list / delete / set configs by name pattern.
#
# Lives outside kafka_realtime_check — uses shared libs from that toolkit.
#
# Safety defaults:
#   - mutations need --apply
#   - --pattern is required for --delete / --set-config
#   - overly broad patterns (.*, ^, .) refused unless --force-broad
#   - names starting with '_' skipped unless --include-internal
#   - parallel workers default 8
#
# Usage:
#   ./manage_topics.sh -c ../kafka_realtime_check/config/clusters/devkafka.env -u USER \
#     --pattern '^cursor-test-' --list
#   ./manage_topics.sh -c … -u USER -y --pattern '^cursor-test-' \
#     --set-config retention.ms=86400000 --apply --jobs 8
#   ./manage_topics.sh -c … -u USER -y --pattern '^cursor-test-' --delete --apply
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_VERSION="$(cat "${ROOT_DIR}/VERSION" 2>/dev/null | tr -d '[:space:]' || echo 0.0.0)"
# Shared HA libs (sibling toolkit)
LIB_DIR="${LIB_DIR:-${ROOT_DIR}/../kafka_realtime_check/lib}"

# shellcheck source=/dev/null
source "${LIB_DIR}/common.sh"
# shellcheck source=/dev/null
source "${LIB_DIR}/parallel.sh"
# shellcheck source=/dev/null
source "${LIB_DIR}/ssh.sh"
# shellcheck source=/dev/null
source "${LIB_DIR}/kafka_cluster.sh"
# shellcheck source=/dev/null
source "${LIB_DIR}/entity_filter.sh"

CONFIG_FILE=""
NONINTERACTIVE=0
SSH_USER_OVERRIDE="${SSH_USER_OVERRIDE:-}"
SUDO_PASSWORD_ENV="${SUDO_PASSWORD:-${SUDO_PASSWORD_ENV:-}}"
USE_SUDO=1
VERBOSE=0
APPLY=0
FORCE_BROAD=0
MAX_TOPICS="${MAX_TOPICS:-200}"
ACTION="list"   # list | delete | set-config
JOBS_ARG="8"
ADMIN_BROKER_HOST=""
_ALL_KAFKA_HOSTS=()
declare -a SET_CONFIGS=()
_OK_FILE=""
_FAIL_FILE=""
_LOCK_FILE=""
_CONFIG_CSV=""

usage() {
  cat <<EOF
Usage: $(basename "$0") -c CONFIG.env [options]

Topic batch admin v${SCRIPT_VERSION}

Actions (pick one):
  --list                 List matching topics (default)
  --delete               Delete matching topics
  --set-config K=V       Alter topic config (repeatable), e.g. retention.ms=86400000

Safety / filter:
  --pattern REGEX        Required for --delete / --set-config (repeatable OR)
  --exclude REGEX        Drop matching names
  --include-internal     Include '_' names
  --force-broad          Allow dangerous patterns like '.*'
  --max-topics N         Abort if match count > N (default: ${MAX_TOPICS})
  --apply                Actually mutate (without it: dry-run report only)
  --jobs N|auto|ask      Parallel workers (default: 8)

Connection:
  -c, --config FILE      Cluster inventory (.env) — reuse HA inventories
  -u, --user USER        SSH username
  -y, --yes              Non-interactive confirms
  -v, --verbose
  -h, --help

Examples:
  $(basename "$0") -c ../kafka_realtime_check/config/clusters/devkafka.env -u USER \\
    --pattern '^cursor-test-' --list
  $(basename "$0") -c … -u USER -y --pattern '^cursor-test-' \\
    --set-config retention.ms=3600000 --set-config retention.bytes=1073741824 --apply
  $(basename "$0") -c … -u USER -y --pattern '^cursor-test-' --delete --apply
EOF
}

parse_args() {
  local have_action=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -c|--config) CONFIG_FILE="$2"; shift 2 ;;
      -u|--user) SSH_USER_OVERRIDE="$2"; shift 2 ;;
      --list) ACTION="list"; have_action=1; shift ;;
      --delete) ACTION="delete"; have_action=1; shift ;;
      --set-config)
        SET_CONFIGS+=("$2")
        ACTION="set-config"
        have_action=1
        shift 2
        ;;
      --pattern|--include|--topic-pattern) entity_filter_add_pattern "$2"; shift 2 ;;
      --exclude|--exclude-pattern) entity_filter_add_exclude "$2"; shift 2 ;;
      --include-internal) INCLUDE_INTERNAL=1; shift ;;
      --force-broad) FORCE_BROAD=1; shift ;;
      --max-topics) MAX_TOPICS="$2"; shift 2 ;;
      --jobs) JOBS_ARG="$2"; shift 2 ;;
      --apply) APPLY=1; shift ;;
      -y|--yes) NONINTERACTIVE=1; shift ;;
      -v|--verbose) VERBOSE=1; shift ;;
      -h|--help) usage; exit 0 ;;
      *) loge "Unknown argument: $1"; usage; exit 2 ;;
    esac
  done
  # if only --set-config was used, ACTION already set; list is default
  [[ "$have_action" == "1" ]] || ACTION="list"
}

load_config() {
  if [[ -z "$CONFIG_FILE" ]]; then
    loge "Missing -c/--config"; usage; exit 2
  fi
  if [[ ! -f "$CONFIG_FILE" ]]; then
    if [[ -f "${ROOT_DIR}/${CONFIG_FILE}" ]]; then
      CONFIG_FILE="${ROOT_DIR}/${CONFIG_FILE}"
    elif [[ -f "${ROOT_DIR}/../kafka_realtime_check/${CONFIG_FILE}" ]]; then
      CONFIG_FILE="${ROOT_DIR}/../kafka_realtime_check/${CONFIG_FILE}"
    else
      loge "Config not found: $CONFIG_FILE"; exit 2
    fi
  fi
  set -a
  # shellcheck disable=SC1090
  source "$CONFIG_FILE"
  set +a
}

_all_kafka_hosts() {
  local role_hosts arr h seen="|"
  _ALL_KAFKA_HOSTS=()
  for role_hosts in "${BROKER_HOSTS:-}" "${CONTROLLER_HOSTS:-}"; do
    arr=()
    csv_to_array arr "$role_hosts"
    for h in "${arr[@]}"; do
      [[ -z "$h" || "$seen" == *"|${h}|"* ]] && continue
      seen+="${h}|"
      _ALL_KAFKA_HOSTS+=("$h")
    done
  done
}

_ensure_ssh_hosts() {
  local h
  _all_kafka_hosts
  ADMIN_BROKER_HOST=""
  for h in "${_ALL_KAFKA_HOSTS[@]}"; do
    check_ssh_host "$h" "node:$h" || true
  done
  local brokers=()
  csv_to_array brokers "${BROKER_HOSTS:-}"
  for h in "${brokers[@]}"; do
    if ssh_host_is_ok "$h"; then
      ADMIN_BROKER_HOST="$h"
      break
    fi
  done
  if [[ -z "$ADMIN_BROKER_HOST" ]]; then
    loge "No SSH-reachable broker in BROKER_HOSTS"; exit 2
  fi
}

_kafka_admin_args_remote() {
  cat <<'FRAG'
ARGS=(--bootstrap-server "$BOOT")
[[ -n "$CONF" && -f "$CONF" ]] && ARGS+=(--command-config "$CONF")
FRAG
}

_remote_run_script() {
  local host="$1" need_sudo="$2" script="$3"
  remote_run "$host" "$need_sudo" "$script"
}

_confirm() {
  local prompt="$1"
  if [[ "$NONINTERACTIVE" == "1" ]]; then
    return 0
  fi
  local ans=""
  read -r -p "${prompt} [y/N] " ans || true
  [[ "$ans" =~ ^[Yy]$ ]]
}

_pattern_is_broad() {
  local p="$1"
  case "$p" in
    ""|"."|".*"|"^"|"$"|"^$"|"^.*$"|"^.*"|"*.*"|"*") return 0 ;;
  esac
  # single-char class / empty matchers
  if [[ "$p" =~ ^\.\*$ ]] || [[ "$p" =~ ^\^?\.\*\$?$ ]]; then
    return 0
  fi
  return 1
}

_validate_patterns_for_mutate() {
  if ((${#NAME_PATTERNS[@]} == 0)); then
    loge "--pattern is required for --delete / --set-config (refusing to touch all topics)"; exit 2
  fi
  local p
  for p in "${NAME_PATTERNS[@]}"; do
    if _pattern_is_broad "$p"; then
      if [[ "$FORCE_BROAD" == "1" ]]; then
        emit "${C_YELLOW}WARNING: broad pattern '${p}' allowed via --force-broad${C_RESET}"
      else
        loge "Pattern '${p}' is too broad. Narrow it (e.g. '^cursor-test-') or pass --force-broad"
        exit 2
      fi
    fi
  done
}

_validate_set_configs() {
  local c
  ((${#SET_CONFIGS[@]} > 0)) || { loge "--set-config K=V required"; exit 2; }
  for c in "${SET_CONFIGS[@]}"; do
    if ! [[ "$c" =~ ^[A-Za-z0-9._-]+=.+$ ]]; then
      loge "Invalid --set-config '${c}' (want key=value)"; exit 2
    fi
  done
}

_remote_list_topics() {
  local host="$1"
  local bootstrap="${KAFKA_CONNECT_BOOTSTRAP:-${KAFKA_BOOTSTRAP:-}}"
  local conf="${KAFKA_COMMAND_CONFIG:-}"
  local bin="${KAFKA_BIN:-/opt/kafka/bin}"
  local remote_cmd
  remote_cmd=$(cat <<REMOTE
set -euo pipefail
BIN='${bin}'; BOOT='${bootstrap}'; CONF='${conf}'
$(_kafka_admin_args_remote)
timeout ${KAFKA_ADMIN_TIMEOUT_SEC:-120} "\$BIN/kafka-topics.sh" "\${ARGS[@]}" --list
REMOTE
)
  _remote_run_script "$host" 0 "$remote_cmd"
}

_remote_delete_topic() {
  local host="$1" topic="$2"
  local bootstrap="${KAFKA_CONNECT_BOOTSTRAP:-${KAFKA_BOOTSTRAP:-}}"
  local conf="${KAFKA_COMMAND_CONFIG:-}"
  local bin="${KAFKA_BIN:-/opt/kafka/bin}"
  local remote_cmd
  remote_cmd="BIN='${bin}'; BOOT='${bootstrap}'; CONF='${conf}'; "
  remote_cmd+="ARGS=(--bootstrap-server \"\$BOOT\"); [[ -n \"\$CONF\" && -f \"\$CONF\" ]] && ARGS+=(--command-config \"\$CONF\"); "
  remote_cmd+="timeout ${KAFKA_ADMIN_TIMEOUT_SEC:-120} \"\$BIN/kafka-topics.sh\" \"\${ARGS[@]}\" "
  remote_cmd+="--delete --topic $(printf '%q' "$topic")"
  _remote_run_script "$host" 0 "$remote_cmd"
}

_remote_alter_topic_configs() {
  local host="$1" topic="$2" csv="$3"
  local bootstrap="${KAFKA_CONNECT_BOOTSTRAP:-${KAFKA_BOOTSTRAP:-}}"
  local conf="${KAFKA_COMMAND_CONFIG:-}"
  local bin="${KAFKA_BIN:-/opt/kafka/bin}"
  local remote_cmd
  remote_cmd="BIN='${bin}'; BOOT='${bootstrap}'; CONF='${conf}'; "
  remote_cmd+="ARGS=(--bootstrap-server \"\$BOOT\"); [[ -n \"\$CONF\" && -f \"\$CONF\" ]] && ARGS+=(--command-config \"\$CONF\"); "
  remote_cmd+="timeout ${KAFKA_ADMIN_TIMEOUT_SEC:-120} \"\$BIN/kafka-configs.sh\" \"\${ARGS[@]}\" "
  remote_cmd+="--entity-type topics --entity-name $(printf '%q' "$topic") "
  remote_cmd+="--alter --add-config $(printf '%q' "$csv")"
  _remote_run_script "$host" 0 "$remote_cmd"
}

_remote_create_topic() {
  local host="$1" topic="$2" partitions="${3:-1}" rf="${4:-1}"
  local bootstrap="${KAFKA_CONNECT_BOOTSTRAP:-${KAFKA_BOOTSTRAP:-}}"
  local conf="${KAFKA_COMMAND_CONFIG:-}"
  local bin="${KAFKA_BIN:-/opt/kafka/bin}"
  local remote_cmd
  remote_cmd="BIN='${bin}'; BOOT='${bootstrap}'; CONF='${conf}'; "
  remote_cmd+="ARGS=(--bootstrap-server \"\$BOOT\"); [[ -n \"\$CONF\" && -f \"\$CONF\" ]] && ARGS+=(--command-config \"\$CONF\"); "
  remote_cmd+="timeout ${KAFKA_ADMIN_TIMEOUT_SEC:-120} \"\$BIN/kafka-topics.sh\" \"\${ARGS[@]}\" "
  remote_cmd+="--create --if-not-exists --topic $(printf '%q' "$topic") "
  remote_cmd+="--partitions ${partitions} --replication-factor ${rf}"
  _remote_run_script "$host" 0 "$remote_cmd"
}

_delete_one() {
  local topic="$1" out rc=0
  out="$(_remote_delete_topic "$ADMIN_BROKER_HOST" "$topic" 2>&1)" || rc=$?
  {
    flock -x 8
    if [[ $rc -eq 0 ]]; then
      printf '%s\n' "$topic" >>"$_OK_FILE"
      printf '  %sOK%s  delete %s\n' "$C_GREEN" "$C_RESET" "$topic"
    else
      printf '%s\n' "$topic" >>"$_FAIL_FILE"
      printf '  %sFAIL%s delete %s (rc=%s)\n' "$C_RED" "$C_RESET" "$topic" "$rc"
      [[ "$VERBOSE" == "1" ]] && printf '         %s\n' "$out"
    fi
  } 8>>"$_LOCK_FILE"
}

_setconfig_one() {
  local topic="$1" out rc=0
  out="$(_remote_alter_topic_configs "$ADMIN_BROKER_HOST" "$topic" "$_CONFIG_CSV" 2>&1)" || rc=$?
  {
    flock -x 8
    if [[ $rc -eq 0 ]]; then
      printf '%s\n' "$topic" >>"$_OK_FILE"
      printf '  %sOK%s  config %s → %s\n' "$C_GREEN" "$C_RESET" "$topic" "$_CONFIG_CSV"
    else
      printf '%s\n' "$topic" >>"$_FAIL_FILE"
      printf '  %sFAIL%s config %s (rc=%s)\n' "$C_RED" "$C_RESET" "$topic" "$rc"
      [[ "$VERBOSE" == "1" ]] && printf '         %s\n' "$out"
    fi
  } 8>>"$_LOCK_FILE"
}

main() {
  parse_args "$@"
  load_config

  if [[ "$ACTION" == "delete" || "$ACTION" == "set-config" ]]; then
    _validate_patterns_for_mutate
  fi
  if [[ "$ACTION" == "set-config" ]]; then
    _validate_set_configs
  fi
  if ! [[ "$MAX_TOPICS" =~ ^[0-9]+$ ]] || (( MAX_TOPICS < 1 )); then
    loge "--max-topics must be >= 1"; exit 2
  fi

  init_result_store
  init_ssh_status_store
  init_parallel_store
  install_interrupt_traps

  emit "${C_BOLD}Topic batch admin v${SCRIPT_VERSION}${C_RESET} — ${CLUSTER_NAME:-cluster}"
  emit "Config: ${CONFIG_FILE}"
  emit "Action: ${ACTION}  apply=$([[ "$APPLY" == "1" ]] && echo yes || echo DRY-RUN)"

  prompt_credentials
  if [[ -n "${KAFKA_BOOTSTRAP:-}" ]]; then
    KAFKA_CONNECT_BOOTSTRAP="$KAFKA_BOOTSTRAP"
  fi
  export KAFKA_CONNECT_BOOTSTRAP

  section "SSH connectivity"
  _ensure_ssh_hosts
  emit "Admin broker: ${ADMIN_BROKER_HOST}"
  emit "Bootstrap:    ${KAFKA_CONNECT_BOOTSTRAP:-}"
  entity_filter_summary

  section "List topics"
  local list_out topic
  local -a matches=()
  list_out="$(_remote_list_topics "$ADMIN_BROKER_HOST" 2>/dev/null || true)"
  if [[ -z "${list_out// }" ]]; then
    loge "Empty topic list — check bootstrap/SASL"; exit 2
  fi

  local total=0 filtered=0
  while IFS= read -r topic; do
    topic="${topic//$'\r'/}"
    [[ -z "$topic" ]] && continue
    total=$((total + 1))
    name_matches_filter "$topic" || { filtered=$((filtered + 1)); continue; }
    matches+=("$topic")
  done <<<"$list_out"

  emit "Cluster topics: ${total}  filter-dropped: ${filtered}  matched: ${#matches[@]}"
  if ((${#matches[@]} == 0)); then
    emit "${C_GREEN}No topics matched.${C_RESET}"
    exit 0
  fi
  if [[ "$ACTION" != "list" ]] && ((${#matches[@]} > MAX_TOPICS)); then
    loge "Matched ${#matches[@]} topics > --max-topics ${MAX_TOPICS} — narrow --pattern or raise cap"
    exit 2
  fi

  section "Matched topics (${#matches[@]})"
  local i
  for i in "${!matches[@]}"; do
    if ((${#matches[@]} <= 40)) || [[ "$VERBOSE" == "1" ]] || (( i < 25 )); then
      emit "  ${matches[$i]}"
    fi
  done
  if ((${#matches[@]} > 40)) && [[ "$VERBOSE" != "1" ]]; then
    emit "  … and $((${#matches[@]} - 25)) more (use -v)"
  fi

  if [[ "$ACTION" == "list" ]]; then
    exit 0
  fi

  if [[ "$APPLY" != "1" ]]; then
    emit ""
    emit "${C_YELLOW}DRY-RUN only. Re-run with --apply to ${ACTION}.${C_RESET}"
    exit 0
  fi

  if [[ "$ACTION" == "set-config" ]]; then
    local IFS=','
    _CONFIG_CSV="${SET_CONFIGS[*]}"
    unset IFS
    section "Set config → ${_CONFIG_CSV}"
    resolve_parallel_jobs "${#matches[@]}"
    if ! _confirm "Alter ${#matches[@]} topic(s) configs (${_CONFIG_CSV}) with ${PARALLEL_JOBS} workers?"; then
      emit "Aborted"; exit 0
    fi
    _OK_FILE="$(mktemp "${TMPDIR:-/tmp}/topicadmin-ok.XXXXXX")"
    _FAIL_FILE="$(mktemp "${TMPDIR:-/tmp}/topicadmin-fail.XXXXXX")"
    _LOCK_FILE="$(mktemp "${TMPDIR:-/tmp}/topicadmin-lock.XXXXXX")"
    : >"$_OK_FILE"; : >"$_FAIL_FILE"; : >"$_LOCK_FILE"
    run_parallel_fn _setconfig_one "${matches[@]}"
  else
    section "Delete topics"
    resolve_parallel_jobs "${#matches[@]}"
    if ! _confirm "DELETE ${#matches[@]} topic(s) permanently with ${PARALLEL_JOBS} workers?"; then
      emit "Aborted"; exit 0
    fi
    _OK_FILE="$(mktemp "${TMPDIR:-/tmp}/topicadmin-ok.XXXXXX")"
    _FAIL_FILE="$(mktemp "${TMPDIR:-/tmp}/topicadmin-fail.XXXXXX")"
    _LOCK_FILE="$(mktemp "${TMPDIR:-/tmp}/topicadmin-lock.XXXXXX")"
    : >"$_OK_FILE"; : >"$_FAIL_FILE"; : >"$_LOCK_FILE"
    run_parallel_fn _delete_one "${matches[@]}"
  fi

  local ok fail
  ok=$(wc -l <"$_OK_FILE" | tr -d ' ')
  fail=$(wc -l <"$_FAIL_FILE" | tr -d ' ')
  rm -f "$_OK_FILE" "$_FAIL_FILE" "$_LOCK_FILE"
  emit ""
  emit "Done: ok=${ok} fail=${fail} action=${ACTION} jobs=${PARALLEL_JOBS}"
  (( fail == 0 )) || exit 2
  exit 0
}

main "$@"

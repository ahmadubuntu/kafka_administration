#!/usr/bin/env bash
# Cluster + topic min.insync.replicas scanner / fixer.
#
# 1) Shows live broker-default min.insync.replicas (+ value in server.properties).
# 2) Optionally sets a new cluster default via kafka-configs --entity-default
#    AND updates server.properties on brokers/controllers (no Kafka restart).
# 3) Scans topics still at --find and alters them in parallel to --set.
#
# Usage:
#   ./fix_topic_min_isr.sh -c config/clusters/stgkafka.env
#   ./fix_topic_min_isr.sh -c config/clusters/stgkafka.env --set 2 --apply
#   ./fix_topic_min_isr.sh -c config/clusters/stgkafka.env --find 1 --set 2 --apply --jobs 24
#   ./fix_topic_min_isr.sh -c config/clusters/stgkafka.env --set 2 --apply -y --jobs auto
#
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
source "${ROOT_DIR}/lib/kafka_cluster.sh"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/tasks.sh"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/entity_filter.sh"

CONFIG_FILE=""
FIND_VAL=1
SET_VAL=""
APPLY=0
NONINTERACTIVE=0
SSH_USER_OVERRIDE="${SSH_USER_OVERRIDE:-}"
SUDO_PASSWORD_ENV="${SUDO_PASSWORD:-${SUDO_PASSWORD_ENV:-}}"
USE_SUDO=1
VERBOSE=0
LIST_FILE=""
JOBS_ARG="8"   # default parallel workers
SKIP_RF_CHECK=0
_ALTER_OK_FILE=""
_ALTER_FAIL_FILE=""
_ALTER_LOCK=""
_ALTER_VALUE=""
_ALTER_BROKERS=()
_RF_SKIPPED=0

# id|prereqs|title|aliases
TASK_CATALOG=(
  "ssh||SSH connectivity|ssh,ssh connectivity,connectivity"
  "cluster|ssh|Cluster default min.insync.replicas|cluster,cluster default,broker default,default"
  "topics|ssh|Topic scan / alter|topics,topic,topic scan,topic alter"
)
TASKS_LIST_EXAMPLES="  $(basename "$0") -c CONFIG.env --only cluster
  $(basename "$0") -c CONFIG.env --only topics --find 1 --set 2 --pattern '^prod-'
  $(basename "$0") -c CONFIG.env --skip topics
  $(basename "$0") -c CONFIG.env --ask-tasks"

usage() {
  cat <<EOF
Usage: $(basename "$0") -c CONFIG.env [options]

min.insync.replicas cluster + topic fixer v${SCRIPT_VERSION}

Options:
  -c, --config FILE   Cluster inventory (.env) — required
  -u, --user USER     SSH username
  --find N            Topics whose effective min.insync.replicas == N (default: 1)
  --set N             Target value (default: ask interactively, or 2 with -y)
  --apply             Apply changes (cluster default + matching topics)
$(entity_filter_help_lines)
  --skip-rf-check     Do not skip topics whose ReplicationFactor < --set
  --only TASKS        Run only these tasks (ssh, cluster, topics)
  --skip TASKS
  --ask-tasks / --list-tasks
  --skip-cluster      Alias for --skip cluster
  --skip-topics       Alias for --skip topics
  -o, --out FILE      Write matching topic names to FILE
  -y, --yes           Non-interactive confirms
  -v, --verbose       Verbose alter failures
  -h, --help

Safety: topics with ReplicationFactor < --set are skipped (min.isr cannot exceed RF)
unless --skip-rf-check. Without --apply: report only (safe).
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -c|--config) CONFIG_FILE="$2"; shift 2 ;;
      -u|--user) SSH_USER_OVERRIDE="$2"; shift 2 ;;
      --find) FIND_VAL="$2"; shift 2 ;;
      --set) SET_VAL="$2"; shift 2 ;;
      --apply) APPLY=1; shift ;;
      --jobs) JOBS_ARG="$2"; shift 2 ;;
      --pattern|--include|--topic-pattern) entity_filter_add_pattern "$2"; shift 2 ;;
      --exclude|--exclude-pattern) entity_filter_add_exclude "$2"; shift 2 ;;
      --include-internal) INCLUDE_INTERNAL=1; shift ;;
      --skip-rf-check) SKIP_RF_CHECK=1; shift ;;
      --only) ONLY_TASKS="$2"; shift 2 ;;
      --skip) SKIP_TASKS="$2"; shift 2 ;;
      --ask-tasks) ASK_TASKS=1; shift ;;
      --list-tasks) LIST_TASKS=1; shift ;;
      --skip-cluster)
        if [[ -n "$SKIP_TASKS" ]]; then SKIP_TASKS+=",cluster"; else SKIP_TASKS="cluster"; fi
        shift
        ;;
      --skip-topics)
        if [[ -n "$SKIP_TASKS" ]]; then SKIP_TASKS+=",topics"; else SKIP_TASKS="topics"; fi
        shift
        ;;
      -o|--out) LIST_FILE="$2"; shift 2 ;;
      -y|--yes) NONINTERACTIVE=1; shift ;;
      -v|--verbose) VERBOSE=1; shift ;;
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
  # prints remote bash fragment that sets ARGS=(...)
  cat <<'FRAG'
ARGS=(--bootstrap-server "$BOOT")
[[ -n "$CONF" && -f "$CONF" ]] && ARGS+=(--command-config "$CONF")
FRAG
}

_remote_run_script() {
  # remote_run already wraps with bash -lc — pass the script body only (no nested bash -lc).
  local host="$1" need_sudo="$2" script="$3"
  remote_run "$host" "$need_sudo" "$script"
}

_remote_broker_default_isr() {
  local host="$1"
  local bootstrap="${KAFKA_CONNECT_BOOTSTRAP:-${KAFKA_BOOTSTRAP:-}}"
  local conf="${KAFKA_COMMAND_CONFIG:-}"
  local bin="${KAFKA_BIN:-/opt/kafka/bin}"
  local remote_cmd
  remote_cmd=$(cat <<REMOTE
set -euo pipefail
BIN='${bin}'; BOOT='${bootstrap}'; CONF='${conf}'
$(_kafka_admin_args_remote)
out=\$(timeout ${KAFKA_ADMIN_TIMEOUT_SEC:-60} "\$BIN/kafka-configs.sh" "\${ARGS[@]}" \
  --entity-type brokers --entity-default --describe --all 2>/dev/null || true)
val=\$(printf '%s\n' "\$out" | awk '{
  for (i=1;i<=NF;i++) if (\$i ~ /^min\\.insync\\.replicas=/) {
    split(\$i,a,"="); print a[2]; exit
  }
}')
if [[ -n "\$val" ]]; then
  printf 'set:%s\n' "\$val"
else
  printf 'unset:builtin\n'
fi
REMOTE
)
  _remote_run_script "$host" 0 "$remote_cmd"
}

# stdout: missing|commented:<val>|active:<val>|absent
_remote_file_isr() {
  local host="$1"
  local props="${KAFKA_SERVER_PROPERTIES:-/var/opt/kafka/config/kraft/server.properties}"
  local remote_cmd
  remote_cmd=$(cat <<REMOTE
set -euo pipefail
f='${props}'
if [[ ! -f "\$f" ]]; then
  echo missing
  exit 0
fi
line=\$(grep -E '^[[:space:]]*min\\.insync\\.replicas[[:space:]]*=' "\$f" | tail -1 || true)
if [[ -n "\$line" ]]; then
  val=\$(printf '%s' "\$line" | cut -d= -f2- | tr -d '[:space:]')
  echo "active:\${val}"
  exit 0
fi
cline=\$(grep -E '^[[:space:]]*#[[:space:]]*min\\.insync\\.replicas[[:space:]]*=' "\$f" | tail -1 || true)
if [[ -n "\$cline" ]]; then
  val=\$(printf '%s' "\$cline" | sed -E 's/^[[:space:]]*#[[:space:]]*min\\.insync\\.replicas[[:space:]]*=[[:space:]]*//' | tr -d '[:space:]')
  echo "commented:\${val}"
  exit 0
fi
echo absent
REMOTE
)
  _remote_run_script "$host" 0 "$remote_cmd"
}

_format_file_isr() {
  local raw="$1"
  case "$raw" in
    missing) echo "FILE MISSING" ;;
    active:*) echo "active=${raw#active:}" ;;
    commented:*) echo "commented-out (#…=${raw#commented:}) — not effective" ;;
    absent) echo "key absent in file (Kafka built-in default usually 1)" ;;
    *) echo "${raw:-unknown}" ;;
  esac
}

_remote_alter_broker_default() {
  local host="$1" value="$2"
  local bootstrap="${KAFKA_CONNECT_BOOTSTRAP:-${KAFKA_BOOTSTRAP:-}}"
  local conf="${KAFKA_COMMAND_CONFIG:-}"
  local bin="${KAFKA_BIN:-/opt/kafka/bin}"
  local remote_cmd
  remote_cmd=$(cat <<REMOTE
set -euo pipefail
BIN='${bin}'; BOOT='${bootstrap}'; CONF='${conf}'
$(_kafka_admin_args_remote)
timeout ${KAFKA_ADMIN_TIMEOUT_SEC:-60} "\$BIN/kafka-configs.sh" "\${ARGS[@]}" \
  --entity-type brokers --entity-default \
  --alter --add-config min.insync.replicas=${value}
REMOTE
)
  _remote_run_script "$host" 0 "$remote_cmd"
}

# Update server.properties in place (no service restart). Uses sudo when needed.
# ALWAYS writes an active (uncommented) min.insync.replicas=VAL line; verifies afterward.
_remote_update_server_properties_isr() {
  local host="$1" value="$2"
  local props="${KAFKA_SERVER_PROPERTIES:-/var/opt/kafka/config/kraft/server.properties}"
  local remote_cmd
  remote_cmd=$(cat <<REMOTE
set -euo pipefail
VAL='${value}'
update_one() {
  local f="\$1"
  if [[ ! -f "\$f" ]]; then
    echo "MISSING:\$f"
    return 1
  fi
  # Backup once per run stamp
  cp -a "\$f" "\$f.bak.minisr.\$(date +%Y%m%d%H%M%S)" 2>/dev/null || true
  if grep -qE '^[[:space:]]*#?[[:space:]]*min\\.insync\\.replicas[[:space:]]*=' "\$f"; then
    # Replace every commented or active occurrence with a single active line
    sed -i -E "s|^[[:space:]]*#?[[:space:]]*min\\.insync\\.replicas[[:space:]]*=.*|min.insync.replicas=\${VAL}|" "\$f"
  else
    printf '\\n# injected by fix_topic_min_isr.sh\\nmin.insync.replicas=%s\\n' "\$VAL" >> "\$f"
  fi
  # Collapse duplicate active lines to the last rewritten value (sed may hit multiples)
  if [[ \$(grep -cE '^[[:space:]]*min\\.insync\\.replicas[[:space:]]*=' "\$f" || true) -gt 1 ]]; then
    local tmp
    tmp=\$(mktemp)
    grep -vE '^[[:space:]]*#?[[:space:]]*min\\.insync\\.replicas[[:space:]]*=' "\$f" >"\$tmp"
    printf 'min.insync.replicas=%s\\n' "\$VAL" >>"\$tmp"
    cat "\$tmp" >"\$f"
    rm -f "\$tmp"
  fi
  act=\$(grep -E '^[[:space:]]*min\\.insync\\.replicas[[:space:]]*=' "\$f" | tail -1 || true)
  if [[ "\$act" != "min.insync.replicas=\${VAL}" && "\$act" != \$'min.insync.replicas='"\${VAL}" ]]; then
    # tolerate whitespace around =
    echo "\$act" | grep -qE "^[[:space:]]*min\\.insync\\.replicas[[:space:]]*=[[:space:]]*\${VAL}[[:space:]]*\$" || {
      echo "VERIFY_FAIL:\$f got=[\$act] want=min.insync.replicas=\${VAL}"
      return 2
    }
  fi
  echo "OK:\$f -> \$act"
}
rc=0
update_one '${props}' || rc=\$?
if [[ '${props}' != '/opt/kafka/config/kraft/server.properties' ]]; then
  update_one '/opt/kafka/config/kraft/server.properties' || rc=\$?
fi
if [[ '${props}' != '/var/opt/kafka/config/kraft/server.properties' ]]; then
  update_one '/var/opt/kafka/config/kraft/server.properties' || rc=\$?
fi
exit \$rc
REMOTE
)
  _remote_run_script "$host" 1 "$remote_cmd"
}

_remote_scan_min_isr() {
  local host="$1"
  local bootstrap="${KAFKA_CONNECT_BOOTSTRAP:-${KAFKA_BOOTSTRAP:-}}"
  local conf="${KAFKA_COMMAND_CONFIG:-}"
  local bin="${KAFKA_BIN:-/opt/kafka/bin}"
  local remote_cmd
  remote_cmd=$(cat <<REMOTE
set -euo pipefail
BIN='${bin}'; BOOT='${bootstrap}'; CONF='${conf}'
$(_kafka_admin_args_remote)
timeout ${KAFKA_ADMIN_TIMEOUT_SEC:-300} "\$BIN/kafka-configs.sh" "\${ARGS[@]}" \
  --entity-type topics --describe --all 2>/dev/null \
  | awk '
    /^All configs for topic / {
      t=\$5; sub(/:\$/, "", t); topic=t; next
    }
    {
      for (i=1; i<=NF; i++) {
        if (\$i ~ /^min\\.insync\\.replicas=/) {
          split(\$i, a, "=");
          if (topic != "") printf "%s\\t%s\\n", topic, a[2];
        }
      }
    }
  '
REMOTE
)
  _remote_run_script "$host" 0 "$remote_cmd"
}

_remote_alter_topic_isr() {
  local host="$1" topic="$2" value="$3"
  local bootstrap="${KAFKA_CONNECT_BOOTSTRAP:-${KAFKA_BOOTSTRAP:-}}"
  local conf="${KAFKA_COMMAND_CONFIG:-}"
  local bin="${KAFKA_BIN:-/opt/kafka/bin}"
  local remote_cmd
  remote_cmd="BIN='${bin}'; BOOT='${bootstrap}'; CONF='${conf}'; "
  remote_cmd+="ARGS=(--bootstrap-server \"\$BOOT\"); [[ -n \"\$CONF\" && -f \"\$CONF\" ]] && ARGS+=(--command-config \"\$CONF\"); "
  remote_cmd+="timeout ${KAFKA_ADMIN_TIMEOUT_SEC:-60} \"\$BIN/kafka-configs.sh\" \"\${ARGS[@]}\" "
  remote_cmd+="--entity-type topics --entity-name $(printf '%q' "$topic") "
  remote_cmd+="--alter --add-config min.insync.replicas=${value}"
  _remote_run_script "$host" 0 "$remote_cmd"
}

# stdout: topic<TAB>rf
_remote_topic_rf_map() {
  local host="$1"
  local bootstrap="${KAFKA_CONNECT_BOOTSTRAP:-${KAFKA_BOOTSTRAP:-}}"
  local conf="${KAFKA_COMMAND_CONFIG:-}"
  local bin="${KAFKA_BIN:-/opt/kafka/bin}"
  local remote_cmd
  remote_cmd=$(cat <<REMOTE
set -euo pipefail
BIN='${bin}'; BOOT='${bootstrap}'; CONF='${conf}'
$(_kafka_admin_args_remote)
timeout ${KAFKA_ADMIN_TIMEOUT_SEC:-300} "\$BIN/kafka-topics.sh" "\${ARGS[@]}" --describe 2>/dev/null \
  | awk '
    /^Topic:/ && /ReplicationFactor:/ {
      topic=""; rf="";
      if (match(\$0, /Topic:[[:space:]]*[^[:space:]]+/)) {
        topic=substr(\$0, RSTART, RLENGTH);
        sub(/^Topic:[[:space:]]*/, "", topic);
      }
      if (match(\$0, /ReplicationFactor:[[:space:]]*[0-9]+/)) {
        rf=substr(\$0, RSTART, RLENGTH);
        sub(/^ReplicationFactor:[[:space:]]*/, "", rf);
      }
      if (topic != "" && rf != "") printf "%s\\t%s\\n", topic, rf;
    }
  '
REMOTE
)
  _remote_run_script "$host" 0 "$remote_cmd"
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

_ask_set_val() {
  if [[ -n "$SET_VAL" ]]; then
    return 0
  fi
  if [[ "$NONINTERACTIVE" == "1" ]]; then
    SET_VAL=2
    return 0
  fi
  local ans=""
  read -r -p "Desired min.insync.replicas value [2]: " ans || true
  if [[ -z "$ans" ]]; then
    SET_VAL=2
  elif [[ "$ans" =~ ^[0-9]+$ ]] && (( ans >= 1 )); then
    SET_VAL="$ans"
  else
    loge "Invalid value: ${ans}"; exit 2
  fi
}

_alter_one_topic() {
  local topic="$1"
  local host n=${#_ALTER_BROKERS[@]}
  # Round-robin brokers to spread SSH/Admin load
  host="${_ALTER_BROKERS[$(( RANDOM % n ))]}"
  local out rc=0
  out="$(_remote_alter_topic_isr "$host" "$topic" "$_ALTER_VALUE" 2>&1)" || rc=$?
  {
    flock -x 9
    if [[ $rc -eq 0 ]]; then
      printf '%s\n' "$topic" >> "$_ALTER_OK_FILE"
      printf '  %sOK%s  %s → %s\n' "$C_GREEN" "$C_RESET" "$topic" "$_ALTER_VALUE"
    else
      printf '%s\n' "$topic" >> "$_ALTER_FAIL_FILE"
      printf '  %sFAIL%s %s (rc=%s)\n' "$C_RED" "$C_RESET" "$topic" "$rc"
      if [[ "$VERBOSE" == "1" ]]; then
        printf '         %s\n' "$out"
      fi
    fi
  } 9>>"$_ALTER_LOCK"
}

main() {
  parse_args "$@"

  if [[ "$LIST_TASKS" == "1" ]]; then
    tasks_list
    exit 0
  fi

  load_config

  if ! [[ "$FIND_VAL" =~ ^[0-9]+$ ]]; then
    loge "--find must be an integer"; exit 2
  fi

  init_result_store
  init_ssh_status_store
  init_parallel_store
  install_interrupt_traps

  emit "${C_BOLD}min.insync.replicas fixer v${SCRIPT_VERSION}${C_RESET} — ${CLUSTER_NAME:-cluster}"
  emit "Config: ${CONFIG_FILE}"
  emit "Mode:   $([[ "$APPLY" == "1" ]] && echo APPLY || echo SCAN-ONLY)"

  tasks_select || exit $?
  prompt_credentials

  if [[ -n "${KAFKA_BOOTSTRAP:-}" ]]; then
    KAFKA_CONNECT_BOOTSTRAP="$KAFKA_BOOTSTRAP"
  fi
  export KAFKA_CONNECT_BOOTSTRAP

  if tasks_selected ssh || tasks_selected cluster || tasks_selected topics; then
    section "SSH connectivity"
    _ensure_ssh_hosts
    local broker="$ADMIN_BROKER_HOST"
    emit "Admin broker: ${broker}"
    emit "Bootstrap:    ${KAFKA_CONNECT_BOOTSTRAP}"
    emit "Command cfg:  ${KAFKA_COMMAND_CONFIG:-none}"
  fi

  local broker="${ADMIN_BROKER_HOST:-}"

  if tasks_selected cluster; then
    section "Cluster default min.insync.replicas"
    local live_raw file_raw live_default file_note
    live_raw="$(_remote_broker_default_isr "$broker" 2>/dev/null | tr -d '\r' | tail -1 || true)"
    case "$live_raw" in
      set:*)
        live_default="${live_raw#set:}"
        emit "Live broker-default (--entity-default): ${live_default}"
        ;;
      unset:builtin|"")
        live_default=""
        emit "Live broker-default (--entity-default): unset — Kafka built-in default is usually 1 (not shown until set dynamically)"
        ;;
      *)
        live_default=""
        emit "Live broker-default (--entity-default): unknown (${live_raw})"
        ;;
    esac

    file_raw="$(_remote_file_isr "$broker" 2>/dev/null | tr -d '\r' | tail -1 || true)"
    file_note="$(_format_file_isr "$file_raw")"
    emit "File on ${broker} (${KAFKA_SERVER_PROPERTIES:-server.properties}): ${file_note}"

    local h
    for h in "${_ALL_KAFKA_HOSTS[@]}"; do
      ssh_host_is_ok "$h" || continue
      local fr
      fr="$(_remote_file_isr "$h" 2>/dev/null | tr -d '\r' | tail -1 || true)"
      emit "  file ${h}: $(_format_file_isr "$fr")"
    done

    _ask_set_val
    if ! [[ "$SET_VAL" =~ ^[0-9]+$ ]] || (( SET_VAL < 1 )); then
      loge "--set must be integer >= 1"; exit 2
    fi
    emit "Target min.insync.replicas: ${SET_VAL}"

    if [[ "$APPLY" == "1" ]]; then
      section "Apply cluster default (no Kafka restart)"
      emit "1) kafka-configs --entity-type brokers --entity-default --alter --add-config min.insync.replicas=${SET_VAL}"
      emit "2) MUST inject active min.insync.replicas=${SET_VAL} into server.properties on every node (uncomment/replace; no restart)"
      if ! _confirm "Set cluster default min.insync.replicas=${SET_VAL} (live + files)?"; then
        emit "Skipped cluster default change."
      else
        local out rc=0
        if [[ "$live_default" == "$SET_VAL" ]]; then
          emit "Live broker-default already ${SET_VAL} — skipping dynamic alter."
        else
          out="$(_remote_alter_broker_default "$broker" "$SET_VAL" 2>&1)" || rc=$?
          if [[ $rc -eq 0 ]]; then
            emit "${C_GREEN}Live broker-default altered to ${SET_VAL}${C_RESET}"
          else
            loge "Failed live broker-default alter (rc=${rc})"
            [[ "$VERBOSE" == "1" ]] && emit "$out"
            exit 2
          fi
        fi
        local file_fail=0
        for h in "${_ALL_KAFKA_HOSTS[@]}"; do
          ssh_host_is_ok "$h" || continue
          rc=0
          out="$(_remote_update_server_properties_isr "$h" "$SET_VAL" 2>&1)" || rc=$?
          if [[ $rc -eq 0 ]]; then
            local fr
            fr="$(_remote_file_isr "$h" 2>/dev/null | tr -d '\r' | tail -1 || true)"
            if [[ "$fr" == "active:${SET_VAL}" ]]; then
              emit "  ${C_GREEN}OK${C_RESET}  ${h} server.properties active min.insync.replicas=${SET_VAL}"
            else
              file_fail=1
              emit "  ${C_RED}FAIL${C_RESET} ${h} inject did not verify (got: $(_format_file_isr "$fr"))"
            fi
            [[ "$VERBOSE" == "1" && -n "$out" ]] && emit "         ${out}"
          else
            file_fail=1
            emit "  ${C_RED}FAIL${C_RESET} ${h} server.properties write (rc=${rc}) — need sudo?"
            [[ "$VERBOSE" == "1" ]] && emit "         ${out}"
          fi
        done
        live_raw="$(_remote_broker_default_isr "$broker" 2>/dev/null | tr -d '\r' | tail -1 || true)"
        emit "Verified live broker-default: ${live_raw:-unknown}"
        (( file_fail == 0 )) || { loge "One or more server.properties injections failed"; exit 2; }
      fi
    else
      emit ""
      emit "${C_YELLOW}Scan-only for cluster: re-run with --apply --set ${SET_VAL} to change broker-default + inject server.properties (no restart).${C_RESET}"
    fi
  fi

  if ! tasks_selected topics; then
    emit "Skipping topic scan (task not selected)."
    exit 0
  fi

  if [[ -z "$SET_VAL" ]]; then
    _ask_set_val
  fi
  if ! [[ "$SET_VAL" =~ ^[0-9]+$ ]] || (( SET_VAL < 1 )); then
    loge "--set must be integer >= 1"; exit 2
  fi

  entity_filter_summary
  section "Scan topics (effective min.insync.replicas)"
  local scan_out rf_out
  scan_out="$(_remote_scan_min_isr "$broker")" || true
  if [[ -z "${scan_out// }" ]]; then
    loge "Empty topic scan — check bootstrap/SASL/command-config"
    exit 2
  fi

  declare -A TOPIC_RF=()
  rf_out="$(_remote_topic_rf_map "$broker" 2>/dev/null || true)"
  local rf_topic rf_val
  while IFS=$'\t' read -r rf_topic rf_val; do
    [[ -z "$rf_topic" || -z "$rf_val" ]] && continue
    TOPIC_RF["$rf_topic"]="$rf_val"
  done <<<"$rf_out"

  local total=0 match=0 filtered_out=0 rf_skip=0
  local -a matches=() match_notes=()
  local topic val rf
  _RF_SKIPPED=0
  while IFS=$'\t' read -r topic val; do
    [[ -z "$topic" ]] && continue
    total=$((total + 1))
    name_matches_filter "$topic" || { filtered_out=$((filtered_out + 1)); continue; }
    [[ "$val" == "$FIND_VAL" ]] || continue

    rf="${TOPIC_RF[$topic]:-}"
    if [[ "$SKIP_RF_CHECK" != "1" ]]; then
      if [[ -z "$rf" ]]; then
        emit "  ${C_YELLOW}WARN${C_RESET} ${topic} — RF unknown; skipping (use --skip-rf-check to force)"
        rf_skip=$((rf_skip + 1))
        _RF_SKIPPED=$((_RF_SKIPPED + 1))
        continue
      fi
      if (( rf < SET_VAL )); then
        emit "  ${C_YELLOW}SKIP${C_RESET} ${topic} — RF=${rf} < min.isr target ${SET_VAL} (Kafka rejects min.isr > RF)"
        rf_skip=$((rf_skip + 1))
        _RF_SKIPPED=$((_RF_SKIPPED + 1))
        continue
      fi
      if (( rf == SET_VAL )); then
        match_notes+=("RF=${rf}(=min.isr) ${topic}")
      else
        match_notes+=("RF=${rf} ${topic}")
      fi
    else
      match_notes+=("RF=${rf:-?} ${topic}")
    fi
    match=$((match + 1))
    matches+=("$topic")
  done <<<"$scan_out"

  emit "Topics scanned: ${total}  filter-dropped: ${filtered_out}  rf-skipped: ${rf_skip}"
  emit "Matches (min.isr=${FIND_VAL} → ${SET_VAL}, after filters): ${match}"

  if (( match == 0 )); then
    emit "${C_GREEN}No topics to alter (after pattern/RF filters).${C_RESET}"
    exit 0
  fi

  section "Matching topics (${match})"
  local i
  for i in "${!matches[@]}"; do
    if (( match <= 40 )) || [[ "$VERBOSE" == "1" ]] || (( i < 20 )); then
      emit "  ${match_notes[$i]}"
    fi
  done
  if (( match > 40 )) && [[ "$VERBOSE" != "1" ]]; then
    emit "  … and $((match - 20)) more (use -v to print all, or -o FILE)"
  fi

  if [[ -n "$LIST_FILE" ]]; then
    printf '%s\n' "${matches[@]}" > "$LIST_FILE"
    emit "Wrote list: ${LIST_FILE}"
  fi

  if [[ "$APPLY" != "1" ]]; then
    emit ""
    emit "${C_YELLOW}Scan only for topics. Re-run with --apply --set ${SET_VAL} --jobs ${JOBS_ARG} to alter in parallel.${C_RESET}"
    exit 0
  fi

  if (( FIND_VAL == SET_VAL )); then
    emit "Find == set (${SET_VAL}); nothing to alter on topics."
    exit 0
  fi

  section "Parallel topic alter → ${SET_VAL}"
  resolve_parallel_jobs "$match"
  if ! _confirm "Alter ${match} topic(s) to min.insync.replicas=${SET_VAL} with ${PARALLEL_JOBS} workers?"; then
    emit "Aborted — no topic changes."
    exit 0
  fi

  _ALTER_OK_FILE="$(mktemp "${TMPDIR:-/tmp}/kafkaha-isr-ok.XXXXXX")"
  _ALTER_FAIL_FILE="$(mktemp "${TMPDIR:-/tmp}/kafkaha-isr-fail.XXXXXX")"
  _ALTER_LOCK="$(mktemp "${TMPDIR:-/tmp}/kafkaha-isr-lock.XXXXXX")"
  : > "$_ALTER_OK_FILE"
  : > "$_ALTER_FAIL_FILE"
  : > "$_ALTER_LOCK"
  _ALTER_VALUE="$SET_VAL"
  _ALTER_BROKERS=()
  local brokers=()
  csv_to_array brokers "${BROKER_HOSTS:-}"
  for h in "${brokers[@]}"; do
    ssh_host_is_ok "$h" && _ALTER_BROKERS+=("$h")
  done
  ((${#_ALTER_BROKERS[@]} > 0)) || _ALTER_BROKERS=("$broker")

  run_parallel_fn _alter_one_topic "${matches[@]}"

  local ok fail
  ok=$(wc -l < "$_ALTER_OK_FILE" | tr -d ' ')
  fail=$(wc -l < "$_ALTER_FAIL_FILE" | tr -d ' ')
  rm -f "$_ALTER_OK_FILE" "$_ALTER_FAIL_FILE" "$_ALTER_LOCK"

  emit ""
  emit "Topic alters done: ok=${ok} fail=${fail} rf_skipped=${_RF_SKIPPED} (target=${SET_VAL}, jobs=${PARALLEL_JOBS})"
  (( fail == 0 )) || exit 2
  exit 0
}

main "$@"

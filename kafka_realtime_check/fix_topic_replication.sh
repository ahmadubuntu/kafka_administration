#!/usr/bin/env bash
# Find topics by replication factor and optionally raise RF via partition reassignment.
#
# Kafka cannot change RF with a simple topic --alter; this script builds a
# kafka-reassign-partitions.json plan (keep existing replicas, add brokers) and
# optionally --execute / --verify.
#
# Usage:
#   ./fix_topic_replication.sh -c config/clusters/stgkafka.env -u USER -y
#   ./fix_topic_replication.sh -c CONFIG.env -u USER --find 1,2 --set 3
#   ./fix_topic_replication.sh -c CONFIG.env -u USER -y --find 1,2 --set 3 --apply
#   ./fix_topic_replication.sh -c CONFIG.env -u USER --only scan --find 1
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

CONFIG_FILE=""
FIND_RF="1,2"
SET_VAL=""
APPLY=0
VERIFY=1
NONINTERACTIVE=0
SSH_USER_OVERRIDE="${SSH_USER_OVERRIDE:-}"
SUDO_PASSWORD_ENV="${SUDO_PASSWORD:-${SUDO_PASSWORD_ENV:-}}"
USE_SUDO=1
VERBOSE=0
LIST_FILE=""
JSON_OUT=""
THROTTLE=""
INCLUDE_INTERNAL=0
EXCLUDE_REGEX=""
ADMIN_BROKER_HOST=""
_ALL_KAFKA_HOSTS=()

TASK_CATALOG=(
  "ssh||SSH connectivity|ssh,ssh connectivity,connectivity"
  "scan|ssh|Scan topics by replication factor|scan,find,list"
  "reassign|ssh|Build / execute RF reassignment|reassign,apply,replication"
)
TASKS_LIST_EXAMPLES="  $(basename "$0") -c CONFIG.env --find 1,2 --set 3
  $(basename "$0") -c CONFIG.env -y --find 1,2 --set 3 --apply
  $(basename "$0") -c CONFIG.env --only scan --find 1
  $(basename "$0") -c CONFIG.env --ask-tasks"

usage() {
  cat <<EOF
Usage: $(basename "$0") -c CONFIG.env [options]

Topic replication-factor scanner / raiser v${SCRIPT_VERSION}

Finds topics whose ReplicationFactor is in --find (default: 1,2) and can raise
them to --set (default: 3) using kafka-reassign-partitions.sh (adds replicas;
never drops replicas below current RF).

Options:
  -c, --config FILE       Cluster inventory (.env) — required
  -u, --user USER         SSH username
  --find LIST             Match these RF values (comma-separated; default: 1,2)
  --set N                 Target replication factor (default: ask, or 3 with -y)
  --apply                 Execute reassignment (default: scan + write plan only)
  --no-verify             Skip --verify after --apply
  --throttle BYTES        Optional reassignment throttle (bytes/sec)
  --include-internal      Include topics starting with '__'
  --exclude REGEX         Drop matching topic names (ERE)
  --json-out FILE         Write reassignment JSON here (default: reports/…)
  -o, --out FILE          Write matching topic names (one per line)
  --only TASKS            ssh, scan, reassign
  --skip TASKS
  --ask-tasks / --list-tasks
  -y, --yes               Non-interactive
  -v, --verbose
  -h, --help

Without --apply: safe report + JSON plan (no cluster change).
Requires at least --set distinct live brokers.
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -c|--config) CONFIG_FILE="$2"; shift 2 ;;
      -u|--user) SSH_USER_OVERRIDE="$2"; shift 2 ;;
      --find) FIND_RF="$2"; shift 2 ;;
      --set) SET_VAL="$2"; shift 2 ;;
      --apply) APPLY=1; shift ;;
      --no-verify) VERIFY=0; shift ;;
      --throttle) THROTTLE="$2"; shift 2 ;;
      --include-internal) INCLUDE_INTERNAL=1; shift ;;
      --exclude) EXCLUDE_REGEX="$2"; shift 2 ;;
      --json-out) JSON_OUT="$2"; shift 2 ;;
      --only) ONLY_TASKS="$2"; shift 2 ;;
      --skip) SKIP_TASKS="$2"; shift 2 ;;
      --ask-tasks) ASK_TASKS=1; shift ;;
      --list-tasks) LIST_TASKS=1; shift ;;
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

_ask_set_val() {
  if [[ -n "$SET_VAL" ]]; then
    return 0
  fi
  if [[ "$NONINTERACTIVE" == "1" ]]; then
    SET_VAL=3
    return 0
  fi
  local ans=""
  read -r -p "Desired replication factor [3]: " ans || true
  if [[ -z "$ans" ]]; then
    SET_VAL=3
  elif [[ "$ans" =~ ^[0-9]+$ ]] && (( ans >= 1 )); then
    SET_VAL="$ans"
  else
    loge "Invalid value: ${ans}"; exit 2
  fi
}

_parse_find_rf() {
  local tok
  FIND_SET="|"
  FIND_RF="${FIND_RF//;/,}"
  IFS=',' read -ra _find_parts <<<"$FIND_RF"
  for tok in "${_find_parts[@]}"; do
    tok="$(printf '%s' "$tok" | tr -d '[:space:]')"
    [[ -z "$tok" ]] && continue
    if ! [[ "$tok" =~ ^[0-9]+$ ]] || (( tok < 1 )); then
      loge "--find entries must be positive integers (got '${tok}')"; exit 2
    fi
    FIND_SET+="${tok}|"
  done
  if [[ "$FIND_SET" == "|" ]]; then
    loge "--find produced an empty set"; exit 2
  fi
}

_rf_in_find() {
  local rf="$1"
  [[ "$FIND_SET" == *"|${rf}|"* ]]
}

_remote_broker_ids() {
  local host="$1"
  local bootstrap="${KAFKA_CONNECT_BOOTSTRAP:-${KAFKA_BOOTSTRAP:-}}"
  local conf="${KAFKA_COMMAND_CONFIG:-}"
  local bin="${KAFKA_BIN:-/opt/kafka/bin}"
  local remote_cmd
  remote_cmd=$(cat <<REMOTE
set -euo pipefail
BIN='${bin}'; BOOT='${bootstrap}'; CONF='${conf}'
$(_kafka_admin_args_remote)
out=\$(timeout ${KAFKA_ADMIN_TIMEOUT_SEC:-60} "\$BIN/kafka-broker-api-versions.sh" "\${ARGS[@]}" 2>/dev/null || true)
printf '%s\n' "\$out" | awk '
  match(\$0, /\\(id:[[:space:]]*[0-9]+/) {
    line=substr(\$0, RSTART, RLENGTH);
    sub(/.*id:[[:space:]]*/, "", line);
    print line;
  }
' | sort -n | uniq
REMOTE
)
  _remote_run_script "$host" 0 "$remote_cmd"
}

_remote_topics_describe() {
  local host="$1"
  local bootstrap="${KAFKA_CONNECT_BOOTSTRAP:-${KAFKA_BOOTSTRAP:-}}"
  local conf="${KAFKA_COMMAND_CONFIG:-}"
  local bin="${KAFKA_BIN:-/opt/kafka/bin}"
  local remote_cmd
  remote_cmd=$(cat <<REMOTE
set -euo pipefail
BIN='${bin}'; BOOT='${bootstrap}'; CONF='${conf}'
$(_kafka_admin_args_remote)
timeout ${KAFKA_ADMIN_TIMEOUT_SEC:-300} "\$BIN/kafka-topics.sh" "\${ARGS[@]}" --describe
REMOTE
)
  _remote_run_script "$host" 0 "$remote_cmd"
}

# stdin: kafka-topics --describe → stdout lines: topic<TAB>rf
_parse_topic_rf_summary() {
  awk '
    /^Topic:/ && /ReplicationFactor:/ {
      topic=""; rf="";
      if (match($0, /Topic:[[:space:]]*[^[:space:]]+/)) {
        topic=substr($0, RSTART, RLENGTH);
        sub(/^Topic:[[:space:]]*/, "", topic);
      }
      if (match($0, /ReplicationFactor:[[:space:]]*[0-9]+/)) {
        rf=substr($0, RSTART, RLENGTH);
        sub(/^ReplicationFactor:[[:space:]]*/, "", rf);
      }
      if (topic != "" && rf != "") print topic "\t" rf;
    }
  '
}

# stdin: describe → stdout JSON partitions array elements for matching topics
# Args via env: MATCH_TOPICS (newline-separated), TARGET_RF, BROKER_IDS (space-separated)
_build_reassignment_partitions() {
  local match_file="$1" target="$2" brokers_csv="$3"
  awk -v matchfile="$match_file" -v target="$target" -v brokers_csv="$brokers_csv" '
    BEGIN {
      while ((getline line < matchfile) > 0) {
        gsub(/\r/, "", line);
        if (line != "") want[line]=1;
      }
      close(matchfile);
      n=split(brokers_csv, brokers, ",");
      rr=0;
    }
    function trim(s){ gsub(/^[[:space:]]+|[[:space:]]+$/, "", s); return s }
    function in_list(arr, n, x, i) {
      for (i=1;i<=n;i++) if (arr[i]==x) return 1;
      return 0;
    }
    /^[[:space:]]*Topic:/ && /Partition:/ && /Replicas:/ {
      topic=""; part=""; reps="";
      line=$0;
      if (match(line, /Topic:[[:space:]]*[^[:space:]]+/)) {
        topic=substr(line, RSTART, RLENGTH);
        sub(/^Topic:[[:space:]]*/, "", topic);
      }
      if (!(topic in want)) next;
      if (match(line, /Partition:[[:space:]]*[0-9]+/)) {
        part=substr(line, RSTART, RLENGTH);
        sub(/^Partition:[[:space:]]*/, "", part);
      }
      if (match(line, /Replicas:[[:space:]]*[0-9]+([,][0-9]+)*/)) {
        reps=substr(line, RSTART, RLENGTH);
        sub(/^Replicas:[[:space:]]*/, "", reps);
      }
      if (topic=="" || part=="" || reps=="") next;

      cn=split(reps, cur, ",");
      for (i=1;i<=cn;i++) cur[i]=trim(cur[i]);
      # already at/above target → skip partition
      if (cn >= target) next;

      # grow: keep order, append brokers not already present
      newn=cn;
      for (i=1;i<=cn;i++) newr[i]=cur[i];
      guard=0;
      while (newn < target) {
        guard++;
        if (guard > (n*target+5)) {
          printf("ERROR\t%s\tp%s\tnot enough brokers to reach RF=%d\n", topic, part, target) > "/dev/stderr";
          exit 2;
        }
        added=0;
        for (k=0;k<n;k++) {
          b=brokers[((rr + k) % n) + 1];
          if (!in_list(newr, newn, b)) {
            newn++;
            newr[newn]=b;
            rr=(rr + k + 1) % n;
            added=1;
            break;
          }
        }
        if (!added) {
          printf("ERROR\t%s\tp%s\tcannot add replica (brokers exhausted)\n", topic, part) > "/dev/stderr";
          exit 2;
        }
      }
      list=newr[1];
      for (i=2;i<=newn;i++) list=list "," newr[i];
      esc=topic;
      gsub(/\\/, "\\\\", esc);
      gsub(/"/, "\\\"", esc);
      if (outn++) printf ",\n";
      printf "    {\"topic\":\"%s\",\"partition\":%s,\"replicas\":[%s]}", esc, part, list;
      delete newr;
    }
    END {
      if (outn) printf "\n";
    }
  '
}

_cluster_slug() {
  printf '%s' "${CLUSTER_NAME:-cluster}" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/-/g; s/^-+|-+$//g; s/-+/-/g'
}

# Upload JSON via scp, then run a short remote command (avoids huge bash -lc payloads
# and false non-zero SSH exits after kafka already printed success).
_remote_reassign() {
  local host="$1" mode="$2" json_path="$3"  # mode: execute|verify
  local bootstrap="${KAFKA_CONNECT_BOOTSTRAP:-${KAFKA_BOOTSTRAP:-}}"
  local conf="${KAFKA_COMMAND_CONFIG:-}"
  local bin="${KAFKA_BIN:-/opt/kafka/bin}"
  local remote_json="/tmp/kafkaha-rf-${mode}-$$-$(date +%s).json"
  local throttle_arg="" additional_arg=""
  local scp_rc=0 out rc=0
  local saved_alive="${SSH_SERVER_ALIVE_INTERVAL:-}"

  if [[ -n "$THROTTLE" && "$mode" == "execute" ]]; then
    throttle_arg="--throttle ${THROTTLE}"
  fi
  if [[ "$mode" == "execute" && "${REASSIGN_ADDITIONAL:-0}" == "1" ]]; then
    additional_arg="--additional"
  fi

  # Keep SSH from dropping during long AdminClient calls
  export SSH_SERVER_ALIVE_INTERVAL="${SSH_SERVER_ALIVE_INTERVAL:-30}"

  # shellcheck disable=SC2086
  scp -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout="${SSH_TIMEOUT_SEC:-12}" \
    -P "${SSH_PORT:-22}" \
    "$json_path" "${SSH_USER}@${host}:${remote_json}" 2>/dev/null || scp_rc=$?
  if [[ $scp_rc -ne 0 ]]; then
    # fallback: stream via ssh
    ssh $(ssh_base_opts "${SSH_TIMEOUT_SEC:-12}" "$host") "${SSH_USER}@${host}" \
      "cat >$(printf '%q' "$remote_json")" <"$json_path" || {
      [[ -n "$saved_alive" ]] && export SSH_SERVER_ALIVE_INTERVAL="$saved_alive" || unset SSH_SERVER_ALIVE_INTERVAL
      return 2
    }
  fi

  local remote_cmd
  remote_cmd=$(cat <<REMOTE
set -uo pipefail
BIN='${bin}'; BOOT='${bootstrap}'; CONF='${conf}'; JSON='${remote_json}'
$(_kafka_admin_args_remote)
timeout ${KAFKA_ADMIN_TIMEOUT_SEC:-600} "\$BIN/kafka-reassign-partitions.sh" "\${ARGS[@]}" \
  --reassignment-json-file "\$JSON" --${mode} ${additional_arg} ${throttle_arg}
rc=\$?
rm -f "\$JSON"
exit \$rc
REMOTE
)
  set +e
  out="$(_remote_run_script "$host" 0 "$remote_cmd" 2>&1)"
  rc=$?
  set -e
  printf '%s\n' "$out"

  if [[ -n "$saved_alive" ]]; then
    export SSH_SERVER_ALIVE_INTERVAL="$saved_alive"
  else
    unset SSH_SERVER_ALIVE_INTERVAL
  fi

  if [[ "$mode" == "execute" ]]; then
    if grep -qE 'Successfully started partition reassignments' <<<"$out"; then
      return 0
    fi
    if grep -qiE 'existing partition assignment' <<<"$out"; then
      return 3  # in progress / need --additional
    fi
  fi
  return "$rc"
}

main() {
  parse_args "$@"

  if [[ "$LIST_TASKS" == "1" ]]; then
    tasks_list
    exit 0
  fi

  # --apply implies reassign task interest; default catalog runs scan+reassign plan
  if [[ "$APPLY" == "1" && -z "$ONLY_TASKS" && -z "$SKIP_TASKS" && "$ASK_TASKS" != "1" ]]; then
    :
  fi

  load_config
  _parse_find_rf

  init_result_store
  init_ssh_status_store
  init_parallel_store
  install_interrupt_traps

  emit "${C_BOLD}Topic replication-factor fixer v${SCRIPT_VERSION}${C_RESET} — ${CLUSTER_NAME:-cluster}"
  emit "Config: ${CONFIG_FILE}"
  emit "Find RF in: ${FIND_RF}"
  emit "Mode:   $([[ "$APPLY" == "1" ]] && echo APPLY || echo SCAN-ONLY)"

  tasks_select || exit $?
  prompt_credentials

  if [[ -n "${KAFKA_BOOTSTRAP:-}" ]]; then
    KAFKA_CONNECT_BOOTSTRAP="$KAFKA_BOOTSTRAP"
  fi
  export KAFKA_CONNECT_BOOTSTRAP

  if tasks_selected ssh || tasks_selected scan || tasks_selected reassign; then
    section "SSH connectivity"
    _ensure_ssh_hosts
    emit "Admin broker: ${ADMIN_BROKER_HOST}"
    emit "Bootstrap:    ${KAFKA_CONNECT_BOOTSTRAP:-}"
    emit "Command cfg:  ${KAFKA_COMMAND_CONFIG:-none}"
  fi

  local broker="${ADMIN_BROKER_HOST}"
  local -a broker_ids=()
  local ids_raw

  if tasks_selected scan || tasks_selected reassign; then
    section "Live brokers"
    ids_raw="$(_remote_broker_ids "$broker" 2>/dev/null | tr -d '\r' || true)"
    while read -r id; do
      [[ -z "$id" ]] && continue
      broker_ids+=("$id")
    done <<<"$ids_raw"
    if ((${#broker_ids[@]} == 0)); then
      loge "Could not list broker ids (kafka-broker-api-versions). Check bootstrap/SASL."; exit 2
    fi
    emit "Broker ids (${#broker_ids[@]}): ${broker_ids[*]}"
  fi

  _ask_set_val
  if ! [[ "$SET_VAL" =~ ^[0-9]+$ ]] || (( SET_VAL < 1 )); then
    loge "--set must be integer >= 1"; exit 2
  fi
  emit "Target replication factor: ${SET_VAL}"

  if (( SET_VAL > ${#broker_ids[@]} )); then
    loge "Target RF=${SET_VAL} > live brokers=${#broker_ids[@]} — add brokers or lower --set"; exit 2
  fi

  local desc="" match=0
  local -a matches=() match_rfs=()
  local topic rf

  if tasks_selected scan || tasks_selected reassign; then
    section "Scan topics (ReplicationFactor)"
    desc="$(_remote_topics_describe "$broker" 2>/dev/null || true)"
    if [[ -z "${desc// }" ]]; then
      loge "Empty topics --describe — check bootstrap/SASL/command-config"; exit 2
    fi

    local total=0
    while IFS=$'\t' read -r topic rf; do
      [[ -z "$topic" || -z "$rf" ]] && continue
      total=$((total + 1))
      if [[ "$INCLUDE_INTERNAL" != "1" && "$topic" == __* ]]; then
        continue
      fi
      if [[ -n "$EXCLUDE_REGEX" ]] && [[ "$topic" =~ $EXCLUDE_REGEX ]]; then
        continue
      fi
      if _rf_in_find "$rf"; then
        if (( rf >= SET_VAL )); then
          [[ "$VERBOSE" == "1" ]] && emit "  skip ${topic} RF=${rf} (already >= ${SET_VAL})"
          continue
        fi
        match=$((match + 1))
        matches+=("$topic")
        match_rfs+=("$rf")
      fi
    done < <(printf '%s\n' "$desc" | _parse_topic_rf_summary)

    emit "Topics with RF summary line: ${total}"
    emit "Matches (RF in {${FIND_RF}} and RF < ${SET_VAL}): ${match}"

    if (( match == 0 )); then
      emit "${C_GREEN}No topics need RF raised to ${SET_VAL}.${C_RESET}"
      exit 0
    fi

    section "Matching topics (${match})"
    local i
    for i in "${!matches[@]}"; do
      if (( match <= 40 )) || [[ "$VERBOSE" == "1" ]] || (( i < 20 )); then
        emit "  RF=${match_rfs[$i]}  ${matches[$i]}"
      fi
    done
    if (( match > 40 )) && [[ "$VERBOSE" != "1" ]]; then
      emit "  … and $((match - 20)) more (use -v to print all, or -o FILE)"
    fi

    if [[ -n "$LIST_FILE" ]]; then
      printf '%s\n' "${matches[@]}" > "$LIST_FILE"
      emit "Wrote topic list: ${LIST_FILE}"
    fi
  fi

  if ! tasks_selected reassign; then
    emit "Skipping reassignment plan (task not selected)."
    exit 0
  fi

  section "Build reassignment plan → RF=${SET_VAL}"
  local match_tmp json_tmp brokers_csv
  match_tmp="$(mktemp "${TMPDIR:-/tmp}/kafkaha-rf-topics.XXXXXX")"
  json_tmp="$(mktemp "${TMPDIR:-/tmp}/kafkaha-rf-json.XXXXXX")"
  printf '%s\n' "${matches[@]}" > "$match_tmp"
  brokers_csv="$(IFS=,; echo "${broker_ids[*]}")"

  {
    echo '{'
    echo '  "version": 1,'
    echo '  "partitions": ['
    if ! printf '%s\n' "$desc" | _build_reassignment_partitions "$match_tmp" "$SET_VAL" "$brokers_csv"; then
      rm -f "$match_tmp" "$json_tmp"
      loge "Failed to build reassignment JSON"; exit 2
    fi
    echo '  ]'
    echo '}'
  } > "$json_tmp"

  rm -f "$match_tmp"

  local part_n
  part_n="$(grep -c '"partition"' "$json_tmp" || true)"
  if (( part_n == 0 )); then
    rm -f "$json_tmp"
    emit "${C_YELLOW}Plan empty — partitions already at target RF or describe lacked partition lines.${C_RESET}"
    exit 0
  fi

  if [[ -z "$JSON_OUT" ]]; then
    mkdir -p "${ROOT_DIR}/reports"
    JSON_OUT="${ROOT_DIR}/reports/rf-reassign-$(_cluster_slug)-$(date +%Y%m%d_%H%M%S).json"
  fi
  cp -f "$json_tmp" "$JSON_OUT"
  rm -f "$json_tmp"
  emit "Reassignment JSON: ${JSON_OUT} (${part_n} partition moves)"

  if [[ "$APPLY" != "1" ]]; then
    emit ""
    emit "${C_YELLOW}Scan/plan only. Re-run with --apply --set ${SET_VAL} to execute kafka-reassign-partitions.${C_RESET}"
    exit 0
  fi

  if ! _confirm "Execute reassignment for ${match} topic(s) / ${part_n} partition(s) → RF=${SET_VAL}?"; then
    emit "Aborted — plan kept at ${JSON_OUT}"
    exit 0
  fi

  section "Execute reassignment"
  local out rc=0
  set +e
  out="$(_remote_reassign "$broker" execute "$JSON_OUT" 2>&1)"
  rc=$?
  set -e
  if [[ $rc -eq 0 ]] || grep -qE 'Successfully started partition reassignments' <<<"$out"; then
    emit "${C_GREEN}Execute accepted${C_RESET}"
    [[ "$VERBOSE" == "1" ]] && emit "$out"
  elif [[ $rc -eq 3 ]] || grep -qiE 'existing partition assignment' <<<"$out"; then
    emit "${C_YELLOW}A partition reassignment is already in progress on the cluster.${C_RESET}"
    emit "Will verify progress against this plan (or re-run with REASSIGN_ADDITIONAL=1 to force --additional)."
    [[ "$VERBOSE" == "1" ]] && emit "$out"
  else
    loge "kafka-reassign-partitions --execute failed (rc=${rc})"
    emit "$out"
    exit 2
  fi

  if [[ "$VERIFY" == "1" ]]; then
    section "Verify reassignment"
    local attempt max=60
    for ((attempt=1; attempt<=max; attempt++)); do
      rc=0
      set +e
      out="$(_remote_reassign "$broker" verify "$JSON_OUT" 2>&1)"
      rc=$?
      set -e
      if ! grep -qiE 'still in progress|is in progress' <<<"$out"; then
        if [[ $rc -eq 0 ]] || grep -qE 'is completed' <<<"$out"; then
          emit "${C_GREEN}Verify OK (attempt ${attempt}/${max})${C_RESET}"
          [[ "$VERBOSE" == "1" ]] && emit "$out"
          break
        fi
      fi
      emit "  … in progress (${attempt}/${max})"
      [[ "$VERBOSE" == "1" ]] && emit "$out"
      if (( attempt == max )); then
        loge "Verify still incomplete after ${max} attempts — re-check with the JSON file"
        emit "$out"
        exit 2
      fi
      sleep 5
    done
  fi

  emit ""
  emit "Done. Plan file: ${JSON_OUT}"
  exit 0
}

main "$@"

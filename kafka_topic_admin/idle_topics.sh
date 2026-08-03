#!/usr/bin/env bash
# Report / delete idle (dead) Kafka topics by last segment write age + consumer attachment.
#
# Default: print age-bucket table.
# Delete: topics idle longer than --idle-days, optionally requiring no active consumers.
#
# Usage:
#   ./idle_topics.sh -c ../kafka_realtime_check/config/clusters/devkafka.env -u USER
#   ./idle_topics.sh -c … -u USER --idle-days 180 --with-consumers
#   ./idle_topics.sh -c … -u USER -y --idle-days 180 --delete --require-no-consumers --apply
#   ./idle_topics.sh -c … -u USER -y --idle-days 180 --delete --allow-with-consumers --apply
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_VERSION="$(cat "${ROOT_DIR}/VERSION" 2>/dev/null | tr -d '[:space:]' || echo 0.0.0)"
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
USE_SUDO=0
VERBOSE=0
APPLY=0
FORCE_BROAD=0
CHECK_SSH=0
WITH_CONSUMERS=0
MAX_TOPICS="${MAX_TOPICS:-500}"
ACTION="report"   # report | delete
IDLE_DAYS="${IDLE_DAYS:-180}"
CONSUMER_POLICY="require-no-consumers"  # require-no-consumers | allow-with-consumers
JOBS_ARG="8"
ADMIN_BROKER_HOST=""
ADMIN_BROKER_OVERRIDE=""
REPORT_DIR="${ROOT_DIR}/reports"
TSV_OUT=""
_OK_FILE=""
_FAIL_FILE=""
_LOCK_FILE=""
_ACTIVE_TOPICS_FILE=""
_DESCRIBE_PARTS_DIR=""
_ALL_KAFKA_HOSTS=()

usage() {
  cat <<EOF
Usage: $(basename "$0") -c CONFIG.env [options]

Idle / dead topic report & cleanup v${SCRIPT_VERSION}

Actions:
  --report                 Age-bucket table + TSV (default)
  --delete                 Delete topics matching idle (+ consumer) policy

Idle / consumer policy:
  --idle-days N            No segment write for N days (default: ${IDLE_DAYS})
  --with-consumers         Also scan groups and annotate active consumers (report)
  --require-no-consumers   Delete only if no active consumer assigned (default)
  --allow-with-consumers   Delete even if a consumer group is assigned

Filters / safety:
  --pattern REGEX          Narrow topics (repeatable OR)
  --exclude REGEX          Drop matching names
  --include-internal       Include '_' names (default: skip)
  --force-broad            Allow delete with no/narrow-broad patterns
  --max-topics N           Abort delete if candidates > N (default: ${MAX_TOPICS})
  --apply                  Actually delete (without it: dry-run)
  --jobs N|auto|ask        Parallel workers (default: 8)

Connection:
  -c, --config FILE        Cluster inventory (.env)
  -u, --user USER          SSH username
  --admin-broker HOST      Broker for kafka CLI (default: first BROKER_HOSTS)
  --check-ssh              Sweep SSH to all inventory hosts
  --sudo                   Enable sudo on remotes
  --tsv FILE               Write per-topic TSV (default under reports/)
  -y, --yes                Non-interactive confirms
  -v, --verbose
  -h, --help

Examples:
  $(basename "$0") -c ../kafka_realtime_check/config/clusters/devkafka.env -u USER
  $(basename "$0") -c … -u USER --idle-days 180 --with-consumers
  $(basename "$0") -c … -u USER -y --idle-days 180 --delete --require-no-consumers --apply
  $(basename "$0") -c … -u USER -y --idle-days 180 --delete --allow-with-consumers \\
    --pattern '^admin-panel-' --apply
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -c|--config) CONFIG_FILE="$2"; shift 2 ;;
      -u|--user) SSH_USER_OVERRIDE="$2"; shift 2 ;;
      --report) ACTION="report"; shift ;;
      --delete) ACTION="delete"; shift ;;
      --idle-days) IDLE_DAYS="$2"; shift 2 ;;
      --with-consumers) WITH_CONSUMERS=1; shift ;;
      --require-no-consumers) CONSUMER_POLICY="require-no-consumers"; shift ;;
      --allow-with-consumers) CONSUMER_POLICY="allow-with-consumers"; shift ;;
      --pattern|--include|--topic-pattern) entity_filter_add_pattern "$2"; shift 2 ;;
      --exclude|--exclude-pattern) entity_filter_add_exclude "$2"; shift 2 ;;
      --include-internal) INCLUDE_INTERNAL=1; shift ;;
      --force-broad) FORCE_BROAD=1; shift ;;
      --max-topics) MAX_TOPICS="$2"; shift 2 ;;
      --jobs) JOBS_ARG="$2"; shift 2 ;;
      --apply) APPLY=1; shift ;;
      --check-ssh) CHECK_SSH=1; shift ;;
      --admin-broker) ADMIN_BROKER_OVERRIDE="$2"; shift 2 ;;
      --tsv) TSV_OUT="$2"; shift 2 ;;
      --sudo) USE_SUDO=1; shift ;;
      -n|--no-sudo) USE_SUDO=0; shift ;;
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

_pick_admin_broker() {
  local brokers=()
  if [[ -n "$ADMIN_BROKER_OVERRIDE" ]]; then
    ADMIN_BROKER_HOST="$ADMIN_BROKER_OVERRIDE"
  else
    csv_to_array brokers "${BROKER_HOSTS:-}"
    if ((${#brokers[@]} == 0)); then
      loge "BROKER_HOSTS empty in inventory"; exit 2
    fi
    ADMIN_BROKER_HOST="${brokers[0]}"
  fi
  if declare -F mark_ssh_ok >/dev/null 2>&1; then
    mark_ssh_ok "$ADMIN_BROKER_HOST"
  fi
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
  if [[ -n "$ADMIN_BROKER_OVERRIDE" ]]; then
    ADMIN_BROKER_HOST="$ADMIN_BROKER_OVERRIDE"
  fi
  if [[ -z "$ADMIN_BROKER_HOST" ]]; then
    loge "No SSH-reachable broker in BROKER_HOSTS"; exit 2
  fi
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
  if [[ "$p" =~ ^\.\*$ ]] || [[ "$p" =~ ^\^?\.\*\$?$ ]]; then
    return 0
  fi
  return 1
}

_validate_patterns_for_delete() {
  if ((${#NAME_PATTERNS[@]} == 0)); then
    if [[ "$FORCE_BROAD" == "1" ]]; then
      emit "${C_YELLOW}WARNING: no --pattern; selecting all idle topics via --force-broad${C_RESET}"
      return 0
    fi
    loge "For --delete without --pattern pass --force-broad (or narrow with --pattern)"
    exit 2
  fi
  local p
  for p in "${NAME_PATTERNS[@]}"; do
    if _pattern_is_broad "$p"; then
      if [[ "$FORCE_BROAD" == "1" ]]; then
        emit "${C_YELLOW}WARNING: broad pattern '${p}' allowed via --force-broad${C_RESET}"
      else
        loge "Pattern '${p}' is too broad. Narrow it or pass --force-broad"
        exit 2
      fi
    fi
  done
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

# Per-broker scan → lines: topic<TAB>mtime_epoch<TAB>bytes
_remote_topic_mtime_bytes() {
  local host="$1"
  local logdir="${KAFKA_LOG_DIR:-/var/opt/kafka/logs}"
  local remote_cmd
  remote_cmd=$(cat <<REMOTE
set -euo pipefail
LOGDIR='${logdir}'
cd "\$LOGDIR" || { echo "NO_LOGDIR \$LOGDIR" >&2; exit 2; }
TMP=\$(mktemp)
trap 'rm -f "\$TMP"' EXIT
for d in */; do
  d="\${d%/}"
  case "\$d" in *-[0-9]*) ;; *) continue ;; esac
  t="\${d%-*}"
  m=\$(find "\$d" -maxdepth 1 -name '*.log' -printf '%T@\\n' 2>/dev/null | sort -rn | head -1 || true)
  [ -z "\$m" ] && continue
  sz=\$(du -sb "\$d" 2>/dev/null | cut -f1 || echo 0)
  printf '%s\\t%s\\t%s\\n' "\$t" "\${m%.*}" "\${sz:-0}"
done > "\$TMP"
awk -F'\\t' '{ if (\$2>m[\$1]) m[\$1]=\$2; s[\$1]+=\$3 } END { for (t in m) printf "%s\\t%s\\t%s\\n", t, m[t], s[t] }' "\$TMP"
REMOTE
)
  _remote_run_script "$host" 0 "$remote_cmd"
}

# Merge all brokers: newest mtime across cluster, max local bytes (avoids RF double-count)
_scan_idle_all_brokers() {
  local brokers=() h out now
  csv_to_array brokers "${BROKER_HOSTS:-}"
  now="$(date +%s)"
  local merge
  merge="$(mktemp "${TMPDIR:-/tmp}/idle-merge.XXXXXX")"
  : >"$merge"
  for h in "${brokers[@]}"; do
    [[ -z "$h" ]] && continue
    if declare -F mark_ssh_ok >/dev/null 2>&1; then
      mark_ssh_ok "$h"
    fi
    # Progress on stderr so callers can redirect stdout → TSV cleanly
    printf '  scanning log.dirs on %s …\n' "$h" >&2
    out="$(_remote_topic_mtime_bytes "$h" 2>/dev/null || true)"
    if [[ -z "${out// }" ]]; then
      printf '  WARN empty/failed scan on %s\n' "$h" >&2
      continue
    fi
    printf '%s\n' "$out" >>"$merge"
  done
  # topic -> max_mtime, max_bytes
  awk -F'\t' -v now="$now" '
    NF>=3 && $1 !~ /[[:space:]]/ {
      t=$1; m=$2+0; b=$3+0
      if (!(t in mm) || m>mm[t]) mm[t]=m
      if (!(t in bb) || b>bb[t]) bb[t]=b
    }
    END {
      for (t in mm) printf "%s\t%.0f\t%.0f\n", t, (now-mm[t])/86400, bb[t]
    }
  ' "$merge" | sort -t$'\t' -k2 -rn
  rm -f "$merge"
}

_remote_list_groups() {
  local host="$1"
  local bootstrap="${KAFKA_CONNECT_BOOTSTRAP:-${KAFKA_BOOTSTRAP:-}}"
  local conf="${KAFKA_COMMAND_CONFIG:-}"
  local bin="${KAFKA_BIN:-/opt/kafka/bin}"
  local remote_cmd
  remote_cmd=$(cat <<REMOTE
set -euo pipefail
BIN='${bin}'; BOOT='${bootstrap}'; CONF='${conf}'
$(_kafka_admin_args_remote)
timeout ${KAFKA_ADMIN_TIMEOUT_SEC:-120} "\$BIN/kafka-consumer-groups.sh" "\${ARGS[@]}" --list
REMOTE
)
  _remote_run_script "$host" 0 "$remote_cmd"
}

_remote_describe_group() {
  local host="$1" group="$2"
  local bootstrap="${KAFKA_CONNECT_BOOTSTRAP:-${KAFKA_BOOTSTRAP:-}}"
  local conf="${KAFKA_COMMAND_CONFIG:-}"
  local bin="${KAFKA_BIN:-/opt/kafka/bin}"
  local remote_cmd
  remote_cmd="BIN='${bin}'; BOOT='${bootstrap}'; CONF='${conf}'; "
  remote_cmd+="ARGS=(--bootstrap-server \"\$BOOT\"); [[ -n \"\$CONF\" && -f \"\$CONF\" ]] && ARGS+=(--command-config \"\$CONF\"); "
  remote_cmd+="timeout ${KAFKA_ADMIN_TIMEOUT_SEC:-90} \"\$BIN/kafka-consumer-groups.sh\" \"\${ARGS[@]}\" "
  remote_cmd+="--describe --group $(printf '%q' "$group")"
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

_topics_with_active_consumers_from_describe() {
  local out="$1"
  awk '
    BEGIN { IGNORECASE=1 }
    /^[[:space:]]*$/ { next }
    /Error|Exception|does not exist|TIMEOUT/ { next }
    $1 ~ /^GROUP$/ && $2 ~ /^TOPIC$/ { next }
    {
      topic=$2
      consumer=$7
      if (topic == "" || topic == "-") next
      if (consumer == "" || consumer == "-") next
      if (consumer ~ /^ConsumerId/) next
      print topic
    }
  ' <<<"$out"
}

_describe_group_one() {
  local group="$1" out topics safe
  out="$(_remote_describe_group "$ADMIN_BROKER_HOST" "$group" 2>/dev/null || true)"
  topics="$(_topics_with_active_consumers_from_describe "$out")"
  if [[ -n "$topics" ]]; then
    safe="$(printf '%s' "$group" | tr -c 'A-Za-z0-9._-' '_')"
    printf '%s\n' "$topics" >"${_DESCRIBE_PARTS_DIR}/${safe}.txt"
  fi
}

_collect_active_consumer_topics() {
  local -a groups=()
  local g groups_out
  section "Scan consumer groups (active assignments)"
  groups_out="$(_remote_list_groups "$ADMIN_BROKER_HOST" 2>/dev/null || true)"
  while IFS= read -r g; do
    g="${g//$'\r'/}"
    [[ -z "$g" ]] && continue
    groups+=("$g")
  done <<<"$groups_out"
  emit "Consumer groups listed: ${#groups[@]}"
  : >"$_ACTIVE_TOPICS_FILE"
  if ((${#groups[@]} == 0)); then
    return 0
  fi

  resolve_parallel_jobs "${#groups[@]}"
  _DESCRIBE_PARTS_DIR="$(mktemp -d "${TMPDIR:-/tmp}/idle-active.XXXXXX")"
  run_parallel_fn _describe_group_one "${groups[@]}"
  if compgen -G "${_DESCRIBE_PARTS_DIR}/*.txt" >/dev/null 2>&1; then
    sort -u "${_DESCRIBE_PARTS_DIR}"/*.txt >"$_ACTIVE_TOPICS_FILE"
  fi
  rm -rf "$_DESCRIBE_PARTS_DIR"
  _DESCRIBE_PARTS_DIR=""
  emit "Topics with active consumers: $(wc -l <"$_ACTIVE_TOPICS_FILE" | tr -d ' ')"
}

_print_age_buckets() {
  local tsv="$1"
  emit ""
  emit "=== Age buckets (newest segment write) ==="
  awk -F'\t' 'NR==1 && $1=="topic" { next }
  {
    a=$2+0
    if (a<1) k="0 <1d"
    else if (a<7) k="1 1-7d"
    else if (a<30) k="2 7-30d"
    else if (a<90) k="3 30-90d"
    else if (a<180) k="4 90-180d"
    else k="5 >180d"
    c[k]++; b[k]+=$3
  }
  END {
    split("0 <1d|1 1-7d|2 7-30d|3 30-90d|4 90-180d|5 >180d", order, "|")
    for (i=1; i<=6; i++) {
      k=order[i]
      printf "  %-10s topics=%-5d bytes=%.1fGB\n", k, c[k]+0, (b[k]+0)/1073741824
    }
  }' "$tsv"
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

main() {
  parse_args "$@"
  load_config

  if ! [[ "$IDLE_DAYS" =~ ^[0-9]+$ ]] || (( IDLE_DAYS < 1 )); then
    loge "--idle-days must be >= 1"; exit 2
  fi
  if ! [[ "$MAX_TOPICS" =~ ^[0-9]+$ ]] || (( MAX_TOPICS < 1 )); then
    loge "--max-topics must be >= 1"; exit 2
  fi
  if [[ "$ACTION" == "delete" ]]; then
    _validate_patterns_for_delete
  fi

  init_result_store
  init_ssh_status_store
  init_parallel_store
  install_interrupt_traps

  mkdir -p "$REPORT_DIR"
  if [[ -z "$TSV_OUT" ]]; then
    local slug
    slug="$(printf '%s' "${CLUSTER_NAME:-cluster}" | tr -cs 'A-Za-z0-9._-' '_' | sed 's/_\+/_/g; s/^_//; s/_$//')"
    TSV_OUT="${REPORT_DIR}/${slug}-idle-topics.tsv"
  fi

  emit "${C_BOLD}Idle topics v${SCRIPT_VERSION}${C_RESET} — ${CLUSTER_NAME:-cluster}"
  emit "Config: ${CONFIG_FILE}"
  emit "Action: ${ACTION}  idle-days>=${IDLE_DAYS}  consumers=${CONSUMER_POLICY}"
  emit "Apply:  $([[ "$APPLY" == "1" ]] && echo yes || echo DRY-RUN)"

  prompt_credentials
  if [[ -n "${KAFKA_BOOTSTRAP:-}" ]]; then
    KAFKA_CONNECT_BOOTSTRAP="$KAFKA_BOOTSTRAP"
  fi
  export KAFKA_CONNECT_BOOTSTRAP

  if [[ "$CHECK_SSH" == "1" ]]; then
    section "SSH connectivity"
    _ensure_ssh_hosts
  else
    _pick_admin_broker
  fi
  emit "Admin broker: ${ADMIN_BROKER_HOST}"
  emit "Log dir:      ${KAFKA_LOG_DIR:-/var/opt/kafka/logs}"
  entity_filter_summary

  section "Scan log.dirs on all brokers"
  local raw_tsv filtered_tsv
  raw_tsv="$(mktemp "${TMPDIR:-/tmp}/idle-raw.XXXXXX")"
  {
    printf 'topic\tidle_days\tbytes\n'
    _scan_idle_all_brokers
  } >"$raw_tsv"

  filtered_tsv="$(mktemp "${TMPDIR:-/tmp}/idle-filt.XXXXXX")"
  {
    printf 'topic\tidle_days\tbytes\n'
    local topic days bytes
    while IFS=$'\t' read -r topic days bytes; do
      [[ "$topic" == "topic" ]] && continue
      [[ -z "$topic" ]] && continue
      name_matches_filter "$topic" || continue
      printf '%s\t%s\t%s\n' "$topic" "$days" "$bytes"
    done < <(tail -n +2 "$raw_tsv")
  } >"$filtered_tsv"

  cp "$filtered_tsv" "$TSV_OUT"
  emit "Wrote ${TSV_OUT} ($(tail -n +2 "$TSV_OUT" | wc -l | tr -d ' ') topics after name filter)"
  _print_age_buckets "$filtered_tsv"

  local idle_count idle_bytes
  idle_count=$(awk -F'\t' -v d="$IDLE_DAYS" 'NR>1 && $2+0>=d {n++} END{print n+0}' "$filtered_tsv")
  idle_bytes=$(awk -F'\t' -v d="$IDLE_DAYS" 'NR>1 && $2+0>=d {b+=$3} END{print b+0}' "$filtered_tsv")
  emit ""
  emit "Idle >= ${IDLE_DAYS}d: ${idle_count} topics, $(awk -v b="$idle_bytes" 'BEGIN{printf "%.1fGB", b/1073741824}')"

  _ACTIVE_TOPICS_FILE="$(mktemp "${TMPDIR:-/tmp}/idle-active-topics.XXXXXX")"
  : >"$_ACTIVE_TOPICS_FILE"

  local need_consumers=0
  if [[ "$ACTION" == "delete" && "$CONSUMER_POLICY" == "require-no-consumers" ]]; then
    need_consumers=1
  fi
  if [[ "$WITH_CONSUMERS" == "1" ]]; then
    need_consumers=1
  fi
  if [[ "$need_consumers" == "1" ]]; then
    _collect_active_consumer_topics
  else
    emit "Consumer scan skipped (use --with-consumers or --delete --require-no-consumers)"
  fi

  local cand_tsv
  cand_tsv="$(mktemp "${TMPDIR:-/tmp}/idle-cand.XXXXXX")"
  {
    printf 'topic\tidle_days\tbytes\thas_active_consumer\n'
    local topic days bytes has
    while IFS=$'\t' read -r topic days bytes; do
      [[ "$topic" == "topic" || -z "$topic" ]] && continue
      (( days + 0 >= IDLE_DAYS )) || continue
      has=0
      if [[ -s "$_ACTIVE_TOPICS_FILE" ]] && grep -Fxq "$topic" "$_ACTIVE_TOPICS_FILE" 2>/dev/null; then
        has=1
      fi
      printf '%s\t%s\t%s\t%s\n' "$topic" "$days" "$bytes" "$has"
    done < <(tail -n +2 "$filtered_tsv")
  } >"$cand_tsv"

  if [[ "$need_consumers" == "1" ]]; then
    local with_c without_c
    with_c=$(awk -F'\t' 'NR>1 && $4==1 {n++} END{print n+0}' "$cand_tsv")
    without_c=$(awk -F'\t' 'NR>1 && $4==0 {n++} END{print n+0}' "$cand_tsv")
    emit "Among idle>=${IDLE_DAYS}d: active-consumer=${with_c}  no-active-consumer=${without_c}"
  fi

  if [[ "$VERBOSE" == "1" ]] || (( idle_count > 0 && idle_count <= 40 )); then
    section "Idle candidates (idle_days >= ${IDLE_DAYS})"
    awk -F'\t' 'NR==1 { printf "  %-8s %-12s %-10s %s\n", "consumers", "idle_days", "bytes", "topic"; next }
      { printf "  %-8s %-12s %-10.1fMB %s\n", ($4==1?"ACTIVE":"none"), $2, $3/1048576, $1 }' "$cand_tsv" | head -n 60
    if (( idle_count > 40 )) && [[ "$VERBOSE" != "1" ]]; then
      emit "  … and $((idle_count - 40)) more (use -v)"
    fi
  fi

  if [[ "$ACTION" == "report" ]]; then
    rm -f "$raw_tsv" "$filtered_tsv" "$cand_tsv" "$_ACTIVE_TOPICS_FILE"
    exit 0
  fi

  local -a matches=()
  local topic days bytes has
  while IFS=$'\t' read -r topic days bytes has; do
    [[ "$topic" == "topic" || -z "$topic" ]] && continue
    if [[ "$CONSUMER_POLICY" == "require-no-consumers" && "$has" == "1" ]]; then
      continue
    fi
    matches+=("$topic")
  done <"$cand_tsv"

  emit ""
  emit "Delete candidates: ${#matches[@]} (policy=${CONSUMER_POLICY})"
  if ((${#matches[@]} == 0)); then
    emit "${C_GREEN}Nothing to delete.${C_RESET}"
    rm -f "$raw_tsv" "$filtered_tsv" "$cand_tsv" "$_ACTIVE_TOPICS_FILE"
    exit 0
  fi
  if ((${#matches[@]} > MAX_TOPICS)); then
    loge "Candidates ${#matches[@]} > --max-topics ${MAX_TOPICS} — narrow --pattern / raise --idle-days / raise cap"
    exit 2
  fi

  if [[ "$APPLY" != "1" ]]; then
    emit "${C_YELLOW}DRY-RUN only. Re-run with --apply to delete.${C_RESET}"
    local i
    for i in "${!matches[@]}"; do
      if ((${#matches[@]} <= 40)) || [[ "$VERBOSE" == "1" ]] || (( i < 25 )); then
        emit "  would-delete ${matches[$i]}"
      fi
    done
    ((${#matches[@]} > 40)) && [[ "$VERBOSE" != "1" ]] && emit "  … and $((${#matches[@]} - 25)) more"
    rm -f "$raw_tsv" "$filtered_tsv" "$cand_tsv" "$_ACTIVE_TOPICS_FILE"
    exit 0
  fi

  section "Delete idle topics"
  resolve_parallel_jobs "${#matches[@]}"
  if ! _confirm "DELETE ${#matches[@]} idle topic(s) (>=${IDLE_DAYS}d, ${CONSUMER_POLICY}) with ${PARALLEL_JOBS} workers?"; then
    emit "Aborted"
    rm -f "$raw_tsv" "$filtered_tsv" "$cand_tsv" "$_ACTIVE_TOPICS_FILE"
    exit 0
  fi
  _OK_FILE="$(mktemp "${TMPDIR:-/tmp}/idle-ok.XXXXXX")"
  _FAIL_FILE="$(mktemp "${TMPDIR:-/tmp}/idle-fail.XXXXXX")"
  _LOCK_FILE="$(mktemp "${TMPDIR:-/tmp}/idle-lock.XXXXXX")"
  : >"$_OK_FILE"; : >"$_FAIL_FILE"; : >"$_LOCK_FILE"
  run_parallel_fn _delete_one "${matches[@]}"

  local ok fail
  ok=$(wc -l <"$_OK_FILE" | tr -d ' ')
  fail=$(wc -l <"$_FAIL_FILE" | tr -d ' ')
  rm -f "$_OK_FILE" "$_FAIL_FILE" "$_LOCK_FILE" "$raw_tsv" "$filtered_tsv" "$cand_tsv" "$_ACTIVE_TOPICS_FILE"
  emit ""
  emit "Done: ok=${ok} fail=${fail} idle-days>=${IDLE_DAYS} jobs=${PARALLEL_JOBS}"
  (( fail == 0 )) || exit 2
  exit 0
}

main "$@"

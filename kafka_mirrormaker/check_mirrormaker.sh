#!/usr/bin/env bash
# MirrorMaker 2 dedicated (connect-mirror-maker.sh) health + lag dump.
# Run on the MM host. Kafka protocol to source/dest; no SSH required.
#
#   ./check_mirrormaker.sh -c config/clusters/prod.env -c config/clusters/dr.env \
#     --mm2-properties /var/opt/kafka/config/mm2.properties -y
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_VERSION="$(cat "${ROOT_DIR}/VERSION" 2>/dev/null | tr -d '[:space:]' || echo 0.0.0)"
LIB_DIR="${LIB_DIR:-${ROOT_DIR}/../kafka_realtime_check/lib}"
MM_LIB="${ROOT_DIR}/lib"

# shellcheck source=/dev/null
source "${LIB_DIR}/common.sh"
# shellcheck source=/dev/null
source "${LIB_DIR}/tasks.sh"
# shellcheck source=/dev/null
source "${MM_LIB}/cli.sh"

VIA="kafka"
NONINTERACTIVE=0
VERBOSE=0
CONFIG_FILES=()
SOURCE_ENV=""
DEST_ENV=""
SOURCE_ALIAS="${SOURCE_ALIAS:-}"
DEST_ALIAS="${DEST_ALIAS:-}"
MM2_PROPERTIES="${MM2_PROPERTIES:-/var/opt/kafka/config/mm2.properties}"
MM2_UNIT="${MM2_SYSTEMD_UNIT:-mirrormaker2}"
REST_URL="${CONNECT_REST:-http://127.0.0.1:8083}"
JOLOKIA_URL="${MM2_JOLOKIA_URL:-http://127.0.0.1:8778/jolokia}"
LOCAL_BIN=""
LAG_WARN="${LAG_WARN:-10000}"
LAG_FAIL="${LAG_FAIL:-100000}"
CHECKPOINT_STALE_SEC="${CHECKPOINT_STALE_SEC:-300}"
REPORT_DIR="${ROOT_DIR}/reports"

TASK_CATALOG=(
  "unit||systemd unit + recent journal errors|service"
  "config||Parse mm2.properties (secrets masked)|props"
  "process||Heap, CPU, open files of MM2 JVM|jvm"
  "internal_topics||heartbeats / checkpoints / offset-syncs size+HWM|mm2topics"
  "lag||Source group lag + mapped HWM gaps + checkpoint age|replication"
  "jmx||Local Jolokia/JMX MM2 metrics if present|metrics"
  "rest||Connect REST (often absent on dedicated MM2)|connect"
)

TASKS_LIST_EXAMPLES=$(cat <<'EOF'
  --only unit,config,process
  --only internal_topics,lag
EOF
)

usage() {
  cat <<EOF
Usage: $(basename "$0") -c SOURCE.env -c DEST.env [options]

MM2 dedicated health v${SCRIPT_VERSION}

  -c, --config FILE
  --mm2-properties FILE    default: ${MM2_PROPERTIES}
  --unit NAME              systemd unit (default: ${MM2_UNIT})
  --source-alias / --dest-alias
  --via kafka|ssh          ssh stub in v0.1
  --rest-url URL           default ${REST_URL}
  --jolokia-url URL        default ${JOLOKIA_URL}
  --only/--skip/--list-tasks
  -y, --yes  -v  -h
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -c|--config) CONFIG_FILES+=("$2"); shift 2 ;;
      --mm2-properties) MM2_PROPERTIES="$2"; shift 2 ;;
      --unit) MM2_UNIT="$2"; shift 2 ;;
      --source-alias) SOURCE_ALIAS="$2"; shift 2 ;;
      --dest-alias) DEST_ALIAS="$2"; shift 2 ;;
      --via) VIA="$2"; shift 2 ;;
      --rest-url) REST_URL="$2"; shift 2 ;;
      --jolokia-url) JOLOKIA_URL="$2"; shift 2 ;;
      --local-bin) LOCAL_BIN="$2"; shift 2 ;;
      --only) ONLY_TASKS="$2"; shift 2 ;;
      --skip) SKIP_TASKS="$2"; shift 2 ;;
      --ask-tasks) ASK_TASKS=1; shift ;;
      --list-tasks) LIST_TASKS=1; shift ;;
      -y|--yes) NONINTERACTIVE=1; shift ;;
      -v|--verbose) VERBOSE=1; shift ;;
      -h|--help) usage; exit 0 ;;
      *) loge "Unknown argument: $1"; usage; exit 2 ;;
    esac
  done
}

_resolve_cfg() {
  local f="$1"
  [[ -f "$f" ]] && { printf '%s' "$f"; return 0; }
  [[ -f "${ROOT_DIR}/${f}" ]] && { printf '%s' "${ROOT_DIR}/${f}"; return 0; }
  [[ -f "${ROOT_DIR}/config/clusters/${f}" ]] && { printf '%s' "${ROOT_DIR}/config/clusters/${f}"; return 0; }
  [[ -f "${ROOT_DIR}/config/clusters/${f}.env" ]] && { printf '%s' "${ROOT_DIR}/config/clusters/${f}.env"; return 0; }
  return 1
}

_assign_roles() {
  SOURCE_ENV=""; DEST_ENV=""
  if ((${#CONFIG_FILES[@]} != 2)); then
    loge "Need exactly two -c inventories (source and dest)"; exit 2
  fi
  local resolved=() f role
  for f in "${CONFIG_FILES[@]}"; do
    resolved+=("$(_resolve_cfg "$f" || { loge "Config not found: $f"; exit 2; })")
  done
  CONFIG_FILES=("${resolved[@]}")
  for f in "${CONFIG_FILES[@]}"; do
    role="$(bash -c 'set -a; source "$1"; set +a; printf "%s" "${ROLE:-}"' _ "$f")"
    case "$role" in
      source|prod) SOURCE_ENV="$f" ;;
      dest|dr) DEST_ENV="$f" ;;
    esac
  done
  if [[ -z "$SOURCE_ENV" || -z "$DEST_ENV" ]]; then
    SOURCE_ENV="${CONFIG_FILES[0]}"
    DEST_ENV="${CONFIG_FILES[1]}"
  fi
  LAG_WARN="$(bash -c 'set -a; source "$1"; set +a; printf "%s" "${LAG_WARN:-10000}"' _ "$SOURCE_ENV")"
  LAG_FAIL="$(bash -c 'set -a; source "$1"; set +a; printf "%s" "${LAG_FAIL:-100000}"' _ "$SOURCE_ENV")"
  CHECKPOINT_STALE_SEC="$(bash -c 'set -a; source "$1"; set +a; printf "%s" "${CHECKPOINT_STALE_SEC:-300}"' _ "$SOURCE_ENV")"
}

_probe_unit() {
  section "systemd ${MM2_UNIT}"
  if ! command -v systemctl >/dev/null 2>&1; then
    emit "INFO  systemctl not available"
    return 0
  fi
  local st nrest
  st="$(systemctl is-active "${MM2_UNIT}" 2>/dev/null || echo unknown)"
  nrest="$(systemctl show "${MM2_UNIT}" -p NRestarts --value 2>/dev/null || echo "?")"
  emit "Active: ${st}  NRestarts: ${nrest}"
  if [[ "$st" != "active" ]]; then
    emit "${C_RED}FAIL${C_RESET} unit is not active — journal:"
  else
    emit "${C_GREEN}PASS${C_RESET} unit active"
  fi
  journalctl -u "${MM2_UNIT}" -n 80 --no-pager 2>/dev/null | grep -iE 'error|exception|failed|timeout|oom' | tail -20 || true
}

_probe_config() {
  section "mm2.properties (masked)"
  if [[ ! -f "$MM2_PROPERTIES" ]]; then
    emit "${C_YELLOW}WARN${C_RESET} file not found: ${MM2_PROPERTIES}"
    return 0
  fi
  python3 "${MM_LIB}/mm2_parse.py" dump-props-masked "$MM2_PROPERTIES"
  emit ""
  emit "Enabled flows:"
  python3 "${MM_LIB}/mm2_parse.py" flows "$MM2_PROPERTIES" || true
  emit "replication.policy: $(python3 "${MM_LIB}/mm2_parse.py" policy "$MM2_PROPERTIES")"
}

_probe_process() {
  section "MM2 JVM process"
  local pid
  pid="$(pgrep -f 'connect-mirror-maker.sh' | head -1 || true)"
  if [[ -z "$pid" ]]; then
    pid="$(pgrep -f 'org.apache.kafka.connect.mirror.MirrorMaker' | head -1 || true)"
  fi
  if [[ -z "$pid" ]]; then
    emit "${C_YELLOW}WARN${C_RESET} connect-mirror-maker process not found"
    return 0
  fi
  emit "pid=${pid}"
  ps -o pid,pcpu,pmem,rss,etime,args -p "$pid" | sed 's/password=.*/password=***/g' || true
  local heap
  heap="$(tr '\0' '\n' <"/proc/${pid}/environ" 2>/dev/null | grep '^KAFKA_HEAP_OPTS=' || true)"
  emit "${heap:-KAFKA_HEAP_OPTS=(not in environ)}"
  if [[ "$heap" == *Xmx2G* || "$heap" == *Xmx2g* ]]; then
    emit "${C_YELLOW}WARN${C_RESET} heap is 2G — often tight for a full-cluster MM2 copy"
  fi
  local nfd
  nfd="$(ls /proc/${pid}/fd 2>/dev/null | wc -l | tr -d ' ')"
  emit "open_fds=${nfd}"
}

_internal_names() {
  local sa="$1" da="$2"
  printf '%s\n' \
    "${sa}.heartbeats" \
    "${da}.heartbeats" \
    "${sa}.checkpoints.internal" \
    "${da}.checkpoints.internal" \
    "mm2-offset-syncs.${sa}.internal" \
    "mm2-offset-syncs.${da}.internal"
}

_probe_internal_topics() {
  section "MM2 internal topics (dest log-dirs + HWM)"
  local names raw totals
  mapfile -t names < <(_internal_names "$SOURCE_ALIAS" "$DEST_ALIAS")
  raw="$(kafka_cli "$DEST_ENV" kafka-log-dirs.sh --describe --json 2>/dev/null || true)"
  printf '%s\n' "$raw" >"${WORK}/dst.logdirs.json"
  python3 "${MM_LIB}/mm2_parse.py" topic-totals <"${WORK}/dst.logdirs.json" >"${WORK}/dst.topics"
  kafka_cli "$DEST_ENV" kafka-get-offsets.sh --time -1 >"${WORK}/dst.off" 2>/dev/null || true
  local t uniq hwm
  emit "$(printf '%-42s %10s %12s' topic unique_GiB latest_hwm_sum)"
  for t in "${names[@]}"; do
    uniq="$(awk -F'\t' -v n="$t" '$1==n {print $3}' "${WORK}/dst.topics")"
    hwm="$(awk -F: -v n="$t" '$1==n {s+=$3} END{print s+0}' "${WORK}/dst.off")"
    if [[ -z "$uniq" ]]; then
      emit "$(printf '%-42s %10s %12s' "$t" MISSING "$hwm")"
    else
      emit "$(printf '%-42s %10s %12s' "$t" "$(python3 -c "print(round($uniq/1073741824,3))")" "$hwm")"
    fi
  done
}

_probe_lag() {
  section "Lag: consumer groups on source + HWM map"
  kafka_cli "$SOURCE_ENV" kafka-consumer-groups.sh --list >"${WORK}/src.groups" 2>/dev/null || true
  emit "Source groups matching mm2/connect/mirror:"
  grep -iE 'mm2|mirror|connect' "${WORK}/src.groups" 2>/dev/null | head -30 || emit "  (none matched name filter)"
  local g
  while IFS= read -r g; do
    [[ -z "$g" ]] && continue
    emit ""
    emit "describe group ${g}"
    kafka_cli "$SOURCE_ENV" kafka-consumer-groups.sh --describe --group "$g" 2>/dev/null | head -40 || true
  done < <(grep -iE 'mm2|mirror|connect' "${WORK}/src.groups" 2>/dev/null | head -8)

  emit ""
  emit "Mapped HWM sum gap (dest - source); WARN/FAIL vs LAG_WARN=${LAG_WARN} LAG_FAIL=${LAG_FAIL}"
  kafka_cli "$SOURCE_ENV" kafka-get-offsets.sh --time -1 >"${WORK}/src.off" 2>/dev/null || true
  kafka_cli "$DEST_ENV" kafka-get-offsets.sh --time -1 >"${WORK}/dst.off" 2>/dev/null || true
  local policy
  policy="$(python3 "${MM_LIB}/mm2_parse.py" policy "$MM2_PROPERTIES" 2>/dev/null || echo default)"
  python3 - "$policy" "$SOURCE_ALIAS" "${WORK}/src.off" "${WORK}/dst.off" "$LAG_WARN" "$LAG_FAIL" <<'PY'
import sys, os
sys.path.insert(0, os.environ["MM_LIB"])
from mm2_parse import parse_offsets, map_source_to_dest
from collections import defaultdict
policy, alias, srcp, dstp = sys.argv[1:5]
warn, fail = int(sys.argv[5]), int(sys.argv[6])
src = parse_offsets(open(srcp, encoding="utf-8", errors="replace").read())
dst = parse_offsets(open(dstp, encoding="utf-8", errors="replace").read())
ss, dd = defaultdict(int), defaultdict(int)
for (t, p), o in src.items():
    ss[t] += max(o, 0)
for (t, p), o in dst.items():
    dd[t] += max(o, 0)
behind = []
ahead = []
for st, so in ss.items():
    dt = map_source_to_dest(st, policy, alias)
    if dt not in dd:
        continue
    gap = so - dd[dt]  # source ahead of dest = lag
    if gap > 0:
        behind.append((gap, st, dt, so, dd[dt]))
    elif gap < 0:
        ahead.append((-gap, st, dt, so, dd[dt]))
behind.sort(reverse=True)
print("Dest behind source (replication lag), top 25:")
print(f"{'lag_hwm':>12} pair")
n_fail = n_warn = 0
for gap, st, dt, so, do in behind[:25]:
    tag = "OK"
    if gap >= fail:
        tag, n_fail = "FAIL", n_fail + 1
    elif gap >= warn:
        tag, n_warn = "WARN", n_warn + 1
    print(f"{gap:12d} {tag:4} {st} -> {dt}")
print(f"lagging_topics={len(behind)} warn_ge_{warn}={n_warn} fail_ge_{fail}={n_fail}")
print(f"dest_ahead_count={len(ahead)} (source retention already dropped data dest still holds)")
PY

  emit ""
  local ck="${SOURCE_ALIAS}.checkpoints.internal"
  if awk -F: -v n="$ck" '$1==n {found=1} END{exit found?0:1}' "${WORK}/dst.off" 2>/dev/null; then
    emit "INFO  ${ck} has offsets on dest (CHECKPOINT_STALE_SEC=${CHECKPOINT_STALE_SEC}; record timestamps not parsed in v0.1)"
  else
    emit "INFO  ${ck} not in dest offset list (missing or empty)"
  fi
}

_probe_jmx() {
  section "JMX / Jolokia on MM host"
  if curl -sf --max-time 3 "${JOLOKIA_URL}/version" >/dev/null 2>&1; then
    emit "Jolokia reachable at ${JOLOKIA_URL}"
    local beans=(
      "kafka.connect:type=connector-metrics,connector=*"
      "kafka.mirrormaker:type=MirrorSourceConnector"
    )
    local b
    for b in "${beans[@]}"; do
      curl -s --max-time 5 "${JOLOKIA_URL}/read/${b}" 2>/dev/null | head -c 500 || true
      echo
    done
    emit "Look for replication-latency-ms, checkpoint-latency-ms, byte-rate, record-count, failed task counts"
  else
    emit "INFO  Jolokia not reachable at ${JOLOKIA_URL} — enable JMX on MM2 JVM to export replication-latency-ms / byte-rate"
  fi
}

_probe_rest() {
  section "Connect REST ${REST_URL}"
  if curl -sf --max-time 3 "${REST_URL}/" >/dev/null 2>&1; then
    emit "REST up — connectors:"
    curl -s --max-time 8 "${REST_URL}/connectors" || true
    echo
  else
    emit "INFO  dedicated connect-mirror-maker.sh usually has no REST API on ${REST_URL} — not a cluster FAIL"
  fi
}

main() {
  parse_args "$@"
  if [[ "${LIST_TASKS}" == "1" ]]; then
    tasks_list
    exit 0
  fi
  tasks_select || exit $?
  _assign_roles
  export VIA MM_LIB
  if [[ "$VIA" == "ssh" ]]; then
    loge "--via ssh is a stub in v0.1"; exit 2
  fi
  if [[ -n "$LOCAL_BIN" ]]; then
    export KAFKA_BIN="$LOCAL_BIN"
  fi

  mkdir -p "$REPORT_DIR"
  WORK="$(mktemp -d "${TMPDIR:-/tmp}/mm-check.XXXXXX")"
  local report="${REPORT_DIR}/mm2-$(date +%Y%m%d-%H%M%S).txt"
  REPORT_FILE="$report"
  export REPORT_FILE

  if [[ -f "$MM2_PROPERTIES" && ( -z "$SOURCE_ALIAS" || -z "$DEST_ALIAS" ) ]]; then
    local flow
    flow="$(python3 "${MM_LIB}/mm2_parse.py" flows "$MM2_PROPERTIES" | head -1 || true)"
    if [[ "$flow" == *'->'* ]]; then
      SOURCE_ALIAS="${SOURCE_ALIAS:-${flow%%->*}}"
      DEST_ALIAS="${DEST_ALIAS:-${flow##*->}}"
    fi
  fi
  SOURCE_ALIAS="${SOURCE_ALIAS:-prod}"
  DEST_ALIAS="${DEST_ALIAS:-dr}"

  emit "${C_BOLD}MM2 health v${SCRIPT_VERSION}${C_RESET}"
  emit "Source env: ${SOURCE_ENV}"
  emit "Dest env:   ${DEST_ENV}"
  emit "Aliases:    ${SOURCE_ALIAS} -> ${DEST_ALIAS}"
  emit "Properties: ${MM2_PROPERTIES}"
  emit "Probes:     ${SELECTED_TASKS}"

  tasks_selected unit && _probe_unit
  tasks_selected config && _probe_config
  tasks_selected process && _probe_process
  tasks_selected internal_topics && _probe_internal_topics
  tasks_selected lag && _probe_lag
  tasks_selected jmx && _probe_jmx
  tasks_selected rest && _probe_rest

  emit ""
  emit "Report: ${report}"
  emit "Done."
}

main "$@"

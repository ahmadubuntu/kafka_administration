#!/usr/bin/env bash
# Compare Kafka log.dirs / topic metadata between source (prod) and dest (DR).
# Run on the MirrorMaker host with Kafka protocol + local CLI (--via kafka).
#
#   ./compare_storage.sh -c config/clusters/prod.env -c config/clusters/dr.env -y
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_VERSION="$(cat "${ROOT_DIR}/VERSION" 2>/dev/null | tr -d '[:space:]' || echo 0.0.0)"
LIB_DIR="${LIB_DIR:-${ROOT_DIR}/../kafka_realtime_check/lib}"
MM_LIB="${ROOT_DIR}/lib"
export MM_LIB
export PYTHONPATH="${MM_LIB}${PYTHONPATH:+:${PYTHONPATH}}"

# shellcheck source=/dev/null
source "${LIB_DIR}/common.sh"
# shellcheck source=/dev/null
source "${LIB_DIR}/tasks.sh"
# shellcheck source=/dev/null
source "${LIB_DIR}/entity_filter.sh"
# shellcheck source=/dev/null
source "${MM_LIB}/cli.sh"

# Full disk picture includes '_' topics unless the operator excludes them.
INCLUDE_INTERNAL=1

VIA="kafka"
NONINTERACTIVE=0
VERBOSE=0
CONFIG_FILES=()
SOURCE_ENV=""
DEST_ENV=""
SOURCE_ALIAS="${SOURCE_ALIAS:-}"
DEST_ALIAS="${DEST_ALIAS:-}"
MM2_PROPERTIES="${MM2_PROPERTIES:-/var/opt/kafka/config/mm2.properties}"
LOCAL_BIN=""
WORK=""
REPORT_DIR="${ROOT_DIR}/reports"

TASK_CATALOG=(
  "summary||Broker counts and log-dirs cluster/mean GiB|disk,brokers"
  "logdirs||Per-broker and per-topic log-dirs sizes|size,du"
  "topics||Partition count and RF vs mapped dest|rf,describe"
  "configs||retention.ms / cleanup.policy drift|retention"
  "gaps||Dest-only and source-only topics|unmapped,mm2"
  "offsets||High-watermark gaps on mapped pairs|hwm,lag"
)

TASKS_LIST_EXAMPLES=$(cat <<'EOF'
  --only summary,logdirs,gaps
  --only topics,configs,offsets
EOF
)

usage() {
  cat <<EOF
Usage: $(basename "$0") -c SOURCE.env -c DEST.env [options]

Prod vs DR storage compare v${SCRIPT_VERSION}

  -c, --config FILE        Inventory (exactly two: source then dest, or ROLE=source/dest)
  --source-alias NAME      MM2 source cluster alias (default: from mm2.properties flow)
  --dest-alias NAME        MM2 dest cluster alias
  --mm2-properties FILE    Dedicated MM2 properties (default: ${MM2_PROPERTIES})
  --via kafka|ssh          Collection method (ssh is a stub in v0.1)
  --local-bin DIR          Override KAFKA_BIN from inventories
  --pattern REGEX          Restrict topic names (entity_filter)
  --exclude REGEX
  --include-internal
  --only/--skip/--list-tasks
  -y, --yes
  -v, --verbose
  -h, --help
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -c|--config) CONFIG_FILES+=("$2"); shift 2 ;;
      --source-alias) SOURCE_ALIAS="$2"; shift 2 ;;
      --dest-alias) DEST_ALIAS="$2"; shift 2 ;;
      --mm2-properties) MM2_PROPERTIES="$2"; shift 2 ;;
      --via) VIA="$2"; shift 2 ;;
      --local-bin) LOCAL_BIN="$2"; shift 2 ;;
      --pattern|--include|--topic-pattern) entity_filter_add_pattern "$2"; shift 2 ;;
      --exclude|--exclude-pattern) entity_filter_add_exclude "$2"; shift 2 ;;
      --include-internal) INCLUDE_INTERNAL=1; shift ;;
      --exclude-internal) INCLUDE_INTERNAL=0; shift ;;
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
  local f role
  SOURCE_ENV=""; DEST_ENV=""
  if ((${#CONFIG_FILES[@]} != 2)); then
    loge "Need exactly two -c inventories (source and dest)"; exit 2
  fi
  local resolved=()
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
    emit "ROLE not set in env files - treating first -c as source, second as dest"
  fi
}

_apply_local_bin() {
  [[ -z "$LOCAL_BIN" ]] && return 0
  export KAFKA_BIN="$LOCAL_BIN"
}

_env_get() {
  bash -c 'set -a; source "$1"; set +a; printf "%s" "${!2}"' _ "$1" "$2"
}

_fetch_logdirs() {
  local envf="$1" out="$2"
  local rc=0
  set +e
  kafka_log_dirs_dump "$envf" "${out}.json" 2>"${out}.err"
  rc=$?
  set -e
  : >>"${out}.json"
  python3 "${MM_LIB}/mm2_parse.py" broker-totals <"${out}.json" >"${out}.brokers"
  python3 "${MM_LIB}/mm2_parse.py" topic-totals <"${out}.json" >"${out}.topics"
  local brows
  brows="$(wc -l <"${out}.brokers" | tr -d ' ')"
  if [[ "$brows" == "0" ]]; then
    emit "${C_YELLOW}WARN${C_RESET} empty log-dirs parse for ${envf} (rc=${rc}). First lines:"
    head -c 400 "${out}.json" | tr '\n' ' '
    emit ""
    [[ -s "${out}.err" ]] && emit "stderr: $(head -c 300 "${out}.err")"
  else
    emit "  ${envf}: ${brows} broker log-dir totals"
  fi
}

_filter_topic_totals() {
  local infile="$1" outfile="$2"
  : >"$outfile"
  local topic raw uniq
  while IFS=$'\t' read -r topic raw uniq; do
    [[ -z "$topic" ]] && continue
    name_matches_filter "$topic" || continue
    printf '%s\t%s\t%s\n' "$topic" "$raw" "$uniq" >>"$outfile"
  done <"$infile"
}

main() {
  parse_args "$@"
  if [[ "${LIST_TASKS}" == "1" ]]; then
    tasks_list
    exit 0
  fi
  tasks_select || exit $?
  _assign_roles
  export VIA
  if [[ "$VIA" != "kafka" && "$VIA" != "ssh" ]]; then
    loge "--via must be kafka or ssh"; exit 2
  fi
  if [[ "$VIA" == "ssh" ]]; then
    emit "${C_YELLOW}--via ssh is a stub in v0.1; falling back is not done. Exiting.${C_RESET}"
    loge "Provide Kafka protocol access or wait for SSH collection"
    exit 2
  fi
  _apply_local_bin

  mkdir -p "$REPORT_DIR"
  WORK="$(mktemp -d "${TMPDIR:-/tmp}/mm-storage.XXXXXX")"
  local report
  report="${REPORT_DIR}/storage-$(date +%Y%m%d-%H%M%S).txt"
  REPORT_FILE="$report"
  export REPORT_FILE

  emit "${C_BOLD}Storage compare v${SCRIPT_VERSION}${C_RESET}"
  emit "Source: ${SOURCE_ENV}"
  emit "Dest:   ${DEST_ENV}"
  emit "Via:    ${VIA}  probes: ${SELECTED_TASKS}"
  emit "MM2 properties: ${MM2_PROPERTIES}"
  entity_filter_summary

  local policy="default"
  if [[ -f "$MM2_PROPERTIES" ]]; then
    policy="$(python3 "${MM_LIB}/mm2_parse.py" policy "$MM2_PROPERTIES")"
    if [[ -z "$SOURCE_ALIAS" || -z "$DEST_ALIAS" ]]; then
      local flow
      flow="$(python3 "${MM_LIB}/mm2_parse.py" flows "$MM2_PROPERTIES" | head -1 || true)"
      if [[ "$flow" == *'->'* ]]; then
        SOURCE_ALIAS="${SOURCE_ALIAS:-${flow%%->*}}"
        DEST_ALIAS="${DEST_ALIAS:-${flow##*->}}"
      fi
    fi
  fi
  SOURCE_ALIAS="${SOURCE_ALIAS:-prod}"
  DEST_ALIAS="${DEST_ALIAS:-dr}"
  emit "Mapping: policy=${policy}  ${SOURCE_ALIAS} -> ${DEST_ALIAS}"

  if tasks_selected summary || tasks_selected logdirs || tasks_selected gaps || tasks_selected topics; then
    section "Fetch log-dirs JSON"
    _fetch_logdirs "$SOURCE_ENV" "${WORK}/src"
    _fetch_logdirs "$DEST_ENV" "${WORK}/dst"
    _filter_topic_totals "${WORK}/src.topics" "${WORK}/src.topics.f"
    _filter_topic_totals "${WORK}/dst.topics" "${WORK}/dst.topics.f"
    mv "${WORK}/src.topics.f" "${WORK}/src.topics"
    mv "${WORK}/dst.topics.f" "${WORK}/dst.topics"
  fi

  if tasks_selected summary; then
    section "Broker API versions (membership)"
    kafka_cli "$SOURCE_ENV" kafka-broker-api-versions.sh >"${WORK}/src.api" 2>/dev/null || true
    kafka_cli "$DEST_ENV" kafka-broker-api-versions.sh >"${WORK}/dst.api" 2>/dev/null || true
    local ns nd
    ns="$(grep -cE '\(id:' "${WORK}/src.api" || true)"
    nd="$(grep -cE '\(id:' "${WORK}/dst.api" || true)"
    emit "Source brokers listed: ${ns:-0}   dest brokers listed: ${nd:-0}"
  fi

  if tasks_selected topics; then
    section "Topic describe (RF / partitions)"
    kafka_cli "$SOURCE_ENV" kafka-topics.sh --describe >"${WORK}/src.describe" 2>/dev/null || true
    kafka_cli "$DEST_ENV" kafka-topics.sh --describe >"${WORK}/dst.describe" 2>/dev/null || true
  fi

  if tasks_selected configs; then
    section "Topic configs (retention / cleanup)"
    kafka_topic_configs_dump "$SOURCE_ENV" "${WORK}/src.configs" || true
    kafka_topic_configs_dump "$DEST_ENV" "${WORK}/dst.configs" || true
  fi

  if tasks_selected summary || tasks_selected logdirs || tasks_selected topics || tasks_selected configs || tasks_selected gaps; then
    section "Join source vs dest"
    local extra_tsv
    extra_tsv="${REPORT_DIR}/storage-extra-$(date +%Y%m%d-%H%M%S).tsv"
    : >>"${WORK}/src.describe"; : >>"${WORK}/dst.describe"
    : >>"${WORK}/src.configs"; : >>"${WORK}/dst.configs"
    [[ -f "${WORK}/src.brokers" ]] || : >"${WORK}/src.brokers"
    [[ -f "${WORK}/dst.brokers" ]] || : >"${WORK}/dst.brokers"
    [[ -f "${WORK}/src.topics" ]] || : >"${WORK}/src.topics"
    [[ -f "${WORK}/dst.topics" ]] || : >"${WORK}/dst.topics"
    python3 "${MM_LIB}/join_storage.py" \
      --policy "$policy" \
      --source-alias "$SOURCE_ALIAS" \
      --dest-alias "$DEST_ALIAS" \
      --src-brokers "${WORK}/src.brokers" \
      --dst-brokers "${WORK}/dst.brokers" \
      --src-topics "${WORK}/src.topics" \
      --dst-topics "${WORK}/dst.topics" \
      --src-describe "${WORK}/src.describe" \
      --dst-describe "${WORK}/dst.describe" \
      --src-configs "${WORK}/src.configs" \
      --dst-configs "${WORK}/dst.configs" \
      --extra-tsv "$extra_tsv" \
      --compress-commands "${REPORT_DIR}/storage-compress-dest-$(date +%Y%m%d-%H%M%S).txt" \
      --inferred-codec "${COMPRESS_INFERRED_CODEC:-lz4}" \
      --top 30
  fi

  if tasks_selected offsets; then
    section "Latest offsets (HWM) mapped pairs"
    kafka_cli "$SOURCE_ENV" kafka-get-offsets.sh --time -1 >"${WORK}/src.off" 2>/dev/null || true
    kafka_cli "$DEST_ENV" kafka-get-offsets.sh --time -1 >"${WORK}/dst.off" 2>/dev/null || true
    python3 - "$policy" "$SOURCE_ALIAS" "${WORK}/src.off" "${WORK}/dst.off" <<'PY'
import sys, os
sys.path.insert(0, os.environ.get("MM_LIB", "."))
from mm2_parse import parse_offsets, map_source_to_dest

policy, alias, srcp, dstp = sys.argv[1:5]
src = parse_offsets(open(srcp, encoding="utf-8", errors="replace").read())
dst = parse_offsets(open(dstp, encoding="utf-8", errors="replace").read())
# aggregate max offset per topic (sum of partition HWMs as a rough size-of-log proxy)
from collections import defaultdict
ss = defaultdict(int)
dd = defaultdict(int)
for (t, p), o in src.items():
    ss[t] += max(o, 0)
for (t, p), o in dst.items():
    dd[t] += max(o, 0)
rows = []
for st, so in ss.items():
    dt = map_source_to_dest(st, policy, alias)
    if dt not in dd:
        continue
    do = dd[dt]
    rows.append((do - so, st, dt, so, do))
rows.sort(reverse=True)
print(f"{'hwm_gap':>12} {'src_sum_hwm':>14} {'dst_sum_hwm':>14} pair")
print("Positive gap: dest HWM sum > source (possible leftover after source retention).")
print("Negative gap: dest behind source (replication lag).")
for i, (gap, st, dt, so, do) in enumerate(rows[:40]):
    print(f"{gap:12d} {so:14d} {do:14d} {st} -> {dt}")
ahead = sum(1 for g, *_ in rows if g > 0)
behind = sum(1 for g, *_ in rows if g < 0)
print(f"pairs={len(rows)} dest_ahead={ahead} dest_behind={behind}")
PY
  fi

  emit ""
  emit "Work dir: ${WORK}"
  emit "Report:   ${report}"
  emit "Done."
}

main "$@"

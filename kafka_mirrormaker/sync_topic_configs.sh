#!/usr/bin/env bash
# Compare topic configs on source vs dest; optionally set dest to match source.
# Default is dry-run. Mutates dest only with --apply and -y.
#
#   ./sync_topic_configs.sh -c config/clusters/prod.env -c config/clusters/dr.env -y
#   ./sync_topic_configs.sh -c prod.env -c dr.env -y --apply
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
source "${MM_LIB}/cli.sh"

VIA="kafka"
NONINTERACTIVE=0
VERBOSE=0
APPLY=0
INCLUDE_INTERNAL=0
SYNC_SKIPPED=0
PATTERN=""
EXCLUDE=""
CONFIG_FILES=()
SOURCE_ENV=""
DEST_ENV=""
LOCAL_BIN=""
REPORT_DIR="${ROOT_DIR}/reports"

usage() {
  cat <<EOF
Usage: $(basename "$0") -c SOURCE.env -c DEST.env [options]

Sync topic configs source → dest  v${SCRIPT_VERSION}

  -c, --config FILE     Exactly two inventories (source then dest, or ROLE=)
  --pattern REGEX       Only these topic names
  --exclude REGEX
  --include-internal    Include _ / __ topics
  --sync-skipped        Also sync throttle replica lists and remote.* keys
  --apply               Alter dest configs (default: print only)
  --local-bin DIR
  -y, --yes             Required together with --apply
  -v, --verbose
  -h, --help

Never creates topics (dry-run or --apply). --apply only sets configs on
topics that already exist on both sides. Does not change RF / partitions.
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -c|--config) CONFIG_FILES+=("$2"); shift 2 ;;
      --pattern) PATTERN="$2"; shift 2 ;;
      --exclude) EXCLUDE="$2"; shift 2 ;;
      --include-internal) INCLUDE_INTERNAL=1; shift ;;
      --sync-skipped) SYNC_SKIPPED=1; shift ;;
      --apply) APPLY=1; shift ;;
      --local-bin) LOCAL_BIN="$2"; shift 2 ;;
      --via) VIA="$2"; shift 2 ;;
      -y|--yes) NONINTERACTIVE=1; shift ;;
      -v|--verbose) VERBOSE=1; shift ;;
      -h|--help) usage; exit 0 ;;
      *) loge "Unknown argument: $1"; usage; exit 2 ;;
    esac
  done
}

main() {
  parse_args "$@"
  export VIA
  mm_assign_roles "$ROOT_DIR" || exit $?
  if [[ -n "$LOCAL_BIN" ]]; then
    export KAFKA_BIN="$LOCAL_BIN"
  fi
  if [[ "$APPLY" == "1" && "$NONINTERACTIVE" != "1" ]]; then
    loge "--apply requires -y"
    exit 2
  fi

  mkdir -p "$REPORT_DIR"
  local report work
  report="${REPORT_DIR}/sync-configs-$(date +%Y%m%d-%H%M%S).txt"
  REPORT_FILE="$report"
  export REPORT_FILE
  work="$(mktemp -d "${TMPDIR:-/tmp}/mm-sync-cfg.XXXXXX")"

  emit "${C_BOLD}Topic config sync v${SCRIPT_VERSION}${C_RESET}"
  emit "Source: ${SOURCE_ENV}"
  emit "Dest:   ${DEST_ENV}"
  emit "Mode:   $([[ "$APPLY" == "1" ]] && echo APPLY || echo DRY-RUN)"

  kafka_topic_configs_dump "$SOURCE_ENV" "${work}/src.configs" || true
  kafka_topic_configs_dump "$DEST_ENV" "${work}/dst.configs" || true
  if [[ ! -s "${work}/src.configs" ]]; then
    loge "No source topic configs parsed"
    exit 1
  fi

  local pyargs=(topic-diff --src-configs "${work}/src.configs" --dst-configs "${work}/dst.configs")
  [[ "$INCLUDE_INTERNAL" == "1" ]] && pyargs+=(--include-internal)
  [[ "$SYNC_SKIPPED" == "1" ]] && pyargs+=(--sync-skipped)
  [[ -n "$PATTERN" ]] && pyargs+=(--pattern "$PATTERN")
  [[ -n "$EXCLUDE" ]] && pyargs+=(--exclude "$EXCLUDE")

  python3 "${MM_LIB}/sync_parse.py" "${pyargs[@]}" >"${work}/diff.json"
  python3 - "${work}/diff.json" <<'PY'
import json, sys
data = json.load(open(sys.argv[1], encoding="utf-8"))
diffs = data.get("diffs") or []
sets = [r for r in diffs if r.get("action") == "set"]
skips = [r for r in diffs if r.get("action") == "skip"]
print("=== config drift (topics on both sides) ===")
print(f"{'topic':<48} {'key':<36} {'source':<22} dest")
if sets:
    for r in sets:
        print(f"{r['topic']:<48} {r['key']:<36} {r['src']:<22} {r['dst']}")
else:
    print("(none)")
print("=== missing dest topics (report only; never created) ===")
if skips:
    for r in skips:
        print(f"{r['topic']:<48} {r['key']:<36} {r['src']:<22} {r['dst']}")
else:
    print("(none)")
print(
    f"set={len(sets)}  report_missing_dest={len(skips)}  "
    f"topics_to_alter={len(data.get('alters') or {})}"
)
PY

  if [[ "$APPLY" != "1" ]]; then
    emit "Dry-run. --apply -y only alters configs on topics present on both sides."
    emit "Missing dest topics are reported only and are never created."
    emit "Report: ${report}"
    exit 0
  fi

  local ok=0 fail=0
  python3 -c '
import json, os, sys
sys.path.insert(0, os.environ["MM_LIB"])
from sync_parse import format_add_config
data = json.load(open(sys.argv[1], encoding="utf-8"))
out = sys.argv[2]
with open(out, "w", encoding="utf-8") as fh:
    for topic, pairs in sorted((data.get("alters") or {}).items()):
        pairs2 = [(p[0], p[1]) for p in pairs]
        for chunk in format_add_config(pairs2):
            fh.write(f"{topic}\t{chunk}\n")
' "${work}/diff.json" "${work}/alters.tsv"

  if [[ ! -s "${work}/alters.tsv" ]]; then
    emit "Nothing to apply (no config drift on topics present on both sides)."
    emit "Report: ${report}"
    exit 0
  fi

  emit "APPLY configs only for intersection topics (no topic create)."

  local topic chunk
  while IFS=$'\t' read -r topic chunk; do
    [[ -z "$topic" || -z "$chunk" ]] && continue
    emit "ALTER ${topic}  ${chunk}"
    if kafka_cli "$DEST_ENV" kafka-configs.sh --entity-type topics --entity-name "$topic" \
      --alter --add-config "$chunk"; then
      ok=$((ok + 1))
    else
      fail=$((fail + 1))
      loge "FAIL alter ${topic}"
    fi
  done <"${work}/alters.tsv"

  emit "Applied ok=${ok} fail=${fail}"
  emit "Report: ${report}"
  [[ "$fail" -eq 0 ]]
}

main "$@"

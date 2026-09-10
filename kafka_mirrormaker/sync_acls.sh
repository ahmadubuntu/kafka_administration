#!/usr/bin/env bash
# Compare kafka-acls --list on source vs dest; add missing dest ACLs.
# Default is dry-run. --prune also removes dest-only ACLs.
#
#   ./sync_acls.sh -c config/clusters/prod.env -c config/clusters/dr.env -y
#   ./sync_acls.sh -c prod.env -c dr.env -y --apply
#   ./sync_acls.sh -c prod.env -c dr.env -y --apply --prune
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
PRUNE=0
INCLUDE_INTERNAL=0
CONFIG_FILES=()
SOURCE_ENV=""
DEST_ENV=""
LOCAL_BIN=""
REPORT_DIR="${ROOT_DIR}/reports"

usage() {
  cat <<EOF
Usage: $(basename "$0") -c SOURCE.env -c DEST.env [options]

Sync ACLs source → dest  v${SCRIPT_VERSION}

  -c, --config FILE     Exactly two inventories (source then dest, or ROLE=)
  --include-internal    Include ACL resource names starting with _
  --apply               Add missing dest ACLs (default: print only)
  --prune               With --apply: also remove dest-only ACLs
  --local-bin DIR
  -y, --yes             Required together with --apply
  -v, --verbose
  -h, --help

Does not change topic data. Authorizer must allow this admin user to describe/add ACLs.
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -c|--config) CONFIG_FILES+=("$2"); shift 2 ;;
      --include-internal) INCLUDE_INTERNAL=1; shift ;;
      --apply) APPLY=1; shift ;;
      --prune) PRUNE=1; shift ;;
      --local-bin) LOCAL_BIN="$2"; shift 2 ;;
      --via) VIA="$2"; shift 2 ;;
      -y|--yes) NONINTERACTIVE=1; shift ;;
      -v|--verbose) VERBOSE=1; shift ;;
      -h|--help) usage; exit 0 ;;
      *) loge "Unknown argument: $1"; usage; exit 2 ;;
    esac
  done
}

_run_acl_line() {
  local envf="$1"
  shift
  kafka_cli "$envf" kafka-acls.sh "$@"
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
  if [[ "$PRUNE" == "1" && "$APPLY" != "1" ]]; then
    emit "${C_YELLOW}NOTE${C_RESET} --prune only takes effect with --apply (shown below as extra dest ACLs)"
  fi

  mkdir -p "$REPORT_DIR"
  local report work
  report="${REPORT_DIR}/sync-acls-$(date +%Y%m%d-%H%M%S).txt"
  REPORT_FILE="$report"
  export REPORT_FILE
  work="$(mktemp -d "${TMPDIR:-/tmp}/mm-sync-acl.XXXXXX")"

  emit "${C_BOLD}ACL sync v${SCRIPT_VERSION}${C_RESET}"
  emit "Source: ${SOURCE_ENV}"
  emit "Dest:   ${DEST_ENV}"
  emit "Mode:   $([[ "$APPLY" == "1" ]] && echo APPLY || echo DRY-RUN)$([[ "$PRUNE" == "1" ]] && echo "+PRUNE")"

  kafka_cli "$SOURCE_ENV" kafka-acls.sh --list >"${work}/src.acls" 2>"${work}/src.acls.err" || true
  kafka_cli "$DEST_ENV" kafka-acls.sh --list >"${work}/dst.acls" 2>"${work}/dst.acls.err" || true
  if grep -qiE 'authorizer|Unauthorized|security' "${work}/src.acls.err" "${work}/dst.acls.err" 2>/dev/null; then
    emit "${C_YELLOW}WARN${C_RESET} ACL list stderr (authorizer may be off or admin lacks Describe):"
    head -c 400 "${work}/src.acls.err" || true
    echo
    head -c 400 "${work}/dst.acls.err" || true
    echo
  fi

  local pyargs=(acl-diff --src-acls "${work}/src.acls" --dst-acls "${work}/dst.acls")
  [[ "$INCLUDE_INTERNAL" == "1" ]] && pyargs+=(--include-internal)
  python3 "${MM_LIB}/sync_parse.py" "${pyargs[@]}" >"${work}/diff.json"

  python3 - "${work}/diff.json" <<'PY'
import json, sys
data = json.load(open(sys.argv[1], encoding="utf-8"))
print("=== Missing on dest (would ADD) ===")
for r in data.get("add") or []:
    print(f"  + {r['perm']:5} {r['operation']:12} {r['rtype']:8} {r['pattern']:8} {r['name']}  {r['principal']} host={r['host']}")
if not data.get("add"):
    print("  (none)")
print("=== Extra on dest (PRUNE would remove) ===")
for r in data.get("extra") or []:
    print(f"  - {r['perm']:5} {r['operation']:12} {r['rtype']:8} {r['pattern']:8} {r['name']}  {r['principal']} host={r['host']}")
if not data.get("extra"):
    print("  (none)")
print("=== Skipped (unsupported resource type; not applied) ===")
for r in data.get("skipped") or []:
    print(f"  ? {r.get('rtype')} {r.get('name')}  {r.get('reason')}")
if not data.get("skipped"):
    print("  (none)")
print(f"add={len(data.get('add') or [])} extra={len(data.get('extra') or [])} skipped={len(data.get('skipped') or [])}")
PY

  if [[ "$APPLY" != "1" ]]; then
    emit "Dry-run. Re-run with --apply -y to add missing dest ACLs."
    emit "Report: ${report}"
    exit 0
  fi

  python3 -c '
import json, os, sys
sys.path.insert(0, os.environ["MM_LIB"])
from sync_parse import acl_cli_args
data = json.load(open(sys.argv[1], encoding="utf-8"))
out = sys.argv[2]
with open(out, "w", encoding="utf-8") as fh:
    for r in data.get("add") or []:
        try:
            acl_cli_args(r)
        except ValueError:
            continue
        fh.write("ADD\t{rtype}\t{name}\t{pattern}\t{principal}\t{host}\t{operation}\t{perm}\n".format(**r))
    if sys.argv[3] == "1":
        for r in data.get("extra") or []:
            try:
                acl_cli_args(r, remove=True)
            except ValueError:
                continue
            fh.write("DEL\t{rtype}\t{name}\t{pattern}\t{principal}\t{host}\t{operation}\t{perm}\n".format(**r))
' "${work}/diff.json" "${work}/ops.tsv" "$([[ "$PRUNE" == "1" ]] && echo 1 || echo 0)"

  local ok=0 fail=0 op rtype name pattern principal host operation perm
  local -a argv=()
  if [[ -s "${work}/ops.tsv" ]]; then
    while IFS=$'\t' read -r op rtype name pattern principal host operation perm; do
      [[ -z "$op" ]] && continue
      argv=()
      mapfile -t argv < <(python3 -c '
import os, sys
sys.path.insert(0, os.environ["MM_LIB"])
from sync_parse import acl_cli_args
e = dict(rtype=sys.argv[1], name=sys.argv[2], pattern=sys.argv[3],
         principal=sys.argv[4], host=sys.argv[5], operation=sys.argv[6], perm=sys.argv[7])
print("\n".join(acl_cli_args(e, remove=(sys.argv[8]=="DEL"))))
' "$rtype" "$name" "$pattern" "$principal" "$host" "$operation" "$perm" "$op")
      emit "${op} ${argv[*]}"
      if _run_acl_line "$DEST_ENV" "${argv[@]}"; then
        ok=$((ok + 1))
      else
        fail=$((fail + 1))
        loge "FAIL ${op} ACL"
      fi
    done <"${work}/ops.tsv"
  fi

  emit "Applied ok=${ok} fail=${fail}"
  emit "Report: ${report}"
  [[ "$fail" -eq 0 ]]
}

main "$@"

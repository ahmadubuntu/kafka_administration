#!/usr/bin/env bash
# Shared name filters + parallel job resolution for topic / consumer-group ops.
#
# Conventions (every topic/group script and future ones):
#   --pattern REGEX     only names matching this ERE (repeatable → OR)
#   --exclude REGEX     drop matching names (repeatable → OR)
#   --include-internal  keep names starting with '_' / '__' (default: skip)
#   --jobs N|auto|ask   parallel workers (default: 8)
#
# Globals (callers may set before parse):
#   NAME_PATTERNS=()  NAME_EXCLUDES=()  INCLUDE_INTERNAL=0
#   JOBS_ARG=8        PARALLEL_JOBS
#
# shellcheck shell=bash

NAME_PATTERNS=()
NAME_EXCLUDES=()
INCLUDE_INTERNAL="${INCLUDE_INTERNAL:-0}"
JOBS_ARG="${JOBS_ARG:-8}"
PARALLEL_JOBS="${PARALLEL_JOBS:-8}"

entity_filter_add_pattern() {
  local p="$1"
  [[ -n "$p" ]] || return 0
  NAME_PATTERNS+=("$p")
}

entity_filter_add_exclude() {
  local p="$1"
  [[ -n "$p" ]] || return 0
  NAME_EXCLUDES+=("$p")
}

# Returns 0 if name should be processed.
name_matches_filter() {
  local name="$1" p
  [[ -n "$name" ]] || return 1

  if [[ "$INCLUDE_INTERNAL" != "1" ]]; then
    # Skip Kafka internal / tool topics & groups that start with underscore.
    if [[ "$name" == _* ]]; then
      return 1
    fi
  fi

  if ((${#NAME_EXCLUDES[@]} > 0)); then
    for p in "${NAME_EXCLUDES[@]}"; do
      [[ -z "$p" ]] && continue
      if [[ "$name" =~ $p ]]; then
        return 1
      fi
    done
  fi

  if ((${#NAME_PATTERNS[@]} == 0)); then
    return 0
  fi
  for p in "${NAME_PATTERNS[@]}"; do
    [[ -z "$p" ]] && continue
    if [[ "$name" =~ $p ]]; then
      return 0
    fi
  done
  return 1
}

entity_filter_summary() {
  local pats excludes
  if ((${#NAME_PATTERNS[@]} > 0)); then
    pats=$(IFS=','; echo "${NAME_PATTERNS[*]}")
  else
    pats="(all)"
  fi
  if ((${#NAME_EXCLUDES[@]} > 0)); then
    excludes=$(IFS=','; echo "${NAME_EXCLUDES[*]}")
  else
    excludes="(none)"
  fi
  if declare -F emit >/dev/null 2>&1; then
    emit "Name filter: pattern=${pats}  exclude=${excludes}  include_internal=${INCLUDE_INTERNAL}"
    emit "Parallel jobs: ${PARALLEL_JOBS:-8} (from --jobs ${JOBS_ARG})"
  else
    echo "Name filter: pattern=${pats}  exclude=${excludes}  include_internal=${INCLUDE_INTERNAL}"
    echo "Parallel jobs: ${PARALLEL_JOBS:-8} (from --jobs ${JOBS_ARG})"
  fi
}

# Suggest worker count from item cardinality.
_suggest_parallel_jobs() {
  local n="${1:-0}" suggested=8
  if (( n <= 0 )); then
    suggested=8
  elif (( n < 20 )); then
    suggested=$(( n < 4 ? n : 4 ))
    (( suggested < 1 )) && suggested=1
  elif (( n < 50 )); then
    suggested=8
  elif (( n < 200 )); then
    suggested=16
  elif (( n < 500 )); then
    suggested=24
  else
    suggested=32
  fi
  (( suggested > 48 )) && suggested=48
  printf '%s' "$suggested"
}

# resolve_parallel_jobs [n_items]
# Sets PARALLEL_JOBS from JOBS_ARG (default 8). Respects NONINTERACTIVE for ask→auto.
resolve_parallel_jobs() {
  local n_items="${1:-0}"
  local suggested ans
  suggested="$(_suggest_parallel_jobs "$n_items")"

  case "${JOBS_ARG}" in
    ""|auto)
      PARALLEL_JOBS="$suggested"
      ;;
    ask)
      if [[ "${NONINTERACTIVE:-0}" == "1" ]]; then
        PARALLEL_JOBS="$suggested"
      else
        if declare -F emit >/dev/null 2>&1; then
          emit "Suggested parallel workers for ${n_items} item(s): ${suggested} (default policy 8–32, max 48)."
        fi
        read -r -p "Parallel jobs [${suggested}]: " ans || true
        if [[ -z "$ans" ]]; then
          PARALLEL_JOBS="$suggested"
        elif [[ "$ans" =~ ^[0-9]+$ ]] && (( ans >= 1 && ans <= 64 )); then
          PARALLEL_JOBS="$ans"
        else
          if declare -F loge >/dev/null 2>&1; then
            loge "Invalid jobs value: ${ans}"; exit 2
          fi
          echo "Invalid jobs value: ${ans}" >&2; exit 2
        fi
      fi
      ;;
    *)
      if [[ "$JOBS_ARG" =~ ^[0-9]+$ ]] && (( JOBS_ARG >= 1 && JOBS_ARG <= 64 )); then
        PARALLEL_JOBS="$JOBS_ARG"
      else
        if declare -F loge >/dev/null 2>&1; then
          loge "--jobs must be ask, auto, or integer 1–64"; exit 2
        fi
        echo "--jobs must be ask, auto, or integer 1–64" >&2; exit 2
      fi
      ;;
  esac
  export PARALLEL_JOBS
  if declare -F emit >/dev/null 2>&1; then
    emit "Using PARALLEL_JOBS=${PARALLEL_JOBS}"
  fi
}

# Parse shared filter/jobs flags; leftover in ENTITY_FILTER_REMAINING_ARGS.
# Usage: entity_filter_parse_args "$@"; set -- "${ENTITY_FILTER_REMAINING_ARGS[@]}"
entity_filter_parse_args() {
  ENTITY_FILTER_REMAINING_ARGS=()
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --pattern|--include|--topic-pattern|--group-pattern)
        entity_filter_add_pattern "$2"; shift 2 ;;
      --exclude|--exclude-pattern)
        entity_filter_add_exclude "$2"; shift 2 ;;
      --include-internal) INCLUDE_INTERNAL=1; shift ;;
      --jobs) JOBS_ARG="$2"; shift 2 ;;
      --) shift; ENTITY_FILTER_REMAINING_ARGS+=("$@"); break ;;
      *) ENTITY_FILTER_REMAINING_ARGS+=("$1"); shift ;;
    esac
  done
}

entity_filter_help_lines() {
  cat <<'EOF'
  --pattern REGEX       Only names matching ERE (repeatable; OR). Applies to topics/groups.
  --exclude REGEX       Drop matching names (repeatable; OR)
  --include-internal    Include names starting with '_' (default: skip)
  --jobs N|auto|ask     Parallel workers (default: 8)
EOF
}

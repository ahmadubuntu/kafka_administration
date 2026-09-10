#!/usr/bin/env bash
# Local Kafka CLI (protocol) helpers for the MM host.
# shellcheck shell=bash

VIA="${VIA:-kafka}"

kafka_cli() {
  # kafka_cli ENVFILE tool [args...]
  local envfile="$1"; shift
  local tool="$1"; shift
  local bin bootstrap conf timeout_sec
  # shellcheck disable=SC1090
  bin="$(bash -c 'set -a; source "$1"; set +a; printf "%s" "${KAFKA_BIN:-/var/opt/kafka/bin}"' _ "$envfile")"
  bootstrap="$(bash -c 'set -a; source "$1"; set +a; printf "%s" "${KAFKA_BOOTSTRAP:-}"' _ "$envfile")"
  conf="$(bash -c 'set -a; source "$1"; set +a; printf "%s" "${KAFKA_COMMAND_CONFIG:-}"' _ "$envfile")"
  timeout_sec="${KAFKA_ADMIN_TIMEOUT_SEC:-180}"
  if [[ "$VIA" == "ssh" ]]; then
    echo "SSH_STUB: --via ssh not implemented in v0.1 (need BROKER_HOSTS + SSH from MM host)" >&2
    return 3
  fi
  if [[ ! -x "${bin}/${tool}" ]]; then
    echo "MISSING_TOOL ${bin}/${tool}" >&2
    return 127
  fi
  if [[ -z "$bootstrap" ]]; then
    echo "NO_BOOTSTRAP in ${envfile}" >&2
    return 2
  fi
  local args=(--bootstrap-server "$bootstrap")
  if [[ -n "$conf" && -f "$conf" ]]; then
    args+=(--command-config "$conf")
  elif [[ -n "$conf" ]]; then
    echo "WARN command-config missing on this host: ${conf}" >&2
  fi
  timeout "$timeout_sec" "${bin}/${tool}" "${args[@]}" "$@"
}

# kafka-log-dirs.sh needs --broker-list. Kafka 3.9 has no --json; default is JSON
# (plus "Querying brokers..." lines). Older builds print text; mm2_parse handles both.
kafka_log_dirs_dump() {
  local envfile="$1"
  local outfile="${2:-}"
  local api ids
  api="$(kafka_cli "$envfile" kafka-broker-api-versions.sh 2>/dev/null || true)"
  ids="$(printf '%s\n' "$api" | python3 "${MM_LIB}/mm2_parse.py" broker-ids)"
  if [[ -z "$ids" ]]; then
    echo "WARN kafka-log-dirs: no broker ids from api-versions" >&2
    printf '%s\n' "$api" >&2
    return 1
  fi
  local extra=(--describe --broker-list "$ids")
  if [[ -n "$outfile" ]]; then
    KAFKA_ADMIN_TIMEOUT_SEC="${KAFKA_LOGDIRS_TIMEOUT_SEC:-300}" \
      kafka_cli "$envfile" kafka-log-dirs.sh "${extra[@]}" >"$outfile"
  else
    KAFKA_ADMIN_TIMEOUT_SEC="${KAFKA_LOGDIRS_TIMEOUT_SEC:-300}" \
      kafka_cli "$envfile" kafka-log-dirs.sh "${extra[@]}"
  fi
}

kafka_log_dirs_json() {
  kafka_log_dirs_dump "$1"
}

# --all is missing on some kafka-configs builds (same class of CLI drift as --json).
kafka_topic_configs_dump() {
  local envfile="$1"
  local outfile="$2"
  local err
  err="$(mktemp)"
  if kafka_cli "$envfile" kafka-configs.sh --entity-type topics --describe --all \
    >"$outfile" 2>"$err"; then
    rm -f "$err"
    return 0
  fi
  if grep -qiE 'unrecognized option|all is not a recognized' "$err"; then
    echo "INFO kafka-configs: no --all; using --describe only" >&2
    if kafka_cli "$envfile" kafka-configs.sh --entity-type topics --describe \
      >"$outfile" 2>"$err"; then
      rm -f "$err"
      return 0
    fi
  fi
  cat "$err" >&2
  rm -f "$err"
  return 1
}

mm_resolve_cfg() {
  local root="${1:-.}" f="$2"
  [[ -f "$f" ]] && { printf '%s' "$f"; return 0; }
  [[ -f "${root}/${f}" ]] && { printf '%s' "${root}/${f}"; return 0; }
  [[ -f "${root}/config/clusters/${f}" ]] && { printf '%s' "${root}/config/clusters/${f}"; return 0; }
  [[ -f "${root}/config/clusters/${f}.env" ]] && { printf '%s' "${root}/config/clusters/${f}.env"; return 0; }
  return 1
}

# Sets SOURCE_ENV DEST_ENV from CONFIG_FILES[2] and ROOT_DIR.
mm_assign_roles() {
  local root="${1:-.}"
  local f role
  SOURCE_ENV=""; DEST_ENV=""
  if ((${#CONFIG_FILES[@]} != 2)); then
    echo "Need exactly two -c inventories (source then dest)" >&2
    return 2
  fi
  local resolved=()
  for f in "${CONFIG_FILES[@]}"; do
    resolved+=("$(mm_resolve_cfg "$root" "$f" || { echo "Config not found: $f" >&2; return 2; })")
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
    echo "ROLE not set in env files - treating first -c as source, second as dest" >&2
  fi
}

bytes_to_gib() {
  python3 -c "print('{:.2f}'.format(int('${1:-0}')/1073741824))" 2>/dev/null || echo "0.00"
}

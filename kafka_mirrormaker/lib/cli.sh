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

bytes_to_gib() {
  python3 -c "print('{:.2f}'.format(int('${1:-0}')/1073741824))" 2>/dev/null || echo "0.00"
}

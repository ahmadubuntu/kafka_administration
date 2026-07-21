#!/usr/bin/env bash
# Shared helpers for Kafka administration scripts.
# shellcheck disable=SC2034

set -euo pipefail

_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_ROOT_DIR="$(cd "$_LIB_DIR/.." && pwd)"

# Load env.sh if present (never commit secrets)
if [[ -f "$_ROOT_DIR/env.sh" ]]; then
  # shellcheck disable=SC1091
  source "$_ROOT_DIR/env.sh"
elif [[ -f "$_ROOT_DIR/env.example" ]]; then
  # shellcheck disable=SC1091
  source "$_ROOT_DIR/env.example"
fi

export KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
export KAFKA_BIN="${KAFKA_BIN:-$KAFKA_HOME/bin}"
export KAFKA_SERVER_PROPERTIES="${KAFKA_SERVER_PROPERTIES:-/var/opt/kafka/config/server.properties}"
export KAFKA_COMMAND_CONFIG="${KAFKA_COMMAND_CONFIG:-/opt/kafka/config/admin.properties}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
export KAFKA_BOOTSTRAP_LOCAL="${KAFKA_BOOTSTRAP_LOCAL:-localhost:9092}"
export KAFKA_LOG_DIR="${KAFKA_LOG_DIR:-/var/opt/kafka/logs}"
export KAFKA_SYSTEMD_UNIT="${KAFKA_SYSTEMD_UNIT:-kafka}"
export KAFKA_JMX_METRICS_URL="${KAFKA_JMX_METRICS_URL:-http://127.0.0.1:7071/metrics}"
export KAFKA_JOLOKIA_URL="${KAFKA_JOLOKIA_URL:-http://127.0.0.1:8779/jolokia}"
export REPORT_DIR="${REPORT_DIR:-$_ROOT_DIR/reports}"

mkdir -p "$REPORT_DIR"

section() {
  echo
  echo "============================================================"
  echo " $*"
  echo "============================================================"
}

subsection() {
  echo
  echo "--- $* ---"
}

have() {
  command -v "$1" >/dev/null 2>&1
}

require_cmd() {
  local c
  for c in "$@"; do
    if ! have "$c"; then
      echo "ERROR: required command not found: $c" >&2
      return 1
    fi
  done
}

kafka_tool() {
  local tool="$1"
  shift
  local path="$KAFKA_BIN/$tool"
  if [[ ! -x "$path" ]]; then
    echo "ERROR: Kafka tool not found or not executable: $path" >&2
    return 1
  fi
  "$path" "$@"
}

# Admin CLI against configured bootstrap (+ optional command-config)
kafka_admin() {
  local tool="$1"
  shift
  local args=("--bootstrap-server" "$KAFKA_BOOTSTRAP")
  if [[ -f "$KAFKA_COMMAND_CONFIG" ]]; then
    args+=("--command-config" "$KAFKA_COMMAND_CONFIG")
  fi
  kafka_tool "$tool" "${args[@]}" "$@"
}

# Same but force local plaintext bootstrap (no command-config)
kafka_admin_local() {
  local tool="$1"
  shift
  kafka_tool "$tool" --bootstrap-server "$KAFKA_BOOTSTRAP_LOCAL" "$@"
}

redact_props() {
  # Redact secrets from a properties stream
  sed -E 's/(password|secret|key|sasl\.jaas\.config)=.*/\1=***/I'
}

http_ok() {
  local url="$1"
  curl -sf -m 5 -o /dev/null "$url"
}

save_report() {
  local name="$1"
  local dest="$REPORT_DIR/${name}.txt"
  cat >"$dest"
  echo "(saved: $dest)" >&2
}

timestamp() {
  date -Iseconds
}

print_env_summary() {
  section "Environment"
  cat <<EOF
time                 : $(timestamp)
host                 : $(hostname)
KAFKA_HOME           : $KAFKA_HOME
KAFKA_BIN            : $KAFKA_BIN
KAFKA_SERVER_PROPERTIES: $KAFKA_SERVER_PROPERTIES
KAFKA_COMMAND_CONFIG : $KAFKA_COMMAND_CONFIG
KAFKA_BOOTSTRAP      : $KAFKA_BOOTSTRAP
KAFKA_BOOTSTRAP_LOCAL: $KAFKA_BOOTSTRAP_LOCAL
KAFKA_LOG_DIR        : $KAFKA_LOG_DIR
KAFKA_SYSTEMD_UNIT   : $KAFKA_SYSTEMD_UNIT
KAFKA_JMX_METRICS_URL: $KAFKA_JMX_METRICS_URL
KAFKA_JOLOKIA_URL    : $KAFKA_JOLOKIA_URL
REPORT_DIR           : $REPORT_DIR
EOF
}

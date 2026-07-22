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

# ---- formatting (TTY + color; disable with NO_COLOR=1) ----
_use_color() {
  [[ -z "${NO_COLOR:-}" ]] || return 1
  [[ "${KAFKA_ADMIN_COLOR:-}" != "0" ]] || return 1
  [[ -t 1 ]] || return 1
  return 0
}

if _use_color; then
  C_RESET=$'\033[0m'
  C_BOLD=$'\033[1m'
  C_DIM=$'\033[2m'
  C_RED=$'\033[31m'
  C_GREEN=$'\033[32m'
  C_YELLOW=$'\033[33m'
  C_BLUE=$'\033[34m'
  C_MAGENTA=$'\033[35m'
  C_CYAN=$'\033[36m'
  C_GRAY=$'\033[90m'
else
  C_RESET=""; C_BOLD=""; C_DIM=""; C_RED=""; C_GREEN=""
  C_YELLOW=""; C_BLUE=""; C_MAGENTA=""; C_CYAN=""; C_GRAY=""
fi

_rule() {
  # _rule 70 ─
  local n="$1" ch="${2:-─}" line
  printf -v line '%*s' "$n" ''
  echo "${line// /$ch}"
}

section() {
  local title="$*"
  local width=72
  local inner
  echo
  echo "${C_CYAN}╭$(_rule $((width - 2)))╮${C_RESET}"
  printf -v inner " %s" "$title"
  printf "${C_CYAN}│${C_RESET}${C_BOLD}%-$((width - 2))s${C_RESET}${C_CYAN}│${C_RESET}\n" "$inner"
  echo "${C_CYAN}╰$(_rule $((width - 2)))╯${C_RESET}"
}

subsection() {
  echo
  echo "${C_BLUE}▸${C_RESET} ${C_BOLD}$*${C_RESET}"
  echo "${C_DIM}  ························································${C_RESET}"
}

kv() {
  # kv "key" "value"
  local key="$1"
  local value="$2"
  printf "  ${C_DIM}%-22s${C_RESET} %s\n" "$key" "$value"
}

badge() {
  local level
  level="$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]')"
  case "$level" in
    OK) echo -n "${C_GREEN}${C_BOLD}[OK]${C_RESET}" ;;
    WATCH|WARN|TIGHT) echo -n "${C_YELLOW}${C_BOLD}[${level}]${C_RESET}" ;;
    CRITICAL|ERROR|FAIL) echo -n "${C_RED}${C_BOLD}[${level}]${C_RESET}" ;;
    *) echo -n "${C_GRAY}${C_BOLD}[${level}]${C_RESET}" ;;
  esac
}

status_line() {
  # status_line OK "message"
  local level="$1"; shift
  echo "  $(badge "$level") $*"
}

info()  { echo "  ${C_BLUE}ℹ${C_RESET} ${C_DIM}$*${C_RESET}"; }
ok()    { status_line OK "$*"; }
warn()  { status_line WARN "$*"; }
err()   { status_line ERROR "$*"; }
bullet(){ echo "  ${C_CYAN}•${C_RESET} $*"; }

step_banner() {
  # step_banner 3 11 "03_cluster_health.sh"
  local idx="$1" total="$2" name="$3"
  echo
  echo "${C_MAGENTA}┏━━${C_RESET} ${C_BOLD}Step ${idx}/${total}${C_RESET}  ${C_DIM}${name}${C_RESET}"
  echo "${C_MAGENTA}┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${C_RESET}"
}

have() {
  command -v "$1" >/dev/null 2>&1
}

require_cmd() {
  local c
  for c in "$@"; do
    if ! have "$c"; then
      err "required command not found: $c"
      return 1
    fi
  done
}

kafka_tool() {
  local tool="$1"
  shift
  local path="$KAFKA_BIN/$tool"
  if [[ ! -x "$path" ]]; then
    err "Kafka tool not found or not executable: $path"
    return 1
  fi
  "$path" "$@"
}

kafka_admin() {
  local tool="$1"
  shift
  local args=("--bootstrap-server" "$KAFKA_BOOTSTRAP")
  if [[ -f "$KAFKA_COMMAND_CONFIG" ]]; then
    args+=("--command-config" "$KAFKA_COMMAND_CONFIG")
  fi
  kafka_tool "$tool" "${args[@]}" "$@"
}

kafka_admin_local() {
  local tool="$1"
  shift
  kafka_tool "$tool" --bootstrap-server "$KAFKA_BOOTSTRAP_LOCAL" "$@"
}

redact_props() {
  sed -E 's/(password|secret|key|sasl\.jaas\.config)=.*/\1=***/I'
}

http_ok() {
  local url="$1"
  curl -sf -m 5 -o /dev/null "$url"
}

timestamp() {
  date -Iseconds
}

print_env_summary() {
  section "Environment"
  kv "time" "$(timestamp)"
  kv "host" "$(hostname)"
  kv "KAFKA_HOME" "$KAFKA_HOME"
  kv "KAFKA_BIN" "$KAFKA_BIN"
  kv "server.properties" "$KAFKA_SERVER_PROPERTIES"
  kv "command-config" "$KAFKA_COMMAND_CONFIG"
  kv "bootstrap" "$KAFKA_BOOTSTRAP"
  kv "bootstrap.local" "$KAFKA_BOOTSTRAP_LOCAL"
  kv "log.dirs" "$KAFKA_LOG_DIR"
  kv "systemd unit" "$KAFKA_SYSTEMD_UNIT"
  kv "JMX metrics" "$KAFKA_JMX_METRICS_URL"
  kv "Jolokia" "$KAFKA_JOLOKIA_URL"
  kv "REPORT_DIR" "$REPORT_DIR"
}

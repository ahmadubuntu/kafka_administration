#!/usr/bin/env bash
# Kafka cascade: VIP/LB → broker client listener → (optional) controller.
# shellcheck shell=bash

_kafka_classify() {
  # stdout: auth|timeout|refused|tls|other
  local msg="$1"
  # SASL/SCRAM listeners often just close the TCP session — kcat says "Disconnected" / transport failure,
  # not always the word SASL. Match those before the word "timeout" (kcat help text mentions "metadata timeout").
  if echo "$msg" | grep -qiE 'SASL|Authentication|not authorized|Unauthorized|Login failed|Invalid credentials|Disconnected|connection closed by peer|Broker transport failure'; then
    echo auth
  elif echo "$msg" | grep -qiE 'timed out|DeadlineExceeded|(^|[^a-zA-Z])Timeout([^a-zA-Z]|$)'; then
    echo timeout
  elif echo "$msg" | grep -qiE 'Connection refused|No route to host|Network is unreachable'; then
    echo refused
  elif echo "$msg" | grep -qiE 'SSL|TLS|handshake_failure|PKIX'; then
    echo tls
  else
    echo other
  fi
}

_kafka_admin_probe() {
  # Probe bootstrap with kafka-broker-api-versions.sh (or metadata via python/kcat if available)
  local bootstrap="$1"
  local conf="${KAFKA_COMMAND_CONFIG:-}"
  local bin="${KAFKA_BIN:-/opt/kafka/bin}"
  local out rc=0
  local host="${bootstrap%%:*}" port="${bootstrap##*:}"

  if [[ -x "${bin}/kafka-broker-api-versions.sh" && ( -z "$conf" || -f "$conf" ) ]]; then
    local args=(--bootstrap-server "$bootstrap")
    [[ -f "$conf" ]] && args+=(--command-config "$conf")
    out="$(timeout "${KAFKA_PROBE_TIMEOUT_SEC:-15}" "${bin}/kafka-broker-api-versions.sh" "${args[@]}" 2>&1 | head -40)" || rc=$?
    echo "$out"
    return "$rc"
  fi

  if have_cmd kcat || have_cmd kafkacat; then
    local kcat_bin
    kcat_bin="$(command -v kcat || command -v kafkacat)"
    out="$(timeout "${KAFKA_PROBE_TIMEOUT_SEC:-15}" "$kcat_bin" -b "$bootstrap" -L 2>&1 | head -40)" || rc=$?
    echo "$out"
    # kcat often exits 0 even when metadata failed — require a real metadata header
    if ! echo "$out" | grep -qE '^Metadata for |^ [0-9]+ brokers:'; then
      return 1
    fi
    return "$rc"
  fi

  # TCP-only fallback (no local Kafka CLI / command-config on checker)
  if tcp_check "$host" "$port"; then
    echo "TCP_OK ${bootstrap} (no local kafka CLI — protocol not verified)"
    return 0
  fi
  echo "TCP_FAIL ${bootstrap}"
  return 1
}

_cascade_report_failure() {
  local layer="$1" target="$2" msg="$3"
  local cls
  cls="$(_kafka_classify "$msg")"
  case "$cls" in
    auth)
      record_check "cascade" "${layer}|${target}" "WARN" \
        "auth failed from checker — admin checks use SSH + KAFKA_COMMAND_CONFIG=${KAFKA_COMMAND_CONFIG:-unset} on a broker (SASL listener not verified locally)" \
        "$msg"
      ;;
    timeout)
      record_check "cascade" "${layer}|${target}" "FAIL" "timeout talking to ${target}" "$msg"
      ;;
    refused)
      record_check "cascade" "${layer}|${target}" "FAIL" "connection refused on ${target}" "$msg"
      ;;
    tls)
      record_check "cascade" "${layer}|${target}" "WARN" "TLS/SSL error on ${target} — check security.protocol / truststore" "$msg"
      ;;
    *)
      record_check "cascade" "${layer}|${target}" "FAIL" "probe failed on ${target}" "$msg"
      ;;
  esac
}

_cascade_tcp_miss() {
  local layer="$1" target="$2" port="$3"
  if is_cluster_internal_port "$port" || ! is_checker_reachable_port "$port"; then
    record_check "cascade" "${layer}|${target}" "INFO" \
      "TCP closed from checker (expected if internal / not in CHECKER_REACHABLE_PORTS)"
  else
    record_check "cascade" "${layer}|${target}" "FAIL" "TCP closed"
  fi
}

# TCP open + SASL from checker, but node-local admin config exists → usable via SSH.
_cascade_accept_ssh_admin_path() {
  local layer="$1" target="$2"
  [[ -n "${KAFKA_COMMAND_CONFIG:-}" ]] || return 1
  record_check "cascade" "${layer}|${target}" "WARN" \
    "TCP open; SASL not verified from checker — admin path via SSH + ${KAFKA_COMMAND_CONFIG}"
  KAFKA_CONNECT_OK=1
  KAFKA_CONNECT_BOOTSTRAP="${KAFKA_BOOTSTRAP:-$target}"
  _CASCADE_DEPTH="$layer"
  return 0
}

run_kafka_cascade() {
  section "Client cascade (VIP → LB → broker)"
  KAFKA_CONNECT_OK=0
  KAFKA_CONNECT_BOOTSTRAP=""
  _CASCADE_DEPTH=""
  local out rc cls

  # Layer 1: VIP
  if [[ -n "${VIP_HOST:-}" ]]; then
    local vp="${VIP_CLIENT_PORTS:-${KAFKA_CLIENT_PORT:-9092}}"
    local p
    for p in $(echo "$vp" | tr ',' ' '); do
      [[ -z "$p" ]] && continue
      local target="${VIP_HOST}:${p}"
      if ! tcp_check "${VIP_HOST}" "$p"; then
        _cascade_tcp_miss "vip" "$target" "$p"
        continue
      fi
      rc=0
      out="$(_kafka_admin_probe "$target")" || rc=$?
      if [[ $rc -eq 0 ]]; then
        if echo "$out" | grep -q 'TCP_OK'; then
          record_check "cascade" "vip|${target}" "WARN" "TCP OK but no local kafka CLI — protocol/SASL not verified; install kafka bin or set readable KAFKA_COMMAND_CONFIG"
        else
          record_check "cascade" "vip|${target}" "PASS" "client path via VIP OK"
        fi
        KAFKA_CONNECT_OK=1
        KAFKA_CONNECT_BOOTSTRAP="$target"
        _CASCADE_DEPTH="vip"
        break
      else
        _cascade_report_failure "vip" "$target" "$out"
        cls="$(_kafka_classify "$out")"
        if [[ "$cls" == "auth" ]] && _cascade_accept_ssh_admin_path "vip" "$target"; then
          break
        fi
      fi
    done
  else
    record_check "cascade" "vip" "INFO" "VIP_HOST not set — skipping VIP layer"
  fi

  # Layer 2: LB hosts
  if [[ "$KAFKA_CONNECT_OK" != "1" && -n "${LB_HOSTS:-}" ]]; then
    local lbs=() lp="${LB_CLIENT_PORTS:-${KAFKA_CLIENT_PORT:-9092}}"
    csv_to_array lbs "${LB_HOSTS}"
    local h p
    for h in "${lbs[@]}"; do
      for p in $(echo "$lp" | tr ',' ' '); do
        [[ -z "$p" ]] && continue
        local target="${h}:${p}"
        if ! tcp_check "$h" "$p"; then
          _cascade_tcp_miss "lb" "$target" "$p"
          continue
        fi
        rc=0
        out="$(_kafka_admin_probe "$target")" || rc=$?
        if [[ $rc -eq 0 ]]; then
          if echo "$out" | grep -q 'TCP_OK'; then
            record_check "cascade" "lb|${target}" "WARN" "TCP OK; protocol/SASL not verified (no local kafka CLI)"
          else
            record_check "cascade" "lb|${target}" "PASS" "client path via LB OK"
          fi
          KAFKA_CONNECT_OK=1
          KAFKA_CONNECT_BOOTSTRAP="$target"
          _CASCADE_DEPTH="lb"
          break 2
        else
          _cascade_report_failure "lb" "$target" "$out"
          cls="$(_kafka_classify "$out")"
          if [[ "$cls" == "auth" ]] && _cascade_accept_ssh_admin_path "lb" "$target"; then
            break 2
          fi
        fi
      done
    done
  fi

  # Layer 3: direct brokers — EXTERNAL first (often the only checker-reachable port)
  if [[ "$KAFKA_CONNECT_OK" != "1" ]]; then
    local brokers=()
    csv_to_array brokers "${BROKER_HOSTS:-}"
    local ports="${KAFKA_EXTERNAL_PORT:-},${KAFKA_CLIENT_PORT:-9092}"
    local h p
    for h in "${brokers[@]}"; do
      for p in $(echo "$ports" | tr ',' ' '); do
        [[ -z "$p" ]] && continue
        local target="${h}:${p}"
        if ! tcp_check "$h" "$p"; then
          _cascade_tcp_miss "broker" "$target" "$p"
          continue
        fi
        rc=0
        out="$(_kafka_admin_probe "$target")" || rc=$?
        if [[ $rc -eq 0 ]]; then
          if echo "$out" | grep -q 'TCP_OK'; then
            record_check "cascade" "broker|${target}" "WARN" "TCP OK; protocol/SASL not verified (no local kafka CLI)"
          else
            record_check "cascade" "broker|${target}" "PASS" "direct broker path OK"
          fi
          KAFKA_CONNECT_OK=1
          KAFKA_CONNECT_BOOTSTRAP="$target"
          _CASCADE_DEPTH="broker"
          break 2
        else
          _cascade_report_failure "broker" "$target" "$out"
          cls="$(_kafka_classify "$out")"
          if [[ "$cls" == "auth" ]] && _cascade_accept_ssh_admin_path "broker" "$target"; then
            break 2
          fi
        fi
      done
    done
  fi

  if [[ "$KAFKA_CONNECT_OK" == "1" ]]; then
    case "${_CASCADE_DEPTH}" in
      vip) record_check "cascade" "path" "PASS" "using VIP bootstrap ${KAFKA_CONNECT_BOOTSTRAP}" ;;
      lb) record_check "cascade" "path" "WARN" "VIP down/missing — using LB ${KAFKA_CONNECT_BOOTSTRAP}" ;;
      broker)
        if [[ -z "${VIP_HOST:-}" && -z "${LB_HOSTS:-}" ]]; then
          record_check "cascade" "path" "PASS" \
            "direct broker path (no VIP/LB in inventory); bootstrap=${KAFKA_CONNECT_BOOTSTRAP}"
        else
          record_check "cascade" "path" "FAIL" \
            "only deep broker path works — VIP/LB unavailable; bootstrap=${KAFKA_CONNECT_BOOTSTRAP}"
        fi
        ;;
    esac
    if [[ -n "${KAFKA_BOOTSTRAP:-}" ]]; then
      KAFKA_CONNECT_BOOTSTRAP="$KAFKA_BOOTSTRAP"
      local conf="${KAFKA_COMMAND_CONFIG:-}"
      if [[ -n "$conf" && ! -f "$conf" ]]; then
        record_check "cascade" "bootstrap" "INFO" \
          "later checks use KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP} with node-local KAFKA_COMMAND_CONFIG=${conf} (via SSH)"
      else
        record_check "cascade" "bootstrap" "INFO" "later checks use KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP}"
      fi
    fi
  else
    record_check "cascade" "path" "FAIL" "no client path worked (VIP/LB/broker)"
  fi
  export KAFKA_CONNECT_OK KAFKA_CONNECT_BOOTSTRAP
}

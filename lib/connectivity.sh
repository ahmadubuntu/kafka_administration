#!/usr/bin/env bash
# Kafka cascade: VIP/LB → broker client listener → (optional) controller.
# shellcheck shell=bash

_kafka_classify() {
  # stdout: auth|timeout|refused|tls|other
  local msg="$1"
  if echo "$msg" | grep -qiE 'SASL|Authentication|not authorized|Unauthorized|Login failed|Invalid credentials'; then
    echo auth
  elif echo "$msg" | grep -qiE 'timed out|Timeout|DeadlineExceeded'; then
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
        "auth failed — set KAFKA_COMMAND_CONFIG (admin.properties) or fix SASL; not marking cluster FAIL" \
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

run_kafka_cascade() {
  section "Client cascade (VIP → LB → broker)"
  KAFKA_CONNECT_OK=0
  KAFKA_CONNECT_BOOTSTRAP=""
  local depth_ok="" out rc

  # Layer 1: VIP
  if [[ -n "${VIP_HOST:-}" ]]; then
    local vp="${VIP_CLIENT_PORTS:-${KAFKA_CLIENT_PORT:-9092}}"
    local p
    for p in $(echo "$vp" | tr ',' ' '); do
      [[ -z "$p" ]] && continue
      local target="${VIP_HOST}:${p}"
      if ! tcp_check "${VIP_HOST}" "$p"; then
        record_check "cascade" "vip|${target}" "FAIL" "TCP closed"
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
        depth_ok="vip"
        break
      else
        _cascade_report_failure "vip" "$target" "$out"
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
          record_check "cascade" "lb|${target}" "FAIL" "TCP closed"
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
          depth_ok="lb"
          break 2
        else
          _cascade_report_failure "lb" "$target" "$out"
        fi
      done
    done
  fi

  # Layer 3: direct brokers (advertised / native)
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
          record_check "cascade" "broker|${target}" "FAIL" "TCP closed"
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
          depth_ok="broker"
          break 2
        else
          _cascade_report_failure "broker" "$target" "$out"
        fi
      done
    done
  fi

  if [[ "$KAFKA_CONNECT_OK" == "1" ]]; then
    case "$depth_ok" in
      vip) record_check "cascade" "path" "PASS" "using VIP bootstrap ${KAFKA_CONNECT_BOOTSTRAP}" ;;
      lb) record_check "cascade" "path" "WARN" "VIP down/missing — using LB ${KAFKA_CONNECT_BOOTSTRAP}" ;;
      broker) record_check "cascade" "path" "FAIL" "only deep broker path works — VIP/LB unavailable; bootstrap=${KAFKA_CONNECT_BOOTSTRAP}" ;;
    esac
    # Prefer inventory bootstrap if set (may include multiple brokers)
    if [[ -n "${KAFKA_BOOTSTRAP:-}" ]]; then
      KAFKA_CONNECT_BOOTSTRAP="$KAFKA_BOOTSTRAP"
      record_check "cascade" "bootstrap" "INFO" "later checks use KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP}"
    fi
  else
    record_check "cascade" "path" "FAIL" "no client path worked (VIP/LB/broker)"
  fi
  export KAFKA_CONNECT_OK KAFKA_CONNECT_BOOTSTRAP
}

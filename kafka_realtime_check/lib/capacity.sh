#!/usr/bin/env bash
# Kafka capacity: FD / connections / network+request handler idle / narrowest layer.
# shellcheck shell=bash

_check_kafka_fd_limits() {
  local host="$1"
  local unit="${KAFKA_SYSTEMD_UNIT:-kafka}"
  local out soft hard open=""
  out="$(remote_run "$host" 1 "systemctl show ${unit} -p LimitNOFILE --value 2>/dev/null; MAIN=\$(systemctl show -p MainPID --value ${unit}); if [[ -n \$MAIN && \$MAIN != 0 ]]; then awk '/Max open files/{print \$4,\$5}' /proc/\$MAIN/limits; ls /proc/\$MAIN/fd 2>/dev/null | wc -l; fi" 2>&1)" || true
  local limit
  limit="$(echo "$out" | head -1 | tr -d '\r')"
  soft="$(echo "$out" | awk 'NR==2{print $1}')"
  hard="$(echo "$out" | awk 'NR==2{print $2}')"
  open="$(echo "$out" | awk 'NR==3{print $1}')"
  [[ "$limit" =~ ^[0-9]+$ ]] || limit="${hard:-${soft:-0}}"
  [[ "$open" =~ ^[0-9]+$ ]] || open=0
  if [[ "${limit:-0}" -le 0 ]]; then
    record_check "capacity" "fd:${host}" "SKIP" "LimitNOFILE unavailable for ${unit}"
    return
  fi
  local pct=$(( open * 100 / limit ))
  local detail="open=${open} LimitNOFILE=${limit} (ulimit soft/hard ${soft:-?}/${hard:-?}) — unit=${unit}; raise via /etc/systemd/system/${unit}.service.d/override.conf then systemctl daemon-reload && systemctl restart ${unit}"
  if (( pct >= ${CONN_FAIL_PCT:-90} )); then
    record_check "capacity" "fd:${host}" "FAIL" "${pct}% FD used — ${detail}"
  elif (( pct >= ${CONN_WARN_PCT:-70} )); then
    record_check "capacity" "fd:${host}" "WARN" "${pct}% FD used — ${detail}"
  else
    record_check "capacity" "fd:${host}" "PASS" "${pct}% FD used (open=${open}/${limit})"
  fi
}

_check_kafka_connections() {
  local host="$1"
  local ports="${KAFKA_EXTERNAL_PORT:-9094},${KAFKA_CLIENT_PORT:-9092}"
  local p count=0
  for p in $(echo "$ports" | tr ',' ' '); do
    [[ -z "$p" ]] && continue
    local c
    c="$(remote_run "$host" 0 "ss -tn state established \"( sport = :${p} )\" 2>/dev/null | tail -n +2 | wc -l" 2>&1)" || c=0
    [[ "$c" =~ ^[0-9]+$ ]] || c=0
    count=$((count + c))
    record_check "capacity" "conn:${host}:${p}" "INFO" "established=${c}"
  done
  local ceil="${CONN_SOFT_CEILING:-${KAFKA_CONN_CAUTION:-5000}}"
  local pct=0
  if [[ "$ceil" -gt 0 ]]; then
    pct=$(( count * 100 / ceil ))
  fi
  local net_threads
  net_threads="$(remote_run "$host" 0 "grep -E '^num.network.threads=' ${KAFKA_SERVER_PROPERTIES:-/var/opt/kafka/config/server.properties} 2>/dev/null | cut -d= -f2" 2>&1)" || true
  net_threads="${net_threads:-3}"
  local msg="connections≈${count} vs ceiling ${ceil} (~${pct}%); num.network.threads=${net_threads} in ${KAFKA_SERVER_PROPERTIES:-server.properties} — raise threads / fix idle clients; restart required for thread change"
  if (( pct >= ${CONN_FAIL_PCT:-90} )); then
    record_check "capacity" "connections:${host}" "FAIL" "$msg"
  elif (( pct >= ${CONN_WARN_PCT:-70} )); then
    record_check "capacity" "connections:${host}" "WARN" "$msg"
  else
    record_check "capacity" "connections:${host}" "PASS" "$msg"
  fi
}

_check_kafka_handler_idle() {
  local host="$1"
  local raw idle
  raw="$(remote_run "$host" 0 'curl -sf -m 5 http://127.0.0.1:8779/jolokia/read/kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent' 2>&1)" || true
  idle="$(echo "$raw" | sed -n 's/.*"Value"[[:space:]]*:[[:space:]]*\([0-9.]*\).*/\1/p' | head -1)"
  if [[ -n "$idle" ]]; then
    # Value is 0..1 idle fraction (sometimes already percent-like)
    local busy_pct
    busy_pct="$(awk -v i="$idle" 'BEGIN{ if(i>1) i=i/100; printf "%d", (1-i)*100 }')"
    if (( busy_pct >= ${HANDLER_BUSY_FAIL_PCT:-90} )); then
      record_check "capacity" "network-idle:${host}" "FAIL" "network processors ~${busy_pct}% busy (idle=${idle}) — raise num.network.threads"
    elif (( busy_pct >= ${HANDLER_BUSY_WARN_PCT:-70} )); then
      record_check "capacity" "network-idle:${host}" "WARN" "network processors ~${busy_pct}% busy (idle=${idle})"
    else
      record_check "capacity" "network-idle:${host}" "PASS" "network processors busy≈${busy_pct}% (idle=${idle})"
    fi
  else
    record_check "capacity" "network-idle:${host}" "SKIP" "Jolokia NetworkProcessorAvgIdlePercent unavailable"
  fi

  raw="$(remote_run "$host" 0 'curl -sf -m 5 "http://127.0.0.1:8779/jolokia/read/kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent"' 2>&1)" || true
  # This metric is a Meter; OneMinuteRate near 1 means mostly idle (Kafka quirk)
  local rate
  rate="$(echo "$raw" | sed -n 's/.*"OneMinuteRate"[[:space:]]*:[[:space:]]*\([0-9.eE+-]*\).*/\1/p' | head -1)"
  if [[ -n "$rate" ]]; then
    record_check "capacity" "request-handler-idle:${host}" "INFO" "RequestHandlerAvgIdlePercent OneMinuteRate=${rate} (≈1+ means mostly idle on many builds)"
  fi
}

check_kafka_capacity() {
  section "Capacity (FD / connections / handler idle)"
  local brokers=() h
  csv_to_array brokers "${BROKER_HOSTS:-}"
  if [[ ${#brokers[@]} -eq 0 ]]; then
    record_check "capacity" "brokers" "SKIP" "BROKER_HOSTS empty"
    return
  fi
  for h in "${brokers[@]}"; do
    if ! ssh_host_is_ok "$h"; then
      record_check "capacity" "${h}" "SKIP" "SSH unavailable"
      continue
    fi
    _check_kafka_fd_limits "$h"
    _check_kafka_connections "$h"
    _check_kafka_handler_idle "$h"
  done

  record_check "capacity" "narrowest" "INFO" \
    "Typical narrowest layers: num.network.threads (live connections) → LimitNOFILE → disk on log.dirs → heap. Tune file: ${KAFKA_SERVER_PROPERTIES:-/var/opt/kafka/config/server.properties}; systemd override for LimitNOFILE."
}

check_vip_owner() {
  # Optional keepalived VIP in front of Kafka LB
  [[ -n "${VIP_HOST:-}" ]] || return 0
  section "VIP ownership"
  local holders=() hosts=() h
  csv_to_array hosts "${LB_HOSTS:-${BROKER_HOSTS:-}}"
  for h in "${hosts[@]}"; do
    ssh_host_is_ok "$h" || continue
    if remote_run "$h" 0 "ip -4 addr show | grep -q '[ /]${VIP_HOST}/'" 2>/dev/null; then
      holders+=("$h")
    fi
  done
  case "${#holders[@]}" in
    0) record_check "vip" "owner" "FAIL" "no node holds VIP ${VIP_HOST}" ;;
    1) record_check "vip" "owner" "PASS" "VIP ${VIP_HOST} held by ${holders[0]}" ;;
    *) record_check "vip" "owner" "FAIL" "split-brain: VIP on ${holders[*]}" ;;
  esac
}

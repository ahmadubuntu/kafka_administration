#!/usr/bin/env bash
# Kafka stack core: service, membership, controller, URP/offline, ISR/imbalance, lag, config drift.
# shellcheck shell=bash

_kafka_tool() {
  local tool="$1"; shift
  local bin="${KAFKA_BIN:-/opt/kafka/bin}"
  local bootstrap="${KAFKA_CONNECT_BOOTSTRAP:-${KAFKA_BOOTSTRAP:-}}"
  local conf="${KAFKA_COMMAND_CONFIG:-}"
  if [[ ! -x "${bin}/${tool}" ]]; then
    echo "MISSING_TOOL ${bin}/${tool}"
    return 127
  fi
  if [[ -z "$bootstrap" ]]; then
    echo "NO_BOOTSTRAP"
    return 2
  fi
  local args=(--bootstrap-server "$bootstrap")
  [[ -f "$conf" ]] && args+=(--command-config "$conf")
  timeout "${KAFKA_ADMIN_TIMEOUT_SEC:-60}" "${bin}/${tool}" "${args[@]}" "$@"
}

_kafka_remote_tool() {
  # Run kafka CLI on a broker host via SSH when checker lacks binaries / command-config
  local host="$1"; shift
  local tool="$1"; shift
  local bootstrap="${KAFKA_CONNECT_BOOTSTRAP:-${KAFKA_BOOTSTRAP:-}}"
  local conf="${KAFKA_COMMAND_CONFIG:-/opt/kafka/config/kraft/admin-user.properties}"
  local bin="${KAFKA_BIN:-/opt/kafka/bin}"
  local remote_cmd
  remote_cmd="BOOT='${bootstrap}'; BIN='${bin}'; CONF='${conf}'; "
  remote_cmd+="ARGS=(--bootstrap-server \"\$BOOT\"); [[ -f \"\$CONF\" ]] && ARGS+=(--command-config \"\$CONF\"); "
  remote_cmd+="timeout ${KAFKA_ADMIN_TIMEOUT_SEC:-60} \"\$BIN/${tool}\" \"\${ARGS[@]}\" $*"
  remote_run "$host" 0 "bash -lc $(printf '%q' "$remote_cmd")"
}

_kafka_run() {
  local tool="$1"; shift
  local conf="${KAFKA_COMMAND_CONFIG:-}"
  local bin="${KAFKA_BIN:-/opt/kafka/bin}"
  # If command-config is set but only exists on cluster nodes, force SSH remote CLI.
  local force_remote=0
  if [[ -n "$conf" && ! -f "$conf" ]]; then
    force_remote=1
  fi
  if [[ "$force_remote" -eq 0 && -x "${bin}/${tool}" ]]; then
    _kafka_tool "$tool" "$@"
    return $?
  fi
  # Fallback: first SSH-OK broker (uses node-local conf path when present there)
  local brokers=() h
  csv_to_array brokers "${BROKER_HOSTS:-}"
  for h in "${brokers[@]}"; do
    if ssh_host_is_ok "$h"; then
      _kafka_remote_tool "$h" "$tool" "$@"
      return $?
    fi
  done
  echo "NO_LOCAL_BIN_AND_NO_SSH"
  return 127
}

check_kafka_services() {
  section "Kafka systemd units"
  local hosts=() unit="${KAFKA_SYSTEMD_UNIT:-kafka}" h seen="|"
  local role_hosts arr
  for role_hosts in "${BROKER_HOSTS:-}" "${CONTROLLER_HOSTS:-}"; do
    arr=()
    csv_to_array arr "$role_hosts"
    for h in "${arr[@]}"; do
      [[ -z "$h" || "$seen" == *"|${h}|"* ]] && continue
      seen+="${h}|"
      hosts+=("$h")
    done
  done
  for h in "${hosts[@]}"; do
    if ! ssh_host_is_ok "$h"; then
      record_check "service" "kafka:${h}" "SKIP" "SSH unavailable"
      continue
    fi
    check_systemd_active "$h" "$unit" "kafka"
  done
}

check_kafka_membership() {
  section "Broker membership vs inventory"
  if [[ "${KAFKA_CONNECT_OK:-0}" != "1" && -z "${KAFKA_BOOTSTRAP:-}" ]]; then
    record_check "membership" "brokers" "SKIP" "no working bootstrap"
    return
  fi
  local out rc=0
  out="$(_kafka_run kafka-broker-api-versions.sh 2>&1)" || rc=$?
  if [[ $rc -ne 0 ]]; then
    record_check "membership" "api-versions" "FAIL" "cannot list brokers (rc=${rc})" "$out"
    return
  fi
  # Lines look like: host:port (id: N rack: ... )
  local live_ids=()
  while read -r line; do
    if [[ "$line" =~ \(id:\ *([0-9]+) ]]; then
      live_ids+=("${BASH_REMATCH[1]}")
    fi
  done <<<"$out"

  local inv=()
  csv_to_array inv "${BROKER_HOSTS:-}"
  local inv_n=${#inv[@]} live_n=${#live_ids[@]}
  if [[ $live_n -eq 0 ]]; then
    # Some kafka versions print differently — count unique host:port headers
    live_n="$(echo "$out" | grep -cE '^[a-zA-Z0-9._-]+:[0-9]+ ' || true)"
  fi
  record_check "membership" "inventory-count" "INFO" "inventory brokers=${inv_n}"
  if [[ "$live_n" -eq "$inv_n" ]]; then
    record_check "membership" "broker-count" "PASS" "live=${live_n} matches inventory=${inv_n}"
  elif [[ "$live_n" -lt "$inv_n" ]]; then
    record_check "membership" "broker-count" "FAIL" "live=${live_n} < inventory=${inv_n} — missing brokers"
  else
    record_check "membership" "broker-count" "WARN" "live=${live_n} > inventory=${inv_n} — inventory incomplete?"
  fi
}

check_kafka_controller() {
  section "Controller / KRaft quorum"
  if [[ "${KAFKA_MODE:-kraft}" == "zk" ]]; then
    record_check "controller" "mode" "INFO" "KAFKA_MODE=zk — use ZK ensemble checks separately (not implemented here)"
    return
  fi

  local out rc=0
  # Prefer metadata-quorum on a broker via SSH (needs local binaries there)
  local brokers=() h
  csv_to_array brokers "${BROKER_HOSTS:-}"
  for h in "${brokers[@]}"; do
    ssh_host_is_ok "$h" || continue
    out="$(remote_run "$h" 0 "bash -lc 'BIN=${KAFKA_BIN:-/opt/kafka/bin}; CONF=${KAFKA_COMMAND_CONFIG:-/opt/kafka/config/admin.properties}; BS=${KAFKA_CONNECT_BOOTSTRAP:-${KAFKA_BOOTSTRAP:-localhost:9092}}; if [[ -x \$BIN/kafka-metadata-quorum.sh ]]; then timeout 30 \$BIN/kafka-metadata-quorum.sh --bootstrap-server \$BS \${CONF:+--command-config \$CONF} describe --status 2>&1; else echo NO_QUORUM_TOOL; fi'" 2>&1)" || rc=$?
    break
  done

  if [[ -z "${out:-}" || "$out" == *NO_QUORUM_TOOL* ]]; then
    # Fallback: Jolokia/JMX active controller count on first broker
    for h in "${brokers[@]}"; do
      ssh_host_is_ok "$h" || continue
      local jmx="${KAFKA_JOLOKIA_URL:-http://127.0.0.1:8779/jolokia}"
      # Rewrite localhost jolokia to remote via SSH curl
      out="$(remote_run "$h" 0 'curl -sf -m 5 http://127.0.0.1:8779/jolokia/read/kafka.controller:type=KafkaController,name=ActiveControllerCount' 2>&1)" || true
      if echo "$out" | grep -q '"Value"[[:space:]]*:[[:space:]]*1'; then
        record_check "controller" "active" "PASS" "ActiveControllerCount=1 on ${h}"
        return
      elif echo "$out" | grep -q '"Value"'; then
        record_check "controller" "active" "FAIL" "unexpected ActiveControllerCount on ${h}" "$out"
        return
      fi
    done
    record_check "controller" "quorum" "SKIP" "kafka-metadata-quorum.sh / Jolokia unavailable"
    return
  fi

  if echo "$out" | grep -qiE 'LeaderId|CurrentVoters|ClusterId'; then
    record_check "controller" "quorum-status" "PASS" "metadata quorum describe OK"
    logv "$out"
  else
    record_check "controller" "quorum-status" "WARN" "unexpected quorum output" "$out"
  fi

  # Exactly one active controller via metric if possible
  if echo "$out" | grep -qiE 'LeaderId[[:space:]]*[:=][[:space:]]*[0-9]+'; then
    record_check "controller" "leader" "PASS" "quorum has a leader"
  fi
}

check_kafka_partition_health() {
  section "Partition health (URP / offline / preferred imbalance)"
  if [[ "${KAFKA_CONNECT_OK:-0}" != "1" && -z "${KAFKA_BOOTSTRAP:-}" ]]; then
    record_check "partitions" "health" "SKIP" "no bootstrap"
    return
  fi

  local urp_out offline_out rc=0
  urp_out="$(_kafka_run kafka-topics.sh --describe --under-replicated-partitions 2>&1)" || rc=$?
  if [[ $rc -ne 0 ]] || echo "$urp_out" | grep -qE 'MISSING_TOOL|NO_LOCAL_BIN|NO_BOOTSTRAP|Error while executing|Timed out'; then
    record_check "partitions" "urp" "SKIP" "cannot query URP (rc=${rc}) — need kafka-topics.sh on checker or SSH to broker + working bootstrap" "$urp_out"
  else
    local urp_n
    urp_n="$(echo "$urp_out" | grep -c 'Partition:' || true)"
    if [[ "${urp_n:-0}" -eq 0 ]]; then
      record_check "partitions" "urp" "PASS" "under-replicated partitions = 0"
    elif [[ "${urp_n}" -le "${URP_WARN_COUNT:-5}" ]]; then
      record_check "partitions" "urp" "WARN" "under-replicated=${urp_n} (≤ warn ${URP_WARN_COUNT:-5}) — check ISR / disk / broker load" "$urp_out"
    else
      record_check "partitions" "urp" "FAIL" "under-replicated=${urp_n} — growing/stuck ISR risk" "$urp_out"
    fi
  fi

  rc=0
  offline_out="$(_kafka_run kafka-topics.sh --describe --unavailable-partitions 2>&1)" || rc=$?
  if [[ $rc -ne 0 ]] || echo "$offline_out" | grep -qE 'MISSING_TOOL|NO_LOCAL_BIN|NO_BOOTSTRAP|Error while executing|Timed out'; then
    record_check "partitions" "offline" "SKIP" "cannot query offline partitions (rc=${rc})" "$offline_out"
  else
    local off_n
    off_n="$(echo "$offline_out" | grep -c 'Partition:' || true)"
    if [[ "${off_n:-0}" -eq 0 ]]; then
      record_check "partitions" "offline" "PASS" "unavailable/offline partitions = 0"
    else
      record_check "partitions" "offline" "FAIL" "unavailable partitions=${off_n}" "$offline_out"
    fi
  fi

  # Preferred replica imbalance via Jolokia on a broker
  local brokers=() h
  csv_to_array brokers "${BROKER_HOSTS:-}"
  for h in "${brokers[@]}"; do
    ssh_host_is_ok "$h" || continue
    local raw
    raw="$(remote_run "$h" 0 'curl -sf -m 5 http://127.0.0.1:8779/jolokia/read/kafka.controller:type=KafkaController,name=PreferredReplicaImbalanceCount' 2>&1)" || true
    if echo "$raw" | grep -q '"Value"'; then
      local imb
      imb="$(echo "$raw" | sed -n 's/.*"Value"[[:space:]]*:[[:space:]]*\([0-9.]*\).*/\1/p' | head -1)"
      imb="${imb%%.*}"
      if [[ "${imb:-0}" -eq 0 ]]; then
        record_check "partitions" "preferred-imbalance" "PASS" "PreferredReplicaImbalanceCount=0"
      elif [[ "${imb}" -le "${PREF_IMBALANCE_WARN:-50}" ]]; then
        record_check "partitions" "preferred-imbalance" "WARN" "imbalance=${imb} — consider preferred replica election"
      else
        record_check "partitions" "preferred-imbalance" "WARN" "imbalance=${imb} high — leadership skewed"
      fi
      break
    fi
  done
}

check_kafka_consumer_lag() {
  section "Consumer group lag"
  if [[ "${SKIP_LAG:-0}" == "1" ]]; then
    record_check "lag" "groups" "SKIP" "--skip-lag set"
    return
  fi
  if [[ "${KAFKA_CONNECT_OK:-0}" != "1" && -z "${KAFKA_BOOTSTRAP:-}" ]]; then
    record_check "lag" "groups" "SKIP" "no bootstrap"
    return
  fi

  local groups_out rc=0
  groups_out="$(_kafka_run kafka-consumer-groups.sh --list 2>&1)" || rc=$?
  if [[ $rc -ne 0 ]]; then
    record_check "lag" "list" "WARN" "cannot list groups (rc=${rc})" "$groups_out"
    return
  fi
  local gcount
  gcount="$(echo "$groups_out" | grep -cve '^$' || true)"
  record_check "lag" "group-count" "INFO" "consumer groups=${gcount}"

  local desc rc2=0
  # --describe --all-groups can be heavy; allow timeout
  desc="$(_kafka_run kafka-consumer-groups.sh --describe --all-groups 2>&1)" || rc2=$?
  if [[ $rc2 -ne 0 ]]; then
    record_check "lag" "describe" "WARN" "describe --all-groups failed/timeout (rc=${rc2}) — try raising KAFKA_ADMIN_TIMEOUT_SEC" "$desc"
    return
  fi

  # Parse LAG column (kafka-consumer-groups table). Sum numeric lags; ignore '-'.
  local max_lag=0 sum_lag=0 bad=0
  while read -r lag; do
    [[ "$lag" =~ ^[0-9]+$ ]] || continue
    sum_lag=$((sum_lag + lag))
    (( lag > max_lag )) && max_lag=$lag
    if (( lag >= ${LAG_FAIL:-100000} )); then
      bad=$((bad + 1))
    fi
  done < <(echo "$desc" | awk 'NR>1 {print $(NF-1)}' 2>/dev/null; echo "$desc" | awk 'toupper($0) ~ /LAG/ {next} {for(i=1;i<=NF;i++) if($i+0==$i && $i!~/\./) print $i}' 2>/dev/null | head -5000)

  # More reliable: look for "LAG" header and take that column
  max_lag=0; sum_lag=0; bad=0
  local lag_col=0
  while IFS= read -r line; do
    if echo "$line" | grep -qiE '[[:space:]]LAG[[:space:]]'; then
      # find LAG field index
      lag_col="$(echo "$line" | awk '{for(i=1;i<=NF;i++) if(toupper($i)=="LAG") print i; exit}')"
      continue
    fi
    [[ "${lag_col:-0}" -gt 0 ]] || continue
    local v
    v="$(echo "$line" | awk -v c="$lag_col" '{print $c}')"
    [[ "$v" =~ ^[0-9]+$ ]] || continue
    sum_lag=$((sum_lag + v))
    (( v > max_lag )) && max_lag=$v
    if (( v >= ${LAG_FAIL:-100000} )); then
      bad=$((bad + 1))
    fi
  done <<<"$desc"

  record_check "lag" "max" "INFO" "max_lag=${max_lag} sum_lag=${sum_lag}"
  if (( max_lag >= ${LAG_FAIL:-100000} )); then
    record_check "lag" "threshold" "FAIL" "max lag ${max_lag} ≥ LAG_FAIL=${LAG_FAIL:-100000} (${bad} partition-rows)"
  elif (( max_lag >= ${LAG_WARN:-10000} )); then
    record_check "lag" "threshold" "WARN" "max lag ${max_lag} ≥ LAG_WARN=${LAG_WARN:-10000}"
  else
    record_check "lag" "threshold" "PASS" "max lag ${max_lag} within thresholds"
  fi
}

check_kafka_config_drift() {
  section "Config drift (EXPECT_*)"
  local brokers=() h
  csv_to_array brokers "${BROKER_HOSTS:-}"
  local props="${KAFKA_SERVER_PROPERTIES:-/var/opt/kafka/config/server.properties}"

  for h in "${brokers[@]}"; do
    ssh_host_is_ok "$h" || continue
    local live
    live="$(remote_run "$h" 0 "grep -E '^(min\\.insync\\.replicas|unclean\\.leader\\.election\\.enable|listener\\.security\\.protocol\\.map)=' ${props} 2>/dev/null || true" 2>&1)" || true

    if [[ -n "${EXPECT_MIN_INSYNC_REPLICAS:-}" ]]; then
      local got
      got="$(echo "$live" | sed -n 's/^min\.insync\.replicas=//p' | head -1)"
      # default if unset is often 1
      got="${got:-1}"
      if [[ "$got" == "$EXPECT_MIN_INSYNC_REPLICAS" ]]; then
        record_check "config" "min.insync.replicas:${h}" "PASS" "live=${got}"
      else
        record_check "config" "min.insync.replicas:${h}" "WARN" "live=${got} expected=${EXPECT_MIN_INSYNC_REPLICAS} — edit ${props} and rolling restart"
      fi
    else
      record_check "config" "min.insync.replicas" "INFO" "EXPECT_MIN_INSYNC_REPLICAS unset — skip drift check"
    fi

    if [[ -n "${EXPECT_UNCLEAN_LEADER_ELECTION:-}" ]]; then
      local got
      got="$(echo "$live" | sed -n 's/^unclean\.leader\.election\.enable=//p' | head -1)"
      got="${got:-false}"
      if [[ "${got,,}" == "${EXPECT_UNCLEAN_LEADER_ELECTION,,}" ]]; then
        record_check "config" "unclean.leader.election:${h}" "PASS" "live=${got}"
      else
        record_check "config" "unclean.leader.election:${h}" "WARN" "live=${got} expected=${EXPECT_UNCLEAN_LEADER_ELECTION}"
      fi
    fi

    if [[ -n "${EXPECT_TLS:-}" ]]; then
      if echo "$live" | grep -qiE 'SSL|SASL_SSL'; then
        if [[ "${EXPECT_TLS}" == "1" ]]; then
          record_check "config" "tls:${h}" "PASS" "TLS/SASL_SSL present in listener map"
        else
          record_check "config" "tls:${h}" "WARN" "TLS present but EXPECT_TLS=0"
        fi
      else
        if [[ "${EXPECT_TLS}" == "1" ]]; then
          record_check "config" "tls:${h}" "WARN" "EXPECT_TLS=1 but no SSL in listener.security.protocol.map"
        else
          record_check "config" "tls:${h}" "PASS" "no TLS as expected"
        fi
      fi
    fi
    break  # one broker is enough for cluster-default configs; per-broker overrides rare
  done
}

check_kafka_disk_logdirs() {
  section "log.dirs disk"
  local brokers=() h
  csv_to_array brokers "${BROKER_HOSTS:-}"
  local logdir="${KAFKA_LOG_DIR:-/var/opt/kafka/logs}"
  for h in "${brokers[@]}"; do
    ssh_host_is_ok "$h" || continue
    local dfout pct
    dfout="$(remote_run "$h" 0 "df -P ${logdir} 2>/dev/null | awk 'NR==2{print \$5}'" 2>&1)" || true
    pct="${dfout%%%*}"
    if [[ ! "$pct" =~ ^[0-9]+$ ]]; then
      record_check "disk" "log.dirs:${h}" "SKIP" "could not read df for ${logdir}"
      continue
    fi
    if (( pct >= ${DISK_FAIL_PCT:-90} )); then
      record_check "disk" "log.dirs:${h}" "FAIL" "${logdir} ${pct}% full — retention fighting disk; free space or shrink retention"
    elif (( pct >= ${DISK_WARN_PCT:-80} )); then
      record_check "disk" "log.dirs:${h}" "WARN" "${logdir} ${pct}% used"
    else
      record_check "disk" "log.dirs:${h}" "PASS" "${logdir} ${pct}% used"
    fi
  done
}

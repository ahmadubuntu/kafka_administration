#!/usr/bin/env bash
# Port matrix: local / listen / cross — only inventory ports; cluster-internal local miss = INFO.

remote_is_listening() {
  local host="$1" port="$2"
  local out
  out=$(remote_run "$host" 0 "ss -lntu 2>/dev/null | awk '{print \$5}' | grep -E '[:.]${port}\$' | head -1" 2>/dev/null) || true
  [[ -n "${out// }" ]] && return 0
  out=$(remote_run "$host" 1 "ss -lntu 2>/dev/null | awk '{print \$5}' | grep -E '[:.]${port}\$' | head -1" 2>/dev/null) || true
  [[ -n "${out// }" ]]
}

remote_tcp_from() {
  # Runs ON src over SSH (reuses ControlMaster if open), then probes dest:port with nc/tcp.
  # Does NOT reuse a previous "health-check SSH login session" as a long-lived shell —
  # each call is a new remote command over the multiplexed SSH master.
  local src="$1" dest="$2" port="$3"
  local to="${TCP_TIMEOUT_SEC:-3}"
  local out rc=0
  out=$(remote_run "$src" 0 "if command -v nc >/dev/null 2>&1; then nc -z -w ${to} ${dest} ${port}; else timeout ${to} bash -c 'echo >/dev/tcp/${dest}/${port}'; fi; echo RC=\$?" 2>&1) || rc=$?
  if printf '%s' "$out" | grep -q 'RC=0'; then
    return 0
  fi
  return 1
}

check_port_triple() {
  local role="$1" host="$2" port="$3"
  local local_ok=0 listen_ok=0
  [[ -n "$port" && "$port" =~ ^[0-9]+$ ]] || return 0

  if tcp_check "$host" "$port"; then
    local_ok=1
    record_check "port-local" "${role}|${host}|${port}" "PASS" "reachable from checker"
  else
    if is_cluster_internal_port "$port" || ! is_checker_reachable_port "$port"; then
      record_check "port-local" "${role}|${host}|${port}" "INFO" "not reachable from checker (cluster-internal / not in CHECKER_REACHABLE_PORTS)"
    else
      record_check "port-local" "${role}|${host}|${port}" "FAIL" "not reachable from checker"
    fi
  fi

  if ! ssh_host_is_ok "$host"; then
    record_check "port-listen" "${role}|${host}|${port}" "SKIP" "SSH unavailable — listen not checked"
  elif remote_is_listening "$host" "$port"; then
    listen_ok=1
    record_check "port-listen" "${role}|${host}|${port}" "PASS" "listening on host"
  else
    record_check "port-listen" "${role}|${host}|${port}" "FAIL" "not listening on host"
  fi

  if [[ $local_ok -eq 1 && $listen_ok -eq 0 ]] && ssh_host_is_ok "$host"; then
    record_check "port-consistency" "${role}|${host}|${port}" "WARN" "reachable locally but not listening on host"
  elif [[ $local_ok -eq 0 && $listen_ok -eq 1 ]]; then
    if is_cluster_internal_port "$port" || ! is_checker_reachable_port "$port"; then
      record_check "port-consistency" "${role}|${host}|${port}" "INFO" "listening on host; blocked from checker (expected if internal)"
    else
      record_check "port-consistency" "${role}|${host}|${port}" "WARN" "listening on host but not reachable from checker"
    fi
  fi
}

_check_ports_one_host() {
  # args via globals: _CP_ROLE _CP_PORTS_CSV  host as $1
  local host="$1" ports=() p
  csv_to_array ports "${_CP_PORTS_CSV}"
  for p in "${ports[@]}"; do
    check_port_triple "${_CP_ROLE}" "$host" "$p"
  done
}

check_ports_for_role() {
  local role="$1" hosts_csv="$2" ports_csv="$3"
  local hosts=() ports=() uniq=() p seen="|"
  csv_to_array hosts "$hosts_csv"
  csv_to_array ports "$ports_csv"
  for p in "${ports[@]}"; do
    [[ -z "$p" || ! "$p" =~ ^[0-9]+$ ]] && continue
    [[ "$seen" == *"|${p}|"* ]] && continue
    seen+="${p}|"
    uniq+=("$p")
  done
  (( ${#uniq[@]} > 0 )) || return 0
  _CP_ROLE="$role"
  _CP_PORTS_CSV="$(join_by ',' "${uniq[@]}")"
  run_parallel_fn _check_ports_one_host "${hosts[@]}"
}

check_cross_mesh() {
  local service="$1" hosts_csv="$2" ports_csv="$3"
  local hosts=() ports=() src dst p
  csv_to_array hosts "$hosts_csv"
  csv_to_array ports "$ports_csv"
  [[ ${#ports[@]} -eq 0 ]] && return
  if [[ ${#hosts[@]} -lt 2 ]]; then
    record_check "port-cross" "${service}" "SKIP" "need >=2 hosts for mesh"
    return
  fi

  emit "  ${C_DIM}mesh ${service}: $(join_by ' ↔ ' "${hosts[@]}") ports=$(join_by , "${ports[@]}")${C_RESET}"

  _cross_one_src() {
    local src="$1" dst p
    if ! ssh_host_is_ok "$src"; then
      record_check "port-cross" "${service}|from:${src}" "SKIP" "SSH down — mesh from this node skipped"
      return
    fi
    for dst in "${hosts[@]}"; do
      [[ "$src" == "$dst" ]] && continue
      for p in "${ports[@]}"; do
        if remote_tcp_from "$src" "$dst" "$p"; then
          record_check "port-cross" "${service}|${src}→${dst}|${p}" "PASS" "peer path open"
        else
          record_check "port-cross" "${service}|${src}→${dst}|${p}" "FAIL" "peer path closed/filtered"
        fi
      done
    done
  }

  run_parallel_fn _cross_one_src "${hosts[@]}"
}

run_port_matrix() {
  section "Port matrix (local / listen / cross)"
  emit "  ${C_DIM}local=from checker; listen=ss on host; cross=peer mesh via SSH${C_RESET}"

  local brokers=() controllers=() lb=()
  csv_to_array brokers "${BROKER_HOSTS:-}"
  csv_to_array controllers "${CONTROLLER_HOSTS:-${BROKER_HOSTS:-}}"
  csv_to_array lb "${LB_HOSTS:-}"

  # Brokers: client + inter-broker + optional admin
  if [[ ${#brokers[@]} -gt 0 ]]; then
    check_ports_for_role "broker" "${BROKER_HOSTS}" \
      "${KAFKA_CLIENT_PORTS:-${KAFKA_CLIENT_PORT:-9092}},${KAFKA_INTER_BROKER_PORT:-},${KAFKA_EXTERNAL_PORT:-},${KAFKA_ADMIN_PORTS:-}"
  fi

  # Controllers (may overlap brokers in combined KRaft nodes)
  if [[ -n "${KAFKA_CONTROLLER_PORT:-}" && ${#controllers[@]} -gt 0 ]]; then
    check_ports_for_role "controller" "${CONTROLLER_HOSTS:-${BROKER_HOSTS}}" "${KAFKA_CONTROLLER_PORT}"
  fi

  # LB / VIP client entry
  if [[ ${#lb[@]} -gt 0 ]]; then
    check_ports_for_role "lb" "${LB_HOSTS}" "${LB_CLIENT_PORTS:-${KAFKA_CLIENT_PORT:-9092}}"
  fi
  if [[ -n "${VIP_HOST:-}" ]]; then
    local p
    for p in $(echo "${VIP_CLIENT_PORTS:-${KAFKA_CLIENT_PORT:-9092}}" | tr ',' ' '); do
      [[ -z "$p" ]] && continue
      if tcp_check "$VIP_HOST" "$p"; then
        if is_checker_reachable_port "$p"; then
          record_check "port-local" "vip|${VIP_HOST}|${p}" "PASS" "reachable from checker"
        else
          record_check "port-local" "vip|${VIP_HOST}|${p}" "INFO" "reachable (not in CHECKER_REACHABLE_PORTS)"
        fi
      else
        if is_checker_reachable_port "$p"; then
          record_check "port-local" "vip|${VIP_HOST}|${p}" "FAIL" "not reachable from checker"
        elif is_cluster_internal_port "$p"; then
          record_check "port-local" "vip|${VIP_HOST}|${p}" "INFO" "not reachable from checker (cluster-internal)"
        else
          record_check "port-local" "vip|${VIP_HOST}|${p}" "WARN" "not reachable from checker"
        fi
      fi
    done
  fi

  section "Cross-node broker / controller mesh"
  local mesh_ports="${KAFKA_INTER_BROKER_PORT:-}"
  [[ -n "${KAFKA_CLIENT_PORT:-}" ]] && mesh_ports="${mesh_ports:+$mesh_ports,}${KAFKA_CLIENT_PORT}"
  [[ -n "${KAFKA_EXTERNAL_PORT:-}" ]] && mesh_ports="${mesh_ports:+$mesh_ports,}${KAFKA_EXTERNAL_PORT}"
  mesh_ports="${mesh_ports#,}"
  if [[ -n "$mesh_ports" && ${#brokers[@]} -gt 1 ]]; then
    check_cross_mesh "broker" "${BROKER_HOSTS}" "$mesh_ports"
  elif [[ ${#brokers[@]} -le 1 ]]; then
    record_check "port-cross" "broker-mesh" "INFO" "single broker — inter-broker mesh N/A"
  fi
  if [[ -n "${KAFKA_CONTROLLER_PORT:-}" && ${#controllers[@]} -gt 1 ]]; then
    check_cross_mesh "controller" "${CONTROLLER_HOSTS:-${BROKER_HOSTS}}" "${KAFKA_CONTROLLER_PORT}"
  fi

  if [[ ${#lb[@]} -gt 0 && ${#brokers[@]} -gt 0 ]]; then
    section "Cross path LB → broker client ports"
    local client_ports=()
    csv_to_array client_ports "${LB_BACKEND_PORTS:-${KAFKA_CLIENT_PORT:-9092},${KAFKA_EXTERNAL_PORT:-}}"
    _lb_to_broker() {
      local h="$1" b port
      if ! ssh_host_is_ok "$h"; then
        record_check "port-cross" "lb→broker|from:${h}" "SKIP" "SSH down on LB — cannot probe from it"
        return
      fi
      for b in "${brokers[@]}"; do
        for port in "${client_ports[@]}"; do
          [[ -z "$port" ]] && continue
          if remote_tcp_from "$h" "$b" "$port"; then
            record_check "port-cross" "lb→broker|${h}→${b}|${port}" "PASS" "from ${h}: ${b}:${port} OK"
          else
            record_check "port-cross" "lb→broker|${h}→${b}|${port}" "FAIL" "from ${h}: ${b}:${port} failed"
          fi
        done
      done
    }
    run_parallel_fn _lb_to_broker "${lb[@]}"
  fi
}

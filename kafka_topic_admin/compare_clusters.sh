#!/usr/bin/env bash
# Compare Kafka clusters on the probes used for kafkio/CLI latency investigation.
#
# Pass one or more inventories with repeated -c, or --clusters-dir + --clusters.
# Select probes with --only / --skip / --list-tasks (shared tasks.sh).
#
# Usage:
#   ./compare_clusters.sh -c ../kafka_realtime_check/config/clusters/devkafka.env -u USER
#   ./compare_clusters.sh -c …/devkafka.env -c …/stgkafka.env -c …/dmzkafka.env -u USER \
#     --only tcp,counts,broker_cli,local_cli --local-bin ~/Softs/kafka/kafka_2.13-3.9.0/bin
#   ./compare_clusters.sh --clusters-dir ../kafka_realtime_check/config/clusters \
#     --clusters devkafka,stgkafka -u USER --only counts,idle
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_VERSION="$(cat "${ROOT_DIR}/VERSION" 2>/dev/null | tr -d '[:space:]' || echo 0.0.0)"
LIB_DIR="${LIB_DIR:-${ROOT_DIR}/../kafka_realtime_check/lib}"

# shellcheck source=/dev/null
source "${LIB_DIR}/common.sh"
# shellcheck source=/dev/null
source "${LIB_DIR}/ssh.sh"
# shellcheck source=/dev/null
source "${LIB_DIR}/tasks.sh"

CONFIG_FILES=()
CLUSTERS_DIR=""
CLUSTERS_CSV=""
NONINTERACTIVE=0
SSH_USER_OVERRIDE="${SSH_USER_OVERRIDE:-}"
SUDO_PASSWORD_ENV="${SUDO_PASSWORD:-${SUDO_PASSWORD_ENV:-}}"
USE_SUDO=0
VERBOSE=0
LOCAL_BIN="${LOCAL_BIN:-}"
LOCAL_RUNS="${LOCAL_RUNS:-2}"
REPORT_DIR="${ROOT_DIR}/reports"
RESULT_DIR=""
declare -a CLUSTER_SLUGS=()
declare -A CLUSTER_LABEL=()
declare -A CLUSTER_CONF=()

TASK_CATALOG=(
  "tcp||TCP connect to bootstrap hosts|connect,rtt"
  "counts||Topic / partition / group counts|inventory,size"
  "broker_cli||CLI timings on admin broker (SSH)|remote_cli,broker"
  "local_cli||CLI timings from this machine|laptop,kafkio"
  "jolokia||Broker Jolokia request / idle / connections|metrics,jmx"
  "idle||Idle topic age-bucket summary|dead,age"
)

TASKS_LIST_EXAMPLES=$(cat <<'EOF'
  --only tcp,counts
  --only broker_cli,local_cli
  --only counts,idle --skip jolokia
EOF
)

usage() {
  cat <<EOF
Usage: $(basename "$0") [cluster selectors] [options]

Cross-cluster Kafka compare v${SCRIPT_VERSION}

Cluster selectors (repeat -c, or use --clusters-dir):
  -c, --config FILE        Inventory .env (repeatable)
  --clusters-dir DIR       Directory of *.env inventories
  --clusters LIST          Names under --clusters-dir (csv; strip .env)

Probes (via shared task selection):
  --only LIST              Run only these probes
  --skip LIST              Skip these probes
  --ask-tasks              Interactive multi-select
  --list-tasks             Print probe catalog and exit

  tcp          TCP connect ms to each bootstrap host:port
  counts       topics / partitions / consumer groups
  broker_cli   kafka CLI wall times on the admin broker (SSH)
  local_cli    same ops from this host (needs --local-bin)
  jolokia      Metadata/ApiVersions means, handler idle, :9094 conns
  idle         age buckets from log.dirs (newest segment mtime)

Local CLI:
  --local-bin DIR          Kafka bin dir (default: ~/Softs/kafka/kafka_2.13-3.9.0/bin if present)
  --local-runs N           Repeats per op after warmup (default: ${LOCAL_RUNS})

Connection:
  -u, --user USER          SSH username
  -y, --yes                Non-interactive
  -v, --verbose
  -h, --help

Output: console comparison table + report under reports/
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -c|--config) CONFIG_FILES+=("$2"); shift 2 ;;
      --clusters-dir) CLUSTERS_DIR="$2"; shift 2 ;;
      --clusters) CLUSTERS_CSV="$2"; shift 2 ;;
      --only) ONLY_TASKS="$2"; shift 2 ;;
      --skip) SKIP_TASKS="$2"; shift 2 ;;
      --ask-tasks) ASK_TASKS=1; shift ;;
      --list-tasks) LIST_TASKS=1; shift ;;
      --local-bin) LOCAL_BIN="$2"; shift 2 ;;
      --local-runs) LOCAL_RUNS="$2"; shift 2 ;;
      -u|--user) SSH_USER_OVERRIDE="$2"; shift 2 ;;
      -y|--yes) NONINTERACTIVE=1; shift ;;
      -v|--verbose) VERBOSE=1; shift ;;
      -h|--help) usage; exit 0 ;;
      *) loge "Unknown argument: $1"; usage; exit 2 ;;
    esac
  done
}

_resolve_config_path() {
  local f="$1"
  if [[ -f "$f" ]]; then
    printf '%s' "$f"; return 0
  fi
  if [[ -f "${ROOT_DIR}/${f}" ]]; then
    printf '%s' "${ROOT_DIR}/${f}"; return 0
  fi
  if [[ -f "${ROOT_DIR}/../kafka_realtime_check/${f}" ]]; then
    printf '%s' "${ROOT_DIR}/../kafka_realtime_check/${f}"; return 0
  fi
  if [[ -n "$CLUSTERS_DIR" && -f "${CLUSTERS_DIR}/${f}" ]]; then
    printf '%s' "${CLUSTERS_DIR}/${f}"; return 0
  fi
  if [[ -n "$CLUSTERS_DIR" && -f "${CLUSTERS_DIR}/${f}.env" ]]; then
    printf '%s' "${CLUSTERS_DIR}/${f}.env"; return 0
  fi
  return 1
}

_collect_configs() {
  local name path
  if [[ -n "$CLUSTERS_CSV" ]]; then
    if [[ -z "$CLUSTERS_DIR" ]]; then
      CLUSTERS_DIR="${ROOT_DIR}/../kafka_realtime_check/config/clusters"
    fi
    IFS=',' read -r -a _names <<<"$CLUSTERS_CSV"
    for name in "${_names[@]}"; do
      name="${name// /}"
      [[ -z "$name" ]] && continue
      name="${name%.env}"
      path="$(_resolve_config_path "${CLUSTERS_DIR}/${name}.env")" || {
        loge "Cluster inventory not found: ${CLUSTERS_DIR}/${name}.env"; exit 2
      }
      CONFIG_FILES+=("$path")
    done
  fi
  if ((${#CONFIG_FILES[@]} == 0)); then
    loge "Provide at least one -c/--config or --clusters LIST"; usage; exit 2
  fi
  local resolved=() f r slug label
  for f in "${CONFIG_FILES[@]}"; do
    r="$(_resolve_config_path "$f")" || { loge "Config not found: $f"; exit 2; }
    resolved+=("$r")
  done
  CONFIG_FILES=("${resolved[@]}")

  CLUSTER_SLUGS=()
  for f in "${CONFIG_FILES[@]}"; do
    # shellcheck disable=SC1090
    label="$(bash -c 'set -a; source "$1"; set +a; printf "%s" "${CLUSTER_NAME:-}"' _ "$f")"
    slug="$(basename "$f" .env)"
    slug="${slug//[^A-Za-z0-9._-]/_}"
    slug="${slug##_}"; slug="${slug%%_}"
    [[ -n "$slug" ]] || slug="cluster"
    local base="$slug" n=2
    while [[ -n "${CLUSTER_CONF[$slug]:-}" ]]; do
      slug="${base}_${n}"
      n=$((n + 1))
    done
    CLUSTER_SLUGS+=("$slug")
    CLUSTER_CONF["$slug"]="$f"
    CLUSTER_LABEL["$slug"]="${label:-$slug}"
  done
}

_kv_set() {
  local file="$1" key="$2" val="$3"
  # replace or append
  if grep -q "^${key}=" "$file" 2>/dev/null; then
    local tmp
    tmp="$(mktemp)"
    awk -F= -v k="$key" -v v="$val" 'BEGIN{OFS="="} $1==k {$0=k"="v; seen=1} {print} END{if(!seen) print k"="v}' "$file" >"$tmp"
    mv "$tmp" "$file"
  else
    printf '%s=%s\n' "$key" "$val" >>"$file"
  fi
}

_kv_get() {
  local file="$1" key="$2"
  [[ -f "$file" ]] || { printf ''; return 0; }
  awk -F= -v k="$key" '$1==k {print substr($0,index($0,"=")+1); exit}' "$file"
}

_ms_now() {
  python3 -c 'import time; print(int(time.perf_counter()*1000))'
}

_first_broker() {
  local hosts=()
  csv_to_array hosts "${BROKER_HOSTS:-}"
  printf '%s' "${hosts[0]:-}"
}

_bootstrap_hostports() {
  local boot="${KAFKA_BOOTSTRAP:-}" part
  if [[ -z "$boot" ]]; then
    local h p
    h="$(_first_broker)"
    p="${KAFKA_EXTERNAL_PORT:-9094}"
    [[ -n "$h" ]] && printf '%s:%s\n' "$h" "$p"
    return 0
  fi
  IFS=',' read -r -a _parts <<<"$boot"
  for part in "${_parts[@]}"; do
    part="${part// /}"
    [[ -n "$part" ]] && printf '%s\n' "$part"
  done
}

_remote() {
  local host="$1" script="$2"
  remote_run "$host" 0 "$script"
}

_probe_tcp() {
  local slug="$1" out="$2"
  local line host port ms avg n=0 sum=0 minv=999999 maxv=0
  emit "  [${slug}] tcp …"
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    host="${line%%:*}"
    port="${line##*:}"
    ms="$(python3 - "$host" "$port" <<'PY'
import socket, sys, time
h, p = sys.argv[1], int(sys.argv[2])
samples=[]
for _ in range(5):
    s=socket.socket(); s.settimeout(5)
    t0=time.perf_counter()
    try:
        s.connect((h,p)); samples.append((time.perf_counter()-t0)*1000)
    except Exception as e:
        print(f"ERR:{e}"); sys.exit(0)
    finally:
        s.close()
if samples:
    print(f"{sum(samples)/len(samples):.1f}:{min(samples):.1f}:{max(samples):.1f}")
else:
    print("ERR")
PY
)"
    if [[ "$ms" == ERR* ]]; then
      _kv_set "$out" "tcp_${host}_${port}" "fail"
      [[ "$VERBOSE" == "1" ]] && emit "    ${host}:${port} FAIL ${ms}"
      continue
    fi
    local a mn mx
    IFS=':' read -r a mn mx <<<"$ms"
    _kv_set "$out" "tcp_${host}_${port}_avg_ms" "$a"
    n=$((n+1)); sum="$(python3 -c "print($sum+$a)")"
    [[ "$VERBOSE" == "1" ]] && emit "    ${host}:${port} avg=${a}ms"
  done < <(_bootstrap_hostports)
  if (( n > 0 )); then
    avg="$(python3 -c "print(round($sum/$n,1))")"
    _kv_set "$out" "tcp_avg_ms" "$avg"
  else
    _kv_set "$out" "tcp_avg_ms" "fail"
  fi
}

_probe_counts() {
  local slug="$1" out="$2" host bin boot conf
  host="$(_first_broker)"
  bin="${KAFKA_BIN:-/opt/kafka/bin}"
  boot="${KAFKA_BOOTSTRAP:-}"
  conf="${KAFKA_COMMAND_CONFIG:-}"
  emit "  [${slug}] counts via ${host} …"
  mark_ssh_ok "$host" 2>/dev/null || true
  local remote_cmd result
  remote_cmd=$(cat <<REMOTE
set -euo pipefail
BIN='${bin}'; BOOT='${boot}'; CONF='${conf}'
ARGS=(--bootstrap-server "\$BOOT")
[[ -n "\$CONF" && -f "\$CONF" ]] && ARGS+=(--command-config "\$CONF")
D=\$(mktemp)
"\$BIN/kafka-topics.sh" "\${ARGS[@]}" --describe >"\$D" 2>/dev/null || true
awk '/^Topic:/ && /PartitionCount:/ {
  t++
  if (match(\$0,/PartitionCount:[[:space:]]*[0-9]+/)) {
    p=substr(\$0,RSTART,RLENGTH); sub(/^PartitionCount:[[:space:]]*/,"",p); parts+=p
  }
} END { print "topics=" (t+0); print "partitions=" (parts+0) }' "\$D"
rm -f "\$D"
g=\$("\$BIN/kafka-consumer-groups.sh" "\${ARGS[@]}" --list 2>/dev/null | grep -c . || true)
echo "groups=\$g"
brokers=\$(printf '%s' '${BROKER_HOSTS:-}' | awk -F',' '{print NF}')
echo "brokers=\$brokers"
REMOTE
)
  result="$(_remote "$host" "$remote_cmd" 2>/dev/null || true)"
  local line k v
  while IFS= read -r line; do
    [[ "$line" == *=* ]] || continue
    k="${line%%=*}"; v="${line#*=}"
    case "$k" in topics|partitions|groups|brokers) _kv_set "$out" "$k" "$v" ;; esac
  done <<<"$result"
}

_probe_broker_cli() {
  local slug="$1" out="$2" host bin boot conf
  host="$(_first_broker)"
  bin="${KAFKA_BIN:-/opt/kafka/bin}"
  boot="${KAFKA_BOOTSTRAP:-}"
  conf="${KAFKA_COMMAND_CONFIG:-}"
  emit "  [${slug}] broker_cli via ${host} …"
  mark_ssh_ok "$host" 2>/dev/null || true
  local remote_cmd result
  remote_cmd=$(cat <<REMOTE
set -euo pipefail
BIN='${bin}'; BOOT='${boot}'; CONF='${conf}'
ARGS=(--bootstrap-server "\$BOOT")
[[ -n "\$CONF" && -f "\$CONF" ]] && ARGS+=(--command-config "\$CONF")
py_ms() { python3 -c 'import time; print(int(time.perf_counter()*1000))'; }
time_op() {
  local name="\$1"; shift
  local t0 t1
  t0=\$(py_ms)
  "\$@" >/dev/null 2>&1 || true
  t1=\$(py_ms)
  echo "\${name}_ms=\$((t1-t0))"
}
# warmup
"\$BIN/kafka-broker-api-versions.sh" "\${ARGS[@]}" >/dev/null 2>&1 || true
time_op api_versions "\$BIN/kafka-broker-api-versions.sh" "\${ARGS[@]}"
time_op topics_list "\$BIN/kafka-topics.sh" "\${ARGS[@]}" --list
time_op topics_describe "\$BIN/kafka-topics.sh" "\${ARGS[@]}" --describe
time_op log_dirs "\$BIN/kafka-log-dirs.sh" "\${ARGS[@]}" --describe
REMOTE
)
  result="$(_remote "$host" "$remote_cmd" 2>/dev/null || true)"
  local line k v
  while IFS= read -r line; do
    [[ "$line" == *_ms=* ]] || continue
    k="${line%%=*}"; v="${line#*=}"
    _kv_set "$out" "broker_${k}" "$v"
  done <<<"$result"
}

_probe_local_cli() {
  local slug="$1" out="$2"
  if [[ -z "$LOCAL_BIN" || ! -x "${LOCAL_BIN}/kafka-topics.sh" ]]; then
    emit "  [${slug}] local_cli SKIP (set --local-bin)"
    _kv_set "$out" "local_cli" "skip"
    return 0
  fi
  local host boot conf remote_conf tmpprops
  host="$(_first_broker)"
  boot="${KAFKA_BOOTSTRAP:-${host}:${KAFKA_EXTERNAL_PORT:-9094}}"
  conf="${KAFKA_COMMAND_CONFIG:-}"
  emit "  [${slug}] local_cli via ${LOCAL_BIN} …"
  mark_ssh_ok "$host" 2>/dev/null || true
  tmpprops="$(mktemp "${RESULT_DIR}/local-${slug}.XXXXXX.properties")"
  chmod 600 "$tmpprops"
  if [[ -n "$conf" ]]; then
    if ! scp -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
      -o ConnectTimeout="${SSH_TIMEOUT_SEC:-12}" -P "${SSH_PORT:-22}" \
      "${SSH_USER}@${host}:${conf}" "$tmpprops" 2>/dev/null; then
      emit "  [${slug}] local_cli SKIP (cannot scp ${conf})"
      _kv_set "$out" "local_cli" "skip_scp"
      rm -f "$tmpprops"
      return 0
    fi
  else
    emit "  [${slug}] local_cli SKIP (no KAFKA_COMMAND_CONFIG)"
    _kv_set "$out" "local_cli" "skip_noconf"
    rm -f "$tmpprops"
    return 0
  fi
  # Force bootstrap from inventory
  if grep -q '^bootstrap.servers=' "$tmpprops"; then
    sed -i "s|^bootstrap.servers=.*|bootstrap.servers=${boot}|" "$tmpprops"
  else
    printf 'bootstrap.servers=%s\n' "$boot" >>"$tmpprops"
  fi

  local runs="${LOCAL_RUNS}" op ms avg
  python3 - "$LOCAL_BIN" "$tmpprops" "$boot" "$runs" "$out" <<'PY'
import subprocess, sys, time, pathlib
bin_dir, conf, boot, runs, out = sys.argv[1:6]
runs = int(runs)
ops = [
  ("api_versions", [f"{bin_dir}/kafka-broker-api-versions.sh", "--bootstrap-server", boot, "--command-config", conf]),
  ("topics_list", [f"{bin_dir}/kafka-topics.sh", "--bootstrap-server", boot, "--command-config", conf, "--list"]),
  ("topics_describe", [f"{bin_dir}/kafka-topics.sh", "--bootstrap-server", boot, "--command-config", conf, "--describe"]),
]
# warmup
subprocess.run(ops[0][1], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
lines = pathlib.Path(out).read_text().splitlines() if pathlib.Path(out).exists() else []
kv = {l.split("=",1)[0]: l.split("=",1)[1] for l in lines if "=" in l}
for name, cmd in ops:
    samples=[]
    ok=True
    for _ in range(runs):
        t0=time.perf_counter()
        p=subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, timeout=180)
        ms=(time.perf_counter()-t0)*1000
        if p.returncode != 0 or "SaslAuthenticationException" in (p.stderr or ""):
            ok=False
            kv[f"local_{name}_ms"] = "fail"
            break
        samples.append(ms)
    if ok and samples:
        kv[f"local_{name}_ms"] = str(int(sum(samples)/len(samples)))
pathlib.Path(out).write_text("\n".join(f"{k}={v}" for k,v in sorted(kv.items()))+"\n")
print("ok")
PY
  rm -f "$tmpprops"
}

_probe_jolokia() {
  local slug="$1" out="$2" host
  host="$(_first_broker)"
  emit "  [${slug}] jolokia via ${host} …"
  mark_ssh_ok "$host" 2>/dev/null || true
  local port="${KAFKA_EXTERNAL_PORT:-9094}"
  local remote_cmd result
  remote_cmd=$(cat <<REMOTE
set -euo pipefail
jfield() {
  local bean="\$1" field="\$2"
  curl -s --max-time 5 "http://127.0.0.1:8779/jolokia/read/\${bean}" 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); v=d.get('value',{});
print(v.get('\$field','') if isinstance(v,dict) else v)" 2>/dev/null || true
}
echo "jolokia_metadata_mean_ms=\$(jfield 'kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Metadata' Mean)"
echo "jolokia_apiversions_mean_ms=\$(jfield 'kafka.network:type=RequestMetrics,name=TotalTimeMs,request=ApiVersions' Mean)"
echo "jolokia_describeconfigs_mean_ms=\$(jfield 'kafka.network:type=RequestMetrics,name=TotalTimeMs,request=DescribeConfigs' Mean)"
echo "jolokia_network_idle=\$(jfield 'kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent' Value)"
echo "jolokia_handler_idle_1m=\$(jfield 'kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent' OneMinuteRate)"
echo "conns_external=\$(ss -tnH state established '( sport = :${port} )' 2>/dev/null | wc -l | tr -d ' ')"
heap=\$(ps -eo args | grep '[k]afka.Kafka' | tr ' ' '\\n' | grep -E '^-Xmx' | head -1 || true)
echo "heap=\${heap:--}"
threads=\$(grep -hE '^num.network.threads=' /var/opt/kafka/config/kraft/server.properties /var/opt/kafka/config/server.properties /opt/kafka/config/server.properties 2>/dev/null | head -1 | cut -d= -f2 || true)
echo "num_network_threads=\${threads:--}"
REMOTE
)
  result="$(_remote "$host" "$remote_cmd" 2>/dev/null || true)"
  local line k v
  while IFS= read -r line; do
    [[ "$line" == *=* ]] || continue
    k="${line%%=*}"; v="${line#*=}"
    case "$k" in
      jolokia_*|conns_external|heap|num_network_threads) _kv_set "$out" "$k" "$v" ;;
    esac
  done <<<"$result"
}

_probe_idle() {
  local slug="$1" out="$2"
  local brokers=() h logdir merge now
  csv_to_array brokers "${BROKER_HOSTS:-}"
  logdir="${KAFKA_LOG_DIR:-/var/opt/kafka/logs}"
  emit "  [${slug}] idle age buckets …"
  merge="$(mktemp "${RESULT_DIR}/idle-${slug}.XXXXXX")"
  : >"$merge"
  now="$(date +%s)"
  for h in "${brokers[@]}"; do
    [[ -z "$h" ]] && continue
    mark_ssh_ok "$h" 2>/dev/null || true
    local remote_cmd piece
    remote_cmd=$(cat <<REMOTE
set -euo pipefail
cd '${logdir}' || exit 2
TMP=\$(mktemp); trap 'rm -f "\$TMP"' EXIT
for d in */; do
  d="\${d%/}"
  case "\$d" in *-[0-9]*) ;; *) continue ;; esac
  t="\${d%-*}"
  m=\$(find "\$d" -maxdepth 1 -name '*.log' -printf '%T@\\n' 2>/dev/null | sort -rn | head -1 || true)
  [ -z "\$m" ] && continue
  sz=\$(du -sb "\$d" 2>/dev/null | cut -f1 || echo 0)
  printf '%s\\t%s\\t%s\\n' "\$t" "\${m%.*}" "\${sz:-0}"
done >"\$TMP"
awk -F'\\t' '{ if (\$2>m[\$1]) m[\$1]=\$2; s[\$1]+=\$3 } END { for (t in m) printf "%s\\t%s\\t%s\\n", t, m[t], s[t] }' "\$TMP"
REMOTE
)
    piece="$(_remote "$h" "$remote_cmd" 2>/dev/null || true)"
    [[ -n "$piece" ]] && printf '%s\n' "$piece" >>"$merge"
  done
  local summary
  summary="$(awk -F'\t' -v now="$now" '
    NF>=3 {
      t=$1; m=$2+0; b=$3+0
      if (!(t in mm) || m>mm[t]) mm[t]=m
      if (!(t in bb) || b>bb[t]) bb[t]=b
    }
    END {
      for (t in mm) {
        a=(now-mm[t])/86400
        if (t ~ /^_/) continue
        n++
        if (a<1) {c1++; b1+=bb[t]}
        else if (a<7) {c7++; b7+=bb[t]}
        else if (a<30) {c30++; b30+=bb[t]}
        else if (a<90) {c90++; b90+=bb[t]}
        else if (a<180) {c180++; b180+=bb[t]}
        else {cX++; bX+=bb[t]}
      }
      printf "idle_topics=%d\n", n+0
      printf "idle_lt1d=%d\n", c1+0
      printf "idle_1_7d=%d\n", c7+0
      printf "idle_7_30d=%d\n", c30+0
      printf "idle_30_90d=%d\n", c90+0
      printf "idle_90_180d=%d\n", c180+0
      printf "idle_gt180d=%d\n", cX+0
      printf "idle_gt180d_gb=%.1f\n", (bX+0)/1073741824
    }
  ' "$merge")"
  rm -f "$merge"
  local line k v
  while IFS= read -r line; do
    [[ "$line" == *=* ]] || continue
    k="${line%%=*}"; v="${line#*=}"
    _kv_set "$out" "$k" "$v"
  done <<<"$summary"
}

_run_cluster() {
  local slug="$1"
  local conf="${CLUSTER_CONF[$slug]}"
  local out="${RESULT_DIR}/${slug}.kv"
  : >"$out"
  emit ""
  emit "${C_BOLD}== ${CLUSTER_LABEL[$slug]} (${slug}) ==${C_RESET}"
  emit "Config: ${conf}"

  # Subshell keeps inventory vars from leaking across clusters
  (
    set -a
    # shellcheck disable=SC1090
    source "$conf"
    set +a
    export SSH_PORT="${SSH_PORT:-22}"
    export KAFKA_CONNECT_BOOTSTRAP="${KAFKA_BOOTSTRAP:-}"
    export SSH_USER
    export NONINTERACTIVE VERBOSE USE_SUDO

    _kv_set "$out" "label" "${CLUSTER_LABEL[$slug]}"
    _kv_set "$out" "config" "$conf"

    if tasks_selected tcp; then _probe_tcp "$slug" "$out"; fi
    if tasks_selected counts; then _probe_counts "$slug" "$out"; fi
    if tasks_selected broker_cli; then _probe_broker_cli "$slug" "$out"; fi
    if tasks_selected local_cli; then _probe_local_cli "$slug" "$out"; fi
    if tasks_selected jolokia; then _probe_jolokia "$slug" "$out"; fi
    if tasks_selected idle; then _probe_idle "$slug" "$out"; fi
  )
}

_print_matrix() {
  local keys=("$@")
  local slug key val width=18
  local header sep_len
  header=$(printf '%-28s' "metric")
  for slug in "${CLUSTER_SLUGS[@]}"; do
    header+=$(printf "  %-${width}s" "$slug")
  done
  sep_len=$((28 + (width + 2) * ${#CLUSTER_SLUGS[@]}))
  emit ""
  emit "${C_BOLD}${header}${C_RESET}"
  emit "$(python3 -c "print('-'*${sep_len})")"
  for key in "${keys[@]}"; do
    local row any=0
    row=$(printf '%-28s' "$key")
    for slug in "${CLUSTER_SLUGS[@]}"; do
      val="$(_kv_get "${RESULT_DIR}/${slug}.kv" "$key")"
      if [[ -n "$val" ]]; then
        any=1
      else
        val="-"
      fi
      row+=$(printf "  %-${width}s" "$val")
    done
    (( any )) || continue
    emit "$row"
  done
}

_default_local_bin() {
  if [[ -n "$LOCAL_BIN" ]]; then
    return 0
  fi
  local candidate="${HOME}/Softs/kafka/kafka_2.13-3.9.0/bin"
  if [[ -x "${candidate}/kafka-topics.sh" ]]; then
    LOCAL_BIN="$candidate"
  fi
}

main() {
  parse_args "$@"
  if [[ "${LIST_TASKS}" == "1" ]]; then
    tasks_list
    exit 0
  fi
  tasks_select || exit $?
  _collect_configs
  _default_local_bin

  if ! [[ "$LOCAL_RUNS" =~ ^[0-9]+$ ]] || (( LOCAL_RUNS < 1 || LOCAL_RUNS > 10 )); then
    loge "--local-runs must be 1–10"; exit 2
  fi

  mkdir -p "$REPORT_DIR"
  RESULT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/kcompare.XXXXXX")"
  local report
  report="${REPORT_DIR}/compare-$(date +%Y%m%d-%H%M%S).txt"
  REPORT_FILE="$report"
  export REPORT_FILE

  init_ssh_status_store
  install_interrupt_traps

  emit "${C_BOLD}Kafka cluster compare v${SCRIPT_VERSION}${C_RESET}"
  emit "Clusters: ${#CLUSTER_SLUGS[@]}  probes: ${SELECTED_TASKS}"
  emit "Local bin: ${LOCAL_BIN:-"(none)"}"
  emit "Report: ${report}"

  prompt_credentials

  local slug
  for slug in "${CLUSTER_SLUGS[@]}"; do
    _run_cluster "$slug"
  done

  emit ""
  emit "${C_BOLD}======== Comparison ========${C_RESET}"
  _print_matrix \
    tcp_avg_ms \
    brokers topics partitions groups \
    broker_api_versions_ms broker_topics_list_ms broker_topics_describe_ms broker_log_dirs_ms \
    local_api_versions_ms local_topics_list_ms local_topics_describe_ms \
    jolokia_metadata_mean_ms jolokia_apiversions_mean_ms jolokia_describeconfigs_mean_ms \
    jolokia_network_idle jolokia_handler_idle_1m conns_external heap num_network_threads \
    idle_topics idle_gt180d idle_gt180d_gb

  emit ""
  emit "Per-cluster detail files: ${RESULT_DIR}/*.kv"
  # Persist kv copies into report dir
  for slug in "${CLUSTER_SLUGS[@]}"; do
    cp "${RESULT_DIR}/${slug}.kv" "${REPORT_DIR}/compare-${slug}-$(date +%Y%m%d-%H%M%S).kv"
  done
  emit "Done. Full console also in ${report}"
}

main "$@"

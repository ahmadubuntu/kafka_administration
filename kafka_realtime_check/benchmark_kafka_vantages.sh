#!/usr/bin/env bash
# Inventory-driven 3-vantage Kafka performance benchmark:
#   1) Kubernetes namespace/pods (e.g., namespace=kafkausage-test on Dev)
#   2) Operator laptop (local JVM CLI + optional kcat metadata probe)
#   3) Broker VM itself over SSH (loopback / local interface)
#
# Dry-run by default (Constitution Principle II). Pass --apply -y to execute.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARSER="${SCRIPT_DIR}/lib/benchmark_parse.py"

INVENTORY=""
K8S_CONTEXT="Dev"
K8S_NAMESPACE="kafkausage-test"
K8S_IMAGE="dockerhub.charisma.tech/bitnami/kafka:3.4.0-debian-11-r15"
SSH_HOST_ALIAS=""
WRITE_TOPIC="kafkausage-bench-write"
READ_TOPIC="kafkausage-bench-read"
PARTITIONS=3
REPLICATION_FACTOR=1
NUM_RECORDS=50000
RECORD_SIZE=1024
LIST_RUNS=3
APPLY=0
ASSUME_YES=0
KEEP_RESOURCES=1
REPORT_JSON=""

usage() {
  cat <<'EOF'
Usage: benchmark_kafka_vantages.sh -c <cluster.env> [options]

Options:
  -c, --config <path>       Cluster inventory .env file (required)
  --k8s-context <ctx>       Kubernetes context (default: Dev)
  --namespace <ns>          Kubernetes namespace (default: kafkausage-test)
  --k8s-image <img_ref>     Container image with Kafka CLI (default: bitnami/kafka:3.4.0)
  --ssh-target <host>       Optional SSH host/alias override for broker VM
  --write-topic <name>      Producer target topic (default: kafkausage-bench-write)
  --read-topic <name>       Concurrent consumer source topic (default: kafkausage-bench-read)
  --records <N>             Records per benchmark run (default: 50000)
  --record-size <bytes>     Bytes per record (default: 1024)
  --list-runs <N>           Topic-list timing iterations per vantage (default: 3)
  --report-json <path>      Optional path to write JSON benchmark report
  --keep                    Keep K8s namespace/pods and test topics after run (default)
  --cleanup                 Delete K8s namespace and test topics after run
  --apply                   Execute live benchmark (without this flag, dry-run only)
  -y, --yes                 Non-interactive confirmation required with --apply
  -h, --help                Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--config) INVENTORY="$2"; shift 2 ;;
    --k8s-context) K8S_CONTEXT="$2"; shift 2 ;;
    --namespace) K8S_NAMESPACE="$2"; shift 2 ;;
    --k8s-image) K8S_IMAGE="$2"; shift 2 ;;
    --ssh-target) SSH_HOST_ALIAS="$2"; shift 2 ;;
    --write-topic) WRITE_TOPIC="$2"; shift 2 ;;
    --read-topic) READ_TOPIC="$2"; shift 2 ;;
    --records) NUM_RECORDS="$2"; shift 2 ;;
    --record-size) RECORD_SIZE="$2"; shift 2 ;;
    --list-runs) LIST_RUNS="$2"; shift 2 ;;
    --report-json) REPORT_JSON="$2"; shift 2 ;;
    --keep) KEEP_RESOURCES=1; shift ;;
    --cleanup) KEEP_RESOURCES=0; shift ;;
    --apply) APPLY=1; shift ;;
    -y|--yes) ASSUME_YES=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$INVENTORY" || ! -f "$INVENTORY" ]]; then
  echo "ERROR: valid -c <cluster.env> is required" >&2
  exit 2
fi

# shellcheck disable=SC1090
source "$INVENTORY"

BOOTSTRAP="${KAFKA_BOOTSTRAP:-}"
BROKER_FIRST="${BROKER_HOSTS%%,*}"
REMOTE_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
REMOTE_CONF="${KAFKA_COMMAND_CONFIG:-/opt/kafka/config/admin.properties}"
SSH_P="${SSH_PORT:-22}"

if [[ -z "$BOOTSTRAP" || -z "$BROKER_FIRST" ]]; then
  echo "ERROR: KAFKA_BOOTSTRAP and BROKER_HOSTS must be defined in $INVENTORY" >&2
  exit 2
fi

if [[ -z "$SSH_HOST_ALIAS" ]]; then
  if ssh -G devkafka 2>/dev/null | grep -q "^hostname ${BROKER_FIRST}\$"; then
    SSH_HOST_ALIAS="devkafka"
  else
    SSH_HOST_ALIAS="${BROKER_FIRST}"
  fi
fi

if [[ "$APPLY" -ne 1 || "$ASSUME_YES" -ne 1 ]]; then
  cat <<EOF
[DRY-RUN] 3-Vantage Kafka Benchmark Plan (pass --apply -y to execute)
  Inventory        : ${INVENTORY} (${CLUSTER_NAME:-unknown})
  Bootstrap        : ${BOOTSTRAP}
  Broker SSH target: ${SSH_HOST_ALIAS} (port ${SSH_P})
  K8s target       : context=${K8S_CONTEXT} namespace=${K8S_NAMESPACE}
  K8s pods         : kafka-producer-client, kafka-consumer-client (${K8S_IMAGE})
  Topics           : write=${WRITE_TOPIC}, read=${READ_TOPIC} (partitions=${PARTITIONS}, rf=${REPLICATION_FACTOR})
  Workload         : ${NUM_RECORDS} records x ${RECORD_SIZE} bytes (concurrent write + read)
  Topic list runs  : ${LIST_RUNS} iterations per vantage
  Retention policy : $([[ "$KEEP_RESOURCES" -eq 1 ]] && echo "KEEP namespace & topics" || echo "CLEANUP after test")
EOF
  exit 0
fi

ssh_broker() {
  if [[ "$SSH_HOST_ALIAS" == "$BROKER_FIRST" ]]; then
    ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new -p "$SSH_P" "$BROKER_FIRST" "$@"
  else
    ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$SSH_HOST_ALIAS" "$@"
  fi
}

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/kafka-bench.XXXXXX")"
chmod 700 "$TMP_DIR"
LOCAL_CONF="${TMP_DIR}/admin.properties"
trap 'rm -rf "$TMP_DIR"' EXIT

echo "==> [1/6] Staging SASL client config from broker ${SSH_HOST_ALIAS} (mode 0600, never printed)..."
ssh_broker "cat '${REMOTE_CONF}' 2>/dev/null || sudo -n cat '${REMOTE_CONF}'" > "$LOCAL_CONF"
chmod 600 "$LOCAL_CONF"

LOCAL_KAFKA_DIR="${TMPDIR:-/tmp}/kafka-cli-cache"
if [[ ! -x "${LOCAL_KAFKA_DIR}/bin/kafka-topics.sh" ]]; then
  echo "==> Syncing Kafka CLI distribution from broker ${SSH_HOST_ALIAS}:/opt/kafka to ${LOCAL_KAFKA_DIR} for laptop JVM parity..."
  mkdir -p "${LOCAL_KAFKA_DIR}/config"
  ssh_broker "tar -C /opt/kafka -cf - bin libs" | tar -C "$LOCAL_KAFKA_DIR" -xf -
  ssh_broker "cat /opt/kafka/config/tools-log4j2.yaml 2>/dev/null || true" > "${LOCAL_KAFKA_DIR}/config/tools-log4j2.yaml"
fi
LOCAL_BIN="${LOCAL_KAFKA_DIR}/bin"

echo "==> [2/6] Provisioning Kubernetes context=${K8S_CONTEXT} namespace=${K8S_NAMESPACE}..."
kubectl --context="$K8S_CONTEXT" get ns "$K8S_NAMESPACE" >/dev/null 2>&1 || \
  kubectl --context="$K8S_CONTEXT" create ns "$K8S_NAMESPACE"

kubectl --context="$K8S_CONTEXT" -n "$K8S_NAMESPACE" create secret generic kafka-client-config \
  --from-file=admin.properties="$LOCAL_CONF" \
  --dry-run=client -o yaml | kubectl --context="$K8S_CONTEXT" -n "$K8S_NAMESPACE" apply -f - >/dev/null

cat <<EOF | kubectl --context="$K8S_CONTEXT" -n "$K8S_NAMESPACE" apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: kafka-producer-client
  namespace: ${K8S_NAMESPACE}
  labels:
    app: kafka-benchmark
    role: producer
spec:
  restartPolicy: Always
  containers:
  - name: kafka-client
    image: ${K8S_IMAGE}
    imagePullPolicy: IfNotPresent
    command: ["sleep", "infinity"]
    volumeMounts:
    - name: kafka-config
      mountPath: /etc/kafka-FromSecret
      readOnly: true
  volumes:
  - name: kafka-config
    secret:
      secretName: kafka-client-config
---
apiVersion: v1
kind: Pod
metadata:
  name: kafka-consumer-client
  namespace: ${K8S_NAMESPACE}
  labels:
    app: kafka-benchmark
    role: consumer
spec:
  restartPolicy: Always
  containers:
  - name: kafka-client
    image: ${K8S_IMAGE}
    imagePullPolicy: IfNotPresent
    command: ["sleep", "infinity"]
    volumeMounts:
    - name: kafka-config
      mountPath: /etc/kafka-FromSecret
      readOnly: true
  volumes:
  - name: kafka-config
    secret:
      secretName: kafka-client-config
EOF

echo "==> Waiting for pods kafka-producer-client and kafka-consumer-client in ${K8S_NAMESPACE} to become Ready..."
kubectl --context="$K8S_CONTEXT" -n "$K8S_NAMESPACE" wait --for=condition=Ready pod/kafka-producer-client pod/kafka-consumer-client --timeout=120s

K8S_BIN="/opt/bitnami/kafka/bin"
K8S_CONF="/etc/kafka-FromSecret/admin.properties"

echo "==> [3/6] Preparing benchmark topics (${WRITE_TOPIC}, ${READ_TOPIC}) and pre-seeding ${READ_TOPIC}..."
ssh_broker "${REMOTE_BIN}/kafka-topics.sh --bootstrap-server '${BOOTSTRAP}' --command-config '${REMOTE_CONF}' --create --if-not-exists --topic '${WRITE_TOPIC}' --partitions ${PARTITIONS} --replication-factor ${REPLICATION_FACTOR}"
ssh_broker "${REMOTE_BIN}/kafka-topics.sh --bootstrap-server '${BOOTSTRAP}' --command-config '${REMOTE_CONF}' --create --if-not-exists --topic '${READ_TOPIC}' --partitions ${PARTITIONS} --replication-factor ${REPLICATION_FACTOR}"

# Pre-seed READ_TOPIC so concurrent consumers always have >= NUM_RECORDS available immediately
ssh_broker "${REMOTE_BIN}/kafka-producer-perf-test.sh --topic '${READ_TOPIC}' --num-records ${NUM_RECORDS} --record-size ${RECORD_SIZE} --throughput -1 --producer-props bootstrap.servers='${BOOTSTRAP}' --producer.config '${REMOTE_CONF}'" >/dev/null

measure_topic_list() {
  local vantage="$1"
  local i start_ns end_ns elapsed_ms count=0
  local samples=()
  for ((i=1; i<=LIST_RUNS; i++)); do
    start_ns="$(date +%s%N)"
    if [[ "$vantage" == "k8s" ]]; then
      out="$(kubectl --context="$K8S_CONTEXT" -n "$K8S_NAMESPACE" exec kafka-producer-client -- \
        "${K8S_BIN}/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --command-config "$K8S_CONF" --list 2>/dev/null)"
    elif [[ "$vantage" == "laptop" ]]; then
      out="$("${LOCAL_BIN}/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --command-config "$LOCAL_CONF" --list 2>/dev/null)"
    elif [[ "$vantage" == "broker_vm" ]]; then
      out="$(ssh_broker "${REMOTE_BIN}/kafka-topics.sh --bootstrap-server '${BOOTSTRAP}' --command-config '${REMOTE_CONF}' --list 2>/dev/null")"
    fi
    end_ns="$(date +%s%N)"
    elapsed_ms="$(awk -v s="$start_ns" -v e="$end_ns" 'BEGIN { printf "%.2f", (e - s) / 1000000.0 }')"
    count="$(printf '%s\n' "$out" | sed '/^[[:space:]]*$/d' | wc -l | tr -d ' ')"
    samples+=("$elapsed_ms")
  done
  local stats_json
  stats_json="$(printf '%s\n' "${samples[@]}" | python3 "$PARSER" timings)"
  python3 -c "import json,sys; d=json.loads(sys.argv[1]); d['topic_count']=int(sys.argv[2]); print(json.dumps(d))" "$stats_json" "$count"
}

echo "==> [4/6] Running Benchmark 1: Topic List Latency (${LIST_RUNS} runs per vantage)..."
LIST_K8S_JSON="$(measure_topic_list k8s)"
LIST_LAPTOP_JSON="$(measure_topic_list laptop)"
LIST_VM_JSON="$(measure_topic_list broker_vm)"

run_concurrent_pair() {
  local vantage="$1"
  local prod_out="${TMP_DIR}/prod_${vantage}.txt"
  local cons_out="${TMP_DIR}/cons_${vantage}.txt"
  local grp="kafkausage-bench-${vantage}-$(date +%s)"
  local start_ns end_ns wall_ms

  start_ns="$(date +%s%N)"
  if [[ "$vantage" == "k8s" ]]; then
    kubectl --context="$K8S_CONTEXT" -n "$K8S_NAMESPACE" exec kafka-producer-client -- \
      "${K8S_BIN}/kafka-producer-perf-test.sh" \
      --topic "$WRITE_TOPIC" \
      --num-records "$NUM_RECORDS" \
      --record-size "$RECORD_SIZE" \
      --throughput -1 \
      --producer-props bootstrap.servers="$BOOTSTRAP" acks=all \
      --producer.config "$K8S_CONF" >"$prod_out" 2>&1 &
    local p_pid=$!

    kubectl --context="$K8S_CONTEXT" -n "$K8S_NAMESPACE" exec kafka-consumer-client -- \
      "${K8S_BIN}/kafka-consumer-perf-test.sh" \
      --bootstrap-server "$BOOTSTRAP" \
      --topic "$READ_TOPIC" \
      --messages "$NUM_RECORDS" \
      --timeout 60000 \
      --group "$grp" \
      --consumer.config "$K8S_CONF" >"$cons_out" 2>&1 &
    local c_pid=$!

    wait "$p_pid"
    wait "$c_pid"
  elif [[ "$vantage" == "laptop" ]]; then
    "${LOCAL_BIN}/kafka-producer-perf-test.sh" \
      --topic "$WRITE_TOPIC" \
      --num-records "$NUM_RECORDS" \
      --record-size "$RECORD_SIZE" \
      --throughput -1 \
      --producer-props bootstrap.servers="$BOOTSTRAP" acks=all \
      --producer.config "$LOCAL_CONF" >"$prod_out" 2>&1 &
    local p_pid=$!

    "${LOCAL_BIN}/kafka-consumer-perf-test.sh" \
      --bootstrap-server "$BOOTSTRAP" \
      --topic "$READ_TOPIC" \
      --messages "$NUM_RECORDS" \
      --timeout 60000 \
      --group "$grp" \
      --consumer.config "$LOCAL_CONF" >"$cons_out" 2>&1 &
    local c_pid=$!

    wait "$p_pid"
    wait "$c_pid"
  elif [[ "$vantage" == "broker_vm" ]]; then
    ssh_broker "${REMOTE_BIN}/kafka-producer-perf-test.sh --topic '${WRITE_TOPIC}' --num-records ${NUM_RECORDS} --record-size ${RECORD_SIZE} --throughput -1 --producer-props bootstrap.servers='${BOOTSTRAP}' acks=all --producer.config '${REMOTE_CONF}'" >"$prod_out" 2>&1 &
    local p_pid=$!

    ssh_broker "${REMOTE_BIN}/kafka-consumer-perf-test.sh --bootstrap-server '${BOOTSTRAP}' --topic '${READ_TOPIC}' --messages ${NUM_RECORDS} --timeout 60000 --group '${grp}' --consumer.config '${REMOTE_CONF}'" >"$cons_out" 2>&1 &
    local c_pid=$!

    wait "$p_pid"
    wait "$c_pid"
  fi
  end_ns="$(date +%s%N)"
  wall_ms="$(awk -v s="$start_ns" -v e="$end_ns" 'BEGIN { printf "%.2f", (e - s) / 1000000.0 }')"

  local p_json c_json
  p_json="$(python3 "$PARSER" producer <"$prod_out" || true)"
  c_json="$(python3 "$PARSER" consumer <"$cons_out" || true)"
  python3 -c "import json,sys; print(json.dumps({'wall_ms': float(sys.argv[1]), 'producer': json.loads(sys.argv[2]), 'consumer': json.loads(sys.argv[3])}))" \
    "$wall_ms" "$p_json" "$c_json"
}

echo "==> [5/6] Running Benchmark 2: Concurrent Produce (${WRITE_TOPIC}) + Consume (${READ_TOPIC}) across 3 vantages..."
PERF_K8S_JSON="$(run_concurrent_pair k8s)"
PERF_LAPTOP_JSON="$(run_concurrent_pair laptop)"
PERF_VM_JSON="$(run_concurrent_pair broker_vm)"

if [[ "$KEEP_RESOURCES" -eq 0 ]]; then
  echo "==> Cleaning up test topics and Kubernetes namespace ${K8S_NAMESPACE}..."
  ssh_broker "${REMOTE_BIN}/kafka-topics.sh --bootstrap-server '${BOOTSTRAP}' --command-config '${REMOTE_CONF}' --delete --topic '${WRITE_TOPIC}'" || true
  ssh_broker "${REMOTE_BIN}/kafka-topics.sh --bootstrap-server '${BOOTSTRAP}' --command-config '${REMOTE_CONF}' --delete --topic '${READ_TOPIC}'" || true
  kubectl --context="$K8S_CONTEXT" delete ns "$K8S_NAMESPACE" --wait=false || true
fi

echo "==> [6/6] Aggregating 3-Vantage Benchmark Report..."
FINAL_JSON="$(python3 - "$LIST_K8S_JSON" "$LIST_LAPTOP_JSON" "$LIST_VM_JSON" "$PERF_K8S_JSON" "$PERF_LAPTOP_JSON" "$PERF_VM_JSON" "$NUM_RECORDS" "$RECORD_SIZE" "$K8S_CONTEXT" "$K8S_NAMESPACE" "$BOOTSTRAP" <<'PY'
import json, sys
list_k8s, list_lap, list_vm, perf_k8s, perf_lap, perf_vm = [json.loads(x) for x in sys.argv[1:7]]
out = {
    "meta": {
        "records": int(sys.argv[7]),
        "record_size_bytes": int(sys.argv[8]),
        "k8s_context": sys.argv[9],
        "k8s_namespace": sys.argv[10],
        "bootstrap": sys.argv[11],
    },
    "vantages": {
        "k8s_pod": {"topic_list": list_k8s, "concurrent_perf": perf_k8s},
        "laptop": {"topic_list": list_lap, "concurrent_perf": perf_lap},
        "broker_vm": {"topic_list": list_vm, "concurrent_perf": perf_vm},
    },
}
print(json.dumps(out, indent=2, sort_keys=True))
PY
)"

if [[ -n "$REPORT_JSON" ]]; then
  printf '%s\n' "$FINAL_JSON" > "$REPORT_JSON"
fi

printf '%s\n' "$FINAL_JSON"

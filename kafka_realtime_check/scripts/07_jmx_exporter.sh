#!/usr/bin/env bash
# Pull high-signal metrics from jmx_exporter /metrics (Prometheus text).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../lib/admin_common.sh"

section "07  JMX exporter"
kv "URL" "$KAFKA_JMX_METRICS_URL"

if ! have curl; then
  err "curl required"
  exit 1
fi

tmp="$(mktemp)"
if ! curl -sf -m 15 -o "$tmp" "$KAFKA_JMX_METRICS_URL"; then
  err "cannot fetch $KAFKA_JMX_METRICS_URL"
  exit 1
fi
cp "$tmp" "$REPORT_DIR/jmx_metrics_raw.prom"
ok "fetched $(wc -c <"$tmp") bytes → $REPORT_DIR/jmx_metrics_raw.prom"

# Pretty key=value extract for selected gauges
_jmx_val() {
  local pat="$1"
  awk -v p="$pat" '
    $0 !~ /^#/ && index($0, p) {
      print $NF
      exit
    }
  ' "$tmp"
}

subsection "Cluster health"
kv "active brokers" "$(_jmx_val 'activebrokercount_value')"
kv "active controller" "$(_jmx_val 'activecontrollercount_value')"
kv "topics" "$(_jmx_val 'globaltopiccount_value')"
kv "partitions" "$(_jmx_val 'globalpartitioncount_value')"
kv "under-replicated" "$(_jmx_val 'underreplicatedpartitions_value')"
kv "offline partitions" "$(_jmx_val 'offlinepartitionscount_value')"
kv "metadata lag ms" "$(_jmx_val 'lastappliedrecordlagms_value')"
kv "request queue" "$(_jmx_val 'requestqueuesize_value')"
kv "fenced brokers" "$(_jmx_val 'fencedbrokercount_value')"

subsection "JVM"
kv "heap used bytes" "$(_jmx_val 'jvm_memory_used_bytes{area="heap"}')"
kv "heap max bytes" "$(_jmx_val 'jvm_memory_max_bytes{area="heap"}')"
kv "threads" "$(_jmx_val 'jvm_threads_current')"
kv "GC young count" "$(_jmx_val 'jvm_gc_collection_seconds_count{gc="G1 Young Generation"}')"
kv "GC old count" "$(_jmx_val 'jvm_gc_collection_seconds_count{gc="G1 Old Generation"}')"

subsection "Throughput (1m rate)"
kv "messages in/s" "$(_jmx_val 'total_messagesinpersec_oneminuterate')"
kv "bytes in/s" "$(_jmx_val 'total_bytesinpersec_oneminuterate')"
kv "bytes out/s" "$(_jmx_val 'total_bytesoutpersec_oneminuterate')"
kv "fetch req/s" "$(_jmx_val 'total_totalfetchrequestspersec_oneminuterate')"
kv "produce req/s" "$(_jmx_val 'total_totalproducerequestspersec_oneminuterate')"
kv "failed fetch/s" "$(_jmx_val 'total_failedfetchrequestspersec_oneminuterate')"
kv "failed produce/s" "$(_jmx_val 'total_failedproducerequestspersec_oneminuterate')"

subsection "Admin-heavy request counts (lifetime)"
grep -E 'kafka_network_requestmetrics_totaltimems_count\{request="(Metadata|DescribeConfigs|DescribeGroups|ListGroups|OffsetFetch|FindCoordinator|ApiVersions|Produce|Fetch)"\}' \
  "$tmp" | sed -E 's/.*request="([^"]+)".* ([0-9.eE+-]+)$/  \1\t\2/' | column -t -s $'\t' 2>/dev/null \
  || grep -E 'kafka_network_requestmetrics_totaltimems_count\{request="(Metadata|DescribeConfigs|DescribeGroups|ListGroups|OffsetFetch|FindCoordinator|ApiVersions|Produce|Fetch)"\}' "$tmp" || true

subsection "Metadata / DescribeConfigs errors"
grep -E 'kafka_network_requestmetrics_errorspersec_count\{request="(Metadata|DescribeConfigs)' \
  "$tmp" | sed -E 's/^kafka_network_requestmetrics_errorspersec_count\{request="([^"]+), error=([^"]+)"\} ([0-9.eE+-]+)$/  \1  error=\2  count=\3/' | head -20 || true

rm -f "$tmp"
info "Raw Prometheus scrape kept for deeper grep."

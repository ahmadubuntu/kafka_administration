#!/usr/bin/env bash
# Pull high-signal metrics from jmx_exporter /metrics (Prometheus text).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../lib/common.sh"

section "07 JMX exporter ($KAFKA_JMX_METRICS_URL)"

if ! have curl; then
  echo "curl required"
  exit 1
fi

tmp="$(mktemp)"
if ! curl -sf -m 15 -o "$tmp" "$KAFKA_JMX_METRICS_URL"; then
  echo "ERROR: cannot fetch $KAFKA_JMX_METRICS_URL"
  exit 1
fi
cp "$tmp" "$REPORT_DIR/jmx_metrics_raw.prom"
echo "fetched bytes=$(wc -c <"$tmp")"

subsection "Cluster controller / replica health"
grep -E 'underreplicatedpartitions_value|offlinepartitionscount_value|activecontrollercount_value|activebrokercount_value|globaltopiccount_value|globalpartitioncount_value|lastappliedrecordlagms_value|requestqueuesize_value$|responsequeuesize_value$|fencedbrokercount_value|metadataerrorcount_value' \
  "$tmp" | grep -v '^#' || true

subsection "JVM memory / GC / threads"
grep -E 'jvm_memory_used_bytes|jvm_memory_max_bytes|jvm_gc_collection_seconds|jvm_threads_current|process_cpu_seconds_total' \
  "$tmp" | grep -v '^#' | head -40 || true

subsection "Broker throughput (1m rates)"
grep -E 'kafka_server_brokertopicmetrics_total_(messagesinpersec_oneminuterate|bytesinpersec_oneminuterate|bytesoutpersec_oneminuterate|totalfetchrequestspersec_oneminuterate|totalproducerequestspersec_oneminuterate|failedfetchrequestspersec_oneminuterate|failedproducerequestspersec_oneminuterate) ' \
  "$tmp" | grep -v '^#' || true

subsection "Request counts (lifetime) for admin-heavy APIs"
grep -E 'kafka_network_requestmetrics_totaltimems_count\{request="(Metadata|DescribeConfigs|DescribeGroups|ListGroups|OffsetFetch|FindCoordinator|ApiVersions|Produce|Fetch)"\}' \
  "$tmp" || true

subsection "Metadata / DescribeConfigs error counts"
grep -E 'kafka_network_requestmetrics_errorspersec_count\{request="(Metadata|DescribeConfigs)' \
  "$tmp" | head -40 || true

rm -f "$tmp"
echo
echo "Full scrape saved to $REPORT_DIR/jmx_metrics_raw.prom"

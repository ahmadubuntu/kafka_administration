#!/usr/bin/env bash
# Human-readable checklist after collecting reports.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../lib/common.sh"

section "10 What to look for"

cat <<'EOF'
1) admin.properties / KAFKA_BOOTSTRAP must match THIS cluster's advertised EXTERNAL.
   Timeout on listTopics (~60s) almost always means wrong bootstrap.

2) Cluster health: UnderReplicatedPartitions=0 and OfflinePartitionsCount=0.

3) UI slowness with healthy broker:
   - DescribeConfigs ResponseSendTimeMs p95/p99 in seconds (Jolokia script)
   - LocalTimeMs still ~tens of ms  => broker OK, client/UI draining slowly
   - Heavy pages: describe --all-groups, describe all topics + configs

4) Connection pressure:
   - Thousands of ESTABLISHED on :9094 with num.network.threads=3 is a smell
   - Check top peer IPs / misconfigured clients (SASL handshake errors)

5) Auth noise:
   - Constant DefaultDeny in authorizer log = clients probing forbidden topics/groups

6) Resources:
   - Disk for log.dirs, heap (no frequent Full GC), CPU of kafka java process

7) Capacity / FD (script 11):
   - Open FD should stay well below ~70% of Max open files
   - Compare partitions/connections to comfort / caution / stretch bands
   - Page cache (MemAvailable) matters as much as heap for Kafka

Reports directory:
EOF
echo "  $REPORT_DIR"
ls -la "$REPORT_DIR" 2>/dev/null || true

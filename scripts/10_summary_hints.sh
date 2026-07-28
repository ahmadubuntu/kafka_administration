#!/usr/bin/env bash
# Human-readable checklist after collecting reports.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../lib/admin_common.sh"

section "10  What to look for"

bullet "admin.properties / KAFKA_BOOTSTRAP must match this cluster's advertised EXTERNAL"
info "Timeout on listTopics (~60s) almost always means wrong bootstrap"

bullet "Cluster health: UnderReplicatedPartitions=0 and OfflinePartitionsCount=0"

bullet "UI slow but broker healthy → check DescribeConfigs ResponseSendTimeMs (Jolokia)"
info "LocalTimeMs tens of ms + ResponseSend seconds ⇒ client/UI drain issue"

bullet "Heavy UI pages: describe --all-groups, describe all topics + configs"

bullet "Connection pressure: many ESTABLISHED on :9094 with low num.network.threads"

bullet "Auth noise: constant DefaultDeny in authorizer log"

bullet "Capacity script (11): FD << 70% max, partitions/connections vs comfort/caution/stretch"
info "Page cache (MemAvailable) matters as much as heap for Kafka"

echo
subsection "Reports written"
kv "directory" "$REPORT_DIR"
if [[ -d "$REPORT_DIR" ]]; then
  ls -lah "$REPORT_DIR" | sed 's/^/  /'
fi

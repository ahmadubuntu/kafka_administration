#!/usr/bin/env bash
# Deep request latency via Jolokia (percentiles + DescribeConfigs breakdown).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../lib/common.sh"

require_cmd curl python3

if ! curl -sf -m 5 -o /dev/null "${KAFKA_JOLOKIA_URL%/}/version"; then
  err "Jolokia not reachable at $KAFKA_JOLOKIA_URL"
  exit 1
fi

export KAFKA_JOLOKIA_URL
python3 "$SCRIPT_DIR/../lib/jolokia_latency.py" | tee "$REPORT_DIR/jolokia_latency.txt"

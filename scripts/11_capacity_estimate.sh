#!/usr/bin/env bash
# FD / RAM / CPU / partition / connection headroom + rough ceilings.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../lib/admin_common.sh"

require_cmd python3

export KAFKA_JOLOKIA_URL KAFKA_JMX_METRICS_URL KAFKA_SYSTEMD_UNIT
export KAFKA_SERVER_PROPERTIES KAFKA_LOG_DIR

python3 "$SCRIPT_DIR/../lib/capacity_estimate.py" | tee "$REPORT_DIR/capacity_estimate.txt"

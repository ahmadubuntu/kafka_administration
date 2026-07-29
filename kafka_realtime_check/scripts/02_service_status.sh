#!/usr/bin/env bash
# systemd / process / listener status for the Kafka broker.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../lib/admin_common.sh"

section "02 Service & listeners"

subsection "systemd unit"
if have systemctl; then
  systemctl is-active "$KAFKA_SYSTEMD_UNIT" 2>/dev/null || true
  systemctl status "$KAFKA_SYSTEMD_UNIT" --no-pager -l 2>/dev/null | head -50 || true
else
  echo "systemctl not available"
fi

subsection "Java Kafka process (ps)"
ps auxww | grep -E '[k]afka\.Kafka|[o]rg.apache.kafka' || echo "(no kafka java process matched)"

subsection "Listening ports (9092/9093/9094 + JMX/Jolokia)"
if have ss; then
  ss -lntp 2>/dev/null | grep -E ':(9092|9093|9094|7071|8779)\b' || ss -lntp | head -40
elif have netstat; then
  netstat -lntp 2>/dev/null | grep -E ':(9092|9093|9094|7071|8779)\b' || true
else
  echo "ss/netstat not available"
fi

subsection "Observability endpoints"
for url in "$KAFKA_JMX_METRICS_URL" "${KAFKA_JOLOKIA_URL%/}/version"; do
  code="$(curl -s -m 5 -o /dev/null -w '%{http_code} size=%{size_download}' "$url" 2>/dev/null || echo fail)"
  echo "$url -> $code"
done

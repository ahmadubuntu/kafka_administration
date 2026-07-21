#!/usr/bin/env bash
# Inspect broker listeners / security / threading and validate admin.properties bootstrap.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../lib/common.sh"

section "05 Broker config & admin.properties check"

subsection "server.properties key settings"
if [[ -f "$KAFKA_SERVER_PROPERTIES" ]]; then
  grep -E '^(process\.roles|node\.id|listeners|advertised\.listeners|listener\.security|controller\.|inter\.broker|log\.dirs|num\.network|num\.io|num\.partitions|default\.replication|min\.insync|socket\.|authorizer|allow\.everyone|sasl\.|super\.users|log\.retention)' \
    "$KAFKA_SERVER_PROPERTIES" || true
else
  echo "WARNING: server properties not found: $KAFKA_SERVER_PROPERTIES"
fi

subsection "admin / command-config (redacted)"
if [[ -f "$KAFKA_COMMAND_CONFIG" ]]; then
  ls -la "$KAFKA_COMMAND_CONFIG"
  redact_props <"$KAFKA_COMMAND_CONFIG"
else
  echo "WARNING: missing $KAFKA_COMMAND_CONFIG"
fi

subsection "Bootstrap vs advertised EXTERNAL (mismatch detector)"
advertised="$(grep -E '^advertised\.listeners=' "$KAFKA_SERVER_PROPERTIES" 2>/dev/null || true)"
bootstrap_cfg="$(grep -E '^bootstrap\.servers=' "$KAFKA_COMMAND_CONFIG" 2>/dev/null || true)"
echo "server:  $advertised"
echo "admin:   $bootstrap_cfg"
echo "env:     KAFKA_BOOTSTRAP=$KAFKA_BOOTSTRAP"

# Extract host:port pairs from advertised EXTERNAL://host:port
ext="$(echo "$advertised" | sed -n 's/.*EXTERNAL:\/\/\([^,]*\).*/\1/p')"
admin_bs="$(echo "$bootstrap_cfg" | cut -d= -f2-)"
if [[ -n "$ext" && -n "$admin_bs" ]]; then
  if [[ "$admin_bs" == *"$ext"* ]]; then
    echo "OK: admin bootstrap includes advertised EXTERNAL ($ext)"
  else
    echo "WARN: admin bootstrap does NOT include advertised EXTERNAL ($ext)"
    echo "      UI / admin tools using this file may timeout against the wrong cluster."
  fi
fi

subsection "JAAS / security related files (names only)"
ls -la "$(dirname "$KAFKA_SERVER_PROPERTIES")" 2>/dev/null | head -40 || true
ls -la /var/opt/kafka/*.conf 2>/dev/null || true

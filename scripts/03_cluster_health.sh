#!/usr/bin/env bash
# Classic Kafka admin health: topics, URP, offline, groups, broker API.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../lib/common.sh"

section "03 Cluster health (CLI)"

require_cmd bash
if [[ ! -x "$KAFKA_BIN/kafka-topics.sh" ]]; then
  echo "ERROR: $KAFKA_BIN/kafka-topics.sh not found. Set KAFKA_HOME/KAFKA_BIN." >&2
  exit 1
fi

subsection "Bootstrap / command-config"
echo "KAFKA_BOOTSTRAP=$KAFKA_BOOTSTRAP"
echo "KAFKA_COMMAND_CONFIG=$KAFKA_COMMAND_CONFIG"
if [[ -f "$KAFKA_COMMAND_CONFIG" ]]; then
  echo "command-config (redacted):"
  redact_props <"$KAFKA_COMMAND_CONFIG"
else
  echo "WARNING: command-config file missing (will try without auth)"
fi

subsection "Broker API versions (timed)"
set +e
time kafka_admin kafka-broker-api-versions.sh 2>&1 | head -40
set -e

subsection "Topic list (timed)"
tmp_topics="$(mktemp)"
set +e
time kafka_admin kafka-topics.sh --list >"$tmp_topics" 2>"${tmp_topics}.err"
rc=$?
set -e
echo "exit=$rc topic_count=$(wc -l <"$tmp_topics")"
if [[ -s "${tmp_topics}.err" ]]; then
  echo "stderr:"
  head -20 "${tmp_topics}.err"
fi
head -5 "$tmp_topics" || true

subsection "Partition count (describe all — can be heavy)"
tmp_desc="$(mktemp)"
set +e
time kafka_admin kafka-topics.sh --describe >"$tmp_desc" 2>"${tmp_desc}.err"
rc=$?
set -e
echo "exit=$rc describe_lines=$(wc -l <"$tmp_desc") partition_lines=$(grep -c 'Partition:' "$tmp_desc" || true)"
head -5 "${tmp_desc}.err" 2>/dev/null || true

subsection "Under-replicated partitions"
set +e
time kafka_admin kafka-topics.sh --describe --under-replicated-partitions 2>&1 | tee "$REPORT_DIR/under_replicated.txt" | head -50
set -e

subsection "Unavailable / offline partitions"
set +e
time kafka_admin kafka-topics.sh --describe --unavailable-partitions 2>&1 | tee "$REPORT_DIR/unavailable_partitions.txt" | head -50
set -e

subsection "Consumer groups list (timed)"
tmp_groups="$(mktemp)"
set +e
time kafka_admin kafka-consumer-groups.sh --list >"$tmp_groups" 2>"${tmp_groups}.err"
rc=$?
set -e
echo "exit=$rc group_count=$(wc -l <"$tmp_groups")"
head -10 "${tmp_groups}.err" 2>/dev/null || true

rm -f "$tmp_topics" "${tmp_topics}.err" "$tmp_desc" "${tmp_desc}.err" "$tmp_groups" "${tmp_groups}.err"

#!/usr/bin/env bash
# Time UI-like admin operations (topics describe, all groups describe).
# Useful when a Kafka UI feels slow but SSH is fine.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../lib/admin_common.sh"

section "04 Admin ops timing (UI-like)"

if [[ ! -x "$KAFKA_BIN/kafka-topics.sh" ]]; then
  echo "ERROR: Kafka binaries missing under $KAFKA_BIN" >&2
  exit 1
fi

if [[ "$KAFKA_BOOTSTRAP" != "$KAFKA_BOOTSTRAP_LOCAL" ]]; then
  subsection "list topics via BOOTSTRAP_LOCAL ($KAFKA_BOOTSTRAP_LOCAL)"
  set +e
  time kafka_admin_local kafka-topics.sh --list | tee "$REPORT_DIR/topics_local.txt" | wc -l
  set -e
fi

subsection "list topics via KAFKA_BOOTSTRAP ($KAFKA_BOOTSTRAP)"
set +e
time kafka_admin kafka-topics.sh --list | tee "$REPORT_DIR/topics_list.txt" | wc -l
set -e

subsection "describe ALL topics"
set +e
time kafka_admin kafka-topics.sh --describe | tee "$REPORT_DIR/topics_describe.txt" | wc -l
set -e

subsection "list consumer groups"
set +e
time kafka_admin kafka-consumer-groups.sh --list | tee "$REPORT_DIR/consumer_groups.txt" | wc -l
set -e

subsection "describe ALL consumer groups (lag — heavy / UI-like)"
set +e
time kafka_admin kafka-consumer-groups.sh --describe --all-groups \
  >"$REPORT_DIR/consumer_groups_describe.txt" \
  2>"$REPORT_DIR/consumer_groups_describe.err"
echo "exit=$? lines=$(wc -l <"$REPORT_DIR/consumer_groups_describe.txt")"
set -e
if [[ -s "$REPORT_DIR/consumer_groups_describe.err" ]]; then
  echo "stderr (tail):"
  tail -20 "$REPORT_DIR/consumer_groups_describe.err"
fi

echo
echo "Tip: if list/describe hang for ~60s, check bootstrap.servers in command-config"
echo "     matches this cluster's advertised EXTERNAL listener."

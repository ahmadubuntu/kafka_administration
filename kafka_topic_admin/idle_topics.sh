#!/usr/bin/env bash
# Compatibility wrapper — prefer ./topic_hygiene.sh
# (former name: idle_topics.sh)
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${ROOT_DIR}/topic_hygiene.sh" "$@"

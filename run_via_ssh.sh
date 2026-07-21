#!/usr/bin/env bash
# Copy this toolkit to a remote Kafka host and run run_all.sh there.
#
# Usage:
#   export KAFKA_SSH_HOST=devkafka
#   ./run_via_ssh.sh
#
# Optional:
#   REMOTE_DIR=/tmp/kafka_administration ./run_via_ssh.sh
#   ./run_via_ssh.sh user@host
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

HOST="${1:-${KAFKA_SSH_HOST:-}}"
if [[ -z "$HOST" ]]; then
  echo "Usage: $0 <ssh-host>   or set KAFKA_SSH_HOST" >&2
  exit 1
fi

REMOTE_DIR="${REMOTE_DIR:-/tmp/kafka_administration}"
STAMP="$(date +%Y%m%d_%H%M%S)"
LOCAL_PULL="$ROOT/reports/remote_${HOST}_${STAMP}"

echo "Syncing toolkit -> $HOST:$REMOTE_DIR"
ssh -o BatchMode=yes "$HOST" "mkdir -p '$REMOTE_DIR'"
# Prefer rsync; fall back to tar over ssh
if command -v rsync >/dev/null 2>&1; then
  rsync -az --delete \
    --exclude '.git/' \
    --exclude 'reports/' \
    --exclude 'env.sh' \
    "$ROOT/" "$HOST:$REMOTE_DIR/"
else
  tar -C "$ROOT" --exclude '.git' --exclude 'reports' --exclude 'env.sh' -czf - . \
    | ssh -o BatchMode=yes "$HOST" "mkdir -p '$REMOTE_DIR' && tar -C '$REMOTE_DIR' -xzf -"
fi

# Ship env.sh if present (contains cluster-specific paths; may include secrets)
if [[ -f "$ROOT/env.sh" ]]; then
  echo "Copying env.sh to remote (contains local overrides / possibly secrets)"
  scp -q "$ROOT/env.sh" "$HOST:$REMOTE_DIR/env.sh"
elif [[ -f "$ROOT/env.example" ]]; then
  scp -q "$ROOT/env.example" "$HOST:$REMOTE_DIR/env.example"
fi

echo "Running suite on $HOST ..."
ssh -o BatchMode=yes "$HOST" "cd '$REMOTE_DIR' && chmod +x run_all.sh scripts/*.sh lib/*.sh 2>/dev/null; ./run_all.sh"

echo "Fetching reports ..."
mkdir -p "$LOCAL_PULL"
# newest reports dir on remote
remote_latest="$(ssh -o BatchMode=yes "$HOST" "ls -1dt '$REMOTE_DIR'/reports/* 2>/dev/null | head -1")"
if [[ -n "$remote_latest" ]]; then
  scp -qr "$HOST:$remote_latest" "$LOCAL_PULL/"
  echo "Saved under $LOCAL_PULL"
else
  echo "WARNING: no remote reports found"
fi

#!/usr/bin/env bash
# Copy the broker-admin toolkit to a remote Kafka host and run run_admin_suite.sh there.
#
# Usage:
#   export KAFKA_SSH_HOST=devkafka
#   ./run_via_ssh.sh
#
# Optional:
#   REMOTE_DIR=/tmp/kafka_administration ./run_via_ssh.sh
#   ./run_via_ssh.sh user@host
#   ./run_via_ssh.sh user@host -- --only host,service
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

HOST=""
CHILD_ARGS=()
if [[ "${1:-}" == "--" ]]; then
  shift
  CHILD_ARGS=("$@")
elif [[ $# -gt 0 && "$1" != --* ]]; then
  HOST="$1"
  shift
  if [[ "${1:-}" == "--" ]]; then
    shift
    CHILD_ARGS=("$@")
  elif [[ $# -gt 0 ]]; then
    CHILD_ARGS=("$@")
  fi
fi
HOST="${HOST:-${KAFKA_SSH_HOST:-}}"
if [[ -z "$HOST" ]]; then
  echo "Usage: $0 <ssh-host> [-- admin-suite-args...]   or set KAFKA_SSH_HOST" >&2
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

echo "Running admin suite on $HOST ..."
remote_cmd="cd '$REMOTE_DIR' && chmod +x run_admin_suite.sh scripts/*.sh lib/*.sh 2>/dev/null; "
remote_cmd+="./run_admin_suite.sh"
for a in "${CHILD_ARGS[@]}"; do
  remote_cmd+=" $(printf '%q' "$a")"
done
ssh -o BatchMode=yes "$HOST" "$remote_cmd"

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

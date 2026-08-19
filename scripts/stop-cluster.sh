#!/usr/bin/env bash
# Stop a cluster started by ./scripts/start-cluster.sh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="${PID_FILE:-$ROOT/.cluster/pids}"

if [[ ! -f "$PID_FILE" ]]; then
  echo "no PID file at $PID_FILE (cluster not running?)" >&2
  exit 1
fi

while read -r pid; do
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
  fi
done <"$PID_FILE"

rm -f "$PID_FILE"
echo "cluster stopped"

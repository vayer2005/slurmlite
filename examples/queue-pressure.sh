#!/usr/bin/env bash
# Submit two jobs onto a small cluster so the second must wait.
#
# Assumes a 4-node cluster (./scripts/start-cluster.sh 4). Job A takes 3 nodes;
# job B needs 2, so it stays pending until A finishes and frees capacity.
#
# Requires slctl on PATH and a running controller.
#
# Usage:
#   ./scripts/start-cluster.sh 4
#   ./examples/queue-pressure.sh
#   slctl queue

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOLD="${ROOT}/examples/hold.sh"

if ! command -v slctl >/dev/null 2>&1; then
  echo "queue-pressure: slctl not on PATH; build with: go build -o slctl ./cmd/slctl" >&2
  exit 1
fi

echo "queue-pressure: submitting job A (3 nodes, 15s hold)"
slctl submit --nodes 3 --cpus-per-node 2 -- "$HOLD" 15

echo "queue-pressure: submitting job B (2 nodes, 10s hold) — should pend until A completes"
slctl submit --nodes 2 --cpus-per-node 2 -- "$HOLD" 10

echo "queue-pressure: run 'slctl queue' and 'slctl nodes' to watch B wait, then start"

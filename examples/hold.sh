#!/usr/bin/env bash
# Hold allocated nodes for a known wall-clock duration, then exit 0.
#
# Use this to watch slctl queue / slctl nodes while a job occupies the cluster,
# then confirm resources are released when it finishes.
#
# Usage (after cluster is running):
#   slctl submit --nodes 2 --cpus-per-node 2 -- ./examples/hold.sh
#   slctl submit --nodes 2 --cpus-per-node 2 -- ./examples/hold.sh 15

set -euo pipefail

SECONDS_TO_HOLD="${1:-15}"

echo "hold: job=${SLURMLITE_JOB_ID:-unknown} rank=${SLURMLITE_NODE_RANK:-?} nnodes=${SLURMLITE_NNODES:-?} host=$(hostname) sleeping ${SECONDS_TO_HOLD}s"
sleep "$SECONDS_TO_HOLD"
echo "hold: done"

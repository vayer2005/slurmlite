#!/usr/bin/env bash
# Sleep longer than --time so the node executor should kill this process.
#
# If timeout enforcement works, the job ends as failed/cancelled before
# "timeout: still running" prints. If you see that line, the kill did not fire.
#
# Usage (after cluster is running):
#   slctl submit --nodes 1 --cpus-per-node 1 --time 5s -- ./examples/timeout.sh
#   slctl submit --nodes 1 --cpus-per-node 1 --time 5s -- ./examples/timeout.sh 120

set -euo pipefail

SECONDS_TO_SLEEP="${1:-120}"

echo "timeout: job=${SLURMLITE_JOB_ID:-unknown} rank=${SLURMLITE_NODE_RANK:-?} host=$(hostname) sleeping ${SECONDS_TO_SLEEP}s (expect kill from max_duration)"
sleep "$SECONDS_TO_SLEEP"
echo "timeout: still running — process was not killed"
exit 1

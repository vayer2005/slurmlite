#!/usr/bin/env bash
# Print identity on every allocated node, then exit 0.
#
# Submit with --nodes N and grep the logs: you should see ranks 0 .. N-1
# (or "?" if the controller has not started injecting SLURMLITE_* env vars).
#
# Usage (after cluster is running):
#   slctl submit --nodes 3 --cpus-per-node 1 -- ./examples/hello.sh

set -euo pipefail

echo "hello: job=${SLURMLITE_JOB_ID:-unknown} rank=${SLURMLITE_NODE_RANK:-?} nnodes=${SLURMLITE_NNODES:-?} cpus=${SLURMLITE_CPUS_PER_NODE:-?} host=$(hostname) pid=$$"

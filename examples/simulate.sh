#!/usr/bin/env bash
# Dummy MPI-style job for SlurmLite demos.
#
# Prints which node this rank is on, holds the allocation briefly, then exits 0.
# Controller-injected env (when implemented): SLURMLITE_JOB_ID, SLURMLITE_NODE_RANK,
# SLURMLITE_NNODES.
#
# Usage (after cluster is running):
#   slctl submit --nodes 3 --cpus-per-node 2 -- ./examples/simulate.sh
#   slctl submit --nodes 3 --cpus-per-node 2 -- ./examples/simulate.sh 5

set -euo pipefail

SECONDS_TO_WORK="${1:-5}"

echo "simulate: job=${SLURMLITE_JOB_ID:-unknown} rank=${SLURMLITE_NODE_RANK:-?} nnodes=${SLURMLITE_NNODES:-?} host=$(hostname) working ${SECONDS_TO_WORK}s"
sleep "$SECONDS_TO_WORK"
echo "simulate: rank=${SLURMLITE_NODE_RANK:-?} done"

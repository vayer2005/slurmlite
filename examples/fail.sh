#!/usr/bin/env bash
# Exit non-zero so the node reports JOB_EVENT_FAILED.
#
# Confirms the job transitions running → failed and that allocated CPUs
# are released even when the command crashes.
#
# Usage (after cluster is running):
#   slctl submit --nodes 1 --cpus-per-node 1 -- ./examples/fail.sh

echo "fail: job=${SLURMLITE_JOB_ID:-unknown} rank=${SLURMLITE_NODE_RANK:-?} host=$(hostname) exiting 1" >&2
exit 1

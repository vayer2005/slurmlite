#!/usr/bin/env bash
# Multi-node overlap check: every rank writes a file, rank 0 waits until all
# ranks have checked in. Proves the gang-scheduled processes ran together,
# not one after another.
#
# Uses a shared directory (default /tmp/slurmlite/<job-id>). That works when
# all node agents share a filesystem — true for ./scripts/start-cluster.sh
# on one machine. Separate hosts would need NFS (or similar) at the same path.
#
# Usage (after cluster is running):
#   slctl submit --nodes 3 --cpus-per-node 1 -- ./examples/barrier.sh
#
# Override the rendezvous directory:
#   slctl submit --nodes 3 --cpus-per-node 1 -- ./examples/barrier.sh /tmp/slurmlite

set -euo pipefail

JOB_ID="${SLURMLITE_JOB_ID:-local-$$}"
RANK="${SLURMLITE_NODE_RANK:-0}"
NNODES="${SLURMLITE_NNODES:-1}"
ROOT="${1:-${SLURMLITE_SHARED_DIR:-/tmp/slurmlite}}"
DIR="${ROOT}/${JOB_ID}"
WAIT_SECONDS="${BARRIER_TIMEOUT:-30}"

mkdir -p "$DIR"
echo "host=$(hostname) rank=${RANK} pid=$$ ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "${DIR}/rank-${RANK}"

echo "barrier: job=${JOB_ID} rank=${RANK} nnodes=${NNODES} host=$(hostname) wrote ${DIR}/rank-${RANK}"

if [[ "$RANK" != "0" ]]; then
  exit 0
fi

deadline=$((SECONDS + WAIT_SECONDS))
while true; do
  found=0
  for ((i = 0; i < NNODES; i++)); do
    if [[ -f "${DIR}/rank-${i}" ]]; then
      found=$((found + 1))
    fi
  done
  if ((found >= NNODES)); then
    echo "barrier: all ${NNODES} ranks present"
    cat "${DIR}"/rank-*
    exit 0
  fi
  if ((SECONDS >= deadline)); then
    echo "barrier: timed out after ${WAIT_SECONDS}s with ${found}/${NNODES} ranks" >&2
    ls -l "$DIR" >&2 || true
    exit 1
  fi
  sleep 0.2
done

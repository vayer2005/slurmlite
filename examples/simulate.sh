#!/usr/bin/env bash
# Dummy MPI-style job for SlurmLite demos.
#
# When gang-scheduled across N nodes, this script should:
#   - Print which node it is running on (hostname, node rank if env var provided)
#   - Simulate work with sleep
#   - Exit 0 on success
#
# Usage (after cluster is running):
#   slctl submit --nodes 3 --cpus-per-node 2 -- ./examples/simulate.sh
#
# Optional: controller can inject env vars like SLURMLITE_NODE_RANK, SLURMLITE_JOB_ID
# so the script can demonstrate multi-node coordination.

# TODO: implement demo script body

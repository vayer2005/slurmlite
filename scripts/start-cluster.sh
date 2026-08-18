#!/usr/bin/env bash
# Start a local SlurmLite cluster for development and demos.
#
# Should:
#   1. Build controller, node, and slctl binaries (go build)
#   2. Start controller on a known port (e.g. localhost:50051)
#   3. Start N node agents (e.g. 4 nodes with configurable CPU counts)
#   4. Print PIDs and how to stop the cluster
#   5. Optionally wait and tail logs
#
# Usage:
#   ./scripts/start-cluster.sh [num_nodes]
#
# Environment overrides (optional):
#   CONTROLLER_ADDR, NODE_COUNT, CPUS_PER_NODE

# TODO: implement cluster startup script

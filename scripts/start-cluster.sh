#!/usr/bin/env bash
# Start a local SlurmLite cluster for development and demos.
#
# Builds controller, node, and slctl into ./bin/, starts the controller,
# then starts N node agents. Logs go to ./.cluster/logs/.
#
# Usage:
#   ./scripts/start-cluster.sh [num_nodes]
#
# Environment overrides (optional):
#   CONTROLLER_ADDR   default 127.0.0.1:50051
#   NODE_COUNT        default 4 (or first script argument)
#   CPUS_PER_NODE     default 4
#   BIN_DIR           default ./bin
#   LOG_DIR           default ./.cluster/logs
#
# Stop the cluster:
#   ./scripts/stop-cluster.sh
#   # or: kill the PIDs printed at startup

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CONTROLLER_ADDR="${CONTROLLER_ADDR:-127.0.0.1:50051}"
NODE_COUNT="${1:-${NODE_COUNT:-4}}"
CPUS_PER_NODE="${CPUS_PER_NODE:-4}"
BIN_DIR="${BIN_DIR:-$ROOT/bin}"
LOG_DIR="${LOG_DIR:-$ROOT/.cluster/logs}"
PID_FILE="${PID_FILE:-$ROOT/.cluster/pids}"

mkdir -p "$BIN_DIR" "$LOG_DIR" "$(dirname "$PID_FILE")"

STOP_SCRIPT="$ROOT/scripts/stop-cluster.sh"
if [[ -f "$PID_FILE" ]]; then
  echo "stopping previous cluster"
  "$STOP_SCRIPT" 2>/dev/null || rm -f "$PID_FILE"
fi

echo "building binaries into $BIN_DIR"
go build -o "$BIN_DIR/controller" ./cmd/controller
go build -o "$BIN_DIR/node" ./cmd/node
go build -o "$BIN_DIR/slctl" ./cmd/slctl

: >"$PID_FILE"

echo "starting controller on $CONTROLLER_ADDR"
nohup "$BIN_DIR/controller" -listen ":${CONTROLLER_ADDR##*:}" >"$LOG_DIR/controller.log" 2>&1 &
echo "$!" >>"$PID_FILE"

sleep 0.5

for ((i = 1; i <= NODE_COUNT; i++)); do
  node_id="node-${i}"
  echo "starting $node_id (${CPUS_PER_NODE} CPUs)"
  nohup "$BIN_DIR/node" \
    -controller "$CONTROLLER_ADDR" \
    -id "$node_id" \
    -cpus "$CPUS_PER_NODE" \
    >"$LOG_DIR/${node_id}.log" 2>&1 &
  echo "$!" >>"$PID_FILE"
done

export PATH="$BIN_DIR:$PATH"
export SLURMLITE_CONTROLLER="$CONTROLLER_ADDR"

cat <<EOF

SlurmLite cluster is up.

  Controller:  $CONTROLLER_ADDR
  Nodes:       $NODE_COUNT × ${CPUS_PER_NODE} CPUs (node-1 … node-${NODE_COUNT})
  Binaries:    $BIN_DIR
  Logs:        $LOG_DIR
  PIDs:        $PID_FILE

Add slctl to your PATH for this shell:

  export PATH="$BIN_DIR:\$PATH"
  export SLURMLITE_CONTROLLER="$CONTROLLER_ADDR"

Try:

  slctl nodes
  slctl submit --nodes 2 --cpus-per-node 1 -- $ROOT/examples/hello.sh
  slctl queue

Stop:

  kill \$(cat "$PID_FILE") && rm -f "$PID_FILE"

EOF

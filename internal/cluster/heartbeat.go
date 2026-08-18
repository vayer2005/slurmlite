// Heartbeat handling: process liveness signals from nodes.
//
// Responsibilities:
//   - Accept Heartbeat RPC payloads (node ID, current CPU usage, optional health info)
//   - Update last_heartbeat timestamp in registry
//   - Transition node back to online if it was offline and reconnected
//   - Reject heartbeats from unregistered nodes (or auto-register if policy allows)
//
// Heartbeat interval and timeout should be configurable:
//   - Nodes send heartbeats every N seconds
//   - Reaper marks node offline if no heartbeat for M seconds

package cluster

// TODO: implement heartbeat processor wired to registry

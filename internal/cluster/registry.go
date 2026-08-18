// Node registry: tracks all compute nodes in the cluster.
//
// Responsibilities:
//   - RegisterNode: add a node on first connection (ID, hostname, total CPUs)
//   - Update node status and resource usage on heartbeat
//   - Mark node offline when reaper detects missed heartbeats
//   - ListNodes: return snapshot for slctl nodes and scheduler
//   - FindAvailableNodes: return nodes with enough free CPUs for gang scheduling
//   - ReserveNodes / ReleaseNodes: tie nodes to a running job
//
// Node fields:
//   - ID, hostname/address
//   - Status: online | offline
//   - Total CPUs, allocated CPUs
//   - Last heartbeat timestamp
//   - Current job ID (if busy)

package cluster

// TODO: implement Node type and Registry with thread-safe operations

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
import (
	"sync"
	"time"
)
type Status string

const (
	StatusOnline Status = "online"
	StatusOffline Status = "offline"
)

type Node struct {
	ID string
	Status Status
	TotalCPUs int
	AllocatedCPUs int
	LastHeartbeat time.Time
	CurrentJobID string // -1 if not busy
}



type Registry struct {
	nodes map[string]Node
	mu sync.RWMutex
}


func (r *Registry) RegisterNode(node Node) {

	
}

func (r *Registry) UpdateNode(node Node) {

}

func (r *Registry) MarkNodeOffline(node Node) {

}

func (r *Registry) ListNodes() []Node {

	return nil
}	

func (r *Registry) FindAvailableNodes(numNodes int) []Node {

	return nil
}

func (r *Registry) ReserveNodes(nodeIDs []string) error {

	return nil
}

func (r *Registry) ReleaseNodes(nodeIDs []string) error {

	return nil
}	
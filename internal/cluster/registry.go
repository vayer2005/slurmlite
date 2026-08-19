// Node registry: tracks all compute nodes in the cluster.
//
// Responsibilities:
//   - RegisterNode: add a node on first connection (ID, hostname, total CPUs)
//   - Update node status and resource usage on heartbeat
//   - Mark node offline when reaper detects missed heartbeats
//   - ListNodes: return snapshot for slctl nodes and scheduler
//   - ReserveNodes / ReleaseNodes: tie nodes to a running job
//
// Node fields:
//   - ID, hostname/address
//   - Status: online | offline
//   - Total CPUs, allocated CPUs
//   - Last heartbeat timestamp
//   - Current job ID (if busy)

package cluster

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type Status string

const (
	StatusOnline  Status = "online"
	StatusOffline Status = "offline"
)

type Node struct {
	ID            string
	Status        Status
	TotalCPUs     int
	AllocatedCPUs int
	LastHeartbeat time.Time
	CurrentJobID  string
}

type Registry struct {
	nodes map[string]Node
	mu    sync.RWMutex
}

func Make() *Registry {
	return &Registry{nodes: make(map[string]Node)}
}

func (r *Registry) RegisterNode(node Node) {
	if node.ID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.nodes == nil {
		r.nodes = make(map[string]Node)
	}
	if node.Status == "" {
		node.Status = StatusOnline
	}
	r.nodes[node.ID] = node
}

func (r *Registry) UpdateNode(node Node) {
	if node.ID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[node.ID]; !ok {
		return
	}
	r.nodes[node.ID] = node
}

func (r *Registry) MarkNodeOffline(node Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[node.ID]
	if !ok {
		return
	}
	n.Status = StatusOffline
	r.nodes[node.ID] = n
}

func (r *Registry) ListNodes() []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (r *Registry) ReserveNodes(jobID string, nodeIDs []string, cpusPerNode int) error {
	if jobID == "" {
		return fmt.Errorf("job id must be non-empty")
	}
	if cpusPerNode < 1 {
		return fmt.Errorf("cpus_per_node must be >= 1, got %d", cpusPerNode)
	}
	if len(nodeIDs) == 0 {
		return fmt.Errorf("no nodes to reserve")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, id := range nodeIDs {
		n, ok := r.nodes[id]
		if !ok {
			return fmt.Errorf("node %s not registered", id)
		}
		if n.Status != StatusOnline {
			return fmt.Errorf("node %s is not online", id)
		}
		if n.CurrentJobID != "" {
			return fmt.Errorf("node %s is busy with job %s", id, n.CurrentJobID)
		}
		if n.TotalCPUs-n.AllocatedCPUs < cpusPerNode {
			return fmt.Errorf("node %s has insufficient free CPUs", id)
		}
	}

	for _, id := range nodeIDs {
		n := r.nodes[id]
		n.AllocatedCPUs += cpusPerNode
		n.CurrentJobID = jobID
		r.nodes[id] = n
	}
	return nil
}

func (r *Registry) ReleaseNodes(nodeIDs []string, cpusPerNode int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, id := range nodeIDs {
		n, ok := r.nodes[id]
		if !ok {
			continue
		}
		n.AllocatedCPUs -= cpusPerNode
		if n.AllocatedCPUs < 0 {
			n.AllocatedCPUs = 0
		}
		n.CurrentJobID = ""
		r.nodes[id] = n
	}
	return nil
}

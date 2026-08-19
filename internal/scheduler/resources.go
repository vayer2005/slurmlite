// Resource accounting: track CPU allocation across the cluster.
//
// Responsibilities:
//   - Compute cluster-wide free capacity (total CPUs minus allocated)
//   - Check if a ResourceRequest (N nodes × M CPUs) can be satisfied
//   - Reserve resources for a job on specific nodes (atomic with gang schedule)
//   - Release resources when a job completes, fails, or is cancelled
//
// Gang scheduling constraint:
//   A job needs nodes_required nodes where each has at least cpus_per_node free CPUs.
//   Partial allocation is never valid — all nodes must be reserved together.

package scheduler

import (
	"distr-scheduling/internal/cluster"
	"distr-scheduling/internal/job"
)

func FreeCPUs(n cluster.Node) int {
	free := n.TotalCPUs - n.AllocatedCPUs
	if free < 0 {
		return 0
	}
	return free
}

func nodeFits(n cluster.Node, cpusPerNode int) bool {
	return n.Status == cluster.StatusOnline && n.CurrentJobID == "" && FreeCPUs(n) >= cpusPerNode
}

// CanSatisfy reports whether the snapshot has enough free nodes for req.
func CanSatisfy(nodes []cluster.Node, req job.ResourceRequest) bool {
	return len(SelectGang(nodes, req)) == req.NodesRequired
}

// SelectGang returns node IDs that can host req together, or nil if the
// request cannot be fully placed. Selection is deterministic (sorted by ID).
func SelectGang(nodes []cluster.Node, req job.ResourceRequest) []string {
	if req.NodesRequired < 1 || req.CPUsPerNode < 1 {
		return nil
	}
	ids := make([]string, 0, req.NodesRequired)
	for _, n := range nodes {
		if !nodeFits(n, req.CPUsPerNode) {
			continue
		}
		ids = append(ids, n.ID)
		if len(ids) == req.NodesRequired {
			return ids
		}
	}
	return nil
}

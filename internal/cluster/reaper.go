package cluster

import "fmt"

func handleDeadNodes(r *Registry, jobs JobFailure, dead []Node) {
	if len(dead) == 0 {
		return
	}

	seen := make(map[string]struct{})
	for _, n := range dead {
		if n.CurrentJobID == "" {
			continue
		}
		if _, ok := seen[n.CurrentJobID]; ok {
			continue
		}
		seen[n.CurrentJobID] = struct{}{}

		reason := fmt.Sprintf("node %s lost", n.ID)
		if jobs != nil {
			ids, cpus, ok := jobs.FailRunningJob(n.CurrentJobID, reason)
			if ok {
				_ = r.ReleaseNodes(ids, cpus)
				continue
			}
		}
		_ = r.ReleaseNodes([]string{n.ID}, n.AllocatedCPUs)
	}
}

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

// TODO: implement resource accounting helpers

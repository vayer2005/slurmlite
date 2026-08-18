// Job model and status definitions.
//
// A Job represents a unit of work submitted by a user via slctl submit.
//
// Fields to include:
//   - ID (unique, e.g. UUID or incrementing)
//   - Status: pending | running | completed | failed | cancelled
//   - Spec: nodes_required, cpus_per_node, command, args, max_duration
//   - Assigned node IDs (populated when gang-scheduled)
//   - Timestamps: submitted_at, started_at, finished_at
//   - Exit code / error message (on completion)
//
// Status transitions:
//   pending → running (scheduler assigns nodes)
//   pending → cancelled (user cancel)
//   running → completed | failed | cancelled

package job

// TODO: implement Job type, JobSpec, JobStatus enum, and transition helpers

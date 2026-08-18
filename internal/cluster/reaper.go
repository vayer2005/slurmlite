// Node reaper: detect and handle stale/dead nodes.
//
// Responsibilities:
//   - Run periodically (background goroutine in controller)
//   - Scan registry for nodes whose last_heartbeat exceeds timeout threshold
//   - Mark those nodes offline
//   - If a dead node was running a job:
//       - Mark the job as failed (partial gang failure)
//       - Release any reserved resources on remaining nodes for that job
//       - Notify scheduler to attempt rescheduling or fail the job
//
// This is critical for fault tolerance in distributed scheduling.

package cluster

// TODO: implement reaper loop with configurable timeout

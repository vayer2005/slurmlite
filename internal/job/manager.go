// Job lifecycle manager: central place for job state changes.
//
// Responsibilities:
//   - Create job on submit, add to queue
//   - Mark job running when scheduler assigns nodes
//   - Mark job completed/failed when nodes report status
//   - Cancel job: if pending remove from queue; if running signal nodes to kill work
//   - List jobs for slctl queue (filter by status)
//   - Store completed jobs briefly for status queries (or persist to disk later)
//
// All state transitions should be atomic and notify the scheduler when relevant
// (e.g. job finished → release resources → scheduler can schedule next job).

package job

// TODO: implement JobManager coordinating queue and in-memory job store
import (
	"sync"
	"time"
)

type JobManager struct {
	queue Queue
	jobs map[string]Job
	mu sync.RWMutex
}

func (m *JobManager) Submit(job *Job) error {}
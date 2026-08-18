// Pending job queue.
//
// Responsibilities:
//   - Enqueue new jobs on submit (status = pending)
//   - Return next schedulable job (typically FIFO head)
//   - Remove or skip jobs that are cancelled while pending
//   - Thread-safe: scheduler loop and gRPC handlers access concurrently


package job

import "sync"

type Queue struct {
	jobs []string // job IDs
	mu sync.RWMutex
}

func (q *Queue) Enqueue(jobID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.jobs = append(q.jobs, jobID)

}

func (q *Queue) Dequeue() (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.jobs) == 0 {
		return "", false
	}
	jobID := q.jobs[0]
	q.jobs = q.jobs[1:]
	return jobID, true
}
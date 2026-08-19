// Job lifecycle manager: central place for job state changes.
//
// Responsibilities:
//   - Create job on submit, add to queue
//   - Mark job running when scheduler assigns nodes
//   - Mark job completed/failed when nodes report status
//   - Cancel job: if pending remove from queue; if running signal nodes to kill work
//   - List jobs for slctl queue (filter by status)
//   - Store completed jobs briefly for status queries (or persist to disk later)

package job

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrJobNotFound = errors.New("job not found")
	ErrJobExists   = errors.New("job already exists")
)

type nodeReport struct {
	failed   bool
	exitCode int
	errMsg   string
}

type JobManager struct {
	pending []string // FIFO job IDs awaiting scheduling
	jobs    map[string]*Job
	reports map[string]map[string]nodeReport
	mu      sync.RWMutex
}


func (m *JobManager) Submit(job *Job) error {
	if job == nil {
		return fmt.Errorf("job must be non-nil")
	}
	if job.Status != StatusPending {
		return fmt.Errorf("submitted job must be pending, got %s", job.Status)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.jobs[job.ID]; exists {
		return ErrJobExists
	}

	m.jobs[job.ID] = job
	m.pending = append(m.pending, job.ID)
	return nil
}

func (m *JobManager) Get(id string) (*Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	job, ok := m.jobs[id]
	if !ok {
		return nil, ErrJobNotFound
	}
	return cloneJob(job), nil
}

// List returns jobs matching any of the given statuses. An empty filter returns all jobs.
func (m *JobManager) List(statuses ...Status) []*Job {
	m.mu.RLock()
	defer m.mu.RUnlock()

	filter := make(map[Status]struct{}, len(statuses))
	for _, s := range statuses {
		filter[s] = struct{}{}
	}
	matchAll := len(filter) == 0

	out := make([]*Job, 0, len(m.jobs))
	for _, job := range m.jobs {
		if matchAll {
			out = append(out, cloneJob(job))
			continue
		}
		if _, ok := filter[job.Status]; ok {
			out = append(out, cloneJob(job))
		}
	}
	return out
}

// PeekPending returns the ID at the head of the pending queue without removing it.
func (m *JobManager) PeekPending() (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.peekPendingID()
}

// PeekPendingJob returns a snapshot of the head pending job.
func (m *JobManager) PeekPendingJob() (*Job, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	id, ok := m.peekPendingID()
	if !ok {
		return nil, false
	}
	job, ok := m.jobs[id]
	if !ok {
		return nil, false
	}
	return cloneJob(job), true
}

func (m *JobManager) MarkRunning(id string, nodeIDs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, ok := m.jobs[id]
	if !ok {
		return ErrJobNotFound
	}
	if err := job.MarkRunning(nodeIDs); err != nil {
		return err
	}

	m.removePending(id)
	m.reports[id] = make(map[string]nodeReport)
	return nil
}

func (m *JobManager) MarkCompleted(id string, exitCode int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, ok := m.jobs[id]
	if !ok {
		return ErrJobNotFound
	}
	if err := job.MarkCompleted(exitCode); err != nil {
		return err
	}

	delete(m.reports, id)
	return nil
}

func (m *JobManager) MarkFailed(id string, exitCode int, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, ok := m.jobs[id]
	if !ok {
		return ErrJobNotFound
	}
	if err := job.MarkFailed(exitCode, errMsg); err != nil {
		return err
	}

	delete(m.reports, id)
	return nil
}

// FailRunningJob marks a running job failed (node loss) and returns its
// assignment so the caller can release nodes. ok is false if the job is not running.
func (m *JobManager) FailRunningJob(id, reason string) ([]string, int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, ok := m.jobs[id]
	if !ok || job.Status != StatusRunning {
		return nil, 0, false
	}
	if err := job.MarkFailed(1, reason); err != nil {
		return nil, 0, false
	}
	delete(m.reports, id)
	assigned := append([]string(nil), job.AssignedNodeIDs...)
	return assigned, job.Spec.Resources.CPUsPerNode, true
}

// Cancel transitions a job to cancelled. The second return value is true when the
// caller must signal assigned nodes to stop the job.
func (m *JobManager) Cancel(id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, ok := m.jobs[id]
	if !ok {
		return false, ErrJobNotFound
	}

	switch job.Status {
	case StatusPending:
		m.removePending(id)
		if err := job.MarkCancelled(); err != nil {
			return false, err
		}
		return false, nil
	case StatusRunning:
		if err := job.MarkCancelled(); err != nil {
			return false, err
		}
		delete(m.reports, id)
		return true, nil
	default:
		return false, fmt.Errorf("cannot cancel job %s in status %s", id, job.Status)
	}
}

// ReportNodeEvent records a completion or failure from one assigned node. The job
// fails immediately on the first node failure; otherwise it completes once every
// assigned node has reported success.
func (m *JobManager) ReportNodeEvent(jobID, nodeID string, failed bool, exitCode int, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, ok := m.jobs[jobID]
	if !ok {
		return ErrJobNotFound
	}
	if job.Status != StatusRunning {
		return nil
	}
	if !assignedNode(job, nodeID) {
		return fmt.Errorf("node %s is not assigned to job %s", nodeID, jobID)
	}

	reports := m.reports[jobID]
	if reports == nil {
		reports = make(map[string]nodeReport)
		m.reports[jobID] = reports
	}
	if _, seen := reports[nodeID]; seen {
		return nil
	}

	reports[nodeID] = nodeReport{
		failed:   failed,
		exitCode: exitCode,
		errMsg:   errMsg,
	}

	if failed {
		if err := job.MarkFailed(exitCode, errMsg); err != nil {
			return err
		}
		delete(m.reports, jobID)
		return nil
	}

	if len(reports) < len(job.AssignedNodeIDs) {
		return nil
	}

	maxExit := 0
	for _, r := range reports {
		if r.exitCode > maxExit {
			maxExit = r.exitCode
		}
	}
	if err := job.MarkCompleted(maxExit); err != nil {
		return err
	}
	delete(m.reports, jobID)
	return nil
}

func (m *JobManager) peekPendingID() (string, bool) {
	if len(m.pending) == 0 {
		return "", false
	}
	return m.pending[0], true
}

func (m *JobManager) removePending(jobID string) {
	for i, id := range m.pending {
		if id == jobID {
			m.pending = append(m.pending[:i], m.pending[i+1:]...)
			return
		}
	}
}

func assignedNode(job *Job, nodeID string) bool {
	for _, id := range job.AssignedNodeIDs {
		if id == nodeID {
			return true
		}
	}
	return false
}

func cloneJob(job *Job) *Job {
	copy := *job
	if job.Spec.Args != nil {
		copy.Spec.Args = append([]string(nil), job.Spec.Args...)
	}
	if job.Spec.Env != nil {
		copy.Spec.Env = make(map[string]string, len(job.Spec.Env))
		for k, v := range job.Spec.Env {
			copy.Spec.Env[k] = v
		}
	}
	if job.AssignedNodeIDs != nil {
		copy.AssignedNodeIDs = append([]string(nil), job.AssignedNodeIDs...)
	}
	return &copy
}

// Entry point for controller to use
func Make() *JobManager {
	return &JobManager{
		pending: []string{},
		jobs:    make(map[string]*Job),
		reports: make(map[string]map[string]nodeReport),
	}
}
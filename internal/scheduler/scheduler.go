// Gang scheduler: assigns jobs only when ALL required nodes are simultaneously available.
//
// Core scheduling loop (called periodically or on queue/node state change):
//   1. Peek at the head of the pending job queue (FIFO or priority)
//   2. Check if enough free nodes exist with sufficient CPUs per node
//   3. If yes: reserve those nodes, transition job to "running", dispatch work to each node
//   4. If no: leave job pending — do NOT partially allocate (gang scheduling guarantee)
//
// Must coordinate with:
//   - internal/job for queue access and job state transitions
//   - internal/cluster for node availability and reservation
//   - internal/scheduler/resources for accounting free vs allocated CPUs

package scheduler

import (
	"strconv"
	"sync"
	"time"

	"distr-scheduling/internal/cluster"
	"distr-scheduling/internal/job"
)

const SCHEDULER_INTERVAL = 1 * time.Second

// Assignment is the per-node work payload pushed when a job is gang-scheduled.
type Assignment struct {
	JobID       string
	Command     string // Command to run on the node
	Args        []string
	Env         map[string]string
	MaxDuration time.Duration
	NodeRank    int
	NNodes      int
	CPUsPerNode int
}

// Dispatcher delivers an assignment to a node (WatchWork stream, mailbox, etc.).
type Dispatcher interface {
	Dispatch(nodeID string, a Assignment) error
}

type Scheduler struct {
	jobManager *job.JobManager
	registry   *cluster.Registry
	dispatch   Dispatcher
	mu         sync.Mutex
}

func (s *Scheduler) Run() {
	for {
		if !s.trySchedule() {
			time.Sleep(SCHEDULER_INTERVAL)
		}
	}
}

func (s *Scheduler) trySchedule() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobManager.PeekPendingJob()
	if !ok {
		return false
	}

	req := j.Spec.Resources
	nodeIDs := SelectGang(s.registry.ListNodes(), req)
	if nodeIDs == nil {
		return false
	}

	if err := s.registry.ReserveNodes(j.ID, nodeIDs, req.CPUsPerNode); err != nil {
		return false
	}

	if err := s.jobManager.MarkRunning(j.ID, nodeIDs); err != nil {
		_ = s.registry.ReleaseNodes(nodeIDs, req.CPUsPerNode)
		return false
	}

	if err := s.dispatchWork(j, nodeIDs); err != nil {
		_ = s.jobManager.MarkFailed(j.ID, 1, err.Error())
		_ = s.registry.ReleaseNodes(nodeIDs, req.CPUsPerNode)
		return false
	}
	return true
}

func (s *Scheduler) dispatchWork(j *job.Job, nodeIDs []string) error {
	if s.dispatch == nil {
		return nil
	}
	nnodes := len(nodeIDs)
	for rank, nodeID := range nodeIDs {
		a := Assignment{
			JobID:       j.ID,
			Command:     j.Spec.Command,
			Args:        append([]string(nil), j.Spec.Args...),
			Env:         assignmentEnv(j, rank, nnodes),
			MaxDuration: j.Spec.Resources.MaxDuration,
			NodeRank:    rank,
			NNodes:      nnodes,
			CPUsPerNode: j.Spec.Resources.CPUsPerNode,
		}
		if err := s.dispatch.Dispatch(nodeID, a); err != nil {
			return err
		}
	}
	return nil
}

func assignmentEnv(j *job.Job, rank, nnodes int) map[string]string {
	env := make(map[string]string, len(j.Spec.Env)+3)
	for k, v := range j.Spec.Env {
		env[k] = v
	}
	env["SLURMLITE_JOB_ID"] = j.ID
	env["SLURMLITE_NODE_RANK"] = strconv.Itoa(rank)
	env["SLURMLITE_NNODES"] = strconv.Itoa(nnodes)
	return env
}

func Make(jobs *job.JobManager, registry *cluster.Registry, dispatch Dispatcher) *Scheduler {
	if jobs == nil {
		jobs = job.Make()
	}
	if registry == nil {
		registry = cluster.Make()
	}
	return &Scheduler{
		jobManager: jobs,
		registry:   registry,
		dispatch:   dispatch,
	}
}
